# Runs the load-test phases behind RESULTS.md and writes CSVs into loadtest\results\.
#
#     .\loadtest\run_benchmark.ps1                          # the before/after tier comparison
#     .\loadtest\run_benchmark.ps1 -Reverse                 # same, phases swapped (drift check)
#     .\loadtest\run_benchmark.ps1 -Tags history -Users 50  # one endpoint, isolated
#     .\loadtest\run_benchmark.ps1 -UvicornWorkers 4 -Phases after -Label workers4
#
# Prerequisites: `docker compose up -d` and `python -m marketdata.ingest` already running, so
# both tiers hold live data.
#
# WHY THIS RUNS FOUR LOAD-GENERATOR PROCESSES
# The first attempt used one Locust process and it pegged a CPU core at 90%, which made the
# CLIENT the bottleneck: the TimescaleDB endpoint measured faster than the Redis one, which is
# impossible. Those numbers were Locust's own overhead. Locust is single-threaded per process
# (gevent), so one process caps out around 1,300 req/s no matter how fast the server is. A
# master plus four workers spreads the client across cores and puts the bottleneck back on the
# thing being measured. `--processes` would do this in one flag, but it is Windows-unsupported.

param(
    [int]$Users = 100,
    [int]$SpawnRate = 20,
    [string]$Duration = "60s",
    [int]$Workers = 4,
    [int]$UvicornWorkers = 1,
    [string]$Tags = "",
    [string[]]$Phases = @("before", "after"),
    [switch]$Reverse,
    [string]$Label = "",
    [string]$ApiHost = "http://localhost:8000"
)

# Deliberately NOT "Stop". Locust logs progress to stderr, and in Windows PowerShell a native
# command writing to stderr under ErrorActionPreference=Stop raises a terminating
# NativeCommandError - which silently killed the master process the first time this ran, while
# the four workers sat retrying a connection to a master that no longer existed.
$ErrorActionPreference = "Continue"
$python = ".venv\Scripts\python.exe"
$locust = ".venv\Scripts\locust.exe"
New-Item -ItemType Directory -Force loadtest\results | Out-Null

function Invoke-Phase {
    param([string]$Name, [string]$ForceDb)

    $outName = if ($Label) { "$Label`_$Name" } else { $Name }
    Write-Host "`n=== Phase: $outName (HISTORY_FORCE_DB=$ForceDb, uvicorn workers=$UvicornWorkers) ===" -ForegroundColor Cyan
    $env:HISTORY_FORCE_DB = $ForceDb

    # One uvicorn worker by default, on purpose: the claim being tested is that a single async
    # process serves this load. Extra workers would hide a blocking call behind parallelism.
    $apiArgs = @("-m", "uvicorn", "marketdata.api:app", "--port", "8000", "--log-level", "warning")
    if ($UvicornWorkers -gt 1) { $apiArgs += @("--workers", "$UvicornWorkers") }
    $api = Start-Process -FilePath $python -ArgumentList $apiArgs -PassThru -NoNewWindow `
        -RedirectStandardOutput "$env:TEMP\bench_api_out.txt" -RedirectStandardError "$env:TEMP\bench_api_err.txt"
    Start-Sleep -Seconds 10

    # Confirm which tier is actually answering before spending a minute measuring it.
    $source = (Invoke-WebRequest "$ApiHost/prices/BTC-USD/history?window=30s&limit=5" -UseBasicParsing | ConvertFrom-Json).source
    Write-Host "history endpoint is serving from: $source"

    # Workers retry until the master accepts them, so starting them first is safe.
    $workerProcs = 1..$Workers | ForEach-Object {
        Start-Process -FilePath $locust -ArgumentList "-f", "loadtest/locustfile.py", "--worker" `
            -PassThru -NoNewWindow -RedirectStandardOutput "$env:TEMP\bench_w$_.txt" -RedirectStandardError "$env:TEMP\bench_we$_.txt"
    }

    $locustArgs = @("-f", "loadtest/locustfile.py", "--master", "--expect-workers", "$Workers",
        "--headless", "-u", "$Users", "-r", "$SpawnRate", "-t", $Duration, "--host", $ApiHost,
        "--csv", "loadtest/results/$outName", "--logfile", "loadtest/results/$outName.log", "--only-summary")
    if ($Tags) { $locustArgs += @("--tags", $Tags) }

    # --logfile, not a `2>&1 |` redirect: see the ErrorActionPreference note above.
    & $locust @locustArgs

    # taskkill /T kills the whole process tree. A venv's python.exe is a launcher that spawns
    # the real interpreter as a child, so Stop-Process on the launcher alone can leave the
    # actual server running - and holding port 8000 against the next phase.
    foreach ($p in $workerProcs) { taskkill /T /F /PID $p.Id 2>$null | Out-Null }
    taskkill /T /F /PID $api.Id 2>$null | Out-Null
    Start-Sleep -Seconds 3
}

# "before" = /history reads TimescaleDB, "after" = /history reads Redis Streams.
$order = if ($Reverse) { $Phases[($Phases.Count - 1)..0] } else { $Phases }
foreach ($phase in $order) {
    Invoke-Phase -Name $phase -ForceDb $(if ($phase -eq "before") { "true" } else { "false" })
}
$env:HISTORY_FORCE_DB = "false"

Write-Host "`nPercentile data written to loadtest\results\*_stats.csv" -ForegroundColor Green

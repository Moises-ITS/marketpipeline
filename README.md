# Real-Time Market Data Pipeline

Ingests a live crypto trade feed, stores it across latency-appropriate tiers, and (once the
API lands) serves it fast enough to prove the design decisions were deliberate.

Full design rationale, tradeoffs, and build order: [`plan.md`](plan.md).

> **Status: build-order steps 1–2 complete.** Infrastructure runs and live trades are
> flowing to the console. Nothing is persisted yet — that is step 3.

---

## Stack

| Layer | Choice | Why |
|---|---|---|
| Feed | Coinbase WebSocket (`matches`) | Push-based, no auth. Binance returns HTTP 451 from US IPs. |
| Hot cache | Redis | In-memory reads for the latency-critical path |
| Durable history | TimescaleDB (Postgres + hypertables) | Time-partitioned chunks keep range queries fast as data grows |
| API | FastAPI (async) | *not built yet — steps 5–7* |

## Prerequisites

Docker Desktop (running) and Python 3.11+.

## Setup

```powershell
# 1. Start Redis + TimescaleDB
docker compose up -d
docker compose ps            # both must say (healthy)

# 2. Python environment
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. Config
Copy-Item .env.example .env
```

## Run

```powershell
# Prove the infrastructure works before blaming anything else
.venv\Scripts\python.exe scripts\verify_infra.py

# Watch live BTC/ETH/SOL trades stream in (Ctrl-C to stop)
.venv\Scripts\python.exe -m marketdata.feed_smoke
```

Expected output from the smoke test — roughly 10–50 trades/sec:

```
19:44:53.118178  BTC-USD   buy     77,516.57  size 0.01331000  lag  142ms
19:44:53.122145  ETH-USD   buy      2,436.48  size 2.28600000  lag  138ms
--- 48.5 ticks/sec  (total 246)  BTC-USD=170  ETH-USD=36  SOL-USD=40 ---
```

## Layout

```
docker-compose.yml          Redis + TimescaleDB, healthchecks, pinned image versions
db/init/001_schema.sql      ticks hypertable — runs on first container start only
marketdata/config.py        env-driven settings, single source of truth
marketdata/feed_smoke.py    connects to Coinbase, prints live trades
scripts/verify_infra.py     pass/fail check that both datastores are usable
```

## Notes for anyone running this

**Postgres is on host port 5434**, not 5432 — ports 5432/5433 were already taken on the
development machine. Inside the Docker network it is still 5432.

**Editing the schema requires destroying the volume.** Postgres runs `db/init/*.sql` only when
its data directory is empty:

```powershell
docker compose down -v && docker compose up -d
```

**Clock accuracy affects the `lag` column.** It is computed as *our clock − exchange clock*,
so it measures network latency only if both clocks are synced. On the development machine the
Windows Time service was stopped and the clock had drifted ~330 ms behind, producing
*negative* lag readings. The smoke test now detects this and tells you. To fix, in an
**Administrator** terminal:

```powershell
net start w32time
w32tm /resync
w32tm /stripchart /computer:time.windows.com /samples:3 /dataonly   # verify: offset near 0
```

This is not cosmetic. Clock discipline is a regulated requirement in real trading systems —
EU MiFID II RTS 25 requires high-frequency firms to hold clocks within 100 microseconds of
UTC, so that events from different venues can be ordered against each other after the fact.

## Not yet built

Steps 3–11 of `plan.md`: writing ticks to Redis (cache, Streams, Pub/Sub) and TimescaleDB,
the FastAPI REST and WebSocket endpoints, the Locust benchmark, and app containerization.

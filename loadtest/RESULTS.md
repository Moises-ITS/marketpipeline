# Benchmark results

Build-order steps 8 and 9: measure, find the bottleneck, fix it, measure again.

Every number here came out of `loadtest/run_benchmark.ps1`; the raw CSVs are in
`loadtest/results/`. Two of the results contradicted what the plan expected, and both are
written up as they happened rather than quietly dropped — the point of measuring is to find
out you were wrong about something.

## Test bed

| | |
|---|---|
| Machine | AMD Ryzen 5 3600, 6 cores / 12 threads, 32 GB RAM, Windows 10 |
| Datastores | Redis 7 and TimescaleDB 2.18 in Docker Desktop |
| API | uvicorn, 1 worker unless stated, running on the host |
| Load generator | Locust 2.46, 1 master + 4 worker processes |
| Live data | ingest worker connected to Coinbase throughout, ~5–20 ticks/sec |
| Workload mix | 3 : 1 : 1 across `/prices`, `/prices/../history?window=30s&limit=100`, `/health` |

Duration is 60s per phase, 100 concurrent users, unless stated otherwise.

---

## 0. The first run measured the wrong thing

The very first benchmark ran Locust as a single process. It reported the hot path at 24 ms p50
and the *TimescaleDB-backed* history endpoint at 22 ms — faster than a Redis `HGETALL`, which
is not physically possible. Locust had also printed:

```
CPU usage above 90%! This may constrain your throughput and may even give inconsistent
response time measurements
```

Locust is single-threaded per process, so one process saturates a core at roughly 1,300 req/s
and starts measuring its own scheduling delay. **The load generator was the bottleneck, not the
API.** Every number below therefore comes from a master plus four load-generator processes, and
the impossible ordering disappeared once the client had room to breathe.

The lesson is the reason step 8 exists at all: an unvalidated benchmark is worse than no
benchmark, because it looks like evidence.

---

## 1. The planned optimization: move `/history` from TimescaleDB to Redis Streams

plan.md step 9 expected this to be the win. It was not — and the reason turned out to be
more interesting than the win would have been.

100 users, 1 API worker. "before" = `HISTORY_FORCE_DB=true`, "after" = the Streams path:

| Endpoint | Tier | p50 | p95 | p99 | req/s |
|---|---|---|---|---|---|
| `/prices/[symbol]` | Redis hash | 38 ms | 75 ms | 130 ms | 824 |
| `/prices/[symbol]/history` | **TimescaleDB** | **8 ms** | 17 ms | 35 ms | 273 |
| `/health` | both | 41 ms | 79 ms | 140 ms | 277 |
| Aggregated | | 35 ms | 71 ms | 130 ms | **1,375** |

| Endpoint | Tier | p50 | p95 | p99 | req/s |
|---|---|---|---|---|---|
| `/prices/[symbol]` | Redis hash | 26 ms | 48 ms | 77 ms | 850 |
| `/prices/[symbol]/history` | **Redis Streams** | **51 ms** | 88 ms | 150 ms | 282 |
| `/health` | both | 29 ms | 51 ms | 83 ms | 279 |
| Aggregated | | 29 ms | 66 ms | 100 ms | **1,412** |

Reading only the `/history` row, the "optimization" made that endpoint **6× slower**. But the
hot path got 32% *faster* in the same run, and total throughput went slightly up. The workload
did not get slower — the latency moved between endpoints.

### Was it real, or was it drift?

Market volume changes minute to minute, so the phases were re-run in the opposite order
(`run_benchmark.ps1 -Reverse`). The pattern held exactly, which rules out time-of-day drift:

| Phase (reversed order) | `/prices` p50 | `/history` p50 | `/health` p50 | Aggregate req/s |
|---|---|---|---|---|
| Redis Streams (ran first) | 24 ms | 46 ms | 27 ms | 1,511 |
| TimescaleDB (ran second) | 35 ms | 8 ms | 38 ms | 1,488 |

### So which tier is actually faster?

Two further measurements, because the mixed workload could not answer it.

**Each endpoint alone** (`-Tags history`, 50 users — no competing traffic):

| Tier | p50 | p95 | p99 | req/s |
|---|---|---|---|---|
| TimescaleDB | 13 ms | 27 ms | 45 ms | 977 |
| Redis Streams | 16 ms | 29 ms | 47 ms | 927 |

Within 3 ms of each other, and both plateau near 950 req/s — the same ceiling, which is the
first clue that neither datastore is the limiting factor.

**Sequentially, with no concurrency at all** (`loadtest/microbench.py`, 300 calls each,
no HTTP layer in the way):

| Operation | p50 | p99 |
|---|---|---|
| Redis `HGETALL` (hot path, 1 record) | **0.47 ms** | 0.89 ms |
| TimescaleDB latest row (1 record) | 0.98 ms | 2.17 ms |
| Redis `XREVRANGE` (100 records) | **1.13 ms** | 1.60 ms |
| TimescaleDB range query (100 records) | 1.46 ms | 2.09 ms |
| Building 100 response models from Redis rows (CPU only) | 0.11 ms | 0.19 ms |
| Building 100 response models from asyncpg rows (CPU only) | 0.10 ms | 0.12 ms |

Uncontended, **Redis is faster on both paths** — 2× on the single-record read. The Python-side
decoding cost is effectively identical between tiers, which rules out the first hypothesis
(that Redis returning untyped strings made it more expensive to deserialize).

### Conclusion, including the part that is still open

- Both tiers sustain the same throughput, and the ~950 req/s ceiling for a single endpoint is
  the same regardless of which one answers. **At this concurrency the bottleneck is the single
  API process, not Redis and not TimescaleDB.** That is what section 2 acts on.
- Uncontended, Redis is the faster tier at both sizes, so the Streams path stays the default:
  it wins on the metric the plan actually cares about (the hot path, which got 32% faster) and
  it keeps a high-rate read off the durable store.
- **What is not explained:** why a saturated event loop redistributes latency so sharply
  between endpoints depending on which tier `/history` uses. The effect is reproducible and
  order-independent, throughput is unchanged, and it does not show up in isolation — so it is a
  queueing effect inside one saturated process, not a property of either datastore. Pinning
  down the mechanism would need per-request server-side tracing, which is out of scope here.
  Saying "unexplained" is more useful than inventing a mechanism that fits.

---

## 2. The optimization that actually worked: scale the API process

The measurement above identified the real bottleneck — one Python process pinned at 100% of one
core, with every latency dominated by queueing behind it. The fix follows directly:
`uvicorn --workers 4`.

This works only because the API is **stateless**: every piece of state lives in Redis or
TimescaleDB, so four processes are interchangeable. That property was a design decision from
plan.md, and this is where it pays off.

100 users, identical workload, 1 worker vs 4:

| | 1 worker | 4 workers | Change |
|---|---|---|---|
| **Aggregate throughput** | 1,511 req/s | **2,351 req/s** | **+56%** |
| `/prices/[symbol]` p50 | 24 ms | **5 ms** | **−79%** |
| `/prices/[symbol]` p95 | 38 ms | 12 ms | −68% |
| `/prices/[symbol]` p99 | 56 ms | 19 ms | −66% |
| `/history` p50 | 46 ms | 7 ms | −85% |
| `/health` p50 | 27 ms | 8 ms | −70% |
| Aggregate p99 | 74 ms | 23 ms | −69% |

Throughput rose 56%, not 300%, because four API workers now contend for the same six physical
cores as four load-generator processes, Redis, TimescaleDB and Docker itself. On dedicated
hardware the gap would be wider; on a laptop this is the honest number.

---

## 3. Does it hit the plan's target?

plan.md's success criterion: **hot-path `GET /prices/{symbol}` under ~5 ms p50 in the load
test.**

| Configuration | `/prices` p50 | p95 | p99 | Aggregate req/s |
|---|---|---|---|---|
| 20 users, 1 worker | **3 ms** | 5 ms | 7 ms | 494 |
| 100 users, 4 workers | **5 ms** | 12 ms | 19 ms | 2,351 |
| 100 users, 1 worker | 24 ms | 38 ms | 56 ms | 1,511 |

**Met, and worth being precise about what that means.** At 20 concurrent users a single worker
answers in 3 ms p50 — that is close to the service time, since `microbench.py` measures the
Redis round trip itself at 0.47 ms and the rest is HTTP parsing, validation and serialization.
At 100 users the same single worker reports 24 ms, which is not the endpoint being slow; it is
100 requests queueing for one core. Four workers restore 5 ms p50 while carrying 2,351 req/s.

A "p50 latency" number is meaningless without the concurrency it was measured at, which is why
all three rows are here instead of just the flattering one.

---

## 4. Pushing throughput: 1,836 → 4,187 req/s

Re-measured on 2026-09-25, one change at a time, each compared with a baseline taken the same
day rather than the numbers above (the same 4-worker setup as section 2 measured 1,836 req/s
that day, against 2,351 the week before — which is why no step is compared across days).

| Step | Change | Users | Aggregate req/s | Avg | p50 | p99 |
|---|---|---|---|---|---|---|
| 0 | Baseline: section 2 setup, 4 workers on the Windows host | 100 | 1,836 | 9.8 ms | 6 ms | 39 ms |
| 1a | Locust `FastHttpUser` instead of `HttpUser` | 100 | 2,479 | 7.5 ms | 5 ms | 25 ms |
| 1b | 200 users | 200 | 2,858 | 37.0 ms | 31 ms | 100 ms |
| 2 | API in its Linux container (uvloop + httptools), no access log | 200 | 3,659 | 22.3 ms | 19 ms | 72 ms |
| 3 | 6 API workers | 200 | 4,104 | 16.5 ms | 12 ms | 76 ms |
| 4 | Serialize each response once (`as_json` in `api.py`) | 200 | 4,107 | 16.5 ms | 13 ms | 67 ms |
| 5 | 8 API workers | 200 | **4,187** | **15.5 ms** | 12 ms | **64 ms** |

Raw CSVs: `loadtest/results/s*_after_stats.csv`. Steps 2 onward use
`run_benchmark.ps1 -UseRunningApi` against `docker compose up -d`.

What each step showed:

- **1a — the client was a bottleneck again.** `HttpUser` is built on `requests`, which is slow
  per request. Swapping the client alone raised throughput 35% with the API unchanged.
- **1b — 100 users could not ask for more.** By Little's law, 100 users with a ~30 ms average
  think time can offer at most ~2,600 req/s however fast the API is. Doubling users removed
  that ceiling and exposed the API as the limit again: p99 jumped to 100 ms.
- **2 — Windows was costing ~28%.** uvicorn's fast event loop (uvloop) and HTTP parser
  (httptools) do not exist on Windows, so the host-run API never had them. The container does.
- **3–5 — the rest is CPU contention.** The p99 tail is spread evenly across all three
  endpoints rather than owned by one slow query, which is the signature of requests queueing
  for a core. Serializing once removed a redundant validation pass (p99 76 → 67 ms); more
  workers helped only marginally, because the API, four Locust processes, Redis, TimescaleDB
  and Docker all share six physical cores.

**At the same 200-user load, steps 2–5 cut average response time 58% (37.0 → 15.5 ms), cut p99
36% (100 → 64 ms) and raised throughput 47%.**

### Where does p99 cross 50 ms?

Same setup as step 5 (8 workers, Linux container), fewer users, repeated because single runs
turned out to vary by several hundred req/s and 20+ ms of p99:

| Users | Run | Aggregate req/s | Avg | p50 | p99 |
|---|---|---|---|---|---|
| 160 | 1 | 3,997 | 8.4 ms | 6 ms | 46 ms |
| 160 | 2 | 3,850 | 9.9 ms | 7 ms | 49 ms |
| 160 | 3 | 3,864 | 9.8 ms | 7 ms | 51 ms |
| 180 | 1 | 4,346 | 9.7 ms | 7 ms | 47 ms |
| 180 | 2 | 3,888 | 14.2 ms | 10 ms | 70 ms |

**160 users is the reproducible operating point: 3,850–3,997 req/s at a 46–51 ms p99** across
three runs. 180 users produced the best single run and also one of the
worst, which is why neither 180-user number is the headline — a single lucky run is not a
result.

**Not reached: a sub-50 ms p99 at 4,000 req/s on this machine.** A fixed-arrival-rate run
(Locust `constant_throughput`, exactly 4,000 req/s offered) gave the same ~65 ms p99, so it is
not an artefact of the closed-loop test. The remaining levers are moving the load generator to
a second machine, or an in-process price cache that takes Redis off the hot path.

---

## Reproducing this

```powershell
docker compose up -d redis timescaledb
.venv\Scripts\python.exe -m marketdata.ingest      # in a second terminal, leave it running

.\loadtest\run_benchmark.ps1                       # section 1: the tier comparison
.\loadtest\run_benchmark.ps1 -Reverse -Label rep   # section 1: the drift check
.\loadtest\run_benchmark.ps1 -Tags history -Users 50 -Duration 30s -Label iso
.\loadtest\run_benchmark.ps1 -Phases after -Label workers4 -UvicornWorkers 4
.\loadtest\run_benchmark.ps1 -Phases after -Label light -Users 20 -SpawnRate 10
.venv\Scripts\python.exe loadtest\microbench.py
```

Absolute numbers will differ on other hardware. The relationships — Redis faster uncontended,
both tiers capped by the API process, multi-worker fixing the queueing — should not.

## What is not measured here

- **The WebSocket endpoint.** Locust measures request/response; a push stream needs a different
  harness (message-arrival latency and fan-out cost per connected client, not req/s). The live
  path is verified functionally instead, by `scripts/watch_stream.py` and by
  `tests/test_api.py::test_websocket_receives_a_published_tick`.
- **Ingestion throughput at scale.** Coinbase delivers 5–20 ticks/sec for three symbols, which
  never came close to stressing the writer — the queue never exceeded 0 and not a single tick
  was shed across any run. Finding the ingest ceiling would need a synthetic feed generator.
- **Cold-cache behaviour under load.** Rehydration is tested functionally, not benchmarked.

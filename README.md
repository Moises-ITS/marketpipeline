# Real-Time Market Data Pipeline + Low-Latency API

Ingests a live crypto trade feed, stores it across latency-appropriate tiers, and serves it
through an async API — REST for point-in-time and historical reads, WebSocket for live push.

Design rationale, tradeoffs and build order: [`plan.md`](plan.md).
Benchmark methodology and numbers: [`loadtest/RESULTS.md`](loadtest/RESULTS.md).

> **Status: complete.** All eleven build-order steps are done. 54 tests pass, the benchmark is
> documented including the two results that contradicted the plan, and the whole system runs
> from one command.

---

## What it does

```
                    [Coinbase WebSocket feed - live trades]
                                    │
                                    ▼
              [Ingest worker - asyncio, its own process]
                                    │
        ┌───────────────────┬───────┴────────┬─────────────────────────┐
        ▼                   ▼                ▼                         ▼
  HSET latest:{sym}   XADD stream:{sym}  PUBLISH channel:{sym}   queue -> batched COPY
  (hot cache)         (capped log)       (live fan-out)          (durable history)
        │                   │                │                         │
     [Redis] ──────────────────────────────────┘                  [TimescaleDB]
        │                   │                │                         │
        └───────────────────┴────────┬───────┴─────────────────────────┘
                                     ▼
              [FastAPI - fully async, pooled connections]
                                     │
        GET /prices/{symbol} ────────┤  Redis hash        ~0.5 ms, 3 ms p50 under load
        GET /prices/{symbol}/history ┤  Streams, else DB   tier chosen by the data
        WS  /stream/{symbol} ────────┤  Redis Pub/Sub      pushed the instant a tick lands
        GET /health ─────────────────┘  both datastores
```

Two processes, on purpose. Ingestion and serving fail differently: a burst of API traffic must
not delay a tick, and a stalled database flush must not make health checks time out.

## Stack, and why each piece

| Layer | Choice | Why | Tradeoff accepted |
|---|---|---|---|
| Feed | Coinbase WebSocket (`matches`) | Push-based and unauthenticated. Binance returns HTTP 451 from US IPs. | Reconnects and backoff are ours to handle |
| Hot cache | Redis hash | In-memory, no query planning — the fastest path in the system | Volatile; rebuilt from TimescaleDB at API startup |
| Recent history | Redis Streams | Ordered append-only log with consumer groups — Kafka's shape without Kafka's operational weight | Capped by `XTRIM`; older data falls back to the DB |
| Live fan-out | Redis Pub/Sub | One publish reaches every subscribed API instance; polling scales as N clients × interval | Fire-and-forget — a disconnected client misses messages, and catches up via `/history` |
| Durable history | TimescaleDB hypertable | Time-partitioned chunks keep range queries fast as the table grows | — |
| Writes to it | Batched `COPY` | Per-tick inserts pay WAL and index cost every time | A crash between flushes loses the buffer |
| API | FastAPI + uvicorn, fully async | One worker serves hundreds of concurrent requests instead of blocking on I/O | — |

## Quick start

Docker Desktop running, and that is all you need:

```powershell
docker compose up -d --build
```

Four containers: Redis, TimescaleDB, the ingest worker, and the API. Give it about thirty
seconds for live trades to arrive, then:

```powershell
curl http://localhost:8000/prices/BTC-USD
curl "http://localhost:8000/prices/BTC-USD/history?window=1m&limit=5"
start http://localhost:8000/docs
```

```json
{"symbol":"BTC-USD","price":78771.08,"size":0.00060192,"side":"sell",
 "trade_id":1086947117,"time":"2026-09-01T04:33:00.561683Z"}
```

To watch the live push path (Coinbase → Redis → API → your terminal):

```powershell
.venv\Scripts\python.exe scripts\watch_stream.py BTC-USD
```

```
04:34:12.987  BTC-USD   sell    78,770.03  size 0.00488575     [107 msgs, 3.9/s]
04:34:13.164  BTC-USD   buy     78,770.02  size 0.00000012     [109 msgs, 3.9/s]
```

## Development setup

For day-to-day work, run the datastores in Docker and the Python on your machine, so edits
take effect without a rebuild:

```powershell
docker compose up -d redis timescaledb
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
Copy-Item .env.example .env

.venv\Scripts\python.exe scripts\verify_infra.py        # prove the foundation works first
.venv\Scripts\python.exe -m marketdata.ingest           # terminal 1
.venv\Scripts\python.exe -m uvicorn marketdata.api:app --port 8000   # terminal 2
```

## API

| Endpoint | Reads from | Notes |
|---|---|---|
| `GET /prices/{symbol}` | Redis hash | The hot path. One `HGETALL`, nothing else. |
| `GET /prices/{symbol}/history?window=1m&limit=500` | Streams, falling back to TimescaleDB | `window` accepts `30s`, `5m`, `2h`. The response's `source` field names the tier that answered. |
| `GET /candles/{symbol}?interval=1m&window=1h&limit=500` | TimescaleDB continuous aggregate | OHLCV bars. `interval` is one of `1m`, `5m`, `15m`, `1h`, `1d`. |
| `WS /stream/{symbol}` | Redis Pub/Sub | Pushes every tick for that symbol. One Redis subscription per symbol, fanned out to all connected clients. |
| `GET /health` | both | Reports a round-trip time for each datastore. |

Unknown symbols are rejected with a 404 rather than reaching Redis — otherwise a WebSocket
client could subscribe to a channel nobody publishes to and hang forever with no error.

Candles are not computed per request. `candles_1m` is a TimescaleDB continuous aggregate:
Timescale keeps the one-minute buckets materialized and refreshes only the ones whose
underlying ticks changed, so a read touches one precomputed row per minute instead of every
trade inside it. Wider intervals roll up from those same buckets — `open` from the first
minute, `close` from the last — which is why one materialized view answers all five intervals.

Measured on 1.84M ticks:

| Range | Aggregating raw `ticks` | Continuous aggregate |
|---|---|---|
| 1 hour | 9.5 ms | 0.40 ms |
| 24 hours | 112 ms | 0.78 ms |

The range matters more than the ratio: the raw query grew 12× between those two rows, the
aggregate grew 2×. Reproduce with the `EXPLAIN (ANALYZE, TIMING OFF)` pair in
[`phase2.md`](phase2.md).

Interactive docs: <http://localhost:8000/docs>

## Benchmark summary

Full methodology, including a failed measurement and an unexplained result, is in
[`loadtest/RESULTS.md`](loadtest/RESULTS.md). The headline:

| Configuration | `/prices` p50 | p95 | p99 | Aggregate req/s |
|---|---|---|---|---|
| 20 users, 1 API worker | **3 ms** | 5 ms | 7 ms | 494 |
| 100 users, 1 API worker | 24 ms | 38 ms | 56 ms | 1,511 |
| 100 users, 4 API workers | **5 ms** | 12 ms | 19 ms | **2,351** |

Three things the measurements changed:

1. **The first benchmark was invalid.** A single Locust process saturated a CPU core and
   reported the TimescaleDB endpoint as faster than a Redis `HGETALL` — impossible. The load
   generator was the bottleneck. Everything since runs a master plus four worker processes.
2. **The optimization the plan predicted did not pay off the way it expected.** Moving
   `/history` from TimescaleDB onto Redis Streams made that endpoint slower under a mixed
   workload while making the hot path 32% faster, at unchanged total throughput. Isolated and
   uncontended measurements show Redis is genuinely the faster tier (0.47 ms vs 0.98 ms on the
   hot path), so it stays the default — but the reason is now measured rather than assumed, and
   the part that is still unexplained is written down as unexplained.
3. **The real bottleneck was the API process itself.** Both tiers plateaued at the same
   ~950 req/s for a single endpoint, which pointed at the single Python process rather than at
   either datastore. `uvicorn --workers 4` — possible only because the API keeps no state of
   its own — lifted throughput 56% and cut hot-path p50 from 24 ms to 5 ms.

## Tests

```powershell
.venv\Scripts\python.exe -m pytest            # 54 tests
.venv\Scripts\python.exe -m pytest -m unit    # 26 of them need no Docker
```

Unit tests cover parsing, wire encoding and input validation — pure functions, so a failure
always means the code is wrong. Integration tests run against the real Redis and the real
TimescaleDB, covering the tier-selection logic, batched writes to the hypertable, cache
rehydration, and the WebSocket fan-out end to end. They skip rather than fail when the stack is
down, because "you forgot `docker compose up`" is not a bug.

## Layout

```
docker-compose.yml           Redis, TimescaleDB, ingest worker, API - healthchecks, pinned versions
Dockerfile                   One image, two roles; layer-cached deps, non-root user
db/init/001_schema.sql       ticks hypertable - runs on first container start only

marketdata/config.py         Env-driven settings, single source of truth
marketdata/feed.py           Coinbase client: normalization + reconnect with jittered backoff
marketdata/store.py          Redis layer - hot cache, capped stream, pub/sub, key namespace
marketdata/db.py             TimescaleDB layer - pool, batched COPY, range queries
marketdata/ingest.py         The worker: feed -> Redis inline, -> TimescaleDB in batches
marketdata/api.py            FastAPI - REST, WebSocket broadcaster, lifespan-managed pools
marketdata/feed_smoke.py     Step-2 diagnostic: prints live trades, stores nothing

tests/                       54 tests, unit and integration
loadtest/locustfile.py       The load profile
loadtest/run_benchmark.ps1   Reproduces every number in RESULTS.md
loadtest/microbench.py       Per-tier latency with no HTTP in the way
scripts/verify_infra.py      Pass/fail check that both datastores are usable
scripts/watch_stream.py      WebSocket client - watch the live push path work
```

## Considered and deliberately deferred

- **Kafka.** Redis Streams provides the same shape — an ordered log with consumer groups — at
  a fraction of the operational weight. At this data volume (5–20 ticks/sec across three
  symbols) a Kafka cluster would be infrastructure with nothing to do. The honest boundary:
  Streams live in one Redis process's memory, so multi-terabyte retention or cross-datacenter
  replication is where that argument stops working.
- **Multi-exchange aggregation.** Normalization is already isolated in `feed.py`, so a second
  exchange is one module plus a symbol-mapping decision, not a redesign.
- **Order book depth (L2).** Trade ticks only. L2 is a different problem — maintaining a
  synchronized book with sequence-gap recovery — and this project is about pipeline
  architecture.
- **Authentication.** Not the point of this project, and adding a token check would not make
  its design decisions any more interesting.
- **Deduplication of replayed ticks.** After a reconnect the feed can resend trades already
  stored. Harmless for charting, and the hypertable deliberately has no unique index, since one
  would cost time on every insert. `(trade_id, time)` is the index to add if that changes.

## Notes for anyone running this

**Postgres is on host port 5434**, not 5432 — ports 5432/5433 were already taken on the
development machine. Inside the Docker network it is still 5432, which is exactly why the
connection details live in the environment and not in the code.

**Editing the schema requires destroying the volume.** Postgres runs `db/init/*.sql` only when
its data directory is empty:

```powershell
docker compose down -v && docker compose up -d
```

**Clock accuracy affects the `lag` column** in `feed_smoke.py`. It is computed as *our clock −
exchange clock*, so it measures network latency only if both clocks are synced. On the
development machine the Windows Time service was stopped and the clock had drifted ~330 ms
behind, producing *negative* lag readings. The smoke test now detects this and says so. To fix,
in an **Administrator** terminal:

```powershell
net start w32time
w32tm /resync
w32tm /stripchart /computer:time.windows.com /samples:3 /dataonly   # verify: offset near 0
```

This is not cosmetic. Clock discipline is a regulated requirement in real trading systems — EU
MiFID II RTS 25 requires high-frequency firms to hold clocks within 100 microseconds of UTC, so
that events from different venues can be ordered against each other after the fact.

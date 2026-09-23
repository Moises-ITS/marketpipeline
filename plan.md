# Real-Time Market Data Pipeline + Low-Latency API — Project Plan

## Mission

Build a system that ingests a live market data feed, stores it across latency-appropriate tiers, and serves it through an API fast enough to prove — with real numbers — that the design decisions behind it were deliberate, not default. This is also a big learning lesson and preperation for Bloomberg in a couple months so I'll be prepared for their interview.

## Goal (what this proves on a resume/interview)

- **API design**: clean separation between hot-path reads (cache), historical reads (DB), and live push (WebSocket)
- **Caching strategy**: multiple cache tiers chosen for different access patterns, each with a stated tradeoff
- **Async programming**: nothing in the ingestion or read path blocks the event loop
- **Measure before optimizing**: a documented before/after benchmark, not a claimed one
- \*\*Clean, readable and condense code: important comments only and maintainability

## Scope

**In scope (v1):**

- Single exchange WebSocket feed (crypto — Binance or Coinbase, no auth friction) for N symbols (start with 3–5, e.g. BTC/USDT, ETH/USDT, SOL/USDT)
- Async ingestion worker as its own process
- Redis: hot cache (latest price), Streams (recent tick history), Pub/Sub (live fan-out)
- TimescaleDB (Postgres extension): durable tick history, batched writes
- FastAPI: async REST endpoints + WebSocket endpoint
- Locust-based load test with documented p50/p95/p99 latency, before and after one real optimization
- Docker Compose for one-command local run
- README with architecture diagram + benchmark numbers

**Explicitly out of scope (name these in the README as "considered, deferred"):**

- Kafka (Streams substitutes for it at this scale — mention why)
- Multi-exchange aggregation
- Order book depth / Level 2 data (trade-level ticks only)
- Authentication/authorization on the API (not the point of this project)
- Horizontal scaling across multiple API instances (stretch goal, see below)

**Stretch goals (only after v1 is solid):**

- Second FastAPI instance behind a load balancer, proving Pub/Sub fan-out works across instances, not just within one
- Simple React chart subscribing to the WebSocket stream
- Threshold alerting (e.g., notify on >X% price move in Y seconds)

---

Catch -> feed.py(holds live connection to Coinbase)
Catch -> ingest.py(Runs the catching, hands ticks to storage)
Keep -> store.py(Redis)
Keep -> db.py(TimescaleDB - slow permenant record)
Serve -> api.py(Answers questions over the web)
Settings -> config.py(Settings)

---
Redis is kept in RAM, 0.2 milliseconds, doesn't survive a restart and holds last few thousand ticks

TimescaleDB is kept in Disk, 10-15 milliseconds, does survive a restart and holds everything forever

## Architecture

```
[Binance/Coinbase WS feed]
        │
        ▼
[Async Ingestion Worker — asyncio + websockets]
        │
        ├──► Redis: HSET latest:{symbol}         (hot cache — current price)
        ├──► Redis: XADD stream:{symbol}          (recent history, capped)
        ├──► Redis: PUBLISH channel:{symbol}      (live fan-out trigger)
        └──► Buffer → batched flush ──► TimescaleDB (durable tick history)
                                              ▲
                                              │
[FastAPI — fully async, redis.asyncio client, connection pool]
        ├── GET  /prices/{symbol}            → Redis hot cache
        ├── GET  /prices/{symbol}/history     → Redis Streams (recent) or TimescaleDB (older)
        └── WS   /stream/{symbol}             → subscribes to Redis Pub/Sub, pushes to client
```

---

## Component plan: feature, why this tool, and the tradeoff you're accepting

### 1. Data source — Exchange WebSocket feed

- **Feature**: Persistent connection to Binance/Coinbase trade stream for chosen symbols.
- **Why**: Push-based data is genuinely real-time; polling can't match it without wasting requests.
- **Tradeoff accepted**: Must handle reconnects, backoff, and exchange-side connection resets — real operational complexity, not hidden away.
- **Build tasks**: connect, subscribe to symbols, parse messages, reconnect-with-backoff logic, basic message validation/normalization (symbol, price, size, timestamp).

### 2. Ingestion worker — `asyncio`, separate process

- **Feature**: Standalone process that owns the WS connection and writes to Redis/TimescaleDB.
- **Why async**: I/O-bound workload; a single event loop cheaply holds the connection and reacts to ticks without thread overhead or GIL contention.
- **Why a separate process from the API**: Isolates ingestion load from API load — a slow API request can't delay processing of an incoming tick, and vice versa.
- **Tradeoff accepted**: Debugging async stack traces is harder; a single accidental blocking call anywhere stalls the whole loop — requires discipline.
- **Build tasks**: main event loop, write-to-Redis logic, write-buffer for batched TimescaleDB flush, graceful shutdown handling.

### 3. Hot cache — Redis `HSET`/`SET` (latest price)

- **Feature**: `GET /prices/{symbol}` always reads from here — target sub-5ms.
- **Why**: In-memory read avoids disk I/O and query planning entirely; this is the path that needs to be fastest.
- **Tradeoff accepted**: Volatile by default — a Redis restart loses it. Mitigation: rehydrate from TimescaleDB's latest row on startup, and say so explicitly in the README as a deliberate durability-vs-speed choice.
- **Build tasks**: write-on-tick, read endpoint, startup rehydration script.

### 4. Recent history — Redis Streams (over Sorted Sets, over querying TimescaleDB directly)

- **Feature**: `GET /prices/{symbol}/history?window=1m` reads from Streams for anything recent.
- **Why Streams over Sorted Sets**: Purpose-built append-only log with consumer-group semantics — multiple independent readers (the WebSocket broadcaster now, an analytics service later) can consume the same stream without coordinating manually, unlike repurposing a Sorted Set.
- **Why not Kafka**: Streams gets ~80% of the pattern (ordered log, consumer groups) without the operational weight of running a Kafka cluster for a solo project's data volume — a scope-appropriate tradeoff, worth stating out loud in an interview.
- **Tradeoff accepted**: Streams are capped (`XTRIM`) to control memory — old data ages out and falls back to TimescaleDB for anything beyond the window.
- **Build tasks**: `XADD` on tick, `XTRIM` to cap size, `XRANGE` read endpoint, consumer group for the broadcaster.

### 5. Live fan-out — Redis Pub/Sub → WebSocket broadcast

- **Feature**: `WS /stream/{symbol}` pushes live updates to connected clients the instant a tick arrives.
- **Why Pub/Sub over client polling**: One publish reaches every subscribed API instance instantly; polling scales badly as N clients × polling interval = redundant Redis load.
- **Tradeoff accepted**: Pub/Sub is fire-and-forget — a briefly disconnected client misses messages with no replay. Acceptable because Streams (above) is the durable catch-up source; Pub/Sub is purely for already-connected clients.
- **Build tasks**: subscribe-per-symbol logic in the API, WebSocket connection manager, broadcast-to-all-connected-clients-for-a-symbol.

### 6. Durable history — TimescaleDB, batched writes

- **Feature**: Full tick history, queryable by time range for anything beyond the Streams window.
- **Why TimescaleDB over vanilla Postgres**: Automatic time-partitioning (hypertables) makes range queries over time-series data much faster — the actually-correct tool for tick history, and a stronger resume line than plain Postgres.
- **Why batched, not per-tick, writes**: Per-tick inserts carry WAL + index overhead each time; buffering in Redis and flushing every N seconds or N ticks amortizes that cost.
- **Tradeoff accepted**: Small durability gap — a crash between flushes loses the unflushed buffer. Reasonable for market history (not trade execution) and worth stating explicitly rather than glossing over.
- **Build tasks**: hypertable schema, batched insert function, flush trigger (time- or size-based), historical range query endpoint.

### 7. API layer — FastAPI, fully async, `redis.asyncio`, connection pooling

- **Feature**: All endpoints above, plus health check.
- **Why fully async**: A sync endpoint blocks its worker thread on the Redis call; async yields during I/O wait, so one worker handles far more concurrent requests — directly measurable in your benchmark.
- **Why connection pooling**: A new Redis connection per request adds real, avoidable latency (handshake + auth); pooling amortizes that to near-zero — a one-line change with a visible before/after in your numbers.
- **Tradeoff accepted**: None significant here — this is close to a strict win over the sync alternative, which is itself worth noting (not every design decision in the project needs an opposing cost).
- **Build tasks**: endpoint definitions, Redis pool setup, TimescaleDB async driver setup, error handling, health/status endpoint.

### 8. Load testing — Locust

- **Feature**: Documented p50/p95/p99 latency and throughput, before and after one real optimization (e.g., adding the connection pool, or moving a read off TimescaleDB onto Redis).
- **Why Locust over eyeballing `curl -w`**: A single request tells you nothing about queueing effects or tail latency under concurrency — where real systems actually break. Locust gives a shareable report/screenshot for the README.
- **Tradeoff accepted**: Extra setup time versus a quick manual check — worth it because this is the evidence, not a nice-to-have.
- **Build tasks**: Locust file simulating concurrent reads across endpoints, baseline run, optimization, re-run, README table of results.

---

## Build order (dependency-driven, not arbitrary)

1. TimescaleDB schema + Redis running locally (Docker Compose) — infra first, nothing to build on top of otherwise
2. Ingestion worker: connect to WS feed, log ticks to console — prove the data flows before storing anything
3. Wire ingestion to Redis hot cache + Streams
4. Wire ingestion's batched buffer to TimescaleDB
5. FastAPI: `GET /prices/{symbol}` (hot cache) — first working endpoint, easiest to verify
6. FastAPI: `GET /prices/{symbol}/history` (Streams, then TimescaleDB fallback)
7. FastAPI: `WS /stream/{symbol}` (Pub/Sub fan-out)
8. Locust baseline benchmark
9. Pick one bottleneck from the baseline, fix it, re-benchmark
10. README: architecture diagram, setup instructions, before/after numbers, "considered but deferred" section
11. Docker Compose polish so a reviewer can run it in one command

## Success criteria

- Hot-path read (`GET /prices/{symbol}`) under ~5ms p50 in the load test
- A documented, real before/after latency improvement from Phase 9 — the actual number matters less than that it's real and explained
- A reviewer can clone the repo, run one command, and see live prices updating within a minute

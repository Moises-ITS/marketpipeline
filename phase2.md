# Phase 2 — Plan

v1 is complete and measured: the hot path is 3 ms p50, `/history` moved to Redis Streams for a
32% gain, and scaling the API process exposed the real bottleneck at 100 users. See
`loadtest/RESULTS.md`.

Phase 2 keeps the same standard v1 set for itself: **every claim gets a number, and anything
unproven is labelled unproven.** Each item below was checked against this machine before being
written down — nothing here needs a paid tier, a cloud account, or a Linux host.

## Feasibility, verified not assumed

| Item | Free | Verified how |
|---|---|---|
| Continuous aggregates | TSL community edition, self-hosted | Created a probe cagg on `ticks`, then dropped it |
| Hypertable compression | Same | `ALTER TABLE ticks SET (timescaledb.compress...)` accepted, then reverted |
| `orjson` | MIT | `pip install --dry-run` resolved a `cp311-win_amd64` wheel |
| nginx | BSD, official image | Standard Docker Hub image, no auth |
| Pattern subscribe | Already have `redis-py` | No new dependency |

### Already done, do not re-claim

`uvloop 0.22.1` is **already installed and active** in the API container — `uvicorn[standard]`
pulled it in during v1. Swapping the event loop is the most-cited async speed tip and it was
banked before the first benchmark ran. Listing it as a Phase 2 optimization would be taking
credit for a number v1 already earned.

(It is absent on the Windows host, where uvloop has no build. Local dev runs the stock asyncio
loop; the container does not. Worth knowing before comparing a local run to a container run.)

---

## 1. OHLCV candles via continuous aggregates — DONE

**Result, measured on 1.84M ticks:**

| Range | Aggregating raw `ticks` | `candles_1m` |
|---|---|---|
| 1 hour | 9.5 ms | 0.40 ms |
| 24 hours | 112 ms (411 ms cold) | 0.78 ms |

The ratio is the less interesting number. Across those two rows the raw query grew 12× and the
aggregate grew 2×, which is the actual property being bought: read cost tracks the number of
buckets returned, not the number of trades underneath them.

Correctness was checked by aggregating one closed 5-minute window both ways in a single query —
rollup from `candles_1m` versus direct from `ticks`. Open, high, low, close, volume and trade
count matched exactly. Reproduce both with the queries in `db/init/002_candles.sql`'s header and
the tests in `tests/test_candles.py`.

**One correction to the plan as first written:** the tradeoff below was going to be "the newest
candle lags by the refresh interval." Declaring the view `materialized_only = false` removes
that — reads UNION the materialized buckets with a live aggregation over the unmaterialized
tail, so the in-progress minute is present and correct. The real cost is that each read also
scans that tail, which `end_offset` keeps to about a minute of ticks.

- **Feature**: `GET /candles/{symbol}?interval=1m` returns open/high/low/close/volume bars.
- **Why this is the flagship**: candles are what market data is actually *consumed* as — every
  chart, every moving average, every volume bar is computed from ticks. Serving raw ticks and
  making the client aggregate them is pushing the work downstream.
- **Why a continuous aggregate and not a `GROUP BY` on read**: Timescale keeps the buckets
  materialized and refreshes only the ones whose underlying data changed. A read touches
  precomputed rows instead of scanning every tick in the range. The cost of aggregation moves
  off the read path, which is the same move v1 made with batched writes.
- **Tradeoff accepted**: every read also scans the unmaterialized tail (about one minute of
  ticks) to keep the in-progress candle live. Cheap, and bounded by `end_offset`.
- **Build tasks**: ~~`002_candles.sql` with the cagg + refresh policy, `fetch_candles` in
  `db.py`, the endpoint, tests, and a benchmark against the equivalent raw-tick `GROUP BY`.~~
  All done. 9 tests in `tests/test_candles.py`; suite is 63 passing.

## 2. Compression policy on old chunks

- **Feature**: compress chunks older than a threshold, automatically.
- **Why**: tick data is written once and never updated, which is the exact shape columnar
  compression is good at. Smaller chunks also mean fewer pages read per range scan.
- **Tradeoff accepted**: compressed chunks are expensive to modify. Irrelevant here — nothing
  ever updates a historical trade — but it would matter in a system that backfills or corrects.
- **Build tasks**: compression settings + policy in SQL, and a before/after of both on-disk size
  and range-query time. The size number is the honest one; the speed number may be small at this
  data volume and should be reported as whatever it is.

## 3. `orjson` for response serialization

- **Feature**: swap FastAPI's default JSON encoder for `ORJSONResponse`.
- **Why**: `/history` can return up to 5,000 rows. Encoding that with the standard library is
  real CPU on the response path, and `orjson` is a compiled encoder with the same output.
- **Tradeoff accepted**: one more dependency, and a compiled one — it needs a wheel for the
  target platform. Verified present for this machine and for the container's Linux/amd64.
- **Expected shape of the win**: negligible on `/prices` (one small object), visible on large
  `/history` responses. Report both, including the one that does not move.
- **Build tasks**: set the response class, re-run the existing Locust profile, add the numbers.

## 4. Multi-symbol batch read

- **Feature**: `GET /prices?symbols=BTC-USD,ETH-USD,SOL-USD` — one request, N symbols.
- **Why**: a dashboard showing three prices currently issues three HTTP requests, each with its
  own round trip and its own pooled connection checkout. One pipelined Redis call answers all of
  them.
- **Tradeoff accepted**: a partial failure is now ambiguous — one missing symbol should not fail
  the whole response, so the shape has to say which symbols were found.
- **Build tasks**: endpoint, pipelined `HGETALL` in `store.py`, tests for the partial case, and a
  latency comparison against N sequential calls.

## 5. Pattern subscribe in the broadcaster

- **Feature**: one `PSUBSCRIBE channel:*` per API process instead of one `SUBSCRIBE` per symbol.
- **Why**: today the broadcaster holds one pooled Redis connection per *watched* symbol, and
  those connections are long-lived, so they permanently subtract from the pool that request
  traffic shares. At roughly 30-40 concurrently streamed symbols the pool starts refusing
  connections — and the failure surfaces on `/prices`, not on the WebSocket that caused it.
- **Why it is not urgent**: with three symbols this is nowhere near binding. It is a scaling
  ceiling worth removing before adding symbols, not a present defect.
- **Tradeoff accepted**: the process now receives every symbol's messages and routes by channel
  name, so it does a little work for symbols nobody is watching. Cheap next to a connection each.
- **Build tasks**: rework `Broadcaster` to one pump, route by channel, keep the per-symbol client
  set, and add a test that two symbols fan out correctly from a single subscription.

## 6. Horizontal scale behind nginx — the unproven claim

- **Feature**: two API containers behind nginx, both serving REST and WebSocket.
- **Why this one matters most**: `plan.md` section 5 asserts *"one publish reaches every
  subscribed API instance instantly."* **That has never been demonstrated.** The broadcaster has
  only ever run in a single process. It is currently the one architectural claim in this project
  that is asserted rather than measured — which is exactly what the mission statement says not to
  do.
- **Tradeoff accepted**: nginx needs WebSocket upgrade headers configured, and the benchmark gets
  harder to attribute with two processes behind a proxy.
- **Build tasks**: nginx config with `proxy_set_header Upgrade`, a second API service, two
  `watch_stream.py` clients pinned to different instances, and evidence that a single Coinbase
  tick reaches both.

---

## Build order

1. **Candles** (item 1) — largest feature gain, and self-contained
2. **Compression** (item 2) — same SQL layer, natural follow-on
3. **orjson** (item 3) — one-line change, benchmark it honestly
4. **Batch read** (item 4) — pure application code
5. **Pattern subscribe** (item 5) — removes the pool ceiling before symbols grow
6. **nginx fan-out** (item 6) — closes the unproven claim

## Success criteria

- `/candles` answers faster than the equivalent raw-tick `GROUP BY`, with both numbers published
- Compression's on-disk saving reported as measured, whatever it is
- A single tick observed arriving at two independent API instances
- Every Phase 2 number reproducible by the commands written next to it, as in `RESULTS.md`

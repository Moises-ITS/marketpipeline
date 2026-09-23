"""Where does the time actually go? Times each tier directly, with no HTTP in the way.

The load test in locustfile.py measures the system as a user experiences it. That is the number
that matters, but when it says something surprising it cannot say WHY. This does: it calls the
storage layer in a loop and separates the network round trip from the Python-side decoding, so
a claim about the cause is measured rather than assumed.

    .venv\\Scripts\\python.exe loadtest\\microbench.py

Needs the ingest worker to have been running, so there is real data in both tiers.
"""

import asyncio
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from marketdata import db, store
from marketdata.api import Tick

SYMBOL = "BTC-USD"
HISTORY_LIMIT = 100
ROUNDS = 300


async def measure(label: str, call, rounds: int = ROUNDS) -> None:
    """Run `call` repeatedly and report the distribution, not just the average.

    An average hides the tail, and the tail is where systems actually fail. Reporting p50 and
    p99 side by side is the whole habit this project is trying to build.
    """
    await call()  # one warm-up: the first call pays for pool setup and cache misses
    samples = []
    for _ in range(rounds):
        start = time.perf_counter()
        await call()
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    print(
        f"  {label:<46} p50 {statistics.median(samples):7.3f}ms   "
        f"p99 {samples[int(len(samples) * 0.99)]:7.3f}ms   mean {statistics.fmean(samples):7.3f}ms"
    )


def measure_sync(label: str, call, rounds: int = ROUNDS) -> None:
    call()
    samples = []
    for _ in range(rounds):
        start = time.perf_counter()
        call()
        samples.append((time.perf_counter() - start) * 1000)
    samples.sort()
    print(
        f"  {label:<46} p50 {statistics.median(samples):7.3f}ms   "
        f"p99 {samples[int(len(samples) * 0.99)]:7.3f}ms   mean {statistics.fmean(samples):7.3f}ms"
    )


async def main() -> None:
    redis = store.make_client()
    pool = await db.make_pool()
    since = datetime.now(timezone.utc) - timedelta(seconds=30)
    since_ms = int(since.timestamp() * 1000)

    print(f"\nSequential, uncontended latency for {SYMBOL} ({ROUNDS} calls each)\n")

    print("HOT PATH - one record")
    await measure("Redis HGETALL  (store.get_latest)", lambda: store.get_latest(redis, SYMBOL))
    await measure(
        "TimescaleDB latest row (for comparison)",
        lambda: db.fetch_latest_per_symbol(pool, [SYMBOL]),
    )

    print(f"\nHISTORY - {HISTORY_LIMIT} records")
    await measure(
        "Redis XREVRANGE (store.read_recent)",
        lambda: store.read_recent(redis, SYMBOL, since_ms, HISTORY_LIMIT),
    )
    await measure(
        "TimescaleDB range query (db.fetch_history)",
        lambda: db.fetch_history(pool, SYMBOL, since, HISTORY_LIMIT),
    )

    # Now split the two history paths into "fetch" and "turn into response objects", because
    # that is the part the load test could not see.
    redis_rows = await store.read_recent(redis, SYMBOL, since_ms, HISTORY_LIMIT)
    pg_rows = await db.fetch_history(pool, SYMBOL, since, HISTORY_LIMIT)
    if not redis_rows or not pg_rows:
        print("\n(!) One of the tiers returned nothing - is the ingest worker running?")
    else:
        print(f"\nCPU ONLY - building {HISTORY_LIMIT} response models, no I/O")
        # This pair tests a specific hypothesis: Redis hands back strings, so every price, size
        # and timestamp must be parsed in Python, while asyncpg returns floats and datetimes
        # already decoded in C. That sounds like it should make the Redis path more expensive
        # to serve - and measuring it says the gap is about 0.02 ms per 100 records, which is
        # too small to explain anything. A plausible mechanism is not a measured one.
        measure_sync("from Redis rows (str -> float, str -> datetime)",
                     lambda: [Tick(**row) for row in redis_rows], rounds=200)
        measure_sync("from TimescaleDB rows (already native types)",
                     lambda: [Tick(**row) for row in pg_rows], rounds=200)

    await redis.aclose()
    await pool.close()
    print()


if __name__ == "__main__":
    asyncio.run(main())

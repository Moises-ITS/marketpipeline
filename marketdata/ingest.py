"""The ingestion worker: owns the feed, writes Redis on every tick, TimescaleDB in batches.

Run it:
    .venv\Scripts\python.exe -m marketdata.ingest

WHY THIS IS A SEPARATE PROCESS FROM THE API
Ingestion and serving fail differently. A burst of API traffic must not delay the processing of
an incoming tick, and a stalled database flush must not make health checks time out. Separate
processes mean separate event loops, separate failure domains, and each can be restarted alone.

THE SHAPE OF THE WORK
Three tasks share one event loop:
  1. reader  - feed -> Redis (hot cache, stream, pub/sub), then hands the tick to a queue
  2. flusher - drains the queue into TimescaleDB in batches
  3. reporter- prints throughput so you can see it working
The queue between 1 and 2 is what decouples them: a slow database slows the flusher, never the
reader, so live prices stay live even while durable writes are lagging.
"""

import asyncio
import contextlib
import logging
import signal
import time

from marketdata import db, store
from marketdata.config import settings
from marketdata.feed import stream_trades

log = logging.getLogger("ingest")

# Bounded on purpose. If TimescaleDB stalls, an unbounded queue would grow until the process
# runs out of memory and dies - taking the live Redis path down with it for no reason. Capping
# it means an unreachable database costs durable history (recoverable) instead of the whole
# worker (not). This is load shedding, and choosing what to shed is the actual design decision.
QUEUE_MAXSIZE = 50_000

REPORT_SECONDS = 5.0


class Stats:
    """Plain counters. Cheap to update on the hot path, printed by the reporter task."""

    def __init__(self) -> None:
        self.ticks = 0
        self.written = 0  # rows actually landed in TimescaleDB
        self.dropped = 0  # ticks shed because the queue was full
        self.batches = 0


async def reader(queue: asyncio.Queue, redis, stats: Stats) -> None:
    async for tick in stream_trades():
        # Redis first: it is the live path, and it is fast enough to stay inline here.
        await store.write_tick(redis, tick)
        stats.ticks += 1
        try:
            queue.put_nowait(tick)
        except asyncio.QueueFull:
            stats.dropped += 1
            if stats.dropped % 1000 == 1:  # log the first, then every thousandth
                log.warning("DB queue full - shedding ticks (%d dropped)", stats.dropped)


async def flusher(queue: asyncio.Queue, pool, stats: Stats) -> None:
    """Accumulate ticks and write them in batches, on whichever trigger fires first.

    The wait is `asyncio.wait_for(queue.get(), timeout=...)` with a shrinking deadline, so a
    half-full batch still gets written within pg_flush_seconds during a quiet market instead of
    sitting in memory waiting for rows that are not coming.
    """
    batch: list[dict] = []
    deadline = time.monotonic() + settings.pg_flush_seconds

    while True:
        timeout = max(0.0, deadline - time.monotonic())
        try:
            batch.append(await asyncio.wait_for(queue.get(), timeout=timeout))
        except asyncio.TimeoutError:
            pass  # time trigger fired; fall through and flush whatever we have

        if len(batch) >= settings.pg_batch_size or time.monotonic() >= deadline:
            await _flush(batch, pool, stats)
            batch = []
            deadline = time.monotonic() + settings.pg_flush_seconds


async def _flush(batch: list[dict], pool, stats: Stats) -> None:
    if not batch:
        return
    try:
        stats.written += await db.insert_ticks(pool, batch)
        stats.batches += 1
    except Exception:
        # A failed batch is lost, and that is the durability gap plan.md section 6 accepts.
        # Retrying here would stall the flusher and back the queue up behind a database that is
        # already unhappy; the live Redis path keeps serving regardless.
        log.exception("Batch of %d ticks failed to write", len(batch))


async def reporter(stats: Stats, queue: asyncio.Queue) -> None:
    last_ticks, last_written = 0, 0
    while True:
        await asyncio.sleep(REPORT_SECONDS)
        rate = (stats.ticks - last_ticks) / REPORT_SECONDS
        wrote = stats.written - last_written
        last_ticks, last_written = stats.ticks, stats.written
        log.info(
            "%.1f ticks/sec | redis %d | db %d (+%d, %d batches) | queued %d | dropped %d",
            rate, stats.ticks, stats.written, wrote, stats.batches, queue.qsize(), stats.dropped,
        )


async def run() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    redis = store.make_client()
    pool = await db.make_pool()
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    stats = Stats()
    stop = asyncio.Event()

    # SIGTERM is what `docker stop` sends. Handling it means a container shutdown drains the
    # queue like Ctrl-C does, instead of losing the buffer. add_signal_handler is Unix-only -
    # on Windows the KeyboardInterrupt path in main() covers Ctrl-C.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, AttributeError):
            loop.add_signal_handler(sig, stop.set)

    tasks = [
        asyncio.create_task(reader(queue, redis, stats), name="reader"),
        asyncio.create_task(flusher(queue, pool, stats), name="flusher"),
        asyncio.create_task(reporter(stats, queue), name="reporter"),
    ]
    log.info("Ingesting %s -> Redis + TimescaleDB", ", ".join(settings.symbols))

    stop_task = asyncio.create_task(stop.wait(), name="stop")
    try:
        # Wake on a shutdown signal OR on any task dying, so a crashed reader is not silently
        # ignored while the process sits there looking healthy.
        await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in (*tasks, stop_task):
            task.cancel()
        await asyncio.gather(*tasks, stop_task, return_exceptions=True)
        try:
            # Drain whatever the flusher had not reached yet - the difference between a clean
            # stop and losing the last couple of seconds of history on every restart.
            remaining = [queue.get_nowait() for _ in range(queue.qsize())]
            if remaining:
                log.info("Draining %d buffered ticks before exit", len(remaining))
                await _flush(remaining, pool, stats)
        finally:
            # Closing runs even if the drain was itself interrupted by a second Ctrl-C.
            with contextlib.suppress(Exception):
                await redis.aclose()
            with contextlib.suppress(Exception):
                await pool.close()
            log.info(
                "Stopped. %d ticks in, %d rows durable, %d dropped.",
                stats.ticks, stats.written, stats.dropped,
            )


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass  # the finally block in run() has already drained and closed everything


if __name__ == "__main__":
    main()

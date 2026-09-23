"""The TimescaleDB layer: durable tick history, written in batches and read by time range.

Redis answers "now" and "the last few minutes". This answers "everything, forever" - and it is
also the source the hot cache is rebuilt from after a Redis restart.
"""

from datetime import datetime, timedelta
from typing import Any

import asyncpg

from marketdata.config import settings

TICK_COLUMNS = ("time", "symbol", "price", "size", "side", "trade_id")


async def make_pool(dsn: str | None = None, min_size: int = 2, max_size: int | None = None) -> asyncpg.Pool:
    """A pool of open Postgres connections, shared by the whole process.

    LEARN: opening a Postgres connection is expensive - TCP handshake, authentication, and a
    new backend PROCESS forked on the server. Doing that per request would dwarf the query
    itself. A pool opens a few up front and lends them out. min_size keeps that many warm so
    the first request after an idle period is not the one that pays for the handshake.
    """
    return await asyncpg.create_pool(
        dsn or settings.pg_dsn,
        min_size=min_size,
        max_size=max_size or settings.pg_max_connections,
        command_timeout=10,  # a hung query must not pin a pooled connection forever
    )


async def insert_ticks(pool: asyncpg.Pool, ticks: list[dict]) -> int:
    """Write a batch of ticks in one COPY.

    WHY BATCHED : every individual INSERT costs a parse, a plan, a WAL
    record and an index update. Buffering and flushing amortizes all of that over hundreds of
    rows. The accepted cost is a small durability gap - a crash between flushes loses whatever
    is still in the buffer. Fine for market history; unacceptable for trade execution.

    WHY COPY AND NOT executemany: COPY streams rows in Postgres's binary format down one
    round trip with no per-row statement handling. It is the fastest bulk path asyncpg offers.
    The catch is all-or-nothing - one bad row rejects the batch - which is why parsing and
    validation happen up front in feed.py rather than here.
    """
    if not ticks:
        return 0
    records = [(t["time"], t["symbol"], t["price"], t["size"], t["side"], t["trade_id"]) for t in ticks]
    async with pool.acquire() as conn:
        await conn.copy_records_to_table("ticks", records=records, columns=list(TICK_COLUMNS))
    return len(records)


async def fetch_history(
    pool: asyncpg.Pool, symbol: str, since: datetime, limit: int
) -> list[dict[str, Any]]:
    """Newest-first ticks for one symbol since a point in time.

    This query is exactly the shape the `(symbol, time DESC)` index and the hypertable's time
    partitioning were built for: Timescale skips every chunk outside the range, then the index
    returns rows already in the order asked for, so there is no sort step at all.
    """
    rows = await pool.fetch(
        """
        SELECT time, symbol, price, size, side, trade_id
        FROM ticks
        WHERE symbol = $1 AND time >= $2
        ORDER BY time DESC
        LIMIT $3
        """,
        symbol,
        since,
        limit,
    )
    return [_row_to_tick(r) for r in rows]


async def fetch_latest_per_symbol(pool: asyncpg.Pool, symbols: list[str]) -> list[dict[str, Any]]:
    """The most recent stored tick for each symbol - used to rehydrate the cache on startup.

    DISTINCT ON is a Postgres extension: ordered by (symbol, time DESC) it keeps the first row
    of each symbol group, which is that symbol's newest tick. The portable alternative is a
    window function or a correlated subquery; both are more code and slower here.
    """
    rows = await pool.fetch(
        """
        SELECT DISTINCT ON (symbol) time, symbol, price, size, side, trade_id
        FROM ticks
        WHERE symbol = ANY($1::text[])
        ORDER BY symbol, time DESC
        """,
        symbols,
    )
    return [_row_to_tick(r) for r in rows]


# Accepted `interval` values -> the bucket width each one means. A fixed map, not free text:
# an unrecognized interval is rejected by name rather than passed through to the query.
#
# These are timedeltas, not strings like '1 minute', and that is not cosmetic. The query casts
# the parameter to ::interval, so asyncpg encodes it as a Postgres interval - which it can do
# from a timedelta and not from a str. Passing the string form raises DataError at bind time.
CANDLE_INTERVALS: dict[str, timedelta] = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "1d": timedelta(days=1),
}


async def fetch_candles(
    pool: asyncpg.Pool, symbol: str, interval: str, since: datetime, limit: int
) -> list[dict[str, Any]]:
    """OHLCV bars for one symbol, newest first, aggregated from the `candles_1m` view.

    WHY THIS READS THE VIEW AND NOT `ticks`: candles_1m is a continuous aggregate - Timescale
    keeps its buckets materialized and refreshes only the ones whose ticks changed. Measured on
    1.8M rows, a 24-hour range costs ~0.8 ms here versus ~112 ms aggregating the raw table, and
    the gap widens with the range because this scans one precomputed row per minute instead of
    every trade in it.

    WHY LARGER INTERVALS ROLL UP RATHER THAN GETTING THEIR OWN VIEW: a 5-minute candle is
    exactly five 1-minute candles combined, so one materialized view answers every interval.
    The combination is not a plain sum - open must come from the FIRST minute and close from
    the LAST, which is what first()/last() ordered by bucket do. Taking max(open) or avg(close)
    here would produce bars that look plausible and are quietly wrong.
    """
    rows = await pool.fetch(
        """
        SELECT time_bucket($2::interval, bucket) AS bucket,
               first(open, bucket)               AS open,
               max(high)                         AS high,
               min(low)                          AS low,
               last(close, bucket)               AS close,
               sum(volume)                       AS volume,
               sum(trades)                       AS trades
        FROM candles_1m
        WHERE symbol = $1 AND bucket >= $3
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT $4
        """,
        symbol,
        CANDLE_INTERVALS[interval],
        since,
        limit,
    )
    return [
        {
            "bucket": r["bucket"],
            "open": r["open"],
            "high": r["high"],
            "low": r["low"],
            "close": r["close"],
            "volume": r["volume"],
            "trades": r["trades"],
        }
        for r in rows
    ]


def _row_to_tick(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "time": row["time"],
        "symbol": row["symbol"],
        "price": row["price"],
        "size": row["size"],
        "side": row["side"],
        "trade_id": row["trade_id"],
    }

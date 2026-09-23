"""The Redis layer: hot cache, recent-history stream, and live fan-out.

Three access patterns, three Redis data types, one key namespace defined here so that no key
name is ever spelled out twice anywhere else in the codebase:

    latest:{symbol}    HASH    the current price          -> GET /prices/{symbol}
    stream:{symbol}    STREAM  capped recent tick log     -> GET /prices/{symbol}/history
    channel:{symbol}   PUBSUB  fire-and-forget fan-out    -> WS  /stream/{symbol}

Why three and not one: a hash read is O(1) and answers "what is it now"; a stream is an ordered
append-only log that answers "what happened over the last minute" and can be read by several
independent consumers; pub/sub answers neither but pushes instantly to whoever is listening.
Using one structure for all three would make two of the three slow or awkward.
"""

import json
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from marketdata.config import settings


def latest_key(symbol: str) -> str:
    return f"latest:{symbol}"


def stream_key(symbol: str) -> str:
    return f"stream:{symbol}"


def channel_key(symbol: str) -> str:
    return f"channel:{symbol}"


def to_wire(tick: dict) -> dict[str, str]:
    """Tick -> flat string fields. Redis stores strings, so datetimes and Nones need encoding.

    `ts_ms` (epoch milliseconds) is stored alongside the ISO timestamp on purpose: it is what
    the history endpoint compares against, and comparing integers is cheaper and less
    error-prone than reparsing an ISO string on every row.
    """
    return {
        "symbol": tick["symbol"],
        "price": repr(float(tick["price"])),
        "size": repr(float(tick["size"])),
        "side": tick["side"] or "",
        "trade_id": str(tick["trade_id"]) if tick.get("trade_id") is not None else "",
        "time": tick["time"].isoformat(),
        "ts_ms": str(int(tick["time"].timestamp() * 1000)),
    }


def from_wire(fields: dict[str, str]) -> dict[str, Any]:
    """Flat string fields -> a tick with real Python types, for JSON responses."""
    return {
        "symbol": fields["symbol"],
        "price": float(fields["price"]),
        "size": float(fields["size"]),
        "side": fields["side"] or None,
        "trade_id": int(fields["trade_id"]) if fields.get("trade_id") else None,
        "time": fields["time"],
    }


def make_client(url: str | None = None, max_connections: int | None = None) -> aioredis.Redis:
    """One shared client, backed by a connection pool.

    LEARN: `from_url` does not open a socket - it creates a POOL. Each command borrows a
    connection and returns it, so the TCP handshake is paid once at startup instead of on every
    request. plan.md section 7 calls this out as the cheapest latency win in the project;
    the benchmark in loadtest/ measures it rather than asserting it.
    """
    return aioredis.from_url(
        url or settings.redis_url,
        decode_responses=True,  # give back str, not bytes - saves a .decode() on every field
        max_connections=max_connections or settings.redis_max_connections,
    )


async def write_tick(redis: aioredis.Redis, tick: dict) -> None:
    """Fan one tick into all three structures in a single network round trip.

    LEARN: a PIPELINE batches commands and sends them together. Three separate awaits would
    cost three round trips - at ~50 ticks/sec that is 150 round trips a second spent waiting on
    the network for no reason. `transaction=False` because these three writes are independent;
    we want the batching, not MULTI/EXEC's atomicity guarantee, which costs extra work.
    """
    wire = to_wire(tick)
    symbol = tick["symbol"]

    pipe = redis.pipeline(transaction=False)
    pipe.hset(latest_key(symbol), mapping=wire)
    # maxlen + approximate=True caps memory. `approximate` lets Redis trim whole internal nodes
    # instead of walking entry by entry to hit an exact count - materially cheaper, and "about
    # 5000 recent ticks" is exactly as useful as "exactly 5000" for this purpose.
    pipe.xadd(stream_key(symbol), wire, maxlen=settings.stream_maxlen, approximate=True)
    pipe.publish(channel_key(symbol), json.dumps(wire))
    await pipe.execute()


async def get_latest(redis: aioredis.Redis, symbol: str) -> dict[str, Any] | None:
    """The hot path. One HGETALL, no query planner, no disk - target sub-5ms end to end."""
    fields = await redis.hgetall(latest_key(symbol))
    return from_wire(fields) if fields else None


async def read_recent(
    redis: aioredis.Redis, symbol: str, since_ms: int, limit: int
) -> list[dict[str, Any]] | None:
    """Ticks newer than `since_ms` from the stream, or None if the stream cannot cover it.

    Returning None is the important part: the stream is capped, so it may simply not reach far
    enough back. Rather than silently returning a truncated answer - the kind of bug nobody
    notices until a chart is quietly missing its left half - we say "I cannot answer this" and
    let the caller fall back to TimescaleDB.

    ONE CLOCK, NOT TWO. Every timestamp here is the stream ID's arrival time, never the tick's
    `ts_ms` exchange time. They differ by the feed lag - tens of milliseconds - and mixing them
    would mean deciding coverage on one clock and slicing on another, which is the sort of
    almost-right that produces an off-by-a-few-ticks bug at exactly the window boundary.
    """
    oldest = await redis.xrange(stream_key(symbol), count=1)
    if not oldest:
        return None
    # A stream ID is "<arrival ms>-<sequence>"; the left half is what the range bounds compare.
    oldest_ms = int(oldest[0][0].split("-")[0])
    if oldest_ms > since_ms:
        return None  # the window starts before the stream does -> only the DB has that part

    # XREVRANGE walks newest-first, so `count` returns the most recent N rather than the
    # oldest N - the order a chart wants, without sorting afterwards.
    entries = await redis.xrevrange(stream_key(symbol), max="+", min=f"{since_ms}-0", count=limit)
    return [from_wire(fields) for _id, fields in entries]


async def rehydrate_latest(redis: aioredis.Redis, rows: list[dict]) -> int:
    """Refill the hot cache from durable history after a Redis restart.

    plan.md section 3 accepts a volatile cache as a speed-for-durability trade. This is the
    other half of that bargain: on API startup the last known price per symbol is read back
    from TimescaleDB, so a cold Redis serves stale-but-real prices instead of 404s until the
    next live tick lands.
    """
    if not rows:
        return 0
    pipe = redis.pipeline(transaction=False)
    for row in rows:
        pipe.hset(latest_key(row["symbol"]), mapping=to_wire(row))
    await pipe.execute()
    return len(rows)


def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

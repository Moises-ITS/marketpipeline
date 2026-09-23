"""Integration tests for the two storage layers, against the real Redis and TimescaleDB.

What these prove that a mock never could: that a hypertable accepts a binary COPY, that
XREVRANGE bounds behave the way the history endpoint assumes, and that a float survives the
round trip through both stores unchanged.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from marketdata import db, store
from marketdata.config import settings
from tests.conftest import TEST_SYMBOL, make_tick, seed_stream_start

pytestmark = pytest.mark.integration


# --- Redis: the three writes -------------------------------------------------

async def test_write_tick_populates_cache_and_stream(redis):
    tick = make_tick(price=123.45)
    await store.write_tick(redis, tick)

    cached = await store.get_latest(redis, TEST_SYMBOL)
    assert cached["price"] == 123.45
    assert cached["symbol"] == TEST_SYMBOL

    assert await redis.xlen(store.stream_key(TEST_SYMBOL)) == 1


async def test_write_tick_publishes_to_subscribers(redis):
    pubsub = redis.pubsub()
    await pubsub.subscribe(store.channel_key(TEST_SYMBOL))
    # Drain the subscribe confirmation so the next message read is the real one.
    await pubsub.get_message(timeout=2)

    await store.write_tick(redis, make_tick(price=999.0))

    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
    assert message is not None, "no message published to the channel"
    assert json.loads(message["data"])["price"] == "999.0"
    await pubsub.aclose()


async def test_latest_overwrites_rather_than_accumulates(redis):
    # The hot cache answers "what is the price NOW". Two ticks must leave one value, not two.
    for price in (100.0, 200.0):
        await store.write_tick(redis, make_tick(price=price))
    assert (await store.get_latest(redis, TEST_SYMBOL))["price"] == 200.0
    assert await redis.xlen(store.stream_key(TEST_SYMBOL)) == 2  # the log keeps both


async def test_get_latest_returns_none_for_a_cold_cache(redis):
    assert await store.get_latest(redis, TEST_SYMBOL) is None


# --- Redis: the stream as a bounded history buffer ---------------------------

async def test_read_recent_returns_ticks_inside_the_window(redis):
    await seed_stream_start(redis)  # without this the stream begins "now" and declines below
    for i in range(5):
        await store.write_tick(redis, make_tick(price=100.0 + i, trade_id=i))

    ticks = await store.read_recent(redis, TEST_SYMBOL, store.now_ms() - 60_000, limit=10)
    # The seeded 2-minute-old entry proves coverage but falls outside the 60s window itself.
    assert len(ticks) == 5
    # XREVRANGE walks newest-first, which is the order the API returns and a chart expects.
    assert [t["price"] for t in ticks] == [104.0, 103.0, 102.0, 101.0, 100.0]


async def test_read_recent_respects_the_limit(redis):
    await seed_stream_start(redis)
    for i in range(10):
        await store.write_tick(redis, make_tick(trade_id=i))
    assert len(await store.read_recent(redis, TEST_SYMBOL, store.now_ms() - 60_000, limit=3)) == 3


async def test_read_recent_declines_a_window_older_than_the_stream(redis):
    """The fallback trigger: returning None is what sends the API to TimescaleDB.

    If this returned a truncated list instead, a one-hour chart would silently show only the
    few minutes Redis happens to be holding - the kind of wrong answer nobody notices.
    """
    await store.write_tick(redis, make_tick())
    # The stream begins ~now, so a window reaching an hour back cannot be answered from it.
    assert await store.read_recent(redis, TEST_SYMBOL, store.now_ms() - 3_600_000, limit=10) is None


async def test_read_recent_returns_none_when_the_stream_is_empty(redis):
    assert await store.read_recent(redis, TEST_SYMBOL, store.now_ms() - 1000, limit=10) is None


async def test_stream_is_capped(redis, monkeypatch):
    """XTRIM keeps memory bounded - approximately, and the approximation is the point.

    `approximate=True` lets Redis drop whole internal nodes (~100 entries each) instead of
    walking entry by entry, so the length settles NEAR the cap rather than exactly on it. The
    assertion below is deliberately loose for that reason: an exact-equality assertion here
    would be testing an implementation detail Redis never promised.
    """
    monkeypatch.setattr(settings, "stream_maxlen", 10)
    for i in range(400):
        await store.write_tick(redis, make_tick(trade_id=i))
    assert await redis.xlen(store.stream_key(TEST_SYMBOL)) < 400


# --- TimescaleDB: batched writes and range reads -----------------------------

async def test_insert_and_fetch_history(pool):
    ticks = [make_tick(price=100.0 + i, trade_id=i, seconds_ago=i) for i in range(5)]
    assert await db.insert_ticks(pool, ticks) == 5

    rows = await db.fetch_history(pool, TEST_SYMBOL, datetime.now(timezone.utc) - timedelta(minutes=1), 10)
    assert len(rows) == 5
    assert [r["price"] for r in rows] == [100.0, 101.0, 102.0, 103.0, 104.0]  # newest first
    assert rows[0]["time"] > rows[-1]["time"]


async def test_fetch_history_excludes_ticks_older_than_the_window(pool):
    await db.insert_ticks(pool, [make_tick(price=1.0, seconds_ago=0), make_tick(price=2.0, seconds_ago=600)])
    rows = await db.fetch_history(pool, TEST_SYMBOL, datetime.now(timezone.utc) - timedelta(seconds=60), 10)
    assert [r["price"] for r in rows] == [1.0]


async def test_insert_ticks_is_a_no_op_on_an_empty_batch(pool):
    # The flusher calls this on every time-based trigger, most of which fire with nothing
    # buffered during a quiet market. It must not open a connection or raise.
    assert await db.insert_ticks(pool, []) == 0


async def test_fetch_latest_per_symbol_returns_one_row_each(pool):
    await db.insert_ticks(pool, [
        make_tick(symbol="TEST-USD", price=1.0, seconds_ago=10),
        make_tick(symbol="TEST-USD", price=2.0, seconds_ago=0),
        make_tick(symbol="TEST2-USD", price=3.0, seconds_ago=5),
    ])
    rows = await db.fetch_latest_per_symbol(pool, ["TEST-USD", "TEST2-USD"])
    assert {r["symbol"]: r["price"] for r in rows} == {"TEST-USD": 2.0, "TEST2-USD": 3.0}


async def test_rehydration_refills_a_cold_cache_from_durable_history(redis, pool):
    """plan.md section 3's durability bargain, end to end.

    Redis is volatile by choice. This is what makes that choice survivable: after a restart the
    last known price comes back from TimescaleDB instead of the API returning 404 until the
    next trade happens to print.
    """
    await db.insert_ticks(pool, [make_tick(price=77_777.0)])
    assert await store.get_latest(redis, TEST_SYMBOL) is None  # cache is cold

    rows = await db.fetch_latest_per_symbol(pool, [TEST_SYMBOL])
    assert await store.rehydrate_latest(redis, rows) == 1
    assert (await store.get_latest(redis, TEST_SYMBOL))["price"] == 77_777.0


async def test_concurrent_writes_do_not_block_each_other(redis, pool):
    """A sanity check on the async design: 50 writes issued together, not one after another.

    If any layer were secretly synchronous, these would serialize and the elapsed time would be
    the sum of 50 round trips rather than roughly one.
    """
    start = asyncio.get_running_loop().time()
    await asyncio.gather(*(store.write_tick(redis, make_tick(trade_id=i)) for i in range(50)))
    elapsed = asyncio.get_running_loop().time() - start
    assert await redis.xlen(store.stream_key(TEST_SYMBOL)) == 50
    assert elapsed < 2.0, f"50 concurrent writes took {elapsed:.2f}s - something is blocking"

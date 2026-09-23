"""Shared fixtures.

TWO KINDS OF TEST LIVE IN THIS SUITE, and keeping them apart matters:

  unit        - pure functions (parsing, encoding, validation). No Redis, no Postgres, no
                network. They run in milliseconds and can never fail for an environmental
                reason, so a failure always means the code is wrong.
  integration - the real Redis and the real TimescaleDB from docker-compose. These prove the
                things a mock cannot: that a hypertable accepts a COPY, that XREVRANGE returns
                what we think it does, that FastAPI serializes what Redis handed back.

Integration tests SKIP (not fail) when the stack is down, because "you forgot docker compose
up" is not a bug in the code and should not look like one.

    .venv\\Scripts\\python.exe -m pytest -v            # everything
    .venv\\Scripts\\python.exe -m pytest -m unit       # no Docker needed
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from marketdata import db, store
from marketdata.config import settings

# Test data is written under symbols no exchange uses, so a test can never collide with - or
# delete - real ingested ticks sitting in the same Redis and the same hypertable.
TEST_SYMBOL = "TEST-USD"
TEST_SYMBOLS = [TEST_SYMBOL, "TEST2-USD"]

_infra_error: str | None = None
_infra_checked = False


async def _probe() -> str | None:
    try:
        client = store.make_client()
        await client.ping()
        await client.aclose()
    except Exception as exc:
        return f"Redis unreachable ({type(exc).__name__}: {exc})"
    try:
        pool = await db.make_pool(min_size=1, max_size=1)
        await pool.fetchval("SELECT 1 FROM ticks LIMIT 1")
        await pool.close()
    except Exception as exc:
        return f"TimescaleDB unreachable ({type(exc).__name__}: {exc})"
    return None


@pytest.fixture(scope="session")
def infra() -> None:
    """Skip the whole integration set once, with a message that says how to fix it."""
    global _infra_error, _infra_checked
    if not _infra_checked:
        _infra_error, _infra_checked = asyncio.run(_probe()), True
    if _infra_error:
        pytest.skip(f"{_infra_error} - run `docker compose up -d` first")


@pytest_asyncio.fixture
async def redis(infra):
    client = store.make_client()
    await _clear_test_keys(client)
    yield client
    await _clear_test_keys(client)
    await client.aclose()


@pytest_asyncio.fixture
async def pool(infra):
    pool = await db.make_pool(min_size=1, max_size=3)
    await _clear_test_rows(pool)
    yield pool
    await _clear_test_rows(pool)
    await pool.close()


@pytest.fixture
def served_symbols(monkeypatch):
    """Point the API's symbol allowlist at the test symbols for the duration of one test."""
    monkeypatch.setattr(settings, "symbols", TEST_SYMBOLS)
    return TEST_SYMBOLS


async def _clear_test_keys(client) -> None:
    keys = []
    for symbol in TEST_SYMBOLS:
        keys += [store.latest_key(symbol), store.stream_key(symbol)]
    await client.delete(*keys)


async def _clear_test_rows(pool) -> None:
    await pool.execute("DELETE FROM ticks WHERE symbol = ANY($1::text[])", TEST_SYMBOLS)


def make_tick(
    symbol: str = TEST_SYMBOL,
    price: float = 100.0,
    size: float = 1.5,
    side: str = "buy",
    trade_id: int = 1,
    seconds_ago: float = 0.0,
) -> dict:
    """One tick in the shape feed.parse_match produces, at a controllable point in time."""
    return {
        "symbol": symbol,
        "price": price,
        "size": size,
        "side": side,
        "trade_id": trade_id,
        "time": datetime.now(timezone.utc) - timedelta(seconds=seconds_ago),
    }


async def seed_stream_start(client, symbol: str = TEST_SYMBOL, seconds_ago: float = 120.0) -> None:
    """Make a stream 'reach back' in time, so the history endpoint will serve from it.

    A freshly written stream begins at *now*, so it can never cover a window that starts in the
    past - store.read_recent correctly declines and the API falls back to TimescaleDB. In a
    running system the stream is minutes deep and that check passes; here we recreate that by
    XADDing one entry with an explicitly old stream ID, which is the only way to write a stream
    entry that is not stamped with the current time.
    """
    old_ms = store.now_ms() - int(seconds_ago * 1000)
    await client.xadd(
        store.stream_key(symbol),
        store.to_wire(make_tick(symbol=symbol, price=0.0, trade_id=-1, seconds_ago=seconds_ago)),
        id=f"{old_ms}-0",
    )

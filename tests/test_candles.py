"""Tests for OHLCV candles served from the `candles_1m` continuous aggregate.

WHY THESE INSERT INTO THE CURRENT MINUTE: the aggregate is declared
`materialized_only = false`, so a read returns materialized buckets UNIONed with a live
aggregation over anything newer than the refresh watermark. The policy's `end_offset` holds
that watermark a minute behind now, which means the current minute is always on the live side
and shows up without waiting for - or forcing - a refresh. Writing into an older bucket would
land below the watermark and test the refresh policy's schedule rather than the query.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from marketdata import db
from marketdata.api import app
from marketdata.config import settings
from tests.conftest import TEST_SYMBOL


def _in_current_minute(offset_ms: int) -> datetime:
    """A timestamp inside the minute that is happening right now.

    Truncating to the minute first is what makes this deterministic: adding a few hundred
    milliseconds to a floored minute can never cross into the next bucket, whereas subtracting
    from `now()` lands in the previous one whenever the test happens to run just after :00.
    """
    now = datetime.now(timezone.utc)
    return now.replace(second=0, microsecond=0) + timedelta(milliseconds=offset_ms)


def _tick(price: float, size: float, trade_id: int, offset_ms: int) -> dict:
    return {
        "symbol": TEST_SYMBOL,
        "price": price,
        "size": size,
        "side": "buy",
        "trade_id": trade_id,
        "time": _in_current_minute(offset_ms),
    }


# --- unit --------------------------------------------------------------------

@pytest.mark.unit
def test_intervals_are_timedeltas_not_strings():
    """Regression guard for a real bug: the query casts this parameter to ::interval, and
    asyncpg encodes that from a timedelta but raises DataError on a str. The map looking
    correct ('1 minute') is exactly how that bug got written in the first place."""
    assert db.CANDLE_INTERVALS
    for name, width in db.CANDLE_INTERVALS.items():
        assert isinstance(width, timedelta), f"{name} must be a timedelta, got {type(width)}"


@pytest.mark.unit
def test_intervals_are_ordered_and_distinct():
    widths = list(db.CANDLE_INTERVALS.values())
    assert widths == sorted(widths), "intervals should read smallest-first"
    assert len(set(widths)) == len(widths)


# --- integration -------------------------------------------------------------

@pytest.fixture
def client(served_symbols, infra):
    with TestClient(app) as client:
        yield client


@pytest.mark.integration
async def test_candle_reports_open_high_low_close(pool):
    """The four prices must come from the right trades, not merely be present.

    Ordering the inserts so that open, high, low and close are four DIFFERENT values is the
    point: if the query used max(price) for open, or avg for close, a candle built from
    identical prices would still pass.
    """
    await db.insert_ticks(pool, [
        _tick(price=100.0, size=1.0, trade_id=1, offset_ms=100),   # first  -> open
        _tick(price=150.0, size=2.0, trade_id=2, offset_ms=200),   # max    -> high
        _tick(price=50.0, size=3.0, trade_id=3, offset_ms=300),    # min    -> low
        _tick(price=120.0, size=4.0, trade_id=4, offset_ms=400),   # last   -> close
    ])

    candles = await db.fetch_candles(
        pool, TEST_SYMBOL, "1m", datetime.now(timezone.utc) - timedelta(minutes=2), 10
    )

    assert candles, "the current minute should be visible via real-time aggregation"
    bar = candles[0]
    assert bar["open"] == 100.0
    assert bar["high"] == 150.0
    assert bar["low"] == 50.0
    assert bar["close"] == 120.0
    assert bar["volume"] == pytest.approx(10.0)
    assert bar["trades"] == 4


@pytest.mark.integration
async def test_rollup_to_a_wider_interval_keeps_first_open_and_last_close(pool):
    """A 5m bar built from 1m bars must take open from the earliest and close from the latest.

    Summing volume is the easy half. Getting open and close right is where a wrong rollup
    produces bars that look plausible and are quietly incorrect.
    """
    await db.insert_ticks(pool, [
        _tick(price=100.0, size=1.0, trade_id=1, offset_ms=100),
        _tick(price=200.0, size=1.0, trade_id=2, offset_ms=500),
    ])

    wide = await db.fetch_candles(
        pool, TEST_SYMBOL, "5m", datetime.now(timezone.utc) - timedelta(minutes=10), 10
    )
    narrow = await db.fetch_candles(
        pool, TEST_SYMBOL, "1m", datetime.now(timezone.utc) - timedelta(minutes=10), 10
    )

    assert wide and narrow
    # Whatever the minute boundaries happen to be, the wide bar must agree with the narrow
    # bars it contains: earliest open, latest close, summed volume.
    assert wide[0]["open"] == narrow[-1]["open"]
    assert wide[0]["close"] == narrow[0]["close"]
    assert wide[0]["volume"] == pytest.approx(sum(c["volume"] for c in narrow))
    assert wide[0]["trades"] == sum(c["trades"] for c in narrow)


@pytest.mark.integration
async def test_empty_range_returns_no_candles(pool):
    """A symbol with no ticks in the window is an empty list, not an error."""
    candles = await db.fetch_candles(
        pool, TEST_SYMBOL, "1m", datetime.now(timezone.utc) - timedelta(minutes=2), 10
    )
    assert candles == []


@pytest.mark.integration
def test_endpoint_rejects_an_unsupported_interval(client):
    body = client.get(f"/candles/{TEST_SYMBOL}?interval=7m")
    assert body.status_code == 400
    assert "interval" in body.json()["detail"]


@pytest.mark.integration
def test_endpoint_rejects_an_unknown_symbol(client):
    assert client.get("/candles/NOPE-USD").status_code == 404


@pytest.mark.integration
def test_endpoint_clamps_limit_to_the_configured_maximum(client, monkeypatch):
    """Asking for more than max_candle_limit must be capped, not honoured or rejected."""
    monkeypatch.setattr(settings, "max_candle_limit", 5)
    body = client.get(f"/candles/{TEST_SYMBOL}?limit=100000")
    assert body.status_code == 200
    assert body.json()["count"] <= 5


@pytest.mark.integration
def test_endpoint_echoes_the_interval_it_used(client):
    body = client.get(f"/candles/{TEST_SYMBOL}?interval=15m&window=2h").json()
    assert body["interval"] == "15m"
    assert body["window"] == "2h"

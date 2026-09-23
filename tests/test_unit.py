"""Unit tests: pure functions only, no Docker required.

These cover the boring-looking code that is where real bugs actually hide - parsing a feed
message, encoding a tick for Redis, validating a query string.
"""

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from marketdata import store
from marketdata.api import parse_window, validate_symbol
from marketdata.config import Settings
from marketdata.feed import parse_match

pytestmark = pytest.mark.unit


# --- feed normalization ------------------------------------------------------

def test_parse_match_normalizes_a_trade():
    tick = parse_match(
        {
            "type": "match",
            "product_id": "BTC-USD",
            "price": "78655.50",
            "size": "0.01331",
            "side": "buy",
            "trade_id": 1086932306,
            "time": "2026-08-28T19:04:11.123456Z",
        }
    )
    assert tick == {
        "symbol": "BTC-USD",
        "price": 78655.50,
        "size": 0.01331,
        "side": "buy",
        "trade_id": 1086932306,
        "time": datetime(2026, 8, 28, 19, 4, 11, 123456, tzinfo=timezone.utc),
    }
    # Timezone-aware, always. A naive datetime here would be silently reinterpreted as local
    # time by Postgres and shift the whole history by the machine's UTC offset.
    assert tick["time"].tzinfo is not None


@pytest.mark.parametrize("msg_type", ["ticker", "subscriptions", "heartbeat", "error"])
def test_parse_match_ignores_non_trade_messages(msg_type):
    assert parse_match({"type": msg_type}) is None


def test_parse_match_accepts_last_match():
    # Coinbase sends `last_match` once per symbol on subscribe - the trade that happened just
    # before you connected. It is a real trade and must not be dropped.
    assert parse_match(
        {"type": "last_match", "product_id": "ETH-USD", "price": "1", "size": "2",
         "time": "2026-08-28T19:04:11Z"}
    ) is not None


def test_parse_match_drops_malformed_messages_instead_of_raising():
    # A missing field must never take the ingest worker down with it.
    assert parse_match({"type": "match", "product_id": "BTC-USD"}) is None
    assert parse_match({"type": "match", "product_id": "BTC-USD", "price": "not-a-number",
                        "size": "1", "time": "2026-08-28T19:04:11Z"}) is None


# --- Redis wire encoding -----------------------------------------------------

def test_wire_round_trip_preserves_values():
    tick = {
        "symbol": "BTC-USD", "price": 78655.5, "size": 0.01331,
        "side": "sell", "trade_id": 42,
        "time": datetime(2026, 8, 28, 19, 4, 11, 123456, tzinfo=timezone.utc),
    }
    back = store.from_wire(store.to_wire(tick))
    assert (back["symbol"], back["price"], back["size"], back["side"], back["trade_id"]) == (
        "BTC-USD", 78655.5, 0.01331, "sell", 42,
    )


def test_wire_encodes_missing_optional_fields_as_empty_strings():
    # Redis hash fields cannot hold None. Empty string is the encoding, and from_wire must
    # turn it back into None rather than the string "None" or "".
    wire = store.to_wire({"symbol": "X-USD", "price": 1.0, "size": 1.0, "side": None,
                          "trade_id": None, "time": datetime.now(timezone.utc)})
    assert wire["side"] == "" and wire["trade_id"] == ""
    back = store.from_wire(wire)
    assert back["side"] is None and back["trade_id"] is None


def test_wire_survives_float_repr_precision():
    # repr() round-trips a float exactly; str formatting like f"{x:.2f}" would not. Tick sizes
    # on Coinbase are routinely 1e-8, which a naive two-decimal format silently turns into 0.
    tick = {"symbol": "X-USD", "price": 78655.123456789, "size": 1.9e-07, "side": "buy",
            "trade_id": 1, "time": datetime.now(timezone.utc)}
    back = store.from_wire(store.to_wire(tick))
    assert back["price"] == tick["price"]
    assert back["size"] == tick["size"]


def test_key_names_are_namespaced_per_symbol():
    assert store.latest_key("BTC-USD") == "latest:BTC-USD"
    assert store.stream_key("BTC-USD") == "stream:BTC-USD"
    assert store.channel_key("BTC-USD") == "channel:BTC-USD"


# --- API input validation ----------------------------------------------------

@pytest.mark.parametrize(
    "text,expected",
    [("30s", timedelta(seconds=30)), ("1m", timedelta(minutes=1)),
     ("2h", timedelta(hours=2)), ("  5M ", timedelta(minutes=5))],
)
def test_parse_window_accepts_the_documented_grammar(text, expected):
    assert parse_window(text) == expected


@pytest.mark.parametrize("bad", ["banana", "", "1d", "-5m", "5", "m5", "1.5m", "0s", "999h"])
def test_parse_window_rejects_everything_else(bad):
    with pytest.raises(HTTPException) as exc:
        parse_window(bad)
    assert exc.value.status_code == 400


def test_validate_symbol_is_case_insensitive_but_allowlisted(monkeypatch):
    from marketdata.config import settings
    monkeypatch.setattr(settings, "symbols", ["BTC-USD"])
    assert validate_symbol("btc-usd") == "BTC-USD"
    with pytest.raises(HTTPException) as exc:
        validate_symbol("DOGE-USD")
    assert exc.value.status_code == 404


# --- configuration -----------------------------------------------------------

def test_symbols_parse_from_a_comma_separated_env_var(monkeypatch):
    # The NoDecode trap documented in config.py: without it, pydantic-settings tries json.loads
    # on this string and crashes at import time. This test is what keeps that fix honest.
    monkeypatch.setenv("SYMBOLS", "BTC-USD, ETH-USD ,SOL-USD")
    assert Settings(_env_file=None).symbols == ["BTC-USD", "ETH-USD", "SOL-USD"]

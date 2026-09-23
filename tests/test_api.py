"""Integration tests for the API, driven through real HTTP and a real WebSocket.

TestClient starts the app exactly as uvicorn does - lifespan included - so these exercise the
connection pools, the cache rehydration, and the Pub/Sub broadcaster, not just handler bodies.
"""

import json
import threading
import time

import pytest
import redis as sync_redis
from fastapi.testclient import TestClient

from marketdata import db, store
from marketdata.api import app
from marketdata.config import settings
from tests.conftest import TEST_SYMBOL, make_tick, seed_stream_start

pytestmark = pytest.mark.integration


@pytest.fixture
def client(served_symbols, infra):
    # `with` is required, not stylistic: it is what runs startup and shutdown. Without it the
    # pools are never created and every request fails on a missing app.state.
    with TestClient(app) as client:
        yield client


# --- health ------------------------------------------------------------------

def test_health_reports_both_datastores(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    # Both probes must actually have been timed. A None here means a dependency was skipped.
    assert body["redis_ms"] is not None and body["timescaledb_ms"] is not None


# --- GET /prices/{symbol} - the hot path -------------------------------------

async def test_get_price_serves_the_cached_tick(redis, client):
    await store.write_tick(redis, make_tick(price=42_000.5, side="sell"))

    body = client.get(f"/prices/{TEST_SYMBOL}").json()
    assert body["price"] == 42_000.5
    assert body["side"] == "sell"
    assert body["symbol"] == TEST_SYMBOL


async def test_get_price_accepts_a_lowercase_symbol(redis, client):
    await store.write_tick(redis, make_tick(price=7.0))
    assert client.get(f"/prices/{TEST_SYMBOL.lower()}").json()["price"] == 7.0


def test_get_price_404s_for_a_symbol_this_instance_does_not_serve(client):
    response = client.get("/prices/DOGE-USD")
    assert response.status_code == 404
    assert "unknown symbol" in response.json()["detail"]


def test_get_price_404s_when_nothing_is_cached_yet(redis, client):
    # A served symbol with an empty cache is a different failure from an unknown symbol, and
    # the message says so - "is the ingest worker running?" is the actual answer nine times out
    # of ten.
    response = client.get(f"/prices/{TEST_SYMBOL}")
    assert response.status_code == 404
    assert "ingest worker" in response.json()["detail"]


# --- GET /prices/{symbol}/history - tier selection ---------------------------

async def test_history_is_served_from_redis_when_the_stream_covers_the_window(redis, client):
    await seed_stream_start(redis)  # a stream deep enough to cover a 30s window
    for i in range(3):
        await store.write_tick(redis, make_tick(price=10.0 + i, trade_id=i))

    body = client.get(f"/prices/{TEST_SYMBOL}/history", params={"window": "30s"}).json()
    assert body["source"] == "redis-stream"
    assert body["count"] == 3
    assert [t["price"] for t in body["ticks"]] == [12.0, 11.0, 10.0]


async def test_history_falls_back_to_timescaledb_for_an_older_window(redis, pool, client):
    """The tier decision the whole caching story rests on.

    The stream starts a moment ago, so it cannot answer a two-hour window; the hypertable can.
    The response says which one ran, so this is verifiable from the outside rather than by
    reading the code.
    """
    await db.insert_ticks(pool, [make_tick(price=555.0, seconds_ago=3600)])
    await store.write_tick(redis, make_tick(price=1.0))  # stream exists, but only covers "now"

    body = client.get(f"/prices/{TEST_SYMBOL}/history", params={"window": "2h"}).json()
    assert body["source"] == "timescaledb"
    assert 555.0 in [t["price"] for t in body["ticks"]]


async def test_history_force_db_switch_bypasses_redis(redis, client, monkeypatch):
    # The benchmark's "before" state. Same request, same data, different tier - which is
    # exactly what makes the before/after numbers in loadtest/RESULTS.md a fair comparison.
    await store.write_tick(redis, make_tick())
    monkeypatch.setattr(settings, "history_force_db", True)
    assert client.get(f"/prices/{TEST_SYMBOL}/history", params={"window": "30s"}).json()["source"] == "timescaledb"


def test_history_rejects_a_malformed_window(client):
    assert client.get(f"/prices/{TEST_SYMBOL}/history", params={"window": "banana"}).status_code == 400


def test_history_caps_an_oversized_limit(redis, client):
    # An unbounded limit is a denial-of-service handed to the caller. FastAPI rejects limit<1;
    # the handler silently clamps anything above max_history_limit rather than erroring.
    assert client.get(f"/prices/{TEST_SYMBOL}/history", params={"limit": 0}).status_code == 422
    assert client.get(f"/prices/{TEST_SYMBOL}/history", params={"limit": 10_000_000}).status_code == 200


# --- WS /stream/{symbol} - live fan-out --------------------------------------

def test_websocket_receives_a_published_tick(client):
    """End to end through Redis Pub/Sub: publish on one side, receive on the socket.

    The publishing runs on a repeating background thread on purpose. Subscribing is
    asynchronous - the broadcaster's pump task may not have reached Redis by the time the
    connection is open - so a single publish is a race. Publishing repeatedly makes the test
    prove delivery rather than winning a timing lottery.
    """
    with client.websocket_connect(f"/stream/{TEST_SYMBOL}") as ws:
        stop = threading.Event()

        def publish_until_received():
            r = sync_redis.from_url(settings.redis_url, decode_responses=True)
            while not stop.is_set():
                r.publish(store.channel_key(TEST_SYMBOL), json.dumps(store.to_wire(make_tick(price=31337.0))))
                time.sleep(0.05)
            r.close()

        publisher = threading.Thread(target=publish_until_received, daemon=True)
        publisher.start()
        try:
            message = ws.receive_json()
        finally:
            stop.set()
            publisher.join(timeout=2)

    assert message["price"] == 31337.0
    assert message["symbol"] == TEST_SYMBOL


def test_websocket_rejects_an_unknown_symbol(client):
    from starlette.websockets import WebSocketDisconnect

    with pytest.raises(WebSocketDisconnect) as exc:
        with client.websocket_connect("/stream/DOGE-USD") as ws:
            ws.receive_json()
    assert exc.value.code == 1008  # policy violation


def test_broadcaster_cleans_up_after_the_last_client_leaves(client):
    """A subscription leak is invisible until it is not. This is the test that catches it."""
    broadcaster = app.state.broadcaster
    with client.websocket_connect(f"/stream/{TEST_SYMBOL}"):
        pass
    # The disconnect handler is asynchronous; give it a moment to unwind before asserting.
    for _ in range(50):
        if not broadcaster._pumps:
            break
        time.sleep(0.05)
    assert broadcaster._pumps == {}, "a Redis subscription outlived its last WebSocket client"

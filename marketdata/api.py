"""The read side: REST for point-in-time and historical reads, WebSocket for live push.

Run it:
    .venv\\Scripts\\python.exe -m uvicorn marketdata.api:app --port 8000
Interactive docs at http://localhost:8000/docs

EVERY ENDPOINT IS ASYNC, and that is the whole point (plan.md section 7). A synchronous handler
occupies its worker for the entire duration of the Redis call - during which the CPU does
nothing but wait on a socket. An async handler yields at that await, so one worker serves
hundreds of concurrent requests. loadtest/RESULTS.md measures the difference instead of
claiming it.
"""

import asyncio
import contextlib
import json
import logging
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from marketdata import db, store
from marketdata.config import settings

log = logging.getLogger("api")

WINDOW_RE = re.compile(r"^(\d+)([smh])$")
WINDOW_UNITS = {"s": "seconds", "m": "minutes", "h": "hours"}
MAX_WINDOW = timedelta(days=7)


def parse_window(text: str) -> timedelta:
    """'30s' / '5m' / '2h' -> timedelta. Raises 400 rather than guessing at bad input.

    Accepting only this tiny grammar is deliberate: a free-form duration string is an easy way
    to hand a user the ability to ask for a query that never returns.
    """
    match = WINDOW_RE.match(text.strip().lower())
    if not match:
        raise HTTPException(400, "window must look like 30s, 5m or 2h")
    value, unit = int(match.group(1)), match.group(2)
    window = timedelta(**{WINDOW_UNITS[unit]: value})
    if window <= timedelta(0) or window > MAX_WINDOW:
        raise HTTPException(400, f"window must be between 1s and {MAX_WINDOW.days}d")
    return window


def validate_symbol(symbol: str) -> str:
    """Only symbols this deployment actually ingests are addressable.

    Without this check, `GET /prices/DOES-NOT-EXIST` reaches Redis, and the WebSocket endpoint
    would happily subscribe to a channel nobody ever publishes to - a client that hangs forever
    with no error. Rejecting unknown symbols up front turns both into an immediate, clear 404.
    """
    upper = symbol.upper()
    if upper not in settings.symbols:
        raise HTTPException(404, f"unknown symbol '{symbol}' - this instance serves {settings.symbols}")
    return upper


class Tick(BaseModel):
    symbol: str
    price: float
    size: float
    side: str | None = None
    trade_id: int | None = None
    time: datetime


class History(BaseModel):
    symbol: str
    window: str
    # Which tier answered - 'redis-stream' or 'timescaledb'. Exposed because it is the single
    # most useful thing to see while proving the caching story works.
    source: str
    count: int
    ticks: list[Tick]


class Candle(BaseModel):
    # `bucket` is the START of the interval, which is the convention every charting library
    # expects. A bar labelled 14:05 covers 14:05:00 to 14:05:59.
    bucket: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int


class Candles(BaseModel):
    symbol: str
    interval: str
    window: str
    count: int
    candles: list[Candle]


class Health(BaseModel):
    status: str
    redis_ms: float | None = None
    timescaledb_ms: float | None = None
    detail: str | None = None


class Broadcaster:
    """One Redis subscription per symbol, fanned out to every WebSocket watching it.

    The naive version gives each connected client its own Redis subscription. With 500 clients
    on BTC-USD that is 500 subscriptions and 500 copies of every message crossing the socket.
    This keeps exactly one subscription per symbol per API process, started on the first client
    and cancelled when the last one leaves, so Redis load stays flat in the number of clients.
    """

    def __init__(self, redis) -> None:
        self._redis = redis
        self._clients: dict[str, set[WebSocket]] = {}
        self._pumps: dict[str, asyncio.Task] = {}

    async def add(self, symbol: str, ws: WebSocket) -> None:
        self._clients.setdefault(symbol, set()).add(ws)
        if symbol not in self._pumps:
            self._pumps[symbol] = asyncio.create_task(self._pump(symbol), name=f"pump:{symbol}")

    async def remove(self, symbol: str, ws: WebSocket) -> None:
        clients = self._clients.get(symbol, set())
        clients.discard(ws)
        if not clients:
            self._clients.pop(symbol, None)
            pump = self._pumps.pop(symbol, None)
            if pump is not None:
                pump.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pump

    async def close(self) -> None:
        for pump in self._pumps.values():
            pump.cancel()
        await asyncio.gather(*self._pumps.values(), return_exceptions=True)
        self._pumps.clear()
        self._clients.clear()

    async def _pump(self, symbol: str) -> None:
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(store.channel_key(symbol))
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue  # subscribe/unsubscribe confirmations
                payload = jsonable(store.from_wire(json.loads(message["data"])))
                # Iterate a copy: a send failure mutates the set mid-loop otherwise.
                for ws in list(self._clients.get(symbol, ())):
                    try:
                        await ws.send_json(payload)
                    except Exception:
                        # The client vanished mid-broadcast. Drop it here; its own handler
                        # finishes cleaning up. One dead client must not stop the fan-out.
                        self._clients.get(symbol, set()).discard(ws)
        except asyncio.CancelledError:
            raise
        finally:
            with contextlib.suppress(Exception):
                await pubsub.aclose()


def jsonable(tick: dict) -> dict:
    """datetime -> ISO string, so send_json can serialize it."""
    out = dict(tick)
    if isinstance(out.get("time"), datetime):
        out["time"] = out["time"].isoformat()
    return out


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open pools once at startup, not per request - and warm the cache before serving.

    LEARN: lifespan runs on either side of the application's life. Everything expensive and
    long-lived belongs here; anything created inside a handler is created again on every single
    request, which is exactly the latency the connection pool exists to avoid.
    """
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)-7s %(name)s  %(message)s")
    app.state.redis = store.make_client()
    app.state.pool = await db.make_pool()
    app.state.broadcaster = Broadcaster(app.state.redis)

    # Rehydration (plan.md section 3): after a Redis restart the hot cache is empty. Refilling
    # it from the durable tier means a cold start serves last-known prices instead of 404s.
    try:
        rows = await db.fetch_latest_per_symbol(app.state.pool, settings.symbols)
        count = await store.rehydrate_latest(app.state.redis, rows)
        log.info("Rehydrated %d symbol(s) into the hot cache", count)
    except Exception:
        log.exception("Cache rehydration failed - serving from live ticks only")

    yield

    await app.state.broadcaster.close()
    await app.state.redis.aclose()
    await app.state.pool.close()


app = FastAPI(
    title="Market Data API",
    version="1.0",
    summary="Hot-path reads from Redis, historical reads from TimescaleDB, live push over WebSocket.",
    lifespan=lifespan,
)


@app.get("/health", response_model=Health)
async def health() -> Health:
    """Are both datastores actually reachable? Used by Docker's healthcheck, and by you.

    Timed with perf_counter, not the event loop's clock: on Windows loop.time() resolves to
    about 15 ms, so every sub-millisecond probe here reported a flat 0.0 ms. perf_counter is
    the highest-resolution timer Python offers, which is the only kind worth using on numbers
    this small.
    """
    timings: dict[str, float] = {}
    try:
        start = time.perf_counter()
        await app.state.redis.ping()
        timings["redis_ms"] = round((time.perf_counter() - start) * 1000, 2)

        start = time.perf_counter()
        await app.state.pool.fetchval("SELECT 1")
        timings["timescaledb_ms"] = round((time.perf_counter() - start) * 1000, 2)
    except Exception as exc:
        return Health(status="degraded", detail=f"{type(exc).__name__}: {exc}", **timings)
    return Health(status="ok", **timings)


@app.get("/prices/{symbol}", response_model=Tick)
async def get_price(symbol: str) -> Tick:
    """THE HOT PATH. One Redis HGETALL, nothing else - target sub-5ms p50 under load."""
    symbol = validate_symbol(symbol)
    tick = await store.get_latest(app.state.redis, symbol)
    if tick is None:
        raise HTTPException(404, f"no price cached for {symbol} yet - is the ingest worker running?")
    return Tick(**tick)


@app.get("/prices/{symbol}/history", response_model=History)
async def get_history(
    symbol: str,
    window: str = Query("1m", description="How far back to look: 30s, 5m, 2h"),
    limit: int = Query(500, ge=1),
) -> History:
    """Recent history from Redis Streams, older history from TimescaleDB - automatically.

    The tier choice is made by the data, not by the caller: if the capped stream still reaches
    back far enough to cover the window it answers, otherwise the query falls through to the
    hypertable. `source` in the response says which one ran.
    """
    symbol = validate_symbol(symbol)
    span = parse_window(window)
    limit = min(limit, settings.max_history_limit)
    since = datetime.now(timezone.utc) - span

    if not settings.history_force_db:
        ticks = await store.read_recent(app.state.redis, symbol, int(since.timestamp() * 1000), limit)
        if ticks is not None:
            return History(symbol=symbol, window=window, source="redis-stream", count=len(ticks), ticks=ticks)

    rows = await db.fetch_history(app.state.pool, symbol, since, limit)
    return History(symbol=symbol, window=window, source="timescaledb", count=len(rows), ticks=rows)


@app.get("/candles/{symbol}", response_model=Candles)
async def get_candles(
    symbol: str,
    interval: str = Query("1m", description=f"One of: {', '.join(db.CANDLE_INTERVALS)}"),
    window: str = Query("1h", description="How far back to look: 30s, 5m, 2h"),
    limit: int = Query(500, ge=1),
) -> Candles:
    """OHLCV bars, served from a continuous aggregate rather than computed per request.

    Unlike /history there is no tier choice to make: Redis holds raw ticks, and aggregating
    them per request is exactly the work this endpoint exists to avoid. The bars come from
    `candles_1m`, which Timescale keeps materialized and refreshes only where ticks changed.

    Measured on 1.8M ticks, a 24-hour range is ~0.8 ms here against ~112 ms for the same
    aggregation over the raw table - and the gap widens with the range, because this reads one
    precomputed row per minute instead of every trade inside it.
    """
    symbol = validate_symbol(symbol)
    if interval not in db.CANDLE_INTERVALS:
        raise HTTPException(400, f"interval must be one of {sorted(db.CANDLE_INTERVALS)}")
    span = parse_window(window)
    limit = min(limit, settings.max_candle_limit)
    since = datetime.now(timezone.utc) - span

    rows = await db.fetch_candles(app.state.pool, symbol, interval, since, limit)
    return Candles(symbol=symbol, interval=interval, window=window, count=len(rows), candles=rows)


@app.websocket("/stream/{symbol}")
async def stream(ws: WebSocket, symbol: str) -> None:
    """Push every tick for one symbol the instant the ingest worker publishes it.

    Pub/Sub is fire-and-forget: a client that disconnects for two seconds misses those two
    seconds, with no replay. That is the accepted tradeoff in plan.md section 5, and it is
    survivable precisely because /history can fill the gap from the stream afterwards.
    """
    upper = symbol.upper()
    if upper not in settings.symbols:
        # The connection must be accepted before it can be closed with a reason the client can
        # read; 1008 is the WebSocket code for "policy violation".
        await ws.accept()
        await ws.close(code=1008, reason=f"unknown symbol '{symbol}'")
        return

    await ws.accept()
    broadcaster: Broadcaster = app.state.broadcaster
    await broadcaster.add(upper, ws)
    try:
        # This endpoint only pushes; the receive loop exists solely to notice a disconnect.
        # Without it the server never learns the client is gone and the subscription leaks.
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await broadcaster.remove(upper, ws)

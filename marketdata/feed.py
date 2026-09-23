"""Coinbase trade feed: one connection, normalized ticks, survives disconnects.

feed_smoke.py proved the data arrives. This module is the production version of that: it
reconnects, backs off, and hands the rest of the system a stable shape to consume.

NORMALIZATION is why this file exists. Every exchange names its fields differently - Coinbase
says `product_id`, Binance says `s`. Converting to our own vocabulary at the single point
where data enters the system means adding a second exchange later touches only this file.
"""

import asyncio #pythons library for concurrency(doing many things at once)
import json #convert between json text and python objects (json.dumps(obj) => turns a python dict into a string & json.loads(text) => turns a string into python dict)
import logging #pythons logging printer(better than just print because each message has a severity associated like info, warning or error)
import random
from collections.abc import AsyncIterator #type hinting
from datetime import datetime 

import websockets
from websockets.asyncio.client import connect

from marketdata.config import settings

log = logging.getLogger(__name__) #automatically loads logger

BACKOFF_START_SECONDS = 1.0 #caps means constant
BACKOFF_MAX_SECONDS = 30.0 #after a disconnect, wait 1 then 2, 4, 8, 16 and capped at 30 seconds for reconnect


def parse_match(msg: dict) -> dict | None:
    """One raw Coinbase message -> our tick shape, or None if it is not a trade."""
    #match is a new trade just happened and last_match is most recent trade, sent once when you subscribe
    if msg.get("type") not in ("match", "last_match"):
        return None #not a trade ignore
    try:
        return {
            # Prices arrive as STRINGS ("77659.95"). That is deliberate on Coinbase's part:
            # JSON numbers are floats, and float("0.1") already loses precision. Sending exact
            # decimal text lets each consumer decide. We take float, matching the DOUBLE
            # PRECISION column - see the tradeoff note in db/init/001_schema.sql.
            "symbol": msg["product_id"],
            "price": float(msg["price"]),
            "size": float(msg["size"]),
            "side": msg.get("side"),
            "trade_id": msg.get("trade_id"),
            # Coinbase sends "2026-08-28T19:04:11.123456Z"; fromisoformat rejected a trailing
            # "Z" before Python 3.11, hence the replace.
            "time": datetime.fromisoformat(msg["time"].replace("Z", "+00:00")),
        }
    except (KeyError, ValueError) as exc:
        # A malformed message must not kill the worker. Log it and drop that one tick.
        log.warning("Unparseable match message dropped: %s (%s)", exc, msg)
        return None


async def stream_trades(
    symbols: list[str] | None = None,
    url: str | None = None,
) -> AsyncIterator[dict]:
    """Yield normalized ticks forever, reconnecting on any connection failure.

    LEARN: an async generator. The caller writes `async for tick in stream_trades():` and gets
    ticks one at a time; between ticks the event loop is free to run other tasks. The reconnect
    logic stays hidden in here, so the consumer never writes a retry loop of its own.
    """
    symbols = symbols or settings.symbols
    url = url or settings.coinbase_ws_url
    subscribe = json.dumps(
        {
            "type": "subscribe",
            "product_ids": symbols,
            # `matches` is completed trades. `ticker` (best-price snapshots) and `level2` (the
            # full order book) are out of scope per plan.md - this project is about pipeline
            # architecture, not market microstructure.
            "channels": ["matches"],
        }
    )
    backoff = BACKOFF_START_SECONDS

    while True:
        try:
            # ping_interval/ping_timeout are the dead-connection detector: if the exchange
            # stops answering pings the client raises instead of silently hanging forever on a
            # socket that will never deliver another byte. Silent hangs are the worst failure
            # mode for a feed - the process looks alive while the data has stopped.
            async with connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(subscribe)
                log.info("Feed connected: %s", ", ".join(symbols))
                backoff = BACKOFF_START_SECONDS  # a successful connect resets the penalty

                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") == "error":
                        log.error("Feed error: %s %s", msg.get("message"), msg.get("reason", ""))
                        continue
                    tick = parse_match(msg)
                    if tick is not None:
                        yield tick

        except asyncio.CancelledError:
            # Shutdown, not a failure. Re-raise so the task actually stops instead of
            # "reconnecting" during Ctrl-C.
            raise
        except (OSError, websockets.exceptions.WebSocketException) as exc:
            # Full jitter: a random wait in [0, backoff]. If several workers ever disconnect at
            # once, fixed delays would make them all retry in lockstep - a thundering herd.
            delay = random.uniform(0, backoff)
            log.warning("Feed dropped (%s: %s) - reconnecting in %.1fs", type(exc).__name__, exc, delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)

"""Connects to Coinbase and prints live trades. Nothing is stored yet.

WHAT THIS FILE IS FOR
One question only: does real market data actually arrive, and what shape is it? Answering that
in isolation

MARKET DATA VOCABULARY
  tick     One record of something that happened in the market. Here, one completed trade.

  match    Coinbase's name for a completed trade: a buy order and a sell order met at a price.
           
  QUOTE    Only an offer nobody has accepted yet.

  side     Which trader was the AGGRESSOR - the one who accepted an existing offer instead of
           waiting for someone to come to them. 'buy' means a buyer paid the asking price.

  size     How much of the asset changed hands, in units of the first symbol. On BTC-USD,
           size 0.5 is half a Bitcoin.

  BTC-USD  A "product" or trading pair: the price of Bitcoin quoted in US dollars.

A WORD ON CLOCKS -
The `lag` column below is computed as (our clock) - (exchange clock), so it is only as
trustworthy as the worse of the two clocks.

To fix it on Windows, in an ADMINISTRATOR terminal:
    net start w32time
    w32tm /resync
Then re-run and confirm lag reads positive.

WHAT IS DELIBERATELY MISSING
No reconnect logic, no backoff, no storage. The production version of this - which reconnects
with backoff and writes to both storage tiers - is marketdata/feed.py plus marketdata/ingest.py.
This file stays deliberately minimal so it remains a diagnostic: if it prints, the network,
the exchange, and the parsing are all fine, and the problem is downstream.

Run it:
    .venv\Scripts\python.exe -m marketdata.feed_smoke
Stop it with Ctrl-C.
"""

import asyncio
import json
import time
from datetime import datetime, timezone

from websockets.asyncio.client import connect

from marketdata.config import settings
# Normalization lives in feed.py, which the ingest worker uses too. Importing it here rather
# than keeping a second copy means the smoke test and production parse a message identically -
# if they drifted, this file would stop being evidence of anything.
from marketdata.feed import parse_match

# How often to print the throughput summary line.
RATE_REPORT_SECONDS = 5.0


async def stream_trades() -> None:
    subscribe_msg = {
        "type": "subscribe",
        "product_ids": settings.symbols,
        # A "channel" is a category of data. `matches` is completed trades. The alternatives -
        # `ticker` (best price snapshots) and `level2` (the full order book) - are out of scope
        # per plan.md: this project is about pipeline architecture, not market microstructure.
        "channels": ["matches"],
    }

    print(f"Connecting to {settings.coinbase_ws_url}")
    print(f"Subscribing to: {', '.join(settings.symbols)}\n")

    async with connect(settings.coinbase_ws_url) as ws:
        # LEARN: a WebSocket is a connection that stays OPEN. Unlike an HTTP request - ask,
        # get an answer, done - the server pushes new data to us the instant it exists. That
        # is what makes this genuinely real-time rather than "polled every second".
        await ws.send(json.dumps(subscribe_msg))

        total = 0
        per_symbol: dict[str, int] = {}
        window_start = time.perf_counter()
        window_count = 0
        clock_warned = False  # so the skew warning prints once, not on every tick

        # `async for` waits for the next message without blocking. While nothing is arriving,
        # the event loop is free to do other work - the whole basis of the async design.
        async for raw in ws:
            msg = json.loads(raw)

            if msg.get("type") == "subscriptions":
                print(f"Subscription confirmed: {msg.get('channels')}\n")
                continue
            if msg.get("type") == "error":
                print(f"Feed error: {msg.get('message')} - {msg.get('reason', '')}")
                continue

            tick = parse_match(msg)
            if tick is None:
                continue

            total += 1
            window_count += 1
            per_symbol[tick["symbol"]] = per_symbol.get(tick["symbol"], 0) + 1

            # LATENCY: how stale the data already is by the time we see it - the network hop
            # from Coinbase's matching engine to this machine. It is the baseline that no
            # amount of downstream optimization can undo, so it is worth knowing before tuning.
            
            # CAVEAT, and it is the important one: this subtracts the exchange's clock from
            # OUR clock. Two different machines, two different clocks. It measures real network
            # latency only if both are synchronized - see the clock section in the docstring.
            lag_ms = (datetime.now(timezone.utc) - tick["time"]).total_seconds() * 1000

            # Negative lag is physically impossible, so it can only mean our clock is behind.
            # Say so once, plainly, instead of printing nonsense numbers for the whole session.
            if lag_ms < 0 and not clock_warned:
                clock_warned = True
                for line in (
                    "",
                    f"!! Negative lag ({lag_ms:.0f}ms) - data cannot arrive before it happens.",
                    f"!! Your clock is ~{-lag_ms:.0f}ms behind the exchange, so every lag",
                    "!! reading below is off by that much. Fix (Administrator terminal):",
                    "!!     net start w32time && w32tm /resync",
                    "",
                ):
                    print(line)

            print(
                f"{tick['time']:%H:%M:%S.%f}  {tick['symbol']:<9} "
                f"{tick['side'] or '?':<4} {tick['price']:>12,.2f}  "
                f"size {tick['size']:<14.8f} lag {lag_ms:6.0f}ms"
            )

            elapsed = time.perf_counter() - window_start
            if elapsed >= RATE_REPORT_SECONDS:
                breakdown = "  ".join(f"{s}={n}" for s, n in sorted(per_symbol.items()))
                print(
                    f"--- {window_count / elapsed:.1f} ticks/sec  "
                    f"(total {total})  {breakdown} ---"
                )
                window_start, window_count = time.perf_counter(), 0


def main() -> None:
    try:
        asyncio.run(stream_trades())
    except KeyboardInterrupt:
        # Ctrl-C raises KeyboardInterrupt inside the loop. Catching it gives a clean exit
        # message instead of dumping a traceback for what is a completely normal shutdown.
        print("\nStopped.")


if __name__ == "__main__":
    main()

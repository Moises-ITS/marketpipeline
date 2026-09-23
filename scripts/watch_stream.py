"""Connects to the API's WebSocket endpoint and prints ticks as they are pushed.

This is the proof that the live path works end to end: Coinbase -> ingest worker -> Redis
Pub/Sub -> API -> this client. Nothing polls anything.

    .venv\\Scripts\\python.exe scripts\\watch_stream.py            # BTC-USD
    .venv\\Scripts\\python.exe scripts\\watch_stream.py ETH-USD
Stop it with Ctrl-C.
"""

import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from websockets.asyncio.client import connect

DEFAULT_URL = "ws://localhost:8000/stream"


async def watch(symbol: str, base_url: str) -> None:
    url = f"{base_url}/{symbol}"
    print(f"Connecting to {url}\nWaiting for the next trade to print...\n")

    async with connect(url) as ws:
        count = 0
        start = time.perf_counter()
        async for raw in ws:
            tick = json.loads(raw)
            count += 1
            elapsed = time.perf_counter() - start
            print(
                f"{tick['time'][11:23]}  {tick['symbol']:<9} {tick['side'] or '?':<4} "
                f"{tick['price']:>12,.2f}  size {tick['size']:<14.8f} "
                f"[{count} msgs, {count / elapsed:.1f}/s]"
            )


def main() -> None:
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTC-USD"
    base_url = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_URL
    try:
        asyncio.run(watch(symbol, base_url))
    except KeyboardInterrupt:
        print("\nStopped.")
    except OSError as exc:
        print(f"Could not connect: {exc}\nIs the API running? uvicorn marketdata.api:app --port 8000")
        sys.exit(1)


if __name__ == "__main__":
    main()

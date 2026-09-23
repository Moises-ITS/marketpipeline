"""Proves Redis and TimescaleDB are actually usable - not merely running.

Run it any time the stack feels wrong:
    .venv\Scripts\python.exe scripts\verify_infra.py
"""

import asyncio
import sys
import time
from pathlib import Path

# Lets the script find the `marketdata` package when run directly as a file rather than with
# `python -m`. sys.path is the list of folders Python searches for imports.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
import redis.asyncio as aioredis

from marketdata.config import settings

PASS, FAIL, INFO = "[PASS]", "[FAIL]", "  ->  "
failures: list[str] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    print(f"{PASS if ok else FAIL} {label}")
    if detail:
        print(f"{INFO}{detail}")
    if not ok:
        failures.append(label)


async def verify_redis() -> None:
    print("\n--- Redis (hot cache) ---")

    # LEARN: `redis.asyncio` is the async version of the client. Every call is awaited, meaning
    # that while Python waits on the network it hands control back to the event loop so other
    # work continues.
    client = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        t0 = time.perf_counter()
        await client.ping()
        check(True, "Connected and responded to PING", f"{(time.perf_counter() - t0) * 1000:.2f} ms")

        # A write-then-read round trip proves the connection is genuinely usable, not just open.
        # This latency is the floor your GET /prices/{symbol} endpoint can ever reach: the
        # ~5ms p50 target in plan.md is realistic only because this number is a fraction of it.
        t0 = time.perf_counter()
        await client.set("verify:probe", "ok", ex=10)  # ex=10 -> Redis deletes it after 10s
        value = await client.get("verify:probe")
        check(value == "ok", "SET/GET round trip", f"{(time.perf_counter() - t0) * 1000:.2f} ms")

        await client.delete("verify:probe")
    except Exception as exc:
        check(False, "Redis connection", f"{type(exc).__name__}: {exc}")
    finally:
        # Always release the socket, even if a check above raised.
        await client.aclose()


async def verify_timescale() -> None:
    print("\n--- TimescaleDB (durable history) ---")
    conn = None
    try:
        conn = await asyncpg.connect(settings.pg_dsn)
        check(True, "Connected", settings.pg_dsn.replace(":marketdata@", ":***@"))

        version = await conn.fetchval("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        check(version is not None, "TimescaleDB extension enabled", f"version {version}")

        # THE important check. `CREATE TABLE ticks` succeeding proves nothing about Timescale -
        # a plain Postgres table would look identical. A row here is the only proof that
        # create_hypertable() ran and time-based partitioning is actually active.
        is_hyper = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = 'ticks')"
        )
        check(bool(is_hyper), "`ticks` is a hypertable (not a plain table)")

        # End-to-end write path check, then clean up after ourselves.
        await conn.execute(
            "INSERT INTO ticks (time, symbol, price, size, side, trade_id) "
            "VALUES (now(), '__VERIFY__', 1.0, 1.0, 'buy', -1)"
        )
        found = await conn.fetchval("SELECT count(*) FROM ticks WHERE symbol = '__VERIFY__'")
        check(found == 1, "Insert and read back a row")
        await conn.execute("DELETE FROM ticks WHERE symbol = '__VERIFY__'")
    except Exception as exc:
        check(False, "TimescaleDB connection", f"{type(exc).__name__}: {exc}")
    finally:
        if conn is not None:
            await conn.close()


async def main() -> int:
    print(f"Verifying infrastructure for symbols: {', '.join(settings.symbols)}")
    await verify_redis()
    await verify_timescale()

    print("\n" + "=" * 60)
    if failures:
        print(f"{len(failures)} CHECK(S) FAILED: {', '.join(failures)}")
        print("Try: docker compose ps   (are both services healthy?)")
        print("     docker compose logs timescaledb")
        return 1
    print("ALL CHECKS PASSED - infrastructure is ready.")
    return 0


if __name__ == "__main__":
    # LEARN: asyncio.run() starts the event loop, runs one async function to completion, and
    # shuts the loop down. It is the single entry point from ordinary sync Python into async.
    # A non-zero exit code is how a CI system or shell script detects failure.
    sys.exit(asyncio.run(main()))

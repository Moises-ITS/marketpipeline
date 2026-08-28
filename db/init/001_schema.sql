-- Schema for durable tick history.
--
-- LEARN: a "tick" is one record of something that happened in the market. In this project a
-- tick is a completed TRADE: somebody bought and somebody sold, at a specific price, for a
-- specific quantity, at a specific instant. Tick data is the rawest form of market data -
-- charts, moving averages and volume bars are all computed from it.
--
-- This file runs automatically the first time the timescaledb container starts with an empty
-- data volume. See the GOTCHA comment in docker-compose.yml before you edit it.

-- The TimescaleDB image ships the extension but each database must enable it explicitly.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE ticks (
    -- The EXCHANGE's timestamp, not our own clock. Critical distinction: our machine's clock
    -- drifts, and network latency means we see a trade a few milliseconds after it happened.
    -- Storing when the market says it happened is what makes the history reproducible.
    -- TIMESTAMPTZ stores an absolute instant (internally UTC) rather than a wall-clock reading
    -- with no timezone, which would be ambiguous across daylight-saving changes.
    time     TIMESTAMPTZ      NOT NULL,

    -- Coinbase calls these "product IDs": 'BTC-USD' means the price of Bitcoin quoted in US
    -- dollars. The left side is what you are buying, the right side is what you pay with.
    symbol   TEXT             NOT NULL,

    price    DOUBLE PRECISION NOT NULL,

    -- Quantity traded, in units of the left-hand asset. size 0.5 on BTC-USD = half a Bitcoin.
    size     DOUBLE PRECISION NOT NULL,

    -- Which side was the AGGRESSOR - the trader who accepted an existing offer rather than
    -- waiting. 'buy' means a buyer crossed the spread and lifted a seller's price. A run of
    -- aggressive buys is a signal that demand is pushing the price up.
    side     TEXT,

    -- The exchange's own ID for this trade. Useful later for detecting duplicates after a
    -- reconnect, when the feed may replay messages you already saw.
    trade_id BIGINT
);

-- WHY DOUBLE PRECISION AND NOT NUMERIC:
-- DOUBLE PRECISION is a binary float - fast to compute with and compact to store, but it
-- cannot represent every decimal exactly (the classic 0.1 + 0.2 != 0.3 problem). That is
-- acceptable here because this table feeds charts and analytics, where an error in the 15th
-- significant digit is invisible. If this system ever settled real money - computing what a
-- customer is owed - NUMERIC would be mandatory, because a fraction-of-a-cent rounding error
-- repeated a million times becomes a real accounting discrepancy. Knowing which situation you
-- are in is the actual skill; picking the fast type is only correct once you have checked.

-- THE HYPERTABLE - the entire reason plan.md picks TimescaleDB over plain Postgres.
--
-- LEARN: `ticks` grows forever. A year of BTC-USD trades is hundreds of millions of rows, and
-- in a normal Postgres table a query like "the last hour" gets slower every single day as the
-- table grows around it.
--
-- create_hypertable() converts `ticks` into a hypertable: Timescale transparently splits it
-- into "chunks", each holding one slice of time (a week by default). To you it is still one
-- ordinary table you write plain SQL against. Underneath, a query for the last hour touches
-- exactly one small chunk and skips the rest - so it stays fast whether the table holds one
-- day or five years of history. That property is called chunk exclusion.
--
-- by_range('time') declares time as the partitioning column.
SELECT create_hypertable('ticks', by_range('time'));

-- Almost every query is "one symbol, most recent first" - the /history endpoint in build-order
-- step 6 is exactly that shape. DESC matches that access pattern so the newest rows are found
-- without the database sorting the results afterwards.
CREATE INDEX ON ticks (symbol, time DESC);

-- WHY THERE IS NO PRIMARY KEY:
-- Two reasons. First, a hypertable requires every unique index to include the partitioning
-- column, so a plain `PRIMARY KEY (trade_id)` is rejected outright - it would have to be
-- (trade_id, time). Second, a unique index costs time on every single insert, and this table
-- takes a high, continuous stream of writes. Duplicate ticks are harmless for charting, so
-- v1 accepts them rather than paying that cost. If deduplication becomes necessary after
-- reconnect handling lands, (trade_id, time) is the index to add.

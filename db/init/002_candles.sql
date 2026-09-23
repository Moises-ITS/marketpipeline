-- Phase 2, item 1: OHLCV candles as a continuous aggregate.
--
-- LEARN: a "candle" (or bar) is what ticks become before anyone looks at them. Every price
-- chart you have ever seen is candles. One candle summarizes all trades inside a time bucket:
--
--     OPEN   the first trade's price in the bucket
--     HIGH   the highest price traded
--     LOW    the lowest price traded
--     CLOSE  the last trade's price in the bucket
--     VOLUME how much changed hands in total
--
-- Open and close are what make it a candle rather than a summary: together they say which
-- direction the price moved during the bucket, which max and min alone cannot tell you.
--
-- Like 001_schema.sql, this runs automatically only on a FIRST start with an empty volume.
-- See the GOTCHA in docker-compose.yml. On an existing database, apply it by hand:
--     docker exec -i marketdata-timescaledb psql -U marketdata -d marketdata < db/init/002_candles.sql

-- WHY A CONTINUOUS AGGREGATE AND NOT A GROUP BY ON EVERY READ
--
-- Computing candles on read means scanning every tick in the range, every single request. A
-- day of BTC-USD is hundreds of thousands of rows to re-aggregate for an answer that has not
-- changed since the last time somebody asked.
--
-- A continuous aggregate materializes the buckets and refreshes only the ones whose underlying
-- ticks actually changed. The aggregation cost moves off the read path - the same move the
-- ingest worker already makes by batching writes instead of paying per tick.
CREATE MATERIALIZED VIEW candles_1m
WITH (
    timescaledb.continuous,

    -- REAL-TIME AGGREGATION. With materialized_only = false the view returns the materialized
    -- buckets UNIONed with a live aggregation over ticks newer than the last refresh. Without
    -- it, the newest candle would be missing until the next refresh runs, which for a live
    -- price chart is the one candle anybody is actually looking at.
    --
    -- The cost is that each read also scans the unmaterialized tail. That tail is bounded by
    -- end_offset below - about a minute of ticks - so it stays small.
    timescaledb.materialized_only = false
) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,

    -- first()/last() are TimescaleDB additions, and they are the reason this is expressible at
    -- all. Plain SQL has no aggregate for "the value of price in the row with the smallest
    -- time" - you would need a window function or a correlated subquery per bucket. These take
    -- the value column and the ordering column and do it in one pass.
    first(price, time) AS open,
    max(price)         AS high,
    min(price)         AS low,
    last(price, time)  AS close,

    sum(size)          AS volume,
    count(*)           AS trades
FROM ticks
GROUP BY bucket, symbol

-- WITH NO DATA: create the view definition without backfilling it right now. Backfilling
-- synchronously would block this script on however much history the table already holds. The
-- policy below fills it in the background instead.
WITH NO DATA;

-- THE REFRESH POLICY - what makes it "continuous" rather than a snapshot.
--
-- A background worker wakes on schedule_interval and re-materializes buckets in the window
-- between start_offset and end_offset, measured backwards from now.
SELECT add_continuous_aggregate_policy(
    'candles_1m',

    -- How far back to look for changes. Ticks arrive in batches up to ~2 seconds late
    -- (pg_flush_seconds), and a feed reconnect can replay slightly older trades, so an hour is
    -- generous headroom rather than a tight fit. Refreshing a bucket that did not change is
    -- cheap; missing one that did is a wrong candle forever.
    start_offset      => INTERVAL '1 hour',

    -- Leave the most recent minute alone: it is still being written to, and a bucket
    -- materialized mid-minute would be wrong until the next refresh overwrote it. Real-time
    -- aggregation (above) covers this gap on read, so nothing is lost by excluding it here.
    end_offset        => INTERVAL '1 minute',

    schedule_interval => INTERVAL '1 minute'
);

-- Index for the read pattern the API actually uses: one symbol, newest buckets first.
-- The cagg's own hypertable is indexed on bucket; this adds the symbol prefix so a
-- single-symbol query does not scan every symbol's rows in the range.
CREATE INDEX IF NOT EXISTS candles_1m_symbol_bucket_idx
    ON candles_1m (symbol, bucket DESC);

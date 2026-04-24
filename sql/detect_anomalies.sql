-- Detect unusually large trades in the last 30 minutes and insert them into
-- the alerts table. Runs every 15 minutes via BQ scheduled query.
--
-- Threshold: size AND notional must both exceed 5x the trailing-7-day median
-- for that market, with a floor of size >= 10. Requires at least 10 trades
-- of history per market; new markets won't trigger alerts until they
-- accumulate baseline volume, which is the intended behavior (no meaningful
-- baseline to compare against yet).

INSERT INTO prediction_markets.alerts (
  alert_id, source, market_id, trade_id, trade_ts,
  price, size, notional, reason
)
WITH recent_trades AS (
  SELECT
    source,
    market_id,
    trade_id,
    ts AS trade_ts,
    price,
    size,
    price * size AS notional
  FROM prediction_markets.trades
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
    AND size IS NOT NULL
    AND price IS NOT NULL
),
rolling_stats AS (
  SELECT
    market_id,
    -- APPROX_QUANTILES preserves input type; cast the size median to FLOAT64
    -- so downstream FORMAT() with %.1f accepts it (BQ won't auto-coerce).
    CAST(APPROX_QUANTILES(size, 100)[OFFSET(50)] AS FLOAT64) AS median_size,
    APPROX_QUANTILES(price * size, 100)[OFFSET(50)] AS median_notional
  FROM prediction_markets.trades
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND size IS NOT NULL
    AND price IS NOT NULL
  GROUP BY market_id
  HAVING COUNT(*) >= 10
),
candidates AS (
  SELECT
    TO_HEX(SHA256(CONCAT(rt.source, '|', rt.trade_id))) AS alert_id,
    rt.source,
    rt.market_id,
    rt.trade_id,
    rt.trade_ts,
    rt.price,
    rt.size,
    rt.notional,
    rs.median_size,
    rs.median_notional
  FROM recent_trades rt
  JOIN rolling_stats rs USING (market_id)
  WHERE rt.size >= GREATEST(10, 5 * rs.median_size)
    AND rt.notional >= 5 * rs.median_notional
)
SELECT
  c.alert_id,
  c.source,
  c.market_id,
  c.trade_id,
  c.trade_ts,
  c.price,
  c.size,
  c.notional,
  FORMAT(
    'size=%d (%.1f x median %.1f), notional=%.2f (%.1f x median %.2f)',
    c.size,
    CAST(c.size AS FLOAT64) / NULLIF(c.median_size, 0),
    c.median_size,
    c.notional,
    c.notional / NULLIF(c.median_notional, 0),
    c.median_notional
  ) AS reason
FROM candidates c
WHERE NOT EXISTS (
  SELECT 1 FROM prediction_markets.alerts a
  WHERE a.alert_id = c.alert_id
);

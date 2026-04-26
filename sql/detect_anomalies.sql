-- Detect unusually large trades in the last 30 minutes and insert them into
-- the alerts table. Runs every 15 minutes via BQ scheduled query.
--
-- Threshold: size AND notional must both exceed 5x the trailing-7-day median
-- for that market, with a floor of size >= 10. Requires at least 10 trades
-- of history per market; new markets won't trigger alerts until they
-- accumulate baseline volume, which is the intended behavior (no meaningful
-- baseline to compare against yet).

INSERT INTO prediction_markets.alerts (
  alert_id, source, market_id, title, trade_id, trade_ts,
  price, size, side, notional, reason
)
WITH recent_trades AS (
  SELECT
    source,
    market_id,
    trade_id,
    ts AS trade_ts,
    price,
    size,
    side,
    price * size AS notional
  FROM prediction_markets.trades
  WHERE ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
    AND size IS NOT NULL
    AND price IS NOT NULL
),
rolling_stats AS (
  SELECT
    market_id,
    -- cast the size median to FLOAT64
    -- so downstream FORMAT() with %.1f accepts it (BQ won't auto-coerce).
    CAST(AVG(size) AS FLOAT64) AS average_size,
    CAST(AVG(price * size) AS FLOAT64) AS average_notional
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
    rt.side,
    rt.notional,
    rs.average_size,
    rs.average_notional
  FROM recent_trades rt
  JOIN rolling_stats rs USING (market_id)
  WHERE rt.size >= GREATEST(10, 5 * rs.average_size)
    AND rt.notional >= 5 * rs.average_notional
)
SELECT
  c.alert_id,
  c.source,
  c.market_id,
  coalesce(kalshi_markets.title, INITCAP(REPLACE(REGEXP_REPLACE(polymarket_markets.slug, r'-[0-9]+$', ''), '-', ' ')) || '?') as title,
  c.trade_id,
  c.trade_ts,
  c.price,
  c.size,
  c.side,
  c.notional,
  FORMAT(
    'size=%.2f (%.1f x average %.1f), notional=%.2f (%.1f x average %.2f)',
    c.size,
    CAST(c.size AS FLOAT64) / NULLIF(c.average_size, 0),
    c.average_size,
    c.notional,
    c.notional / NULLIF(c.average_notional, 0),
    c.average_notional
  ) AS reason
FROM candidates c
LEFT JOIN prediction_markets.markets as kalshi_markets
  ON c.market_id = kalshi_markets.ticker
LEFT JOIN prediction_markets.polymarket_markets as polymarket_markets
  ON c.market_id = polymarket_markets.id
WHERE NOT EXISTS (
  SELECT 1 FROM prediction_markets.alerts a
  WHERE a.alert_id = c.alert_id
);

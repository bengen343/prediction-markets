-- Alerts table. Column order mirrors detect_anomalies_v2.sql's alerts CTE
-- output so the INSERT INTO ... (cols) ... SELECT * stays self-consistent.
-- Update this file AND the v2 query column list together.
--
-- trade_ts is DATETIME (not TIMESTAMP) - the v2 query converts to
-- America/Denver at the recent_trades CTE so alerts read in local time.
--
-- size is FLOAT64 to match trades.size (Polymarket has fractional shares).
CREATE TABLE IF NOT EXISTS prediction_markets.alerts (
  alert_id         STRING    NOT NULL,
  series_ticker    STRING,
  series_title     STRING,
  source           STRING    NOT NULL,
  title            STRING,
  market_id        STRING    NOT NULL,
  trade_id         STRING    NOT NULL,
  trade_ts         DATETIME  NOT NULL,
  side             STRING,
  price            FLOAT64,
  size             FLOAT64,
  notional         FLOAT64,
  size_zscore      FLOAT64,
  notional_zscore  FLOAT64,
  volume           FLOAT64,
  volume_24h       FLOAT64,
  reason           STRING,
  detected_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  notified_at      TIMESTAMP,
  discord_thread_id STRING
)
PARTITION BY DATE(detected_at)
CLUSTER BY source, market_id;

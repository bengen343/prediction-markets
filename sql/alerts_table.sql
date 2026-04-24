CREATE TABLE IF NOT EXISTS prediction_markets.alerts (
  alert_id     STRING    NOT NULL,
  source       STRING    NOT NULL,
  market_id    STRING    NOT NULL,
  trade_id     STRING    NOT NULL,
  trade_ts     TIMESTAMP NOT NULL,
  price        FLOAT64,
  size         INT64,
  notional     FLOAT64,
  reason       STRING,
  detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL,
  notified_at  TIMESTAMP
)
PARTITION BY DATE(detected_at)
CLUSTER BY source, market_id;

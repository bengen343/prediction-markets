CREATE TABLE IF NOT EXISTS prediction_markets.trades (
  source       STRING    NOT NULL,
  market_id    STRING    NOT NULL,
  trade_id     STRING    NOT NULL,
  ts           TIMESTAMP NOT NULL,
  price        FLOAT64,
  size         INT64,
  side         STRING,
  raw          JSON,
  ingested_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP() NOT NULL
)
PARTITION BY DATE(ts)
CLUSTER BY source, market_id;

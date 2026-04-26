-- Kalshi /series payloads. Refreshed every resolver run via WRITE_TRUNCATE.
-- Update this file AND src/prediction_markets/kalshi/resolver.py in lockstep
-- when Kalshi adds or removes a top-level Series field.
--
-- Nested arrays/objects (tags, settlement_sources, additional_prohibitions,
-- product_metadata) are stored as STRING (JSON-encoded text), matching the
-- convention in markets_table.sql. Query with PARSE_JSON / JSON_VALUE.
CREATE TABLE IF NOT EXISTS prediction_markets.kalshi_series (
  source                   STRING    NOT NULL,
  ticker                   STRING    NOT NULL,
  category                 STRING,
  frequency                STRING,
  title                    STRING,
  tags                     STRING,
  settlement_sources       STRING,
  contract_url             STRING,
  contract_terms_url       STRING,
  fee_type                 STRING,
  fee_multiplier           FLOAT64,
  additional_prohibitions  STRING,
  product_metadata         STRING,
  volume_fp                FLOAT64,
  last_updated_ts          TIMESTAMP,
  resolved_at              TIMESTAMP NOT NULL
)
CLUSTER BY category;

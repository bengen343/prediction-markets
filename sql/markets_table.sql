-- Schema for the markets table. Mirrors the Kalshi /markets payload exactly,
-- with our four conventions tacked on (source, series_ticker, category,
-- resolved_at). Update this file AND src/prediction_markets/kalshi/resolver.py
-- in lockstep when Kalshi adds or removes a top-level Market field.
--
-- Nested objects/arrays (price_ranges, custom_strike, mve_selected_legs) are
-- stored as STRING (JSON-encoded text) rather than BQ JSON type to avoid
-- BQ's NDJSON loader inspecting the contents and tripping on nested
-- case-conflicting keys. Query with PARSE_JSON(col) or JSON_VALUE(col, '$.x').
CREATE TABLE IF NOT EXISTS prediction_markets.markets (
  -- Conventions (we set these, not Kalshi)
  source                          STRING    NOT NULL,
  ticker                          STRING    NOT NULL,
  series_ticker                   STRING,
  category                        STRING,
  resolved_at                     TIMESTAMP NOT NULL,

  -- Identifiers / metadata
  event_ticker                    STRING,
  market_type                     STRING,
  title                           STRING,
  subtitle                        STRING,
  yes_sub_title                   STRING,
  no_sub_title                    STRING,

  -- Lifecycle timestamps
  created_time                    TIMESTAMP,
  updated_time                    TIMESTAMP,
  open_time                       TIMESTAMP,
  close_time                      TIMESTAMP,
  expected_expiration_time        TIMESTAMP,
  expiration_time                 TIMESTAMP,
  latest_expiration_time          TIMESTAMP,
  fee_waiver_expiration_time      TIMESTAMP,
  occurrence_datetime             TIMESTAMP,
  settlement_ts                   TIMESTAMP,
  settlement_timer_seconds        INT64,

  -- Status / state flags
  status                          STRING,
  result                          STRING,
  is_provisional                  BOOL,
  can_close_early                 BOOL,
  fractional_trading_enabled      BOOL,
  early_close_condition           STRING,
  expiration_value                STRING,

  -- Pricing
  yes_bid_dollars                 FLOAT64,
  yes_ask_dollars                 FLOAT64,
  yes_bid_size_fp                 FLOAT64,
  yes_ask_size_fp                 FLOAT64,
  no_bid_dollars                  FLOAT64,
  no_ask_dollars                  FLOAT64,
  last_price_dollars              FLOAT64,
  previous_price_dollars          FLOAT64,
  previous_yes_bid_dollars        FLOAT64,
  previous_yes_ask_dollars        FLOAT64,

  -- Volume / liquidity / settlement values
  volume_fp                       FLOAT64,
  volume_24h_fp                   FLOAT64,
  open_interest_fp                FLOAT64,
  liquidity_dollars               FLOAT64,
  notional_value_dollars          FLOAT64,
  settlement_value_dollars        FLOAT64,

  -- Pricing structure
  tick_size                       FLOAT64,
  price_level_structure           STRING,
  price_ranges                    STRING,
  response_price_units            STRING,

  -- Strike (scalar markets)
  strike_type                     STRING,
  floor_strike                    FLOAT64,
  cap_strike                      FLOAT64,
  functional_strike               STRING,
  custom_strike                   STRING,

  -- Multivariate event support
  mve_collection_ticker           STRING,
  mve_selected_legs               STRING,

  -- Misc
  primary_participant_key         STRING,
  rules_primary                   STRING,
  rules_secondary                 STRING
)
CLUSTER BY source, category, status;

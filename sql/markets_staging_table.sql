-- Staging table for the kalshi-resolver. Truncated and reloaded each run,
-- then a MERGE upserts from staging into markets. Schema mirrors the markets
-- table exactly except that resolved_at is omitted (the MERGE sets it via
-- CURRENT_TIMESTAMP()).
--
-- CREATE OR REPLACE (not IF NOT EXISTS) so every provision-gcp.ps1 run gives
-- staging the current schema. Staging is ephemeral - data is overwritten by
-- the resolver every run anyway - so blowing it away is safe and prevents
-- schema drift between staging and markets when the DDL evolves.
CREATE OR REPLACE TABLE prediction_markets.markets_staging (
  source                          STRING    NOT NULL,
  ticker                          STRING    NOT NULL,
  series_ticker                   STRING,
  category                        STRING,

  event_ticker                    STRING,
  market_type                     STRING,
  title                           STRING,
  subtitle                        STRING,
  yes_sub_title                   STRING,
  no_sub_title                    STRING,

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

  status                          STRING,
  result                          STRING,
  is_provisional                  BOOL,
  can_close_early                 BOOL,
  fractional_trading_enabled      BOOL,
  early_close_condition           STRING,
  expiration_value                STRING,

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

  volume_fp                       FLOAT64,
  volume_24h_fp                   FLOAT64,
  open_interest_fp                FLOAT64,
  liquidity_dollars               FLOAT64,
  notional_value_dollars          FLOAT64,
  settlement_value_dollars        FLOAT64,

  tick_size                       FLOAT64,
  price_level_structure           STRING,
  price_ranges                    STRING,
  response_price_units            STRING,

  strike_type                     STRING,
  floor_strike                    FLOAT64,
  cap_strike                      FLOAT64,
  functional_strike               STRING,
  custom_strike                   STRING,

  mve_collection_ticker           STRING,
  mve_selected_legs               STRING,

  primary_participant_key         STRING,
  rules_primary                   STRING,
  rules_secondary                 STRING
);

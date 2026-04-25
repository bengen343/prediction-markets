-- Staging table for the polymarket-resolver. Truncated and reloaded each run,
-- then a MERGE upserts from staging into polymarket_markets. Schema mirrors
-- polymarket_markets exactly except that resolved_at is omitted (the MERGE
-- sets it via CURRENT_TIMESTAMP()).
--
-- CREATE OR REPLACE (not IF NOT EXISTS) so every provision-gcp.ps1 run gives
-- staging the current schema. Staging is ephemeral - data is overwritten by
-- the resolver every run anyway - so blowing it away is safe and prevents
-- schema drift between staging and polymarket_markets when the DDL evolves.
CREATE OR REPLACE TABLE prediction_markets.polymarket_markets_staging (
  source                          STRING    NOT NULL,

  event_id                        STRING    NOT NULL,
  event_ticker                    STRING,
  event_slug                      STRING,
  event_title                     STRING,
  event_tag_slugs                 STRING,

  id                              STRING    NOT NULL,
  condition_id                    STRING,
  question_id                     STRING,
  slug                            STRING,

  outcomes_raw                    STRING,
  outcome_prices_raw              STRING,
  clob_token_ids_raw              STRING,
  yes_token_id                    STRING,
  no_token_id                     STRING,
  yes_price                       FLOAT64,
  no_price                        FLOAT64,

  question                        STRING,
  description                     STRING,
  image                           STRING,
  icon                            STRING,
  resolution_source               STRING,

  created_at                      TIMESTAMP,
  updated_at                      TIMESTAMP,
  start_date                      TIMESTAMP,
  end_date                        TIMESTAMP,
  closed_time                     TIMESTAMP,
  accepting_orders_timestamp      TIMESTAMP,
  uma_end_date                    TIMESTAMP,

  active                          BOOL,
  closed                          BOOL,
  archived                        BOOL,
  accepting_orders                BOOL,
  enable_order_book               BOOL,
  funded                          BOOL,
  approved                        BOOL,
  restricted                      BOOL,
  featured                        BOOL,
  automatically_resolved          BOOL,

  neg_risk                        BOOL,
  neg_risk_other                  BOOL,
  neg_risk_request_id             STRING,

  group_item_title                STRING,
  group_item_threshold            STRING,

  order_min_size                  INT64,
  order_price_min_tick_size       FLOAT64,
  spread                          FLOAT64,
  best_bid                        FLOAT64,
  best_ask                        FLOAT64,
  last_trade_price                FLOAT64,

  volume                          FLOAT64,
  volume_24h                      FLOAT64,
  volume_1wk                      FLOAT64,
  volume_1mo                      FLOAT64,
  volume_1yr                      FLOAT64,
  liquidity                       FLOAT64,
  liquidity_clob                  FLOAT64,

  one_hour_price_change           FLOAT64,
  one_day_price_change            FLOAT64,
  one_week_price_change           FLOAT64,
  one_month_price_change          FLOAT64,

  uma_resolution_status           STRING,
  uma_bond                        STRING,
  uma_reward                      STRING,
  resolved_by                     STRING,
  submitted_by                    STRING,

  clob_rewards_raw                STRING,
  rewards_min_size                INT64,
  rewards_max_spread              FLOAT64,
  holding_rewards_enabled         BOOL
);

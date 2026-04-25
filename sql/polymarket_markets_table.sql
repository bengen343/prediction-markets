-- Schema for Polymarket markets, mirroring the Gamma API event.markets[]
-- payload with our four conventions (source, resolved_at, event context).
-- One row per Polymarket market; the two outcome tokens (Yes/No) are
-- represented as parallel columns so each market is a single row even though
-- the collector subscribes to both clob_token_ids.
--
-- Update this file AND src/prediction_markets/polymarket/resolver.py in
-- lockstep when Polymarket adds or removes a top-level market field we care
-- about.
CREATE TABLE IF NOT EXISTS prediction_markets.polymarket_markets (
  -- Conventions (we set these, not Polymarket)
  source                          STRING    NOT NULL,
  resolved_at                     TIMESTAMP NOT NULL,

  -- Parent event context (we flatten markets out of events at resolve time)
  event_id                        STRING    NOT NULL,
  event_ticker                    STRING,
  event_slug                      STRING,
  event_title                     STRING,
  event_tag_slugs                 STRING,    -- JSON array of tag slugs

  -- Identifiers
  id                              STRING    NOT NULL,    -- Polymarket market id
  condition_id                    STRING,                -- on-chain condition id; used by CLOB /trades
  question_id                     STRING,                -- UMA question hash
  slug                            STRING,

  -- Outcome representation (binary; raw arrays preserved for safety)
  outcomes_raw                    STRING,                -- JSON: e.g. ["Yes","No"]
  outcome_prices_raw              STRING,                -- JSON: e.g. ["0.42","0.58"]
  clob_token_ids_raw              STRING,                -- JSON: two token ids
  yes_token_id                    STRING,
  no_token_id                     STRING,
  yes_price                       FLOAT64,
  no_price                        FLOAT64,

  -- Question / metadata
  question                        STRING,                -- the title used in alerts
  description                     STRING,
  image                           STRING,
  icon                            STRING,
  resolution_source               STRING,

  -- Lifecycle timestamps
  created_at                      TIMESTAMP,
  updated_at                      TIMESTAMP,
  start_date                      TIMESTAMP,
  end_date                        TIMESTAMP,
  closed_time                     TIMESTAMP,
  accepting_orders_timestamp      TIMESTAMP,
  uma_end_date                    TIMESTAMP,

  -- Status / lifecycle flags
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

  -- NegRisk (multi-outcome events resolved as a group; affects payout math)
  neg_risk                        BOOL,
  neg_risk_other                  BOOL,
  neg_risk_request_id             STRING,

  -- Group context (this market's slot inside a multi-market event)
  group_item_title                STRING,
  group_item_threshold            STRING,

  -- Trading params
  order_min_size                  INT64,
  order_price_min_tick_size       FLOAT64,
  spread                          FLOAT64,
  best_bid                        FLOAT64,
  best_ask                        FLOAT64,
  last_trade_price                FLOAT64,

  -- Volume / liquidity (use the *Num/*Clob FLOAT versions, not the string ones)
  volume                          FLOAT64,
  volume_24h                      FLOAT64,
  volume_1wk                      FLOAT64,
  volume_1mo                      FLOAT64,
  volume_1yr                      FLOAT64,
  liquidity                       FLOAT64,
  liquidity_clob                  FLOAT64,

  -- Price-change rollups
  one_hour_price_change           FLOAT64,
  one_day_price_change            FLOAT64,
  one_week_price_change           FLOAT64,
  one_month_price_change          FLOAT64,

  -- Resolution
  uma_resolution_status           STRING,
  uma_bond                        STRING,
  uma_reward                      STRING,
  resolved_by                     STRING,
  submitted_by                    STRING,

  -- Rewards config (kept as JSON since structure varies)
  clob_rewards_raw                STRING,
  rewards_min_size                INT64,
  rewards_max_spread              FLOAT64,
  holding_rewards_enabled         BOOL
)
CLUSTER BY source, closed, accepting_orders;

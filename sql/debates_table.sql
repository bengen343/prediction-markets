-- Debates table. One row per debate run (or cache-hit re-serve).
--
-- Lookups by (source, market_id, finished_at) drive the 3-hour reuse cache;
-- clustering matches the access pattern.
--
-- outcome values:
--   consensus         agents reached agreement; verdict populated
--   deadlock          moderator declared deadlock; verdict may hold dissent summary
--   budget_exhausted  ran past the cost cap before converging
--   error             unrecoverable failure during the run
--   cached            re-served a prior consensus; source_debate_id points to it
--
-- Only outcome='consensus' rows are eligible for re-serve via cache.
CREATE TABLE IF NOT EXISTS prediction_markets.debates (
  debate_id           STRING    NOT NULL,
  alert_id            STRING    NOT NULL,
  source              STRING    NOT NULL,
  market_id           STRING    NOT NULL,
  title               STRING,
  started_at          TIMESTAMP NOT NULL,
  finished_at         TIMESTAMP,
  outcome             STRING,
  verdict             JSON,
  turn_count          INT64,
  total_cost_usd      FLOAT64,
  cost_by_provider    JSON,
  transcript_gcs_uri  STRING,
  discord_thread_id   STRING,
  source_debate_id    STRING
)
PARTITION BY DATE(started_at)
CLUSTER BY source, market_id;

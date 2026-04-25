import json
import os
import re
from typing import Any

import yaml
from google.cloud import bigquery, storage

from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id
from ..shared.subscriptions import write_subscriptions_for_source
from .gamma import get_keyset_paginated

# (api_field, db_column, type) - type in {STRING, TIMESTAMP, INT64, FLOAT64,
# BOOL, JSON_STRING}. Mirrors sql/polymarket_markets_table.sql; update both
# in lockstep. The 'id' field is also the row's primary key (per source).
MARKET_FIELDS: tuple[tuple[str, str, str], ...] = (
    # Identifiers
    ("id", "id", "STRING"),
    ("conditionId", "condition_id", "STRING"),
    ("questionID", "question_id", "STRING"),
    ("slug", "slug", "STRING"),

    # Question / metadata
    ("question", "question", "STRING"),
    ("description", "description", "STRING"),
    ("image", "image", "STRING"),
    ("icon", "icon", "STRING"),
    ("resolutionSource", "resolution_source", "STRING"),

    # Lifecycle timestamps
    ("createdAt", "created_at", "TIMESTAMP"),
    ("updatedAt", "updated_at", "TIMESTAMP"),
    ("startDate", "start_date", "TIMESTAMP"),
    ("endDate", "end_date", "TIMESTAMP"),
    ("closedTime", "closed_time", "TIMESTAMP"),
    ("acceptingOrdersTimestamp", "accepting_orders_timestamp", "TIMESTAMP"),
    ("umaEndDate", "uma_end_date", "TIMESTAMP"),

    # Status / lifecycle flags
    ("active", "active", "BOOL"),
    ("closed", "closed", "BOOL"),
    ("archived", "archived", "BOOL"),
    ("acceptingOrders", "accepting_orders", "BOOL"),
    ("enableOrderBook", "enable_order_book", "BOOL"),
    ("funded", "funded", "BOOL"),
    ("approved", "approved", "BOOL"),
    ("restricted", "restricted", "BOOL"),
    ("featured", "featured", "BOOL"),
    ("automaticallyResolved", "automatically_resolved", "BOOL"),

    # NegRisk
    ("negRisk", "neg_risk", "BOOL"),
    ("negRiskOther", "neg_risk_other", "BOOL"),
    ("negRiskRequestID", "neg_risk_request_id", "STRING"),

    # Group context
    ("groupItemTitle", "group_item_title", "STRING"),
    ("groupItemThreshold", "group_item_threshold", "STRING"),

    # Trading params
    ("orderMinSize", "order_min_size", "INT64"),
    ("orderPriceMinTickSize", "order_price_min_tick_size", "FLOAT64"),
    ("spread", "spread", "FLOAT64"),
    ("bestBid", "best_bid", "FLOAT64"),
    ("bestAsk", "best_ask", "FLOAT64"),
    ("lastTradePrice", "last_trade_price", "FLOAT64"),

    # Volume / liquidity (use Num/Clob FLOAT versions, not the string-typed
    # 'volume'/'liquidity' fields)
    ("volumeNum", "volume", "FLOAT64"),
    ("volume24hr", "volume_24h", "FLOAT64"),
    ("volume1wk", "volume_1wk", "FLOAT64"),
    ("volume1mo", "volume_1mo", "FLOAT64"),
    ("volume1yr", "volume_1yr", "FLOAT64"),
    ("liquidityNum", "liquidity", "FLOAT64"),
    ("liquidityClob", "liquidity_clob", "FLOAT64"),

    # Price-change rollups
    ("oneHourPriceChange", "one_hour_price_change", "FLOAT64"),
    ("oneDayPriceChange", "one_day_price_change", "FLOAT64"),
    ("oneWeekPriceChange", "one_week_price_change", "FLOAT64"),
    ("oneMonthPriceChange", "one_month_price_change", "FLOAT64"),

    # Resolution
    ("umaResolutionStatus", "uma_resolution_status", "STRING"),
    ("umaBond", "uma_bond", "STRING"),
    ("umaReward", "uma_reward", "STRING"),
    ("resolvedBy", "resolved_by", "STRING"),
    # API uses snake_case for this single field; not a typo
    ("submitted_by", "submitted_by", "STRING"),

    # Rewards
    ("clobRewards", "clob_rewards_raw", "JSON_STRING"),
    ("rewardsMinSize", "rewards_min_size", "INT64"),
    ("rewardsMaxSpread", "rewards_max_spread", "FLOAT64"),
    ("holdingRewardsEnabled", "holding_rewards_enabled", "BOOL"),
)

# Columns we set ourselves rather than read from the API market object.
HEADER_COLUMNS: tuple[str, ...] = (
    "source",
    "event_id", "event_ticker", "event_slug", "event_title", "event_tag_slugs",
    "outcomes_raw", "outcome_prices_raw", "clob_token_ids_raw",
    "yes_token_id", "no_token_id", "yes_price", "no_price",
)

# All columns loaded into staging (excludes resolved_at, set by MERGE).
LOAD_COLUMNS: tuple[str, ...] = HEADER_COLUMNS + tuple(db for _, db, _ in MARKET_FIELDS)


_ISO_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _parse_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _parse_int(val: Any) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(val: Any) -> str | None:
    """Return val if it parses as ISO 8601 timestamp, else None. Polymarket
    sometimes returns "" or non-ISO strings for unset timestamps; BQ
    TIMESTAMP load fails on those, so we null them client-side - and we
    have to do it before the WRITE_TRUNCATE load or BQ silently degrades
    the column to STRING in the staging schema.
    """
    if isinstance(val, str) and _ISO_DT_RE.match(val):
        return val
    return None


def _parse_bool(val: Any) -> bool | None:
    return val if isinstance(val, bool) else None


def _coerce(val: Any, ftype: str) -> Any:
    if val is None:
        return None
    if ftype == "FLOAT64":
        return _parse_float(val)
    if ftype == "INT64":
        return _parse_int(val)
    if ftype == "BOOL":
        return _parse_bool(val)
    if ftype == "TIMESTAMP":
        return _parse_timestamp(val)
    if ftype == "JSON_STRING":
        return val if isinstance(val, str) else json.dumps(val, default=str)
    return val if isinstance(val, str) else str(val)


def _parse_str_array(raw: Any) -> list[Any] | None:
    """Polymarket encodes outcomes / outcomePrices / clobTokenIds as
    JSON-encoded string arrays (e.g. '["Yes","No"]')."""
    if not isinstance(raw, str):
        return None
    try:
        arr = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return arr if isinstance(arr, list) else None


def _build_row(market: dict, event: dict) -> dict:
    tag_slugs = [
        t.get("slug")
        for t in event.get("tags", []) or []
        if isinstance(t, dict) and t.get("slug")
    ]

    outcomes_raw = market.get("outcomes")
    outcome_prices_raw = market.get("outcomePrices")
    clob_token_ids_raw = market.get("clobTokenIds")

    token_ids = _parse_str_array(clob_token_ids_raw) or []
    prices = _parse_str_array(outcome_prices_raw) or []

    yes_token_id = str(token_ids[0]) if len(token_ids) >= 1 and token_ids[0] is not None else None
    no_token_id = str(token_ids[1]) if len(token_ids) >= 2 and token_ids[1] is not None else None
    yes_price = _parse_float(prices[0]) if len(prices) >= 1 else None
    no_price = _parse_float(prices[1]) if len(prices) >= 2 else None

    event_id = event.get("id")
    row: dict[str, Any] = {
        "source": "polymarket",
        "event_id": str(event_id) if event_id is not None else None,
        "event_ticker": event.get("ticker"),
        "event_slug": event.get("slug"),
        "event_title": event.get("title"),
        "event_tag_slugs": json.dumps(tag_slugs),
        "outcomes_raw": outcomes_raw if isinstance(outcomes_raw, str) else None,
        "outcome_prices_raw": outcome_prices_raw if isinstance(outcome_prices_raw, str) else None,
        "clob_token_ids_raw": clob_token_ids_raw if isinstance(clob_token_ids_raw, str) else None,
        "yes_token_id": yes_token_id,
        "no_token_id": no_token_id,
        "yes_price": yes_price,
        "no_price": no_price,
    }

    for api_name, db_name, ftype in MARKET_FIELDS:
        row[db_name] = _coerce(market.get(api_name), ftype)
    return row


def _staging_schema() -> list[bigquery.SchemaField]:
    """Explicit BQ schema for polymarket_markets_staging passed to every load
    job. Without it WRITE_TRUNCATE rewrites the staging schema with one
    inferred from the loaded data, which can silently degrade nullable
    TIMESTAMP columns to STRING and break the subsequent MERGE.
    """
    fields = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_ticker", "STRING"),
        bigquery.SchemaField("event_slug", "STRING"),
        bigquery.SchemaField("event_title", "STRING"),
        bigquery.SchemaField("event_tag_slugs", "STRING"),
        bigquery.SchemaField("outcomes_raw", "STRING"),
        bigquery.SchemaField("outcome_prices_raw", "STRING"),
        bigquery.SchemaField("clob_token_ids_raw", "STRING"),
        bigquery.SchemaField("yes_token_id", "STRING"),
        bigquery.SchemaField("no_token_id", "STRING"),
        bigquery.SchemaField("yes_price", "FLOAT64"),
        bigquery.SchemaField("no_price", "FLOAT64"),
    ]
    for _, db_name, ftype in MARKET_FIELDS:
        bq_type = "STRING" if ftype == "JSON_STRING" else ftype
        mode = "REQUIRED" if db_name == "id" else "NULLABLE"
        fields.append(bigquery.SchemaField(db_name, bq_type, mode=mode))
    return fields


def _build_merge_sql(target: str, staging: str) -> str:
    update_cols = [c for c in LOAD_COLUMNS if c not in {"source", "id"}]
    set_clause = ", ".join(f"`{c}` = S.`{c}`" for c in update_cols)
    set_clause += ", resolved_at = CURRENT_TIMESTAMP()"

    insert_cols = list(LOAD_COLUMNS) + ["resolved_at"]
    insert_vals = [f"S.`{c}`" for c in LOAD_COLUMNS] + ["CURRENT_TIMESTAMP()"]

    # T.closed IS NOT TRUE matches both NULL and FALSE - once a market is
    # marked closed=true we freeze its row and skip future updates.
    return f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.source = S.source AND T.id = S.id
    WHEN MATCHED AND T.closed IS NOT TRUE THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(f"`{c}`" for c in insert_cols)})
      VALUES ({", ".join(insert_vals)})
    """


def _upsert_markets(
    bq_client: bigquery.Client,
    project_id: str,
    dataset: str,
    rows: list[dict],
    log,
) -> None:
    staging = f"{project_id}.{dataset}.polymarket_markets_staging"
    target = f"{project_id}.{dataset}.polymarket_markets"

    load_cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=_staging_schema(),
    )
    load_job = bq_client.load_table_from_json(rows, staging, job_config=load_cfg)
    load_job.result(timeout=180)
    log.info("staging.loaded", count=len(rows))

    bq_client.query(_build_merge_sql(target, staging)).result(timeout=180)


def _load_yaml_from_gcs(bucket, path: str) -> dict:
    blob = bucket.blob(path)
    if not blob.exists():
        return {}
    return yaml.safe_load(blob.download_as_bytes()) or {}


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    bucket_name = os.environ.get("CONFIG_BUCKET", f"{project_id}-config")
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")

    log.info(
        "polymarket_resolver.startup",
        project_id=project_id, bucket=bucket_name, dataset=dataset,
    )

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    config = _load_yaml_from_gcs(bucket, "markets.yaml")
    poly_cfg = config.get("polymarket") or {}
    tag_slugs: list[str] = list(poly_cfg.get("tag_slug") or [])
    # Cap total rows for smoke-testing without burning a full pagination run.
    markets_limit = int(os.environ.get("MARKETS_LIMIT", "0") or "0")
    log.info("polymarket_resolver.config", tag_slugs=tag_slugs, markets_limit=markets_limit)

    rows: list[dict] = []
    seen_market_ids: set[str] = set()

    for tag in tag_slugs:
        count = 0
        for event in get_keyset_paginated(
            "/events/keyset",
            items_key="events",
            params={"tag_slug": tag, "closed": "false", "limit": 500},
        ):
            for market in event.get("markets") or []:
                if markets_limit and len(rows) >= markets_limit:
                    break
                market_id = market.get("id")
                if not market_id:
                    continue
                market_id = str(market_id)
                if market_id in seen_market_ids:
                    continue  # same market under multiple tags - keep first
                seen_market_ids.add(market_id)
                rows.append(_build_row(market, event))
                count += 1
            if markets_limit and len(rows) >= markets_limit:
                break
        log.info("polymarket_resolver.markets_for_tag", tag=tag, count=count)
        if markets_limit and len(rows) >= markets_limit:
            log.info("polymarket_resolver.limit_applied", limit=markets_limit)
            break

    log.info("polymarket_resolver.markets_total", count=len(rows))

    if rows:
        _upsert_markets(bq_client, project_id, dataset, rows, log)
        log.info("polymarket_resolver.markets_upserted", count=len(rows))

    # Subscribe only to markets that are tradable (open + accepting orders)
    # and have both outcome token IDs - the collector needs both to map a
    # trade event back to YES vs NO via the asset_id field.
    subs = [
        {
            "market_id": r["id"],
            "yes_token_id": r["yes_token_id"],
            "no_token_id": r["no_token_id"],
        }
        for r in rows
        if not r.get("closed")
        and r.get("accepting_orders")
        and r.get("yes_token_id")
        and r.get("no_token_id")
    ]

    write_subscriptions_for_source(bucket, "polymarket", {"markets": subs})
    log.info("polymarket_resolver.subscriptions_written", count=len(subs))


if __name__ == "__main__":
    main()

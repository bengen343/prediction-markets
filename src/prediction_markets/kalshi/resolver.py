import json
import os
import re
from typing import Any

import yaml
from google.cloud import bigquery, storage

from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id, get_secret, get_secret_bytes
from ..shared.subscriptions import write_subscriptions_for_source
from .auth import load_private_key
from .rest import get_paginated

# Type-specific field sets used by _build_row to coerce Kalshi payload values
# into the right Python types for BQ load. Keep these in sync with
# sql/markets_table.sql and sql/markets_staging_table.sql.
TIMESTAMP_FIELDS: frozenset[str] = frozenset({
    "created_time", "updated_time", "open_time", "close_time",
    "expected_expiration_time", "expiration_time", "latest_expiration_time",
    "fee_waiver_expiration_time", "occurrence_datetime", "settlement_ts",
})

INT64_FIELDS: frozenset[str] = frozenset({
    "settlement_timer_seconds",
})

FLOAT64_FIELDS: frozenset[str] = frozenset({
    "yes_bid_dollars", "yes_ask_dollars", "yes_bid_size_fp", "yes_ask_size_fp",
    "no_bid_dollars", "no_ask_dollars",
    "last_price_dollars", "previous_price_dollars",
    "previous_yes_bid_dollars", "previous_yes_ask_dollars",
    "volume_fp", "volume_24h_fp", "open_interest_fp",
    "liquidity_dollars", "notional_value_dollars", "settlement_value_dollars",
    "tick_size", "floor_strike", "cap_strike",
})

BOOL_FIELDS: frozenset[str] = frozenset({
    "is_provisional", "can_close_early", "fractional_trading_enabled",
})

# Nested objects/arrays stored as JSON-encoded strings (BQ STRING column).
JSON_STRING_FIELDS: frozenset[str] = frozenset({
    "price_ranges", "custom_strike", "mve_selected_legs",
})

# Every Kalshi top-level Market field except `ticker` (which we set explicitly
# as the merge key). Must mirror sql/markets_table.sql column list.
KALSHI_FIELDS: tuple[str, ...] = (
    "event_ticker", "market_type",
    "title", "subtitle", "yes_sub_title", "no_sub_title",
    "created_time", "updated_time", "open_time", "close_time",
    "expected_expiration_time", "expiration_time", "latest_expiration_time",
    "fee_waiver_expiration_time", "occurrence_datetime",
    "settlement_ts", "settlement_timer_seconds",
    "status", "result",
    "is_provisional", "can_close_early", "fractional_trading_enabled",
    "early_close_condition", "expiration_value",
    "yes_bid_dollars", "yes_ask_dollars", "yes_bid_size_fp", "yes_ask_size_fp",
    "no_bid_dollars", "no_ask_dollars",
    "last_price_dollars", "previous_price_dollars",
    "previous_yes_bid_dollars", "previous_yes_ask_dollars",
    "volume_fp", "volume_24h_fp", "open_interest_fp",
    "liquidity_dollars", "notional_value_dollars", "settlement_value_dollars",
    "tick_size", "price_level_structure", "price_ranges", "response_price_units",
    "strike_type", "floor_strike", "cap_strike", "functional_strike",
    "custom_strike",
    "mve_collection_ticker", "mve_selected_legs",
    "primary_participant_key",
    "rules_primary", "rules_secondary",
)

# All columns we load into staging (excludes resolved_at - the MERGE sets it).
LOAD_COLUMNS: tuple[str, ...] = (
    "source", "ticker", "series_ticker", "category",
) + KALSHI_FIELDS


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
    """Return val as an ISO 8601 string if it looks like a valid timestamp,
    else None. Kalshi sends empty string for timestamps that don't apply
    (e.g., fee_waiver_expiration_time on markets with no waiver), which BQ
    can't load as TIMESTAMP - we have to null those out client-side to
    avoid BQ silently reverting the column to STRING during WRITE_TRUNCATE.
    """
    if isinstance(val, str) and _ISO_DT_RE.match(val):
        return val
    return None


def _parse_bool(val: Any) -> bool | None:
    """Defensive bool coercion - non-bool values become None rather than
    being silently truthified (e.g., '' would become False under bool('')).
    """
    return val if isinstance(val, bool) else None


def _build_row(market: dict, series_ticker: str, category: str) -> dict:
    row: dict[str, Any] = {
        "source": "kalshi",
        "ticker": market.get("ticker"),
        "series_ticker": series_ticker,
        "category": category,
    }
    for field in KALSHI_FIELDS:
        val = market.get(field)
        if val is None:
            row[field] = None
        elif field in FLOAT64_FIELDS:
            row[field] = _parse_float(val)
        elif field in INT64_FIELDS:
            row[field] = _parse_int(val)
        elif field in BOOL_FIELDS:
            row[field] = _parse_bool(val)
        elif field in TIMESTAMP_FIELDS:
            row[field] = _parse_timestamp(val)
        elif field in JSON_STRING_FIELDS:
            row[field] = val if isinstance(val, str) else json.dumps(val)
        else:
            # Plain STRING fields.
            row[field] = val if isinstance(val, str) else str(val)
    return row


def _staging_schema() -> list[bigquery.SchemaField]:
    """Explicit BQ schema for markets_staging, passed to every load job.
    Without this, WRITE_TRUNCATE causes BQ to overwrite the table's schema
    with one auto-detected from the loaded data - and a TIMESTAMP column
    that's null on most rows can silently degrade to STRING in the new
    schema, breaking the subsequent MERGE.
    """
    fields = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("series_ticker", "STRING"),
        bigquery.SchemaField("category", "STRING"),
    ]
    for field in KALSHI_FIELDS:
        if field in TIMESTAMP_FIELDS:
            ftype = "TIMESTAMP"
        elif field in INT64_FIELDS:
            ftype = "INT64"
        elif field in FLOAT64_FIELDS:
            ftype = "FLOAT64"
        elif field in BOOL_FIELDS:
            ftype = "BOOL"
        else:
            ftype = "STRING"
        fields.append(bigquery.SchemaField(field, ftype))
    return fields


def _build_merge_sql(target: str, staging: str) -> str:
    update_cols = [c for c in LOAD_COLUMNS if c not in {"source", "ticker"}]
    set_clause = ", ".join(f"`{c}` = S.`{c}`" for c in update_cols)
    set_clause += ", resolved_at = CURRENT_TIMESTAMP()"

    insert_cols = list(LOAD_COLUMNS) + ["resolved_at"]
    insert_vals = [f"S.`{c}`" for c in LOAD_COLUMNS] + ["CURRENT_TIMESTAMP()"]

    return f"""
    MERGE `{target}` T
    USING `{staging}` S
    ON T.source = S.source AND T.ticker = S.ticker
    WHEN MATCHED AND T.settlement_ts IS NULL THEN
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
    staging = f"{project_id}.{dataset}.markets_staging"
    target = f"{project_id}.{dataset}.markets"

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


def _write_subscriptions(bucket, tickers: list[str]) -> None:
    write_subscriptions_for_source(bucket, "kalshi", {
        "tickers": sorted(set(tickers)),
        "categories": [],
    })


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    bucket_name = os.environ.get("CONFIG_BUCKET", f"{project_id}-config")
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")

    log.info("resolver.startup", project_id=project_id, bucket=bucket_name, dataset=dataset)

    private_key = load_private_key(get_secret_bytes("kalshi-private-key"))
    access_key_id = get_secret("kalshi-api-key-id")

    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    config = _load_yaml_from_gcs(bucket, "markets.yaml")
    kalshi_cfg = config.get("kalshi") or {}
    categories: list[str] = list(kalshi_cfg.get("categories") or [])
    manual_tickers: list[str] = list(kalshi_cfg.get("tickers") or [])
    log.info("resolver.config", categories=categories, manual_tickers=len(manual_tickers))

    series_by_category: list[tuple[str, str]] = []
    for category in categories:
        count = 0
        for series in get_paginated(
            private_key, access_key_id,
            "/trade-api/v2/series",
            items_key="series",
            params={"category": category, "limit": 200, "include_volume": "true"},
        ):
            try:
                vol = float(series.get("volume_fp", "0") or "0")
            except (TypeError, ValueError):
                vol = 0.0
            if vol <= 0:
                continue  # skip series with no lifetime trades - saves a /markets call
            series_by_category.append((category, series["ticker"]))
            count += 1
        log.info("resolver.series_discovered", category=category, count=count)

    series_limit = int(os.environ.get("SERIES_LIMIT", "0") or "0")
    if series_limit > 0 and len(series_by_category) > series_limit:
        log.info(
            "resolver.series_limit_applied",
            limit=series_limit,
            original=len(series_by_category),
        )
        series_by_category = series_by_category[:series_limit]

    rows: list[dict] = []
    for category, series_ticker in series_by_category:
        count = 0
        for market in get_paginated(
            private_key, access_key_id,
            "/trade-api/v2/markets",
            items_key="markets",
            params={"series_ticker": series_ticker, "limit": 1000},
        ):
            rows.append(_build_row(market, series_ticker, category))
            count += 1
        log.info("resolver.markets_for_series", series=series_ticker, count=count)

    log.info("resolver.markets_total", count=len(rows))

    if rows:
        _upsert_markets(bq_client, project_id, dataset, rows, log)
        log.info("resolver.markets_upserted", count=len(rows))

    open_tickers = {r["ticker"] for r in rows if r.get("status") == "active"}
    open_tickers.update(manual_tickers)
    _write_subscriptions(bucket, sorted(open_tickers))
    log.info("resolver.subscriptions_written", count=len(open_tickers))


if __name__ == "__main__":
    main()

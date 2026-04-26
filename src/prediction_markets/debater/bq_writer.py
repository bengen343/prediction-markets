import json
from datetime import UTC, datetime
from typing import Any

from google.cloud import bigquery


def write_debate_row(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    row: dict[str, Any],
) -> None:
    """Insert one debate row using a streaming-style load job.

    INSERT DML would honor DEFAULTs but is rate-limited and quota-heavy for
    one-row-at-a-time use; load_table_from_json works fine for our cadence.
    """
    table_ref = f"{project_id}.{dataset}.debates"

    # Serialize JSON columns; BQ load expects strings or dicts.
    serialized = dict(row)
    for json_col in ("verdict", "cost_by_provider"):
        v = serialized.get(json_col)
        if v is not None and not isinstance(v, str):
            serialized[json_col] = json.dumps(v, default=str)

    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json([serialized], table_ref, job_config=config)
    job.result(timeout=60)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()

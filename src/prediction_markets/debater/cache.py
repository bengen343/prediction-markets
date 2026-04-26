from google.cloud import bigquery


def find_cached_consensus(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    source: str,
    market_id: str,
    hours: int = 3,
) -> dict | None:
    """Returns the most recent consensus debate for this market within the window, or None."""
    sql = f"""
        SELECT debate_id, verdict, transcript_gcs_uri, finished_at
        FROM `{project_id}.{dataset}.debates`
        WHERE source = @source
          AND market_id = @market_id
          AND outcome = 'consensus'
          AND finished_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
        ORDER BY finished_at DESC
        LIMIT 1
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("source", "STRING", source),
                bigquery.ScalarQueryParameter("market_id", "STRING", market_id),
                bigquery.ScalarQueryParameter("hours", "INT64", hours),
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    r = rows[0]
    return {
        "debate_id": r.debate_id,
        "verdict": r.verdict,
        "transcript_gcs_uri": r.transcript_gcs_uri,
        "finished_at": r.finished_at,
    }

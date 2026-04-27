from google.cloud import bigquery


def find_cached_consensus(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    source: str,
    series_ticker: str | None,
    market_id: str,
    hours: int = 3,
) -> dict | None:
    """Returns the most recent consensus debate eligible for reuse, or None.

    Cache key is (source, COALESCE(series_ticker, market_id)):
    - When series_ticker is present, every market within a series shares one
      cache entry — the agents' research is mostly the same.
    - When series_ticker is null (sources without series resolution), we fall
      back to per-market caching.

    Returns the original `title` and `market_id` so callers can surface which
    specific market the cached verdict was actually debated for.
    """
    cache_key = series_ticker or market_id
    sql = f"""
        SELECT debate_id, verdict, transcript_gcs_uri, finished_at,
               title AS source_title, market_id AS source_market_id
        FROM `{project_id}.{dataset}.debates`
        WHERE source = @source
          AND COALESCE(series_ticker, market_id) = @cache_key
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
                bigquery.ScalarQueryParameter("cache_key", "STRING", cache_key),
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
        "source_title": r.source_title,
        "source_market_id": r.source_market_id,
    }

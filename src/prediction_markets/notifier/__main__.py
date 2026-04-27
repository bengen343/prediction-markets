import json
import os
from collections import OrderedDict

from google.cloud import bigquery, storage

from ..shared.config import _parse as parse_markets_yaml
from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id, get_secret
from .discord import post_message


# Discord user(s) to ping on each alert. Override via env var if needed.
_DISCORD_MENTION = os.environ.get("DISCORD_MENTION", "<@746817328242491532>")

# Topic name is conventional; project ID is discovered from the metadata server.
# Topic itself is created by scripts/provision-gcp-debater.ps1, not the alerting
# provisioner. If debater.enabled is false this code path never runs and the
# topic doesn't need to exist.
_DEBATE_TOPIC = "debate-requests"


def _format_message(row) -> str:
    trade_ts = row.trade_ts.isoformat() if hasattr(row.trade_ts, "isoformat") else row.trade_ts
    title_line = f"_{row.title}_\n" if row.title else ""
    return (
        f"{_DISCORD_MENTION} Unusual trade on {row.market_id} ({row.source})\n"
        f"**{title_line}**"
        f"Side: {row.side or '?'}  Size: {row.size}  Price: ${row.price:.3f}  Notional: ${row.notional:.2f}\n"
        f"Trigger: {row.reason}\n"
        f"Trade time: {trade_ts}"
    )


def _thread_title(row) -> str:
    # Forum-channel webhook requires a thread title on the first post. Prefer
    # the series title (so all markets in one series share one thread), then
    # fall back to the market title, then a market-id-based label.
    return (
        getattr(row, "series_title", None)
        or row.title
        or f"{row.source}: {row.market_id}"
    )


def _thread_key(row) -> tuple[str, str]:
    # Anchor threads on series when we have one (series_ticker is populated
    # for sources we've integrated series resolution for). Falls back to
    # market_id so series-less alerts still get their own thread.
    series_ticker = getattr(row, "series_ticker", None)
    return (row.source, series_ticker or row.market_id)


def _lookup_recent_threads(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    hours: int = 1,
) -> dict[tuple[str, str], str]:
    """Build (source, series_ticker) -> most-recent thread_id map for alerts
    notified within the lookback window. Used to consolidate alerts in the
    same series into a single thread across notifier cycles. Series-less
    alerts (NULL series_ticker) are intentionally excluded — they always
    create their own thread.
    """
    sql = f"""
        SELECT
          source,
          series_ticker,
          ARRAY_AGG(discord_thread_id ORDER BY notified_at DESC LIMIT 1)[OFFSET(0)] AS thread_id
        FROM `{project_id}.{dataset}.alerts`
        WHERE notified_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
          AND discord_thread_id IS NOT NULL
          AND series_ticker IS NOT NULL
        GROUP BY source, series_ticker
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("hours", "INT64", hours)]
        ),
    )
    return {(r.source, r.series_ticker): r.thread_id for r in job.result() if r.thread_id}


def _load_debater_config(project_id: str, log) -> tuple[bool, bool]:
    """Returns (enabled, auto_publish) read from gs://<bucket>/markets.yaml."""
    bucket_name = os.environ.get("CONFIG_BUCKET", f"{project_id}-config")
    try:
        client = storage.Client(project=project_id)
        blob = client.bucket(bucket_name).blob("markets.yaml")
        if not blob.exists():
            return (False, True)
        config = parse_markets_yaml(blob.download_as_bytes())
        return (config.debater.enabled, config.debater.auto_publish)
    except Exception:
        log.exception("notifier.debater_config_load_failed")
        return (False, True)


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")
    webhook_url = get_secret("discord-webhook-url")

    debater_enabled, debater_auto_publish = _load_debater_config(project_id, log)
    log.info(
        "notifier.startup",
        debater_enabled=debater_enabled,
        debater_auto_publish=debater_auto_publish,
    )

    client = bigquery.Client(project=project_id)

    select_sql = f"""
        SELECT alert_id, source, market_id, title, series_ticker, series_title,
               trade_id, trade_ts, price, size, side, notional, reason
        FROM `{project_id}.{dataset}.alerts`
        WHERE notified_at IS NULL
        ORDER BY COALESCE(series_title, title), detected_at
        LIMIT 50
    """
    rows = list(client.query(select_sql).result())

    if not rows:
        log.info("notifier.no_alerts")
        return

    log.info("notifier.pending", count=len(rows))

    # Group by (source, series_ticker_or_market_id), preserving the order of
    # first appearance so the *earliest* alert per group is the thread starter.
    # All markets in the same series share one thread.
    groups: "OrderedDict[tuple[str, str], list]" = OrderedDict()
    for row in rows:
        groups.setdefault(_thread_key(row), []).append(row)

    # Look up existing threads for these groups from the past hour so that a
    # series with rolling alerts consolidates into one ongoing thread rather
    # than spawning a new thread per cycle.
    existing_threads = _lookup_recent_threads(client, project_id, dataset, hours=1)

    sent_ids: list[str] = []
    # thread_key -> thread_id (used downstream when publishing per-market
    # debate requests; multiple markets in one series share the same thread).
    thread_id_by_group: dict[tuple[str, str], str] = {}

    for group_key, group_rows in groups.items():
        first = group_rows[0]
        thread_id: str | None = existing_threads.get(group_key)
        reused = thread_id is not None

        try:
            if reused:
                # Post directly into the existing thread; no thread_name (Discord
                # rejects thread_name + thread_id together).
                post_message(
                    webhook_url,
                    _format_message(first),
                    thread_id=thread_id,
                )
            else:
                response = post_message(
                    webhook_url,
                    _format_message(first),
                    thread_name=_thread_title(first),
                )
                # For forum-channel webhooks, response.channel_id is the new thread's ID.
                if response:
                    thread_id = str(response.get("channel_id") or response.get("id") or "") or None
            sent_ids.append(first.alert_id)
            if thread_id:
                thread_id_by_group[group_key] = thread_id
        except Exception:
            log.exception(
                "notifier.post_failed",
                alert_id=first.alert_id,
                reused_thread=reused,
            )
            # Skip follow-ups for this group if the thread starter failed.
            continue

        for follow_up in group_rows[1:]:
            try:
                post_message(
                    webhook_url,
                    _format_message(follow_up),
                    thread_id=thread_id,
                )
                sent_ids.append(follow_up.alert_id)
            except Exception:
                log.exception("notifier.post_failed", alert_id=follow_up.alert_id)

    if sent_ids:
        # Build alert_id -> thread_id map so the discord-bot can later query
        # alerts by thread_id (e.g. when /debate is invoked in a thread).
        alert_to_thread: dict[str, str] = {}
        for group_key, group_rows in groups.items():
            tid = thread_id_by_group.get(group_key)
            if not tid:
                continue
            for row in group_rows:
                if row.alert_id in sent_ids:
                    alert_to_thread[row.alert_id] = tid

        # Bucket sent_ids by their thread_id. One UPDATE per thread keeps the
        # set of values bounded (~5 distinct threads per cycle in practice).
        # Alerts with no thread_id (rare — Discord post returned no body) get
        # a final UPDATE that only stamps notified_at.
        threads_to_alerts: dict[str, list[str]] = {}
        no_thread: list[str] = []
        for aid in sent_ids:
            tid = alert_to_thread.get(aid)
            if tid:
                threads_to_alerts.setdefault(tid, []).append(aid)
            else:
                no_thread.append(aid)

        for tid, alert_ids in threads_to_alerts.items():
            client.query(
                f"""
                UPDATE `{project_id}.{dataset}.alerts`
                SET notified_at = CURRENT_TIMESTAMP(),
                    discord_thread_id = @tid
                WHERE alert_id IN UNNEST(@ids)
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("tid", "STRING", tid),
                        bigquery.ArrayQueryParameter("ids", "STRING", alert_ids),
                    ]
                ),
            ).result()

        if no_thread:
            client.query(
                f"""
                UPDATE `{project_id}.{dataset}.alerts`
                SET notified_at = CURRENT_TIMESTAMP()
                WHERE alert_id IN UNNEST(@ids)
                """,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("ids", "STRING", no_thread)
                    ]
                ),
            ).result()

        log.info("notifier.marked_sent", count=len(sent_ids))

    if debater_enabled and debater_auto_publish and thread_id_by_group:
        # Build per-market debate requests. Each unique (source, market_id)
        # triggers its own debate (each market is a distinct question), but
        # all markets within a series share a thread_id so verdicts land
        # together in the series thread.
        debate_requests: list[dict] = []
        seen_markets: set[tuple[str, str]] = set()
        for group_key, group_rows in groups.items():
            thread_id = thread_id_by_group.get(group_key)
            if not thread_id:
                continue
            for row in group_rows:
                market_key = (row.source, row.market_id)
                if market_key in seen_markets:
                    continue
                seen_markets.add(market_key)
                debate_requests.append({
                    "alert_id": row.alert_id,
                    "source": row.source,
                    "series_ticker": getattr(row, "series_ticker", None),
                    "market_id": row.market_id,
                    "title": row.title,
                    "thread_id": thread_id,
                })
        if debate_requests:
            _publish_debate_requests(project_id, debate_requests, log)


def _publish_debate_requests(project_id: str, requests: list[dict], log) -> None:
    # Lazy import: keeps google-cloud-pubsub off the import path when debater
    # is disabled, so an alert-only deploy doesn't pull or load the dep.
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, _DEBATE_TOPIC)

    published = 0
    for req in requests:
        try:
            future = publisher.publish(
                topic_path, json.dumps(req).encode("utf-8")
            )
            future.result(timeout=10)
            published += 1
        except Exception:
            log.exception(
                "notifier.publish_failed",
                source=req.get("source"),
                market_id=req.get("market_id"),
                alert_id=req.get("alert_id"),
            )

    log.info("notifier.published", count=published)


if __name__ == "__main__":
    main()

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
    # Forum-channel webhook requires a thread title on the first post. Use the
    # market title when available; fall back to a market-id-based label.
    return row.title or f"{row.source}: {row.market_id}"


def _load_debater_config(project_id: str, log) -> bool:
    bucket_name = os.environ.get("CONFIG_BUCKET", f"{project_id}-config")
    try:
        client = storage.Client(project=project_id)
        blob = client.bucket(bucket_name).blob("markets.yaml")
        if not blob.exists():
            return False
        config = parse_markets_yaml(blob.download_as_bytes())
        return config.debater.enabled
    except Exception:
        log.exception("notifier.debater_config_load_failed")
        return False


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")
    webhook_url = get_secret("discord-webhook-url")

    debater_enabled = _load_debater_config(project_id, log)
    log.info("notifier.startup", debater_enabled=debater_enabled)

    client = bigquery.Client(project=project_id)

    select_sql = f"""
        SELECT alert_id, source, market_id, title, trade_id, trade_ts,
               price, size, side, notional, reason
        FROM `{project_id}.{dataset}.alerts`
        WHERE notified_at IS NULL
        ORDER BY title, detected_at
        LIMIT 50
    """
    rows = list(client.query(select_sql).result())

    if not rows:
        log.info("notifier.no_alerts")
        return

    log.info("notifier.pending", count=len(rows))

    # Group by (source, market_id), preserving the order of first appearance
    # so the *earliest* alert per market is the thread starter.
    groups: "OrderedDict[tuple[str, str], list]" = OrderedDict()
    for row in rows:
        groups.setdefault((row.source, row.market_id), []).append(row)

    sent_ids: list[str] = []
    # market_key -> {thread_id, alert_id (first), title}
    thread_index: dict[tuple[str, str], dict] = {}

    for market_key, market_rows in groups.items():
        first = market_rows[0]
        thread_id: str | None = None
        try:
            response = post_message(
                webhook_url,
                _format_message(first),
                thread_name=_thread_title(first),
            )
            sent_ids.append(first.alert_id)
            # For forum-channel webhooks, response.channel_id is the new thread's ID.
            if response:
                thread_id = str(response.get("channel_id") or response.get("id") or "") or None
            if thread_id:
                thread_index[market_key] = {
                    "thread_id": thread_id,
                    "alert_id": first.alert_id,
                    "title": first.title,
                }
        except Exception:
            log.exception("notifier.post_failed", alert_id=first.alert_id)
            # Skip follow-ups for this market if the thread starter failed.
            continue

        for follow_up in market_rows[1:]:
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
        update_sql = f"""
            UPDATE `{project_id}.{dataset}.alerts`
            SET notified_at = CURRENT_TIMESTAMP()
            WHERE alert_id IN UNNEST(@ids)
        """
        job = client.query(
            update_sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("ids", "STRING", sent_ids)
                ]
            ),
        )
        job.result()
        log.info("notifier.marked_sent", count=len(sent_ids))

    if debater_enabled and thread_index:
        _publish_debate_requests(project_id, thread_index, log)


def _publish_debate_requests(project_id: str, thread_index: dict, log) -> None:
    # Lazy import: keeps google-cloud-pubsub off the import path when debater
    # is disabled, so an alert-only deploy doesn't pull or load the dep.
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, _DEBATE_TOPIC)

    published = 0
    for (source, market_id), info in thread_index.items():
        payload = {
            "alert_id": info["alert_id"],
            "source": source,
            "market_id": market_id,
            "title": info.get("title"),
            "thread_id": info["thread_id"],
        }
        try:
            future = publisher.publish(
                topic_path, json.dumps(payload).encode("utf-8")
            )
            future.result(timeout=10)
            published += 1
        except Exception:
            log.exception(
                "notifier.publish_failed",
                source=source, market_id=market_id, alert_id=info["alert_id"],
            )

    log.info("notifier.published", count=published)


if __name__ == "__main__":
    main()

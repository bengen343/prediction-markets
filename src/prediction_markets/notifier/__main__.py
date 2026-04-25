import os

from google.cloud import bigquery

from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id, get_secret
from .discord import post_message


# Discord user(s) to ping on each alert. Override via env var if needed.
_DISCORD_MENTION = os.environ.get("DISCORD_MENTION", "<@746817328242491532>")


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


def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")
    webhook_url = get_secret("discord-webhook-url")

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

    sent_ids: list[str] = []
    for row in rows:
        try:
            post_message(webhook_url, _format_message(row))
            sent_ids.append(row.alert_id)
        except Exception:
            log.exception("notifier.post_failed", alert_id=row.alert_id)

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


if __name__ == "__main__":
    main()

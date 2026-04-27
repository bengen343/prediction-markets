import json
import os
import uuid

from flask import jsonify
from google.cloud import bigquery, pubsub_v1

from ..shared.secrets import get_project_id

_DEBATE_TOPIC = "debate-requests"

# Discord interaction-response flags. EPHEMERAL (1<<6) makes a response
# visible only to the user who invoked the command.
_FLAG_EPHEMERAL = 64


def _ephemeral(content: str) -> dict:
    return {"type": 4, "data": {"content": content, "flags": _FLAG_EPHEMERAL}}


def _public(content: str) -> dict:
    return {"type": 4, "data": {"content": content}}


def handle_debate_command(payload: dict, log) -> dict:
    options = {
        opt["name"]: opt.get("value")
        for opt in (payload.get("data", {}).get("options") or [])
    }
    custom_question = (options.get("question") or "").strip() or None

    # Discord channel_id is the thread_id when a command is issued in a thread.
    channel_id = payload.get("channel_id")
    member = payload.get("member") or {}
    user = member.get("user") or payload.get("user") or {}
    user_label = (
        user.get("global_name") or user.get("username") or user.get("id") or "?"
    )

    project_id = get_project_id()
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")

    if custom_question:
        # Synthetic identifiers — unique per invocation so the worker's cache
        # always misses for custom questions (different prompt, no reuse).
        synthetic = uuid.uuid4().hex[:12]
        alert_id = f"manual-{synthetic}"
        source = "manual"
        series_ticker = None
        market_id = f"manual-{synthetic}"
        question = custom_question
        ack = f"Debate queued by **{user_label}** — _{question}_"
    else:
        alert = _lookup_thread_alert(project_id, dataset, channel_id)
        if not alert:
            return jsonify(_ephemeral(
                "No alert found in this thread. Either invoke `/debate` "
                "inside an alert thread, or pass `question:\"...\"` to debate "
                "a custom question."
            ))
        alert_id = alert["alert_id"]
        source = alert["source"]
        series_ticker = alert.get("series_ticker")
        market_id = alert["market_id"]
        question = alert["title"]
        ack = (
            f"Debate queued by **{user_label}** for the most recent alert in "
            f"this thread — _{question}_"
        )

    msg = {
        "alert_id": alert_id,
        "source": source,
        "series_ticker": series_ticker,
        "market_id": market_id,
        "title": question,
        "thread_id": str(channel_id) if channel_id else None,
    }

    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, _DEBATE_TOPIC)
        future = publisher.publish(topic_path, json.dumps(msg).encode("utf-8"))
        future.result(timeout=10)
        log.info(
            "discord_bot.published",
            alert_id=alert_id, market_id=market_id, user=user_label,
            mode="custom" if custom_question else "thread_default",
        )
    except Exception:
        log.exception("discord_bot.publish_failed", alert_id=alert_id)
        return jsonify(_ephemeral(
            "Failed to enqueue debate (Pub/Sub publish error). Check logs."
        ))

    return jsonify(_public(ack))


def _lookup_thread_alert(project_id: str, dataset: str, thread_id) -> dict | None:
    if not thread_id:
        return None
    client = bigquery.Client(project=project_id)
    sql = f"""
        SELECT alert_id, source, series_ticker, market_id, title
        FROM `{project_id}.{dataset}.alerts`
        WHERE discord_thread_id = @tid
        ORDER BY detected_at DESC
        LIMIT 1
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tid", "STRING", str(thread_id))
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    r = rows[0]
    return {
        "alert_id": r.alert_id,
        "source": r.source,
        "series_ticker": r.series_ticker,
        "market_id": r.market_id,
        "title": r.title,
    }

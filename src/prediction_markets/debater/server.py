import base64
import json
import os
import uuid

from flask import Flask, request
from google.cloud import bigquery

from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id, get_secret
from .bq_writer import utc_now_iso, write_debate_row
from .cache import find_cached_consensus
from .debate import load_api_keys, run_debate
from .verdict_post import post_transcript, post_verdict

configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = get_logger(__name__)

app = Flask(__name__)


def _bucket_name(project_id: str) -> str:
    return os.environ.get("CONFIG_BUCKET", f"{project_id}-config")


def _dataset() -> str:
    return os.environ.get("BQ_DATASET", "prediction_markets")


@app.route("/health", methods=["GET"])
def health() -> tuple[str, int]:
    return ("ok", 200)


@app.route("/", methods=["POST"])
def handle_pubsub_push() -> tuple[str, int]:
    envelope = request.get_json(silent=True)
    if not envelope or "message" not in envelope:
        log.warning("debater.bad_envelope")
        return ("bad request", 400)

    message = envelope["message"]
    try:
        data = json.loads(base64.b64decode(message["data"]).decode("utf-8"))
    except Exception:
        log.exception("debater.bad_message_data")
        return ("bad message", 400)

    alert_id = data.get("alert_id")
    source = data.get("source")
    series_ticker = data.get("series_ticker")
    market_id = data.get("market_id")
    title = data.get("title")
    thread_id = data.get("thread_id")

    if not (alert_id and source and market_id and title):
        log.warning("debater.missing_fields", payload=data)
        # Ack so Pub/Sub doesn't keep redelivering a malformed message.
        return ("", 204)

    project_id = get_project_id()
    dataset = _dataset()
    bucket = _bucket_name(project_id)
    bq = bigquery.Client(project=project_id)
    webhook_url = get_secret("discord-webhook-url")

    # Cache check: re-serve recent consensus rather than re-debating.
    try:
        cached = find_cached_consensus(
            bq, project_id, dataset, source, series_ticker, market_id, hours=3,
        )
    except Exception:
        log.exception(
            "debater.cache_lookup_failed",
            source=source, series_ticker=series_ticker, market_id=market_id,
        )
        cached = None

    if cached:
        log.info(
            "debater.cache_hit",
            source=source, series_ticker=series_ticker, market_id=market_id,
            source_debate_id=cached["debate_id"],
            source_market_id=cached.get("source_market_id"),
        )
        _post_and_record_cached(
            bq=bq, project_id=project_id, dataset=dataset,
            webhook_url=webhook_url, thread_id=thread_id,
            alert_id=alert_id, source=source, series_ticker=series_ticker,
            market_id=market_id, title=title, cached=cached,
        )
        return ("", 204)

    log.info("debater.run_start", source=source, market_id=market_id, alert_id=alert_id)
    api_keys = load_api_keys()

    started_at = utc_now_iso()
    output = run_debate(question=title, bucket=bucket, api_keys=api_keys)
    finished_at = utc_now_iso()

    log.info(
        "debater.run_complete",
        debate_id=output.debate_id,
        outcome=output.outcome,
        turns=output.turn_count,
        cost_usd=round(output.total_cost_usd, 4),
    )

    # Persist BQ row first so the verdict is recorded even if the Discord post
    # fails (e.g., webhook rate limit).
    try:
        write_debate_row(bq, project_id, dataset, {
            "debate_id": output.debate_id,
            "alert_id": alert_id,
            "source": source,
            "series_ticker": series_ticker,
            "market_id": market_id,
            "title": title,
            "started_at": started_at,
            "finished_at": finished_at,
            "outcome": output.outcome,
            "verdict": output.verdict,
            "turn_count": output.turn_count,
            "total_cost_usd": output.total_cost_usd,
            "cost_by_provider": output.cost_by_provider,
            "transcript_gcs_uri": output.transcript_gcs_uri,
            "discord_thread_id": thread_id,
            "source_debate_id": None,
        })
    except Exception:
        log.exception("debater.bq_write_failed", debate_id=output.debate_id)

    if thread_id:
        try:
            post_transcript(webhook_url, thread_id, output.full_transcript)
        except Exception:
            log.exception("debater.transcript_post_failed", debate_id=output.debate_id)
        try:
            post_verdict(webhook_url, thread_id, output.verdict, output.outcome)
        except Exception:
            log.exception("debater.verdict_post_failed", debate_id=output.debate_id)

    return ("", 204)


def _post_and_record_cached(
    *,
    bq, project_id, dataset, webhook_url, thread_id,
    alert_id, source, series_ticker, market_id, title, cached,
) -> None:
    finished_str = ""
    finished_at = cached.get("finished_at")
    if finished_at:
        try:
            finished_str = finished_at.strftime("%H:%M UTC")
        except Exception:
            finished_str = str(finished_at)

    # Surface the original market when a series-cached verdict is being reused
    # for a *different* market in the same series — readers should know which
    # specific market the verdict was actually debated against.
    cached_from_label = finished_str or "earlier"
    source_title = cached.get("source_title")
    source_market_id = cached.get("source_market_id")
    if source_market_id and source_market_id != market_id and source_title:
        cached_from_label = f"{cached_from_label}; originally debated for: _{source_title}_"

    if thread_id:
        try:
            post_verdict(
                webhook_url, thread_id, cached.get("verdict"),
                outcome="consensus", cached_from=cached_from_label,
            )
        except Exception:
            log.exception("debater.cached_verdict_post_failed")

    started_at = utc_now_iso()
    try:
        write_debate_row(bq, project_id, dataset, {
            "debate_id": uuid.uuid4().hex,
            "alert_id": alert_id,
            "source": source,
            "series_ticker": series_ticker,
            "market_id": market_id,
            "title": title,
            "started_at": started_at,
            "finished_at": started_at,
            "outcome": "cached",
            "verdict": cached.get("verdict"),
            "turn_count": 0,
            "total_cost_usd": 0.0,
            "cost_by_provider": {},
            "transcript_gcs_uri": cached.get("transcript_gcs_uri"),
            "discord_thread_id": thread_id,
            "source_debate_id": cached["debate_id"],
        })
    except Exception:
        log.exception("debater.cache_row_write_failed")

import json
import os

from flask import Flask, jsonify, request

from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_secret
from .handlers import handle_debate_command
from .signing import verify_signature

configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = get_logger(__name__)

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health() -> tuple[str, int]:
    return ("ok", 200)


@app.route("/interaction", methods=["POST"])
def interaction():
    sig = request.headers.get("X-Signature-Ed25519")
    ts = request.headers.get("X-Signature-Timestamp")
    body = request.get_data()
    if not sig or not ts:
        return ("missing signature headers", 401)

    try:
        public_key = get_secret("discord-bot-public-key")
    except Exception:
        log.exception("discord_bot.public_key_fetch_failed")
        return ("server misconfigured", 500)

    if not verify_signature(public_key, sig, ts, body):
        # Discord probes this exact path with bad signatures during URL
        # registration to confirm we reject unauthenticated requests.
        return ("invalid signature", 401)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return ("bad json", 400)

    itype = payload.get("type")

    # PING — Discord uses this to validate the endpoint URL. Must echo a PONG.
    if itype == 1:
        return jsonify({"type": 1})

    # APPLICATION_COMMAND — slash command was invoked.
    if itype == 2:
        cmd_name = (payload.get("data") or {}).get("name")
        if cmd_name == "debate":
            return handle_debate_command(payload, log)
        return jsonify({
            "type": 4,
            "data": {"content": f"Unknown command: {cmd_name}", "flags": 64},
        })

    # Other types (component interactions, autocomplete, modal submits) we
    # don't currently use — ack with a no-op so Discord doesn't retry.
    log.info("discord_bot.unhandled_interaction_type", itype=itype)
    return jsonify({"type": 4, "data": {"content": "Unsupported interaction.", "flags": 64}})

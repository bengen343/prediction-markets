import json
import urllib.request

# Discord rejects requests using Python's default urllib User-Agent (blocklisted
# years ago to discourage scrapers). Any custom UA is fine.
_USER_AGENT = "prediction-markets-notifier/0.1"


def post_message(webhook_url: str, content: str, timeout: float = 10.0) -> None:
    # allowed_mentions parse:["users"] lets <@USER_ID> in content trigger a real
    # ping. Without this, Discord renders the mention but suppresses the
    # notification (default webhook behavior to prevent spam).
    payload = json.dumps({
        "content": content,
        "allowed_mentions": {"parse": ["users"]},
    }).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        if response.status >= 400:
            raise RuntimeError(f"Discord responded with status {response.status}")

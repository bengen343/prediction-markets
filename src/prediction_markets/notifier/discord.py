import json
import urllib.parse
import urllib.request
from typing import Any

# Discord rejects requests using Python's default urllib User-Agent (blocklisted
# years ago to discourage scrapers). Any custom UA is fine.
_USER_AGENT = "prediction-markets-notifier/0.1"


def post_message(
    webhook_url: str,
    content: str,
    thread_name: str | None = None,
    thread_id: str | None = None,
    timeout: float = 10.0,
) -> dict[str, Any] | None:
    # allowed_mentions parse:["users"] lets <@USER_ID> in content trigger a real
    # ping. Without this, Discord renders the mention but suppresses the
    # notification (default webhook behavior to prevent spam).
    body: dict[str, Any] = {
        "content": content,
        "allowed_mentions": {"parse": ["users"]},
    }
    # thread_name: forum-channel webhooks require this on the first post in a
    # new thread. Discord caps the title at 100 chars.
    # thread_id: query-string param that lands the post in an existing thread.
    if thread_name and not thread_id:
        body["thread_name"] = thread_name[:100]

    url = webhook_url
    params = {"wait": "true"}
    if thread_id:
        params["thread_id"] = str(thread_id)
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}{urllib.parse.urlencode(params)}"

    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
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
        raw = response.read()
    if not raw:
        return None
    return json.loads(raw)

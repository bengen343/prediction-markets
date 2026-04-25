import json
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import Any

REST_BASE = "https://gamma-api.polymarket.com"

# Default Python urllib UA (Python-urllib/3.x) is blocklisted on some CDN edges -
# we hit this with Discord. Polymarket hasn't blocked us in testing but a custom
# UA is cheap defense.
_USER_AGENT = "prediction-markets-collector/1.0"


def get(path: str, params: dict[str, Any] | None = None, timeout: float = 30.0) -> Any:
    url = REST_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT}, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def get_keyset_paginated(
    path: str,
    items_key: str = "events",
    params: dict[str, Any] | None = None,
) -> Iterator[dict]:
    """Paginate Gamma API keyset endpoints (e.g. /events/keyset).

    Stops when next_cursor is missing, empty, or the LTE= end-sentinel.
    """
    page_params: dict[str, Any] = dict(params or {})
    while True:
        data = get(path, page_params)
        for item in data.get(items_key, []) or []:
            yield item
        cursor = data.get("next_cursor")
        if not cursor or cursor == "LTE=":
            return
        page_params["after_cursor"] = cursor

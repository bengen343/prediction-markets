import json
import urllib.parse
import urllib.request
from collections.abc import Iterator
from typing import Any

from cryptography.hazmat.primitives.asymmetric import rsa

from .auth import build_headers

REST_BASE = "https://api.elections.kalshi.com"


def get(
    private_key: rsa.RSAPrivateKey,
    access_key_id: str,
    path: str,
    params: dict[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict:
    url = REST_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = build_headers(private_key, access_key_id, "GET", path)
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read())


def get_paginated(
    private_key: rsa.RSAPrivateKey,
    access_key_id: str,
    path: str,
    items_key: str,
    params: dict[str, Any] | None = None,
) -> Iterator[dict]:
    page_params: dict[str, Any] = dict(params or {})
    while True:
        data = get(private_key, access_key_id, path, page_params)
        for item in data.get(items_key, []) or []:
            yield item
        cursor = data.get("cursor")
        if not cursor:
            return
        page_params["cursor"] = cursor

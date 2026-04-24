import os
import urllib.request
from functools import lru_cache

from google.cloud import secretmanager

_client: secretmanager.SecretManagerServiceClient | None = None


def _get_client() -> secretmanager.SecretManagerServiceClient:
    global _client
    if _client is None:
        _client = secretmanager.SecretManagerServiceClient()
    return _client


@lru_cache(maxsize=1)
def get_project_id() -> str:
    env = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if env:
        return env
    req = urllib.request.Request(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"},
    )
    with urllib.request.urlopen(req, timeout=2) as r:
        return r.read().decode("utf-8")


# Cached for process lifetime. Rotating a secret requires restarting the process;
# acceptable here since secrets change rarely and we don't want to hit Secret Manager
# on every request.
@lru_cache(maxsize=32)
def get_secret_bytes(name: str) -> bytes:
    path = f"projects/{get_project_id()}/secrets/{name}/versions/latest"
    resp = _get_client().access_secret_version(name=path)
    return resp.payload.data


def get_secret(name: str) -> str:
    return get_secret_bytes(name).decode("utf-8").strip()

import yaml


def read_subscriptions(bucket) -> dict:
    """Load the current subscriptions.yaml from GCS, or {} if not yet written."""
    blob = bucket.blob("subscriptions.yaml")
    if not blob.exists():
        return {}
    return yaml.safe_load(blob.download_as_bytes()) or {}


def write_subscriptions_for_source(bucket, source: str, payload: dict) -> None:
    """Read-modify-write of subscriptions.yaml: replaces only the subkey for
    the given source, leaving sibling sources intact. Resolvers must use this
    helper rather than overwriting the file wholesale, otherwise each one
    would clobber the others' subscription lists.
    """
    current = read_subscriptions(bucket)
    current[source] = payload
    bucket.blob("subscriptions.yaml").upload_from_string(
        yaml.safe_dump(current, default_flow_style=False, sort_keys=True),
        content_type="text/x-yaml",
    )

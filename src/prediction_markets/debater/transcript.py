import json
from datetime import UTC, datetime
from typing import Any

from google.cloud import storage


class TranscriptWriter:
    """Buffers debate turns in memory, flushes the JSONL transcript to GCS at end."""

    def __init__(self, bucket: str, debate_id: str):
        self.bucket = bucket
        self.debate_id = debate_id
        self._lines: list[str] = []

    def append(self, record: dict[str, Any]) -> None:
        record = {"ts": datetime.now(UTC).isoformat(), **record}
        self._lines.append(json.dumps(record, default=str))

    def flush(self) -> str:
        client = storage.Client()
        path = f"debates/{self.debate_id}/transcript.jsonl"
        blob = client.bucket(self.bucket).blob(path)
        blob.upload_from_string("\n".join(self._lines), content_type="application/jsonl")
        return f"gs://{self.bucket}/{path}"

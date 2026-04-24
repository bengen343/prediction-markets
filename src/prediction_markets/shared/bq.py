import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from google.cloud import bigquery

from .log import get_logger

log = get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class TradeRow:
    source: str
    market_id: str
    trade_id: str
    ts: str
    price: float | None = None
    size: int | None = None
    side: str | None = None
    raw: dict[str, Any] | None = None
    # Populated at row-creation time because BQ load jobs (unlike DML INSERT)
    # do not apply column DEFAULT clauses - we must provide every NOT NULL field.
    ingested_at: str = field(default_factory=_utc_now_iso)


class BqWriter:
    def __init__(
        self,
        project_id: str,
        dataset: str,
        table: str = "trades",
        max_batch: int = 500,
        max_delay_seconds: float = 300.0,
    ):
        self._client = bigquery.Client(project=project_id)
        self._table_ref = f"{project_id}.{dataset}.{table}"
        self._max_batch = max_batch
        self._max_delay = max_delay_seconds
        self._buffer: list[TradeRow] = []
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.flush()

    async def write(self, row: TradeRow) -> None:
        async with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self._max_batch:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return
        batch, self._buffer = self._buffer, []
        rows = [
            {
                "source": r.source,
                "market_id": r.market_id,
                "trade_id": r.trade_id,
                "ts": r.ts,
                "price": r.price,
                "size": r.size,
                "side": r.side,
                "raw": r.raw,
                "ingested_at": r.ingested_at,
            }
            for r in batch
        ]
        try:
            job = await asyncio.to_thread(self._load, rows)
            log.info("bq.loaded", rows=len(rows), job_id=job.job_id)
        except Exception:
            log.exception("bq.load_failed", rows=len(rows))

    def _load(self, rows: list[dict[str, Any]]):
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = self._client.load_table_from_json(rows, self._table_ref, job_config=config)
        job.result(timeout=120)
        return job

    async def _flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._max_delay)
                await self.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("bq.flush_loop_error")

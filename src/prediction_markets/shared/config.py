import asyncio
from dataclasses import dataclass

import yaml
from google.cloud import storage

from .log import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class KalshiConfig:
    tickers: tuple[str, ...] = ()
    categories: tuple[str, ...] = ()


@dataclass(frozen=True)
class PolymarketSubscription:
    market_id: str
    yes_token_id: str
    no_token_id: str


@dataclass(frozen=True)
class PolymarketConfig:
    markets: tuple[PolymarketSubscription, ...] = ()


@dataclass(frozen=True)
class MarketsConfig:
    kalshi: KalshiConfig = KalshiConfig()
    polymarket: PolymarketConfig = PolymarketConfig()


def _parse(raw: bytes) -> MarketsConfig:
    data = yaml.safe_load(raw) or {}
    k = data.get("kalshi") or {}
    p = data.get("polymarket") or {}
    return MarketsConfig(
        kalshi=KalshiConfig(
            tickers=tuple(k.get("tickers") or []),
            categories=tuple(k.get("categories") or []),
        ),
        polymarket=PolymarketConfig(
            markets=tuple(
                PolymarketSubscription(
                    market_id=str(m["market_id"]),
                    yes_token_id=str(m["yes_token_id"]),
                    no_token_id=str(m["no_token_id"]),
                )
                for m in (p.get("markets") or [])
                if isinstance(m, dict) and m.get("market_id")
                and m.get("yes_token_id") and m.get("no_token_id")
            ),
        ),
    )


def _fetch(bucket: str, path: str) -> MarketsConfig:
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    return _parse(blob.download_as_bytes())


class ConfigWatcher:
    def __init__(self, bucket: str, path: str = "markets.yaml", refresh_seconds: int = 60):
        self.bucket = bucket
        self.path = path
        self.refresh_seconds = refresh_seconds
        self._current: MarketsConfig | None = None
        self._task: asyncio.Task | None = None

    @property
    def current(self) -> MarketsConfig:
        if self._current is None:
            raise RuntimeError("ConfigWatcher not started")
        return self._current

    async def start(self) -> None:
        self._current = await asyncio.to_thread(_fetch, self.bucket, self.path)
        log.info(
            "config.loaded",
            kalshi_tickers=len(self._current.kalshi.tickers),
            kalshi_categories=len(self._current.kalshi.categories),
            polymarket_markets=len(self._current.polymarket.markets),
        )
        self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _refresh_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.refresh_seconds)
                new = await asyncio.to_thread(_fetch, self.bucket, self.path)
                if new != self._current:
                    log.info(
                        "config.changed",
                        kalshi_tickers=len(new.kalshi.tickers),
                        kalshi_categories=len(new.kalshi.categories),
                        polymarket_markets=len(new.polymarket.markets),
                    )
                    self._current = new
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("config.refresh_failed")

import asyncio
import json
import random
from datetime import UTC, datetime

import websockets
from cryptography.hazmat.primitives.asymmetric import rsa

from ..shared.bq import BqWriter, TradeRow
from ..shared.config import ConfigWatcher
from ..shared.log import get_logger
from .auth import build_headers

log = get_logger(__name__)

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
WS_SIGN_PATH = "/trade-api/ws/v2"


class KalshiWebsocketClient:
    def __init__(
        self,
        watcher: ConfigWatcher,
        writer: BqWriter,
        private_key: rsa.RSAPrivateKey,
        access_key_id: str,
    ):
        self.watcher = watcher
        self.writer = writer
        self.private_key = private_key
        self.access_key_id = access_key_id
        self._cmd_id = 0
        self._stop: asyncio.Event | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        self._stop = stop_event
        backoff = 1.0
        while not stop_event.is_set():
            tickers = list(self.watcher.current.kalshi.tickers)
            if not tickers:
                log.info("kalshi.no_tickers_waiting")
                await self._sleep_or_stop(10)
                continue
            try:
                await self._session(tickers)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("kalshi.session_error")
                wait = min(backoff, 60.0) + random.uniform(0, 1)
                log.info("kalshi.reconnect", wait=round(wait, 2))
                await self._sleep_or_stop(wait)
                backoff = min(backoff * 2, 60.0)

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _session(self, tickers: list[str]) -> None:
        headers = build_headers(self.private_key, self.access_key_id, "GET", WS_SIGN_PATH)
        log.info("kalshi.connecting", url=WS_URL, tickers=len(tickers))
        async with websockets.connect(WS_URL, additional_headers=headers) as ws:
            await self._subscribe(ws, tickers)
            subscribed = set(tickers)
            reader = asyncio.create_task(self._read_loop(ws))
            config_task = asyncio.create_task(self._config_change_watcher(subscribed))
            stop_task = asyncio.create_task(self._stop.wait())
            try:
                done, pending = await asyncio.wait(
                    [reader, config_task, stop_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for t in pending:
                    t.cancel()
                for t in done:
                    if t.cancelled():
                        continue
                    exc = t.exception()
                    if exc:
                        raise exc
            finally:
                for t in (reader, config_task, stop_task):
                    if not t.done():
                        t.cancel()

    def _next_id(self) -> int:
        self._cmd_id += 1
        return self._cmd_id

    async def _subscribe(self, ws, tickers: list[str]) -> None:
        msg = {
            "id": self._next_id(),
            "cmd": "subscribe",
            "params": {"channels": ["trade"], "market_tickers": tickers},
        }
        await ws.send(json.dumps(msg))
        log.info("kalshi.subscribed", tickers=len(tickers))

    async def _read_loop(self, ws) -> None:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("kalshi.bad_json")
                continue
            msg_type = msg.get("type")
            if msg_type == "trade":
                trade = _parse_trade(msg.get("msg") or {})
                if trade:
                    await self.writer.write(trade)
            elif msg_type == "error":
                log.error("kalshi.server_error", msg=msg)
            else:
                log.debug("kalshi.msg", type=msg_type)

    async def _config_change_watcher(self, subscribed: set[str]) -> None:
        # Reconnecting on ticker changes is simpler than sending unsubscribe/subscribe
        # deltas - config changes are rare and reconnect completes in under a second.
        while True:
            await asyncio.sleep(10)
            desired = set(self.watcher.current.kalshi.tickers)
            if desired != subscribed:
                log.info(
                    "kalshi.tickers_changed",
                    added=len(desired - subscribed),
                    removed=len(subscribed - desired),
                )
                return


def _parse_trade(msg: dict) -> TradeRow | None:
    try:
        iso_ts = datetime.fromtimestamp(msg["ts_ms"] / 1000, tz=UTC).isoformat()
        return TradeRow(
            source="kalshi",
            market_id=msg["market_ticker"],
            trade_id=msg["trade_id"],
            ts=iso_ts,
            price=float(msg["yes_price_dollars"]),
            size=int(float(msg["count_fp"])),
            side=msg.get("taker_side"),
            raw=msg,
        )
    except (KeyError, ValueError, TypeError):
        log.exception("kalshi.trade_parse_failed", keys=list(msg.keys()) if msg else None)
        return None

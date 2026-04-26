import asyncio
import json
import os
import random
from datetime import UTC, datetime

import websockets

from ..shared.bq import BqWriter, TradeRow
from ..shared.config import ConfigWatcher, PolymarketSubscription
from ..shared.log import get_logger

log = get_logger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Polymarket requires the client to send "PING" (plain text, not JSON) every
# 10s; server replies "PONG". Connection is dropped silently if we miss it.
PING_INTERVAL_SECONDS = 10.0

# Polymarket's WS server silently drops connections when the subscribe payload
# is too large. ~31k asset_ids in one frame is ~2.6 MB of JSON and the server
# disconnects with no close frame after ~3 minutes (apparently after some
# internal queue/processing timeout). Sharding into smaller batches keeps
# each subscribe under ~100 KB and routes fine. Tunable via env for future
# ops dialing.
SUBSCRIBE_BATCH_SIZE = int(os.environ.get("POLYMARKET_BATCH_SIZE", "1000"))

# Stagger handshakes across connections. Opening 30+ SSL/WS handshakes in
# parallel from one IP causes handshake timeouts - either Polymarket's edge
# throttles concurrent connects, or our e2-micro CPU saturates on parallel
# SSL math. 250ms gap means ~4 connects/sec.
CONNECTION_OPEN_STAGGER_SECONDS = float(
    os.environ.get("POLYMARKET_CONNECT_STAGGER", "0.25")
)
# Default open_timeout=10s isn't enough under contention.
CONNECTION_OPEN_TIMEOUT_SECONDS = 30.0


class PolymarketWebsocketClient:
    def __init__(self, watcher: ConfigWatcher, writer: BqWriter):
        self.watcher = watcher
        self.writer = writer
        self._stop: asyncio.Event | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        self._stop = stop_event
        backoff = 1.0
        while not stop_event.is_set():
            markets = list(self.watcher.current.polymarket.markets)
            if not markets:
                log.info("polymarket.no_markets_waiting")
                await self._sleep_or_stop(10)
                continue
            try:
                await self._session(markets)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("polymarket.session_error")
                wait = min(backoff, 60.0) + random.uniform(0, 1)
                log.info("polymarket.reconnect", wait=round(wait, 2))
                await self._sleep_or_stop(wait)
                backoff = min(backoff * 2, 60.0)

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def _session(self, markets: list[PolymarketSubscription]) -> None:
        # Map asset_id -> (market_id, side) for trade routing in the read loop.
        # Side is "yes"/"no" (which outcome was traded), matching Kalshi's
        # taker_side convention. The raw BUY/SELL taker direction lives in raw.
        asset_id_to_market: dict[str, tuple[str, str]] = {}
        for m in markets:
            asset_id_to_market[m.yes_token_id] = (m.market_id, "yes")
            asset_id_to_market[m.no_token_id] = (m.market_id, "no")
        all_assets = list(asset_id_to_market.keys())

        # Shard into N parallel connections; one too-large subscribe payload
        # gets us silently dropped (see SUBSCRIBE_BATCH_SIZE comment).
        batches = [
            all_assets[i:i + SUBSCRIBE_BATCH_SIZE]
            for i in range(0, len(all_assets), SUBSCRIBE_BATCH_SIZE)
        ]
        log.info(
            "polymarket.connecting",
            url=WS_URL, markets=len(markets), assets=len(all_assets),
            batches=len(batches), batch_size=SUBSCRIBE_BATCH_SIZE,
        )

        subscribed = {m.market_id for m in markets}
        # Each connection has its own retry loop - a single drop doesn't
        # tear down the other 31 healthy connections. _session itself only
        # exits on stop or config change.
        connection_tasks = [
            asyncio.create_task(self._connection_loop(idx, batch, asset_id_to_market))
            for idx, batch in enumerate(batches)
        ]
        config_task = asyncio.create_task(self._config_change_watcher(subscribed))
        stop_task = asyncio.create_task(self._stop.wait())
        all_tasks = connection_tasks + [config_task, stop_task]

        try:
            done, pending = await asyncio.wait(
                [config_task, stop_task], return_when=asyncio.FIRST_COMPLETED,
            )
            for t in done:
                if t.cancelled():
                    continue
                exc = t.exception()
                if exc:
                    raise exc
        finally:
            for t in all_tasks:
                if not t.done():
                    t.cancel()
            # Let cancellations propagate so we don't leak sockets.
            await asyncio.gather(*all_tasks, return_exceptions=True)

    async def _connection_loop(
        self,
        idx: int,
        asset_ids: list[str],
        asset_id_to_market: dict[str, tuple[str, str]],
    ) -> None:
        """Per-connection retry loop. Stagger only on the first attempt so
        the initial fan-out spreads out, but reconnects fire on their own
        backoff (which already has random jitter so 32 simultaneous drops
        wouldn't all reconnect in lockstep)."""
        if idx > 0:
            await asyncio.sleep(idx * CONNECTION_OPEN_STAGGER_SECONDS)
        backoff = 1.0
        while True:
            try:
                await self._connection(idx, asset_ids, asset_id_to_market)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning(
                    "polymarket.connection_dropped",
                    idx=idx, error_type=type(e).__name__, error=str(e)[:200],
                )
                wait = min(backoff, 60.0) + random.uniform(0, 1)
                log.info("polymarket.connection_reconnect", idx=idx, wait=round(wait, 2))
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 60.0)

    async def _connection(
        self,
        idx: int,
        asset_ids: list[str],
        asset_id_to_market: dict[str, tuple[str, str]],
    ) -> None:
        log.info("polymarket.connection.opening", idx=idx, assets=len(asset_ids))
        # ping_interval=None disables the websockets library's WS-protocol-level
        # control-frame ping. Polymarket only documents the plain-text "PING"
        # heartbeat (which we send ourselves at the application level), and we
        # don't want two competing keepalive mechanisms confusing the server.
        async with websockets.connect(
            WS_URL,
            ping_interval=None,
            open_timeout=CONNECTION_OPEN_TIMEOUT_SECONDS,
        ) as ws:
            await self._subscribe(ws, asset_ids, idx)
            reader = asyncio.create_task(self._read_loop(ws, asset_id_to_market, idx))
            ping = asyncio.create_task(self._ping_loop(ws))
            try:
                done, pending = await asyncio.wait(
                    [reader, ping], return_when=asyncio.FIRST_COMPLETED,
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
                for t in (reader, ping):
                    if not t.done():
                        t.cancel()

    async def _subscribe(self, ws, asset_ids: list[str], idx: int) -> None:
        # initial_dump=False skips the orderbook snapshot - we only care about
        # last_trade_price events.
        msg = {
            "type": "market",
            "assets_ids": asset_ids,
            "initial_dump": False,
        }
        await ws.send(json.dumps(msg))
        log.info("polymarket.subscribed", idx=idx, assets=len(asset_ids))

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            await ws.send("PING")

    async def _read_loop(
        self,
        ws,
        asset_id_to_market: dict[str, tuple[str, str]],
        idx: int,
    ) -> None:
        async for raw in ws:
            if raw == "PONG":
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("polymarket.bad_json", idx=idx)
                continue

            # Server may batch multiple events into one frame as a JSON array.
            events = data if isinstance(data, list) else [data]
            for ev in events:
                if not isinstance(ev, dict):
                    continue
                event_type = ev.get("event_type")
                if event_type == "last_trade_price":
                    trade = _parse_trade(ev, asset_id_to_market)
                    if trade:
                        await self.writer.write(trade)
                elif event_type in (None, "book", "price_change", "tick_size_change"):
                    # We only care about trades; ignore orderbook + tick noise.
                    continue
                else:
                    log.debug("polymarket.unknown_event", idx=idx, event_type=event_type)

    async def _config_change_watcher(self, subscribed_market_ids: set[str]) -> None:
        # Reconnect on subscription changes rather than diffing & sending
        # subscribe/unsubscribe ops - config changes are rare (daily resolver)
        # and a reconnect completes in well under a second.
        while True:
            await asyncio.sleep(10)
            desired = {m.market_id for m in self.watcher.current.polymarket.markets}
            if desired != subscribed_market_ids:
                log.info(
                    "polymarket.markets_changed",
                    added=len(desired - subscribed_market_ids),
                    removed=len(subscribed_market_ids - desired),
                )
                return


def _parse_trade(
    msg: dict,
    asset_id_to_market: dict[str, tuple[str, str]],
) -> TradeRow | None:
    asset_id = msg.get("asset_id")
    routing = asset_id_to_market.get(asset_id) if asset_id else None
    if routing is None:
        # Trade for a token we never subscribed to - shouldn't happen, but
        # could occur briefly during a reconnect race after a subscription
        # change. Skip silently to avoid log noise.
        return None
    market_id, side = routing
    try:
        ts_ms = int(msg["timestamp"])
        return TradeRow(
            source="polymarket",
            market_id=market_id,
            trade_id=msg["transaction_hash"],
            ts=datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat(),
            price=float(msg["price"]),
            size=float(msg["size"]),
            side=side,
            raw=msg,
        )
    except (KeyError, ValueError, TypeError):
        log.exception("polymarket.trade_parse_failed", keys=list(msg.keys()) if msg else None)
        return None

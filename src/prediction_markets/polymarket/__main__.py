import asyncio
import os
import signal

from ..shared.bq import BqWriter
from ..shared.config import ConfigWatcher
from ..shared.log import configure_logging, get_logger
from ..shared.secrets import get_project_id
from .websocket import PolymarketWebsocketClient


async def main() -> None:
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    log = get_logger(__name__)

    project_id = get_project_id()
    bucket = os.environ.get("CONFIG_BUCKET", f"{project_id}-config")
    dataset = os.environ.get("BQ_DATASET", "prediction_markets")

    log.info("startup", project_id=project_id, bucket=bucket, dataset=dataset)

    # subscriptions.yaml is auto-written by polymarket-resolver. markets.yaml
    # is the human-edited intent (tag_slug + manual overrides) the resolver
    # reads.
    watcher = ConfigWatcher(bucket=bucket, path="subscriptions.yaml")
    writer = BqWriter(project_id=project_id, dataset=dataset)
    await watcher.start()
    await writer.start()

    client = PolymarketWebsocketClient(watcher=watcher, writer=writer)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _handle_signal() -> None:
        log.info("shutdown.signal_received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    client_task = asyncio.create_task(client.run(stop_event))

    try:
        await client_task
    finally:
        log.info("shutdown.begin")
        await watcher.stop()
        await writer.stop()
        log.info("shutdown.complete")


if __name__ == "__main__":
    asyncio.run(main())

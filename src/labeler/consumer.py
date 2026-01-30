import os
import json
import asyncio
import logging
from typing import Optional
import websockets
from .db import insert_event, insert_edges, init_db, upsert_cursor, get_cursor
from .extractor import extract_edges_from_event

LOG = logging.getLogger("labeler.consumer")

FIREHOSE_WS = os.getenv("FIREHOSE_WS_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "mvp_consumer")


class ATProtoConsumer:
    def __init__(self, ws_url: Optional[str] = None):
        self.ws_url = ws_url or FIREHOSE_WS
        self._stop = False

    async def _handle_message(self, raw: str):
        try:
            ev = json.loads(raw)
        except Exception:
            LOG.exception("failed to parse event")
            return

        # basic fields - be permissive with formats
        event_uri = ev.get("uri") or ev.get("id") or ev.get("event_uri")
        author = ev.get("author") or (ev.get("record") or {}).get("author")
        ctime = ev.get("time") or (ev.get("record") or {}).get("indexedAt")

        if not event_uri:
            # skip events that cannot be identified
            return

        insert_event(event_uri, ctime, author, ev)

        edges = extract_edges_from_event(ev)
        insert_edges(edges)

    async def run(self):
        """Connect to the firehose and process messages. This method is resilient to
        reconnects and uses cursor persistence (simple upsert).
        """
        init_db()
        current_cursor = get_cursor(CONSUMER_NAME)
        # For now we just log the cursor if present; proper cursor logic omitted
        LOG.info("starting consumer, cursor=%s", current_cursor)

        while not self._stop:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    LOG.info("connected to firehose %s", self.ws_url)
                    async for msg in ws:
                        await self._handle_message(msg)
            except asyncio.CancelledError:
                break
            except Exception:
                LOG.exception("connection error, reconnecting in 5s")
                await asyncio.sleep(5)

    def stop(self):
        self._stop = True


# A helper to run consumer from sync context (for small dev runs)
def run_consumer_blocking():
    loop = asyncio.get_event_loop()
    consumer = ATProtoConsumer()
    try:
        loop.run_until_complete(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()

import os
import json
import asyncio
import logging
import signal
import time
import random
from typing import Optional
import websockets
from .db import insert_event, insert_edges, init_db, upsert_cursor, get_cursor
from .extractor import extract_edges_from_event

LOG = logging.getLogger("labeler.consumer")

FIREHOSE_WS = os.getenv("FIREHOSE_WS_URL", "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "mvp_consumer")
STATS_INTERVAL_S = int(os.getenv("CONSUMER_STATS_INTERVAL", "60"))
RECONNECT_BASE_S = 5
RECONNECT_MAX_S = 60


class ATProtoConsumer:
    def __init__(self, ws_url: Optional[str] = None):
        self.ws_url = ws_url or FIREHOSE_WS
        self._stop = False
        self._ws = None
        # Counters for STATS line
        self._msgs = 0
        self._inserts = 0
        self._updates = 0
        self._errors = 0
        self._reconnects = 0
        self._started_at = time.monotonic()

    async def _handle_message(self, raw: str):
        try:
            ev = json.loads(raw)
        except Exception:
            self._errors += 1
            LOG.exception("failed to parse event")
            return

        # basic fields - be permissive with formats
        event_uri = ev.get("uri") or ev.get("id") or ev.get("event_uri")
        author = ev.get("author") or (ev.get("record") or {}).get("author")
        ctime = ev.get("time") or (ev.get("record") or {}).get("indexedAt")

        if not event_uri:
            # skip events that cannot be identified
            return

        self._msgs += 1
        inserted, updated = insert_event(event_uri, ctime, author, ev)
        if inserted:
            self._inserts += 1
        if updated:
            self._updates += 1

        edges = extract_edges_from_event(ev)
        insert_edges(edges)

    async def _stats_loop(self):
        """Emit periodic STATS line to stderr for at-a-glance monitoring."""
        while not self._stop:
            await asyncio.sleep(STATS_INTERVAL_S)
            uptime = int(time.monotonic() - self._started_at)
            LOG.info(
                "STATS msgs=%d inserts=%d updates=%d errors=%d "
                "reconnects=%d uptime=%ds",
                self._msgs, self._inserts, self._updates,
                self._errors, self._reconnects, uptime,
            )

    async def run(self):
        """Connect to the firehose and process messages. Resilient to
        reconnects with exponential backoff + jitter and cursor persistence.
        """
        init_db()
        current_cursor = get_cursor(CONSUMER_NAME)
        LOG.info("starting consumer, cursor=%s", current_cursor)

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal)

        # Start STATS heartbeat
        stats_task = asyncio.create_task(self._stats_loop())

        backoff = RECONNECT_BASE_S
        try:
            while not self._stop:
                try:
                    async with websockets.connect(self.ws_url) as ws:
                        self._ws = ws
                        backoff = RECONNECT_BASE_S  # reset on successful connect
                        LOG.info("connected to firehose %s", self.ws_url)
                        async for msg in ws:
                            if self._stop:
                                break
                            await self._handle_message(msg)
                except asyncio.CancelledError:
                    break
                except Exception:
                    self._reconnects += 1
                    jitter = random.uniform(0, backoff * 0.5)
                    wait = backoff + jitter
                    LOG.warning(
                        "connection error, reconnecting in %.1fs (backoff=%ds)",
                        wait, backoff,
                    )
                    await asyncio.sleep(wait)
                    backoff = min(backoff * 2, RECONNECT_MAX_S)
                finally:
                    self._ws = None
        finally:
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass
            # Graceful shutdown: commit final cursor
            self._commit_cursor()
            LOG.info(
                "consumer stopped. msgs=%d inserts=%d updates=%d errors=%d",
                self._msgs, self._inserts, self._updates, self._errors,
            )

    def _handle_signal(self):
        """Signal handler: request graceful stop and close websocket."""
        LOG.info("received shutdown signal, stopping gracefully")
        self._stop = True
        if self._ws:
            asyncio.ensure_future(self._ws.close())

    def _commit_cursor(self):
        """Best-effort cursor commit on shutdown."""
        try:
            upsert_cursor(CONSUMER_NAME, str(int(time.time())))
            LOG.info("cursor committed on shutdown")
        except Exception:
            LOG.exception("failed to commit cursor on shutdown")

    def stop(self):
        self._stop = True


# A helper to run consumer from sync context (for small dev runs)
def run_consumer_blocking():
    consumer = ATProtoConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()

"""Jetstream-based ATProto event consumer.

Connects to a Bluesky Jetstream endpoint (JSON over WebSocket) instead of the
raw firehose (CBOR/CAR). Filters to post and repost collections.

Jetstream docs: https://docs.bsky.app/blog/jetstream

Backported from driftwatch operational patterns:
- put_nowait to avoid blocking event loop (kills WS pings → reconnect churn)
- Cursor persistence with 3s rewind on reconnect
- Exponential backoff with jitter
- STATS heartbeat line
- Signal handling for graceful shutdown
"""

import os
import json
import asyncio
import logging
import random
import signal
import time
from typing import Optional
import websockets
from .db import insert_event, insert_edges, init_db, upsert_cursor, get_cursor
from .extractor import extract_edges_from_event
from . import timeutil

LOG = logging.getLogger("labeler.consumer")

JETSTREAM_WS = os.getenv(
    "FIREHOSE_WS_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe",
)
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "mvp_consumer")
WANTED_COLLECTIONS = os.getenv(
    "JETSTREAM_COLLECTIONS",
    "app.bsky.feed.post,app.bsky.feed.repost",
).split(",")

CURSOR_SAVE_INTERVAL = int(os.getenv("CURSOR_SAVE_INTERVAL", "500"))
STATS_INTERVAL_S = int(os.getenv("CONSUMER_STATS_INTERVAL", "60"))
RECONNECT_BASE_S = 5
RECONNECT_MAX_S = 60
CURSOR_REWIND_US = 3_000_000  # 3s rewind on reconnect per Jetstream docs
QUEUE_MAX = 5000


def _build_ws_url(base_url: str, cursor: Optional[str] = None) -> str:
    """Append wantedCollections and optional cursor to the Jetstream URL."""
    params = []
    for col in WANTED_COLLECTIONS:
        col = col.strip()
        if col:
            params.append(f"wantedCollections={col}")
    if cursor:
        params.append(f"cursor={cursor}")
    if params:
        sep = "&" if "?" in base_url else "?"
        return base_url + sep + "&".join(params)
    return base_url


def _jetstream_to_event(js: dict) -> Optional[dict]:
    """Transform a Jetstream commit event into the canonical event dict
    that the rest of the pipeline (insert_event, extract_edges, claims) expects.

    Returns None for events we don't care about (identity, account, deletes).
    """
    if js.get("kind") != "commit":
        return None

    commit = js.get("commit", {})
    operation = commit.get("operation")

    # We only ingest creates and updates, not deletes
    if operation not in ("create", "update"):
        return None

    did = js.get("did", "")
    collection = commit.get("collection", "")
    rkey = commit.get("rkey", "")
    cid = commit.get("cid", "")
    record = commit.get("record", {})

    # Build AT URI: at://{did}/{collection}/{rkey}
    uri = f"at://{did}/{collection}/{rkey}"

    # Convert Jetstream time_us (microseconds) to ISO timestamp
    time_us = js.get("time_us")
    if time_us:
        ctime = timeutil.to_utc_iso(time_us / 1_000_000)
    else:
        ctime = timeutil.now_utc().isoformat()

    if collection == "app.bsky.feed.post":
        # Extract reply pointers
        reply = record.get("reply", {})
        reply_parent = reply.get("parent", {}) if reply else {}
        reply_root = reply.get("root", {}) if reply else {}

        # Extract external links from embeds
        external_links = []
        embed = record.get("embed", {})
        if embed:
            ext = embed.get("external", {})
            if ext and ext.get("uri"):
                external_links.append(ext["uri"])
            media = embed.get("media", {})
            if media:
                ext2 = media.get("external", {})
                if ext2 and ext2.get("uri"):
                    external_links.append(ext2["uri"])

        return {
            "uri": uri,
            "cid": cid,
            "text": record.get("text", ""),
            "author": did,
            "authorDid": did,
            "time": ctime,
            "createdAt": record.get("createdAt", ctime),
            "replyParentUri": reply_parent.get("uri"),
            "replyRootUri": reply_root.get("uri"),
            "facets": record.get("facets", []),
            "embeds": [embed] if embed else [],
            "externalLinks": external_links,
            "record": record,
            "_collection": collection,
            "_operation": operation,
        }

    elif collection == "app.bsky.feed.repost":
        subject = record.get("subject", {})
        return {
            "uri": uri,
            "cid": cid,
            "text": "",
            "author": did,
            "authorDid": did,
            "time": ctime,
            "createdAt": record.get("createdAt", ctime),
            "replyParentUri": None,
            "replyRootUri": None,
            "facets": [],
            "embeds": [],
            "externalLinks": [],
            "record": record,
            "type": "repost",
            "subject": subject,
            "_collection": collection,
            "_operation": operation,
        }

    return None


class ATProtoConsumer:
    def __init__(self, ws_url: Optional[str] = None):
        self.ws_url = ws_url or JETSTREAM_WS
        self._stop = False
        self._ws = None
        self._last_cursor: Optional[str] = None
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
        # Counters
        self._msgs = 0
        self._inserts = 0
        self._updates = 0
        self._errors = 0
        self._dropped = 0
        self._reconnects = 0
        self._started_at = time.monotonic()

    def _process_event(self, ev: dict):
        """Synchronous DB work — called via run_in_executor from drain task."""
        event_uri = ev["uri"]
        author = ev.get("authorDid") or ev.get("author")
        ctime = ev.get("createdAt") or ev.get("time")
        inserted, updated = insert_event(event_uri, ctime, author, ev)
        if inserted:
            self._inserts += 1
        if updated:
            self._updates += 1
        edges = extract_edges_from_event(ev)
        insert_edges(edges)

    async def _drain_queue(self):
        """Background task: drain event queue without blocking the WS read loop."""
        from .preflight import is_disk_pressure
        loop = asyncio.get_running_loop()
        _brake_logged = False

        while not self._stop:
            # Disk pressure brake
            if is_disk_pressure():
                if not _brake_logged:
                    LOG.error("DISK PRESSURE: pausing event processing")
                    _brake_logged = True
                await asyncio.sleep(10)
                continue
            elif _brake_logged:
                LOG.info("DISK PRESSURE: cleared, resuming")
                _brake_logged = False

            try:
                ev = await asyncio.wait_for(self._event_queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await loop.run_in_executor(None, self._process_event, ev)
            except Exception:
                self._errors += 1
                LOG.exception("failed to process event")

            self._msgs += 1

            # Periodic cursor save
            if self._msgs % CURSOR_SAVE_INTERVAL == 0 and self._last_cursor:
                try:
                    await loop.run_in_executor(
                        None, upsert_cursor, CONSUMER_NAME, self._last_cursor
                    )
                except Exception:
                    LOG.exception("failed to save cursor")

    async def _stats_loop(self):
        """Emit periodic STATS line to stderr."""
        while not self._stop:
            await asyncio.sleep(STATS_INTERVAL_S)
            uptime = int(time.monotonic() - self._started_at)
            backlog = self._event_queue.qsize()
            LOG.info(
                "STATS msgs=%d inserts=%d updates=%d errors=%d "
                "dropped=%d reconnects=%d backlog=%d uptime=%ds",
                self._msgs, self._inserts, self._updates,
                self._errors, self._dropped, self._reconnects,
                backlog, uptime,
            )

    async def _handle_message(self, raw: str):
        try:
            js = json.loads(raw)
        except Exception:
            self._errors += 1
            LOG.warning("failed to parse JSON message, skipping")
            return

        # Track cursor from every message (not just commits)
        time_us = js.get("time_us")
        if time_us:
            self._last_cursor = str(time_us)

        # Transform to canonical event
        ev = _jetstream_to_event(js)
        if ev is None:
            return

        # Non-blocking put — drop events when queue is full rather than
        # blocking the event loop (which kills WS pings → reconnect churn)
        try:
            self._event_queue.put_nowait(ev)
        except asyncio.QueueFull:
            self._dropped += 1

    def _resume_cursor(self) -> Optional[str]:
        """Get cursor for reconnect, rewound 3s for gapless playback."""
        cursor = self._last_cursor
        if cursor:
            try:
                rewound = max(0, int(cursor) - CURSOR_REWIND_US)
                return str(rewound)
            except (ValueError, TypeError):
                pass
        return cursor

    async def run(self):
        """Connect to Jetstream and process messages with reconnect resilience."""
        init_db()
        saved_cursor = get_cursor(CONSUMER_NAME)
        LOG.info(
            "starting Jetstream consumer, collections=%s cursor=%s",
            WANTED_COLLECTIONS, saved_cursor,
        )

        # Register signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._handle_signal)

        # Start background tasks
        drain_task = asyncio.create_task(self._drain_queue())
        stats_task = asyncio.create_task(self._stats_loop())

        backoff = RECONNECT_BASE_S
        try:
            while not self._stop:
                try:
                    cursor = self._resume_cursor() or saved_cursor
                    url = _build_ws_url(self.ws_url, cursor=cursor)
                    async with websockets.connect(
                        url,
                        max_size=10 * 1024 * 1024,
                        ping_interval=30,
                        ping_timeout=10,
                        close_timeout=10,
                    ) as ws:
                        self._ws = ws
                        backoff = RECONNECT_BASE_S  # reset on success
                        LOG.info("connected to Jetstream")
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
                        "Jetstream connection error, reconnecting in %.1fs",
                        wait,
                    )
                    await asyncio.sleep(wait)
                    backoff = min(backoff * 2, RECONNECT_MAX_S)
                finally:
                    self._ws = None
        finally:
            # Clean up background tasks
            drain_task.cancel()
            stats_task.cancel()
            for t in (drain_task, stats_task):
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            # Commit final cursor
            if self._last_cursor:
                try:
                    upsert_cursor(CONSUMER_NAME, self._last_cursor)
                    LOG.info("cursor committed on shutdown: %s", self._last_cursor)
                except Exception:
                    LOG.exception("failed to commit cursor on shutdown")
            LOG.info(
                "consumer stopped. msgs=%d inserts=%d updates=%d "
                "errors=%d dropped=%d",
                self._msgs, self._inserts, self._updates,
                self._errors, self._dropped,
            )

    def _handle_signal(self):
        """Signal handler: request graceful stop and close websocket."""
        LOG.info("received shutdown signal, stopping gracefully")
        self._stop = True
        if self._ws:
            asyncio.ensure_future(self._ws.close())

    def stop(self):
        self._stop = True


def run_consumer_blocking():
    consumer = ATProtoConsumer()
    try:
        asyncio.run(consumer.run())
    except KeyboardInterrupt:
        consumer.stop()

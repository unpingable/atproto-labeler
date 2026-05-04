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
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
import websockets
from .db import insert_event, insert_edges, insert_event_txn, insert_edges_txn, init_db, upsert_cursor, get_cursor, get_conn
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

# Batched-write knobs: drain up to N events per writer transaction, or wait at
# most M seconds for the batch to fill. One commit per batch.
# A naive per-event commit cadence (the obvious shape) self-sheds under real
# firehose load — see docs/INGEST_INVARIANTS.md.
BATCH_MAX_EVENTS = int(os.getenv("BATCH_MAX_EVENTS", "100"))
BATCH_MAX_WAIT_S = float(os.getenv("BATCH_MAX_WAIT_S", "0.25"))

# Writer-owned WAL truncate. The persistent writer thread calls
# wal_checkpoint(TRUNCATE) on its own connection right after a successful
# commit — that's the cleanest moment to attempt a WAL restart, since the
# writer just released its frame and is least likely to be racing readers
# (auto-checkpoint at 1000 frames is PASSIVE only and never truncates).
# Rate-limited so per-batch overhead is bounded. See INGEST_INVARIANTS
# section 6 for the bucket-vocabulary doctrine that motivates this.
WAL_TRUNCATE_INTERVAL_S = float(os.getenv("WAL_TRUNCATE_INTERVAL_S", "30"))


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
        # Single-thread writer: ensures the persistent SQLite conn is only
        # ever touched from one OS thread (sqlite3 default check_same_thread).
        # See docs/INGEST_INVARIANTS.md for why batching is correctness, not
        # optimization.
        self._writer_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="lbl-writer"
        )
        self._writer_conn = None  # lazily opened inside the writer thread
        self._last_wal_truncate_mono = 0.0  # writer thread only
        # Counts events shed by the writer when a batch hits a write-lock
        # conflict (e.g. retention or another connection holding the lock)
        # and rolls back. Tracked alongside _dropped so a future health
        # surface can sum both into intake-loss accounting — a green
        # recovery flag must not hide lock-conflict shedding (see
        # INGEST_INVARIANTS section 6).
        self._rollback_lost = 0  # main thread only

    def _get_writer_conn(self):
        """Return the persistent writer connection, opening it on first call.

        Must only be called from the writer executor thread.
        """
        if self._writer_conn is None:
            self._writer_conn = get_conn()
        return self._writer_conn

    def _process_batch(self, batch):
        """Synchronous DB work for a batch of events. Runs in the writer thread.

        One transaction, one commit per batch. On any exception, rolls back
        the entire batch — we'd rather lose a batch than half-write it.

        Returns (written, inserted_delta, updated_delta, lost). ``lost`` is
        non-zero only when the batch failed; it must count against intake
        health so a recovery gate can't hide lock-conflict shedding.
        """
        if not batch:
            return (0, 0, 0, 0)
        conn = self._get_writer_conn()
        inserted_delta = 0
        updated_delta = 0
        try:
            for ev in batch:
                event_uri = ev["uri"]
                author = ev.get("authorDid") or ev.get("author")
                ctime = ev.get("createdAt") or ev.get("time")
                inserted, updated = insert_event_txn(conn, event_uri, ctime, author, ev)
                if inserted:
                    inserted_delta += 1
                if updated:
                    updated_delta += 1
                edges = extract_edges_from_event(ev)
                insert_edges_txn(conn, edges)
            conn.commit()
            self._maybe_wal_truncate(conn)
            return (len(batch), inserted_delta, updated_delta, 0)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                LOG.exception("rollback failed after batch error")
            LOG.exception("batch failed; rolled back %d events", len(batch))
            return (0, 0, 0, len(batch))

    def _maybe_wal_truncate(self, conn):
        """Attempt PRAGMA wal_checkpoint(TRUNCATE) from the writer thread.

        Called only from inside _process_batch after a successful commit —
        the writer just released its frame, the least racy moment to
        attempt a WAL restart. Auto-checkpoint at 1000 frames is PASSIVE
        only (frames flush to main DB but the WAL file itself never
        shrinks); without an explicit TRUNCATE the WAL grows to its
        high-water mark and stays. Rate-limited so the cost is bounded.

        Logs only when the result is interesting (busy or non-trivial work
        done); silent on the typical no-op case.
        """
        now = time.monotonic()
        if now - self._last_wal_truncate_mono < WAL_TRUNCATE_INTERVAL_S:
            return
        self._last_wal_truncate_mono = now
        try:
            row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if not row:
                return
            busy, log, ckpt = row
            if busy or log >= 1000:
                LOG.info(
                    "wal_truncate: busy=%d log=%d checkpointed=%d",
                    busy, log, ckpt,
                )
        except Exception:
            LOG.debug("wal_truncate failed", exc_info=True)

    async def _drain_queue(self):
        """Background task: drain event queue in batches without blocking WS read.

        Pulls up to BATCH_MAX_EVENTS (or BATCH_MAX_WAIT_S, whichever first)
        and hands the whole batch to the dedicated writer thread, which runs
        one transaction with one commit. Per-event SQLite connection churn
        and per-event fsync barriers were the bottleneck pre-fix.
        """
        from .preflight import is_disk_pressure
        loop = asyncio.get_running_loop()
        _brake_logged = False
        events_since_cursor_save = 0

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

            # Build a batch: first event blocks (with sane timeout); subsequent
            # events are pulled non-blockingly until cap or short-wait deadline.
            batch = []
            try:
                first = await asyncio.wait_for(self._event_queue.get(), timeout=5.0)
                batch.append(first)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            deadline = loop.time() + BATCH_MAX_WAIT_S
            while len(batch) < BATCH_MAX_EVENTS:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    ev = self._event_queue.get_nowait()
                    batch.append(ev)
                except asyncio.QueueEmpty:
                    try:
                        ev = await asyncio.wait_for(
                            self._event_queue.get(), timeout=remaining
                        )
                        batch.append(ev)
                    except asyncio.TimeoutError:
                        break
                    except asyncio.CancelledError:
                        self._stop = True
                        break

            try:
                written, inserted_delta, updated_delta, lost = await loop.run_in_executor(
                    self._writer_executor, self._process_batch, batch
                )
            except Exception:
                self._errors += 1
                LOG.exception("failed to process batch")
                written, inserted_delta, updated_delta, lost = 0, 0, 0, 0

            if lost:
                # Database-locked rollbacks are intake loss — surface them
                # in STATS so they cannot hide behind a green recovery flag.
                self._rollback_lost += lost

            if written:
                self._msgs += written
                self._inserts += inserted_delta
                self._updates += updated_delta
                events_since_cursor_save += written

                # Periodic cursor save (after a successful commit)
                if events_since_cursor_save >= CURSOR_SAVE_INTERVAL and self._last_cursor:
                    try:
                        await loop.run_in_executor(
                            None, upsert_cursor, CONSUMER_NAME, self._last_cursor
                        )
                        events_since_cursor_save = 0
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
                "dropped=%d rollback_lost=%d reconnects=%d backlog=%d uptime=%ds",
                self._msgs, self._inserts, self._updates,
                self._errors, self._dropped, self._rollback_lost,
                self._reconnects, backlog, uptime,
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

            # Close the persistent writer connection from inside the writer
            # thread, then shut the executor down. Order matters: don't
            # shutdown the executor first or the close hop has nowhere to run.
            def _close_writer():
                if self._writer_conn is not None:
                    try:
                        self._writer_conn.close()
                    except Exception:
                        LOG.debug("writer conn close failed", exc_info=True)
                    self._writer_conn = None

            try:
                await loop.run_in_executor(self._writer_executor, _close_writer)
            except Exception:
                LOG.debug("writer close hop failed", exc_info=True)
            finally:
                self._writer_executor.shutdown(wait=True, cancel_futures=False)

            # Commit final cursor
            if self._last_cursor:
                try:
                    upsert_cursor(CONSUMER_NAME, self._last_cursor)
                    LOG.info("cursor committed on shutdown: %s", self._last_cursor)
                except Exception:
                    LOG.exception("failed to commit cursor on shutdown")
            LOG.info(
                "consumer stopped. msgs=%d inserts=%d updates=%d "
                "errors=%d dropped=%d rollback_lost=%d",
                self._msgs, self._inserts, self._updates,
                self._errors, self._dropped, self._rollback_lost,
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

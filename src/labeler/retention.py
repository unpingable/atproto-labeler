"""Retention loop: prune old data to prevent unbounded disk growth.

Configurable via environment variables:
  RETENTION_EVENTS_DAYS     — delete events older than N days (default 7)
  RETENTION_EDGES_DAYS      — delete edges older than N days (default 14)
  RETENTION_VERSIONS_DAYS   — delete event_versions older than N days (default 7)
  RETENTION_CLAIMS_DAYS     — delete claim_history older than N days (default 30)
  RETENTION_INTERVAL_HOURS  — hours between retention passes (default 6)
  RETENTION_BATCH_SIZE      — rows per DELETE batch (default 5000)
"""
import datetime
import logging
import os
import time

from . import timeutil
from .db import get_conn

LOG = logging.getLogger("labeler.retention")

EVENTS_DAYS = int(os.getenv("RETENTION_EVENTS_DAYS", "7"))
EDGES_DAYS = int(os.getenv("RETENTION_EDGES_DAYS", "14"))
VERSIONS_DAYS = int(os.getenv("RETENTION_VERSIONS_DAYS", "7"))
CLAIMS_DAYS = int(os.getenv("RETENTION_CLAIMS_DAYS", "30"))
BATCH_SIZE = int(os.getenv("RETENTION_BATCH_SIZE", "5000"))


def _cutoff(days: int) -> str:
    return (timeutil.now_utc() - datetime.timedelta(days=days)).isoformat()


def _batch_delete(conn, table: str, ts_col: str, cutoff: str) -> int:
    """Delete rows older than cutoff in batches to avoid long locks."""
    total = 0
    while True:
        cur = conn.execute(
            f"DELETE FROM {table} WHERE rowid IN "
            f"(SELECT rowid FROM {table} WHERE {ts_col} < ? LIMIT ?)",
            (cutoff, BATCH_SIZE),
        )
        deleted = cur.rowcount
        conn.commit()
        total += deleted
        if deleted < BATCH_SIZE:
            break
    return total


def run_retention() -> dict:
    """Run one retention pass. Returns counts of deleted rows."""
    t0 = time.monotonic()
    conn = get_conn()
    stats = {}

    stats["events"] = _batch_delete(conn, "events", "ctime", _cutoff(EVENTS_DAYS))
    stats["edges"] = _batch_delete(conn, "edges", "ctime", _cutoff(EDGES_DAYS))
    stats["event_versions"] = _batch_delete(
        conn, "event_versions", "version_ts", _cutoff(VERSIONS_DAYS)
    )
    stats["claim_history"] = _batch_delete(
        conn, "claim_history", "createdAt", _cutoff(CLAIMS_DAYS)
    )

    elapsed = time.monotonic() - t0
    LOG.info(
        "retention pass: events=%d edges=%d versions=%d claims=%d (%.1fs)",
        stats["events"], stats["edges"], stats["event_versions"],
        stats["claim_history"], elapsed,
    )
    conn.close()
    return stats

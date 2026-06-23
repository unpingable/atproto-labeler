"""Health surface must not read clean "ok" while intake loss is unaccounted.

These tests encode the doctrine that operationally-up is not epistemically-OK:
a consumer that rolled back batches or dropped events is degraded even after it
recovers and resumes ingesting. See docs/INGEST_INVARIANTS.md.
"""

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient


def _client():
    from labeler.main import app
    return TestClient(app)


def _reset_health():
    from labeler.db import init_db, get_conn
    init_db()
    conn = get_conn()
    conn.execute("DELETE FROM consumer_health")
    conn.commit()
    conn.close()


def test_health_surfaces_loss():
    """A fresh heartbeat with zero loss is ok; a rollback_lost > 0 flips the
    surface to degraded and exposes the loss count."""
    from labeler.db import upsert_consumer_health

    _reset_health()
    client = _client()

    # Clean, live consumer: fresh heartbeat, no loss -> ok.
    upsert_consumer_health("test_consumer", rollback_lost=0, dropped=0)
    body = client.get("/health/extended").json()
    assert body["status"] == "ok"
    assert body["consumer"]["rollback_lost"] == 0
    assert body["consumer"]["heartbeat_stale"] is False

    # Same consumer, still live (fresh heartbeat), but a batch was lost to a
    # lock-conflict rollback. The surface must report degraded.
    upsert_consumer_health("test_consumer", rollback_lost=3, dropped=0)
    body = client.get("/health/extended").json()
    assert body["status"] == "degraded"
    assert body["consumer"]["rollback_lost"] == 3
    # Degraded despite a fresh heartbeat — the consumer is up, not OK.
    assert body["consumer"]["heartbeat_stale"] is False

    # Dropped events (queue full) are also intake loss and must degrade.
    _reset_health()
    upsert_consumer_health("test_consumer", rollback_lost=0, dropped=7)
    body = client.get("/health/extended").json()
    assert body["status"] == "degraded"
    assert body["consumer"]["dropped"] == 7


def test_green_recovery_cannot_mask_loss():
    """A successful heartbeat AFTER a loss event must not zero the loss or flip
    the surface back to ok. Recovery is not absolution for unaccounted loss."""
    from labeler.db import upsert_consumer_health, get_consumer_health

    _reset_health()
    client = _client()

    # Loss happens.
    upsert_consumer_health("test_consumer", rollback_lost=5, dropped=0)
    body = client.get("/health/extended").json()
    assert body["status"] == "degraded"

    # Consumer recovers and keeps ingesting: the next stats tick writes a fresh
    # heartbeat. The counters are monotonic for the life of the process, so the
    # healthy tick carries the SAME accumulated loss, not a reset.
    upsert_consumer_health("test_consumer", rollback_lost=5, dropped=0)

    persisted = get_consumer_health("test_consumer")
    assert persisted["rollback_lost"] == 5, "a later heartbeat must not zero loss"

    body = client.get("/health/extended").json()
    assert body["status"] == "degraded", "green recovery must not mask earlier loss"
    assert body["consumer"]["heartbeat_stale"] is False  # alive, just not OK

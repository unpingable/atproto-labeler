"""Finite offline acceptance: real routes, SQLite, rule receipts and suppression."""
import json
import socket

import pytest
from httpx import AsyncClient, ASGITransport


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    from labeler import db
    monkeypatch.setattr(db, "DATA_DIR", tmp_path)
    monkeypatch.setenv("LABELER_EMIT_AUDIT_DIR", str(tmp_path / "audit"))
    for name in ("ADMIN_API_TOKEN", "LABELER_EMIT_MODE", "LABELER_EMIT_CONFIRM",
                 "LABELER_RULE_BUDGETS", "ENABLE_CLAIM_RECHECK", "REDIS_URL"):
        monkeypatch.delenv(name, raising=False)
    from labeler import recheck_queue
    monkeypatch.setattr(recheck_queue, "REDIS_URL", None)
    original = socket.socket.connect
    def offline(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            pytest.fail(f"offline acceptance attempted network: {address}")
        return original(sock, address)
    monkeypatch.setattr(socket.socket, "connect", offline)
    db.init_db()


PROTECTED = ("/admin/mappings", "/admin/cooldowns", "/recent-decisions",
             "/quarantine/recent", "/exposure/did:example:synthetic",
             "/strain/top", "/labels/uri:synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("path", PROTECTED)
async def test_routes_fail_closed_without_configured_token(path):
    from labeler.main import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://offline.test") as client:
        assert (await client.get(path)).status_code == 503
        assert (await client.get(path, headers={"X-Admin-Token": "anything"})).status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("path", PROTECTED)
async def test_routes_reject_wrong_token_and_accept_correct(path, monkeypatch):
    from labeler.main import app
    monkeypatch.setenv("ADMIN_API_TOKEN", "synthetic-test-token")
    async def empty():
        return {}
    monkeypatch.setattr("labeler.cooldown.get_all_mappings", empty)
    monkeypatch.setattr("labeler.cooldown.get_all_active_cooldowns", empty)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://offline.test") as client:
        assert (await client.get(path)).status_code == 401
        assert (await client.get(path, headers={"X-Admin-Token": "wrong"})).status_code == 401
        for headers in ({"X-Admin-Token": "synthetic-test-token"},
                        {"Authorization": "Bearer synthetic-test-token"}):
            response = await client.get(path, headers=headers)
            assert response.status_code == (404 if path.startswith("/labels/") else 200)


def test_quarantine_failure_is_not_reported_as_success(tmp_path, monkeypatch):
    from labeler import emitter
    def unavailable(**kwargs):
        raise RuntimeError("synthetic unavailable store")
    monkeypatch.setattr(emitter, "insert_quarantine_emit", unavailable)
    with pytest.raises(RuntimeError, match="unavailable store"):
        emitter.record_emit_decision([{"subject_uri": "uri:synthetic", "label": "test"}],
                                     "quarantine", str(tmp_path / "audit.jsonl"))


def test_receipt_evidence_belongs_to_subject():
    from labeler.db import insert_event, get_conn
    from labeler.longitudinal import recheck_once
    from labeler import timeutil
    posts = [
        {"uri": "uri:synthetic:root", "cid": "1", "authorDid": "did:example:synthetic",
         "text": "According to source X, 100 people were evacuated.", "createdAt": "2026-01-01T00:00:00Z"},
        {"uri": "uri:synthetic:later", "cid": "2", "authorDid": "did:example:synthetic",
         "replyRootUri": "uri:synthetic:root", "text": "100 people were evacuated.", "createdAt": "2026-01-01T00:10:00Z"},
        {"uri": "uri:synthetic:last", "cid": "3", "authorDid": "did:example:other",
         "replyRootUri": "uri:synthetic:root", "text": "Unrelated 999 widgets.", "createdAt": "2026-01-01T00:20:00Z"},
    ]
    for p in posts:
        insert_event(p["uri"], timeutil.to_utc_datetime(p["createdAt"]), p["authorDid"], p)
    recheck_once()
    conn = get_conn()
    rows = conn.execute("SELECT inputs_json, root_uri FROM label_decisions WHERE subject_uri = ?", (posts[1]["uri"],)).fetchall()
    conn.close()
    assert rows
    for inputs, root in rows:
        assert "100" in json.loads(inputs)["quantities"]
        assert "999" not in json.loads(inputs)["quantities"]
        assert root == posts[0]["uri"]


@pytest.mark.asyncio
async def test_live_autostart_refused(monkeypatch):
    from labeler.main import startup_event
    monkeypatch.setenv("FIREHOSE_AUTO_START", "1")
    def no_consumer():
        raise AssertionError("unqualified consumer must not be constructed")
    monkeypatch.setattr("labeler.main.ATProtoConsumer", no_consumer)
    with pytest.raises(RuntimeError, match="unqualified"):
        await startup_event()


def test_explicit_data_directory(tmp_path, monkeypatch):
    import os
    import subprocess
    import sys
    destination = tmp_path / "explicit-data"
    monkeypatch.setenv("LABELER_DATA_DIR", str(destination))
    result = subprocess.run([sys.executable, "-c", "from labeler.db import init_db; init_db()"],
                            env=dict(os.environ, PYTHONPATH="src"), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (destination / "labeler.sqlite").exists()


def test_offline_demo_runs_without_network_and_checks_restart():
    import subprocess
    import sys
    result = subprocess.run([sys.executable, "-m", "labeler.offline_demo"],
                            env={"PYTHONPATH": "src"}, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads(result.stdout)
    assert summary == {
        "budget_mode": "quarantine", "detect_mode": "detect-only", "dual_gate": "pass",
        "evidence_receipts": "pass", "network_attempts": 0, "quarantine_restart": "pass",
        "state_loss": "unknown-history", "status": "PASS", "store_failure": "propagated",
    }


@pytest.mark.parametrize("mode,confirm,want", [
    (None, None, "detect-only"), ("emit", None, "detect-only"),
    (None, "true", "detect-only"), ("emit", "true", "emit"),
    ("invalid", "true", "detect-only"), ("quarantine", "true", "quarantine"),
])
def test_dual_gate(mode, confirm, want, monkeypatch):
    from labeler.emit_mode import get_emit_mode
    if mode is not None:
        monkeypatch.setenv("LABELER_EMIT_MODE", mode)
    if confirm is not None:
        monkeypatch.setenv("LABELER_EMIT_CONFIRM", confirm)
    assert get_emit_mode() == want


@pytest.mark.asyncio
async def test_durable_health_and_stale_heartbeat():
    from labeler import db
    from labeler.main import app
    db.upsert_consumer_health("synthetic", rollback_lost=2, dropped=3)
    assert db.get_consumer_health("synthetic")["dropped"] == 3
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://offline.test") as client:
        body = (await client.get("/health/extended")).json()
        assert body["status"] == "degraded"
        assert body["consumer"]["rollback_lost"] == 2
        assert body["consumer"]["heartbeat_stale"] is False
        conn = db.get_conn()
        conn.execute("UPDATE consumer_health SET updated_at = ?", ("2000-01-01T00:00:00+00:00",))
        conn.commit()
        conn.close()
        body = (await client.get("/health/extended")).json()
        assert body["status"] == "degraded"
        assert body["consumer"]["heartbeat_stale"] is True

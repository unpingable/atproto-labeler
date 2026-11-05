"""Finite synthetic demonstration, isolated SQLite and no network access.

Run from the repository root: PYTHONPATH=src python -m labeler.offline_demo
No service is launched. Temporary databases and audit files retire on exit.
"""
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile


def main():
    network_attempts = []

    def no_network(event, args):
        if event in ("socket.connect", "socket.getaddrinfo", "socket.sendto"):
            network_attempts.append(event)
            raise RuntimeError("offline demonstration prohibits network access")

    sys.addaudithook(no_network)
    # Override known service inputs before importing modules that cache env config.
    for key in ("REDIS_URL", "LABELER_ENDPOINT", "ENABLE_CLAIM_RECHECK",
                "LABELER_RULE_BUDGETS", "LABELER_EMIT_MODE", "LABELER_EMIT_CONFIRM"):
        os.environ.pop(key, None)
    os.environ["DB_BACKEND"] = "sqlite"

    if len(sys.argv) == 3 and sys.argv[1] == "--inspect":
        os.environ["LABELER_DATA_DIR"] = sys.argv[2]
        from .db import get_conn, get_consumer_health
        conn = get_conn()
        result = [conn.execute("SELECT COUNT(*) FROM " + table).fetchone()[0]
                  for table in ("label_decisions", "quarantine_emits")]
        conn.close()
        health = get_consumer_health("synthetic")
        assert health["rollback_lost"] == 2 and health["dropped"] == 3
        assert not network_attempts
        print(json.dumps(result))
        return
    if len(sys.argv) != 1:
        raise SystemExit("usage: python -m labeler.offline_demo")

    with tempfile.TemporaryDirectory(prefix="reference-labeler-offline-") as temp:
        root = Path(temp)
        os.environ["LABELER_DATA_DIR"] = str(root / "default")
        os.environ["LABELER_EMIT_AUDIT_DIR"] = str(root / "audit")
        from . import db, emitter, timeutil
        from .emit_mode import get_emit_mode
        from .longitudinal import recheck_once
        db.init_db()
        fixture = Path(__file__).resolve().parents[2] / "fixtures" / "offline_closeout.jsonl"
        posts = [json.loads(line) for line in fixture.read_text().splitlines()]

        def replay():
            for post in posts:
                db.insert_event(post["uri"], timeutil.to_utc_datetime(post["createdAt"]),
                                post["authorDid"], post)
            assert recheck_once() > 0
            conn = db.get_conn()
            rows = conn.execute("SELECT subject_uri, inputs_json, evidence_hashes_json, config_hash FROM label_decisions").fetchall()
            suppressed = conn.execute("SELECT emit_mode, emit_status, payload_json FROM quarantine_emits").fetchall()
            conn.close()
            assert rows and suppressed
            for subject, inputs, hashes, config in rows:
                assert subject in {post["uri"] for post in posts}
                expected_quantity = "999" if subject == "uri:synthetic:last" else "100"
                assert expected_quantity in json.loads(inputs)["quantities"]
                assert json.loads(hashes) and config
            assert all(row[1] == "suppressed" for row in suppressed)
            return rows, suppressed

        assert get_emit_mode() == "detect-only"
        rows, suppressed = replay()
        assert all(row[0] == "detect-only" for row in suppressed)
        assert not (root / "audit" / "live_emits.jsonl").exists()
        db.upsert_consumer_health("synthetic", rollback_lost=2, dropped=3)
        # Fresh interpreter reads the same committed store, not in-memory objects.
        result = subprocess.run(
            [sys.executable, "-m", "labeler.offline_demo", "--inspect", str(db.DATA_DIR)],
            env={"PYTHONPATH": str(Path(__file__).resolve().parents[1])},
            capture_output=True, text=True, check=True, timeout=10,
        )
        assert json.loads(result.stdout) == [len(rows), len(suppressed)]

        # Dual opt-in only selects local audit mode; no publisher is implemented.
        os.environ["LABELER_EMIT_MODE"] = "emit"
        assert get_emit_mode() == "detect-only"
        os.environ["LABELER_EMIT_CONFIRM"] = "true"
        assert get_emit_mode() == "emit"
        os.environ["LABELER_EMIT_MODE"] = "detect-only"
        assert get_emit_mode() == "detect-only"
        os.environ.pop("LABELER_EMIT_CONFIRM")

        # A new isolated run actually exercises the per-rule quarantine trip.
        db.DATA_DIR = root / "budget"
        db.DATA_DIR.mkdir()
        db.init_db()
        os.environ["LABELER_RULE_BUDGETS"] = "provenance_laundering:0"
        _, budget_suppressed = replay()
        assert all(row[0] == "quarantine" for row in budget_suppressed)
        assert any("budget_exceeded" in json.loads(row[2])["emit_reason"] for row in budget_suppressed)

        # Real unavailable SQLite table: no successful return may hide failed custody.
        conn = db.get_conn()
        conn.execute("ALTER TABLE quarantine_emits RENAME TO held_quarantine")
        conn.commit()
        conn.close()
        try:
            emitter.record_emit_decision([{"subject_uri": "uri:synthetic:negative", "label": "test"}], "quarantine")
        except sqlite3.OperationalError:
            pass
        else:
            raise AssertionError("suppressed store failure did not propagate")

        # Store replacement has no invented cursor, health history or receipts.
        db.DATA_DIR = root / "lost-state"
        db.DATA_DIR.mkdir()
        db.init_db()
        assert db.get_cursor("synthetic") is None
        assert db.get_consumer_health("synthetic") is None
        conn = db.get_conn()
        assert conn.execute("SELECT COUNT(*) FROM label_decisions").fetchone()[0] == 0
        conn.close()
        assert not network_attempts
        print(json.dumps({
            "budget_mode": "quarantine", "detect_mode": "detect-only", "dual_gate": "pass",
            "evidence_receipts": "pass", "network_attempts": len(network_attempts),
            "quarantine_restart": "pass", "state_loss": "unknown-history", "status": "PASS",
            "store_failure": "propagated",
        }, sort_keys=True))


if __name__ == "__main__":
    main()

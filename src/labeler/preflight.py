"""Preflight checks and disk pressure monitoring.

Preflight: run at startup or on demand to verify DB health.
Disk pressure: check disk usage, set brake flag if critical.
"""
import logging
import os
import shutil

from .db import get_conn, DATA_DIR

LOG = logging.getLogger("labeler.preflight")

DISK_WARN_PCT = int(os.getenv("DISK_WARN_PCT", "85"))
DISK_BRAKE_PCT = int(os.getenv("DISK_BRAKE_PCT", "92"))
PRESSURE_FLAG = DATA_DIR / ".disk_pressure"


def check_disk() -> dict:
    """Check disk usage for the data directory."""
    usage = shutil.disk_usage(str(DATA_DIR))
    pct = (usage.used / usage.total) * 100
    free_gb = usage.free / (1024 ** 3)
    result = {
        "disk_pct": round(pct, 1),
        "free_gb": round(free_gb, 1),
        "status": "ok",
    }
    if pct >= DISK_BRAKE_PCT:
        result["status"] = "brake"
        if not PRESSURE_FLAG.exists():
            PRESSURE_FLAG.touch()
            LOG.error("DISK PRESSURE BRAKE: %.1f%% used, %.1fGB free", pct, free_gb)
    elif pct >= DISK_WARN_PCT:
        result["status"] = "warn"
        LOG.warning("disk warning: %.1f%% used, %.1fGB free", pct, free_gb)
    else:
        # Clear brake if it was set
        if PRESSURE_FLAG.exists():
            PRESSURE_FLAG.unlink()
            LOG.info("disk pressure cleared: %.1f%% used", pct)
    return result


def is_disk_pressure() -> bool:
    """Check if the disk pressure brake flag is set."""
    return PRESSURE_FLAG.exists()


def run_preflight() -> dict:
    """Run startup preflight checks. Returns verdict dict."""
    checks = {}
    verdict = "PASS"

    # 1. Disk check
    disk = check_disk()
    checks["disk"] = disk
    if disk["status"] == "brake":
        verdict = "FAIL"
    elif disk["status"] == "warn":
        verdict = "WARN"

    # 2. DB accessible
    try:
        conn = get_conn()
        conn.execute("SELECT 1").fetchone()
        checks["db_accessible"] = True

        # 3. Core tables exist
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        expected = {"events", "labels", "cursors", "claim_history"}
        missing = expected - tables
        checks["tables_present"] = len(missing) == 0
        if missing:
            checks["tables_missing"] = list(missing)
            verdict = "FAIL"

        # 4. WAL mode
        journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        checks["wal_mode"] = journal == "wal"
        if journal != "wal":
            verdict = "WARN" if verdict == "PASS" else verdict

        # 5. DB size
        db_path = DATA_DIR / "labeler.sqlite"
        if db_path.exists():
            size_mb = db_path.stat().st_size / (1024 * 1024)
            checks["db_size_mb"] = round(size_mb, 1)

        conn.close()
    except Exception as e:
        checks["db_accessible"] = False
        checks["db_error"] = str(e)
        verdict = "FAIL"

    checks["verdict"] = verdict
    LOG.info("preflight: %s (%s)", verdict, checks)
    return checks

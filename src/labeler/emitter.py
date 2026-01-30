import json
from pathlib import Path
from typing import List, Tuple

from .emit_mode import get_emit_audit_path


def emit_labels_to_audit(labels: List[dict], audit_path: str = "out/live_emits.jsonl"):
    """Write emitted labels to an audit JSONL file. This is the safe default for "live" emission.

    Real network emission can be added behind configuration (not enabled by default).
    """
    p = Path(audit_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a") as f:
        for l in labels:
            # store a compact deterministic JSON record
            f.write(json.dumps(l, sort_keys=True, separators=(",",":")) + "\n")
    return str(p)


def record_emit_decision(labels: List[dict], mode: str, audit_path: str = None) -> Tuple[str, str]:
    mode_norm = (mode or "detect-only").lower()
    if mode_norm not in ("detect-only", "emit", "quarantine"):
        mode_norm = "detect-only"
    status = "emitted" if mode_norm == "emit" else "suppressed"
    if audit_path is None:
        audit_path = get_emit_audit_path(mode_norm)
    recs = []
    for l in labels:
        rec = dict(l)
        rec["emit_status"] = status
        rec["emit_mode"] = mode_norm
        recs.append(rec)
    path = emit_labels_to_audit(recs, audit_path=audit_path)
    return path, status

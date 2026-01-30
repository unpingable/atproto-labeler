# Bluesky Labeler — MVP (Spec-Style Overview)

## 0. Abstract
This repository implements a minimal Bluesky (ATProto) labeler with a governed
drift detector, longitudinal tracking, and a label decision ledger. The system
separates **proposal** (rule detection) from **commit** (durable labeling), and
records sufficient receipts to audit decisions without depending on language
output as authority.

## 1. Scope
In-scope: ingestion, storage, labeling, longitudinal rechecks, and auditability.
Out-of-scope: external enforcement, production moderation policy, or LLM agency.

## 2. Terminology (Normative)
The following terms are used in the IETF sense:
- **MUST**: absolute requirement.
- **SHOULD**: recommended unless a documented exception applies.
- **MAY**: optional.

## 3. System Goals
The system MUST:
- Ingest and persist events durably.
- Derive labels deterministically from rules.
- Record a decision ledger for any committed label.
The system SHOULD:
- Fail closed on ambiguity (no state mutation).
- Emit only under explicit operator consent.

## 4. Components (At a Glance)
- Firehose consumer → `events`, `edges`, `cursors`.
- Label ingest worker (queryLabels).
- Drift detector + longitudinal rechecks.
- Decision ledger: `label_decisions` receipts with inputs and evidence hashes.

Quickstart
1. Create a virtualenv and install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Initialize the DB and run the API locally:

```bash
python -m labeler.main
# or
uvicorn labeler.main:app --host 0.0.0.0 --port 8000
```
Docker Compose (dev):

```bash
# Build and start the API, worker, simulated labeler, and replay
# (this will create a local `data` folder for the shared DuckDB)
docker compose up --build -d

# Check API health
curl http://localhost:8000/health

# Run the demo replay (one-shot) which inserts demo events into the DB
docker compose run --rm replay

# Start the ingestion worker (if not already running via `docker compose up`) and watch logs
docker compose logs -f worker

# You can also watch the simulated labeler logs
docker compose logs -f sim_labeler

# Inspect metrics and admin endpoints (admin endpoints protected by ADMIN_API_TOKEN if set):
curl http://localhost:8000/metrics
curl http://localhost:8000/admin/mappings  # admin token may be required
curl http://localhost:8000/admin/cooldowns # admin token may be required
```

Notes about the demo
- The simulated labeler (`sim_labeler`) will return 429 for the first `SIM_LABELER_RATE_LIMIT_COUNT` requests per URI, with `Retry-After` set to `SIM_LABELER_RETRY_AFTER` seconds. This exercises retry + cooldown behavior.
- `worker` performs periodic label ingestion against the simulated labeler (its `LABELER_ENDPOINT` is set to the sim_labeler service). The `replay` service seeds a few demo events so the worker has subjects to query.
- The shared `./data` directory contains the DuckDB database file used by both `api` and `worker` containers.
3. To run tests:

```bash
pytest -q
```

Files of interest
- `src/labeler/consumer.py` — firehose consumer stub
- `src/labeler/db.py` — DuckDB wrapper + schema
- `src/labeler/extractor.py` — edge extraction logic
- `src/labeler/labeler.py` — label query stub
- `src/labeler/main.py` — FastAPI app + startup hooks
- `src/labeler/longitudinal.py` — drift rules + recheck loop
- `src/labeler/claims.py` — fingerprinting + config hash
- `src/labeler/emitter.py` — audit-only emission
- `src/labeler/emit_mode.py` — emit gating

## 5. Invariants (Minimal)
- Language MAY propose; only evidence commits state (ledger recorded).
- Fingerprint version is a contract; changes require explicit bump.
- Emit is gated; default is detect-only and auditable.

## 6. Design Notes
- Schema is intentionally small and analytic-friendly.
- Consumer is resilient to replay (idempotent insert by event URI).
## 7. Observability
- Prometheus metrics exposed at `/metrics`.
- Decision receipts in `label_decisions`.
- Inspection ports: `/health/extended` and `/recent-decisions` (admin gated).

Claim-group rechecks (opt-in)
- Root-based scheduling remains the default to bound blast radius.
- Enable claim-group scheduling with `ENABLE_CLAIM_RECHECK=1`.
- `CLAIM_RECHECK_MAX_PER_RUN` caps claim-group work per loop.

Fingerprinting and migration policy
- Fingerprint version is a contract: `FP_VERSION` is a constant and must change only for intentional semantic changes.
- When fingerprint behavior changes, bump `FP_VERSION` and document the rationale in the PR.
- Migration rule: if you need a transition period, emit both old and new fingerprints for audit only, never for live labels.
- Configuration safety: a hash of relevant knobs is recorded in the label decision ledger (`label_decisions.config_hash`).

Emit modes (safe by default)
- `LABELER_EMIT_MODE=detect-only` (default): write would-emit records to `out/detect_only_emits.jsonl`.
- `LABELER_EMIT_MODE=emit`: requires `LABELER_EMIT_CONFIRM=true`.
- `LABELER_EMIT_MODE=quarantine`: force suppress emits and write to `out/quarantine_emits.jsonl`.
- `LABELER_EMIT_MAX_PER_RECHECK` caps emits per recheck; exceeding it flips to quarantine for that run.
- `LABELER_EMIT_AUDIT_DIR` overrides the output directory for emit audit files.

Rule activation budgets
- `LABELER_RULE_BUDGETS` defines per-rule caps (e.g., `provenance_laundering:5,assertiveness_increase:3`).
- `LABELER_RULE_BUDGET_WINDOW_HOURS` (default 24) controls the rolling window for budget checks.
- Budget breaches force quarantine emit for that recheck and annotate the audit records.

Contributing
- This scaffold is intentionally minimal. Next work: background consumer runner, label ingestion scheduler, strain detector, dashboards.

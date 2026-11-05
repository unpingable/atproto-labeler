# Governed labeler: parked offline reference

**PARKED WITH KNOWN LIMITATIONS.** This research artifact demonstrates synthetic
evidence → decision receipt → suppressed local audit/quarantine. It is not a
production labeler. No network label publisher is implemented. Development ends
with this finite closeout; further work requires a new owner decision.

[Closing snapshot](https://github.com/unpingable/atproto-labeler/tree/reference-closeout-20261105)
 · [Closeout and limitations](docs/CLOSEOUT-2026-11-05.md)
 · [Authorizing issue](https://github.com/unpingable/atproto-labeler/issues/1)

## Reproduce the offline example

Run these commands on the machine holding this checkout; they launch no server
and require no DID, account, credential, Redis or Jetstream connection. Dependency
installation is preparation; the demonstration and acceptance checks run offline.
The example uses disposable temporary SQLite databases and deletes its own files
on exit. Do not point it at an operational database.

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-offline.txt
PYTHONPATH=src python -m labeler.offline_demo
PYTHONPATH=src python -m pytest -q tests/test_closeout.py
```

The deterministic summary is:

```json
{"budget_mode":"quarantine","detect_mode":"detect-only","dual_gate":"pass","evidence_receipts":"pass","network_attempts":0,"quarantine_restart":"pass","state_loss":"unknown-history","status":"PASS","store_failure":"propagated"}
```

JSON spacing is immaterial. UUIDs and receipt timestamps are not asserted to be
byte-stable. The fixed input is [offline_closeout.jsonl](fixtures/offline_closeout.jsonl).
The command runs real rules and SQLite writes, verifies receipt input/evidence
fields, suppression and a zero per-rule budget trip, reads quarantine and health
state from a fresh interpreter, and checks that a fresh store has no prior
history. A deliberately unavailable quarantine table must raise an error. A Python
audit hook rejects network connects, name resolution and datagram sends; the final
summary requires zero attempts. Nothing is emitted to the network.

Tested: CPython 3.12.3, SQLite 3.45.1; pinned offline test dependencies are in
[requirements-offline.txt](requirements-offline.txt). This file records the
available test environment, not a newly resolved lock. Two upstream Pydantic
compatibility deprecation warnings are expected. The historical full suite and
`requirements.txt` are **not qualified**; see the exact failures in the closeout.

## Implemented behavior and authority

- `LABELER_EMIT_MODE` defaults to `detect-only`; unknown modes also resolve there.
  `emit` requires `LABELER_EMIT_CONFIRM=true` (also accepts `1`/`yes`). All modes
  only write local JSONL. The legacy `emitted` status means local audit selection,
  not publication or remote acceptance. Direct Python callers can pass an explicit
  mode to the recorder; they are trusted code, not an authenticated API.
- Suppressed payloads are also stored in SQLite `quarantine_emits`. Failure now
  propagates from the recorder. File and database writes are separate, so partial
  output remains possible. The legacy recheck wrapper still catches recorder
  failures; the demo detects missing receipts/quarantine rather than trusting it.
- Thread-root rechecks record evidence for the subject being labeled, rather than
  the last post in the thread. The prior duplicate empty receipt is suppressed on
  this path. Label, detailed receipt and quarantine writes are separate commits;
  they do **not** provide atomic governed commitment under failure.
- Per-rule caps (`LABELER_RULE_BUDGETS`, e.g. `provenance_laundering:0`) trip local
  quarantine when counts exceed a cap. Unspecified rules are uncapped. Malformed
  budget entries are ignored; this is not a fail-closed policy engine.
- Admin and per-subject HTTP routes reject an unset `ADMIN_API_TOKEN` with 503;
  missing/wrong credentials with a configured token return 401. Correct
  `Authorization: Bearer …` or `X-Admin-Token` permits access. Protected routes:
  `/admin/mappings`, `/admin/cooldowns`, `/recent-decisions`, `/quarantine/recent`,
  `/exposure/{did}`, `/strain/top`, `/labels/{subject_uri}`. Tests exercise the
  actual ASGI app using an in-process transport, with no listening socket.
- Public `/health` is liveness only. `/health/extended`, `/health/preflight` and
  `/metrics` are unauthenticated diagnostics; they can expose local paths, cursor,
  queue and runtime information. Framework API docs/schema are also public.
  They are not safe to expose on an untrusted network. No HTTP service is required
  for the accepted offline example. `python -m labeler.main` binds loopback;
  `FIREHOSE_AUTO_START=1` is refused as unqualified.
- `quarantine list/show` inspect local stored payloads under filesystem authority.
  There is **no quarantine release API or CLI**. `release quarantine/promote`
  only creates fingerprint-report manifests; it is not authorization, a signing
  process, or label release. No token or manifest can publish a label here.

## Storage and operating limits

SQLite defaults to `src/data/labeler.sqlite`; set `LABELER_DATA_DIR` before imports
to choose an isolated directory. It uses WAL, a 60-second busy timeout and
`synchronous=NORMAL`. SQLite serializes writes, but the legacy service has
multiple connections and is not a proven single-writer architecture. A directory
or volume must contain the SQLite database **and its WAL/SHM sidecars**. A live
file copy is not a qualified backup procedure.

The consumer has a bounded receive queue and batch writer, but its persisted
cursor follows received messages rather than a proved committed prefix. Queue
saturation/rollback can therefore leave gaps; shutdown cancels the drain and may
save a cursor ahead of completed work. Graceful lossless shutdown, reliable
acknowledgment, sustained operation and recovery are **UNQUALIFIED**. Do not start
its module, worker or ingestion flags as a service from this artifact.

Persisted loss counters/heartbeat survive ordinary reads and a fresh interpreter.
A consumer restart writes zero-initialized counters; lifetime totals and gap
history do not survive that restart reliably. A missing health row returns
`consumer: null` and may still report `ok`; this means unknown history, not proven
coverage. Total store loss loses receipts, quarantine, counters and cursors; no
historical continuity or recovery coverage is inferred.

The old [Compose file](docker-compose.yml), worker, DuckDB, Redis, retention,
label-query ingestion, fingerprint promotion rail and facts/Labelwatch bridge are
retained historical code, **UNQUALIFIED and outside the offline quickstart**.
Compose ports are loopback-bound, but `.env` is still consumed. Docker flattens
`src/` into `/app`, with a `/app/data` mount; repository execution instead defaults
to `src/data`. Multi-process/network behavior has not been accepted. Do not run Compose as a supported demo. No new
integration or live qualification is planned.

## Research context and provenance

The rules illustrate attribution loss, claim similarity and assertiveness signals;
they are not validated moderation judgments. Rules can compare with later posts,
produce false positives and repeat timestamp-varying labels. Fingerprint version
`v1` and configuration hashes are research identities, not a migration guarantee.
[Design notes](DESIGN_NOTES.md), [theory map](THEORY_TO_CODE.md) and [old roadmap](ROADMAP.md)
are historical or illustrative, not implemented authority or a commitment to work.

This artifact comes from the Δt Framework research program (Beck, 2025–2026),
starting with [The Coherence Criterion](https://zenodo.org/records/17726790).
[Tooling provenance](PROVENANCE.md) and original authorship are preserved.
Unless otherwise noted, licensing remains **MIT OR Apache-2.0**, at your option:
[MIT](LICENSE-MIT), [Apache-2.0](LICENSE-APACHE).

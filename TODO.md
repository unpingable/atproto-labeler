# TODO — Short Backlog (Normative)

## Priority: High (MUST/SHOULD)

- Label decision ledger (minimal receipts) ✅
  - Add `label_decisions` table with rule_id, fingerprint_version, inputs/evidence, config_hash, status. ✅
  - Insert decision rows on label commit and update on expiry. ✅
  - Minimal test: recheck_once inserts a decision with rule_id + fingerprint_version + inputs/evidence. ✅

- Mode fences (detect-only / emit / quarantine) ✅
  - Default to detect-only; emit requires explicit flag. ✅
  - Quarantine mode auto-disables emit if budgets spike. ✅ (caps + budget checks)
  - Acceptance: emit mode gated; audit log contains what would have been emitted. ✅

- Rule activation budgets ✅
  - Per-rule daily/fixture budget checks using decision ledger as source of truth. ✅
  - Acceptance: budget test fails on regressions and logs which rule(s) spiked. ✅
  - Rolling window checks enforce quarantine mode. ✅

- Regression suites ✅
  - Add fixture-driven regression tests (fingerprint stability + label decision ledger invariants). ✅

## Longitudinal Tracking (MVP) ✅

Minimum requirements (from to_tracking.md, normative):

1. Stable subject identity ✅
   - Implement `claim_fingerprint` and record `(authorDid, claim_fingerprint)` as claim identity.
   - Add `claim_history` table keyed by `(authorDid, claim_fingerprint)`.

2. Ordered history ✅
   - Enforce ordering on ingest (normalize/validate `createdAt`).
   - Use `createdAt` as the monotonic ordering field.

3. Stateful comparison ✅
   - Compare prior claim state by `(authorDid, claim_fingerprint)`.
   - Compute deltas between current and prior states.

4. Explicit deltas ✅
   - Expose deltas to rules (confidence/evidence/attribution).
   - Assertiveness increase rule wired into rechecks.

Required tests (minimal):
- Test A: Same claim, confidence increases without new evidence → assertiveness flag. ✅
- Test B: Attribution removed in later edit → provenance laundering flag. ✅

Implementation tasks (completed):
1. Add `claim_history` table and helpers. ✅
2. Compute `claim_fingerprint` at insert time. ✅
3. Add delta utilities. ✅
4. Add assertiveness rule. ✅
5. Add claim-group rechecks (opt-in dual scheduler). ✅
6. Add tests A & B. ✅
7. Document the process in `CONTRIBUTING.md` and add a golden test. ✅

- Expand golden tests & adversarial corpus ✅ (in progress)
  - Add more edge cases: currencies, percentages, dates, ordinal numbers, localized formats. ✅ (currencies/percentages/dates/localized)
  - Add near-miss cases to assert separation (e.g., 20 vs 200; 1.5% vs 15%). ✅
  - Acceptance: golden test suite demonstrates collision rate and near-miss separation with thresholds.

- Redis queue integration tests & metrics collection ✅ (in progress)
  - Add Redis-backed `recheck_queue` tests using `fakeredis` or a test Redis container. ✅
  - Add metrics tests for queue depth, recheck churn, and label insertion rates. ✅ (queue depth, recheck labels)
  - Acceptance: CI runs Redis-backed tests conditionally or via container; basic metrics assertions present.

## Priority: Medium (SHOULD/MAY)

- CLI fingerprint debug command (small audit tool) ✅
  - Print normalized fingerprint source + fingerprint + config knobs for given text or JSONL fixture.
  - Acceptance: deterministic output, tested, and added to docs.

- Instrument metrics for fingerprint stability
  - Collision rate on fixtures, churn rate for edits, drift sensitivity tests.
  - Acceptance: unit tests that compute those metrics over fixtures and assert stable behavior.

## Priority: Low (MAY)

- Small metrics dashboard (seeded from Prometheus metrics) for manual inspection.
- Policy docs: Add README section explaining fingerprint knobs and migration story. ✅

---

Notes:
- When changing fingerprinting heuristics, always bump `FP_VERSION` and store it to `claim_history` as `fingerprint_version`.
- When migrating, emit both old and new fingerprints for a period (audit-only) and add a migration test to assert behavior consistency.

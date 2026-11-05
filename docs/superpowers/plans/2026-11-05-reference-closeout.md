# Reference labeler finite closeout plan

**Goal:** Preserve an honest offline governed-labeling example, then park it.
**Spec:** https://github.com/unpingable/atproto-labeler/issues/1
**Architecture:** Keep the existing SQLite/rules/ledger/audit implementation. Fix exposed authority and demonstrated receipt/quarantine defects; narrow live-operation claims instead of expanding the service.
**Execution:** Inline, as explicitly dispatched by the owner; independent frozen-candidate review belongs to the root steward. No further delegation.

## Constraints and review focus

Offline synthetic inputs only; no credentials, network producers, production, push, tags, default-branch updates or issue mutation. At most 128 MiB of new lane artifacts. Preserve dual licensing and provenance. Review unset/wrong/correct token behavior; receipt evidence belongs to its subject; suppression cannot silently lose durable quarantine; database location and restart preserve synthetic records; no label network path exists.

## Steps

- [x] Pin baseline and inventory; run baseline with available offline interpreters.
- [x] Add real route tests for missing/wrong/correct administrative and per-subject tokens; observe RED, fail closed and close read connections; observe GREEN.
- [x] Add focused tests for subject-correct receipts, suppressed-store failures and configurable local database path. Observe RED, make minimal fixes, observe GREEN.
- [x] Add deterministic synthetic offline command and checks for receipts, quarantine, dual gate, budgets, restart/state loss and zero network attempts. No release authority is invented: existing CLI manifests do not release quarantined labels.
- [x] Replace overstated README/ingest claims with the tested offline contract and limitations. Record all baseline failures and unsupported historical integrations.
- [x] Run focused acceptance, full suite and compile checks; retain exact outcomes, freeze commit and hand review packet to root.

## Rulings

The full baseline with existing Driftwatch venv fails collection on an existing indentation error in distributed_ratelimit.py; system Python passes only 39 tests with 27 skips. Neither is represented as acceptance. Distributed Redis and live ingest are outside the advertised offline artifact, so their defects are disclosed rather than repaired. Existing dependency pins differ from the reused offline environment; record exact versions and do not install dependencies or claim the historical lock was reproduced.

## Results

26 focused tests and 11 selected existing seam tests pass. Demo PASS with zero network attempts. Full suite and compileall retain the existing distributed limiter indentation failure. Candidate handed to root for the single independent review; no publication by this lane.

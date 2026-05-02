# Ingest Invariants

A reference labeler is a **loss-sensitive stream consumer**, not a websocket inspection script.

This doc states the non-negotiable behaviors any serious labeler implementation must implement. The reference code in `src/labeler/consumer.py`, `src/labeler/db.py`, and `src/labeler/claims.py` exists to demonstrate these invariants. If a derived implementation violates one of them, it is not a labeler — it is a self-shedding fiction with JSON output.

## Sharp lines

> **A reference labeler must report when it is sampling instead of covering.**
>
> **A labeler that cannot represent degraded coverage is not operationally truthful.**
>
> **Process liveness is not coverage truth.** A green `/health` does not imply trustworthy output.

## 1. Ingest and backpressure

The labeler reads a real firehose. Real firehoses produce events faster than a naive single-writer SQLite consumer can absorb. The labeler must:

- Bound the internal queue between WS read and DB write.
- On saturation, choose **explicitly**: drop, delay, or sample. The reference uses `put_nowait` + drop, surfaced as a `dropped` counter.
- **Never block the WS read loop on the DB write path.** Blocking the read loop kills the WS keepalive ping, triggering reconnect churn and amplifying loss.
- Never silently absorb intake loss into `status=ok`.

The reference implementation drains in **batches** to a dedicated writer thread that owns a **persistent SQLite connection** and **commits once per batch**. A naive per-event commit cadence (the obvious shape) self-sheds under real load — see [JETSTREAM_INGEST_REALITIES.md in driftwatch](https://github.com/unpingable/atproto-driftwatch/blob/main/docs/JETSTREAM_INGEST_REALITIES.md) for the case study.

This is **correctness infrastructure, not micro-optimization.** A labeler without batching and a persistent writer connection will silently shed input on first contact with production traffic.

## 2. Coverage and degradation reporting

`/health/extended` must surface, at minimum:

- `drop_frac` — fraction of events shed at the consumer's own queue boundary, over a recent window.
- `events_per_sec` and a baseline (e.g., EWMA) — so a downstream observer can tell "low traffic" from "high loss."
- A `platform_health` state with explicit reasons (`high_drop_rate`, `lag_high`, `consumer_backlog`, etc.). Not a flat boolean.

A labeler that does not distinguish "process is up" from "intake is complete" is not operationally truthful. Both signals must exist and must be inspectable without reading the code.

## 3. Retention and archive semantics

Whatever the labeler persists, it must persist **honestly**:

- Document what is retained, for how long, and under what storage shape (raw rows / archives / rollups).
- If retention is enabled and active, expose enough state for an operator to verify it is keeping up: rows pruned per pass, oldest row age per table, archive day-file sizes.
- Archives produced during a degraded window are **conditioned by sampling loss**. The labeler must not retroactively pretend they represent full coverage.

## 4. Operational truthfulness

A serious labeler exposes, at minimum:

- Whether ingest is currently degraded
- Why (`gate_reasons`, `drop_frac`, lag, backlog)
- A recovery criterion that is *not* "the alert cleared"

An ops dashboard that shows green while `drop_frac=0.357` is a lie. A `/health` that returns `status=ok` while the queue is pegged at maxsize is a lie. The labeler must surface degradation *first-class*, not as a footnote.

## 5. Recovery semantics

A degraded window is not closed by a clean snapshot. The labeler (or its operator) must:

- Mark the start of degradation when it is detected.
- Mark the end only when explicit criteria hold for an explicit horizon. The reference horizon is `drop_frac < 0.05` sustained for **24 hours**.
- Preserve the marked window so downstream consumers know which artifacts were produced under loss.

A patch that restores throughput is not the same as a recovery proof. The degraded window stays open until the criteria-with-horizon are met, regardless of how clean the patch was.

## 6. Coverage honesty: the bucket vocabulary

A green recovery signal is **structurally inadmissible** if any known shedding path has no instrumented loss bucket, OR has a bucket that is currently zero only because the path isn't being exercised.

Health metrics are conditional admissions about *observed* buckets, not unconditional claims about reality. When a labeler fixes one shedding mechanism, throughput pressure typically migrates to whatever the next-narrowest constraint is. If the new shedding path wasn't instrumented when the original health metric was designed, the metric becomes a green light that the loss has *moved*, not stopped.

The reference labeler exposes `dropped` (queue-overflow shedding). A serious labeler under load may also discover:

- **Lock-conflict shedding** — a writer batch fails with `database is locked`; the rollback path drops events. Must surface as a `rollback_lost` (or equivalent) counter, summed into `drop_frac` for `platform_health` purposes.
- **Writer-thread starvation** — a single-threaded writer occupied by maintenance work cannot drain the ingest queue; `put_nowait` drops events. The bucket is the existing `dropped` counter, but the *cause* is internal scheduling, not upstream pressure. Surface chunk wall-times or maintenance-occupancy stats so the cause is diagnosable.

**Operational rule.** Before stamping recovery: enumerate every known shedding path and confirm each has an instrumented bucket. When recovery is reaffirmed, re-ask: what shedding paths exist now that weren't instrumented when this signal was designed? Recovery is parole, not exoneration.

This was the load-bearing failure in driftwatch's 2026-04-29 stamp: `drop_frac=0.0` sustained 27h, but the lock-conflict rollback path's loss surfaced only as `LOG.exception` lines that didn't aggregate. See `JETSTREAM_INGEST_REALITIES.md` for the case study and `specs/gaps/gap-spec-single-writer-invariant.md` for the architectural follow-up.

## 7. Maintenance shares the write contract

**Maintenance is mutation.** Retention, archive deletion, schema migration, WAL truncation, host-side cron writes — all subject to the same write-lock arbitration as ingest. "Background job" is not an immunity from the lock contest.

Two failure shapes the reference labeler implementer should know about:

1. **Maintenance on its own write connection** competes with the persistent ingest writer for SQLite's single write lock. Under load, contests can result in `database is locked` rollbacks for either side. Driftwatch lost ~444 events in 24h to this in 2026-04-30 before the rollback bucket was instrumented (see invariant 6).

2. **Naively routing maintenance through the ingest writer thread** eliminates the lock contest but introduces a worse failure: a long maintenance chunk (e.g. a multi-second `DELETE` of 5000 rows) blocks the writer thread, the ingest queue overflows, events drop. Driftwatch attempted this fix at commit `5850d01` and **failed acceptance under firehose load** — `drop_frac=67%`, no retention pass completed in 2.5h, and the rollback bucket showed zero (because shedding had migrated back to plain `QueueFull`).

**The doctrine that survived:**

> Single-writer is necessary but **insufficient**. Maintenance routed through the writer must be **prioritized, bounded, resumable, and preemptable**. The naive version failed by replacing lock-conflict loss with writer-thread starvation.

What that requires concretely:

- Ingest work has priority over maintenance work.
- Maintenance is chunked, with bounded wall-time per chunk (small enough that one chunk's writer-occupancy is shorter than the time it takes the ingest queue to fill at peak rate).
- Maintenance yields to ingest pressure (pre-chunk gate on backlog/queue-depth).
- Maintenance progress is checkpointable so it can resume after preemption.
- Maintenance pauses entirely under sustained ingest pressure rather than starve ingest.

Until a labeler implements this scheduling layer, the safe state is **maintenance on its own connection** (accepting the lock-conflict shedding from invariant 6 — at least it's instrumented) rather than naive routing through the writer thread. Don't confuse serialization with scheduling.

## What this rules out

A reference labeler implementation **may not**:

- Open a fresh SQLite connection per event in the hot path.
- Commit per event in the hot path.
- Drop events without recording the drop in a counter that surfaces in `/health/extended`.
- Report `status=ok` while shedding intake.
- Retroactively normalize archives produced under degraded sampling.
- Treat platform_health as advisory-only when intake completeness is at stake.

These are not style preferences. They are the difference between a labeler and a self-shedding script.

## Pointers

- Reference implementation: `src/labeler/consumer.py` (drain + writer thread), `src/labeler/db.py` (`_txn` variants), `src/labeler/claims.py` (`add_claim_history_txn`).
- Case study (driftwatch April 2026 incident): the broken-by-default pattern this doc exists to prevent.

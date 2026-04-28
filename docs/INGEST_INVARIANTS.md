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

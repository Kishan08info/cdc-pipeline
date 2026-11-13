# Change Data Capture (CDC) Pipeline

**SYNTHETIC DATA** — all customers, orders, and change events are fictional,
generated with a fixed seed. This is a portfolio demo of CDC mechanics, not
production CDC experience and not work for any employer.

## Business question

How do you keep a warehouse in sync with a source database that keeps
changing — without re-copying the whole database, and without corrupting the
target when the change stream itself is messy (duplicate deliveries,
out-of-order events, deletes, rows deleted then re-created)?

## Why this theme

Picked from the latest Data Engineer job descriptions (Nov 13, 2026 pool):
- **M&T Bank — Senior Data Engineer** lists Kafka/CDC as a preferred
  qualification, with Snowflake/dbt/Spark as the core stack.
- **Solve Intelligence — Data Engineer** wants "large-scale resumable
  pipelines" over large messy datasets — the checkpoint/resume here is that
  mechanism at small scale.
- **Flow Traders — Core Data Engineer** asks for data-quality practice
  (tests, contracts, lineage, observability) — the reconciliation step is
  the quality gate for this pipeline.

Nothing in the portfolio covered change capture yet: project 13 did
incremental loads via an API watermark (no deletes, no ordering), project 14
consumed CDC feeds but never built the capture/apply layer, and project 12
was a streaming consumer with no deletes or resume. This project is the
missing piece: the log → apply → reconcile loop.

## What it does

```
source.db (snapshot + change_log + delivery_log)
        │  snapshot load
        ▼
target.db (warehouse: customers, orders)
        │  CDC consumer: dedupe · last-write-wins · tombstones · checkpoint
        ▼
reconciliation vs independent replay of the change log
```

1. **Generate** (`generate_changes.py`) — a synthetic source: 500 customers
   and 1,500 orders as the snapshot, plus a 640-event change log
   (260 customer updates, 170 order updates, 60 + 100 inserts,
   15 + 15 deletes, 10 delete-then-re-insert "resurrections"). The
   *delivery* log — what the consumer actually receives — is deliberately
   messy: 25 duplicate deliveries and 15 delayed deliveries (an earlier
   event for a row arriving after a later one).
2. **Snapshot load** (`cdc_consumer.py snapshot`) — initial copy of the
   snapshot tables into a fresh target warehouse.
3. **CDC apply** (`cdc_consumer.py cdc`) — per delivery event:
   - **Dedupe:** each event seq is applied at most once (`applied_events`).
   - **Last-write-wins:** every target row carries `_cdc_seq`; an event
     with seq ≤ the row's last seq is stale (out-of-order) and skipped.
   - **Deletes are tombstones:** the row is removed and
     `(table, pk, seq)` recorded, so a later UPDATE with a higher seq
     resurrects the row while an older UPDATE stays skipped.
   - **Checkpoint:** the highest seq seen is persisted, so a crash
     mid-stream resumes exactly where it stopped.
4. **Reconcile** (`reconcile.py`) — ground truth is computed *without* the
   consumer: the change log is replayed in seq order over the snapshot, then
   the target is compared row-for-row (counts + SHA-256 checksums).

## Results (from an actual run — `python3 run.py`)

Seeded and deterministic: deleting `outputs/` and re-running reproduces
`summary.json` **byte-identically**; 10/10 pytest checks pass.

| Metric | Value |
|---|---|
| Snapshot | 500 customers, 1,500 orders |
| Change events generated | 640 |
| Deliveries (incl. 25 duplicates) | 665 |
| Crash run (simulated, `--stop-after 200`) | 200 read, 188 applied, checkpoint seq 200 |
| Resume run | 665 read, 437 applied, checkpoint seq 640 |
| Idempotency run (3rd pass) | 0 applied — nothing double-applied |
| Unique events applied (of 640) | 625 (15 stale-skipped) |
| Duplicate deliveries skipped | 25 injected + 200 re-reads on resume |
| Out-of-order (stale) events skipped | 15 |
| Deletes applied | 33 of 40 in the log |
| Final target | 545 customers, 1,585 orders |
| Reconciliation | **exact match** — checksums `4b8409271a90a65b` (customers), `09bf27e7cf2fa008` (orders); 0 missing, 0 extra, 0 value diffs |

Two findings worth calling out:

- **7 of the 40 deletes never "applied" — correctly.** 7 of the 10
  resurrection DELETEs were among the delayed deliveries: they arrived
  *after* their re-INSERT, so last-write-wins skipped them as stale. The
  final state is still exact (all 10 resurrected customers present), and
  the consumer's resurrection counter (3) counts only the INSERTs that
  actually found a tombstone. A naive "apply every event in arrival order"
  consumer would have deleted those 7 rows after re-inserting them.
- **Resume costs a full re-read, and that's the honest trade-off.**
  The resume run re-reads all 665 deliveries and skips the 200 already
  applied via `applied_events`. A production consumer would resume from a
  per-partition offset instead; the checkpoint here is a single
  "highest seq seen", which is simple and correct but re-reads.

## Run it

```bash
python3 run.py            # full demo: generate → snapshot → crash → resume → reconcile
python3 -m pytest tests/ -v   # 10 checks
```

Runtime is stdlib-only (sqlite3). Tests need pytest.

## Layout

- `cdc_lib.py` — shared schema, synthetic data generation, expected-state replay, checksums
- `generate_changes.py` — builds `outputs/source.db` (snapshot + change log + delivery log)
- `cdc_consumer.py` — snapshot load + CDC apply (dedupe, last-write-wins, tombstones, checkpoint)
- `reconcile.py` — independent reconciliation against a seq-ordered replay
- `run.py` — orchestrates the full demo and writes `outputs/summary.json`
- `tests/test_cdc.py` — 10 pytest checks (dedupe, staleness, deletes, resurrection, crash-resume, idempotency, tamper detection, determinism)
- `outputs/` — `source.db`, `target.db`, `summary.json`, `reconciliation.json` from the latest run

## Honest labelling

- The source is a SQLite table standing in for a database binlog/WAL, and
  the delivery log stands in for a Kafka topic — no Kafka broker, no
  Debezium, no production CDC tool is used or claimed.
- SQLite stands in for the warehouse; no Snowflake/BigQuery/Redshift.
- The scale (640 events) demonstrates the mechanics, not throughput.

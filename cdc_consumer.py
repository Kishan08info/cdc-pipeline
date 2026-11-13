"""CDC consumer: apply a source change log to a target warehouse.

Semantics (the part that matters in a real CDC pipeline):
- Dedupe: each event seq is applied at most once (applied_events table).
  A redelivered event is skipped, never double-applied.
- Last-write-wins per row: every target row carries _cdc_seq, the seq of
  the last event applied to it. An event with seq <= that value is stale
  (arrived out of order) and is skipped.
- Deletes are tombstones: the row is removed and (table, pk, seq) is
  recorded, so a later UPDATE with a HIGHER seq resurrects the row while
  an older UPDATE stays skipped.
- Checkpoint: the highest seq seen is persisted after every batch, so a
  crash mid-stream resumes exactly where it stopped (--stop-after
  simulates the crash; re-running the consumer finishes the stream and a
  third run applies 0 new events).

Phases:
  snapshot  — copy the source snapshot tables into a fresh target.
  cdc       — apply delivery_log in delivery order (the messy stream).

Usage:
  python3 cdc_consumer.py snapshot --source outputs/source.db --target outputs/target.db
  python3 cdc_consumer.py cdc --source outputs/source.db --target outputs/target.db [--stop-after N]
"""
import argparse
import json
import sqlite3

from cdc_lib import (CUSTOMER_COLUMNS, ORDER_COLUMNS, TARGET_SCHEMA,
                     connect, table_rows)

TABLES = {
    "customers": (CUSTOMER_COLUMNS, "customer_id"),
    "orders": (ORDER_COLUMNS, "order_id"),
}


def snapshot_load(source_path, target_path):
    """Initial load: copy source snapshot tables into a fresh target."""
    src = connect(source_path)
    tgt = connect(target_path)
    tgt.executescript(TARGET_SCHEMA)
    counts = {}
    for table, (cols, pk) in TABLES.items():
        rows = table_rows(src, table, cols, pk)
        tgt.execute(f"DELETE FROM {table}")
        tgt.executemany(
            f"INSERT INTO {table} ({', '.join(cols)}, _cdc_seq) "
            f"VALUES ({', '.join('?' * len(cols))}, 0)",
            [[r[c] for c in cols] for r in rows])
        counts[table] = len(rows)
    tgt.execute("DELETE FROM applied_events")
    tgt.execute("DELETE FROM tombstones")
    tgt.execute("DELETE FROM checkpoint")
    tgt.execute("INSERT INTO checkpoint (key, value) VALUES ('last_seq_seen', 0)")
    tgt.execute("INSERT INTO checkpoint (key, value) VALUES ('snapshot_rows', ?)",
                (sum(counts.values()),))
    tgt.commit()
    src.close()
    tgt.close()
    return counts


def _row_last_seq(tgt, table, pk_col, pk):
    row = tgt.execute(
        f"SELECT _cdc_seq FROM {table} WHERE {pk_col} = ?", (pk,)
    ).fetchone()
    if row is not None:
        return row[0]
    tomb = tgt.execute(
        "SELECT seq FROM tombstones WHERE table_name = ? AND pk = ?",
        (table, pk)).fetchone()
    return tomb[0] if tomb is not None else -1


def apply_cdc(source_path, target_path, stop_after=None):
    """Apply delivery_log events to the target. Returns a metrics dict."""
    src = connect(source_path)
    tgt = connect(target_path)
    metrics = {"deliveries_read": 0, "applied": 0, "duplicates_skipped": 0,
               "stale_skipped": 0, "deletes_applied": 0,
               "resurrections": 0, "inserts": 0, "updates": 0}
    deliveries = src.execute(
        "SELECT delivery_id, seq FROM delivery_log ORDER BY delivery_id"
    ).fetchall()
    for delivery_id, seq in deliveries:
        if stop_after is not None and metrics["deliveries_read"] >= stop_after:
            break
        metrics["deliveries_read"] += 1
        seen = tgt.execute(
            "SELECT 1 FROM applied_events WHERE seq = ?", (seq,)).fetchone()
        if seen is not None:
            metrics["duplicates_skipped"] += 1
            continue
        ev = src.execute(
            "SELECT * FROM change_log WHERE seq = ?", (seq,)).fetchone()
        table, op, pk = ev["table_name"], ev["op"], ev["pk"]
        cols, pk_col = TABLES[table]
        last_seq = _row_last_seq(tgt, table, pk_col, pk)
        if seq <= last_seq:
            # Out-of-order arrival: a newer event for this row already won.
            metrics["stale_skipped"] += 1
            tgt.execute("INSERT OR IGNORE INTO applied_events (seq) VALUES (?)",
                        (seq,))
            continue
        if op == "DELETE":
            existed = tgt.execute(
                f"SELECT 1 FROM {table} WHERE {pk_col} = ?", (pk,)).fetchone()
            tgt.execute(f"DELETE FROM {table} WHERE {pk_col} = ?", (pk,))
            tgt.execute(
                "INSERT OR REPLACE INTO tombstones (table_name, pk, seq) "
                "VALUES (?, ?, ?)", (table, pk, seq))
            metrics["deletes_applied"] += 1
        else:
            payload = json.loads(ev["payload"])
            was_tombstoned = tgt.execute(
                "SELECT 1 FROM tombstones WHERE table_name = ? AND pk = ?",
                (table, pk)).fetchone()
            tgt.execute(
                f"INSERT OR REPLACE INTO {table} "
                f"({', '.join(cols)}, _cdc_seq) "
                f"VALUES ({', '.join('?' * len(cols))}, ?)",
                [payload[c] for c in cols] + [seq])
            tgt.execute(
                "DELETE FROM tombstones WHERE table_name = ? AND pk = ?",
                (table, pk))
            if op == "INSERT":
                metrics["inserts"] += 1
                if was_tombstoned is not None:
                    metrics["resurrections"] += 1
            else:
                metrics["updates"] += 1
        tgt.execute("INSERT OR IGNORE INTO applied_events (seq) VALUES (?)",
                    (seq,))
        metrics["applied"] += 1
        # Checkpoint: highest seq seen so far (persists across crashes).
        tgt.execute(
            "UPDATE checkpoint SET value = MAX(value, ?) "
            "WHERE key = 'last_seq_seen'", (seq,))
        if metrics["deliveries_read"] % 100 == 0:
            tgt.commit()
    tgt.commit()
    row = tgt.execute(
        "SELECT value FROM checkpoint WHERE key = 'last_seq_seen'").fetchone()
    metrics["checkpoint_last_seq_seen"] = row[0] if row else None
    for table, (cols, pk) in TABLES.items():
        metrics[f"target_{table}"] = tgt.execute(
            f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    src.close()
    tgt.close()
    return metrics


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="phase", required=True)
    p1 = sub.add_parser("snapshot")
    p1.add_argument("--source", required=True)
    p1.add_argument("--target", required=True)
    p2 = sub.add_parser("cdc")
    p2.add_argument("--source", required=True)
    p2.add_argument("--target", required=True)
    p2.add_argument("--stop-after", type=int, default=None)
    args = ap.parse_args()
    if args.phase == "snapshot":
        print(snapshot_load(args.source, args.target))
    else:
        print(apply_cdc(args.source, args.target, args.stop_after))

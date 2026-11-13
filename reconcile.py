"""Reconcile the CDC target against the source, independently.

Ground truth is computed WITHOUT the consumer: the change log is replayed
in seq order over the snapshot (cdc_lib.replay_expected_state), then the
target tables are compared row-for-row (counts + sha256 checksums). Any
mismatch is listed with the offending pks.

Usage: python3 reconcile.py --source outputs/source.db --target outputs/target.db
"""
import argparse
import json

from cdc_lib import (CUSTOMER_COLUMNS, ORDER_COLUMNS, connect,
                     replay_expected_state, row_checksum, table_rows)

TABLES = {
    "customers": (CUSTOMER_COLUMNS, "customer_id"),
    "orders": (ORDER_COLUMNS, "order_id"),
}


def reconcile(source_path, target_path):
    src = connect(source_path)
    tgt = connect(target_path)
    snapshot = {
        "customers": table_rows(src, "customers", CUSTOMER_COLUMNS, "customer_id"),
        "orders": table_rows(src, "orders", ORDER_COLUMNS, "order_id"),
    }
    events = [dict(r) for r in src.execute(
        "SELECT * FROM change_log ORDER BY seq").fetchall()]
    expected = replay_expected_state(snapshot, events)

    report = {"tables": {}, "match": True, "mismatched_pks": {}}
    for table, (cols, pk) in TABLES.items():
        exp_rows = list(expected[table].values())
        tgt_rows = table_rows(tgt, table, cols, pk)
        exp_sum = row_checksum(exp_rows, cols, pk)
        tgt_sum = row_checksum(tgt_rows, cols, pk)
        exp_pks = {r[pk] for r in exp_rows}
        tgt_pks = {r[pk] for r in tgt_rows}
        missing = sorted(exp_pks - tgt_pks)
        extra = sorted(tgt_pks - exp_pks)
        # Value-level diff for pks present on both sides.
        exp_map = {r[pk]: r for r in exp_rows}
        tgt_map = {r[pk]: r for r in tgt_rows}
        value_diffs = sorted(
            pk for pk in exp_pks & tgt_pks
            if any(str(exp_map[pk][c]) != str(tgt_map[pk][c]) for c in cols))
        table_ok = (exp_sum == tgt_sum and not missing and not extra
                    and not value_diffs)
        report["tables"][table] = {
            "expected_rows": len(exp_rows),
            "target_rows": len(tgt_rows),
            "expected_checksum": exp_sum,
            "target_checksum": tgt_sum,
            "missing_in_target": missing,
            "extra_in_target": extra,
            "value_diffs": value_diffs,
            "match": table_ok,
        }
        if not table_ok:
            report["match"] = False
            report["mismatched_pks"][table] = {
                "missing": missing, "extra": extra, "value_diffs": value_diffs}
    # Delete cross-check: every DELETE that was not later superseded by a
    # re-INSERT of the same pk must leave the row absent from the target.
    deletes = [e for e in events if e["op"] == "DELETE"]
    report["deletes_in_log"] = len(deletes)
    stayed_deleted = 0
    resurrected = 0
    for e in deletes:
        pk_col = TABLES[e["table_name"]][1]
        in_target = tgt.execute(
            f"SELECT 1 FROM {e['table_name']} WHERE {pk_col} = ?",
            (e["pk"],)).fetchone() is not None
        in_expected = e["pk"] in expected[e["table_name"]]
        if in_expected:
            resurrected += 1  # deleted then re-inserted; must be present
        elif not in_target:
            stayed_deleted += 1
    report["deletes_stayed_deleted"] = stayed_deleted
    report["deletes_resurrected"] = resurrected
    src.close()
    tgt.close()
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True)
    ap.add_argument("--target", required=True)
    args = ap.parse_args()
    print(json.dumps(reconcile(args.source, args.target), indent=2))

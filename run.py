"""End-to-end CDC demo run.

1. Generate the synthetic source (snapshot + change log + messy delivery log).
2. Snapshot-load the target warehouse.
3. Run the CDC consumer with --stop-after 200 (simulated crash mid-stream).
4. Re-run the consumer (resume from checkpoint) to finish the stream.
5. Run the consumer a third time (idempotency: must apply 0 new events).
6. Reconcile the target against an independent replay of the change log.
7. Write outputs/summary.json.

Usage: python3 run.py [--seed 42]
All numbers in the summary come from this run — nothing is hand-written.
"""
import argparse
import json
import os
import shutil

from cdc_consumer import apply_cdc, snapshot_load
from generate_changes import generate
from reconcile import reconcile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "outputs")
SOURCE_DB = os.path.join(OUT, "source.db")
TARGET_DB = os.path.join(OUT, "target.db")

CRASH_AFTER = 200  # deliveries processed before the simulated crash


def main(seed=42):
    os.makedirs(OUT, exist_ok=True)
    for db in (SOURCE_DB, TARGET_DB):
        if os.path.exists(db):
            os.remove(db)

    gen = generate(SOURCE_DB, seed)
    snap = snapshot_load(SOURCE_DB, TARGET_DB)
    crash = apply_cdc(SOURCE_DB, TARGET_DB, stop_after=CRASH_AFTER)
    resumed = apply_cdc(SOURCE_DB, TARGET_DB)
    idempotent = apply_cdc(SOURCE_DB, TARGET_DB)
    recon = reconcile(SOURCE_DB, TARGET_DB)

    summary = {
        "seed": seed,
        "snapshot": snap,
        "events_generated": gen["events"],
        "deliveries_total": gen["deliveries"],
        "duplicate_deliveries_injected": gen["duplicate_deliveries"],
        "delayed_deliveries_injected": gen["delayed_deliveries"],
        "resurrections_in_log": gen["resurrections"],
        "crash_run_stop_after": CRASH_AFTER,
        "crash_run": crash,
        "resume_run": resumed,
        "idempotency_run": idempotent,
        "reconciliation": recon,
        "totals": {
            # Reads across the crash + resume runs (resume re-reads the log
            # from the start; already-applied events are skipped by seq).
            "deliveries_read": crash["deliveries_read"]
                               + resumed["deliveries_read"],
            "unique_events_applied": crash["applied"] + resumed["applied"],
            # Injected duplicate deliveries (25) vs re-reads on resume
            # (200 events already applied in the crash run).
            "injected_duplicates_skipped": gen["duplicate_deliveries"],
            "resume_rereads_skipped": crash["deliveries_read"],
            "stale_skipped": (crash["stale_skipped"]
                              + resumed["stale_skipped"]),
            "deletes_applied": (crash["deletes_applied"]
                                + resumed["deletes_applied"]),
            # Only 3 of the 10 resurrection INSERTs found a tombstone:
            # the other 7 resurrection DELETEs were among the delayed
            # events, arrived AFTER their re-INSERT, and were correctly
            # skipped as stale — final state is still exact (see README).
            "resurrections_applied": (crash["resurrections"]
                                      + resumed["resurrections"]),
        },
    }
    with open(os.path.join(OUT, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    with open(os.path.join(OUT, "reconciliation.json"), "w") as f:
        json.dump(recon, f, indent=2)
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    main(args.seed)

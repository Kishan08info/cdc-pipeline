"""Tests for the CDC pipeline (stdlib + pytest).

Every test runs against freshly generated synthetic data (seeded), so the
suite is deterministic. Run: python -m pytest tests/ -v
"""
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cdc_consumer import apply_cdc, snapshot_load  # noqa: E402
from cdc_lib import connect  # noqa: E402
from generate_changes import generate  # noqa: E402
from reconcile import reconcile  # noqa: E402


def _setup(tmp_path, seed=42):
    os.makedirs(str(tmp_path), exist_ok=True)
    src = str(tmp_path / "source.db")
    tgt = str(tmp_path / "target.db")
    gen = generate(src, seed)
    snapshot_load(src, tgt)
    return src, tgt, gen


def test_snapshot_load_matches_source(tmp_path):
    src, tgt, _ = _setup(tmp_path)
    s, t = connect(src), connect(tgt)
    for table in ("customers", "orders"):
        assert s.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == \
            t.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    assert t.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 500
    assert t.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 1500


def test_full_run_reconciles_exactly(tmp_path):
    src, tgt, gen = _setup(tmp_path)
    crash = apply_cdc(src, tgt, stop_after=200)
    assert crash["deliveries_read"] == 200
    resumed = apply_cdc(src, tgt)
    assert resumed["applied"] > 0
    third = apply_cdc(src, tgt)
    assert third["applied"] == 0, "third run must apply nothing (idempotent)"
    recon = reconcile(src, tgt)
    assert recon["match"] is True, json.dumps(recon["mismatched_pks"], indent=2)
    assert recon["deletes_in_log"] == 40  # 15 cust + 15 order + 10 resurrection
    assert recon["deletes_stayed_deleted"] == 30
    assert recon["deletes_resurrected"] == 10


def test_duplicate_deliveries_skipped(tmp_path):
    src, tgt, gen = _setup(tmp_path)
    assert gen["duplicate_deliveries"] == 25
    m = apply_cdc(src, tgt)
    # Every duplicate delivery must be skipped, never double-applied.
    assert m["duplicates_skipped"] == 25
    # 15 delayed events arrive stale and are skipped; everything else applies.
    assert m["applied"] == gen["events"] - m["stale_skipped"]
    assert m["applied"] + m["duplicates_skipped"] + m["stale_skipped"] \
        == gen["deliveries"]


def test_stale_delayed_events_skipped(tmp_path):
    src, tgt, gen = _setup(tmp_path)
    assert gen["delayed_deliveries"] == 15
    m = apply_cdc(src, tgt)
    # Each delayed event arrives after a newer event for the same row.
    assert m["stale_skipped"] == 15


def test_resume_after_crash_matches_uninterrupted(tmp_path):
    src, tgt, _ = _setup(tmp_path)
    apply_cdc(src, tgt, stop_after=200)
    apply_cdc(src, tgt)
    recon_crash = reconcile(src, tgt)

    src2, tgt2, _ = _setup(tmp_path / "b")
    apply_cdc(src2, tgt2)
    recon_clean = reconcile(src2, tgt2)

    assert recon_crash["match"] is True
    assert recon_clean["match"] is True
    assert (recon_crash["tables"]["customers"]["target_checksum"]
            == recon_clean["tables"]["customers"]["target_checksum"])
    assert (recon_crash["tables"]["orders"]["target_checksum"]
            == recon_clean["tables"]["orders"]["target_checksum"])


def test_delete_then_resurrect(tmp_path):
    src, tgt, _ = _setup(tmp_path)
    m = apply_cdc(src, tgt)
    # All 10 resurrected customers are present in the target again.
    # NOTE: the consumer's "resurrections" counter is only 3, not 10:
    # 7 of the 10 resurrection DELETEs were among the delayed deliveries,
    # so they arrived AFTER their re-INSERT and were correctly skipped as
    # stale — no tombstone ever existed for those rows. The final state
    # is still exactly right (all 10 present), which is what matters.
    assert m["resurrections"] == 3
    s = connect(src)
    resurrected = s.execute(
        "SELECT pk FROM change_log WHERE op='DELETE' AND table_name='customers'"
    ).fetchall()
    t = connect(tgt)
    present = sum(
        1 for (pk,) in resurrected
        if t.execute("SELECT 1 FROM customers WHERE customer_id = ?",
                     (pk,)).fetchone() is not None)
    assert present == 10


def test_delete_of_unknown_row_is_safe(tmp_path):
    src, tgt, _ = _setup(tmp_path)
    # Inject a DELETE for a pk that never existed, as a fresh event.
    s = connect(src)
    max_seq = s.execute("SELECT MAX(seq) FROM change_log").fetchone()[0]
    s.execute(
        "INSERT INTO change_log (seq, table_name, op, pk, payload, "
        "before_payload, event_ts) VALUES (?, 'customers', 'DELETE', "
        "'C9999', NULL, NULL, '2026-06-30T00:00:00')", (max_seq + 1,))
    max_del = s.execute("SELECT MAX(delivery_id) FROM delivery_log").fetchone()[0]
    s.execute("INSERT INTO delivery_log (delivery_id, seq, delivered_at) "
              "VALUES (?, ?, '2026-06-30T00:00:00')", (max_del + 1, max_seq + 1))
    s.commit()
    m = apply_cdc(src, tgt)
    # 40 deletes in the log, but 7 resurrection DELETEs arrive stale
    # (delayed past their re-INSERT) and are skipped, so 33 apply + the
    # injected one = 34. The unknown pk leaves a tombstone, no crash,
    # and reconciliation still matches exactly.
    assert m["deletes_applied"] == 34
    assert reconcile(src, tgt)["match"] is True


def test_checkpoint_persists_highest_seq(tmp_path):
    src, tgt, gen = _setup(tmp_path)
    apply_cdc(src, tgt)
    t = connect(tgt)
    ckpt = t.execute(
        "SELECT value FROM checkpoint WHERE key='last_seq_seen'").fetchone()[0]
    assert ckpt == gen["events"]
    applied = t.execute("SELECT COUNT(*) FROM applied_events").fetchone()[0]
    assert applied == gen["events"]


def test_reconciliation_catches_tampering(tmp_path):
    src, tgt, _ = _setup(tmp_path)
    apply_cdc(src, tgt)
    assert reconcile(src, tgt)["match"] is True
    t = connect(tgt)
    t.execute("UPDATE orders SET amount = 0.01 WHERE order_id = 'O00001'")
    t.commit()
    recon = reconcile(src, tgt)
    assert recon["match"] is False
    assert "O00001" in recon["tables"]["orders"]["value_diffs"]


def test_generation_is_deterministic(tmp_path):
    src1, _, gen1 = _setup(tmp_path / "a")
    src2, _, gen2 = _setup(tmp_path / "b")
    assert gen1 == gen2
    c1 = connect(src1)
    c2 = connect(src2)
    for table in ("customers", "orders", "change_log", "delivery_log"):
        assert c1.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == \
            c2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

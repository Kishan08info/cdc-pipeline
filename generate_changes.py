"""Generate the synthetic source database: snapshot + change log + delivery log.

All data is SYNTHETIC (fictional SaaS customers/orders), seeded (seed=42) so
a re-run reproduces the same source exactly.

Change-log mix (per table):
- UPDATEs (plan/email/status/amount changes)
- INSERTs (new customers/orders)
- DELETEs (customers and orders removed at the source)
- Resurrections: DELETE followed later by an INSERT of the same pk
  (a row deleted then re-created — the consumer must bring it back)

Delivery log (what the consumer actually receives) is intentionally messy,
the way a real CDC stream is:
- 25 duplicate deliveries (same event delivered twice)
- 15 delayed deliveries (an earlier event for a row arrives AFTER a later
  event for the same row — the consumer must skip it as stale)

Usage: python3 generate_changes.py --out outputs/source.db
"""
import argparse
import json
import os
import random
import sqlite3
from datetime import datetime, timedelta

from cdc_lib import (CUSTOMER_COLUMNS, ORDER_COLUMNS, PLANS, REGIONS,
                     SOURCE_SCHEMA, STATUSES, connect, make_rng,
                     snapshot_customers, snapshot_orders)

N_CUSTOMERS = 500
N_ORDERS = 1500

# Change-log mix
N_CUST_UPDATES = 260
N_ORDER_UPDATES = 170
N_CUST_INSERTS = 60
N_ORDER_INSERTS = 100
N_CUST_DELETES = 15
N_ORDER_DELETES = 15
N_RESURRECTIONS = 10          # delete + later insert of the same customer pk
N_DUPLICATE_DELIVERIES = 25
N_DELAYED_DELIVERIES = 15


def _ts(base, minutes):
    return (base + timedelta(minutes=minutes)).isoformat(timespec="seconds")


def build_change_log(rng, customers, orders):
    """Return (events, stats). Events are dicts with seq assigned in order."""
    events = []
    seq = 0
    base = datetime(2026, 1, 1, 0, 0, 0)
    cust_by_id = {c["customer_id"]: dict(c) for c in customers}
    order_by_id = {o["order_id"]: dict(o) for o in orders}
    live_cust = list(cust_by_id)
    live_orders = list(order_by_id)
    stats = {"resurrections": 0}

    def add(table, op, pk, payload, before, minute):
        nonlocal seq
        seq += 1
        events.append({
            "seq": seq, "table_name": table, "op": op, "pk": pk,
            "payload": json.dumps(payload) if payload is not None else None,
            "before_payload": json.dumps(before) if before is not None else None,
            "event_ts": _ts(base, minute),
        })
        return seq

    minute = 0

    def next_minute():
        nonlocal minute
        minute += rng.randint(5, 180)
        return minute

    # --- Resurrections first (delete + later insert of same pk) ---
    resurrect_ids = rng.sample(live_cust, N_RESURRECTIONS)
    for cid in resurrect_ids:
        before = dict(cust_by_id[cid])
        add("customers", "DELETE", cid, None, before, next_minute())
        new_row = dict(before)
        new_row["plan"] = rng.choice([p for p in PLANS if p != before["plan"]])
        new_row["email"] = before["email"].replace("@example.com", "+v2@example.com")
        add("customers", "INSERT", cid, new_row, None, next_minute())
        cust_by_id[cid] = new_row
        stats["resurrections"] += 1

    # --- Customer updates ---
    for cid in rng.sample(live_cust, N_CUST_UPDATES):
        before = dict(cust_by_id[cid])
        after = dict(before)
        field = rng.choice(["plan", "email", "region"])
        if field == "plan":
            after["plan"] = rng.choice([p for p in PLANS if p != before["plan"]])
        elif field == "email":
            after["email"] = before["email"].replace("@example.com", "+new@example.com")
        else:
            after["region"] = rng.choice([r for r in REGIONS if r != before["region"]])
        add("customers", "UPDATE", cid, after, before, next_minute())
        cust_by_id[cid] = after

    # --- Customer deletes (stay deleted) ---
    for cid in rng.sample([c for c in live_cust if c not in resurrect_ids],
                          N_CUST_DELETES):
        add("customers", "DELETE", cid, None, dict(cust_by_id[cid]), next_minute())
        cust_by_id.pop(cid, None)

    # --- Customer inserts ---
    next_cust = N_CUSTOMERS + 1
    for _ in range(N_CUST_INSERTS):
        cid = f"C{next_cust:04d}"
        next_cust += 1
        row = {
            "customer_id": cid,
            "name": f"New Customer {cid}",
            "email": f"new{cid.lower()}@example.com",
            "plan": rng.choice(PLANS),
            "region": rng.choice(REGIONS),
        }
        add("customers", "INSERT", cid, row, None, next_minute())
        cust_by_id[cid] = row
        live_cust.append(cid)

    # --- Order updates ---
    for oid in rng.sample(live_orders, N_ORDER_UPDATES):
        before = dict(order_by_id[oid])
        after = dict(before)
        if rng.random() < 0.6:
            after["status"] = rng.choice([s for s in STATUSES if s != before["status"]])
        else:
            after["amount"] = round(rng.uniform(19.0, 999.0), 2)
        add("orders", "UPDATE", oid, after, before, next_minute())
        order_by_id[oid] = after

    # --- Order deletes ---
    for oid in rng.sample(live_orders, N_ORDER_DELETES):
        add("orders", "DELETE", oid, None, dict(order_by_id[oid]), next_minute())
        order_by_id.pop(oid, None)

    # --- Order inserts ---
    next_order = N_ORDERS + 1
    for _ in range(N_ORDER_INSERTS):
        oid = f"O{next_order:05d}"
        next_order += 1
        row = {
            "order_id": oid,
            "customer_id": rng.choice(live_cust),
            "amount": round(rng.uniform(19.0, 999.0), 2),
            "status": rng.choices(STATUSES, weights=[0.85, 0.08, 0.07])[0],
        }
        add("orders", "INSERT", oid, row, None, next_minute())
        order_by_id[oid] = row

    return events, stats


def build_delivery_log(rng, events):
    """Delivery order: mostly seq order, with duplicates and delays.

    - Duplicates: 25 events delivered a second time later in the stream.
    - Delays: 15 events whose row has a LATER event too — the earlier event
      is moved later in delivery, so it arrives after its successor and
      must be skipped as stale by the consumer.
    Returns (delivery_seqs, stats).
    """
    delivery = [e["seq"] for e in events]
    by_pk = {}
    for e in events:
        by_pk.setdefault((e["table_name"], e["pk"]), []).append(e["seq"])
    multi = [seqs for seqs in by_pk.values() if len(seqs) >= 2]

    # Delayed: pick 15 rows with >=2 events; move the EARLIER event later.
    delayed = []
    rng.shuffle(multi)
    for seqs in multi[:N_DELAYED_DELIVERIES]:
        early = seqs[0]
        delivery.remove(early)
        # insert it after its successor's position (plus a small gap)
        succ_pos = delivery.index(seqs[1])
        insert_at = min(len(delivery), succ_pos + rng.randint(5, 40))
        delivery.insert(insert_at, early)
        delayed.append(early)

    # Duplicates: 25 events delivered twice (second copy later in stream).
    dup_seqs = rng.sample([e["seq"] for e in events], N_DUPLICATE_DELIVERIES)
    for s in dup_seqs:
        pos = delivery.index(s)
        insert_at = min(len(delivery), pos + rng.randint(10, 60))
        delivery.insert(insert_at, s)

    stats = {"duplicate_deliveries": len(dup_seqs), "delayed_deliveries": len(delayed)}
    return delivery, stats


def generate(out_path, seed=42):
    rng = make_rng(seed)
    customers = snapshot_customers(rng, N_CUSTOMERS)
    orders = snapshot_orders(rng, customers, N_ORDERS)
    events, change_stats = build_change_log(rng, customers, orders)
    delivery, delivery_stats = build_delivery_log(rng, events)

    if os.path.exists(out_path):
        os.remove(out_path)
    conn = connect(out_path)
    conn.executescript(SOURCE_SCHEMA)
    conn.executemany(
        f"INSERT INTO customers ({', '.join(CUSTOMER_COLUMNS)}) "
        f"VALUES ({', '.join('?' * len(CUSTOMER_COLUMNS))})",
        [[c[col] for col in CUSTOMER_COLUMNS] for c in customers])
    conn.executemany(
        f"INSERT INTO orders ({', '.join(ORDER_COLUMNS)}) "
        f"VALUES ({', '.join('?' * len(ORDER_COLUMNS))})",
        [[o[col] for col in ORDER_COLUMNS] for o in orders])
    conn.executemany(
        "INSERT INTO change_log (seq, table_name, op, pk, payload, "
        "before_payload, event_ts) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(e["seq"], e["table_name"], e["op"], e["pk"], e["payload"],
          e["before_payload"], e["event_ts"]) for e in events])
    base = datetime(2026, 1, 1, 0, 0, 0)
    conn.executemany(
        "INSERT INTO delivery_log (delivery_id, seq, delivered_at) "
        "VALUES (?, ?, ?)",
        [(i + 1, s, _ts(base, (i + 1) * 3)) for i, s in enumerate(delivery)])
    conn.commit()
    conn.close()
    summary = {
        "seed": seed,
        "snapshot": {"customers": len(customers), "orders": len(orders)},
        "events": len(events),
        "deliveries": len(delivery),
        **change_stats,
        **delivery_stats,
    }
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="outputs/source.db")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    print(generate(args.out, args.seed))

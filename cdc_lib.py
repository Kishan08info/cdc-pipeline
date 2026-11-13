"""Shared helpers for the CDC pipeline demo.

Stdlib only. All data is SYNTHETIC (fictional SaaS customers/orders).
"""
import hashlib
import json
import random
import sqlite3

SEED = 42

CUSTOMER_COLUMNS = ["customer_id", "name", "email", "plan", "region"]
ORDER_COLUMNS = ["order_id", "customer_id", "amount", "status"]

PLANS = ["starter", "growth", "pro", "enterprise"]
REGIONS = ["us-east", "us-west", "eu-west", "apac"]
STATUSES = ["paid", "refunded", "failed"]

FIRST_NAMES = ["Ava", "Liam", "Maya", "Noah", "Zoe", "Ethan", "Ivy", "Lucas",
               "Nora", "Owen", "Ruby", "Felix", "June", "Hugo", "Lena", "Omar"]
LAST_NAMES = ["Chen", "Patel", "Garcia", "Kim", "Novak", "Singh", "Brooks",
              "Haddad", "Fischer", "Okafor", "Larsen", "Moreau", "Reyes", "Tanaka"]

SOURCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY, name TEXT, email TEXT,
    plan TEXT, region TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY, customer_id TEXT,
    amount REAL, status TEXT
);
CREATE TABLE IF NOT EXISTS change_log (
    seq INTEGER PRIMARY KEY,
    table_name TEXT NOT NULL,
    op TEXT NOT NULL,
    pk TEXT NOT NULL,
    payload TEXT,
    before_payload TEXT,
    event_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_log (
    delivery_id INTEGER PRIMARY KEY,
    seq INTEGER NOT NULL,
    delivered_at TEXT NOT NULL
);
"""

TARGET_SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY, name TEXT, email TEXT,
    plan TEXT, region TEXT, _cdc_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY, customer_id TEXT,
    amount REAL, status TEXT, _cdc_seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tombstones (
    table_name TEXT NOT NULL, pk TEXT NOT NULL,
    seq INTEGER NOT NULL, PRIMARY KEY (table_name, pk)
);
CREATE TABLE IF NOT EXISTS applied_events (
    seq INTEGER PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS checkpoint (
    key TEXT PRIMARY KEY, value INTEGER NOT NULL
);
"""


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def make_rng(seed=SEED):
    return random.Random(seed)


def snapshot_customers(rng, n=500):
    rows = []
    for i in range(1, n + 1):
        cid = f"C{i:04d}"
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        rows.append({
            "customer_id": cid,
            "name": f"{first} {last}",
            "email": f"{first.lower()}.{last.lower()}{i}@example.com",
            "plan": rng.choice(PLANS),
            "region": rng.choice(REGIONS),
        })
    return rows


def snapshot_orders(rng, customers, n=1500):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "order_id": f"O{i:05d}",
            "customer_id": rng.choice(customers)["customer_id"],
            "amount": round(rng.uniform(19.0, 999.0), 2),
            "status": rng.choices(STATUSES, weights=[0.85, 0.08, 0.07])[0],
        })
    return rows


def row_checksum(rows, columns, pk_col):
    """Deterministic checksum over a table's rows (sorted by pk)."""
    lines = []
    for r in sorted(rows, key=lambda x: x[pk_col]):
        vals = [str(r[c]) for c in columns]
        lines.append("|".join(vals))
    blob = "\n".join(lines).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def replay_expected_state(snapshot, events):
    """Apply change_log events in seq order (ideal semantics) to a snapshot.

    Returns dict table_name -> {pk: row-dict}. Last write wins per row;
    DELETE removes the row. This is the independent ground truth the
    consumer's target is reconciled against.
    """
    state = {
        "customers": {r["customer_id"]: dict(r) for r in snapshot["customers"]},
        "orders": {r["order_id"]: dict(r) for r in snapshot["orders"]},
    }
    pk_col = {"customers": "customer_id", "orders": "order_id"}
    for ev in sorted(events, key=lambda e: e["seq"]):
        table = ev["table_name"]
        pk = ev["pk"]
        if ev["op"] == "DELETE":
            state[table].pop(pk, None)
        else:
            state[table][pk] = dict(json.loads(ev["payload"]))
    return state


def table_rows(conn, table, columns, pk_col):
    cur = conn.execute(
        f"SELECT {', '.join(columns)} FROM {table} ORDER BY {pk_col}")
    return [dict(zip(columns, row)) for row in cur.fetchall()]

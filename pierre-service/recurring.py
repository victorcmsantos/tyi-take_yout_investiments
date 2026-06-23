"""Recurring commitments (fixed expenses, subscriptions, installments, debts).

Mirrors the user's spreadsheet: each item has a bucket (receita/despfixa/cartao/
despavulsa), an expected monthly amount, and either runs every month ('fixed')
or for a fixed number of installments ('installment'), auto-ending when done.
Used to show Planned (committed) vs Realized in the month's cash flow.
"""

import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.getenv("PIERRE_CACHE_DB", "/data/pierre_cache.db")
_LOCK = threading.Lock()
BUCKETS = {"receita", "despfixa", "cartao", "despavulsa"}


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _ym_index(ym):
    try:
        y, m = (int(p) for p in str(ym).split("-")[:2])
        return y * 12 + (m - 1)
    except (ValueError, TypeError):
        return None


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recurring_items (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          description TEXT NOT NULL,
          bucket TEXT NOT NULL,
          amount REAL NOT NULL,
          category TEXT DEFAULT '',
          bank TEXT DEFAULT '',
          kind TEXT NOT NULL DEFAULT 'fixed',
          total_installments INTEGER DEFAULT 0,
          start_month TEXT DEFAULT '',
          active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL
        )
        """
    )
    return conn


def list_items():
    with _LOCK, _db() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM recurring_items ORDER BY bucket, description"
        ).fetchall()]


def add_item(description, bucket, amount, category="", bank="", kind="fixed",
             total_installments=0, start_month=""):
    description = (description or "").strip()
    bucket = (bucket or "despfixa").strip().lower()
    if not description:
        raise ValueError("Descrição é obrigatória.")
    if bucket not in BUCKETS:
        raise ValueError("Bucket inválido.")
    try:
        amount = abs(float(amount))
    except (TypeError, ValueError):
        raise ValueError("Valor inválido.")
    kind = "installment" if str(kind) == "installment" else "fixed"
    with _LOCK, _db() as c:
        cur = c.execute(
            "INSERT INTO recurring_items (description, bucket, amount, category, bank, kind, "
            "total_installments, start_month, active, created_at) VALUES (?,?,?,?,?,?,?,?,1,?)",
            (description, bucket, amount, (category or "").strip(), (bank or "").strip(),
             kind, int(total_installments or 0), (start_month or "").strip(), _now()),
        )
        c.commit()
        return {"id": cur.lastrowid}


def update_item(item_id, **fields):
    cols = {"description", "bucket", "amount", "category", "bank", "kind", "total_installments", "start_month", "active"}
    sets, args = [], []
    for k, v in fields.items():
        if k not in cols or v is None:
            continue
        if k == "amount":
            v = abs(float(v))
        if k == "bucket":
            v = str(v).lower()
            if v not in BUCKETS:
                continue
        if k == "active":
            v = 1 if v else 0
        sets.append(f"{k} = ?")
        args.append(v)
    if not sets:
        return {"id": item_id}
    args.append(item_id)
    with _LOCK, _db() as c:
        c.execute(f"UPDATE recurring_items SET {', '.join(sets)} WHERE id = ?", args)
        c.commit()
    return {"id": item_id}


def delete_item(item_id):
    with _LOCK, _db() as c:
        c.execute("DELETE FROM recurring_items WHERE id = ?", (item_id,))
        c.commit()


def _installment_number(item, ym):
    sidx = _ym_index(item.get("start_month"))
    idx = _ym_index(ym)
    if sidx is None or idx is None:
        return None
    return idx - sidx + 1


def active_in_month(item, ym):
    if not item.get("active"):
        return False
    if item.get("kind") == "installment":
        n = _installment_number(item, ym)
        total = int(item.get("total_installments") or 0)
        return n is not None and 1 <= n <= total
    return True


def planned_for_month(ym):
    """Per-bucket planned totals + the active items (with installment label)."""
    buckets = {b: 0.0 for b in BUCKETS}
    items = []
    for it in list_items():
        if not active_in_month(it, ym):
            continue
        buckets[it["bucket"]] += float(it["amount"] or 0.0)
        entry = dict(it)
        if it.get("kind") == "installment":
            entry["installment_label"] = f"{_installment_number(it, ym)}/{it.get('total_installments')}"
        items.append(entry)
    sobra = buckets["receita"] - (buckets["despfixa"] + buckets["cartao"] + buckets["despavulsa"])
    return {
        "buckets": {k: round(v, 2) for k, v in buckets.items()},
        "sobra": round(sobra, 2),
        "items": items,
    }

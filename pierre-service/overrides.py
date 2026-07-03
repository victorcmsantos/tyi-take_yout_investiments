"""User category overrides (recategorization rules).

Pierre is read-only, so when the user recategorizes a transaction we store a
rule here keyed by a normalized merchant/description pattern. Rules are applied
to every transaction (all months) whose description contains the pattern, so
e.g. "MINI MARKET OF JOY" can be forced to "Supermercado" everywhere.
"""

import copy
import os
import sqlite3
import threading
import unicodedata
from datetime import datetime

DB_PATH = os.getenv("PIERRE_CACHE_DB", "/data/pierre_cache.db")
_LOCK = threading.Lock()


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _norm(text):
    return unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower().strip()


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS category_overrides (
          pattern TEXT PRIMARY KEY,
          category TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    return conn


def list_overrides():
    try:
        with _LOCK, _db() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT pattern, category, updated_at FROM category_overrides ORDER BY pattern"
            )]
    except Exception:
        return []


def add_override(pattern, category):
    pattern = (pattern or "").strip()
    category = (category or "").strip()
    if not pattern or not category:
        raise ValueError("pattern e category são obrigatórios.")
    with _LOCK, _db() as conn:
        conn.execute(
            """
            INSERT INTO category_overrides (pattern, category, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(pattern) DO UPDATE SET category = excluded.category, updated_at = excluded.updated_at
            """,
            (pattern, category, _now()),
        )
        conn.commit()
    return {"pattern": pattern, "category": category}


def delete_override(pattern):
    with _LOCK, _db() as conn:
        conn.execute("DELETE FROM category_overrides WHERE pattern = ?", ((pattern or "").strip(),))
        conn.commit()


def _rules():
    return [(_norm(r["pattern"]), r["category"]) for r in list_overrides() if r.get("pattern")]


def apply(tx_payload):
    """Return a copy of the transactions payload with categories overridden by
    the stored rules (matched by normalized description/merchant substring)."""
    rules = _rules()
    if not rules or not isinstance(tx_payload, dict):
        return tx_payload
    payload = copy.deepcopy(tx_payload)
    accounts = ((payload.get("data") or {}).get("transactions") or {}).get("accounts") or {}

    def fix(item):
        text = _norm((item.get("description") or "") + " " + (item.get("merchant") or ""))
        for pat, cat in rules:
            if pat and pat in text:
                item["category"] = cat
                return

    for a in accounts.values():
        if not isinstance(a, dict):
            continue
        for it in a.get("received") or []:
            fix(it)
        for it in a.get("bank_transfer") or []:
            fix(it)
        for cv in (a.get("credit_cards") or {}).values():
            if isinstance(cv, dict):
                for it in cv.get("purchases") or []:
                    fix(it)
                for it in cv.get("payments") or []:
                    fix(it)
    return payload

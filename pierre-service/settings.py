"""Small key/value settings store (same SQLite as the rest)."""

import os
import sqlite3
import threading

DB_PATH = os.getenv("PIERRE_CACHE_DB", "/data/pierre_cache.db")
_LOCK = threading.Lock()


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
    return conn


def get(key, default=None):
    try:
        with _LOCK, _db() as c:
            row = c.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default
    except Exception:
        return default


def set_value(key, value):
    with _LOCK, _db() as c:
        c.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value)),
        )
        c.commit()
    return {key: str(value)}


def all_settings():
    with _LOCK, _db() as c:
        return {r["key"]: r["value"] for r in c.execute("SELECT key, value FROM settings").fetchall()}


def card_closing_day():
    """Day the card invoice closes. <=1 means use calendar month (no cycle)."""
    raw = get("card_closing_day", os.getenv("PIERRE_CARD_CLOSING_DAY", "25"))
    try:
        return max(1, min(28, int(raw)))
    except (TypeError, ValueError):
        return 25

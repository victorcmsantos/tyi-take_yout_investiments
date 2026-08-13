"""Small key/value settings store (same SQLite as the rest)."""

import json
import os
import sqlite3
import threading
import unicodedata

# Closing day varies by bank (the day the statement closes). These defaults are
# overridden by the "card_closing_days" setting (a JSON map connector->day).
DEFAULT_CLOSING_DAYS = {"itau": 22, "santander": 25}

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
    """Default day the card invoice closes. <=1 means calendar month (no cycle)."""
    raw = get("card_closing_day", os.getenv("PIERRE_CARD_CLOSING_DAY", "25"))
    try:
        return max(1, min(28, int(raw)))
    except (TypeError, ValueError):
        return 25


def card_closing_days():
    """Map of connector substring -> closing day (overrides the global default)."""
    raw = get("card_closing_days")
    if raw:
        try:
            return {str(k).lower(): max(1, min(28, int(v))) for k, v in json.loads(raw).items()}
        except Exception:
            pass
    return dict(DEFAULT_CLOSING_DAYS)


def card_closing_by_last4():
    """Map of card last4 -> default closing day. Per physical card / invoice,
    beating the per-bank and global defaults but yielding to a per-month override.
    Lets two cards of the same bank (e.g. two Itaú cards) close on different days."""
    raw = get("card_closing_by_last4")
    if raw:
        try:
            return {str(k).strip()[-4:]: max(1, min(28, int(v)))
                    for k, v in json.loads(raw).items()}
        except Exception:
            pass
    return {}


def card_closing_overrides():
    """Map of "last4|YYYY-MM" -> closing day. The MOST specific rule: a per-MONTH
    override for one card, because the real cut shifts month to month (weekends /
    holidays push it to the next business day). Beats the per-card default."""
    raw = get("card_closing_overrides")
    if raw:
        try:
            return {str(k): max(1, min(28, int(v))) for k, v in json.loads(raw).items()}
        except Exception:
            pass
    return {}


def closing_for(connector, last4=None, ym=None):
    """Closing day for a card. Resolution order, most specific first: a per-month
    override (last4|ym) -> a per-card default (last4) -> the per-bank default
    (connector substring) -> the global default."""
    last4 = str(last4).strip()[-4:] if last4 else None
    if last4 and ym:
        day = card_closing_overrides().get(f"{last4}|{ym}")
        if day:
            return day
    if last4:
        day = card_closing_by_last4().get(last4)
        if day:
            return day
    name = unicodedata.normalize("NFKD", str(connector or "")).encode("ascii", "ignore").decode().lower()
    for key, day in card_closing_days().items():
        if key and key in name:
            return day
    return card_closing_day()


def card_labels_by_last4():
    """Map of card last4 -> apelido ("Victor", "Eliane"). Rotula o plástico que
    fez a compra na quebra por cartão da fatura."""
    raw = get("card_labels_by_last4")
    if raw:
        try:
            return {str(k).strip()[-4:]: str(v).strip()[:24]
                    for k, v in json.loads(raw).items() if str(v).strip()}
        except Exception:
            pass
    return {}

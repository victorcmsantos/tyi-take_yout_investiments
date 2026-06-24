"""Manual (non-Open-Finance) checking accounts and their transactions.

Stored locally (same SQLite as cache/overrides) and merged into the Pierre
transactions structure so manual entries flow through every Finanças view
(ledger, categories, cash-flow buckets, account balances).
"""

import os
import sqlite3
import threading
from datetime import datetime

DB_PATH = os.getenv("PIERRE_CACHE_DB", "/data/pierre_cache.db")
_LOCK = threading.Lock()


def _now():
    return datetime.now().isoformat(timespec="seconds")


def _db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          logo TEXT DEFAULT '',
          created_at TEXT NOT NULL
        )
        """
    )
    cols = [r[1] for r in conn.execute("PRAGMA table_info(manual_accounts)")]
    if "logo" not in cols:
        conn.execute("ALTER TABLE manual_accounts ADD COLUMN logo TEXT DEFAULT ''")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_transactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          account_id INTEGER NOT NULL,
          date TEXT NOT NULL,
          description TEXT NOT NULL,
          category TEXT NOT NULL,
          amount REAL NOT NULL,
          flow TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    return conn


# --- accounts -----------------------------------------------------------------

def list_accounts():
    with _LOCK, _db() as c:
        rows = c.execute("SELECT id, name, logo, created_at FROM manual_accounts ORDER BY id").fetchall()
        bal = {r["account_id"]: r["b"] for r in c.execute(
            "SELECT account_id, SUM(CASE WHEN flow='in' THEN amount ELSE -amount END) AS b "
            "FROM manual_transactions GROUP BY account_id"
        ).fetchall()}
    return [{
        "id": r["id"], "name": r["name"], "logo": r["logo"] or "",
        "balance": round(bal.get(r["id"], 0.0), 2),
    } for r in rows]


def add_account(name, logo=""):
    name = (name or "").strip()
    if not name:
        raise ValueError("Nome da conta é obrigatório.")
    logo = (logo or "").strip()
    with _LOCK, _db() as c:
        cur = c.execute("INSERT INTO manual_accounts (name, logo, created_at) VALUES (?, ?, ?)", (name, logo, _now()))
        c.commit()
        return {"id": cur.lastrowid, "name": name, "logo": logo, "balance": 0.0}


def update_account(account_id, name=None, logo=None):
    sets, args = [], []
    if name is not None and str(name).strip():
        sets.append("name = ?")
        args.append(str(name).strip())
    if logo is not None:
        sets.append("logo = ?")
        args.append(str(logo).strip())
    if not sets:
        return {"id": account_id}
    args.append(account_id)
    with _LOCK, _db() as c:
        c.execute(f"UPDATE manual_accounts SET {', '.join(sets)} WHERE id = ?", args)
        c.commit()
    return {"id": account_id}


def delete_account(account_id):
    with _LOCK, _db() as c:
        c.execute("DELETE FROM manual_transactions WHERE account_id = ?", (account_id,))
        c.execute("DELETE FROM manual_accounts WHERE id = ?", (account_id,))
        c.commit()


# --- transactions -------------------------------------------------------------

def list_transactions(account_id=None):
    q = "SELECT id, account_id, date, description, category, amount, flow FROM manual_transactions"
    args = ()
    if account_id is not None:
        q += " WHERE account_id = ?"
        args = (account_id,)
    q += " ORDER BY date DESC, id DESC"
    with _LOCK, _db() as c:
        return [dict(r) for r in c.execute(q, args).fetchall()]


def add_transaction(account_id, date, description, category, amount, flow):
    if not account_id or not date or not category:
        raise ValueError("account_id, date e category são obrigatórios.")
    flow = "in" if str(flow) == "in" else "out"
    try:
        amount = abs(float(amount))
    except (TypeError, ValueError):
        raise ValueError("Valor inválido.")
    with _LOCK, _db() as c:
        cur = c.execute(
            "INSERT INTO manual_transactions (account_id, date, description, category, amount, flow, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (account_id, date, (description or "").strip() or category, category, amount, flow, _now()),
        )
        c.commit()
        return {"id": cur.lastrowid}


def update_transaction(tx_id, date=None, description=None, category=None, amount=None, flow=None):
    sets, args = [], []
    if date:
        sets.append("date = ?"); args.append(date)
    if description is not None:
        sets.append("description = ?"); args.append((description or "").strip())
    if category:
        sets.append("category = ?"); args.append(category)
    if amount is not None:
        try:
            sets.append("amount = ?"); args.append(abs(float(amount)))
        except (TypeError, ValueError):
            raise ValueError("Valor inválido.")
    if flow:
        sets.append("flow = ?"); args.append("in" if str(flow) == "in" else "out")
    if not sets:
        return {"id": tx_id}
    args.append(tx_id)
    with _LOCK, _db() as c:
        c.execute(f"UPDATE manual_transactions SET {', '.join(sets)} WHERE id = ?", args)
        c.commit()
    return {"id": tx_id}


def delete_transaction(tx_id):
    with _LOCK, _db() as c:
        c.execute("DELETE FROM manual_transactions WHERE id = ?", (tx_id,))
        c.commit()


# --- merge into Pierre payload ------------------------------------------------

def bank_accounts_for_overview():
    """Manual accounts shaped like overview bank entries."""
    return [{
        "name": a["name"],
        "balance": a["balance"],
        "logo": a.get("logo") or "",
        "subtype": "CHECKING_ACCOUNT",
        "manual": True,
        "id": a["id"],
    } for a in list_accounts()]


def inject(payload, start, end):
    """Merge manual transactions whose date is in [start, end] into a Pierre
    transactions payload (accounts items + summary totals)."""
    if not isinstance(payload, dict):
        return payload
    accts = {a["id"]: a for a in list_accounts()}
    txs = [t for t in list_transactions() if start <= (t["date"] or "") <= end]
    if not txs:
        return payload

    accounts = ((payload.setdefault("data", {}).setdefault("transactions", {})).setdefault("accounts", {}))
    summary = payload["data"].setdefault("summary", {})
    for key in ("by_day", "by_month", "by_month_expense", "by_month_income"):
        summary.setdefault(key, {})

    by_acct = {}
    for t in txs:
        by_acct.setdefault(t["account_id"], []).append(t)

    for acct_id, items in by_acct.items():
        acct = accts.get(acct_id) or {}
        name = acct.get("name", f"Conta {acct_id}")
        acct_logo = acct.get("logo") or ""
        received, bank_transfer = [], []
        for t in items:
            entry = {
                "date": t["date"],
                "category": t["category"],
                "amount": t["amount"],
                "description": t["description"],
                "merchant": "",
                "account_info": {"name": name, "type": "BANK", "subtype": "CHECKING_ACCOUNT", "logo": acct_logo},
                "type": "CREDIT" if t["flow"] == "in" else "DEBIT",
                "transaction_type": "received" if t["flow"] == "in" else "transfer",
                # carried through so the UI can edit THIS manual tx (not a bulk rule)
                "manual_id": t["id"],
                "manual_flow": t["flow"],
            }
            (received if t["flow"] == "in" else bank_transfer).append(entry)
            ym, day = (t["date"] or "")[:7], t["date"]
            amt = float(t["amount"] or 0.0)
            if t["flow"] == "out":
                summary["total_spent"] = float(summary.get("total_spent") or 0.0) + amt
                summary["by_day"][day] = float(summary["by_day"].get(day) or 0.0) + amt
                summary["by_month"][ym] = float(summary["by_month"].get(ym) or 0.0) + amt
                summary["by_month_expense"][ym] = float(summary["by_month_expense"].get(ym) or 0.0) + amt
            else:
                summary["total_received"] = float(summary.get("total_received") or 0.0) + amt
                summary["by_month_income"][ym] = float(summary["by_month_income"].get(ym) or 0.0) + amt
        accounts[f"manual:{acct_id}"] = {
            "received": received,
            "bank_transfer": bank_transfer,
            "credit_cards": {},
            "total_received": round(sum(x["amount"] for x in received), 2),
            "total_bank_transfer": round(sum(x["amount"] for x in bank_transfer), 2),
        }
    return payload

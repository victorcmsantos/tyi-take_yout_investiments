"""Builds a consolidated 'Visão geral' payload for the Finanças dashboard from
Pierre data: accounts + balances + credit limits, current-vs-previous month
spend (rhythm, heatmap, categories with variation), recent transactions, and
the cash-flow buckets (Receita/DespFixa/Cartao/DespAvulsa/Sobra).
"""

import calendar
from datetime import date

import cashflow
import pierre

# Pierre's own virtual wallet (not a real bank account) is hidden.
HIDDEN_ACCOUNT_NAMES = {"carteira pierre", "carteira"}
# Not real spending categories (settlement / internal moves).
HIDDEN_SPEND_CATEGORIES = {"Pagamento de cartão de crédito", "Transferência mesma titularidade"}


def _month_bounds(year, month):
    last = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last:02d}", last


def _prev_month(year, month):
    return (year - 1, 12) if month == 1 else (year, month - 1)


def _summary(payload):
    data = (payload or {}).get("data") or {}
    return data.get("summary") or {}


def _pct_delta(cur, prev):
    if not prev:
        return None
    return round(((cur - prev) / prev) * 100.0, 1)


def _spend_by_category(accounts_tx, ym):
    """Aggregate real spending by category for transactions whose DATE falls in
    the given month (YYYY-MM) — installments keep their purchase-month, matching
    Pierre. Income and settlement/internal categories are excluded."""
    agg = {}

    def add(items):
        for it in items or []:
            if str(it.get("date") or "")[:7] != ym:
                continue
            cat = it.get("category")
            if cat in HIDDEN_SPEND_CATEGORIES:
                continue
            agg[cat] = agg.get(cat, 0.0) + abs(float(it.get("amount") or 0.0))

    for a in accounts_tx.values():
        if not isinstance(a, dict):
            continue
        add(a.get("bank_transfer"))
        for cv in (a.get("credit_cards") or {}).values():
            if isinstance(cv, dict):
                add(cv.get("purchases"))
    return agg


def _cumulative(by_day, year, month, days_in_month):
    series = []
    running = 0.0
    for day in range(1, days_in_month + 1):
        key = f"{year:04d}-{month:02d}-{day:02d}"
        running += float(by_day.get(key) or 0.0)
        series.append(round(running, 2))
    return series


def build_overview(year, month):
    cur_start, cur_end, cur_days = _month_bounds(year, month)
    py, pm = _prev_month(year, month)
    prev_start, prev_end, prev_days = _month_bounds(py, pm)

    cur_tx = pierre.get_transactions(start_date=cur_start, end_date=cur_end)
    prev_tx = pierre.get_transactions(start_date=prev_start, end_date=prev_end)
    accounts_raw = pierre.get_accounts()

    cur_s = _summary(cur_tx)
    prev_s = _summary(prev_tx)
    buckets = cashflow.summarize_cashflow(cur_tx)["buckets"]

    # Per-card month spend: each purchase carries account_info.brand/level, which
    # identifies the physical card, so card totals can be split.
    cur_accounts_tx = ((cur_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    card_spend = {}
    for acct in cur_accounts_tx.values():
        if not isinstance(acct, dict):
            continue
        for cv in (acct.get("credit_cards") or {}).values():
            if not isinstance(cv, dict):
                continue
            for it in cv.get("purchases") or []:
                ai = it.get("account_info") or {}
                key = (str(ai.get("brand") or "").upper(), str(ai.get("level") or "").upper())
                card_spend[key] = card_spend.get(key, 0.0) + abs(float(it.get("amount") or 0.0))

    # --- accounts -------------------------------------------------------------
    acc_list = (accounts_raw or {}).get("data")
    if isinstance(acc_list, dict):
        acc_list = acc_list.get("data")
    acc_list = acc_list or []
    bank, credit = [], []
    total_bank = total_limit = total_available = total_used = 0.0
    for a in acc_list:
        kind = a.get("type")
        credit_data = a.get("creditData") or {}
        logo = a.get("connectorImageUrl") or ""
        if kind == "CREDIT":
            limit = float(credit_data.get("limit") or credit_data.get("creditLimit") or 0.0)
            available = float(credit_data.get("availableCreditLimit")
                              or credit_data.get("available") or a.get("balance") or 0.0)
            used = max(limit - available, 0.0)
            additional = [str(c.get("number")) for c in (credit_data.get("additionalCards") or []) if c.get("number")]
            brand = str(credit_data.get("brand") or "").upper()
            level = str(credit_data.get("level") or "").upper()
            spent = card_spend.get((brand, level), 0.0)
            total_limit += limit
            total_available += available
            total_used += used
            credit.append({
                "name": a.get("customName") or a.get("name"),
                "brand": credit_data.get("brand"),
                "level": credit_data.get("level"),
                "limit": round(limit, 2),
                "available": round(available, 2),
                "used": round(used, 2),
                "spent": round(spent, 2),
                "logo": logo,
                "last4": str(a.get("number") or "")[-4:],
                "additional_cards": additional,
                "due_date": credit_data.get("balanceDueDate") or credit_data.get("dueDate"),
            })
        elif kind == "BANK":
            name = a.get("customName") or a.get("name") or ""
            if name.strip().lower() in HIDDEN_ACCOUNT_NAMES:
                continue
            bal = float(a.get("balance") or 0.0)
            total_bank += bal
            bank.append({
                "name": name,
                "balance": round(bal, 2),
                "logo": logo,
                "subtype": a.get("subtype"),
            })

    # --- spend rhythm + heatmap ----------------------------------------------
    cur_by_day = cur_s.get("by_day") or {}
    prev_by_day = prev_s.get("by_day") or {}
    cur_spent = float(cur_s.get("total_spent") or 0.0)
    prev_spent = float(prev_s.get("total_spent") or 0.0)

    # --- categories with variation (date-filtered spending, like Pierre) ------
    prev_accounts_tx = ((prev_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    cur_cat = _spend_by_category(cur_accounts_tx, f"{year:04d}-{month:02d}")
    prev_cat = _spend_by_category(prev_accounts_tx, f"{py:04d}-{pm:02d}")
    categories = []
    for name in set(cur_cat) | set(prev_cat):
        ct = cur_cat.get(name, 0.0)
        pt = prev_cat.get(name, 0.0)
        if ct <= 0 and pt <= 0:
            continue
        categories.append({
            "category": name,
            "total": round(ct, 2),
            "prev_total": round(pt, 2),
            "delta_pct": _pct_delta(ct, pt),
            "is_new": pt == 0 and ct > 0,
        })
    categories.sort(key=lambda c: c["total"], reverse=True)
    top_category = categories[0] if categories else None

    # --- recent transactions, split by source (card vs bank account) ----------
    cur_accounts = ((cur_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    card_items, acct_items = [], []

    def _norm(it, flow):
        return {
            "date": it.get("date"),
            "description": it.get("description") or it.get("merchant") or it.get("category"),
            "category": it.get("category"),
            "amount": round(abs(float(it.get("amount") or 0.0)), 2),
            "flow": flow,
        }

    for a in cur_accounts.values():
        if not isinstance(a, dict):
            continue
        for it in a.get("received") or []:
            acct_items.append(_norm(it, "in"))
        for it in a.get("bank_transfer") or []:
            acct_items.append(_norm(it, "out"))
        cc = a.get("credit_cards") or {}
        if isinstance(cc, dict):
            for card in cc.values():
                if isinstance(card, dict):
                    for it in card.get("purchases") or []:
                        card_items.append(_norm(it, "out"))

    card_items.sort(key=lambda x: x["date"] or "", reverse=True)
    acct_items.sort(key=lambda x: x["date"] or "", reverse=True)

    return {
        "month": f"{year:04d}-{month:02d}",
        "accounts": {
            "bank": bank,
            "credit": credit,
            "total_bank_balance": round(total_bank, 2),
            "total_credit_limit": round(total_limit, 2),
            "total_credit_available": round(total_available, 2),
            "total_credit_used": round(total_used, 2),
        },
        "spend": {
            "month_total": round(cur_spent, 2),
            "prev_total": round(prev_spent, 2),
            "delta_pct": _pct_delta(cur_spent, prev_spent),
            "daily_avg": round(cur_spent / cur_days, 2) if cur_days else 0.0,
            "by_day": {k: round(float(v or 0.0), 2) for k, v in cur_by_day.items()},
            "cumulative": {
                "days": list(range(1, cur_days + 1)),
                "current": _cumulative(cur_by_day, year, month, cur_days),
                "previous": _cumulative(prev_by_day, py, pm, prev_days),
            },
            "top_category": top_category,
        },
        "categories": categories,
        "buckets": buckets,
        "recent_card": card_items[:6],
        "recent_account": acct_items[:6],
    }

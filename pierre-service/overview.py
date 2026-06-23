"""Builds a consolidated 'Visão geral' payload for the Finanças dashboard from
Pierre data: accounts + balances + credit limits, current-vs-previous month
spend (rhythm, heatmap, categories with variation), recent transactions, and
the cash-flow buckets (Receita/DespFixa/Cartao/DespAvulsa/Sobra).
"""

import calendar
import unicodedata
from datetime import date, timedelta

import cashflow
import manual
import overrides
import pierre
import recurring
import settings

# Pierre's own virtual wallet (not a real bank account) is hidden.
HIDDEN_ACCOUNT_NAMES = {"carteira pierre", "carteira"}
# Savings accounts are not checking accounts; excluded from "Contas correntes".
SAVINGS_SUBTYPES = {"SAVINGS", "SAVINGS_ACCOUNT", "POUPANCA"}
# Not real spending categories (settlement / internal moves).
HIDDEN_SPEND_CATEGORIES = {"Pagamento de cartão de crédito", "Transferência mesma titularidade"}


def _norm(text):
    """Lowercase + strip accents, for matching bank/connector names."""
    return unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower()


def _acc_list(accounts_raw):
    acc = (accounts_raw or {}).get("data")
    if isinstance(acc, dict):
        acc = acc.get("data")
    return acc or []


def _make_logo_fn(acc_list):
    """Resolve a bank/card logo for a transaction by matching its connector
    (from account_info.name) against the connected accounts."""
    conn_logo = []
    for a in acc_list:
        logo = a.get("connectorImageUrl")
        conn = _norm(a.get("connectorName"))
        if conn and logo:
            conn_logo.append((conn, logo))

    def fn(item):
        ai = item.get("account_info") or {}
        if ai.get("logo"):
            return ai["logo"]
        name = _norm(ai.get("name") or item.get("description"))
        for conn, logo in conn_logo:
            if conn in name:
                return logo
        return ""

    return fn


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


def _spend_by_category(accounts_tx, ym, collect=False, logo_fn=None):
    """Aggregate real spending by category for transactions whose DATE falls in
    the given month (YYYY-MM) — installments keep their purchase-month, matching
    Pierre. Income and settlement/internal categories are excluded. When
    ``collect`` is set, also returns the underlying transactions per category."""
    agg = {}
    items_by_cat = {}

    def add(items, source):
        for it in items or []:
            if str(it.get("date") or "")[:7] != ym:
                continue
            cat = it.get("category")
            if cat in HIDDEN_SPEND_CATEGORIES:
                continue
            value = abs(float(it.get("amount") or 0.0))
            agg[cat] = agg.get(cat, 0.0) + value
            if collect:
                items_by_cat.setdefault(cat, []).append({
                    "date": it.get("date"),
                    "description": it.get("description") or it.get("merchant") or cat,
                    "amount": round(value, 2),
                    "source": source,
                    "logo": logo_fn(it) if logo_fn else "",
                })

    for a in accounts_tx.values():
        if not isinstance(a, dict):
            continue
        add(a.get("bank_transfer"), "conta")
        for cv in (a.get("credit_cards") or {}).values():
            if isinstance(cv, dict):
                add(cv.get("purchases"), "cartao")

    if collect:
        for items in items_by_cat.values():
            items.sort(key=lambda x: x["date"] or "", reverse=True)
        return agg, items_by_cat
    return agg


def _cumulative(by_day, year, month, days_in_month):
    series = []
    running = 0.0
    for day in range(1, days_in_month + 1):
        key = f"{year:04d}-{month:02d}-{day:02d}"
        running += float(by_day.get(key) or 0.0)
        series.append(round(running, 2))
    return series


def build_ledger(year, month):
    """Flat, date-sorted list of the month's transactions for the Transações
    tab: incoming (conta), bank debits (conta) and card purchases (cartão)."""
    start, end, _ = _month_bounds(year, month)
    tx = manual.inject(overrides.apply(pierre.get_transactions(start_date=start, end_date=end)), start, end)
    accounts = ((tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    logo_fn = _make_logo_fn(_acc_list(pierre.get_accounts()))

    def row(it, source, flow):
        return {
            "date": it.get("date"),
            "description": it.get("description") or it.get("merchant") or it.get("category"),
            "category": it.get("category"),
            "amount": round(abs(float(it.get("amount") or 0.0)), 2),
            "source": source,
            "flow": flow,
            "logo": logo_fn(it),
        }

    rows = []
    for a in accounts.values():
        if not isinstance(a, dict):
            continue
        for it in a.get("received") or []:
            rows.append(row(it, "conta", "in"))
        for it in a.get("bank_transfer") or []:
            rows.append(row(it, "conta", "out"))
        for cv in (a.get("credit_cards") or {}).values():
            if isinstance(cv, dict):
                for it in cv.get("purchases") or []:
                    rows.append(row(it, "cartao", "out"))
    rows.sort(key=lambda r: r["date"] or "", reverse=True)
    return {"month": f"{year:04d}-{month:02d}", "transactions": rows}


def build_overview(year, month):
    cur_start, cur_end, cur_days = _month_bounds(year, month)
    py, pm = _prev_month(year, month)
    prev_start, prev_end, prev_days = _month_bounds(py, pm)

    cur_tx = manual.inject(overrides.apply(pierre.get_transactions(start_date=cur_start, end_date=cur_end)), cur_start, cur_end)
    prev_tx = manual.inject(overrides.apply(pierre.get_transactions(start_date=prev_start, end_date=prev_end)), prev_start, prev_end)
    accounts_raw = pierre.get_accounts()

    cur_s = _summary(cur_tx)
    prev_s = _summary(prev_tx)
    buckets = cashflow.summarize_cashflow(cur_tx)["buckets"]

    cur_accounts_tx = ((cur_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}

    # Card id -> (brand, level), to map installments (which carry accountId) to
    # the physical card.
    card_by_id = {}
    for a in _acc_list(accounts_raw):
        if a.get("type") == "CREDIT":
            cd = a.get("creditData") or {}
            card_by_id[a.get("id")] = (str(cd.get("brand") or "").upper(), str(cd.get("level") or "").upper())

    # Cards follow the invoice cycle (closes on day D): month M = day D+1 of M-1
    # through day D of M. Bank/cash stay calendar. Closing <= 1 = calendar month.
    closing = settings.card_closing_day()
    card_accounts_tx = cur_accounts_tx
    card_win_start, card_win_end = cur_start, cur_end
    if closing >= 2:
        close_this = date(year, month, min(closing, calendar.monthrange(year, month)[1]))
        close_prev = date(py, pm, min(closing, calendar.monthrange(py, pm)[1]))
        card_win_start = (close_prev + timedelta(days=1)).isoformat()
        card_win_end = close_this.isoformat()
        cycle_tx = overrides.apply(pierre.get_transactions(start_date=card_win_start, end_date=card_win_end))
        card_accounts_tx = ((cycle_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}

    # The invoice for the window = upfront purchases made in the window + the
    # installment PORTIONS due in the window (at the installment value, from
    # /installments), so a parcelada appears every month at its parcela amount.
    internal_cats = cashflow.DEFAULT_INTERNAL_CATEGORIES
    card_spend = {}
    card_purchases = {}
    cartao_total = 0.0

    # 1) upfront (non-installment) purchases in the window
    for acct in card_accounts_tx.values():
        if not isinstance(acct, dict):
            continue
        for cv in (acct.get("credit_cards") or {}).values():
            if not isinstance(cv, dict):
                continue
            for it in cv.get("purchases") or []:
                if it.get("installment_due_date"):
                    continue  # installment -> from /installments below
                d = it.get("date") or ""
                if not (card_win_start <= d <= card_win_end):
                    continue
                ai = it.get("account_info") or {}
                key = (_norm(ai.get("name")), str(ai.get("brand") or "").upper(), str(ai.get("level") or "").upper())
                value = abs(float(it.get("amount") or 0.0))
                card_spend[key] = card_spend.get(key, 0.0) + value
                if it.get("category") not in internal_cats:
                    cartao_total += value
                card_purchases.setdefault(key, []).append({
                    "date": it.get("date"),
                    "description": it.get("description") or it.get("merchant") or it.get("category"),
                    "category": it.get("category"),
                    "amount": round(value, 2),
                    "source": "cartao",
                })

    # 2) installment portions due in the window
    try:
        inst_raw = pierre.get_installments(start_date=f"{year - 2}-01-01", end_date=card_win_end)
        inst_purchases = ((inst_raw.get("data") or {}).get("purchases")) or []
    except Exception:
        inst_purchases = []
    for p in inst_purchases:
        bl = card_by_id.get(p.get("accountId"))
        if not bl:
            continue
        brand, level = bl
        key = (_norm(p.get("accountName")), brand, level)
        for inst in p.get("installments") or []:
            due = str(inst.get("dueDate") or "")[:10]
            if not (card_win_start <= due <= card_win_end):
                continue
            amt = abs(float(inst.get("amount") or 0.0))
            card_spend[key] = card_spend.get(key, 0.0) + amt
            cartao_total += amt
            card_purchases.setdefault(key, []).append({
                "date": due,
                "description": f"{p.get('description')} {inst.get('installmentNumber')}/{inst.get('totalInstallments')}",
                "category": inst.get("category") or "Parcelamento",
                "amount": round(amt, 2),
                "source": "cartao",
            })

    buckets["cartao"] = round(cartao_total, 2)
    buckets["sobra"] = round(buckets["receita"] - buckets["despfixa"] - buckets["cartao"] - buckets["despavulsa"], 2)

    # --- accounts -------------------------------------------------------------
    acc_list = _acc_list(accounts_raw)
    logo_fn = _make_logo_fn(acc_list)
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
            conn = _norm(a.get("connectorName"))
            spent = sum(
                v for (nm, b, l), v in card_spend.items()
                if b == brand and l == level and conn and conn in nm
            )
            card_txs = []
            for (nm, b, l), items in card_purchases.items():
                if b == brand and l == level and conn and conn in nm:
                    card_txs.extend(items)
            card_txs.sort(key=lambda x: x["date"] or "", reverse=True)
            card_txs = [dict(x, logo=logo) for x in card_txs[:120]]
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
                "transactions": card_txs,
            })
        elif kind == "BANK":
            name = a.get("customName") or a.get("name") or ""
            if name.strip().lower() in HIDDEN_ACCOUNT_NAMES:
                continue
            if str(a.get("subtype") or "").upper() in SAVINGS_SUBTYPES:
                continue
            bal = float(a.get("balance") or 0.0)
            total_bank += bal
            bank.append({
                "name": name,
                "balance": round(bal, 2),
                "logo": logo,
                "subtype": a.get("subtype"),
            })

    # Manual (non-Open-Finance) checking accounts.
    for m in manual.bank_accounts_for_overview():
        bank.append(m)
        total_bank += m["balance"]

    # --- spend rhythm + heatmap ----------------------------------------------
    cur_by_day = cur_s.get("by_day") or {}
    prev_by_day = prev_s.get("by_day") or {}
    cur_spent = float(cur_s.get("total_spent") or 0.0)
    prev_spent = float(prev_s.get("total_spent") or 0.0)

    # --- categories with variation (date-filtered spending, like Pierre) ------
    prev_accounts_tx = ((prev_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    cur_cat, cur_cat_items = _spend_by_category(cur_accounts_tx, f"{year:04d}-{month:02d}", collect=True, logo_fn=logo_fn)
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
            "transactions": cur_cat_items.get(name, [])[:30],
        })
    categories.sort(key=lambda c: c["total"], reverse=True)
    top_category = categories[0] if categories else None

    # --- recent transactions, split by source (card vs bank account) ----------
    cur_accounts = ((cur_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    card_items, acct_items = [], []

    def _recent_row(it, flow):
        return {
            "date": it.get("date"),
            "description": it.get("description") or it.get("merchant") or it.get("category"),
            "category": it.get("category"),
            "amount": round(abs(float(it.get("amount") or 0.0)), 2),
            "flow": flow,
            "logo": logo_fn(it),
        }

    for a in cur_accounts.values():
        if not isinstance(a, dict):
            continue
        for it in a.get("received") or []:
            acct_items.append(_recent_row(it, "in"))
        for it in a.get("bank_transfer") or []:
            acct_items.append(_recent_row(it, "out"))
    # Recent card items = the invoice items already computed per card (upfront +
    # installment portions), so they match the card totals.
    for c in credit:
        for t in c.get("transactions") or []:
            card_items.append(dict(t, flow="out"))

    card_items.sort(key=lambda x: x["date"] or "", reverse=True)
    acct_items.sort(key=lambda x: x["date"] or "", reverse=True)

    # --- planned (recurring) vs realized, with reconciliation ----------------
    plan = recurring.planned_for_month(f"{year:04d}-{month:02d}")
    actual_desc = []
    for a in cur_accounts_tx.values():
        if not isinstance(a, dict):
            continue
        for it in (a.get("received") or []) + (a.get("bank_transfer") or []):
            actual_desc.append(_norm(it.get("description") or it.get("merchant") or ""))
        for cv in (a.get("credit_cards") or {}).values():
            if isinstance(cv, dict):
                for it in cv.get("purchases") or []:
                    actual_desc.append(_norm(it.get("description") or it.get("merchant") or ""))

    def _posted(desc):
        d = _norm(desc)
        return bool(d) and any(d in ad for ad in actual_desc)

    plan_items, pending_total = [], 0.0
    for it in plan["items"]:
        posted = _posted(it["description"])
        if not posted:
            pending_total += float(it["amount"] or 0.0)
        plan_items.append({
            "id": it["id"],
            "description": it["description"],
            "bucket": it["bucket"],
            "amount": round(float(it["amount"] or 0.0), 2),
            "posted": posted,
            "installment_label": it.get("installment_label"),
        })

    return {
        "month": f"{year:04d}-{month:02d}",
        "planned": {
            "buckets": plan["buckets"],
            "sobra": plan["sobra"],
            "pending_total": round(pending_total, 2),
            "items": plan_items,
        },
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

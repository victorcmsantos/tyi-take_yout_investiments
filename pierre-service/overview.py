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
# Not real spending categories (card settlement / transfers between own accounts).
HIDDEN_SPEND_CATEGORIES = {
    "Pagamento de cartão de crédito",
    "Transferência mesma titularidade",
    "Movimentação interna",
}


def _norm(text):
    """Lowercase + strip accents, for matching bank/connector names."""
    return unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower()


def _amt(it):
    """BRL value of a transaction: effectiveAmount is the converted amount for
    international purchases; amount may be in the foreign currency."""
    v = it.get("effectiveAmount")
    if v is None:
        v = it.get("amount")
    try:
        return abs(float(v or 0.0))
    except (TypeError, ValueError):
        return 0.0


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


def _next_month(year, month):
    return (year + 1, 1) if month == 12 else (year, month + 1)


def _cycle_window(year, month, closing=None):
    """Purchase window for the statement that CLOSES in month M (on the closing
    day) — i.e. the card spending OF month M, which is paid on the 1st of M+1 and
    which the user budgets as month M's debt. Window = [day after the M-1 closing,
    the M closing]. ``closing`` defaults to the global setting; closing <= 1 falls
    back to the calendar month M."""
    if closing is None:
        closing = settings.card_closing_day()
    start, end, _ = _month_bounds(year, month)
    if closing >= 2:
        py, pm = _prev_month(year, month)
        end = date(year, month, min(closing, calendar.monthrange(year, month)[1])).isoformat()
        start = (date(py, pm, min(closing, calendar.monthrange(py, pm)[1])) + timedelta(days=1)).isoformat()
    return start, end


def _closing_resolver(credit_accounts, ym=None):
    """Build fn(x) -> closing day for a card in month ``ym`` (YYYY-MM), resolving
    the MOST SPECIFIC rule via settings.closing_for: a per-month override, else the
    per-card default (by last4) when brand+level+connector pin one physical card,
    else the per-bank default, else the global default. ``x`` may be a purchase's
    ``account_info`` (name/brand/level), a full credit account (creditData +
    number), or a bare connector string."""
    def resolve(x):
        if isinstance(x, dict) and isinstance(x.get("creditData"), dict):  # full account
            return settings.closing_for(x.get("connectorName"), str(x.get("number") or "")[-4:], ym)
        if isinstance(x, dict):
            name = _norm(x.get("name"))
            brand = str(x.get("brand") or "").upper()
            level = str(x.get("level") or "").upper()
        else:
            name, brand, level = _norm(x), "", ""
        best = None
        for a in credit_accounts:
            if not (_norm(a.get("connectorName")) and _norm(a.get("connectorName")) in name):
                continue
            acd = a.get("creditData") or {}
            ab = str(acd.get("brand") or "").upper()
            al = str(acd.get("level") or "").upper()
            if (not brand or ab == brand) and (not level or al == level):
                best = a
                if brand and level and ab == brand and al == level:
                    break  # exact card match
        if best is not None:
            return settings.closing_for(best.get("connectorName"), str(best.get("number") or "")[-4:], ym)
        return settings.closing_for(x.get("name") if isinstance(x, dict) else x, None, ym)
    return resolve


def _summary(payload):
    data = (payload or {}).get("data") or {}
    return data.get("summary") or {}


def _pct_delta(cur, prev):
    if not prev:
        return None
    return round(((cur - prev) / prev) * 100.0, 1)


def _spend_by_category(accounts_tx, ym, collect=False, logo_fn=None, include_cards=True):
    """Aggregate real spending by category for transactions whose DATE falls in
    the given month (YYYY-MM) — installments keep their purchase-month, matching
    Pierre. Income and settlement/internal categories are excluded. When
    ``collect`` is set, also returns the underlying transactions per category.
    With ``include_cards=False`` only bank (conta) movements are counted — card
    spend is merged in separately from the invoice cycle (see build_overview)."""
    agg = {}
    items_by_cat = {}

    def add(items, source):
        for it in items or []:
            if str(it.get("date") or "")[:7] != ym:
                continue
            cat = it.get("category")
            if cat in HIDDEN_SPEND_CATEGORIES:
                continue
            value = _amt(it)
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
        if include_cards:
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
    """Flat, date-sorted list of the month's transactions for the Transações tab.
    Account movements (conta) follow the CALENDAR month; card purchases (cartão)
    follow the INVOICE that is due in month M (so they match the Cartões tab and
    the cash-flow model: purchases land in the month their statement is paid)."""
    start, end, _ = _month_bounds(year, month)
    acc_raw = _acc_list(pierre.get_accounts())
    logo_fn = _make_logo_fn(acc_raw)
    # overrides applied AFTER manual.inject (below) so rules reach manual txs too.
    card_by_id = {}
    credit_accounts = []
    for a in acc_raw:
        if a.get("type") == "CREDIT":
            cd = a.get("creditData") or {}
            card_by_id[a.get("id")] = (str(cd.get("brand") or "").upper(),
                                       str(cd.get("level") or "").upper(), a.get("connectorName"))
            credit_accounts.append(a)

    # Each card's statement closes on its own day (Itaú 22, Santander 25, or a
    # per-card default / per-month override...); the statement closing in month M
    # is the card spending OF month M.
    _closing = _closing_resolver(credit_accounts, ym=f"{year:04d}-{month:02d}")
    _win_cache = {}

    def _win_for(x):
        cl = _closing(x)
        if cl not in _win_cache:
            _win_cache[cl] = _cycle_window(year, month, cl)
        return _win_cache[cl]

    def row(it, source, flow, date_override=None):
        return {
            "date": date_override or it.get("date"),
            "description": it.get("description") or it.get("merchant") or it.get("category"),
            "category": it.get("category"),
            "amount": round(_amt(it), 2),
            "source": source,
            "flow": flow,
            "logo": logo_fn(it),
            "manual_id": it.get("manual_id"),
        }

    rows = []

    # Account movements: calendar month. inject first, then overrides (so rules
    # recategorize manual transactions too).
    tx = overrides.apply(manual.inject(pierre.get_transactions(start_date=start, end_date=end), start, end))
    accounts = ((tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    for a in accounts.values():
        if not isinstance(a, dict):
            continue
        for it in a.get("received") or []:
            rows.append(row(it, "conta", "in"))
        for it in a.get("bank_transfer") or []:
            rows.append(row(it, "conta", "out"))

    # Card purchases: the statement closing in month M (paid 1st of M+1) = upfront
    # purchases in the window + installment portions due in month M, each card
    # using its own bank's closing day.
    _wins = [_win_for(a) for a in credit_accounts]
    fetch_start = min((w[0] for w in _wins), default=_cycle_window(year, month)[0])
    fetch_end = max((w[1] for w in _wins), default=_cycle_window(year, month)[1])
    cyc = overrides.apply(pierre.get_transactions(start_date=fetch_start, end_date=fetch_end, account_type="CREDIT"))
    cyc_accounts = ((cyc.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    for a in cyc_accounts.values():
        if not isinstance(a, dict):
            continue
        for cv in (a.get("credit_cards") or {}).values():
            if not isinstance(cv, dict):
                continue
            for it in cv.get("purchases") or []:
                if it.get("installment_due_date"):
                    continue  # installment -> from /installments below
                ws, we = _win_for(it.get("account_info") or {})
                if not (ws <= (it.get("date") or "") <= we):
                    continue
                rows.append(row(it, "cartao", "out"))

    # Parcelas follow the monthly schedule: a parcela due in calendar month M is
    # billed on the statement that closes in M (paid the 1st of M+1).
    cur_ym = f"{year:04d}-{month:02d}"
    try:
        inst_raw = pierre.get_installments(start_date=f"{year - 2}-01-01", end_date=end)
        inst_purchases = ((inst_raw.get("data") or {}).get("purchases")) or []
    except Exception:
        inst_purchases = []
    for p in inst_purchases:
        bl = card_by_id.get(p.get("accountId"))
        if not bl:
            continue
        for inst in p.get("installments") or []:
            due = str(inst.get("dueDate") or "")[:10]
            if due[:7] != cur_ym:
                continue
            rows.append({
                "date": due,
                "description": f"{p.get('description')} {inst.get('installmentNumber')}/{inst.get('totalInstallments')}",
                "category": inst.get("category") or "Parcelamento",
                "amount": round(abs(float(inst.get("amount") or 0.0)), 2),
                "source": "cartao",
                "flow": "out",
                "logo": "",
            })

    rows.sort(key=lambda r: r["date"] or "", reverse=True)
    return {"month": f"{year:04d}-{month:02d}", "transactions": rows}


def build_overview(year, month):
    cur_start, cur_end, cur_days = _month_bounds(year, month)
    py, pm = _prev_month(year, month)
    prev_start, prev_end, prev_days = _month_bounds(py, pm)

    # overrides AFTER manual.inject so recategorization rules also apply to manual
    # transactions (e.g. "Eliane -> Investimentos" on a manual ECS transfer).
    cur_tx = overrides.apply(manual.inject(pierre.get_transactions(start_date=cur_start, end_date=cur_end), cur_start, cur_end))
    prev_tx = overrides.apply(manual.inject(pierre.get_transactions(start_date=prev_start, end_date=prev_end), prev_start, prev_end))
    accounts_raw = pierre.get_accounts()

    cur_s = _summary(cur_tx)
    prev_s = _summary(prev_tx)
    cf = cashflow.summarize_cashflow(cur_tx)
    buckets = cf["buckets"]
    bucket_items = cf.get("bucket_items") or {"receita": [], "despfixa": [], "cartao": [], "despavulsa": []}

    cur_accounts_tx = ((cur_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}

    # Card id -> (brand, level, connector), to map installments (which carry
    # accountId) to the physical card and its bank (for the closing day).
    card_by_id = {}
    credit_accounts = []
    for a in _acc_list(accounts_raw):
        if a.get("type") == "CREDIT":
            cd = a.get("creditData") or {}
            card_by_id[a.get("id")] = (str(cd.get("brand") or "").upper(),
                                       str(cd.get("level") or "").upper(), a.get("connectorName"))
            credit_accounts.append(a)

    # The card spending OF month M = the statement that CLOSES in M (paid on the
    # 1st of M+1, which the user budgets as month M's debt). Its exact total is the
    # bank's CLOSED bill (get-bills, below); here we build the line items for that
    # cycle, since Pierre tags no billId. Closing day varies BY CARD (per-card
    # override by last4) and BY BANK (Itaú 22, Santander 25...), so each card uses
    # its own window, all closing in month M.
    month_ym = f"{year:04d}-{month:02d}"
    _closing = _closing_resolver(credit_accounts, ym=month_ym)
    _win_cache = {}

    def _win_for(x):
        cl = _closing(x)
        if cl not in _win_cache:
            _win_cache[cl] = _cycle_window(year, month, cl)
        return _win_cache[cl]

    _wins = [_win_for(a) for a in credit_accounts]
    fetch_start = min((w[0] for w in _wins), default=_cycle_window(year, month)[0])
    fetch_end = max((w[1] for w in _wins), default=_cycle_window(year, month)[1])
    cycle_tx = overrides.apply(pierre.get_transactions(start_date=fetch_start, end_date=fetch_end))
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
                ai = it.get("account_info") or {}
                ws, we = _win_for(ai)
                d = it.get("date") or ""
                if not (ws <= d <= we):
                    continue
                key = (_norm(ai.get("name")), str(ai.get("brand") or "").upper(), str(ai.get("level") or "").upper())
                value = _amt(it)
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

    # 2) installment portions billed in this statement. Unlike upfront purchases
    # (which follow the closing-day window), a parcela follows the bank's monthly
    # schedule: a parcela DUE in calendar month M is billed on the statement that
    # closes in M (paid on the 1st of M+1). So we match by month M — this keeps the
    # final parcela on its real invoice (e.g. VINILICOS 2/2 due 27/05 lands on the
    # statement of May, paid 01/06).
    cur_ym = f"{year:04d}-{month:02d}"
    try:
        inst_raw = pierre.get_installments(start_date=f"{year - 2}-01-01", end_date=cur_end)
        inst_purchases = ((inst_raw.get("data") or {}).get("purchases")) or []
    except Exception:
        inst_purchases = []
    for p in inst_purchases:
        bl = card_by_id.get(p.get("accountId"))
        if not bl:
            continue
        brand, level, conn = bl
        key = (_norm(p.get("accountName")), brand, level)
        for inst in p.get("installments") or []:
            due = str(inst.get("dueDate") or "")[:10]
            if due[:7] != cur_ym:
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

    # Exact statement totals from the bank's CLOSED bills (get-bills). Month M's
    # card debt is the statement that closes in M and is PAID on the 1st of M+1,
    # so we match the bill whose dueDate is in M+1. Source of truth for the
    # headline per-card total and the "Cartão" bucket; the cycle sum above is the
    # fallback while that statement is still open (the current month).
    ny, nm = _next_month(year, month)
    month_key = f"{ny:04d}-{nm:02d}"
    bill_by_acct = {}
    try:
        bills_raw = pierre.get_bills()
        bdata = (bills_raw or {}).get("data")
        if isinstance(bdata, dict):
            bdata = bdata.get("data")
        for b in bdata or []:
            if str(b.get("dueDate") or "")[:7] != month_key:
                continue
            aid = b.get("accountId")
            slot = bill_by_acct.setdefault(aid, {"total": 0.0, "min": 0.0, "due": str(b.get("dueDate") or "")[:10]})
            slot["total"] += float(b.get("totalAmount") or 0.0)
            slot["min"] += float(b.get("minimumPaymentAmount") or 0.0)
    except Exception:
        bill_by_acct = {}

    if bill_by_acct:
        cartao_total = sum(v["total"] for v in bill_by_acct.values())
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
            # Headline total = the bank's closed bill (exact). Fall back to the
            # cycle sum only when no bill is due in this month yet.
            bill = bill_by_acct.get(a.get("id"))
            if bill:
                spent = bill["total"]
                invoice_due = bill["due"]
                invoice_min = round(bill["min"], 2)
            else:
                spent = sum(
                    v for (knm, kb, kl), v in card_spend.items()
                    if kb == brand and kl == level and conn and conn in knm
                )
                # Open (not-yet-closed) statement: it will be paid on the 1st of M+1.
                invoice_due = f"{ny:04d}-{nm:02d}-01"
                invoice_min = None
            card_txs = []
            for (knm, kb, kl), items in card_purchases.items():
                if kb == brand and kl == level and conn and conn in knm:
                    card_txs.extend(items)
            card_txs.sort(key=lambda x: x["date"] or "", reverse=True)
            card_txs = [dict(x, logo=logo) for x in card_txs[:120]]
            # Reconciliation: the cycle line items can't sum exactly to the bank's
            # closed bill (purchase-date vs posting-date at the cycle boundary, and
            # Pierre tags no billId). Add an explicit adjustment row so the list
            # always totals the exact invoice. Positive = unlisted spend (shown as
            # an expense); negative = listed beyond the bill (shown as a credit).
            if bill:
                adj = round(spent - sum(t["amount"] for t in card_txs), 2)
                if abs(adj) >= 0.01:
                    card_txs.append({
                        "date": bill["due"],
                        "description": "Ajuste de fechamento (data de corte)",
                        "category": "Ajuste",
                        "amount": adj,
                        "source": "cartao",
                        "logo": logo,
                        "adjustment": True,
                    })
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
                "invoice_total": round(spent, 2),
                "invoice_min": invoice_min,
                "invoice_closed": bool(bill),
                # effective = what the cycle window uses this month; default = the
                # per-card/bank fallback (ignoring any month override); month = the
                # explicit override for THIS month (null if none).
                "closing_day": settings.closing_for(a.get("connectorName"), str(a.get("number") or "")[-4:], month_ym),
                "closing_day_default": settings.closing_for(a.get("connectorName"), str(a.get("number") or "")[-4:]),
                "closing_day_month": settings.card_closing_overrides().get(f"{str(a.get('number') or '')[-4:]}|{month_ym}"),
                "logo": logo,
                "last4": str(a.get("number") or "")[-4:],
                "additional_cards": additional,
                "due_date": invoice_due,
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
    # Account (conta) spend follows the CALENDAR month; card spend follows the
    # INVOICE CYCLE (same basis as the Cartões tile and buckets.cartao), so the
    # card categories here reflect the real invoice being built — not just the
    # handful of purchases posted since the 1st. Current-month card spend is
    # reused from `card_purchases` (already installment-aware); the previous
    # month keeps its calendar-month card spend as an approximate baseline.
    prev_accounts_tx = ((prev_tx.get("data") or {}).get("transactions") or {}).get("accounts") or {}
    cur_cat, cur_cat_items = _spend_by_category(
        cur_accounts_tx, cur_ym, collect=True, logo_fn=logo_fn, include_cards=False)
    prev_cat = _spend_by_category(prev_accounts_tx, f"{py:04d}-{pm:02d}")

    # Merge the current invoice-cycle card purchases into the category totals.
    for items in card_purchases.values():
        for it in items:
            cat = it.get("category")
            if cat in HIDDEN_SPEND_CATEGORIES:
                continue
            cur_cat[cat] = cur_cat.get(cat, 0.0) + it.get("amount", 0.0)
            cur_cat_items.setdefault(cat, []).append({
                "date": it.get("date"),
                "description": it.get("description"),
                "amount": it.get("amount"),
                "source": "cartao",
                "logo": "",
            })
    for items in cur_cat_items.values():
        items.sort(key=lambda x: x["date"] or "", reverse=True)

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
            "amount": round(_amt(it), 2),
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
            if t.get("adjustment"):
                continue  # reconciliation line, not a real purchase
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
        # Cartão bucket items = the real per-card invoice detail (incl. the
        # closing adjustment), so the Fluxo drill-down sums to the bill total.
        "bucket_items": {
            **bucket_items,
            "cartao": [
                {"date": t.get("date"), "description": t.get("description"),
                 "category": t.get("category"), "amount": t.get("amount"),
                 "source": "cartao", "adjustment": bool(t.get("adjustment"))}
                for c in credit for t in (c.get("transactions") or [])
            ],
        },
        "recent_card": card_items[:6],
        "recent_account": acct_items[:6],
    }

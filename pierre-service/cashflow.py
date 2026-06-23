"""Maps Pierre transaction data into the cash-flow model:
Receita / DespFixa / Cartao / DespAvulsa -> Sobra.

Walks the per-account item lists (received / bank_transfer / credit_cards) so
there is NO double counting:
  - Receita      = incoming bank items (received), internal transfers excluded
  - Cartao       = credit-card PURCHASES (not the bill payment)
  - DespFixa     = bank debits in recurring categories
  - DespAvulsa   = other bank debits (incl. PIX sent)
  - excluded     = card bill payments (settlement) and same-ownership transfers
  - Sobra        = Receita - Cartao - DespFixa - DespAvulsa

Override category sets via env (comma-separated): PIERRE_FIXED_CATEGORIES,
PIERRE_INTERNAL_CATEGORIES, PIERRE_CARD_PAYMENT_CATEGORIES.
"""

import os

DEFAULT_FIXED_CATEGORIES = {
    "Seguros",
    "Escola",
    "Saúde",
    "Saúde e bem-estar",
    "Telecomunicação",
    "Internet",
    "Serviços digitais",
    "Serviços",
    "Impostos sobre operações financeiras",
}
# Internal movements that must not count as income or expense.
DEFAULT_INTERNAL_CATEGORIES = {"Transferência mesma titularidade"}
# Inbound transfer categories: by default these are money circulating, NOT
# income. An inbound transfer only counts as Receita when its description
# matches a known income source (the user's billing entities / clients), so
# real income (e.g. "Pix recebido VCMS/ECS") is kept while internal moves and
# FGC reimbursements ("VICTOR...", "FUNDO GARANTIDOR") are excluded.
DEFAULT_INBOUND_IGNORE_CATEGORIES = {
    "Transferência - TED",
    "Transferência - PIX",
    "Transferências",
}
# Description keywords that mark an inbound transfer as real income. Income is
# now captured at the source (the VCMS/ECS PJ accounts), so transfers FROM them
# into the personal accounts are internal, not new income. Override via
# PIERRE_INCOME_SOURCES if some inbound transfer really is external income.
DEFAULT_INCOME_SOURCES = set()
# Credit-card bill settlement (the spend is already counted as purchases).
DEFAULT_CARD_PAYMENT_CATEGORIES = {"Pagamento de cartão de crédito"}


def _set_from_env(key, default):
    raw = os.getenv(key, "")
    if raw.strip():
        return {part.strip() for part in raw.split(",") if part.strip()}
    return set(default)


def _amount(item):
    try:
        return abs(float(item.get("effectiveAmount") if item.get("effectiveAmount") is not None
                          else item.get("amount") or 0.0))
    except (TypeError, ValueError):
        return 0.0


def _month(item):
    return str(item.get("date") or "")[:7]


def summarize_cashflow(transactions_payload):
    fixed = _set_from_env("PIERRE_FIXED_CATEGORIES", DEFAULT_FIXED_CATEGORIES)
    internal = _set_from_env("PIERRE_INTERNAL_CATEGORIES", DEFAULT_INTERNAL_CATEGORIES)
    card_payment = _set_from_env("PIERRE_CARD_PAYMENT_CATEGORIES", DEFAULT_CARD_PAYMENT_CATEGORIES)
    inbound_ignore = _set_from_env("PIERRE_INBOUND_IGNORE_CATEGORIES", DEFAULT_INBOUND_IGNORE_CATEGORIES)
    income_sources = _set_from_env("PIERRE_INCOME_SOURCES", DEFAULT_INCOME_SOURCES)

    def _is_income_transfer(item):
        text = (str(item.get("description") or "") + " " + str(item.get("merchant") or "")).upper()
        return any(src.upper() in text for src in income_sources)

    data = (transactions_payload or {}).get("data") or {}
    accounts = (data.get("transactions") or {}).get("accounts") or {}
    summary = data.get("summary") or {}

    receita = cartao = fixa = avulsa = 0.0
    by_month_income = {}
    by_month_expense = {}
    cat_acc = {}

    def add_month(store, item, value):
        m = _month(item)
        if m:
            store[m] = round(store.get(m, 0.0) + value, 2)

    def add_cat(category, bucket, value):
        # Key by (category, bucket): the same category can be both income and
        # expense (e.g. PIX received vs sent), so they must not collapse.
        key = (category or "Não categorizada", bucket)
        entry = cat_acc.setdefault(key, {"total": 0.0})
        entry["total"] += value

    for account in accounts.values():
        if not isinstance(account, dict):
            continue

        # Incoming. Internal transfers are excluded; inbound TED/PIX only counts
        # as income when the description matches a known income source.
        for item in account.get("received") or []:
            category = item.get("category")
            if category in internal:
                continue
            if category in inbound_ignore and not _is_income_transfer(item):
                continue
            value = _amount(item)
            receita += value
            add_month(by_month_income, item, value)
            add_cat(category, "receita", value)

        # Bank debits out. Skip card settlement and internal transfers.
        for item in account.get("bank_transfer") or []:
            category = item.get("category")
            if category in internal or category in card_payment:
                continue
            value = _amount(item)
            bucket = "fixa" if category in fixed else "avulsa"
            if bucket == "fixa":
                fixa += value
            else:
                avulsa += value
            add_month(by_month_expense, item, value)
            add_cat(category, bucket, value)

        # Credit-card purchases (the real card spend).
        credit_cards = account.get("credit_cards") or {}
        if isinstance(credit_cards, dict):
            for card in credit_cards.values():
                if not isinstance(card, dict):
                    continue
                for item in card.get("purchases") or []:
                    category = item.get("category")
                    if category in internal:
                        continue
                    value = _amount(item)
                    cartao += value
                    add_month(by_month_expense, item, value)
                    add_cat(category, "cartao", value)

    sobra = receita - cartao - fixa - avulsa
    categories = sorted(
        ({"category": name, "bucket": bucket, "total": round(v["total"], 2)}
         for (name, bucket), v in cat_acc.items()),
        key=lambda c: c["total"],
        reverse=True,
    )

    return {
        "period": summary.get("period"),
        "buckets": {
            "receita": round(receita, 2),
            "despfixa": round(fixa, 2),
            "cartao": round(cartao, 2),
            "despavulsa": round(avulsa, 2),
            "sobra": round(sobra, 2),
        },
        "by_month_income": by_month_income,
        "by_month_expense": by_month_expense,
        "categories": categories,
        "totals_source": {
            "total_received": summary.get("total_received"),
            "total_spent": summary.get("total_spent"),
            "total_bank_transfer": summary.get("total_bank_transfer"),
        },
    }

"""Pluggy → formato Pierre: fonte de dados alternativa que emula o shape dos
payloads do Pierre (get_accounts / get_transactions "structured" / get_bills /
get_installments) a partir da API da Pluggy (pluggy_api).

Objetivo: overview/ledger/cashflow consomem qualquer uma das fontes sem mudar.
Diferenças de semântica tratadas aqui:
  - categorias: Pluggy manda EN + categoryId; traduzimos via /categories
    (descriptionTranslated) — a MESMA nomenclatura PT que o Pierre repassa.
    Subcategorias de "Transferência mesma titularidade - X" são colapsadas na
    categoria-mãe para não quebrar os conjuntos neutros do cashflow.
  - parcelas: no feed da Pluggy cada parcela é uma transação futura datada no
    mês da fatura, com creditCardMetadata (nº da parcela / total). Purchases
    parceladas recebem installment_due_date (o ledger as pula) e o
    get_installments é sintetizado agrupando essas transações por compra.
  - identidade do banco: itens Open Finance usam o connector genérico
    "MeuPluggy"; o rótulo por item vem de PLUGGY_ITEM_LABELS
    ("itemId=Rótulo,..."), com fallback por heurística do nome das contas.

Cache: memo em processo com TTL (PLUGGY_CACHE_TTL_SECONDS, default 120s) — a
Pluggy não tem o rate-limit do Pierre, então não precisamos do SQLite.
"""

import logging
import os
import threading
import time
from datetime import datetime

import inter_api
import pluggy_api

log = logging.getLogger("pluggy_source")

_ICON = "https://cdn.pluggy.ai/assets/connector-icons/{}.svg".format
_KNOWN_ICONS = {
    "itau": _ICON(201),
    "santander": _ICON(208),
    "nubank": _ICON(212),
    "inter": _ICON(215),
    "mercado pago": _ICON(206),
    "bradesco": _ICON(203),
}

# Categorias que o cashflow trata como neutras — subvariantes colapsam na mãe.
_COLLAPSE_PREFIXES = (
    "Transferência mesma titularidade",
    "Pagamento de cartão de crédito",
)

# Traduções da Pluggy que divergem da nomenclatura do Pierre (usada no app).
_CATEGORY_FIXUPS = {
    "Vestiário": "Vestuário",
}


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _ttl():
    try:
        return int(os.getenv("PLUGGY_CACHE_TTL_SECONDS", "120"))
    except ValueError:
        return 120


_MEMO = {}
_MEMO_LOCK = threading.Lock()


def _memo(key, fn):
    now = time.time()
    with _MEMO_LOCK:
        hit = _MEMO.get(key)
        if hit and now - hit[0] < _ttl():
            return hit[1]
    value = fn()
    with _MEMO_LOCK:
        _MEMO[key] = (now, value)
    return value


# --- rótulos por item -----------------------------------------------------------

def _item_labels():
    """PLUGGY_ITEM_LABELS: "itemId=Rótulo,itemId=Rótulo"."""
    raw = os.getenv("PLUGGY_ITEM_LABELS") or ""
    out = {}
    for part in raw.split(","):
        if "=" in part:
            item_id, label = part.split("=", 1)
            out[item_id.strip()] = label.strip()
    return out


def _guess_label(accounts):
    names = " ".join(
        f"{a.get('name') or ''} {a.get('marketingName') or ''}" for a in accounts
    ).lower()
    for needle, label in (
        ("itau", "Itaú"), ("itaú", "Itaú"),
        ("santander", "Santander"),
        ("mercado pago", "Mercado Pago"),
        ("nubank", "Nubank"), ("nu pagamentos", "Nubank"),
        ("inter", "Inter"),
        ("bradesco", "Bradesco"),
    ):
        if needle in names:
            return label
    return "Cartões"


def _icon_for(label, fallback=None):
    import unicodedata

    key = unicodedata.normalize("NFKD", str(label or "")).encode("ascii", "ignore").decode().lower()
    for needle, url in _KNOWN_ICONS.items():
        if needle in key:
            return url
    return fallback


# --- categorias -----------------------------------------------------------------

def _category_map():
    def _fetch():
        cats = pluggy_api._get("/categories").get("results", [])
        return {c.get("id"): (c.get("descriptionTranslated") or c.get("description")) for c in cats}

    return _memo("categories", _fetch)


def _translate_category(tx):
    name = _category_map().get(tx.get("categoryId")) or tx.get("category") or "Não categorizada"
    for prefix in _COLLAPSE_PREFIXES:
        if name.startswith(prefix + " -") or name.startswith(prefix + "-"):
            name = prefix
            break
    return _CATEGORY_FIXUPS.get(name, name)


# --- contas ---------------------------------------------------------------------

def _raw_accounts():
    def _fetch():
        labels = _item_labels()
        out = []
        for item_id in pluggy_api.item_ids():
            try:
                item = pluggy_api.get_item(item_id)
                accounts = pluggy_api.get_accounts(item_id)
            except Exception as exc:
                log.warning("pluggy: item %s indisponivel: %s", item_id, exc)
                continue
            label = labels.get(item_id) or _guess_label(accounts)
            conn = item.get("connector") or {}
            icon = _icon_for(label, fallback=conn.get("imageUrl"))
            for a in accounts:
                a = dict(a)
                a["connectorName"] = label
                a["connectorImageUrl"] = icon
                a["itemLastUpdatedAt"] = item.get("lastUpdatedAt") or item.get("updatedAt")
                a["itemIsActive"] = (item.get("status") not in ("LOGIN_ERROR", "OUTDATED"))
                out.append(a)
        return out

    return _memo("accounts", _fetch)


def _inter_accounts():
    """Contas PJ do Inter como pseudo-contas no shape Pluggy/Pierre."""
    out = []
    for p in inter_api.account_prefixes():
        label = f"{p} (Inter)"
        balance = None
        try:
            balance = (inter_api.get_saldo(p) or {}).get("disponivel")
        except Exception as exc:  # noqa: BLE001 - saldo é opcional
            log.warning("inter: saldo %s indisponivel: %s", p, exc)
        out.append({
            "id": f"inter-{p.lower()}",
            "itemId": f"inter-{p.lower()}",
            "type": "BANK",
            "subtype": "CHECKING_ACCOUNT",
            "name": p,
            "number": None,
            "balance": balance,
            "currencyCode": "BRL",
            "creditData": None,
            "marketingName": f"Banco Inter PJ — {p}",
            "connectorName": label,
            "connectorImageUrl": _KNOWN_ICONS["inter"],
            "itemLastUpdatedAt": _now_iso(),
            "itemIsActive": True,
        })
    return out


def get_accounts():
    data = _raw_accounts() + _memo("inter_accounts", _inter_accounts)
    return {"success": True, "data": data, "count": len(data), "timestamp": _now_iso()}


# --- transações -----------------------------------------------------------------

def _fetch_account_txs(account_id, start, end):
    return _memo(
        f"txs:{account_id}:{start}:{end}",
        lambda: pluggy_api.get_transactions(account_id, date_from=start, date_to=end, max_pages=40),
    )


def _tx_item(t, account, group_name, transaction_type):
    cc = t.get("creditCardMetadata") or {}
    # Compra internacional: `amount` vem na moeda original (USD etc.) e
    # `amountInAccountCurrency` traz o valor em BRL — é este que o app usa.
    in_account = t.get("amountInAccountCurrency")
    amount = abs(float(in_account if in_account is not None else (t.get("amount") or 0.0)))
    item = {
        "date": str(t.get("date") or "")[:10],
        "category": _translate_category(t),
        "amount": amount,
        "effectiveAmount": amount,
        "hasBrlEquivalent": in_account is not None,
        "currency": t.get("currencyCode") or "BRL",
        "account_info": {
            "name": group_name,
            "type": account.get("type"),
            "subtype": account.get("subtype"),
        },
        "type": t.get("type"),
        "merchant": ((t.get("merchant") or {}).get("name") if isinstance(t.get("merchant"), dict) else None) or "não identificado",
        "description": t.get("description") or t.get("descriptionRaw") or "",
        "transaction_type": transaction_type,
    }
    if account.get("type") == "CREDIT":
        cd = account.get("creditData") or {}
        item["account_info"].update({
            "brand": cd.get("brand"),
            "level": cd.get("level"),
            "status": (cd.get("status") or "ACTIVE"),
        })
        total_inst = cc.get("totalInstallments") or 0
        if total_inst and int(total_inst) > 1:
            # parcela: no feed Pluggy a data já é o mês da fatura
            item["installment_due_date"] = item["date"]
            item["installment_number"] = cc.get("installmentNumber")
            item["total_installments"] = total_inst
        if cc.get("purchaseDate"):
            item["purchase_date"] = str(cc.get("purchaseDate"))[:10]
        if cc.get("billForecastDate"):
            item["bill_forecast_date"] = cc.get("billForecastDate")
        # Final do PLÁSTICO que fez a compra (titular ou adicional). Só as
        # quitações de fatura ("Pagamento recebido") vêm sem ele.
        if cc.get("cardNumber"):
            item["card_last4"] = str(cc.get("cardNumber"))[-4:]
    return item


def get_transactions(start_date=None, end_date=None, account_type=None, fmt="structured"):
    accounts = _raw_accounts()
    if account_type:
        accounts = [a for a in accounts if a.get("type") == account_type]

    groups = {}
    total_received = total_transfer = total_spent = 0.0
    by_day = {}

    for a in accounts:
        label = a.get("connectorName") or "Conta"
        try:
            txs = _fetch_account_txs(a["id"], start_date, end_date)
        except Exception as exc:
            log.warning("pluggy: transacoes da conta %s falharam: %s", a.get("id"), exc)
            continue
        if a.get("type") == "CREDIT":
            group_name = f"{a.get('name')} - {label}"
            card_group = groups.setdefault(label, {"received": [], "bank_transfer": [], "credit_cards": {}})
            card = card_group["credit_cards"].setdefault(
                group_name, {"payments": [], "purchases": [], "total_payments": 0.0, "total_purchases": 0.0}
            )
            for t in txs:
                item = _tx_item(t, a, group_name, "credit_card")
                if (t.get("type") == "CREDIT") or (float(t.get("amount") or 0.0) < 0):
                    card["payments"].append(item)
                    card["total_payments"] = round(card["total_payments"] + item["amount"], 2)
                else:
                    card["purchases"].append(item)
                    card["total_purchases"] = round(card["total_purchases"] + item["amount"], 2)
                    if not item.get("installment_due_date"):
                        total_spent += item["amount"]
                        d = item["date"]
                        by_day[d] = round(by_day.get(d, 0.0) + item["amount"], 2)
        else:
            group = groups.setdefault(label, {"received": [], "bank_transfer": [], "credit_cards": {}})
            for t in txs:
                incoming = float(t.get("amount") or 0.0) > 0
                item = _tx_item(t, a, label, "received" if incoming else "bank_transfer")
                if incoming:
                    group["received"].append(item)
                    total_received += item["amount"]
                else:
                    group["bank_transfer"].append(item)
                    total_transfer += item["amount"]
                    total_spent += item["amount"]
                    d = item["date"]
                    by_day[d] = round(by_day.get(d, 0.0) + item["amount"], 2)

    # Contas PJ do Inter (extrato via API direta) — só quando o pedido inclui
    # contas bancárias (ledger/cashflow pedem sem filtro; ciclo CREDIT não).
    if account_type in (None, "", "BANK") and start_date and end_date:
        for p in inter_api.account_prefixes():
            label = f"{p} (Inter)"
            try:
                extrato = _memo(f"inter:{p}:{start_date}:{end_date}",
                                lambda p=p: inter_api.get_extrato(p, start_date, end_date))
            except Exception as exc:  # noqa: BLE001 - conta indisponível não derruba o resto
                log.warning("inter: extrato %s falhou: %s", p, exc)
                continue
            group = groups.setdefault(label, {"received": [], "bank_transfer": [], "credit_cards": {}})
            for t in extrato:
                item = _inter_tx_item(t, label)
                if item is None:
                    continue
                if item["type"] == "CREDIT":
                    item["transaction_type"] = "received"
                    group["received"].append(item)
                    total_received += item["amount"]
                else:
                    item["transaction_type"] = "bank_transfer"
                    group["bank_transfer"].append(item)
                    total_transfer += item["amount"]
                    total_spent += item["amount"]
                    d = item["date"]
                    by_day[d] = round(by_day.get(d, 0.0) + item["amount"], 2)

    summary = {
        "period": {"startDate": start_date, "endDate": end_date},
        "total_received": round(total_received, 2),
        "total_bank_transfer": round(total_transfer, 2),
        "total_spent": round(total_spent, 2),
        "by_day": by_day,
    }
    return {
        "success": True,
        "data": {"type": "structured", "transactions": {"accounts": groups}, "summary": summary},
        "count": sum(
            len(g["received"]) + len(g["bank_transfer"])
            + sum(len(c["purchases"]) + len(c["payments"]) for c in g["credit_cards"].values())
            for g in groups.values()
        ),
        "timestamp": _now_iso(),
    }


def _inter_internal_keywords():
    raw = os.getenv("INTER_INTERNAL_KEYWORDS") or "VICTOR,ELIANE,VCMS,ECS"
    return [k.strip().upper() for k in raw.split(",") if k.strip()]


def _inter_category(t, incoming, text):
    """Categoria heurística do extrato PJ (o Inter não categoriza). As regras de
    override do app rodam por cima e corrigem o que escapar."""
    kind = str(t.get("tipoTransacao") or "").upper()
    if any(k in text for k in _inter_internal_keywords()):
        return "Transferência mesma titularidade"
    if kind in ("APLICACAO", "RESGATE", "INVESTIMENTO"):
        return "Investimentos"
    if "DARF" in kind or "IMPOSTO" in kind or kind == "DAS":
        return "Impostos"
    if "DARF" in text or "IMPOSTO" in text or " DAS " in f" {text} " or "TRIBUTO" in text or "PREFEITURA" in text or "PMSP" in text:
        return "Impostos"
    if kind in ("TARIFA", "TAXA"):
        return "Serviços"
    if incoming:
        # entrada externa numa conta PJ = receita na origem
        return "Faturamento"
    if kind == "PIX":
        return "Transferência - PIX"
    if kind == "TED":
        return "Transferência - TED"
    return "Transferências"


def _inter_tx_item(t, group_name):
    date = str(t.get("dataEntrada") or t.get("dataTransacao") or "")[:10]
    try:
        amount = abs(float(t.get("valor") or 0.0))
    except (TypeError, ValueError):
        return None
    if not date or not amount:
        return None
    incoming = str(t.get("tipoOperacao") or "").upper() == "C"
    titulo = str(t.get("titulo") or "").strip()
    desc = str(t.get("descricao") or "").strip()
    text = f"{titulo} {desc}".upper()
    description = desc if titulo.upper() in desc.upper() else " ".join(x for x in (titulo, desc) if x)
    return {
        "date": date,
        "category": _inter_category(t, incoming, text),
        "amount": amount,
        "effectiveAmount": amount,
        "hasBrlEquivalent": False,
        "currency": "BRL",
        "account_info": {"name": group_name, "type": "BANK", "subtype": "CHECKING_ACCOUNT"},
        "type": "CREDIT" if incoming else "DEBIT",
        "merchant": "não identificado",
        "description": description or (t.get("tipoTransacao") or "Movimentação"),
        "transaction_type": "bank_transfer",
    }


# --- faturas --------------------------------------------------------------------

def get_bills(account_id=None):
    def _fetch():
        out = []
        for a in _raw_accounts():
            if a.get("type") != "CREDIT":
                continue
            if account_id and a.get("id") != account_id:
                continue
            try:
                bills = pluggy_api._get("/bills", {"accountId": a["id"]}).get("results", [])
            except Exception as exc:
                log.warning("pluggy: bills da conta %s falharam: %s", a.get("id"), exc)
                continue
            for b in bills:
                b = dict(b)
                b.setdefault("accountId", a["id"])
                b.setdefault("itemId", a.get("itemId"))
                out.append(b)
        return out

    bills = _memo(f"bills:{account_id or 'all'}", _fetch)
    return {"success": True, "data": bills, "count": len(bills), "filters": {"accountId": account_id}, "timestamp": _now_iso()}


# --- parcelas -------------------------------------------------------------------

def _bill_month_due(tx_date, account):
    """Mês da fatura em que a parcela cai, pela regra de fechamento do app:
    dia > fechamento ⇒ fatura do mês seguinte."""
    import settings

    try:
        closing = settings.closing_for(
            account.get("connectorName"), str(account.get("number") or "")[-4:], tx_date[:7]
        )
    except Exception:
        closing = 22
    try:
        year, month, day = int(tx_date[:4]), int(tx_date[5:7]), int(tx_date[8:10])
    except (TypeError, ValueError):
        return tx_date
    if day > int(closing):
        month += 1
        if month > 12:
            month, year = 1, year + 1
    return f"{year:04d}-{month:02d}-01"


def _installment_base_desc(desc):
    # "PAYPAL *MONITORIAS01/03" / "IPLACE            12/12" -> descrição-base
    import re

    return re.sub(r"\s*\d{1,2}/\d{1,2}\s*$", "", str(desc or "")).strip()


def get_installments(start_date=None, end_date=None):
    accounts = [a for a in _raw_accounts() if a.get("type") == "CREDIT"]
    label_by_id = {a["id"]: a.get("connectorName") for a in accounts}
    groups = {}
    for a in accounts:
        try:
            txs = _fetch_account_txs(a["id"], start_date, end_date)
        except Exception as exc:
            log.warning("pluggy: parcelas da conta %s falharam: %s", a.get("id"), exc)
            continue
        for t in txs:
            cc = t.get("creditCardMetadata") or {}
            total = cc.get("totalInstallments") or 0
            if not total or int(total) <= 1:
                continue
            base = _installment_base_desc(t.get("description"))
            purchase_date = str(cc.get("purchaseDate") or t.get("date") or "")[:10]
            key = (a["id"], base, int(total), purchase_date[:7])
            g = groups.setdefault(key, {
                "purchaseDate": purchase_date,
                "description": base,
                "accountName": label_by_id.get(a["id"]),
                "accountId": a["id"],
                "currencyCode": t.get("currencyCode") or "BRL",
                "totalInstallments": int(total),
                "installments": [],
            })
            # dueDate = mês da fatura pelo MODELO DO APP (fatura que fecha no mês
            # M, paga dia 1º de M+1), derivado do dia de fechamento do cartão —
            # o billForecastDate da Pluggy rotula pelo mês de abertura do ciclo
            # e a data da transação idem (Itaú data a parcela no início do ciclo).
            due = _bill_month_due(str(t.get("date") or "")[:10], a)
            in_account = t.get("amountInAccountCurrency")
            g["installments"].append({
                "description": t.get("description"),
                "amount": abs(float(in_account if in_account is not None else (t.get("amount") or 0.0))),
                "cardLast4": str(cc.get("cardNumber"))[-4:] if cc.get("cardNumber") else None,
                "installmentNumber": cc.get("installmentNumber"),
                "totalInstallments": int(total),
                "dueDate": due,
                "category": _translate_category(t),
                "status": t.get("status") or "PENDING",
                "billId": None,
            })
    purchases = []
    for g in groups.values():
        g["installments"].sort(key=lambda i: (i.get("installmentNumber") or 0, i.get("dueDate") or ""))
        vals = [i["amount"] for i in g["installments"]]
        g["installmentValue"] = vals[0] if vals else 0.0
        g["totalAmount"] = round(g["installmentValue"] * g["totalInstallments"], 2)
        g["amountRemaining"] = round(sum(v for i, v in zip(g["installments"], vals) if i.get("status") == "PENDING"), 2)
        purchases.append(g)
    purchases.sort(key=lambda p: p.get("purchaseDate") or "", reverse=True)
    return {
        "success": True,
        "data": {"purchases": purchases, "summary": {}, "purchasesByCard": {}, "instructions": ""},
        "purchases": purchases,
        "dateRange": {"startDate": start_date, "endDate": end_date},
        "timestamp": _now_iso(),
    }


def is_configured():
    return pluggy_api.is_configured() and bool(pluggy_api.item_ids())

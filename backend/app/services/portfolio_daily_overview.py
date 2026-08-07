"""Overview diario da carteira: fatos deterministicos + leitura IA.

Os fatos sao SEMPRE calculados aqui, dos dados reais: variacao do dia da
carteira e por classe (``assets.variation_day`` aplicado sobre o valor atual
das posicoes), maior alta/queda por classe e proximos vencimentos de renda
fixa. A IA (OpenClaw) apenas narra esses fatos — o prompt proibe inventar
numeros — e a narrativa fica cacheada por escopo em
``portfolio_daily_overview``; a geracao acontece atras de um POST explicito.
"""

import json
from datetime import date

from ..db import get_db
from ..openclaw_client import OpenClawError
from . import _legacy as legacy
from . import openclaw as openclaw_services

_CATEGORY_LABELS = {
    "br_stocks": "Acoes BR",
    "us_stocks": "Acoes US",
    "etfs": "ETFs",
    "crypto": "Cripto",
    "fiis": "FIIs",
}

_ALLOWED_TONE = {"positivo", "negativo", "neutro"}


def _scope_key(portfolio_ids):
    ids = sorted(
        {str(item).strip() for item in (portfolio_ids or []) if str(item).strip()},
        key=lambda value: (len(value), value),
    )
    return ",".join(ids) if ids else "all"


def build_daily_overview_facts(portfolio_ids):
    snapshot = legacy.get_portfolio_snapshot(portfolio_ids, sort_by="value", sort_dir="desc")
    positions = (snapshot or {}).get("positions") or []
    db = get_db()

    variations = {}
    tickers = sorted({str(p.get("ticker") or "").upper() for p in positions if p.get("ticker")})
    if tickers:
        placeholders = ",".join(["?"] * len(tickers))
        rows = db.execute(
            f"SELECT ticker, variation_day FROM assets WHERE ticker IN ({placeholders})",
            tuple(tickers),
        ).fetchall()
        variations = {str(row["ticker"]).upper(): row["variation_day"] for row in rows}

    classes = {}
    total_change = 0.0
    total_prev = 0.0
    for item in positions:
        ticker = str(item.get("ticker") or "").upper()
        category = str(item.get("category") or "outros")
        value = float(item.get("value") or 0.0)
        if value <= 0:
            continue
        state = classes.setdefault(
            category,
            {
                "key": category,
                "label": _CATEGORY_LABELS.get(category, category),
                "value": 0.0,
                "prev_value": 0.0,
                "change_value": 0.0,
                "best": None,
                "worst": None,
            },
        )
        state["value"] += value
        raw_var = variations.get(ticker)
        if raw_var is None:
            continue
        var = float(raw_var)
        denom = 1.0 + var / 100.0
        if denom <= 0:
            continue
        prev = value / denom
        change = value - prev
        state["prev_value"] += prev
        state["change_value"] += change
        total_prev += prev
        total_change += change
        mover = {"ticker": ticker, "variation_day": round(var, 2), "change_value": round(change, 2)}
        if state["best"] is None or var > state["best"]["variation_day"]:
            state["best"] = mover
        if state["worst"] is None or var < state["worst"]["variation_day"]:
            state["worst"] = mover

    class_list = []
    for state in sorted(classes.values(), key=lambda entry: entry["value"], reverse=True):
        prev_value = state.pop("prev_value")
        state["day_change_pct"] = round((state["change_value"] / prev_value) * 100.0, 2) if prev_value > 0 else 0.0
        state["change_value"] = round(state["change_value"], 2)
        state["value"] = round(state["value"], 2)
        class_list.append(state)

    today = date.today()
    pids = legacy.normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    fixed_rows = db.execute(
        f"""
        SELECT issuer, investment_type, aporte, maturity_date
        FROM fixed_incomes
        WHERE portfolio_id IN ({placeholders}) AND maturity_date >= ?
        ORDER BY maturity_date ASC
        """,
        tuple(pids) + (today.isoformat(),),
    ).fetchall()
    upcoming = []
    total_30d = 0.0
    total_90d = 0.0
    future_count = 0
    for row in fixed_rows:
        maturity = str(row["maturity_date"] or "")
        try:
            days_left = (date.fromisoformat(maturity) - today).days
        except ValueError:
            continue
        amount = float(row["aporte"] or 0.0)
        if amount <= 0:
            continue
        future_count += 1
        if days_left <= 30:
            total_30d += amount
        if days_left <= 90:
            total_90d += amount
        if len(upcoming) < 6:
            upcoming.append(
                {
                    "issuer": str(row["issuer"] or "").strip() or "Nao informado",
                    "investment_type": str(row["investment_type"] or "").strip(),
                    "amount": round(amount, 2),
                    "maturity_date": maturity,
                    "days_left": days_left,
                }
            )

    total_value = float((snapshot or {}).get("total_value") or 0.0)
    return {
        "ref_date": today.isoformat(),
        "portfolio": {
            "total_value": round(total_value, 2),
            "day_change_value": round(total_change, 2),
            "day_change_pct": round((total_change / total_prev) * 100.0, 2) if total_prev > 0 else 0.0,
        },
        "classes": class_list,
        "fixed_income": {
            "upcoming": upcoming,
            "maturing_30d_total": round(total_30d, 2),
            "maturing_90d_total": round(total_90d, 2),
            "future_count": future_count,
        },
    }


def _normalize_overview_payload(payload):
    if not isinstance(payload, dict):
        return None
    tone = str(payload.get("tom") or "").strip().lower()
    if tone not in _ALLOWED_TONE:
        tone = "neutro"
    raw_highlights = payload.get("destaques")
    highlights = []
    if isinstance(raw_highlights, list):
        for item in raw_highlights:
            text = str(item or "").strip()
            if text:
                highlights.append(text)
    return {
        "resumo": str(payload.get("resumo") or "").strip(),
        "destaques": highlights[:4],
        "alerta_renda_fixa": str(payload.get("alerta_renda_fixa") or "").strip(),
        "tom": tone,
    }


def _has_meaningful_overview(payload):
    if not isinstance(payload, dict):
        return False
    return bool(str(payload.get("resumo") or "").strip() or payload.get("destaques"))


def _build_prompt(facts):
    facts_json = json.dumps(facts, ensure_ascii=False, indent=2)
    return (
        "Voce e um assistente de investimentos. Abaixo estao os FATOS do dia da carteira do "
        "investidor, ja calculados pelo sistema (variacao do dia em BRL e %, variacao por classe "
        "com a maior alta e a maior queda de cada uma, e os proximos vencimentos de renda fixa). "
        "Escreva uma leitura curta do dia em portugues, baseada APENAS nesses fatos. Nao invente "
        "numeros, ativos ou datas.\n\n"
        "FATOS (JSON):\n"
        + facts_json
        + "\n\n"
        "Responda APENAS um JSON valido, sem texto fora do JSON, com as chaves: "
        "resumo (2 a 3 frases sobre o dia da carteira, citando o movimento total), "
        "destaques (lista de ate 4 strings curtas com os movimentos mais relevantes por classe/ativo), "
        "alerta_renda_fixa (string curta sobre vencimentos proximos e valores; vazia se nada vence em ate 90 dias), "
        "tom (positivo|negativo|neutro, conforme o dia). "
        "Seja objetivo e interprete os numeros, nao apenas repita-os."
    )


def get_daily_overview(portfolio_ids):
    """Le a narrativa IA cacheada para o escopo (instantaneo, sem OpenClaw)."""
    scope = _scope_key(portfolio_ids)
    row = get_db().execute(
        "SELECT scope_key, ref_date, payload_json, raw_reply, generated_at "
        "FROM portfolio_daily_overview WHERE scope_key = ?",
        (scope,),
    ).fetchone()
    if not row:
        return None
    payload = None
    payload_json = (row["payload_json"] or "").strip()
    if payload_json:
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = None
    return {
        "scope_key": row["scope_key"],
        "ref_date": row["ref_date"],
        "payload": payload,
        "raw_reply": row["raw_reply"],
        "generated_at": row["generated_at"],
    }


def _upsert_overview(scope, ref_date, payload, raw_reply):
    db = get_db()
    db.execute(
        """
        INSERT INTO portfolio_daily_overview (scope_key, ref_date, payload_json, raw_reply, generated_at)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(scope_key) DO UPDATE SET
            ref_date = excluded.ref_date,
            payload_json = excluded.payload_json,
            raw_reply = excluded.raw_reply,
            generated_at = CURRENT_TIMESTAMP
        """,
        (scope, ref_date, json.dumps(payload or {}, ensure_ascii=False), raw_reply or ""),
    )
    db.commit()


def generate_daily_overview(portfolio_ids):
    """Gera (ou atualiza) a narrativa IA do overview do dia via OpenClaw."""
    scope = _scope_key(portfolio_ids)
    facts = build_daily_overview_facts(portfolio_ids)
    if not facts["classes"] and not facts["fixed_income"]["future_count"]:
        return False, "Carteira vazia: nada para resumir.", None

    prompt = _build_prompt(facts)
    try:
        reply, parsed = openclaw_services.run_structured_openclaw_prompt(
            prompt,
            session_key=openclaw_services._session_key("portfolio-daily-overview"),
            normalizer=_normalize_overview_payload,
            is_meaningful=_has_meaningful_overview,
        )
    except OpenClawError as exc:
        return False, str(exc), None

    if not _has_meaningful_overview(parsed):
        _upsert_overview(scope, facts["ref_date"], parsed or {}, reply)
        if reply.strip():
            return True, "OpenClaw respondeu, mas sem JSON util. Exibindo resposta bruta.", get_daily_overview(portfolio_ids)
        return False, "OpenClaw nao retornou uma leitura utilizavel. Tente novamente em instantes.", None

    _upsert_overview(scope, facts["ref_date"], parsed, reply)
    return True, "OK", get_daily_overview(portfolio_ids)


__all__ = [
    "build_daily_overview_facts",
    "generate_daily_overview",
    "get_daily_overview",
]

"""Proactive finance insights via OpenClaw, grounded in real Pierre data.

Pulls the consolidated monthly overview from the internal pierre-service,
computes the concrete facts and spending anomalies in Python (categories that
jumped, new categories, cash-flow buckets, leftover), and feeds those real
numbers to the model so it writes a grounded monthly narrative + severity-tagged
insights instead of guessing. Cached per reference month (YYYY-MM).
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime

from ..db import get_db
from ..openclaw_client import OpenClawError
from . import openclaw as openclaw_services

_ALLOWED_HEALTH = {"boa", "atencao", "alerta"}
_ALLOWED_SEVERITY = {"alta", "media", "baixa"}

# Anomaly thresholds: a category is "material" if it is at least R$150 or 3% of
# the month's spend, and flagged when it rose >= 30% vs the previous month (or
# is brand new this month).
_MATERIAL_MIN = 150.0
_MATERIAL_FRACTION = 0.03
_DELTA_FLAG_PCT = 30.0


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _service_base_url():
    try:
        from flask import current_app, has_app_context

        if has_app_context():
            return str(current_app.config.get("PIERRE_SERVICE_URL", "http://pierre-service:8000")).rstrip("/")
    except Exception:
        pass
    return (os.getenv("PIERRE_SERVICE_URL") or "http://pierre-service:8000").rstrip("/")


def _service_timeout():
    try:
        from flask import current_app, has_app_context

        if has_app_context():
            return float(current_app.config.get("PIERRE_TIMEOUT_SECONDS", 20))
    except Exception:
        pass
    try:
        return float(os.getenv("PIERRE_TIMEOUT_SECONDS") or 20)
    except (TypeError, ValueError):
        return 20.0


def _fetch_overview(month=None):
    base = _service_base_url()
    query = ""
    if month:
        try:
            year_str, month_str = str(month).split("-")[:2]
            query = f"?year={int(year_str)}&month={int(month_str)}"
        except (ValueError, TypeError):
            query = ""
    url = f"{base}/overview{query}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_service_timeout()) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"pierre-service HTTP {exc.code}") from exc
    except Exception as exc:  # noqa: BLE001 - service unreachable
        raise RuntimeError(f"pierre-service indisponivel: {exc}") from exc
    data = json.loads(raw) if raw else {}
    if not isinstance(data, dict):
        raise RuntimeError("overview do pierre-service em formato inesperado")
    return data


def _norm_str(value):
    return str(value or "").strip()


def _flag_categories(categories, month_total):
    floor = max(_MATERIAL_MIN, float(month_total or 0.0) * _MATERIAL_FRACTION)
    flags = []
    for cat in categories or []:
        if not isinstance(cat, dict):
            continue
        total = float(cat.get("total") or 0.0)
        if total < floor:
            continue
        prev = float(cat.get("prev_total") or 0.0)
        delta = cat.get("delta_pct")
        name = _norm_str(cat.get("category"))
        if bool(cat.get("is_new")):
            flags.append({"categoria": name, "total": round(total, 2), "prev": round(prev, 2), "delta_pct": None, "tipo": "nova"})
        elif isinstance(delta, (int, float)) and float(delta) >= _DELTA_FLAG_PCT:
            flags.append({"categoria": name, "total": round(total, 2), "prev": round(prev, 2), "delta_pct": round(float(delta), 1), "tipo": "alta"})
    flags.sort(key=lambda item: item["total"], reverse=True)
    return flags[:8]


def _build_facts(overview: dict):
    spend = overview.get("spend") or {}
    buckets = overview.get("buckets") or {}
    categories = overview.get("categories") or []
    month_total = float(spend.get("month_total") or 0.0)
    prev_total = float(spend.get("prev_total") or 0.0)
    delta_pct = spend.get("delta_pct")

    lines = [
        f"Mes de referencia: {_norm_str(overview.get('month'))}.",
        f"Gasto total no mes: R$ {month_total:.2f} (mes anterior R$ {prev_total:.2f}"
        + (f", variacao {float(delta_pct):.1f}%" if isinstance(delta_pct, (int, float)) else "")
        + ").",
        "Fluxo de caixa (buckets): "
        + f"receita R$ {float(buckets.get('receita') or 0.0):.2f}; "
        + f"despesas fixas R$ {float(buckets.get('despfixa') or 0.0):.2f}; "
        + f"cartao R$ {float(buckets.get('cartao') or 0.0):.2f}; "
        + f"despesas avulsas R$ {float(buckets.get('despavulsa') or 0.0):.2f}; "
        + f"sobra R$ {float(buckets.get('sobra') or 0.0):.2f}.",
    ]

    top = spend.get("top_category")
    if isinstance(top, dict) and _norm_str(top.get("category")):
        lines.append(
            f"Maior categoria de gasto: {_norm_str(top.get('category'))} "
            f"(R$ {float(top.get('total') or 0.0):.2f})."
        )

    flags = _flag_categories(categories, month_total)
    if flags:
        flag_lines = []
        for flag in flags:
            if flag["tipo"] == "nova":
                flag_lines.append(f"- {flag['categoria']}: categoria NOVA neste mes, R$ {flag['total']:.2f}")
            else:
                flag_lines.append(
                    f"- {flag['categoria']}: subiu {flag['delta_pct']:.1f}% "
                    f"(de R$ {flag['prev']:.2f} para R$ {flag['total']:.2f})"
                )
        lines.append("Categorias sinalizadas (variacao relevante):\n" + "\n".join(flag_lines))
    else:
        lines.append("Nenhuma categoria com variacao relevante detectada pelo sistema neste mes.")

    return "\n".join(lines), flags


def _build_prompt(facts: str) -> str:
    return (
        "Voce e um assistente financeiro pessoal. Gere insights do mes com base APENAS nos "
        "dados reais abaixo. Nao invente valores nem categorias que nao estejam listadas.\n\n"
        "DADOS FINANCEIROS DO MES:\n"
        + facts
        + "\n\n"
        "Responda APENAS um JSON valido, sem texto fora do JSON, com as chaves: "
        "resumo (narrativa curta e util do mes), "
        "saude (use boa, atencao ou alerta conforme a sobra e o ritmo de gastos), "
        "insights (lista de objetos com titulo, categoria, severidade [alta|media|baixa], mensagem, "
        "valor [numero em reais], delta_pct [numero ou null]), "
        "sugestoes (lista de strings com acoes praticas). "
        "Priorize as categorias sinalizadas como alta ou nova. Interprete os numeros de forma objetiva."
    )


def _norm_number(value):
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _normalize_insights_payload(payload):
    if not isinstance(payload, dict):
        return None
    health = _norm_str(payload.get("saude")).lower()
    if health not in _ALLOWED_HEALTH:
        health = "atencao"
    insights = []
    for raw in payload.get("insights") or []:
        if not isinstance(raw, dict):
            continue
        severity = _norm_str(raw.get("severidade")).lower()
        if severity not in _ALLOWED_SEVERITY:
            severity = "media"
        insights.append(
            {
                "titulo": _norm_str(raw.get("titulo")),
                "categoria": _norm_str(raw.get("categoria")),
                "severidade": severity,
                "mensagem": _norm_str(raw.get("mensagem")),
                "valor": _norm_number(raw.get("valor")),
                "delta_pct": _norm_number(raw.get("delta_pct")),
            }
        )
    suggestions = []
    for item in payload.get("sugestoes") or []:
        text = _norm_str(item)
        if text:
            suggestions.append(text)
    return {
        "resumo": _norm_str(payload.get("resumo")),
        "saude": health,
        "insights": insights,
        "sugestoes": suggestions,
    }


def _has_meaningful_insights(payload):
    if not isinstance(payload, dict):
        return False
    if _norm_str(payload.get("resumo")):
        return True
    if payload.get("insights"):
        return True
    if payload.get("sugestoes"):
        return True
    return False


def get_finance_insights(month=None):
    """Read cached insights for a month (or the most recent one). No OpenClaw."""
    db = get_db()
    if month:
        row = db.execute(
            "SELECT month, payload_json, raw_reply, generated_at FROM finance_insights WHERE month = ?",
            (str(month).strip(),),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT month, payload_json, raw_reply, generated_at FROM finance_insights "
            "ORDER BY month DESC LIMIT 1"
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
        "month": row["month"],
        "payload": payload,
        "raw_reply": row["raw_reply"],
        "generated_at": row["generated_at"],
    }


def _upsert_insights(month, payload, raw_reply):
    db = get_db()
    db.execute(
        """
        INSERT INTO finance_insights (month, payload_json, raw_reply, generated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(month) DO UPDATE SET
            payload_json = excluded.payload_json,
            raw_reply = excluded.raw_reply,
            generated_at = CURRENT_TIMESTAMP
        """,
        (str(month).strip(), json.dumps(payload or {}, ensure_ascii=False), raw_reply or ""),
    )
    db.commit()


def generate_finance_insights(month=None):
    """Generate (or refresh) the monthly finance insights via OpenClaw."""
    try:
        overview = _fetch_overview(month)
    except RuntimeError as exc:
        return False, str(exc), None

    month_key = _norm_str(overview.get("month")) or _norm_str(month)
    if not month_key:
        month_key = datetime.utcnow().strftime("%Y-%m")

    facts, _flags = _build_facts(overview)
    prompt = _build_prompt(facts)

    try:
        reply, parsed = openclaw_services.run_structured_openclaw_prompt(
            prompt,
            session_key=openclaw_services._session_key("financas-insights"),
            normalizer=_normalize_insights_payload,
            is_meaningful=_has_meaningful_insights,
        )
    except OpenClawError as exc:
        return False, str(exc), None

    if not _has_meaningful_insights(parsed):
        _upsert_insights(month_key, parsed or {}, reply)
        if reply.strip():
            return True, "OpenClaw respondeu, mas sem JSON util. Exibindo resposta bruta.", get_finance_insights(month_key)
        return False, "OpenClaw nao retornou insights utilizaveis. Tente novamente em instantes.", None

    _upsert_insights(month_key, parsed, reply)
    return True, "OK", get_finance_insights(month_key)


__all__ = [
    "generate_finance_insights",
    "get_finance_insights",
]

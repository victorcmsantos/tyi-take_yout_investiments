"""Whole-portfolio AI analysis via OpenClaw.

Builds a single structured read of the entire portfolio (overall stance,
concentration risks, diversification gaps and concrete rebalancing suggestions)
grounded in the REAL numbers the system already computes in
``get_portfolio_snapshot`` — weights, P&L, class breakdown and the rule-based
tactical summary — plus any per-asset OpenClaw signals already stored. The model
is told to use only those facts, so it interprets real data instead of inventing
positions or numbers.

Results are cached per selected-portfolio scope in the ``portfolio_analysis``
table; reads are instant, generation happens behind an explicit POST.
"""

import json
from datetime import datetime

from ..db import get_db
from ..openclaw_client import OpenClawError
from . import _legacy as legacy
from . import openclaw as openclaw_services

_ALLOWED_STANCE = {"defensivo", "neutro", "construtivo"}
_ALLOWED_ACTION = {"aumentar", "reduzir", "manter", "revisar"}
_ALLOWED_PRIORITY = {"alta", "media", "baixa"}

_CATEGORY_LABELS = {
    "br_stocks": "Acoes BR",
    "us_stocks": "Acoes US",
    "crypto": "Cripto",
    "fiis": "FIIs",
    "fixed_income": "Renda Fixa",
}


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _scope_key(portfolio_ids):
    ids = []
    for item in portfolio_ids or []:
        text = str(item).strip()
        if text:
            ids.append(text)
    ids = sorted(set(ids), key=lambda value: (len(value), value))
    return ",".join(ids) if ids else "all"


def _norm_str(value):
    return str(value or "").strip()


def _norm_list(value):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = _norm_str(item)
        if text:
            out.append(text)
    return out


def _normalize_analysis_payload(payload):
    if not isinstance(payload, dict):
        return None
    stance = _norm_str(payload.get("postura_geral")).lower()
    if stance not in _ALLOWED_STANCE:
        stance = "neutro"
    suggestions = []
    for raw in payload.get("sugestoes") or []:
        if not isinstance(raw, dict):
            continue
        action = _norm_str(raw.get("acao")).lower()
        if action not in _ALLOWED_ACTION:
            action = "revisar"
        priority = _norm_str(raw.get("prioridade")).lower()
        if priority not in _ALLOWED_PRIORITY:
            priority = "media"
        suggestions.append(
            {
                "titulo": _norm_str(raw.get("titulo")),
                "acao": action,
                "alvo": _norm_str(raw.get("alvo")),
                "motivo": _norm_str(raw.get("motivo")),
                "prioridade": priority,
            }
        )
    return {
        "postura_geral": stance,
        "resumo": _norm_str(payload.get("resumo")),
        "riscos_concentracao": _norm_list(payload.get("riscos_concentracao")),
        "diversificacao": _norm_str(payload.get("diversificacao")),
        "sugestoes": suggestions,
        "observacoes": _norm_str(payload.get("observacoes")),
    }


def _has_meaningful_analysis(payload):
    if not isinstance(payload, dict):
        return False
    if _norm_str(payload.get("resumo")):
        return True
    if payload.get("sugestoes"):
        return True
    if payload.get("riscos_concentracao"):
        return True
    if _norm_str(payload.get("diversificacao")):
        return True
    return False


def _signal_for(enrichment):
    if not isinstance(enrichment, dict):
        return "", ""
    payload = enrichment.get("payload")
    if not isinstance(payload, dict):
        return "", ""
    return (
        _norm_str(payload.get("humor_do_mercado")),
        _norm_str(payload.get("acao_sugerida")),
    )


def _build_facts(snapshot: dict, enrichments: dict) -> str:
    total_value = float(snapshot.get("total_value") or 0.0)
    open_pnl_pct = float(snapshot.get("open_pnl_pct") or 0.0)
    positions = snapshot.get("positions") or []
    group_summaries = snapshot.get("group_summaries") or {}
    tactical = snapshot.get("tactical_summary") or {}

    lines = [
        f"Total da carteira: R$ {total_value:.2f}.",
        f"Resultado aberto (P&L): {open_pnl_pct:.2f}%.",
    ]

    # Class breakdown (real weights).
    class_parts = []
    for key, summary in group_summaries.items():
        class_value = float((summary or {}).get("total_value") or 0.0)
        weight = (class_value / total_value * 100.0) if total_value else 0.0
        class_pnl = float((summary or {}).get("open_pnl_pct") or 0.0)
        class_parts.append(
            f"{_CATEGORY_LABELS.get(key, key)} {weight:.1f}% (P&L {class_pnl:.1f}%)"
        )
    if class_parts:
        lines.append("Pesos por classe: " + "; ".join(class_parts) + ".")

    # Top positions by weight, with stored OpenClaw signal when available.
    top = sorted(positions, key=lambda item: float(item.get("weight") or 0.0), reverse=True)[:15]
    pos_lines = []
    for item in top:
        ticker = _norm_str(item.get("ticker"))
        mood, action = _signal_for(enrichments.get(ticker))
        signal = ""
        if mood or action:
            signal = f", sinal OpenClaw: humor={mood or 'n/a'}/acao={action or 'n/a'}"
        pos_lines.append(
            f"- {ticker} ({_CATEGORY_LABELS.get(item.get('category'), item.get('category') or '')}): "
            f"peso {float(item.get('weight') or 0.0):.2f}%, "
            f"P&L {float(item.get('open_pnl_pct') or 0.0):.2f}%{signal}"
        )
    if pos_lines:
        lines.append("Principais posicoes por peso:\n" + "\n".join(pos_lines))

    # Concentration alerts already computed by the rule-based tactical summary.
    alerts = (tactical.get("concentration_alerts") or [])
    if alerts:
        alert_lines = [f"- {_norm_str(a.get('label'))}: {_norm_str(a.get('detail'))}" for a in alerts]
        lines.append("Alertas de concentracao (regras do sistema):\n" + "\n".join(alert_lines))

    return "\n".join(lines)


def _build_prompt(facts: str) -> str:
    return (
        "Voce e um analista de carteira de investimentos. Analise a carteira do investidor "
        "com base APENAS nos dados reais abaixo. Nao invente numeros, precos ou ativos que nao "
        "estejam listados.\n\n"
        "DADOS DA CARTEIRA:\n"
        + facts
        + "\n\n"
        "Responda APENAS um JSON valido, sem texto fora do JSON, com as chaves: "
        "postura_geral (use defensivo, neutro ou construtivo), "
        "resumo (string curta com a leitura geral), "
        "riscos_concentracao (lista de strings apontando concentracoes relevantes), "
        "diversificacao (string com lacunas ou observacoes de diversificacao por classe), "
        "sugestoes (lista de 2 a 4 objetos, cada um com titulo, acao [aumentar|reduzir|manter|revisar], "
        "alvo [ticker ou classe existente], motivo, prioridade [alta|media|baixa]), "
        "observacoes (string). "
        "As sugestoes devem ser coerentes com os pesos e P&L reais informados. "
        "Seja objetivo e interprete os numeros, nao apenas repita-os."
    )


def get_portfolio_analysis(portfolio_ids):
    """Read the cached AI analysis for a portfolio scope (instant, no OpenClaw)."""
    scope = _scope_key(portfolio_ids)
    db = get_db()
    row = db.execute(
        "SELECT scope_key, payload_json, raw_reply, total_value, positions_count, generated_at "
        "FROM portfolio_analysis WHERE scope_key = ?",
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
        "payload": payload,
        "raw_reply": row["raw_reply"],
        "total_value": float(row["total_value"] or 0.0),
        "positions_count": int(row["positions_count"] or 0),
        "generated_at": row["generated_at"],
    }


def _upsert_analysis(scope, payload, raw_reply, total_value, positions_count):
    db = get_db()
    db.execute(
        """
        INSERT INTO portfolio_analysis (scope_key, payload_json, raw_reply, total_value, positions_count, generated_at)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(scope_key) DO UPDATE SET
            payload_json = excluded.payload_json,
            raw_reply = excluded.raw_reply,
            total_value = excluded.total_value,
            positions_count = excluded.positions_count,
            generated_at = CURRENT_TIMESTAMP
        """,
        (scope, json.dumps(payload or {}, ensure_ascii=False), raw_reply or "", float(total_value or 0.0), int(positions_count or 0)),
    )
    db.commit()


def analyze_portfolio_with_openclaw(portfolio_ids):
    """Generate (or refresh) the whole-portfolio AI analysis via OpenClaw."""
    scope = _scope_key(portfolio_ids)
    snapshot = legacy.get_portfolio_snapshot(portfolio_ids, sort_by="value", sort_dir="desc")
    positions = (snapshot or {}).get("positions") or []
    if not positions:
        return False, "Carteira vazia: nada para analisar.", None

    tickers = [item.get("ticker") for item in positions]
    enrichments = legacy.get_asset_enrichments_map(tickers) or {}
    facts = _build_facts(snapshot, enrichments)
    prompt = _build_prompt(facts)

    try:
        reply, parsed = openclaw_services.run_structured_openclaw_prompt(
            prompt,
            session_key=openclaw_services._session_key("portfolio-analysis"),
            normalizer=_normalize_analysis_payload,
            is_meaningful=_has_meaningful_analysis,
        )
    except OpenClawError as exc:
        return False, str(exc), None

    total_value = float((snapshot or {}).get("total_value") or 0.0)
    positions_count = len(positions)

    if not _has_meaningful_analysis(parsed):
        # Store the raw reply so the UI can surface something instead of nothing.
        _upsert_analysis(scope, parsed or {}, reply, total_value, positions_count)
        if reply.strip():
            return True, "OpenClaw respondeu, mas sem JSON util. Exibindo resposta bruta.", get_portfolio_analysis(portfolio_ids)
        return False, "OpenClaw nao retornou uma analise utilizavel. Tente novamente em instantes.", None

    _upsert_analysis(scope, parsed, reply, total_value, positions_count)
    return True, "OK", get_portfolio_analysis(portfolio_ids)


__all__ = [
    "analyze_portfolio_with_openclaw",
    "get_portfolio_analysis",
]

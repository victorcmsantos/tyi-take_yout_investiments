"""OpenClaw enrichment services."""

import json
from datetime import datetime

from ..db import get_db
from ..openclaw_client import OpenClawError, invoke_tool
from . import _legacy as legacy


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def get_asset_enrichment(ticker: str):
    if not ticker:
        return None
    db = get_db()
    row = db.execute(
        "SELECT ticker, payload_json, raw_reply, updated_at FROM asset_enrichments WHERE ticker = ?",
        (ticker.upper(),),
    ).fetchone()
    if not row:
        return None
    payload_json = (row["payload_json"] or "").strip()
    payload = None
    if payload_json:
        try:
            payload = json.loads(payload_json)
        except Exception:
            payload = None
    return {
        "ticker": row["ticker"],
        "payload": payload,
        "raw_reply": row["raw_reply"],
        "updated_at": row["updated_at"],
    }


def get_asset_enrichment_history(ticker: str, limit: int = 12):
    if not ticker:
        return []
    try:
        history_limit = max(1, min(int(limit), 50))
    except (TypeError, ValueError):
        history_limit = 12
    db = get_db()
    rows = db.execute(
        """
        SELECT id, ticker, payload_json, raw_reply, price_at_update, mood, suggested_action, created_at
        FROM asset_enrichment_history
        WHERE ticker = ?
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (ticker.upper(), history_limit),
    ).fetchall()
    items = []
    for row in rows:
        payload_json = (row["payload_json"] or "").strip()
        payload = None
        if payload_json:
            try:
                payload = json.loads(payload_json)
            except Exception:
                payload = None
        items.append(
            {
                "id": row["id"],
                "ticker": row["ticker"],
                "payload": payload,
                "raw_reply": row["raw_reply"],
                "price_at_update": float(row["price_at_update"] or 0.0),
                "mood": str(row["mood"] or "").strip(),
                "suggested_action": str(row["suggested_action"] or "").strip(),
                "created_at": row["created_at"],
            }
        )
    return items


def _normalize_enrichment_list(value):
    if not isinstance(value, list):
        return []
    items = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _extract_json_from_text(text: str):
    if not text:
        return None
    raw = text.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    candidate = raw[start : end + 1]
    try:
        return json.loads(candidate)
    except Exception:
        repaired = []
        in_string = False
        escaping = False
        for char in candidate:
            if in_string and char in {"\n", "\r"}:
                repaired.append(" ")
                continue
            repaired.append(char)
            if escaping:
                escaping = False
                continue
            if char == "\\":
                escaping = True
                continue
            if char == '"':
                in_string = not in_string
        try:
            return json.loads("".join(repaired))
        except Exception:
            return None


def _normalize_asset_enrichment_payload(payload):
    if not isinstance(payload, dict):
        return None
    return {
        "resumo": str(payload.get("resumo") or "").strip(),
        "modelo_de_negocio": str(payload.get("modelo_de_negocio") or "").strip(),
        "tese": _normalize_enrichment_list(payload.get("tese")),
        "riscos": _normalize_enrichment_list(payload.get("riscos")),
        "dividendos": str(payload.get("dividendos") or "").strip(),
        "visao_do_mercado": str(payload.get("visao_do_mercado") or "").strip(),
        "humor_do_mercado": str(payload.get("humor_do_mercado") or "").strip(),
        "acao_sugerida": str(payload.get("acao_sugerida") or "").strip(),
        "justificativa_da_acao": str(payload.get("justificativa_da_acao") or "").strip(),
        "observacoes": str(payload.get("observacoes") or "").strip(),
    }


def _extract_openclaw_reply(result):
    if not isinstance(result, dict):
        return ""

    direct_reply = str(result.get("reply") or "").strip()
    if direct_reply:
        return direct_reply

    details = result.get("details")
    if isinstance(details, dict):
        details_reply = str(details.get("reply") or "").strip()
        if details_reply:
            return details_reply

    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            parsed = _extract_json_from_text(text)
            if isinstance(parsed, dict):
                nested_reply = str(parsed.get("reply") or "").strip()
                if nested_reply:
                    return nested_reply
            return text

    return ""


def _asset_prompt_profile(ticker: str, name: str, sector: str):
    ticker_up = (ticker or "").strip().upper()
    name_up = (name or "").strip().upper()
    sector_up = (sector or "").strip().upper()
    category = legacy._position_category(ticker_up, name_up, sector_up)
    is_probable_etf = any(
        marker in name_up or marker in sector_up
        for marker in ("ETF", "INDEX", "TRUST", "FUND")
    )
    is_probable_reit = any(
        marker in name_up or marker in sector_up
        for marker in ("REIT", "REAL ESTATE", "REALTY", "PROPERTIES")
    )

    if category == "crypto":
        return "crypto", (
            "Foque em utilidade do token, ecossistema, adocao, narrativa e principais riscos de execucao/regulacao. "
            "Nao trate como empresa operacional."
        )

    if category == "fiis":
        return "fiis", (
            "Foque em tipo de fundo, qualidade dos ativos/imoveis, perfil de contratos, vacancia, alavancagem e previsibilidade de rendimentos. "
            "Se parecer ETF/fundo de indice, ajuste a resposta para estrategia e indice de referencia."
        )

    if (
        "BANCO" in name_up
        or "BANK" in name_up
        or sector_up in {"FINANCEIRO", "FINANCIAL SERVICES", "BANCOS", "BANKS"}
    ):
        return "banks", (
            "Foque em credito, margem financeira, inadimplencia, eficiencia, diversificacao de receitas, competicao com fintechs e regulacao."
        )

    if category == "us_stocks":
        if is_probable_etf:
            return "us_etf", (
                "Trate como ETF/fundo de indice. Foque em indice de referencia, concentracao setorial, exposicao geografica, custo, liquidez e riscos de composicao/valuation do indice."
            )

        if is_probable_reit:
            return "reit", (
                "Trate como REIT. Foque em tipo de ativo, perfil dos contratos, ocupacao, custo de capital, sensibilidade a juros e sustentabilidade dos dividendos."
            )

        return "us_stocks", (
            "Foque em modelo de negocio, vantagem competitiva, qualidade da receita, disciplina de capital e riscos de setor/valuation."
        )

    return "br_stocks", (
        "Foque em modelo de negocio, execucao, ciclo setorial, qualidade da receita, alocacao de capital e riscos macro/regulatorios."
    )


def _build_asset_enrichment_prompt(asset: dict):
    ticker = (asset.get("ticker") or "").strip().upper()
    name = str(asset.get("name") or "").strip()
    sector = str(asset.get("sector") or "").strip()
    current_price = float(asset.get("price") or 0.0)
    profile_key, guidance = _asset_prompt_profile(ticker, name, sector)
    preferred_sources = (
        "Para visao_do_mercado e humor_do_mercado, quando houver contexto recente disponivel, "
        "priorize sinais vindos de NewsAPI, InfoMoney, Money Times, The Verge, Investidor10, FundosExplorer, FIIS.com e fontes equivalentes confiaveis do mesmo nicho. "
        "Para FIIs, de peso maior a Investidor10, FundosExplorer e FIIS.com. "
        "Para tech/acoes US, The Verge pode complementar o contexto setorial. "
        "Se nao houver contexto recente confiavel dessas fontes, seja conservador e nao invente manchetes, fatos ou numeros."
    )
    return (
        f"Ticker: {ticker}. "
        f"Nome: {name}. "
        f"Setor: {sector}. "
        f"Preco atual aproximado: {current_price:.2f}. "
        f"Contexto: {profile_key}. "
        "Responda APENAS JSON valido com as chaves resumo, modelo_de_negocio, tese, riscos, dividendos, visao_do_mercado, humor_do_mercado, acao_sugerida, justificativa_da_acao, observacoes. "
        "Use humor_do_mercado como positivo, neutro ou cauteloso. "
        "Use acao_sugerida como comprar_mais, segurar, reduzir ou observar. "
        "Se nao souber, use string vazia ou lista vazia. "
        "Seja conciso, objetivo e nao invente numeros precisos. "
        + preferred_sources
        + " "
        + guidance
    )


def _build_asset_enrichment_retry_prompt(asset: dict):
    ticker = (asset.get("ticker") or "").strip().upper()
    name = str(asset.get("name") or "").strip()
    sector = str(asset.get("sector") or "").strip()
    current_price = float(asset.get("price") or 0.0)
    profile_key, guidance = _asset_prompt_profile(ticker, name, sector)
    preferred_sources = (
        "Se houver contexto recente disponivel, priorize referencias de NewsAPI, InfoMoney, Money Times, The Verge, Investidor10, FundosExplorer e FIIS.com para montar visao_do_mercado e humor_do_mercado. "
        "Nao invente noticias, chamadas ou fatos se essas fontes nao estiverem acessiveis no contexto."
    )
    return (
        f"Ativo {ticker} ({name}). "
        f"Setor {sector}. "
        f"Preco atual aproximado {current_price:.2f}. "
        f"Contexto {profile_key}. "
        "Retorne SOMENTE um JSON valido com resumo, modelo_de_negocio, tese, riscos, dividendos, visao_do_mercado, humor_do_mercado, acao_sugerida, justificativa_da_acao, observacoes. "
        "humor_do_mercado deve ser positivo, neutro ou cauteloso. "
        "acao_sugerida deve ser comprar_mais, segurar, reduzir ou observar. "
        "Preencha todas as chaves. "
        "Se faltar confianca, use texto curto, sem numeros precisos. "
        + preferred_sources
        + " "
        + guidance
    )


def _has_meaningful_enrichment_payload(payload):
    if not isinstance(payload, dict):
        return False
    if str(payload.get("resumo") or "").strip():
        return True
    if str(payload.get("modelo_de_negocio") or "").strip():
        return True
    if str(payload.get("dividendos") or "").strip():
        return True
    if str(payload.get("visao_do_mercado") or "").strip():
        return True
    if str(payload.get("humor_do_mercado") or "").strip():
        return True
    if str(payload.get("acao_sugerida") or "").strip():
        return True
    if str(payload.get("justificativa_da_acao") or "").strip():
        return True
    if str(payload.get("observacoes") or "").strip():
        return True
    if _normalize_enrichment_list(payload.get("tese")):
        return True
    if _normalize_enrichment_list(payload.get("riscos")):
        return True
    return False


def _is_transient_openclaw_reply(reply):
    text = str(reply or "").strip().lower()
    if not text:
        return False

    transient_markers = (
        "aguarde",
        "um momento",
        "enquanto busco",
        "estou buscando",
        "buscando essas informacoes",
        "busco essas informacoes",
        "buscando essas informações",
        "busco essas informações",
        "ja volto",
        "processando",
    )
    return any(marker in text for marker in transient_markers)


def _invoke_openclaw_asset_prompt(prompt: str):
    result = invoke_tool(
        "sessions_send",
        {
            "sessionKey": "main",
            "message": prompt,
            "timeoutSeconds": 120,
        },
        timeout_seconds=150,
    )
    if not isinstance(result, dict):
        return "", None
    reply = _extract_openclaw_reply(result)
    parsed = _normalize_asset_enrichment_payload(_extract_json_from_text(reply))
    return reply, parsed


def upsert_asset_enrichment(ticker: str, payload: dict | None, raw_reply: str, price_at_update: float = 0.0):
    if not ticker:
        return False
    db = get_db()
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    normalized_price = 0.0
    try:
        normalized_price = float(price_at_update or 0.0)
    except (TypeError, ValueError):
        normalized_price = 0.0
    mood = str((payload or {}).get("humor_do_mercado") or "").strip()
    suggested_action = str((payload or {}).get("acao_sugerida") or "").strip()
    db.execute(
        """
        INSERT INTO asset_enrichments (ticker, payload_json, raw_reply, updated_at)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(ticker) DO UPDATE SET
            payload_json = excluded.payload_json,
            raw_reply = excluded.raw_reply,
            updated_at = CURRENT_TIMESTAMP
        """,
        (ticker.upper(), payload_json, (raw_reply or "")),
    )
    if _has_meaningful_enrichment_payload(payload or {}) or str(raw_reply or "").strip():
        db.execute(
            """
            INSERT INTO asset_enrichment_history (
                ticker, payload_json, raw_reply, price_at_update, mood, suggested_action, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                ticker.upper(),
                payload_json,
                (raw_reply or ""),
                normalized_price,
                mood,
                suggested_action,
            ),
        )
    db.commit()
    return True


def enrich_asset_with_openclaw(ticker: str):
    ticker_norm = (ticker or "").strip().upper()
    if not ticker_norm:
        return False, "Ticker e obrigatorio.", None

    asset = legacy.get_asset(ticker_norm)
    if not asset:
        return False, "Ativo nao encontrado.", None
    asset_price = float(asset.get("price") or 0.0)

    try:
        reply, parsed = _invoke_openclaw_asset_prompt(_build_asset_enrichment_prompt(asset))
    except OpenClawError as exc:
        return False, str(exc), None

    retried = False
    if _is_transient_openclaw_reply(reply) or not _has_meaningful_enrichment_payload(parsed):
        try:
            retry_reply, retry_parsed = _invoke_openclaw_asset_prompt(
                _build_asset_enrichment_retry_prompt(asset)
            )
        except OpenClawError as exc:
            retry_reply = ""
            retry_parsed = None
            stored_reply = ""
            if not _is_transient_openclaw_reply(reply):
                stored_reply = reply or str(exc)
            upsert_asset_enrichment(ticker_norm, parsed or {}, stored_reply, asset_price)
            if _has_meaningful_enrichment_payload(parsed):
                return True, "OK", get_asset_enrichment(ticker_norm)
            return False, str(exc), None

        if _has_meaningful_enrichment_payload(retry_parsed):
            reply = retry_reply
            parsed = retry_parsed
        elif retry_reply and not _is_transient_openclaw_reply(retry_reply):
            reply = retry_reply
            parsed = retry_parsed
        retried = True

    if _is_transient_openclaw_reply(reply):
        upsert_asset_enrichment(ticker_norm, parsed or {}, "", asset_price)
        return False, "OpenClaw ainda nao retornou o conteudo final. Tente novamente em instantes.", None

    if not parsed:
        upsert_asset_enrichment(ticker_norm, {}, reply, asset_price)
        return (
            True,
            "OpenClaw respondeu, mas nao retornou JSON valido. Exibindo resposta bruta.",
            get_asset_enrichment(ticker_norm),
        )

    upsert_asset_enrichment(ticker_norm, parsed, reply, asset_price)
    if _has_meaningful_enrichment_payload(parsed):
        return True, ("OK (apos retry)" if retried else "OK"), get_asset_enrichment(ticker_norm)
    return (
        True,
        "OpenClaw respondeu, mas sem conteudo util. Exibindo resposta bruta.",
        get_asset_enrichment(ticker_norm),
    )


def enrich_assets_with_openclaw_batch(tickers=None, only_missing=True, limit=None):
    db = get_db()
    normalized_tickers = []
    if tickers:
        seen = set()
        for item in tickers:
            ticker = str(item or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen.add(ticker)
            normalized_tickers.append(ticker)

    if normalized_tickers:
        placeholders = ",".join("?" for _ in normalized_tickers)
        rows = db.execute(
            f"""
            SELECT ticker
            FROM assets
            WHERE ticker IN ({placeholders})
            ORDER BY ticker ASC
            """,
            normalized_tickers,
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT ticker
            FROM assets
            ORDER BY market_cap_bi DESC, ticker ASC
            """
        ).fetchall()

    queue = []
    skipped = []
    for row in rows:
        ticker = str(row["ticker"] or "").strip().upper()
        if not ticker:
            continue
        enrichment = get_asset_enrichment(ticker)
        has_enrichment = bool(
            enrichment
            and (
                enrichment.get("raw_reply")
                or (isinstance(enrichment.get("payload"), dict) and enrichment.get("payload"))
            )
        )
        if only_missing and has_enrichment:
            skipped.append({"ticker": ticker, "reason": "ja_enriquecido"})
            continue
        queue.append(ticker)

    if isinstance(limit, int) and limit > 0:
        queue = queue[:limit]

    results = []
    started_at = _now_iso()
    for ticker in queue:
        ok, message, enrichment = enrich_asset_with_openclaw(ticker)
        results.append(
            {
                "ticker": ticker,
                "ok": bool(ok),
                "message": str(message or ""),
                "updated_at": (enrichment or {}).get("updated_at") if isinstance(enrichment, dict) else None,
            }
        )

    success_count = sum(1 for item in results if item["ok"])
    failure_count = len(results) - success_count
    return {
        "started_at": started_at,
        "finished_at": _now_iso(),
        "only_missing": bool(only_missing),
        "requested_limit": limit if isinstance(limit, int) and limit > 0 else None,
        "processed_count": len(results),
        "success_count": success_count,
        "failure_count": failure_count,
        "skipped_count": len(skipped),
        "results": results,
        "skipped": skipped,
    }


__all__ = [
    "enrich_asset_with_openclaw",
    "enrich_assets_with_openclaw_batch",
    "get_asset_enrichment",
    "get_asset_enrichment_history",
    "upsert_asset_enrichment",
]

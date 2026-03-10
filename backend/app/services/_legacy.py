import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from datetime import datetime, timedelta
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import current_app, has_request_context

from ..auth import get_current_user
from ..db import get_db

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

_FX_CACHE = {"usdbrl": None, "expires_at": 0.0}
_BCB_SERIES_CACHE = {}
_COINGECKO_CACHE = {}
_COINGECKO_CIRCUIT = {"until": 0.0, "status_code": None}
_TWELVE_DATA_CACHE = {}
_ALPHA_VANTAGE_CACHE = {}
_YAHOO_MONTHLY_CACHE = {}
_ASSET_PRICE_HISTORY_CACHE = {}
_BENCHMARK_CACHE = {}
_MARKET_SCANNER_CACHE = {}
_LOGGER = logging.getLogger(__name__)
_BRAPI_DIAG = {
    "missing_token_logged": False,
    "empty_payload_tickers": set(),
    "empty_results_tickers": set(),
}
_BRAPI_BATCH_LIMIT = 10
_BRAPI_QUOTE_CACHE_TTL_DEFAULT_SECONDS = 120.0
_BRAPI_QUOTE_RESULT_CACHE = {}
_BRAPI_CIRCUIT = {"until": 0.0, "status_code": None}
_MARKET_DATA_PROVIDER_CAPABILITIES = {
    "alpha_vantage": {"metrics", "profile", "history"},
    "brapi": {"metrics", "profile", "history"},
    "coingecko": {"metrics", "profile", "history"},
    "market_scanner": {"metrics", "profile", "history"},
    "twelve_data": {"metrics", "history"},
    "google": {"metrics"},
    "yahoo": {"metrics", "profile", "history"},
}
_METRIC_FORMULA_FIELDS = (
    "price",
    "dy",
    "pl",
    "pvp",
    "variation_day",
    "variation_7d",
    "variation_30d",
    "market_cap_bi",
)
_METRIC_FORMULA_CATALOG = {
    "price": {
        "title": "Preco",
        "description": "Preco atual do ativo.",
        "formula": "value",
    },
    "dy": {
        "title": "Dividend Yield",
        "description": "DY anual em percentual.",
        "formula": "value",
    },
    "pl": {
        "title": "P/L",
        "description": "Preco sobre lucro.",
        "formula": "value",
    },
    "pvp": {
        "title": "P/VP",
        "description": "Preco sobre valor patrimonial.",
        "formula": "value",
    },
    "variation_day": {
        "title": "Variacao no dia",
        "description": "Variacao percentual no dia.",
        "formula": "value",
    },
    "variation_7d": {
        "title": "Variacao 7d",
        "description": "Variacao percentual em 7 dias.",
        "formula": "value",
    },
    "variation_30d": {
        "title": "Variacao 30d",
        "description": "Variacao percentual em 30 dias.",
        "formula": "value",
    },
    "market_cap_bi": {
        "title": "Valor de mercado (bi)",
        "description": "Market cap em bilhoes de reais.",
        "formula": "value",
    },
}
_METRIC_FORMULA_ALLOWED_FUNCS = {
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
}


def _get_app_logger():
    logger = _LOGGER
    try:
        logger = current_app.logger
    except Exception:
        pass
    return logger


def _should_log_market_sources():
    return _is_truthy_env("MARKET_DATA_LOG_SOURCES", "0")


def _row_to_dict(row):
    return dict(row) if row else None


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _snapshot_now():
    return datetime.now().isoformat(timespec="seconds")


def _snapshot_age_seconds(iso_text: str):
    try:
        created = datetime.fromisoformat((iso_text or "").strip())
    except (TypeError, ValueError):
        return None
    return max((datetime.now() - created).total_seconds(), 0.0)


def _normalize_metric_formula_value(value, fallback=0.0):
    numeric = _to_number(value)
    if numeric is None:
        return float(_to_number(fallback) or 0.0)
    return float(numeric)


def _metric_formula_context(values: dict):
    context = {}
    for field in _METRIC_FORMULA_FIELDS:
        context[field] = _normalize_metric_formula_value((values or {}).get(field), fallback=0.0)
    return context


def _upsert_asset_metric_baseline(db, ticker: str, values: dict, updated_at: str | None = None):
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return
    payload = _metric_formula_context(values)
    db.execute(
        """
        INSERT INTO asset_metric_baselines (
            ticker, price, dy, pl, pvp, variation_day, variation_7d, variation_30d, market_cap_bi, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            price = excluded.price,
            dy = excluded.dy,
            pl = excluded.pl,
            pvp = excluded.pvp,
            variation_day = excluded.variation_day,
            variation_7d = excluded.variation_7d,
            variation_30d = excluded.variation_30d,
            market_cap_bi = excluded.market_cap_bi,
            updated_at = excluded.updated_at
        """,
        (
            normalized_ticker,
            payload["price"],
            payload["dy"],
            payload["pl"],
            payload["pvp"],
            payload["variation_day"],
            payload["variation_7d"],
            payload["variation_30d"],
            payload["market_cap_bi"],
            updated_at or _now_iso(),
        ),
    )
def get_metric_formulas_catalog():
    from . import scanner as scanner_services

    return scanner_services.get_metric_formulas_catalog()


def recalculate_metric_formulas_for_all_assets():
    from . import scanner as scanner_services

    return scanner_services.recalculate_metric_formulas_for_all_assets()


def update_metric_formula(metric_key: str, formula: str):
    from . import scanner as scanner_services

    return scanner_services.update_metric_formula(metric_key, formula)


def _market_data_stale_after_seconds():
    raw = os.getenv("MARKET_DATA_STALE_AFTER_SECONDS") or "43200"
    try:
        return max(int(raw), 60)
    except (TypeError, ValueError):
        return 43200


def _parse_iso_datetime(value):
    raw = (value or "").strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        try:
            return parsed.astimezone().replace(tzinfo=None)
        except Exception:
            return parsed.replace(tzinfo=None)
    return parsed


def _market_data_meta_from_asset(asset):
    if not asset:
        return {
            "status": "unknown",
            "source": "",
            "updated_at": None,
            "last_attempt_at": None,
            "last_error": "",
            "age_seconds": None,
            "stale_after_seconds": _market_data_stale_after_seconds(),
            "is_stale": True,
            "is_live": False,
        }

    stale_after_seconds = _market_data_stale_after_seconds()
    updated_at = asset.get("market_data_updated_at")
    updated_dt = _parse_iso_datetime(updated_at)
    age_seconds = None
    if updated_dt is not None:
        age_seconds = max(int((datetime.utcnow() - updated_dt).total_seconds()), 0)

    status = (asset.get("market_data_status") or "unknown").strip().lower()
    is_stale = status in {"stale", "failed", "unknown"} or updated_dt is None
    if age_seconds is not None and age_seconds > stale_after_seconds:
        is_stale = True

    return {
        "status": status or "unknown",
        "source": (asset.get("market_data_source") or "").strip(),
        "updated_at": updated_at,
        "last_attempt_at": asset.get("market_data_last_attempt_at"),
        "last_error": (asset.get("market_data_last_error") or "").strip(),
        "age_seconds": age_seconds,
        "stale_after_seconds": stale_after_seconds,
        "is_stale": bool(is_stale),
        "is_live": not bool(is_stale),
    }


def get_top_assets():
    from . import market_data as market_data_services

    return market_data_services.get_top_assets()


def _current_user_id():
    user = get_current_user()
    if not user or user.get("is_admin"):
        return None
    return int(user["id"])


def get_asset(ticker: str):
    from . import market_data as market_data_services

    return market_data_services.get_asset(ticker)

def get_asset_enrichment(ticker: str):
    from . import openclaw as openclaw_services

    return openclaw_services.get_asset_enrichment(ticker)


def get_asset_enrichment_history(ticker: str, limit: int = 12):
    from . import openclaw as openclaw_services

    return openclaw_services.get_asset_enrichment_history(ticker, limit=limit)


def get_asset_enrichments_map(tickers):
    normalized = []
    seen = set()
    for item in tickers or []:
        ticker = str(item or "").strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            normalized.append(ticker)
    if not normalized:
        return {}
    db = get_db()
    placeholders = ",".join(["?"] * len(normalized))
    rows = db.execute(
        """
        SELECT ticker, payload_json, raw_reply, updated_at
        FROM asset_enrichments
        WHERE ticker IN ("""
        + placeholders
        + """)
        """,
        tuple(normalized),
    ).fetchall()
    payload = {}
    for row in rows:
        payload_json = (row["payload_json"] or "").strip()
        parsed = None
        if payload_json:
            try:
                parsed = json.loads(payload_json)
            except Exception:
                parsed = None
        payload[row["ticker"]] = {
            "ticker": row["ticker"],
            "payload": parsed,
            "raw_reply": row["raw_reply"],
            "updated_at": row["updated_at"],
        }
    return payload


def _normalize_search_text(value):
    text = str(value or "").strip().lower()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def _normalize_structured_market_mood(value):
    text = _normalize_search_text(value)
    if not text:
        return ""
    if any(marker in text for marker in ("positivo", "favoravel", "otimista")):
        return "positive"
    if any(marker in text for marker in ("cauteloso", "negativo", "pessimista")):
        return "negative"
    if "neutro" in text:
        return "neutral"
    return ""


def _infer_market_mood_from_text(value):
    text = _normalize_search_text(value)
    if not text:
        return {"key": "neutral", "label": "Sem leitura clara", "score": 0}

    positive_markers = (
        "positivo",
        "otimista",
        "confianca",
        "resiliente",
        "solido",
        "barato",
        "atrativo",
        "desconto",
        "favoravel",
        "crescimento",
        "melhora",
        "forte",
    )
    negative_markers = (
        "negativo",
        "cautela",
        "pressionado",
        "caro",
        "esticado",
        "risco",
        "incerteza",
        "fraco",
        "desaceleracao",
        "volatil",
        "volatilidade",
        "piora",
        "desafiador",
    )
    score = 0
    for marker in positive_markers:
        if marker in text:
            score += 1
    for marker in negative_markers:
        if marker in text:
            score -= 1
    if score >= 2:
        return {"key": "positive", "label": "Mercado com vies positivo", "score": score}
    if score <= -2:
        return {"key": "negative", "label": "Mercado com vies cauteloso", "score": score}
    return {"key": "neutral", "label": "Mercado sem direcao forte", "score": score}


def _market_mood_presentation(structured_mood, market_view):
    normalized = _normalize_structured_market_mood(structured_mood)
    if normalized == "positive":
        return {"key": "positive", "label": "Mercado com vies positivo", "score": 2}
    if normalized == "negative":
        return {"key": "negative", "label": "Mercado com vies cauteloso", "score": -2}
    if normalized == "neutral":
        return {"key": "neutral", "label": "Mercado sem direcao forte", "score": 0}
    return _infer_market_mood_from_text(market_view)


def _normalize_structured_action(value):
    text = _normalize_search_text(value)
    if not text:
        return ""
    if "comprar_mais" in text or "comprar mais" in text or text == "compra":
        return "buy_more"
    if "segurar" in text or "manter" in text:
        return "hold"
    if "reduzir" in text or "vender" in text:
        return "reduce"
    if "observar" in text or "aguardar" in text or "monitorar" in text:
        return "watch"
    return ""


def _structured_action_label(value):
    normalized = _normalize_structured_action(value)
    if normalized == "buy_more":
        return "Comprar mais"
    if normalized == "hold":
        return "Segurar"
    if normalized == "reduce":
        return "Reduzir"
    if normalized == "watch":
        return "Monitorar"
    return ""


def _portfolio_decision_for_position(item: dict, enrichment: dict | None):
    payload = {}
    if isinstance(enrichment, dict) and isinstance(enrichment.get("payload"), dict):
        payload = enrichment.get("payload") or {}
    current_price = float(item.get("price") or 0.0)
    avg_price = float(item.get("avg_price") or 0.0)
    open_pnl_pct = float(item.get("open_pnl_pct") or 0.0)
    weight = float(item.get("weight") or 0.0)
    market_view = str(payload.get("visao_do_mercado") or "").strip()
    structured_mood = str(payload.get("humor_do_mercado") or "").strip()
    structured_action = str(payload.get("acao_sugerida") or "").strip()
    structured_action_key = _normalize_structured_action(structured_action)
    mood = _market_mood_presentation(structured_mood, market_view)
    price_gap_pct = ((current_price - avg_price) / avg_price) * 100 if avg_price > 0 else 0.0

    recommendation_key = "hold"
    rationale = "Sem sinal forte o bastante para mudar a posicao agora."

    if structured_action_key == "buy_more":
        recommendation_key = "increase"
        rationale = "OpenClaw sugeriu aumentar e a leitura atual nao indica deterioracao relevante."
    elif structured_action_key == "reduce":
        recommendation_key = "reduce"
        rationale = "OpenClaw sugeriu reduzir e a posicao pede mais cautela."
    elif structured_action_key == "watch":
        recommendation_key = "hold"
        rationale = "OpenClaw preferiu monitorar antes de mexer na posicao."
    elif mood["key"] == "positive" and price_gap_pct <= -7:
        recommendation_key = "increase"
        rationale = "Humor construtivo com preco abaixo do seu custo medio."
    elif mood["key"] == "negative" and (price_gap_pct >= 8 or open_pnl_pct >= 10):
        recommendation_key = "reduce"
        rationale = "Leitura mais cautelosa com espaco para reduzir risco."
    elif price_gap_pct <= -10:
        recommendation_key = "hold"
        rationale = "Preco caiu abaixo do medio, mas sem melhora clara no humor ainda."

    if recommendation_key == "increase" and weight >= 18:
        rationale = "Existe argumento para aumentar, mas o peso atual ja esta relevante na carteira."
    elif recommendation_key == "reduce" and weight >= 18:
        rationale = "A leitura pede mais prudencia e o peso atual amplifica o risco."

    recommendation_label = {
        "increase": "Aumentar",
        "hold": "Segurar",
        "reduce": "Reduzir",
    }.get(recommendation_key, "Segurar")

    conviction = 0.0
    if recommendation_key == "increase":
        conviction = max(0.0, -price_gap_pct) + max(0.0, mood["score"] * 3) + (2.0 if structured_action_key == "buy_more" else 0.0)
    elif recommendation_key == "reduce":
        conviction = max(0.0, price_gap_pct) + max(0.0, open_pnl_pct / 2.0) + (2.0 if structured_action_key == "reduce" else 0.0)
    else:
        conviction = abs(mood["score"]) + abs(price_gap_pct) / 4.0

    return {
        "ticker": item.get("ticker"),
        "name": item.get("name"),
        "category": item.get("category"),
        "value": round(float(item.get("value") or 0.0), 2),
        "weight": round(weight, 2),
        "price": round(current_price, 2),
        "avg_price": round(avg_price, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "price_gap_pct": round(price_gap_pct, 2),
        "mood_key": mood["key"],
        "mood_label": mood["label"],
        "structured_action": structured_action_key,
        "structured_action_label": _structured_action_label(structured_action) or "Sem sinal",
        "recommendation_key": recommendation_key,
        "recommendation_label": recommendation_label,
        "rationale": rationale,
        "conviction": round(conviction, 2),
    }


def _portfolio_bucket_sort_key(item):
    return (
        float(item.get("conviction") or 0.0),
        float(item.get("weight") or 0.0),
        float(item.get("value") or 0.0),
    )


def _build_portfolio_tactical_summary(positions, group_summaries, total_value):
    enrichments = get_asset_enrichments_map([item.get("ticker") for item in positions])
    increase = []
    hold = []
    reduce = []
    concentration_alerts = []
    single_position_limit = 18.0
    category_limit = 55.0

    for item in positions:
        decision = _portfolio_decision_for_position(item, enrichments.get(item.get("ticker")))
        if decision["recommendation_key"] == "increase":
            increase.append(decision)
        elif decision["recommendation_key"] == "reduce":
            reduce.append(decision)
        else:
            hold.append(decision)

        weight = float(item.get("weight") or 0.0)
        if weight >= single_position_limit:
            concentration_alerts.append(
                {
                    "kind": "position",
                    "label": f"{item.get('ticker')} ocupa {weight:.2f}% da carteira",
                    "detail": "Peso alto para uma posicao individual.",
                    "ticker": item.get("ticker"),
                    "weight": round(weight, 2),
                }
            )

    category_labels = {
        "br_stocks": "Acoes BR",
        "us_stocks": "Acoes US",
        "crypto": "Cripto",
        "fiis": "FIIs",
    }
    for category_key, summary in (group_summaries or {}).items():
        group_weight = ((float(summary.get("total_value") or 0.0) / float(total_value or 0.0)) * 100.0) if total_value else 0.0
        if group_weight >= category_limit:
            concentration_alerts.append(
                {
                    "kind": "category",
                    "label": f"{category_labels.get(category_key, category_key)} ocupa {group_weight:.2f}% da carteira",
                    "detail": "Concentracao alta por classe dentro da renda variavel.",
                    "category": category_key,
                    "weight": round(group_weight, 2),
                }
            )

    increase = sorted(increase, key=_portfolio_bucket_sort_key, reverse=True)
    hold = sorted(hold, key=_portfolio_bucket_sort_key, reverse=True)
    reduce = sorted(reduce, key=_portfolio_bucket_sort_key, reverse=True)
    concentration_alerts = sorted(
        concentration_alerts, key=lambda item: float(item.get("weight") or 0.0), reverse=True
    )

    return {
        "summary": {
            "increase_count": len(increase),
            "hold_count": len(hold),
            "reduce_count": len(reduce),
            "concentration_count": len(concentration_alerts),
            "analyzed_positions": len(positions),
        },
        "thresholds": {
            "single_position_weight_pct": single_position_limit,
            "category_weight_pct": category_limit,
        },
        "increase": increase,
        "hold": hold,
        "reduce": reduce,
        "concentration_alerts": concentration_alerts,
    }


def upsert_asset_enrichment(ticker: str, payload: dict | None, raw_reply: str, price_at_update: float = 0.0):
    from . import openclaw as openclaw_services

    return openclaw_services.upsert_asset_enrichment(
        ticker,
        payload,
        raw_reply,
        price_at_update=price_at_update,
    )


def enrich_asset_with_openclaw(ticker: str):
    from . import openclaw as openclaw_services

    return openclaw_services.enrich_asset_with_openclaw(ticker)


def enrich_assets_with_openclaw_batch(tickers=None, only_missing=True, limit=None):
    from . import openclaw as openclaw_services

    return openclaw_services.enrich_assets_with_openclaw_batch(
        tickers=tickers,
        only_missing=only_missing,
        limit=limit,
    )


def get_portfolios():
    from . import portfolio as portfolio_services

    return portfolio_services.get_portfolios()


def get_portfolio(portfolio_id: int):
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        row = db.execute("SELECT id, name FROM portfolios WHERE id = ?", (portfolio_id,)).fetchone()
    elif current_user_id is None:
        return None
    else:
        row = db.execute(
            "SELECT id, name FROM portfolios WHERE id = ? AND user_id = ?",
            (portfolio_id, current_user_id),
        ).fetchone()
    return _row_to_dict(row)


def _all_portfolio_ids():
    db = get_db()
    rows = db.execute("SELECT id FROM portfolios ORDER BY id ASC").fetchall()
    return [int(row["id"]) for row in rows]


def normalize_portfolio_ids(raw_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.normalize_portfolio_ids(raw_ids)


def get_default_portfolio_id():
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        row = db.execute("SELECT id FROM portfolios ORDER BY id ASC LIMIT 1").fetchone()
    elif current_user_id is None:
        return None
    else:
        row = db.execute(
            "SELECT id FROM portfolios WHERE user_id = ? ORDER BY id ASC LIMIT 1",
            (current_user_id,),
        ).fetchone()
    return int(row["id"]) if row else None


def resolve_portfolio_id(raw_portfolio_id):
    from . import portfolio as portfolio_services

    return portfolio_services.resolve_portfolio_id(raw_portfolio_id)


def create_portfolio(name: str):
    from . import portfolio as portfolio_services

    return portfolio_services.create_portfolio(name)


def delete_portfolio(portfolio_id):
    from . import portfolio as portfolio_services

    return portfolio_services.delete_portfolio(portfolio_id)


def _parse_float(value: str):
    if isinstance(value, (int, float)):
        return float(value)

    raw_value = (value or "").strip()
    raw_value = (
        raw_value.replace("R$", "")
        .replace("r$", "")
        .replace("US$", "")
        .replace("us$", "")
        .replace("$", "")
        .replace("%", "")
        .replace(" ", "")
    )

    if "," in raw_value and "." in raw_value:
        # Ex.: 1.234,56 -> 1234.56
        raw_value = raw_value.replace(".", "").replace(",", ".")
    elif "," in raw_value:
        # Ex.: 26,76 -> 26.76
        raw_value = raw_value.replace(",", ".")
    # Se vier somente com ponto decimal (ex.: 31.50), mantem como esta.

    if raw_value == "":
        return None
    try:
        return float(raw_value)
    except ValueError:
        return None


def _to_yahoo_symbol(ticker: str):
    symbol = (ticker or "").strip().upper()
    if "." in symbol or "-" in symbol:
        return symbol
    return f"{symbol}.SA"


def _to_number(value):
    try:
        if value is None:
            return None
        if hasattr(value, "iloc"):
            try:
                if len(value) == 0:
                    return None
                value = value.iloc[0]
            except Exception:
                return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_get(container, key):
    if container is None:
        return None
    try:
        return container.get(key)
    except Exception:
        return None


def _candidate_yahoo_symbols(ticker: str):
    raw_ticker = (ticker or "").strip().upper()
    if not raw_ticker:
        return []

    # Ativos dos EUA e cripto em USD devem consultar o simbolo original.
    if _is_us_stock_ticker(raw_ticker) or raw_ticker.endswith("-USD"):
        return [raw_ticker]

    symbols = [_to_yahoo_symbol(raw_ticker)]
    if "." not in raw_ticker and raw_ticker not in symbols:
        symbols.append(raw_ticker)
    return symbols


def _http_get_json_with_status(url: str, headers=None, timeout: float = 8.0, attempts: int = 2):
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }
    if headers:
        request_headers.update(headers)
    total_attempts = max(int(attempts or 1), 1)
    for attempt in range(total_attempts):
        request = Request(
            url,
            headers=request_headers,
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
                status_code = int(getattr(response, "status", 200) or 200)
            if not body:
                if attempt < total_attempts - 1:
                    time.sleep(0.15 * (attempt + 1))
                    continue
                return None, status_code
            try:
                return json.loads(body.decode("utf-8")), status_code
            except ValueError:
                if attempt < total_attempts - 1:
                    time.sleep(0.15 * (attempt + 1))
                    continue
                return None, status_code
        except HTTPError as exc:
            status_code = int(getattr(exc, "code", 0) or 0) or None
            if status_code in {408, 425, 429, 500, 502, 503, 504} and attempt < total_attempts - 1:
                time.sleep(0.15 * (attempt + 1))
                continue
            return None, status_code
        except (URLError, TimeoutError):
            if attempt < total_attempts - 1:
                time.sleep(0.15 * (attempt + 1))
                continue
            return None, None
        except Exception:
            if attempt < total_attempts - 1:
                time.sleep(0.15 * (attempt + 1))
                continue
            return None, None
    return None, None


def _http_get_json(url: str, headers=None, timeout: float = 8.0):
    payload, _ = _http_get_json_with_status(url, headers=headers, timeout=timeout, attempts=2)
    return payload


def _http_get_text(url: str, timeout: float = 8.0):
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
        },
    )
    for _ in range(2):
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
            if not body:
                continue
            return body.decode("utf-8", "ignore")
        except (URLError, TimeoutError):
            time.sleep(0.15)
            continue
        except Exception:
            time.sleep(0.15)
            continue
    return ""


def _number_from_text(value):
    raw = (value or "").strip().replace("\u00a0", "").replace(" ", "")
    if not raw:
        return None

    # Mantem apenas sinais/decimal, removendo moeda e textos.
    filtered = re.sub(r"[^0-9,.\-+]", "", raw)
    if not filtered:
        return None
    if "," in filtered and "." in filtered:
        filtered = filtered.replace(",", "")
    elif "," in filtered:
        filtered = filtered.replace(",", ".")

    try:
        return float(filtered)
    except Exception:
        return None


def _extract_google_metric_value(page_html: str, label: str):
    pattern = rf">{re.escape(label)}</div>.*?<div class=\"P6K39c\">([^<]+)</div>"
    match = re.search(pattern, page_html, re.S)
    if not match:
        return None
    return (match.group(1) or "").strip()


def _parse_market_cap_to_bi(value):
    raw = (value or "").strip().upper()
    if not raw:
        return None

    multiplier = 1.0 / 1_000_000_000.0
    if "T" in raw:
        multiplier = 1000.0
    elif "B" in raw:
        multiplier = 1.0
    elif "M" in raw:
        multiplier = 0.001
    elif "K" in raw:
        multiplier = 0.000001

    numeric = _number_from_text(raw)
    if numeric is None:
        return None
    return numeric * multiplier


def _is_crypto_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    return ticker_up.endswith("-USD")


def _get_coingecko_api_key():
    return (
        os.getenv("COINGECKO_API_KEY")
        or os.getenv("COINGECKO_DEMO_API_KEY")
        or ""
    ).strip()


def _get_coingecko_headers():
    headers = {}
    api_key = _get_coingecko_api_key()
    if api_key:
        headers["x-cg-demo-api-key"] = api_key
    return headers


def _get_coingecko_base_url():
    return (os.getenv("COINGECKO_BASE_URL") or "https://api.coingecko.com/api/v3").rstrip("/")


def _coingecko_cooldown_seconds():
    raw_value = (os.getenv("COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS") or "900").strip()
    try:
        return max(int(raw_value), 30)
    except (TypeError, ValueError):
        return 900


def _coingecko_is_temporarily_unavailable():
    now = time.time()
    disabled_until = float(_COINGECKO_CIRCUIT.get("until", 0.0) or 0.0)
    if disabled_until <= 0:
        return False
    if now >= disabled_until:
        _COINGECKO_CIRCUIT["until"] = 0.0
        _COINGECKO_CIRCUIT["status_code"] = None
        return False
    return True


def _coingecko_open_circuit(status_code=None):
    now = time.time()
    cooldown_seconds = _coingecko_cooldown_seconds()
    was_open = _coingecko_is_temporarily_unavailable()
    _COINGECKO_CIRCUIT["until"] = now + cooldown_seconds
    _COINGECKO_CIRCUIT["status_code"] = status_code
    if not was_open:
        logger = _get_app_logger()
        logger.warning(
            "CoinGecko temporariamente pausado por %ss apos falha HTTP status=%s.",
            cooldown_seconds,
            status_code if status_code is not None else "n/a",
        )


def _coingecko_close_circuit():
    _COINGECKO_CIRCUIT["until"] = 0.0
    _COINGECKO_CIRCUIT["status_code"] = None


def _coingecko_get_json(url: str, timeout: float = 12.0):
    if _coingecko_is_temporarily_unavailable():
        return None

    payload, status_code = _http_get_json_with_status(
        url,
        headers=_get_coingecko_headers(),
        timeout=timeout,
        attempts=2,
    )
    if payload is None:
        if status_code is None or status_code in {401, 403, 408, 425, 429, 500, 502, 503, 504}:
            _coingecko_open_circuit(status_code)
        return None

    _coingecko_close_circuit()
    return payload


def _coingecko_symbol_from_ticker(ticker: str):
    raw = (ticker or "").strip().upper()
    if not _is_crypto_ticker(raw):
        return ""
    return raw[:-4].strip().lower()


def _coingecko_history_config(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"days": "1", "date_fmt": "%H:%M", "supported": True},
        "7d": {"days": "7", "date_fmt": "%d/%m", "supported": True},
        "30d": {"days": "30", "date_fmt": "%d/%m", "supported": True},
        "6m": {"days": "180", "date_fmt": "%d/%m", "supported": True},
        "1y": {"days": "365", "date_fmt": "%d/%m/%y", "supported": True},
        "5y": {"days": None, "date_fmt": "%d/%m/%y", "supported": False},
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _fetch_coingecko_market_item(ticker: str):
    symbol = _coingecko_symbol_from_ticker(ticker)
    if not symbol:
        return None

    cache_key = ("cg_market", symbol)
    cached = _memory_cache_get(_COINGECKO_CACHE, cache_key)
    if cached is not None:
        return dict(cached)

    payload = _coingecko_get_json(
        (
            f"{_get_coingecko_base_url()}/coins/markets"
            f"?vs_currency=usd&symbols={symbol}&price_change_percentage=24h,7d,30d"
        ),
        timeout=12.0,
    ) or []
    try:
        item = (payload[0] if payload else None) or None
    except Exception:
        item = None
    if item:
        _memory_cache_set(_COINGECKO_CACHE, cache_key, dict(item), 120)
        return item
    return None


def _resolve_coingecko_coin_id(ticker: str):
    symbol = _coingecko_symbol_from_ticker(ticker)
    if not symbol:
        return None
    item = _fetch_coingecko_market_item(ticker)
    if not item:
        return None
    return (item.get("id") or "").strip() or None


def _fetch_coingecko_profile(ticker: str):
    item = _fetch_coingecko_market_item(ticker)
    if not item:
        return {}
    return {
        "name": (item.get("name") or item.get("symbol") or "").strip(),
        "sector": "Cripto",
        "logo_url": (item.get("image") or "").strip(),
    }


def _fetch_coingecko_metrics(ticker: str):
    item = _fetch_coingecko_market_item(ticker)
    if not item:
        return None
    metrics = {
        "price": _to_number(item.get("current_price")),
        "pl": None,
        "pvp": None,
        "dy": None,
        "variation_day": _to_number(item.get("price_change_percentage_24h_in_currency"))
        or _to_number(item.get("price_change_percentage_24h")),
        "variation_7d": _to_number(item.get("price_change_percentage_7d_in_currency")),
        "variation_30d": _to_number(item.get("price_change_percentage_30d_in_currency")),
        "market_cap_bi": (
            _to_number(item.get("market_cap")) / 1_000_000_000
            if _to_number(item.get("market_cap")) is not None
            else None
        ),
    }
    return _metrics_in_brl_if_needed(ticker, metrics) if _has_market_metrics(metrics) else None


def _fetch_coingecko_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _coingecko_history_config(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }
    if not cfg.get("supported", True):
        return result

    coin_id = _resolve_coingecko_coin_id(ticker)
    if not coin_id:
        return result

    cache_key = ("cg_history", coin_id, normalized_key)
    cached = _memory_cache_get(_COINGECKO_CACHE, cache_key)
    if cached is not None:
        return dict(cached)

    payload = _coingecko_get_json(
        f"{_get_coingecko_base_url()}/coins/{coin_id}/market_chart?vs_currency=usd&days={cfg['days']}",
        timeout=12.0,
    ) or {}
    prices_payload = payload.get("prices") or []
    if not prices_payload:
        return result

    usdbrl = _get_usdbrl_rate() if _is_usd_quoted_ticker(ticker) else None
    prices = []
    labels = []
    for point in prices_payload:
        try:
            timestamp_ms, close_value = point[0], point[1]
        except Exception:
            continue
        price_value = _to_number(close_value)
        if price_value is None:
            continue
        try:
            dt = datetime.fromtimestamp(float(timestamp_ms) / 1000.0)
        except Exception:
            continue
        if usdbrl is not None and usdbrl > 0:
            price_value *= usdbrl
        prices.append(round(float(price_value), 2))
        labels.append(dt.strftime(cfg["date_fmt"]))

    if not prices:
        return result

    first = prices[0]
    last = prices[-1]
    result["labels"] = labels
    result["prices"] = prices
    result["change_pct"] = ((last / first) - 1) * 100 if first not in (None, 0) else None
    _memory_cache_set(_COINGECKO_CACHE, cache_key, dict(result), 300)
    return result


def _get_alpha_vantage_api_key():
    return (os.getenv("ALPHA_VANTAGE_API_KEY") or "").strip()


def _get_alpha_vantage_base_url():
    return (os.getenv("ALPHA_VANTAGE_BASE_URL") or "https://www.alphavantage.co/query").rstrip("/")


def _fetch_alpha_vantage(function_name: str, params=None, ttl_seconds: int = 300):
    api_key = _get_alpha_vantage_api_key()
    if not api_key:
        return None

    query = {"function": function_name, "apikey": api_key}
    if params:
        query.update(params)
    cache_key = (function_name, tuple(sorted(query.items())))
    cached = _memory_cache_get(_ALPHA_VANTAGE_CACHE, cache_key)
    if cached is not None:
        return dict(cached) if isinstance(cached, dict) else cached

    payload = _http_get_json(
        f"{_get_alpha_vantage_base_url()}?{urlencode(query)}",
        timeout=15.0,
    )
    if not payload:
        return None
    if payload.get("Information") or payload.get("Note") or payload.get("Error Message"):
        return None
    _memory_cache_set(_ALPHA_VANTAGE_CACHE, cache_key, payload, ttl_seconds)
    return payload


def _fetch_alpha_vantage_quote(ticker: str):
    if not _is_us_stock_ticker(ticker):
        return None
    payload = _fetch_alpha_vantage(
        "GLOBAL_QUOTE",
        {"symbol": (ticker or "").strip().upper()},
        ttl_seconds=300,
    ) or {}
    quote = payload.get("Global Quote") or {}
    return quote or None


def _fetch_alpha_vantage_overview(ticker: str):
    if not _is_us_stock_ticker(ticker):
        return None
    payload = _fetch_alpha_vantage(
        "OVERVIEW",
        {"symbol": (ticker or "").strip().upper()},
        ttl_seconds=21600,
    ) or {}
    return payload or None


def _fetch_alpha_vantage_profile(ticker: str):
    overview = _fetch_alpha_vantage_overview(ticker)
    if not overview:
        return {}
    return {
        "name": (overview.get("Name") or "").strip(),
        "sector": (overview.get("Sector") or "").strip(),
        "logo_url": "",
    }


def _fetch_alpha_vantage_metrics(ticker: str):
    quote = _fetch_alpha_vantage_quote(ticker) or {}
    overview = _fetch_alpha_vantage_overview(ticker) or {}
    if not quote and not overview:
        return None

    price = _to_number(quote.get("05. price"))
    previous_close = _to_number(quote.get("08. previous close"))
    variation_day = None
    if price is not None and previous_close not in (None, 0):
        variation_day = ((price / previous_close) - 1) * 100
    else:
        change_pct_text = (quote.get("10. change percent") or "").replace("%", "")
        variation_day = _to_number(change_pct_text)

    dy_raw = _to_number(overview.get("DividendYield"))
    dy = None if dy_raw is None else (dy_raw * 100 if dy_raw <= 1.5 else dy_raw)
    market_cap = _to_number(overview.get("MarketCapitalization"))
    metrics = {
        "price": price,
        "pl": _to_number(overview.get("PERatio")),
        "pvp": _to_number(overview.get("PriceToBookRatio")),
        "dy": dy,
        "variation_day": variation_day,
        "variation_7d": None,
        "variation_30d": None,
        "market_cap_bi": (market_cap / 1_000_000_000) if market_cap is not None else None,
    }

    history_30d = _fetch_alpha_vantage_history(ticker, "30d")
    prices_30d = history_30d.get("prices") or []
    metrics["variation_30d"] = history_30d.get("change_pct")
    if len(prices_30d) >= 8:
        base_7 = _to_number(prices_30d[-8])
        last = _to_number(prices_30d[-1])
        if base_7 not in (None, 0) and last is not None:
            metrics["variation_7d"] = ((last / base_7) - 1) * 100

    return _metrics_in_brl_if_needed(ticker, metrics) if _has_market_metrics(metrics) else None


def _alpha_vantage_history_config(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"function": None, "series_key": None, "date_fmt": "%H:%M", "supported": False},
        "7d": {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "series_key": "Time Series (Daily)",
            "date_fmt": "%d/%m",
            "supported": True,
            "outputsize": "compact",
            "limit": 7,
        },
        "30d": {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "series_key": "Time Series (Daily)",
            "date_fmt": "%d/%m",
            "supported": True,
            "outputsize": "compact",
            "limit": 30,
        },
        "6m": {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "series_key": "Time Series (Daily)",
            "date_fmt": "%d/%m",
            "supported": True,
            "outputsize": "full",
            "limit": 180,
        },
        "1y": {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "series_key": "Time Series (Daily)",
            "date_fmt": "%d/%m/%y",
            "supported": True,
            "outputsize": "full",
            "limit": 365,
        },
        "5y": {
            "function": "TIME_SERIES_WEEKLY_ADJUSTED",
            "series_key": "Weekly Adjusted Time Series",
            "date_fmt": "%d/%m/%y",
            "supported": True,
            "limit": 260,
        },
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _fetch_alpha_vantage_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _alpha_vantage_history_config(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }
    if not _is_us_stock_ticker(ticker) or not cfg.get("supported", False):
        return result

    params = {"symbol": (ticker or "").strip().upper()}
    if cfg.get("outputsize"):
        params["outputsize"] = cfg["outputsize"]
    payload = _fetch_alpha_vantage(
        cfg["function"],
        params,
        ttl_seconds=3600,
    ) or {}
    series = payload.get(cfg["series_key"]) or {}
    if not series:
        return result

    usdbrl = _get_usdbrl_rate() if _is_usd_quoted_ticker(ticker) else None
    items = []
    for date_text, values in sorted(series.items()):
        try:
            dt = datetime.strptime(date_text, "%Y-%m-%d")
        except ValueError:
            continue
        close_value = _to_number(values.get("5. adjusted close")) or _to_number(values.get("4. close"))
        if close_value is None:
            continue
        if usdbrl is not None and usdbrl > 0:
            close_value *= usdbrl
        items.append((dt, round(float(close_value), 2)))

    if not items:
        return result

    limit = int(cfg.get("limit") or len(items))
    items = items[-limit:]
    prices = [price for _, price in items]
    labels = [dt.strftime(cfg["date_fmt"]) for dt, _ in items]
    first = prices[0]
    last = prices[-1]
    result["labels"] = labels
    result["prices"] = prices
    result["change_pct"] = ((last / first) - 1) * 100 if first not in (None, 0) else None
    return result


def _get_twelve_data_api_key():
    return (os.getenv("TWELVE_DATA_API_KEY") or "").strip()


def _get_twelve_data_base_url():
    return (os.getenv("TWELVE_DATA_BASE_URL") or "https://api.twelvedata.com").rstrip("/")


def _fetch_twelve_data(path: str, params=None, ttl_seconds: int = 300):
    api_key = _get_twelve_data_api_key()
    if not api_key:
        return None
    params = params or {}
    cache_key = (path, tuple(sorted(params.items())))
    cached = _memory_cache_get(_TWELVE_DATA_CACHE, cache_key)
    if cached is not None:
        return dict(cached) if isinstance(cached, dict) else cached

    payload = _http_get_json(
        f"{_get_twelve_data_base_url()}/{path}?{urlencode(params)}",
        headers={
            "Authorization": f"apikey {api_key}",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
        },
        timeout=15.0,
    )
    if not payload:
        return None
    if payload.get("status") == "error" or payload.get("code"):
        return None
    _memory_cache_set(_TWELVE_DATA_CACHE, cache_key, payload, ttl_seconds)
    return payload


def _fetch_twelve_data_quote(ticker: str):
    if not _is_us_stock_ticker(ticker):
        return None
    return _fetch_twelve_data(
        "quote",
        {"symbol": (ticker or "").strip().upper()},
        ttl_seconds=300,
    )


def _twelve_data_history_config(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"interval": "1h", "outputsize": 24, "date_fmt": "%H:%M"},
        "7d": {"interval": "1day", "outputsize": 7, "date_fmt": "%d/%m"},
        "30d": {"interval": "1day", "outputsize": 30, "date_fmt": "%d/%m"},
        "6m": {"interval": "1day", "outputsize": 180, "date_fmt": "%d/%m"},
        "1y": {"interval": "1day", "outputsize": 365, "date_fmt": "%d/%m/%y"},
        "5y": {"interval": "1week", "outputsize": 260, "date_fmt": "%d/%m/%y"},
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _fetch_twelve_data_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _twelve_data_history_config(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }
    if not _is_us_stock_ticker(ticker):
        return result

    payload = _fetch_twelve_data(
        "time_series",
        {
            "symbol": (ticker or "").strip().upper(),
            "interval": cfg["interval"],
            "outputsize": cfg["outputsize"],
            "order": "ASC",
        },
        ttl_seconds=1800,
    ) or {}
    values = payload.get("values") or []
    if not values:
        return result

    usdbrl = _get_usdbrl_rate() if _is_usd_quoted_ticker(ticker) else None
    prices = []
    labels = []
    for item in values:
        close_value = _to_number(item.get("close"))
        date_text = (item.get("datetime") or "").strip()
        if close_value is None or not date_text:
            continue
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(date_text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            continue
        if usdbrl is not None and usdbrl > 0:
            close_value *= usdbrl
        prices.append(round(float(close_value), 2))
        labels.append(parsed.strftime(cfg["date_fmt"]))

    if not prices:
        return result
    first = prices[0]
    last = prices[-1]
    result["labels"] = labels
    result["prices"] = prices
    result["change_pct"] = ((last / first) - 1) * 100 if first not in (None, 0) else None
    return result


def _fetch_twelve_data_metrics(ticker: str):
    quote = _fetch_twelve_data_quote(ticker) or {}
    if not quote:
        return None
    price = _to_number(quote.get("close"))
    previous_close = _to_number(quote.get("previous_close"))
    variation_day = None
    if price is not None and previous_close not in (None, 0):
        variation_day = ((price / previous_close) - 1) * 100
    else:
        variation_day = _to_number(quote.get("percent_change"))

    metrics = {
        "price": price,
        "pl": None,
        "pvp": None,
        "dy": None,
        "variation_day": variation_day,
        "variation_7d": None,
        "variation_30d": None,
        "market_cap_bi": None,
    }
    history_30d = _fetch_twelve_data_history(ticker, "30d")
    prices_30d = history_30d.get("prices") or []
    metrics["variation_30d"] = history_30d.get("change_pct")
    if len(prices_30d) >= 8:
        base_7 = _to_number(prices_30d[-8])
        last = _to_number(prices_30d[-1])
        if base_7 not in (None, 0) and last is not None:
            metrics["variation_7d"] = ((last / base_7) - 1) * 100

    return _metrics_in_brl_if_needed(ticker, metrics) if _has_market_metrics(metrics) else None


def _is_brazilian_market_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    if not ticker_up:
        return False
    if ticker_up.endswith("-USD") or _is_us_stock_ticker(ticker_up):
        return False
    return True


def _market_scanner_symbol_from_ticker(ticker: str):
    normalized = _normalize_brapi_symbol(ticker)
    if not normalized or not _is_brazilian_market_ticker(normalized):
        return ""
    return f"{normalized}.SA"


def _market_scanner_db_path():
    return (os.getenv("MARKET_SCANNER_DATABASE_PATH") or "").strip()


def _market_scanner_data_ttl_seconds():
    raw_value = (os.getenv("MARKET_SCANNER_DATA_TTL_SECONDS") or "120").strip()
    try:
        return max(int(raw_value), 10)
    except (TypeError, ValueError):
        return 120


def _market_scanner_load_snapshot(symbol: str):
    if not symbol:
        return None
    db_path = _market_scanner_db_path()
    if not db_path or not os.path.exists(db_path):
        return None

    conn = None
    try:
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.5)
        except Exception:
            conn = sqlite3.connect(db_path, timeout=1.5)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        ticker_base = symbol.removesuffix(".SA")

        catalog_row = cursor.execute(
            """
            SELECT ticker, yahoo_symbol, issuer_name, trading_name, specification,
                   is_active, yahoo_supported, last_scan_at, last_verified_at
            FROM tickers
            WHERE yahoo_symbol = ? OR ticker = ?
            LIMIT 1
            """,
            (symbol, ticker_base),
        ).fetchone()

        price_rows = cursor.execute(
            """
            SELECT close, timestamp
            FROM prices
            WHERE ticker = ? AND interval = '1d'
            ORDER BY timestamp DESC
            LIMIT 400
            """,
            (symbol,),
        ).fetchall()
        if not price_rows:
            price_rows = cursor.execute(
                """
                SELECT close, timestamp
                FROM prices
                WHERE ticker = ?
                ORDER BY timestamp DESC
                LIMIT 400
                """,
                (symbol,),
            ).fetchall()

        metric_rows = cursor.execute(
            """
            SELECT metric_name, metric_value, timestamp
            FROM metrics
            WHERE ticker = ?
            ORDER BY timestamp DESC
            LIMIT 200
            """,
            (symbol,),
        ).fetchall()

        latest_metrics = {}
        latest_metric_ts = None
        if metric_rows:
            latest_metric_ts = metric_rows[0]["timestamp"]
            for row in metric_rows:
                if row["timestamp"] != latest_metric_ts:
                    continue
                metric_name = str(row["metric_name"] or "").strip().lower()
                metric_value = _to_number(row["metric_value"])
                if metric_name and metric_value is not None:
                    latest_metrics[metric_name] = float(metric_value)

        catalog = dict(catalog_row) if catalog_row else {}
        prices_desc = [dict(row) for row in price_rows]
        latest_price = _to_number(prices_desc[0]["close"]) if prices_desc else None
        latest_price_ts = prices_desc[0]["timestamp"] if prices_desc else None
        return {
            "symbol": symbol,
            "catalog": catalog,
            "latest_price": latest_price,
            "latest_price_timestamp": latest_price_ts,
            "latest_metrics": latest_metrics,
            "latest_metrics_timestamp": latest_metric_ts,
            "prices_desc": prices_desc,
        }
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _fetch_market_scanner_snapshot(ticker: str):
    symbol = _market_scanner_symbol_from_ticker(ticker)
    if not symbol:
        return None

    cache_key = ("market_scanner_snapshot", symbol)
    cached = _memory_cache_get(_MARKET_SCANNER_CACHE, cache_key)
    if cached is not None:
        return dict(cached)

    snapshot = _market_scanner_load_snapshot(symbol)
    if snapshot is None:
        return None
    _memory_cache_set(
        _MARKET_SCANNER_CACHE,
        cache_key,
        dict(snapshot),
        _market_scanner_data_ttl_seconds(),
    )
    return snapshot


def _market_scanner_price_series_desc(snapshot):
    rows = (snapshot or {}).get("prices_desc") or []
    series = []
    for row in rows:
        close_value = _to_number((row or {}).get("close"))
        if close_value is None:
            continue
        series.append(float(close_value))
    return series


def _scanner_datetime_from_text(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _fetch_market_scanner_profile(ticker: str):
    snapshot = _fetch_market_scanner_snapshot(ticker)
    if not snapshot:
        return {}

    catalog = snapshot.get("catalog") or {}
    name = (
        str(catalog.get("trading_name") or "").strip()
        or str(catalog.get("issuer_name") or "").strip()
        or str(catalog.get("ticker") or "").strip()
        or str((ticker or "").strip().upper())
    )

    specification = str(catalog.get("specification") or "").strip().upper()
    ticker_up = str((ticker or "").strip().upper())
    if "FII" in specification or ticker_up.endswith("11"):
        sector = "Fundos/ETFs"
    else:
        sector = "Acoes BR"

    return {
        "name": name,
        "sector": sector,
        "logo_url": "",
    }


def _fetch_market_scanner_metrics(ticker: str):
    snapshot = _fetch_market_scanner_snapshot(ticker)
    if not snapshot:
        return None

    prices_desc = _market_scanner_price_series_desc(snapshot)
    current_price = _to_number(snapshot.get("latest_price"))
    if current_price is None and prices_desc:
        current_price = prices_desc[0]

    variation_day = None
    variation_7d = None
    variation_30d = None
    if len(prices_desc) >= 2:
        previous = _to_number(prices_desc[1])
        if previous not in (None, 0):
            variation_day = ((float(prices_desc[0]) / float(previous)) - 1) * 100
    if len(prices_desc) >= 8:
        base_7d = _to_number(prices_desc[7])
        if base_7d not in (None, 0):
            variation_7d = ((float(prices_desc[0]) / float(base_7d)) - 1) * 100
    if len(prices_desc) >= 31:
        base_30d = _to_number(prices_desc[30])
        if base_30d not in (None, 0):
            variation_30d = ((float(prices_desc[0]) / float(base_30d)) - 1) * 100

    metrics = {
        "price": _to_number(current_price),
        "pl": None,
        "pvp": None,
        "dy": None,
        "variation_day": variation_day,
        "variation_7d": variation_7d,
        "variation_30d": variation_30d,
        "market_cap_bi": None,
    }
    return metrics if _has_market_metrics(metrics) else None


def _market_scanner_history_limit_for_range(range_key: str):
    mapping = {
        "1d": 2,
        "7d": 8,
        "30d": 31,
        "6m": 180,
        "1y": 365,
        "5y": 1825,
    }
    return mapping.get((range_key or "1y").strip().lower(), 365)


def _fetch_market_scanner_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _history_config_for_brapi(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }

    snapshot = _fetch_market_scanner_snapshot(ticker)
    if not snapshot:
        return result

    rows_desc = (snapshot.get("prices_desc") or [])[: _market_scanner_history_limit_for_range(normalized_key)]
    rows = list(reversed(rows_desc))
    prices = []
    labels = []
    for row in rows:
        close_value = _to_number((row or {}).get("close"))
        dt = _scanner_datetime_from_text((row or {}).get("timestamp"))
        if close_value is None or dt is None:
            continue
        prices.append(round(float(close_value), 2))
        labels.append(dt.strftime(cfg["date_fmt"]))

    if not prices:
        return result

    first = prices[0]
    last = prices[-1]
    result["labels"] = labels
    result["prices"] = prices
    result["change_pct"] = ((last / first) - 1) * 100 if first not in (None, 0) else None
    return result


def _get_brapi_token():
    return (os.getenv("BRAPI_TOKEN") or "").strip()


def _get_brapi_headers():
    token = _get_brapi_token()
    if not token:
        return None
    return {"Authorization": f"Bearer {token}"}


def _get_brapi_base_url():
    return (os.getenv("BRAPI_BASE_URL") or "https://brapi.dev/api").rstrip("/")


def _brapi_quote_cache_ttl_seconds():
    raw_value = (os.getenv("BRAPI_QUOTE_CACHE_TTL_SECONDS") or "").strip()
    if not raw_value:
        return _BRAPI_QUOTE_CACHE_TTL_DEFAULT_SECONDS
    try:
        return max(float(raw_value), 5.0)
    except (TypeError, ValueError):
        return _BRAPI_QUOTE_CACHE_TTL_DEFAULT_SECONDS


def _brapi_cooldown_seconds():
    raw_value = (os.getenv("BRAPI_RATE_LIMIT_COOLDOWN_SECONDS") or "300").strip()
    try:
        return max(int(raw_value), 30)
    except (TypeError, ValueError):
        return 300


def _brapi_is_temporarily_unavailable():
    now = time.time()
    disabled_until = float(_BRAPI_CIRCUIT.get("until", 0.0) or 0.0)
    if disabled_until <= 0:
        return False
    if now >= disabled_until:
        _BRAPI_CIRCUIT["until"] = 0.0
        _BRAPI_CIRCUIT["status_code"] = None
        return False
    return True


def _brapi_open_circuit(status_code=None):
    now = time.time()
    cooldown_seconds = _brapi_cooldown_seconds()
    was_open = _brapi_is_temporarily_unavailable()
    _BRAPI_CIRCUIT["until"] = now + cooldown_seconds
    _BRAPI_CIRCUIT["status_code"] = status_code
    if not was_open:
        _get_app_logger().warning(
            "BRAPI temporariamente pausado por %ss apos falha HTTP status=%s.",
            cooldown_seconds,
            status_code if status_code is not None else "n/a",
        )


def _brapi_close_circuit():
    _BRAPI_CIRCUIT["until"] = 0.0
    _BRAPI_CIRCUIT["status_code"] = None


def _normalize_brapi_modules(modules):
    if not modules:
        return tuple()
    normalized = []
    for item in modules:
        value = str(item or "").strip()
        if value and value not in normalized:
            normalized.append(value)
    return tuple(normalized)


def _normalize_brapi_symbol(symbol: str):
    normalized = (symbol or "").strip().upper()
    if normalized.endswith(".SA"):
        normalized = normalized[:-3]
    return normalized


def _build_brapi_quote_query(range_key: str = None, interval: str = None, modules=None):
    params = {}
    if range_key:
        params["range"] = str(range_key).strip()
    if interval:
        params["interval"] = str(interval).strip()
    normalized_modules = _normalize_brapi_modules(modules)
    if normalized_modules:
        params["modules"] = ",".join(normalized_modules)
    return ("?" + urlencode(params)) if params else ""


def _brapi_quote_cache_key(ticker: str, range_key: str = None, interval: str = None, modules=None):
    return (
        _normalize_brapi_symbol(ticker),
        (range_key or "").strip().lower(),
        (interval or "").strip().lower(),
        _normalize_brapi_modules(modules),
    )


def _set_brapi_cached_quote_result(
    ticker: str,
    result,
    range_key: str = None,
    interval: str = None,
    modules=None,
):
    cache_key = _brapi_quote_cache_key(ticker, range_key=range_key, interval=interval, modules=modules)
    _BRAPI_QUOTE_RESULT_CACHE[cache_key] = {
        "expires_at": time.time() + _brapi_quote_cache_ttl_seconds(),
        "result": result,
    }
    if len(_BRAPI_QUOTE_RESULT_CACHE) > 500:
        now_ts = time.time()
        expired_keys = [
            key
            for key, value in _BRAPI_QUOTE_RESULT_CACHE.items()
            if float((value or {}).get("expires_at") or 0.0) <= now_ts
        ]
        for key in expired_keys:
            _BRAPI_QUOTE_RESULT_CACHE.pop(key, None)


def _get_brapi_cached_quote_result(ticker: str, range_key: str = None, interval: str = None, modules=None):
    cache_key = _brapi_quote_cache_key(ticker, range_key=range_key, interval=interval, modules=modules)
    cached = _BRAPI_QUOTE_RESULT_CACHE.get(cache_key)
    if not cached:
        return False, None
    expires_at = float(cached.get("expires_at") or 0.0)
    if expires_at <= time.time():
        _BRAPI_QUOTE_RESULT_CACHE.pop(cache_key, None)
        return False, None
    return True, cached.get("result")


def _log_brapi_empty_payload(tickers, query: str):
    if not _should_log_market_sources():
        return
    for ticker in tickers or []:
        normalized = _normalize_brapi_symbol(ticker) or "?"
        if (
            normalized not in _BRAPI_DIAG["empty_payload_tickers"]
            and len(_BRAPI_DIAG["empty_payload_tickers"]) < 15
        ):
            _get_app_logger().info(
                "BRAPI sem resposta (payload vazio): ticker=%s query=%s",
                normalized,
                query or "(none)",
            )
            _BRAPI_DIAG["empty_payload_tickers"].add(normalized)


def _log_brapi_empty_results(tickers, query: str):
    if not _should_log_market_sources():
        return
    for ticker in tickers or []:
        normalized = _normalize_brapi_symbol(ticker) or "?"
        if (
            normalized not in _BRAPI_DIAG["empty_results_tickers"]
            and len(_BRAPI_DIAG["empty_results_tickers"]) < 15
        ):
            _get_app_logger().info(
                "BRAPI retornou results vazio: ticker=%s query=%s",
                normalized,
                query or "(none)",
            )
            _BRAPI_DIAG["empty_results_tickers"].add(normalized)


def _fetch_brapi_quote_results_batch(tickers, range_key: str = None, interval: str = None, modules=None):
    normalized_tickers = []
    seen = set()
    for item in tickers or []:
        ticker = _normalize_brapi_symbol(item)
        if not ticker or ticker in seen:
            continue
        if not _is_brazilian_market_ticker(ticker):
            continue
        seen.add(ticker)
        normalized_tickers.append(ticker)
    if not normalized_tickers:
        return {}

    headers = _get_brapi_headers()
    if not headers:
        if _should_log_market_sources() and not _BRAPI_DIAG["missing_token_logged"]:
            _get_app_logger().warning(
                "BRAPI desabilitado: BRAPI_TOKEN ausente (ticker=%s)",
                normalized_tickers[0] if normalized_tickers else "?",
            )
            _BRAPI_DIAG["missing_token_logged"] = True
        return {}

    normalized_modules = _normalize_brapi_modules(modules)
    query = _build_brapi_quote_query(range_key=range_key, interval=interval, modules=normalized_modules)

    result_map = {}
    pending = []
    for ticker in normalized_tickers:
        has_cached, cached_result = _get_brapi_cached_quote_result(
            ticker,
            range_key=range_key,
            interval=interval,
            modules=normalized_modules,
        )
        if has_cached:
            result_map[ticker] = cached_result
        else:
            pending.append(ticker)

    if _brapi_is_temporarily_unavailable():
        for ticker in pending:
            result_map[ticker] = None
            _set_brapi_cached_quote_result(
                ticker,
                None,
                range_key=range_key,
                interval=interval,
                modules=normalized_modules,
            )
        return result_map

    for start in range(0, len(pending), _BRAPI_BATCH_LIMIT):
        chunk = pending[start : start + _BRAPI_BATCH_LIMIT]
        payload, status_code = _http_get_json_with_status(
            f"{_get_brapi_base_url()}/quote/{','.join(chunk)}{query}",
            headers=headers,
            timeout=12.0,
            attempts=2,
        )
        if payload is None:
            if status_code is None or status_code in {401, 403, 408, 425, 429, 500, 502, 503, 504}:
                _brapi_open_circuit(status_code)
        else:
            _brapi_close_circuit()
        if not payload:
            _log_brapi_empty_payload(chunk, query)
            for ticker in chunk:
                result_map[ticker] = None
                _set_brapi_cached_quote_result(
                    ticker,
                    None,
                    range_key=range_key,
                    interval=interval,
                    modules=normalized_modules,
                )
            continue

        try:
            results = payload.get("results") or []
        except Exception:
            results = []
        if not results:
            _log_brapi_empty_results(chunk, query)

        parsed_chunk = {}
        for idx, item in enumerate(results):
            if not isinstance(item, dict):
                continue
            symbol = _normalize_brapi_symbol(item.get("symbol"))
            if symbol and symbol in chunk:
                parsed_chunk[symbol] = item
                continue
            if idx < len(chunk):
                requested = chunk[idx]
                if requested not in parsed_chunk:
                    parsed_chunk[requested] = item

        for ticker in chunk:
            result = parsed_chunk.get(ticker)
            if result is None:
                _log_brapi_empty_results([ticker], query)
            result_map[ticker] = result
            _set_brapi_cached_quote_result(
                ticker,
                result,
                range_key=range_key,
                interval=interval,
                modules=normalized_modules,
            )

    return result_map


def _fetch_brapi_quote_result(ticker: str, range_key: str = None, interval: str = None, modules=None):
    normalized_ticker = _normalize_brapi_symbol(ticker)
    if not _is_brazilian_market_ticker(normalized_ticker):
        return None
    result_map = _fetch_brapi_quote_results_batch(
        [normalized_ticker],
        range_key=range_key,
        interval=interval,
        modules=modules,
    )
    return result_map.get(normalized_ticker)


def _history_config_for_brapi(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"range": "5d", "interval": "1h", "date_fmt": "%H:%M"},
        "7d": {"range": "5d", "interval": "1h", "date_fmt": "%d/%m"},
        "30d": {"range": "1mo", "interval": "1d", "date_fmt": "%d/%m"},
        "6m": {"range": "6mo", "interval": "1d", "date_fmt": "%d/%m"},
        "1y": {"range": "1y", "interval": "1d", "date_fmt": "%d/%m/%y"},
        "5y": {"range": "5y", "interval": "1wk", "date_fmt": "%d/%m/%y"},
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _candidate_google_quotes(ticker: str):
    raw = (ticker or "").strip().upper()
    if not raw:
        return []

    clean = raw.replace(".SA", "")
    candidates = []

    if clean.endswith("-USD"):
        candidates.extend([clean, raw])
    elif _is_us_stock_ticker(clean):
        candidates.extend([f"{clean}:NASDAQ", f"{clean}:NYSE", f"{clean}:AMEX", clean])
    else:
        candidates.extend([f"{clean}:BVMF", clean, raw])

    unique = []
    for item in candidates:
        candidate = (item or "").strip()
        if candidate and candidate not in unique:
            unique.append(candidate)
    return unique


def _fetch_google_metrics(ticker: str):
    timeout_ms = int(os.getenv("GOOGLE_SCRAPER_TIMEOUT_MS", "2500") or "2500")
    timeout_s = max(timeout_ms, 500) / 1000.0
    retries = int(os.getenv("GOOGLE_SCRAPER_MAX_RETRIES", "1") or "1")
    retries = min(max(retries, 0), 3)

    for quote in _candidate_google_quotes(ticker):
        page_html = ""
        for _ in range(retries + 1):
            page_html = _http_get_text(f"https://www.google.com/finance/quote/{quote}", timeout=timeout_s)
            if page_html:
                break
            time.sleep(0.1)
        if not page_html:
            continue

        price_match = re.search(r'data-last-price="([^"]+)"', page_html)
        price = _number_from_text(price_match.group(1)) if price_match else None

        pl = _number_from_text(_extract_google_metric_value(page_html, "P/E ratio"))
        pvp = _number_from_text(_extract_google_metric_value(page_html, "P/B ratio"))
        dy = _number_from_text(_extract_google_metric_value(page_html, "Dividend yield"))
        market_cap_bi = _parse_market_cap_to_bi(_extract_google_metric_value(page_html, "Market cap"))

        metrics = {
            "price": price,
            "pl": pl,
            "pvp": pvp,
            "dy": dy,
            "variation_day": None,
            "variation_7d": None,
            "variation_30d": None,
            "market_cap_bi": market_cap_bi,
        }
        if any(metrics.get(field) is not None for field in ("price", "pl", "pvp", "dy", "market_cap_bi")):
            return _metrics_in_brl_if_needed(ticker, metrics)
    return None


def _fetch_brapi_profile(ticker: str):
    modules = ["summaryProfile"]
    result = _fetch_brapi_quote_result(ticker, modules=modules)
    if not result:
        return {}

    summary_profile = result.get("summaryProfile") or {}
    name = (
        (result.get("longName") or result.get("shortName") or "").strip()
        or (summary_profile.get("name") or "").strip()
    )
    sector = (
        (summary_profile.get("sectorDisp") or summary_profile.get("sector") or "").strip()
        or (summary_profile.get("industryDisp") or summary_profile.get("industry") or "").strip()
    )
    logo_url = (
        (result.get("logourl") or "").strip()
        or (summary_profile.get("logoUrl") or "").strip()
    )
    return {"name": name, "sector": sector, "logo_url": logo_url}


def _fetch_brapi_metrics(ticker: str):
    # Observacao: nem todas as chaves/plans do BRAPI retornam dados quando pedimos
    # modulos avancados (ex: defaultKeyStatistics/summaryDetail). Para evitar cair
    # sempre em providers de fallback, usamos o payload basico (top-level keys).
    result = _fetch_brapi_quote_result(ticker)
    if not result:
        return None

    price = _to_number(result.get("regularMarketPrice"))
    previous_close = _to_number(result.get("regularMarketPreviousClose"))
    variation_day = None
    if price is not None and previous_close not in (None, 0):
        variation_day = ((price / previous_close) - 1) * 100
    else:
        variation_day = _to_number(result.get("regularMarketChangePercent"))

    dy_raw = _to_number(result.get("dividendYield"))
    dy = None if dy_raw is None else (dy_raw * 100 if dy_raw <= 1.5 else dy_raw)

    pvp = _to_number(result.get("priceToBook"))
    pl = _to_number(result.get("priceEarnings"))
    market_cap = _to_number(result.get("marketCap"))

    history = _fetch_brapi_history(ticker, "30d")
    prices = history.get("prices") or []
    variation_30d = history.get("change_pct")
    variation_7d = None
    variation_day_from_history = None
    if len(prices) >= 8:
        base_7 = _to_number(prices[-8])
        last = _to_number(prices[-1])
        if base_7 not in (None, 0) and last is not None:
            variation_7d = ((last / base_7) - 1) * 100
    if len(prices) >= 2:
        previous = _to_number(prices[-2])
        last = _to_number(prices[-1])
        if previous not in (None, 0) and last is not None:
            variation_day_from_history = ((last / previous) - 1) * 100

    metrics = {
        "price": price,
        "pl": pl,
        "pvp": pvp,
        "dy": dy,
        "variation_day": variation_day if variation_day is not None else variation_day_from_history,
        "variation_7d": variation_7d,
        "variation_30d": variation_30d,
        "market_cap_bi": (market_cap / 1_000_000_000) if market_cap is not None else None,
    }
    return metrics if _has_market_metrics(metrics) else None


def _fetch_brapi_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _history_config_for_brapi(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }

    quote = _fetch_brapi_quote_result(
        ticker,
        range_key=cfg["range"],
        interval=cfg["interval"],
    )
    if not quote:
        return result

    points = quote.get("historicalDataPrice") or []
    prices = []
    labels = []
    for item in points:
        close_value = _to_number(item.get("close"))
        timestamp = item.get("date")
        if close_value is None or timestamp is None:
            continue
        try:
            dt = datetime.fromtimestamp(int(timestamp))
        except Exception:
            continue
        prices.append(round(float(close_value), 2))
        labels.append(dt.strftime(cfg["date_fmt"]))

    if not prices:
        return result

    first = prices[0]
    last = prices[-1]
    result["labels"] = labels
    result["prices"] = prices
    result["change_pct"] = ((last / first) - 1) * 100 if first not in (None, 0) else None
    return result


def _prefetch_brapi_market_data_for_tickers(tickers):
    from . import legacy_compat

    return legacy_compat._prefetch_brapi_market_data_for_tickers(tickers)


def _fetch_bcb_series(series_code: int, date_start: str, date_end: str):
    cache_key = (int(series_code), date_start, date_end)
    if cache_key in _BCB_SERIES_CACHE:
        return _BCB_SERIES_CACHE[cache_key]

    start_dt = datetime.strptime(date_start, "%Y-%m-%d")
    end_dt = datetime.strptime(date_end, "%Y-%m-%d")
    data_inicial = start_dt.strftime("%d/%m/%Y")
    data_final = end_dt.strftime("%d/%m/%Y")
    url = (
        "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
        f"{series_code}/dados?formato=json&dataInicial={data_inicial}&dataFinal={data_final}"
    )
    payload = _http_get_json(url) or []
    parsed = []
    for item in payload:
        raw_date = (item.get("data") or "").strip()
        raw_value = item.get("valor")
        try:
            date_value = datetime.strptime(raw_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            continue
        numeric = _parse_float(raw_value)
        if numeric is None:
            continue
        parsed.append((date_value, float(numeric)))

    _BCB_SERIES_CACHE[cache_key] = parsed
    return parsed


def _compound_from_bcb_series(
    series_code: int,
    start_date,
    end_date,
    multiplier: float = 1.0,
    extrapolation_step_days: float = 1.0,
):
    if start_date > end_date:
        return 1.0, True
    start_iso = start_date.strftime("%Y-%m-%d")
    end_iso = end_date.strftime("%Y-%m-%d")
    try:
        series = _fetch_bcb_series(series_code, start_iso, end_iso)
    except Exception:
        return 1.0, False
    if not series:
        return 1.0, False

    factor = 1.0
    for _, pct_value in series:
        factor *= 1 + ((pct_value / 100.0) * multiplier)
    try:
        last_series_date = datetime.strptime(series[-1][0], "%Y-%m-%d").date()
    except Exception:
        last_series_date = end_date
    missing_days = max((end_date - last_series_date).days, 0)
    if missing_days > 0 and extrapolation_step_days > 0:
        last_pct_value = float(series[-1][1])
        step_factor = 1 + ((last_pct_value / 100.0) * multiplier)
        factor *= step_factor ** (missing_days / extrapolation_step_days)
    return factor, True


def _fetch_yahoo_quote(symbol: str):
    payload = _http_get_json(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}")
    if not payload:
        return {}
    try:
        result = payload.get("quoteResponse", {}).get("result", [])
        if result:
            return result[0] or {}
    except Exception:
        return {}
    return {}


def _fetch_yahoo_quote_summary(symbol: str):
    modules = "assetProfile,summaryDetail,defaultKeyStatistics,price"
    payload = _http_get_json(
        f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}?modules={modules}"
    )
    if not payload:
        return {}
    try:
        result = payload.get("quoteSummary", {}).get("result", [])
        if result:
            return result[0] or {}
    except Exception:
        return {}
    return {}


def _metrics_from_quote(quote: dict):
    if not quote:
        return None

    quote_price = _to_number(quote.get("regularMarketPrice")) or _to_number(quote.get("postMarketPrice"))
    quote_prev_close = _to_number(quote.get("regularMarketPreviousClose"))
    quote_change_pct = _to_number(quote.get("regularMarketChangePercent"))
    quote_pl = _to_number(quote.get("trailingPE")) or _to_number(quote.get("forwardPE"))
    quote_pvp = _to_number(quote.get("priceToBook"))
    quote_cap = _to_number(quote.get("marketCap"))
    quote_dy_raw = _to_number(quote.get("trailingAnnualDividendYield")) or _to_number(
        quote.get("dividendYield")
    )
    quote_dy = None if quote_dy_raw is None else (quote_dy_raw * 100 if quote_dy_raw <= 1.5 else quote_dy_raw)

    quote_variation = None
    if quote_price is not None and quote_prev_close not in (None, 0):
        quote_variation = ((quote_price / quote_prev_close) - 1) * 100
    elif quote_change_pct is not None:
        quote_variation = quote_change_pct

    if any(value is not None for value in [quote_price, quote_pl, quote_pvp, quote_dy]):
        return {
            "price": quote_price,
            "pl": quote_pl,
            "pvp": quote_pvp,
            "dy": quote_dy,
            "variation_day": quote_variation,
            "variation_7d": None,
            "variation_30d": None,
            "market_cap_bi": (quote_cap / 1_000_000_000) if quote_cap is not None else None,
        }

    return None


def _is_us_stock_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    if not ticker_up:
        return False
    if ticker_up.endswith("11"):
        return False
    if ticker_up.endswith("USDT") or ticker_up.endswith("-USD"):
        return False
    clean = ticker_up.replace(".", "")
    return clean.isalpha() and len(clean) <= 6


def _is_usd_quoted_ticker(ticker: str):
    ticker_up = (ticker or "").strip().upper()
    return ticker_up.endswith("-USD") or _is_us_stock_ticker(ticker_up)


def _get_usdbrl_rate():
    now = time.time()
    stale_rate = _FX_CACHE["usdbrl"]
    if _FX_CACHE["usdbrl"] is not None and now < _FX_CACHE["expires_at"]:
        return _FX_CACHE["usdbrl"]

    for symbol in ("BRL=X", "USDBRL=X"):
        quote = _fetch_yahoo_quote(symbol)
        rate = _to_number(quote.get("regularMarketPrice")) or _to_number(quote.get("postMarketPrice"))
        if rate is not None and rate > 0:
            _FX_CACHE["usdbrl"] = rate
            _FX_CACHE["expires_at"] = now + 300
            return rate

    # Fallback HTTP fora do Yahoo (mais resiliencia quando Yahoo oscila).
    awesome = _http_get_json("https://economia.awesomeapi.com.br/json/last/USD-BRL")
    try:
        awesome_rate = _to_number((awesome or {}).get("USDBRL", {}).get("bid"))
    except Exception:
        awesome_rate = None
    if awesome_rate is not None and awesome_rate > 0:
        _FX_CACHE["usdbrl"] = awesome_rate
        _FX_CACHE["expires_at"] = now + 300
        return awesome_rate

    erapi = _http_get_json("https://open.er-api.com/v6/latest/USD")
    try:
        erapi_rate = _to_number((erapi or {}).get("rates", {}).get("BRL"))
    except Exception:
        erapi_rate = None
    if erapi_rate is not None and erapi_rate > 0:
        _FX_CACHE["usdbrl"] = erapi_rate
        _FX_CACHE["expires_at"] = now + 300
        return erapi_rate

    if yf is not None:
        for symbol in ("BRL=X", "USDBRL=X"):
            try:
                ticker = yf.Ticker(symbol)
                fast = ticker.fast_info or {}
                info = ticker.info or {}
                rate = (
                    _to_number(_safe_get(fast, "lastPrice"))
                    or _to_number(_safe_get(info, "regularMarketPrice"))
                    or _to_number(_safe_get(info, "currentPrice"))
                )
                if rate is not None and rate > 0:
                    _FX_CACHE["usdbrl"] = rate
                    _FX_CACHE["expires_at"] = now + 300
                    return rate
            except Exception:
                continue

        # Fallback final via historico diario.
        for symbol in ("BRL=X", "USDBRL=X"):
            try:
                hist = yf.download(symbol, period="5d", interval="1d", progress=False, threads=False)
                if hist is not None and not hist.empty:
                    rate = _to_number(hist["Close"].dropna().iloc[-1])
                    if rate is not None and rate > 0:
                        _FX_CACHE["usdbrl"] = rate
                        _FX_CACHE["expires_at"] = now + 300
                        return rate
            except Exception:
                continue
    # Se nada respondeu agora, usa ultimo valor em cache para evitar falhas em lote.
    return stale_rate


def _metrics_in_brl_if_needed(ticker: str, metrics: dict):
    if not metrics or not _is_usd_quoted_ticker(ticker):
        return metrics

    usdbrl = _get_usdbrl_rate()
    if usdbrl is None:
        return metrics

    updated = dict(metrics)
    if updated.get("price") is not None:
        updated["price"] = updated["price"] * usdbrl
    if updated.get("market_cap_bi") is not None:
        updated["market_cap_bi"] = updated["market_cap_bi"] * usdbrl
    return updated


def _convert_usd_to_brl_if_needed(ticker: str, amount: float):
    if amount is None:
        return True, None, None
    if not _is_us_stock_ticker(ticker):
        return True, amount, None

    usdbrl = _get_usdbrl_rate()
    if usdbrl is None:
        return False, None, "Nao foi possivel obter cotacao USD/BRL para converter ativo dos EUA."
    return True, amount * usdbrl, None


def _history_variations(hist, current_price=None):
    if hist is None or hist.empty:
        return {"variation_7d": None, "variation_30d": None}

    try:
        closes = hist["Close"].dropna()
    except Exception:
        return {"variation_7d": None, "variation_30d": None}

    if closes.empty:
        return {"variation_7d": None, "variation_30d": None}

    last = _to_number(current_price) or _to_number(closes.iloc[-1])
    if last is None or last == 0:
        return {"variation_7d": None, "variation_30d": None}

    var_7d = None
    var_30d = None

    if len(closes) >= 8:
        base_7 = _to_number(closes.iloc[-8])
        if base_7 not in (None, 0):
            var_7d = ((last / base_7) - 1) * 100

    if len(closes) >= 31:
        base_30 = _to_number(closes.iloc[-31])
        if base_30 not in (None, 0):
            var_30d = ((last / base_30) - 1) * 100

    return {"variation_7d": var_7d, "variation_30d": var_30d}


def _history_config(range_key: str):
    key = (range_key or "1y").lower()
    configs = {
        "1d": {"period": "5d", "interval": "30m", "date_fmt": "%H:%M"},
        "7d": {"period": "10d", "interval": "1h", "date_fmt": "%d/%m"},
        "30d": {"period": "2mo", "interval": "1d", "date_fmt": "%d/%m"},
        "6m": {"period": "6mo", "interval": "1d", "date_fmt": "%d/%m"},
        "1y": {"period": "1y", "interval": "1d", "date_fmt": "%d/%m/%y"},
        "5y": {"period": "5y", "interval": "1wk", "date_fmt": "%d/%m/%y"},
    }
    return key if key in configs else "1y", configs.get(key, configs["1y"])


def _extract_close_series(hist):
    if hist is None:
        return None
    try:
        if "Close" in hist:
            closes = hist["Close"]
        else:
            return None
    except Exception:
        return None

    # Em algumas versoes/formatos, o "Close" pode vir como DataFrame.
    try:
        if hasattr(closes, "columns"):
            columns = list(getattr(closes, "columns", []))
            if not columns:
                return None
            closes = closes[columns[0]]
    except Exception:
        return None
    return closes


def _fetch_chart_points(symbol: str, range_key: str):
    range_map = {
        "1d": ("5d", "30m"),
        "7d": ("10d", "1h"),
        "30d": ("1mo", "1d"),
        "6m": ("6mo", "1d"),
        "1y": ("1y", "1d"),
        "5y": ("5y", "1wk"),
    }
    r, i = range_map.get(range_key, ("1y", "1d"))
    payload = _http_get_json(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={r}&interval={i}"
    )
    if not payload:
        return []
    try:
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []
        item = result[0] or {}
        timestamps = item.get("timestamp") or []
        quote = ((item.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
    except Exception:
        return []

    points = []
    for ts, close in zip(timestamps, closes):
        close_value = _to_number(close)
        if close_value is None:
            continue
        try:
            dt = datetime.fromtimestamp(int(ts))
        except Exception:
            continue
        points.append((dt, float(close_value)))
    return points


def _get_yahoo_asset_price_history(ticker: str, range_key: str = "1y"):
    normalized_key, cfg = _history_config(range_key)
    result = {
        "range_key": normalized_key,
        "labels": [],
        "prices": [],
        "change_pct": None,
    }

    usdbrl = _get_usdbrl_rate() if _is_usd_quoted_ticker(ticker) else None

    for symbol in _candidate_yahoo_symbols(ticker):
        points = []

        if yf is not None:
            try:
                hist = yf.download(
                    symbol,
                    period=cfg["period"],
                    interval=cfg["interval"],
                    progress=False,
                    threads=False,
                    auto_adjust=False,
                )
            except Exception:
                hist = None

            closes = _extract_close_series(hist)
            if closes is not None:
                try:
                    close_series = closes.dropna()
                except Exception:
                    close_series = closes
                try:
                    for idx, value in close_series.items():
                        close_value = _to_number(value)
                        if close_value is None:
                            continue
                        dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                        points.append((dt, float(close_value)))
                except Exception:
                    points = []

        if not points:
            points = _fetch_chart_points(symbol, normalized_key)

        if not points:
            continue

        prices = []
        labels = []
        for dt, price in points:
            price_value = float(price)
            if usdbrl is not None and usdbrl > 0:
                price_value *= usdbrl
            prices.append(round(price_value, 2))
            try:
                labels.append(dt.strftime(cfg["date_fmt"]))
            except Exception:
                labels.append(str(dt))

        if not prices:
            continue

        first = prices[0]
        last = prices[-1]
        change_pct = ((last / first) - 1) * 100 if first not in (None, 0) else None
        return {
            "range_key": normalized_key,
            "labels": labels,
            "prices": prices,
            "change_pct": change_pct,
        }

    return result


def get_asset_price_history(ticker: str, range_key: str = "1y"):
    from . import market_data as market_data_services

    return market_data_services.get_asset_price_history(ticker, range_key=range_key)


def _fetch_yahoo_info(ticker: str):
    if yf is None:
        return {}

    for symbol in _candidate_yahoo_symbols(ticker):
        for _ in range(2):
            try:
                info = yf.Ticker(symbol).info or {}
            except Exception:
                info = {}
            if info:
                return info
    return {}


def _fetch_yahoo_profile(ticker: str):
    info = _fetch_yahoo_info(ticker)
    name = (info.get("longName") or info.get("shortName") or info.get("displayName") or "").strip()
    sector = (info.get("sectorDisp") or info.get("sector") or "").strip()

    if not name or not sector:
        for symbol in _candidate_yahoo_symbols(ticker):
            quote = _fetch_yahoo_quote(symbol)
            summary = _fetch_yahoo_quote_summary(symbol)

            if not name:
                name = (
                    (quote.get("longName") or quote.get("shortName") or "").strip()
                    or (
                        ((summary.get("price") or {}).get("longName"))
                        or ((summary.get("price") or {}).get("shortName"))
                        or ""
                    ).strip()
                )

            if not sector:
                sector = (
                    ((summary.get("assetProfile") or {}).get("sector") or "").strip()
                    or (quote.get("sectorDisp") or quote.get("sector") or "").strip()
                )

            if name and sector:
                break

    # Fallback para ativos que o Yahoo nao classifica em setor (ex.: alguns ETFs/Fundos).
    if not sector:
        name_upper = name.upper()
        short_name = (info.get("shortName") or "").upper()
        if "ETF" in name_upper or "ETF" in short_name:
            sector = "ETF"
        elif ticker.upper().endswith("11"):
            sector = "Fundos/ETFs"

    return {"name": name, "sector": sector}


def _fetch_yahoo_metrics(ticker: str):
    if yf is None:
        for symbol in _candidate_yahoo_symbols(ticker):
            quote_metrics = _metrics_from_quote(_fetch_yahoo_quote(symbol))
            if quote_metrics:
                return _metrics_in_brl_if_needed(ticker, quote_metrics)
        return None

    for symbol in _candidate_yahoo_symbols(ticker):
        for _ in range(2):
            yf_ticker = yf.Ticker(symbol)

            try:
                fast = yf_ticker.fast_info or {}
            except Exception:
                fast = {}

            try:
                info = yf_ticker.info or {}
            except Exception:
                info = {}

            price = (
                _to_number(_safe_get(fast, "lastPrice"))
                or _to_number(_safe_get(info, "regularMarketPrice"))
                or _to_number(_safe_get(info, "currentPrice"))
            )
            previous_close = _to_number(_safe_get(fast, "previousClose")) or _to_number(
                _safe_get(info, "regularMarketPreviousClose")
            )

            variation_day = None
            if price is not None and previous_close not in (None, 0):
                variation_day = ((price / previous_close) - 1) * 100
            else:
                raw_change = _to_number(_safe_get(info, "regularMarketChangePercent"))
                if raw_change is not None:
                    variation_day = raw_change * 100 if -1 <= raw_change <= 1 else raw_change

            dy_raw = _to_number(_safe_get(info, "dividendYield"))
            if dy_raw is None:
                dy_raw = _to_number(_safe_get(info, "trailingAnnualDividendYield"))
            dy = None if dy_raw is None else (dy_raw * 100 if dy_raw <= 1.5 else dy_raw)

            market_cap = _to_number(_safe_get(info, "marketCap"))
            pl = _to_number(_safe_get(info, "trailingPE")) or _to_number(_safe_get(info, "forwardPE"))
            pvp = _to_number(_safe_get(info, "priceToBook"))

            if any(value is not None for value in [price, pl, pvp, dy]):
                history_variations = {"variation_7d": None, "variation_30d": None}
                try:
                    hist = yf.download(symbol, period="3mo", interval="1d", progress=False, threads=False)
                    history_variations = _history_variations(hist, current_price=price)
                except Exception:
                    pass

                return _metrics_in_brl_if_needed(ticker, {
                    "price": price,
                    "pl": pl,
                    "pvp": pvp,
                    "dy": dy,
                    "variation_day": variation_day,
                    "variation_7d": history_variations["variation_7d"],
                    "variation_30d": history_variations["variation_30d"],
                    "market_cap_bi": (market_cap / 1_000_000_000) if market_cap is not None else None,
                })

            # Fallback mais estavel em momentos de intermitencia do yfinance.
            quote_metrics = _metrics_from_quote(_fetch_yahoo_quote(symbol))
            if quote_metrics:
                return _metrics_in_brl_if_needed(ticker, quote_metrics)

            # Fallback: algumas series retornam vazio em fast_info/info, mas possuem historico.
            try:
                hist = yf.download(symbol, period="5d", interval="1d", progress=False, threads=False)
            except Exception:
                hist = None
            if hist is not None and not hist.empty:
                close_value = _to_number(hist["Close"].dropna().iloc[-1])
                if close_value is not None:
                    history_variations = _history_variations(hist, current_price=close_value)
                    return _metrics_in_brl_if_needed(ticker, {
                        "price": close_value,
                        "pl": pl,
                        "pvp": pvp,
                        "dy": dy,
                        "variation_day": variation_day,
                        "variation_7d": history_variations["variation_7d"],
                        "variation_30d": history_variations["variation_30d"],
                        "market_cap_bi": (market_cap / 1_000_000_000)
                        if market_cap is not None
                        else None,
                    })

    return None


def _has_market_metrics(metrics: dict):
    from . import legacy_compat

    return legacy_compat._has_market_metrics(metrics)


def _market_data_class_key(ticker: str):
    raw_ticker = (ticker or "").strip().upper()
    if _is_crypto_ticker(raw_ticker):
        return "crypto"
    if _is_us_stock_ticker(raw_ticker):
        return "us"
    return "br"


def _providers_from_csv(raw_value: str):
    return [item.strip().lower() for item in (raw_value or "").split(",") if item.strip()]


def _default_market_data_providers(class_key: str):
    defaults = {
        # Yahoo primeiro para reduzir burst/rate-limit no CoinGecko.
        "crypto": ["yahoo", "coingecko"],
        "us": ["twelve_data", "alpha_vantage", "yahoo"],
        "br": ["market_scanner"],
    }
    return list(defaults.get(class_key, ["yahoo"]))


def _market_data_providers_from_env(ticker: str = "", include_scanner_br: bool = True):
    configured = []
    class_key = _market_data_class_key(ticker)

    class_specific_env = {
        "crypto": "MARKET_DATA_PROVIDERS_CRYPTO",
        "us": "MARKET_DATA_PROVIDERS_US",
        "br": "MARKET_DATA_PROVIDERS_BR",
    }
    class_specific_value = os.getenv(class_specific_env.get(class_key, ""), "")
    class_specific = _providers_from_csv(class_specific_value)
    if class_specific:
        configured.extend(class_specific)
    else:
        raw_list = (os.getenv("MARKET_DATA_PROVIDERS") or "").strip()
        configured.extend(_providers_from_csv(raw_list))

        single = (os.getenv("MARKET_DATA_PROVIDER") or "").strip().lower()
        if single:
            configured.append(single)

        # Compatibilidade com a configuracao antiga.
        primary = (os.getenv("MARKET_DATA_PRIMARY") or "").strip().lower()
        fallback = (os.getenv("MARKET_DATA_FALLBACK") or "").strip().lower()
        configured.extend([primary, fallback])

    if not configured:
        configured.extend(_default_market_data_providers(class_key))

    order = []
    for provider in configured:
        if provider in _MARKET_DATA_PROVIDER_CAPABILITIES and provider not in order:
            order.append(provider)

    if not order:
        order.extend(_default_market_data_providers(class_key))

    use_scanner_br = (os.getenv("MARKET_DATA_USE_SCANNER_BR", "1") or "1").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if class_key == "br" and include_scanner_br and use_scanner_br and "market_scanner" not in order:
        order.insert(0, "market_scanner")
    if class_key == "br" and not include_scanner_br:
        order = [provider for provider in order if provider != "market_scanner"]
        if not order:
            order = ["brapi", "yahoo", "google"]
    return order


def _market_data_provider_order(capability: str, ticker: str = "", include_scanner_br: bool = True):
    order = []
    for provider in _market_data_providers_from_env(ticker, include_scanner_br=include_scanner_br):
        capabilities = _MARKET_DATA_PROVIDER_CAPABILITIES.get(provider, set())
        if capability in capabilities and provider not in order:
            order.append(provider)
    return order


def _market_data_provider_label(ticker: str = "", include_scanner_br: bool = True):
    from . import legacy_compat

    return legacy_compat._market_data_provider_label(
        ticker,
        include_scanner_br=include_scanner_br,
    )


def _is_truthy_env(name: str, default: str = "0"):
    from . import legacy_compat

    return legacy_compat._is_truthy_env(name, default=default)


def _fetch_market_profile(ticker: str, include_scanner_br: bool = True):
    from . import legacy_compat

    return legacy_compat._fetch_market_profile(
        ticker,
        include_scanner_br=include_scanner_br,
    )


def _fetch_market_metrics(ticker: str, include_scanner_br: bool = True):
    from . import legacy_compat

    return legacy_compat._fetch_market_metrics(
        ticker,
        include_scanner_br=include_scanner_br,
    )


def _fetch_market_history(ticker: str, range_key: str, include_scanner_br: bool = True):
    from . import legacy_compat

    return legacy_compat._fetch_market_history(
        ticker,
        range_key,
        include_scanner_br=include_scanner_br,
    )


def refresh_asset_market_data(ticker: str, include_scanner_br: bool = True):
    from . import market_data as market_data_services

    return market_data_services.refresh_asset_market_data(
        ticker,
        include_scanner_br=include_scanner_br,
    )


def refresh_all_assets_market_data(attempts: int = 3):
    from . import market_data as market_data_services

    return market_data_services.refresh_all_assets_market_data(attempts=attempts)


def refresh_market_data_for_tickers(tickers, attempts: int = 2):
    from . import market_data as market_data_services

    return market_data_services.refresh_market_data_for_tickers(tickers, attempts=attempts)


def _parse_date(value: str):
    raw_value = (value or "").strip().replace("\u00a0", " ")
    if not raw_value:
        return None

    month_map = {
        "jan": "01",
        "fev": "02",
        "feb": "02",
        "mar": "03",
        "abr": "04",
        "apr": "04",
        "mai": "05",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "ago": "08",
        "aug": "08",
        "set": "09",
        "sep": "09",
        "out": "10",
        "oct": "10",
        "nov": "11",
        "dez": "12",
        "dec": "12",
    }

    raw_lower = raw_value.lower().replace(".", "")
    for name, number in month_map.items():
        raw_lower = raw_lower.replace(f"/{name}/", f"/{number}/")
        raw_lower = raw_lower.replace(f"-{name}-", f"-{number}-")
    raw_value = raw_lower

    # Excel/planilha pode exportar numero de serie de data.
    if raw_value.isdigit():
        try:
            base = datetime(1899, 12, 30)
            parsed = base + timedelta(days=int(raw_value))
            return parsed.strftime("%Y-%m-%d")
        except Exception:
            pass

    date_formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
    )
    for fmt in date_formats:
        try:
            parsed = datetime.strptime(raw_value, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _position_category(ticker: str, name: str, sector: str):
    ticker_up = (ticker or "").upper()
    name_up = (name or "").upper()
    sector_up = (sector or "").upper()

    if (
        "-USD" in ticker_up
        or ticker_up.endswith("USDT")
        or "CRYPTO" in sector_up
        or "CRYPTO" in name_up
    ):
        return "crypto"

    is_fii = (
        ticker_up.endswith("11")
        and (
            "FII" in name_up
            or "IMOBILI" in name_up
            or "REIT" in name_up
            or sector_up in {"REAL ESTATE", "FUNDOS IMOBILIARIOS"}
        )
    )
    if is_fii:
        return "fiis"

    # Heuristica simples para ticker americano (ex.: AAPL, MSFT, GOOGL).
    clean = ticker_up.replace(".", "")
    if clean.isalpha() and len(clean) <= 6:
        return "us_stocks"

    return "br_stocks"


def add_transaction(form_data: dict):
    from . import portfolio as portfolio_services

    return portfolio_services.add_transaction(form_data)


def import_transactions_csv(file_bytes, target_portfolio_id: int):
    from . import portfolio as portfolio_services

    return portfolio_services.import_transactions_csv(file_bytes, target_portfolio_id)


def add_income(form_data: dict):
    from . import portfolio as portfolio_services

    return portfolio_services.add_income(form_data)


def add_fixed_income(form_data: dict):
    from . import portfolio as portfolio_services

    return portfolio_services.add_fixed_income(form_data)


def import_fixed_incomes_csv(file_bytes, target_portfolio_id: int):
    from . import portfolio as portfolio_services

    return portfolio_services.import_fixed_incomes_csv(file_bytes, target_portfolio_id)


def _fixed_income_projection(item):
    aporte_date = datetime.strptime(item["date_aporte"], "%Y-%m-%d").date()
    maturity_date = datetime.strptime(item["maturity_date"], "%Y-%m-%d").date()
    today = datetime.now().date()

    principal = float(item["aporte"]) + float(item["reinvested"])
    total_days = max((maturity_date - aporte_date).days, 1)
    elapsed_days = max(min((today - aporte_date).days, total_days), 0)

    rate_fixed = max(float(item.get("rate_fixed", 0.0)), 0.0)
    rate_ipca = max(float(item.get("rate_ipca", 0.0)), 0.0)
    rate_cdi = max(float(item.get("rate_cdi", 0.0)), 0.0)
    rate_type = (item.get("rate_type") or "").upper()

    # Compatibilidade para registros antigos sem componentes separados.
    if rate_fixed == 0 and rate_ipca == 0 and rate_cdi == 0:
        legacy_rate = max(float(item.get("annual_rate", 0.0)), 0.0)
        if legacy_rate > 0:
            if rate_type == "FIXO":
                rate_fixed = legacy_rate
            elif rate_type == "CDI":
                rate_cdi = legacy_rate
            elif rate_type == "IPCA":
                rate_ipca = legacy_rate
            elif rate_type in {"FIXO+IPCA", "FIXO+CDI"}:
                rate_fixed = legacy_rate

    def _fixed_factor(days: int):
        if days <= 0 or rate_fixed <= 0:
            return 1.0
        return (1 + (rate_fixed / 100.0)) ** (days / 365.0)

    def _annualized_factor(rate: float, days: int):
        if days <= 0 or rate <= 0:
            return 1.0
        return (1 + (rate / 100.0)) ** (days / 365.0)

    def _cdi_factor(start_date, end_date):
        if rate_cdi <= 0 or start_date > end_date:
            return 1.0
        factor, has_data = _compound_from_bcb_series(
            11,
            start_date,
            end_date,
            multiplier=(rate_cdi / 100.0),
            extrapolation_step_days=1.0,
        )
        if has_data:
            return factor
        days = max((end_date - start_date).days, 0)
        return _annualized_factor(rate_cdi, days)

    def _ipca_factor(start_date, end_date):
        if rate_ipca <= 0 or start_date > end_date:
            return 1.0
        factor, has_data = _compound_from_bcb_series(
            433,
            start_date,
            end_date,
            multiplier=(rate_ipca / 100.0),
            extrapolation_step_days=30.0,
        )
        if has_data:
            return factor
        days = max((end_date - start_date).days, 0)
        return _annualized_factor(rate_ipca, days)

    def _factor_for_period(start_date, end_date, days: int):
        fixed_factor = _fixed_factor(days)
        cdi_factor = _cdi_factor(start_date, end_date)
        ipca_factor = _ipca_factor(start_date, end_date)

        if rate_type == "FIXO":
            return fixed_factor
        if rate_type == "CDI":
            return cdi_factor
        if rate_type == "IPCA":
            return ipca_factor
        if rate_type == "FIXO+IPCA":
            return fixed_factor * ipca_factor
        if rate_type == "FIXO+CDI":
            return fixed_factor * cdi_factor
        return fixed_factor * cdi_factor * ipca_factor

    current_end = aporte_date + timedelta(days=elapsed_days)
    final_end = aporte_date + timedelta(days=total_days)
    current_factor = _factor_for_period(aporte_date, current_end, elapsed_days)
    final_factor = _factor_for_period(aporte_date, final_end, total_days)
    current_value = principal * current_factor
    final_value = principal * final_factor
    is_matured = today >= maturity_date
    active_applied_value = 0.0 if is_matured else principal
    active_current_value = 0.0 if is_matured else current_value
    active_current_income = 0.0 if is_matured else (current_value - principal)
    total_received = final_value if is_matured else 0.0
    rendimento = (final_value - principal) if is_matured else 0.0

    projected = dict(item)
    projected["applied_value"] = round(principal, 2)
    projected["active_applied_value"] = round(active_applied_value, 2)
    projected["elapsed_days"] = int(elapsed_days)
    projected["total_days"] = int(total_days)
    projected["is_matured"] = is_matured
    projected["current_gross_value"] = round(active_current_value, 2)
    projected["current_income"] = round(active_current_income, 2)
    projected["final_gross_value"] = round(final_value, 2)
    projected["final_income"] = round(final_value - principal, 2)
    projected["total_received"] = round(total_received, 2)
    projected["rendimento"] = round(rendimento, 2)
    return projected


def get_fixed_incomes(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    from . import portfolio as portfolio_services

    return portfolio_services.get_fixed_incomes(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)

def delete_fixed_incomes(fixed_income_ids, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.delete_fixed_incomes(fixed_income_ids, portfolio_ids)

def get_fixed_income_summary(portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_fixed_income_summary(portfolio_ids)

def get_fixed_income_summary_from_items(items):
    from . import portfolio as portfolio_services

    return portfolio_services.get_fixed_income_summary_from_items(items)

def _memory_cache_get(cache_store, cache_key):
    entry = cache_store.get(cache_key)
    if not entry:
        return None
    expires_at = float(entry.get("expires_at", 0.0) or 0.0)
    if expires_at > 0 and time.time() < expires_at:
        return entry.get("value")
    cache_store.pop(cache_key, None)
    return None


def _memory_cache_set(cache_store, cache_key, value, ttl_seconds: int):
    cache_store[cache_key] = {
        "value": value,
        "expires_at": time.time() + max(int(ttl_seconds), 1),
    }


def _clear_benchmark_cache():
    _BENCHMARK_CACHE.clear()


def invalidate_fixed_income_snapshot(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    try:
        db.execute(
            "DELETE FROM fixed_income_snapshot_items WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.execute(
            "DELETE FROM fixed_income_snapshot_summary WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.commit()
    except Exception:
        db.rollback()


def rebuild_fixed_income_snapshots(portfolio_ids=None):
    from . import portfolio as portfolio_services

    return portfolio_services.rebuild_fixed_income_snapshots(portfolio_ids=portfolio_ids)

def get_fixed_income_payload_cached(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    from . import portfolio as portfolio_services

    return portfolio_services.get_fixed_income_payload_cached(
        portfolio_ids,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )

def get_transactions(portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_transactions(portfolio_ids)

def delete_transactions(transaction_ids, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.delete_transactions(transaction_ids, portfolio_ids)

def get_incomes(portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_incomes(portfolio_ids)

def delete_incomes(income_ids, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.delete_incomes(income_ids, portfolio_ids)

def get_income_totals_by_ticker(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        GROUP BY ticker
        """,
        tuple(pids),
    ).fetchall()

    by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in rows}
    total = round(sum(by_ticker.values()), 2)
    return by_ticker, total


def get_asset_transactions(ticker: str, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_asset_transactions(ticker, portfolio_ids)

def get_asset_incomes(ticker: str, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_asset_incomes(ticker, portfolio_ids)

def get_asset_position_summary(ticker: str, portfolio_ids):
    from . import portfolio as portfolio_services

    return portfolio_services.get_asset_position_summary(ticker, portfolio_ids)

def get_sectors_summary():
    from . import portfolio as portfolio_services

    return portfolio_services.get_sectors_summary()

def get_portfolio_snapshot(portfolio_ids, sort_by: str = "name", sort_dir: str = "asc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            a.ticker,
            a.name,
            a.sector,
            a.logo_url,
            a.market_data_status,
            a.market_data_source,
            a.market_data_updated_at,
            a.market_data_last_attempt_at,
            a.market_data_last_error,
            b.shares,
            a.price,
            a.dy,
            (a.price * b.shares) AS value
        FROM assets a
        JOIN (
            SELECT
                ticker,
                SUM(CASE WHEN tx_type = 'buy' THEN shares ELSE -shares END) AS shares
            FROM transactions
            WHERE portfolio_id IN ("""
        + placeholders
        + """)
            GROUP BY ticker
            HAVING shares > 0
        ) b ON b.ticker = a.ticker
        ORDER BY value DESC
        """,
        tuple(pids),
    ).fetchall()

    positions = []
    total = 0.0
    monthly_dividends = 0.0
    invested_total = 0.0
    incomes_total = 0.0
    incomes_current_month = 0.0
    incomes_3m = 0.0
    incomes_12m = 0.0

    for row in rows:
        item = dict(row)
        total += item["value"]
        monthly_dividends += item["value"] * (item["dy"] / 100) / 12
        positions.append(
            {
                "ticker": item["ticker"],
                "name": item["name"],
                "sector": item["sector"],
                "logo_url": item.get("logo_url", ""),
                "shares": item["shares"],
                "price": item["price"],
                "value": item["value"],
                "market_data": _market_data_meta_from_asset(item),
            }
        )

    # Custo em aberto por ticker (media movel), para calcular resultado em aberto da carteira.
    tx_rows = db.execute(
        """
        SELECT ticker, tx_type, shares, price
        FROM transactions
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY ticker ASC, date ASC, id ASC
        """,
        tuple(pids),
    ).fetchall()
    cost_state = {}
    for tx in tx_rows:
        ticker = tx["ticker"]
        current = cost_state.get(ticker, {"shares": 0, "cost": 0.0})
        shares = current["shares"]
        cost = current["cost"]

        if tx["tx_type"] == "buy":
            shares += tx["shares"]
            cost += tx["shares"] * tx["price"]
        else:
            if shares > 0:
                avg_price = cost / shares
                sell_shares = min(tx["shares"], shares)
                shares -= sell_shares
                cost -= avg_price * sell_shares
                if shares == 0:
                    cost = 0.0

        cost_state[ticker] = {"shares": shares, "cost": cost}

    today = datetime.now().date()
    current_month_start = today.replace(day=1)

    def _subtract_months(date_value, months_back):
        year = date_value.year
        month = date_value.month - months_back
        while month <= 0:
            month += 12
            year -= 1
        return date_value.replace(year=year, month=month, day=1)

    start_3m = _subtract_months(current_month_start, 2)
    start_12m = _subtract_months(current_month_start, 11)

    # Proventos por ticker para as carteiras selecionadas.
    income_rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        GROUP BY ticker
        """,
        tuple(pids),
    ).fetchall()
    incomes_by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in income_rows}
    incomes_total = sum(incomes_by_ticker.values())
    income_current_month_rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
          AND date >= ?
        GROUP BY ticker
        """,
        tuple(pids + [current_month_start.strftime("%Y-%m-%d")]),
    ).fetchall()
    incomes_current_month_by_ticker = {
        row["ticker"]: float(row["total_incomes"]) for row in income_current_month_rows
    }
    income_3m_rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
          AND date >= ?
        GROUP BY ticker
        """,
        tuple(pids + [start_3m.strftime("%Y-%m-%d")]),
    ).fetchall()
    incomes_3m_by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in income_3m_rows}
    income_12m_rows = db.execute(
        """
        SELECT ticker, COALESCE(SUM(amount), 0) AS total_incomes
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
          AND date >= ?
        GROUP BY ticker
        """,
        tuple(pids + [start_12m.strftime("%Y-%m-%d")]),
    ).fetchall()
    incomes_12m_by_ticker = {row["ticker"]: float(row["total_incomes"]) for row in income_12m_rows}
    incomes_summary_row = db.execute(
        """
        SELECT
            COALESCE(SUM(amount), 0) AS total_incomes,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_current_month,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_3m,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_12m
        FROM incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        """,
        tuple(
            [
                current_month_start.strftime("%Y-%m-%d"),
                start_3m.strftime("%Y-%m-%d"),
                start_12m.strftime("%Y-%m-%d"),
            ]
            + pids
        ),
    ).fetchone()
    if incomes_summary_row:
        incomes_total = float(incomes_summary_row["total_incomes"] or incomes_total)
        incomes_current_month = float(incomes_summary_row["incomes_current_month"] or 0.0)
        incomes_3m = float(incomes_summary_row["incomes_3m"] or 0.0)
        incomes_12m = float(incomes_summary_row["incomes_12m"] or 0.0)

    for item in positions:
        invested_total += cost_state.get(item["ticker"], {"cost": 0.0})["cost"]

    grouped_positions = {"br_stocks": [], "us_stocks": [], "crypto": [], "fiis": []}

    for item in positions:
        invested_item = cost_state.get(item["ticker"], {"cost": 0.0})["cost"]
        open_pnl_item = item["value"] - invested_item
        open_pnl_pct_item = (open_pnl_item / invested_item) * 100 if invested_item > 0 else 0.0
        avg_price_item = (invested_item / item["shares"]) if item["shares"] > 0 else 0.0

        item["invested_value"] = round(invested_item, 2)
        item["avg_price"] = round(avg_price_item, 2)
        item["open_pnl_value"] = round(open_pnl_item, 2)
        item["open_pnl_pct"] = round(open_pnl_pct_item, 2)
        item["total_incomes"] = round(incomes_by_ticker.get(item["ticker"], 0.0), 2)
        item["incomes_current_month"] = round(
            incomes_current_month_by_ticker.get(item["ticker"], 0.0), 2
        )
        item["incomes_3m"] = round(incomes_3m_by_ticker.get(item["ticker"], 0.0), 2)
        item["incomes_12m"] = round(incomes_12m_by_ticker.get(item["ticker"], 0.0), 2)
        item["weight"] = round((item["value"] / total) * 100, 2) if total else 0.0
        item["category"] = _position_category(item["ticker"], item["name"], item["sector"])
        grouped_positions[item["category"]].append(item)

    sort_key_map = {
        "ticker": "ticker",
        "name": "name",
        "shares": "shares",
        "price": "price",
        "avg_price": "avg_price",
        "invested_value": "invested_value",
        "value": "value",
        "total_incomes": "total_incomes",
        "open_pnl_value": "open_pnl_value",
        "open_pnl_pct": "open_pnl_pct",
        "weight": "weight",
    }
    safe_sort_by = sort_key_map.get((sort_by or "").strip().lower(), "name")
    safe_sort_dir = "asc" if (sort_dir or "").strip().lower() == "asc" else "desc"
    reverse = safe_sort_dir == "desc"

    def _sort_value(item):
        value = item.get(safe_sort_by)
        if isinstance(value, str):
            return value.upper()
        return value if value is not None else 0

    for key in grouped_positions:
        grouped_positions[key] = sorted(grouped_positions[key], key=_sort_value, reverse=reverse)

    open_pnl_value = total - invested_total
    open_pnl_pct = (open_pnl_value / invested_total) * 100 if invested_total > 0 else 0.0
    group_totals = {
        key: round(sum(item["value"] for item in items), 2)
        for key, items in grouped_positions.items()
    }
    group_summaries = {}
    for key, items in grouped_positions.items():
        group_total = sum(item["value"] for item in items)
        group_invested = sum(item["invested_value"] for item in items)
        group_open_pnl = group_total - group_invested
        group_open_pnl_pct = (group_open_pnl / group_invested) * 100 if group_invested > 0 else 0.0
        group_incomes = sum(item["total_incomes"] for item in items)
        group_incomes_current_month = sum(item["incomes_current_month"] for item in items)
        group_incomes_3m = sum(item["incomes_3m"] for item in items)
        group_incomes_12m = sum(item["incomes_12m"] for item in items)
        group_summaries[key] = {
            "total_value": round(group_total, 2),
            "invested_value": round(group_invested, 2),
            "open_pnl_value": round(group_open_pnl, 2),
            "open_pnl_pct": round(group_open_pnl_pct, 2),
            "incomes_current_month": round(group_incomes_current_month, 2),
            "incomes_3m": round(group_incomes_3m, 2),
            "incomes_12m": round(group_incomes_12m, 2),
            "total_incomes": round(group_incomes, 2),
        }

    tactical_summary = _build_portfolio_tactical_summary(positions, group_summaries, total)

    return {
        "total_value": round(total, 2),
        "invested_value": round(invested_total, 2),
        "monthly_dividends": round(monthly_dividends, 2),
        "total_incomes": round(incomes_total, 2),
        "incomes_current_month": round(incomes_current_month, 2),
        "incomes_3m": round(incomes_3m, 2),
        "incomes_12m": round(incomes_12m, 2),
        "open_pnl_value": round(open_pnl_value, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "positions": positions,
        "grouped_positions": grouped_positions,
        "group_totals": group_totals,
        "group_summaries": group_summaries,
        "tactical_summary": tactical_summary,
        "sort_by": safe_sort_by,
        "sort_dir": safe_sort_dir,
    }


def _build_monthly_class_summary(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    month_names = {
        1: "jan",
        2: "fev",
        3: "mar",
        4: "abr",
        5: "mai",
        6: "jun",
        7: "jul",
        8: "ago",
        9: "set",
        10: "out",
        11: "nov",
        12: "dez",
    }

    def _month_key(date_text: str):
        try:
            parsed = datetime.strptime((date_text or "")[:10], "%Y-%m-%d")
            return parsed.year, parsed.month
        except ValueError:
            return None

    def _category_bucket(ticker: str, name: str, sector: str):
        category = _position_category(ticker, name, sector)
        if category == "br_stocks":
            return "br"
        if category == "us_stocks":
            return "us"
        if category == "fiis":
            return "fii"
        if category == "crypto":
            return "cripto"
        return None

    rows_map = {}
    month_set = set()

    def _ensure_month_entry(month_key):
        if month_key not in rows_map:
            rows_map[month_key] = {
                "br_invested": 0.0,
                "br_incomes": 0.0,
                "us_invested": 0.0,
                "us_incomes": 0.0,
                "fii_invested": 0.0,
                "fii_incomes": 0.0,
                "fixa_invested": 0.0,
                "fixa_incomes": 0.0,
                "cripto_invested": 0.0,
                "cripto_incomes": 0.0,
            }
        month_set.add(month_key)

    tx_rows = db.execute(
        """
        SELECT
            t.date,
            t.tx_type,
            (t.shares * t.price) AS amount,
            a.ticker,
            a.name,
            a.sector
        FROM transactions t
        JOIN assets a ON a.ticker = t.ticker
        WHERE t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date ASC, t.id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in tx_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        bucket = _category_bucket(row["ticker"], row["name"], row["sector"])
        if not bucket:
            continue
        _ensure_month_entry(month_key)
        # "Investidos" segue aporte de compras no mes.
        if row["tx_type"] == "buy":
            rows_map[month_key][f"{bucket}_invested"] += float(row["amount"] or 0.0)

    income_rows = db.execute(
        """
        SELECT
            i.date,
            i.amount,
            a.ticker,
            a.name,
            a.sector
        FROM incomes i
        JOIN assets a ON a.ticker = i.ticker
        WHERE i.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date ASC, i.id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in income_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        bucket = _category_bucket(row["ticker"], row["name"], row["sector"])
        if not bucket:
            continue
        _ensure_month_entry(month_key)
        rows_map[month_key][f"{bucket}_incomes"] += float(row["amount"] or 0.0)

    fixed_rows = db.execute(
        """
        SELECT
            id,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            maturity_date,
            aporte,
            reinvested
        FROM fixed_incomes
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY date_aporte ASC, id ASC
        """,
        tuple(pids),
    ).fetchall()
    for row in fixed_rows:
        item = dict(row)
        aporte_month = _month_key(item["date_aporte"])
        if aporte_month:
            _ensure_month_entry(aporte_month)
            rows_map[aporte_month]["fixa_invested"] += float(item.get("aporte") or 0.0) + float(
                item.get("reinvested") or 0.0
            )

        maturity_month = _month_key(item["maturity_date"])
        if maturity_month:
            projected = _fixed_income_projection(item)
            _ensure_month_entry(maturity_month)
            rows_map[maturity_month]["fixa_incomes"] += float(projected.get("final_income") or 0.0)

    if not month_set:
        return []

    ordered_months = sorted(month_set)
    result = []
    for year, month in ordered_months:
        values = rows_map[(year, month)]
        total_invested = (
            values["br_invested"]
            + values["us_invested"]
            + values["fii_invested"]
            + values["fixa_invested"]
            + values["cripto_invested"]
        )
        total_incomes = (
            values["br_incomes"]
            + values["us_incomes"]
            + values["fii_incomes"]
            + values["fixa_incomes"]
            + values["cripto_incomes"]
        )
        result.append(
            {
                "label": f"{month_names[month]}/{str(year)[2:]}",
                "br_invested": round(values["br_invested"], 2),
                "br_incomes": round(values["br_incomes"], 2),
                "us_invested": round(values["us_invested"], 2),
                "us_incomes": round(values["us_incomes"], 2),
                "fii_invested": round(values["fii_invested"], 2),
                "fii_incomes": round(values["fii_incomes"], 2),
                "fixa_invested": round(values["fixa_invested"], 2),
                "fixa_incomes": round(values["fixa_incomes"], 2),
                "cripto_invested": round(values["cripto_invested"], 2),
                "cripto_incomes": round(values["cripto_incomes"], 2),
                "total_invested": round(total_invested, 2),
                "total_incomes": round(total_incomes, 2),
            }
        )
    return result


def _build_monthly_ticker_summary(portfolio_ids, months=24):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    try:
        months = int(months)
    except (TypeError, ValueError):
        months = 8
    months = max(1, min(months, 24))

    month_names = {
        1: "jan.",
        2: "fev.",
        3: "mar.",
        4: "abr.",
        5: "mai.",
        6: "jun.",
        7: "jul.",
        8: "ago.",
        9: "set.",
        10: "out.",
        11: "nov.",
        12: "dez.",
    }

    def _month_key(date_text: str):
        try:
            parsed = datetime.strptime((date_text or "")[:10], "%Y-%m-%d")
            return f"{parsed.year:04d}-{parsed.month:02d}"
        except ValueError:
            return None

    def _month_label(month_key: str):
        try:
            year, month = month_key.split("-", 1)
            month_num = int(month)
            return f"{month_names.get(month_num, month)} / {str(year)[2:]}"
        except (ValueError, TypeError):
            return month_key

    ticker_month_map = {}
    month_totals = {}
    month_set = set()
    ticker_name_map = {}

    def _ensure_values(month_map, month_key):
        if month_key not in month_map:
            month_map[month_key] = {"invested": 0.0, "incomes": 0.0}

    tx_rows = db.execute(
        """
        SELECT
            t.date,
            t.ticker,
            t.tx_type,
            (t.shares * t.price) AS amount,
            a.name
        FROM transactions t
        LEFT JOIN assets a ON a.ticker = t.ticker
        WHERE t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date ASC, t.id ASC
        """,
        tuple(pids),
    ).fetchall()

    for row in tx_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        if (row["tx_type"] or "").lower() != "buy":
            continue
        ticker = (row["ticker"] or "").strip().upper()
        if not ticker:
            continue

        amount = float(row["amount"] or 0.0)
        ticker_name_map[ticker] = (row["name"] or ticker).strip() or ticker

        ticker_values = ticker_month_map.setdefault(ticker, {})
        _ensure_values(ticker_values, month_key)
        ticker_values[month_key]["invested"] += amount

        _ensure_values(month_totals, month_key)
        month_totals[month_key]["invested"] += amount
        month_set.add(month_key)

    income_rows = db.execute(
        """
        SELECT
            i.date,
            i.ticker,
            i.amount,
            a.name
        FROM incomes i
        LEFT JOIN assets a ON a.ticker = i.ticker
        WHERE i.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date ASC, i.id ASC
        """,
        tuple(pids),
    ).fetchall()

    for row in income_rows:
        month_key = _month_key(row["date"])
        if not month_key:
            continue
        ticker = (row["ticker"] or "").strip().upper()
        if not ticker:
            continue

        amount = float(row["amount"] or 0.0)
        ticker_name_map[ticker] = (row["name"] or ticker).strip() or ticker

        ticker_values = ticker_month_map.setdefault(ticker, {})
        _ensure_values(ticker_values, month_key)
        ticker_values[month_key]["incomes"] += amount

        _ensure_values(month_totals, month_key)
        month_totals[month_key]["incomes"] += amount
        month_set.add(month_key)

    if not month_set:
        return {"months": [], "totals": [], "rows": []}

    ordered_months = sorted(month_set)
    if len(ordered_months) > months:
        ordered_months = ordered_months[-months:]
    selected_months = set(ordered_months)

    month_items = [{"key": key, "label": _month_label(key)} for key in ordered_months]

    totals = []
    for key in ordered_months:
        values = month_totals.get(key, {"invested": 0.0, "incomes": 0.0})
        totals.append(
            {
                "month_key": key,
                "invested": round(float(values.get("invested", 0.0)), 2),
                "incomes": round(float(values.get("incomes", 0.0)), 2),
            }
        )

    rows = []
    for ticker in sorted(ticker_month_map.keys()):
        per_month = ticker_month_map[ticker]
        month_values = {}
        row_invested = 0.0
        row_incomes = 0.0

        for key in ordered_months:
            values = per_month.get(key, {"invested": 0.0, "incomes": 0.0})
            invested = round(float(values.get("invested", 0.0)), 2)
            incomes = round(float(values.get("incomes", 0.0)), 2)
            month_values[key] = {"invested": invested, "incomes": incomes}
            row_invested += invested
            row_incomes += incomes

        has_any_value = any(
            key in selected_months
            and (
                abs(float(per_month.get(key, {}).get("invested", 0.0))) > 0
                or abs(float(per_month.get(key, {}).get("incomes", 0.0))) > 0
            )
            for key in per_month.keys()
        )
        if not has_any_value:
            continue

        rows.append(
            {
                "ticker": ticker,
                "name": ticker_name_map.get(ticker, ticker),
                "total_invested": round(row_invested, 2),
                "total_incomes": round(row_incomes, 2),
                "months": month_values,
            }
        )

    return {"months": month_items, "totals": totals, "rows": rows}


def _month_label_sort_key(label: str):
    raw = (label or "").strip().lower()
    if "/" not in raw:
        return (0, 0)
    month_key, year_short = raw.split("/", 1)
    month_order = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }
    try:
        return (2000 + int(year_short), month_order.get(month_key, 0))
    except ValueError:
        return (0, 0)


def _combine_monthly_class_rows(parts):
    metric_keys = (
        "br_invested",
        "br_incomes",
        "us_invested",
        "us_incomes",
        "fii_invested",
        "fii_incomes",
        "fixa_invested",
        "fixa_incomes",
        "cripto_invested",
        "cripto_incomes",
    )
    rows_map = {}
    for rows in parts:
        for row in rows or []:
            label = row.get("label")
            if not label:
                continue
            if label not in rows_map:
                rows_map[label] = {"label": label}
                for key in metric_keys:
                    rows_map[label][key] = 0.0
            for key in metric_keys:
                rows_map[label][key] += float(row.get(key, 0.0) or 0.0)

    result = []
    for label in sorted(rows_map.keys(), key=_month_label_sort_key):
        values = rows_map[label]
        total_invested = (
            values["br_invested"]
            + values["us_invested"]
            + values["fii_invested"]
            + values["fixa_invested"]
            + values["cripto_invested"]
        )
        total_incomes = (
            values["br_incomes"]
            + values["us_incomes"]
            + values["fii_incomes"]
            + values["fixa_incomes"]
            + values["cripto_incomes"]
        )
        result.append(
            {
                "label": label,
                "br_invested": round(values["br_invested"], 2),
                "br_incomes": round(values["br_incomes"], 2),
                "us_invested": round(values["us_invested"], 2),
                "us_incomes": round(values["us_incomes"], 2),
                "fii_invested": round(values["fii_invested"], 2),
                "fii_incomes": round(values["fii_incomes"], 2),
                "fixa_invested": round(values["fixa_invested"], 2),
                "fixa_incomes": round(values["fixa_incomes"], 2),
                "cripto_invested": round(values["cripto_invested"], 2),
                "cripto_incomes": round(values["cripto_incomes"], 2),
                "total_invested": round(total_invested, 2),
                "total_incomes": round(total_incomes, 2),
            }
        )
    return result


def _trim_monthly_ticker_summary(payload, months=8):
    if not payload:
        return {"months": [], "totals": [], "rows": []}
    try:
        months = int(months)
    except (TypeError, ValueError):
        months = 8
    months = max(1, min(months, 24))

    ordered_months = list(payload.get("months") or [])
    if len(ordered_months) > months:
        ordered_months = ordered_months[-months:]
    month_keys = [item.get("key") for item in ordered_months if item.get("key")]

    totals_map = {
        item.get("month_key"): {
            "invested": round(float(item.get("invested", 0.0) or 0.0), 2),
            "incomes": round(float(item.get("incomes", 0.0) or 0.0), 2),
        }
        for item in (payload.get("totals") or [])
        if item.get("month_key")
    }
    totals = [
        {
            "month_key": key,
            "invested": totals_map.get(key, {}).get("invested", 0.0),
            "incomes": totals_map.get(key, {}).get("incomes", 0.0),
        }
        for key in month_keys
    ]

    rows = []
    for row in payload.get("rows") or []:
        per_month = {}
        total_invested = 0.0
        total_incomes = 0.0
        source_months = row.get("months") or {}
        for key in month_keys:
            values = source_months.get(key) or {}
            invested = round(float(values.get("invested", 0.0) or 0.0), 2)
            incomes = round(float(values.get("incomes", 0.0) or 0.0), 2)
            per_month[key] = {"invested": invested, "incomes": incomes}
            total_invested += invested
            total_incomes += incomes
        has_any_value = any(
            abs(float(values.get("invested", 0.0) or 0.0)) > 0
            or abs(float(values.get("incomes", 0.0) or 0.0)) > 0
            for values in per_month.values()
        )
        if not has_any_value:
            continue
        rows.append(
            {
                "ticker": row.get("ticker", ""),
                "name": row.get("name", row.get("ticker", "")),
                "total_invested": round(total_invested, 2),
                "total_incomes": round(total_incomes, 2),
                "months": per_month,
            }
        )

    rows.sort(key=lambda item: str(item.get("ticker", "")).upper())
    return {"months": ordered_months, "totals": totals, "rows": rows}


def _combine_monthly_ticker_summaries(parts, months=8):
    month_label_map = {}
    month_totals = {}
    ticker_rows = {}

    for payload in parts:
        for month in payload.get("months") or []:
            month_key = month.get("key")
            if month_key:
                month_label_map[month_key] = month.get("label") or month_key

        for total in payload.get("totals") or []:
            month_key = total.get("month_key")
            if not month_key:
                continue
            state = month_totals.setdefault(month_key, {"invested": 0.0, "incomes": 0.0})
            state["invested"] += float(total.get("invested", 0.0) or 0.0)
            state["incomes"] += float(total.get("incomes", 0.0) or 0.0)

        for row in payload.get("rows") or []:
            ticker = (row.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            state = ticker_rows.setdefault(
                ticker,
                {"ticker": ticker, "name": row.get("name", ticker), "months": {}},
            )
            for month_key, values in (row.get("months") or {}).items():
                month_state = state["months"].setdefault(month_key, {"invested": 0.0, "incomes": 0.0})
                month_state["invested"] += float(values.get("invested", 0.0) or 0.0)
                month_state["incomes"] += float(values.get("incomes", 0.0) or 0.0)

    ordered_month_keys = sorted(month_label_map.keys())
    merged = {
        "months": [{"key": key, "label": month_label_map.get(key, key)} for key in ordered_month_keys],
        "totals": [
            {
                "month_key": key,
                "invested": round(float(month_totals.get(key, {}).get("invested", 0.0)), 2),
                "incomes": round(float(month_totals.get(key, {}).get("incomes", 0.0)), 2),
            }
            for key in ordered_month_keys
        ],
        "rows": [],
    }
    for ticker in sorted(ticker_rows.keys()):
        row = ticker_rows[ticker]
        merged["rows"].append(
            {
                "ticker": ticker,
                "name": row.get("name", ticker),
                "months": {
                    key: {
                        "invested": round(float(values.get("invested", 0.0) or 0.0), 2),
                        "incomes": round(float(values.get("incomes", 0.0) or 0.0), 2),
                    }
                    for key, values in (row.get("months") or {}).items()
                },
            }
        )
    return _trim_monthly_ticker_summary(merged, months=months)


def invalidate_chart_snapshots(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    try:
        db.execute(
            "DELETE FROM chart_snapshot_monthly_class WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.execute(
            "DELETE FROM chart_snapshot_monthly_ticker WHERE portfolio_id IN (" + placeholders + ")",
            tuple(pids),
        )
        db.commit()
    except Exception:
        db.rollback()
    _clear_benchmark_cache()


def rebuild_chart_snapshots(portfolio_ids=None):
    if portfolio_ids is None:
        pids = _all_portfolio_ids()
    else:
        pids = normalize_portfolio_ids(portfolio_ids)
    if not pids:
        return {"portfolios": 0}

    db = get_db()
    stamp = _snapshot_now()
    for pid in pids:
        monthly_class = _build_monthly_class_summary([pid])
        monthly_ticker = _build_monthly_ticker_summary([pid], months=24)
        try:
            db.execute(
                """
                INSERT INTO chart_snapshot_monthly_class (portfolio_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(portfolio_id) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (pid, json.dumps(monthly_class, ensure_ascii=False), stamp),
            )
            db.execute(
                """
                INSERT INTO chart_snapshot_monthly_ticker (portfolio_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(portfolio_id) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (pid, json.dumps(monthly_ticker, ensure_ascii=False), stamp),
            )
        except Exception:
            db.rollback()
            raise
    db.commit()
    return {"portfolios": len(pids)}


def get_monthly_class_summary(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    max_age_seconds = int(current_app.config.get("CHART_SNAPSHOT_MAX_AGE_SECONDS", 900))
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    try:
        rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM chart_snapshot_monthly_class
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()
        if len(rows) != len(pids):
            raise RuntimeError("snapshot_miss")
        parts = []
        for row in rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            parts.append(json.loads(row["payload_json"] or "[]"))
        return _combine_monthly_class_rows(parts)
    except Exception:
        return _build_monthly_class_summary(pids)


def get_monthly_ticker_summary(portfolio_ids, months=8):
    pids = normalize_portfolio_ids(portfolio_ids)
    max_age_seconds = int(current_app.config.get("CHART_SNAPSHOT_MAX_AGE_SECONDS", 900))
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    try:
        rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM chart_snapshot_monthly_ticker
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()
        if len(rows) != len(pids):
            raise RuntimeError("snapshot_miss")
        parts = []
        for row in rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            parts.append(json.loads(row["payload_json"] or "{}"))
        return _combine_monthly_ticker_summaries(parts, months=months)
    except Exception:
        return _build_monthly_ticker_summary(pids, months=months)


def _subtract_months_from_date(date_value, months_back: int):
    year = date_value.year
    month = date_value.month - months_back
    while month <= 0:
        month += 12
        year -= 1
    return date_value.replace(year=year, month=month, day=1)


def _benchmark_range_config(range_key: str):
    normalized = (range_key or "12m").strip().lower()
    mapping = {
        "6m": {"months": 6, "period": "6mo"},
        "12m": {"months": 12, "period": "1y"},
        "24m": {"months": 24, "period": "2y"},
        "60m": {"months": 60, "period": "5y"},
    }
    if normalized not in mapping:
        normalized = "12m"
    cfg = mapping[normalized]
    return normalized, cfg["months"], cfg["period"]


def _month_keys_back(months: int):
    today = datetime.now().date().replace(day=1)
    keys = []
    for offset in range(months - 1, -1, -1):
        dt = _subtract_months_from_date(today, offset)
        keys.append(f"{dt.year:04d}-{dt.month:02d}")
    return keys


def _month_label(month_key: str):
    try:
        dt = datetime.strptime(month_key + "-01", "%Y-%m-%d")
        return dt.strftime("%m/%y")
    except Exception:
        return month_key


def _download_monthly_close_map(symbol: str, period: str):
    if yf is None:
        return {}
    cache_ttl = int(current_app.config.get("YAHOO_MONTHLY_CACHE_TTL_SECONDS", 21600))
    cache_key = ((symbol or "").strip().upper(), (period or "").strip().lower())
    cached = _memory_cache_get(_YAHOO_MONTHLY_CACHE, cache_key)
    if cached is not None:
        return dict(cached)

    def _to_map(hist):
        closes = _extract_close_series(hist)
        if closes is None:
            return {}
        try:
            series = closes.dropna()
        except Exception:
            series = closes
        result = {}
        try:
            for idx, value in series.items():
                close_value = _to_number(value)
                if close_value is None:
                    continue
                dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                key = f"{dt.year:04d}-{dt.month:02d}"
                # Mantem o ultimo valor do mes.
                result[key] = float(close_value)
        except Exception:
            return {}
        return result

    try:
        hist = yf.download(
            symbol,
            period=period,
            interval="1mo",
            progress=False,
            threads=False,
            auto_adjust=True,
        )
    except Exception:
        hist = None
    month_map = _to_map(hist)
    if month_map:
        _memory_cache_set(_YAHOO_MONTHLY_CACHE, cache_key, dict(month_map), cache_ttl)
        return month_map

    try:
        hist_daily = yf.download(
            symbol,
            period=period,
            interval="1d",
            progress=False,
            threads=False,
            auto_adjust=True,
        )
    except Exception:
        hist_daily = None
    month_map = _to_map(hist_daily)
    if month_map:
        _memory_cache_set(_YAHOO_MONTHLY_CACHE, cache_key, dict(month_map), cache_ttl)
    return month_map


def _levels_from_month_map(month_keys, month_map):
    levels = []
    last_value = None
    for key in month_keys:
        if key in month_map:
            last_value = month_map[key]
        levels.append(last_value)
    return levels


def _cumulative_pct_from_levels(levels):
    base = None
    for value in levels:
        if value not in (None, 0):
            base = float(value)
            break
    if base is None:
        return [None for _ in levels]
    result = []
    for value in levels:
        if value is None:
            result.append(None)
        else:
            result.append(((float(value) / base) - 1.0) * 100.0)
    return result


def _cdi_monthly_cumulative(month_keys):
    if not month_keys:
        return [None]
    start_date = month_keys[0] + "-01"
    end_date = datetime.now().strftime("%Y-%m-%d")
    series = _fetch_bcb_series(12, start_date, end_date)  # CDI diario
    if not series:
        return [None for _ in month_keys]

    month_factor = {}
    factor = 1.0
    for date_text, daily_rate in series:
        factor *= 1.0 + (float(daily_rate) / 100.0)
        month_key = date_text[:7]
        month_factor[month_key] = factor

    levels = []
    last_factor = 1.0
    for key in month_keys:
        if key in month_factor:
            last_factor = month_factor[key]
        levels.append(last_factor)
    return _cumulative_pct_from_levels(levels)


def _portfolio_monthly_cumulative(snapshot, month_keys, period: str, scope_key: str):
    positions = snapshot.get("positions", [])
    if not positions:
        return [None for _ in month_keys]

    selected_categories = {
        "all": {"br_stocks", "us_stocks", "fiis", "crypto"},
        "br": {"br_stocks"},
        "us": {"us_stocks"},
        "fiis": {"fiis"},
        "crypto": {"crypto"},
    }.get(scope_key, {"br_stocks", "us_stocks", "fiis", "crypto"})

    selected_positions = []
    for item in positions:
        category = _position_category(item.get("ticker"), item.get("name"), item.get("sector"))
        if category in selected_categories:
            selected_positions.append(item)
    if not selected_positions:
        return [None for _ in month_keys]

    usdbrl_map = _download_monthly_close_map("USDBRL=X", period)
    if not usdbrl_map:
        usdbrl_map = _download_monthly_close_map("BRL=X", period)
    usdbrl_levels = _levels_from_month_map(month_keys, usdbrl_map) if usdbrl_map else []

    total_value = sum(float(item.get("value", 0.0) or 0.0) for item in selected_positions)
    if total_value <= 0:
        return [None for _ in month_keys]

    weighted_rel = [0.0 for _ in month_keys]
    weights_used = [0.0 for _ in month_keys]

    for item in selected_positions:
        ticker = (item.get("ticker") or "").upper()
        position_value = float(item.get("value", 0.0) or 0.0)
        if position_value <= 0:
            continue
        weight = position_value / total_value

        month_map = {}
        for symbol in _candidate_yahoo_symbols(ticker):
            month_map = _download_monthly_close_map(symbol, period)
            if month_map:
                break
        if not month_map:
            continue

        levels = _levels_from_month_map(month_keys, month_map)
        if _is_usd_quoted_ticker(ticker) and usdbrl_levels:
            converted_levels = []
            for idx, value in enumerate(levels):
                fx = usdbrl_levels[idx] if idx < len(usdbrl_levels) else None
                converted_levels.append((value * fx) if (value is not None and fx is not None) else None)
            levels = converted_levels

        base = next((value for value in levels if value not in (None, 0)), None)
        if base in (None, 0):
            continue
        for idx, value in enumerate(levels):
            if value is None:
                continue
            rel = float(value) / float(base)
            weighted_rel[idx] += weight * rel
            weights_used[idx] += weight

    series = []
    for idx in range(len(month_keys)):
        if weights_used[idx] <= 0:
            series.append(None)
            continue
        normalized_rel = weighted_rel[idx] / weights_used[idx]
        series.append((normalized_rel - 1.0) * 100.0)
    return series


def get_benchmark_comparison(portfolio_ids, range_key: str = "12m", scope_key: str = "all"):
    normalized_range, months, period = _benchmark_range_config(range_key)
    valid_scopes = {"all", "br", "us", "fiis", "crypto"}
    normalized_scope = scope_key if scope_key in valid_scopes else "all"
    pids = tuple(sorted(normalize_portfolio_ids(portfolio_ids)))
    cache_ttl = int(current_app.config.get("BENCHMARK_CACHE_TTL_SECONDS", 900))
    cache_key = (pids, normalized_range, normalized_scope)
    cached = _memory_cache_get(_BENCHMARK_CACHE, cache_key)
    if cached is not None:
        return cached

    month_keys = _month_keys_back(months)
    labels = [_month_label(key) for key in month_keys]
    snapshot = get_portfolio_snapshot(pids)

    portfolio_series = _portfolio_monthly_cumulative(snapshot, month_keys, period, normalized_scope)
    cdi_series = _cdi_monthly_cumulative(month_keys)
    ibov_series = _cumulative_pct_from_levels(
        _levels_from_month_map(month_keys, _download_monthly_close_map("^BVSP", period))
    )
    sp500_levels = _levels_from_month_map(month_keys, _download_monthly_close_map("^GSPC", period))
    usdbrl_levels = _levels_from_month_map(month_keys, _download_monthly_close_map("USDBRL=X", period))
    if not any(value is not None for value in usdbrl_levels):
        usdbrl_levels = _levels_from_month_map(month_keys, _download_monthly_close_map("BRL=X", period))
    sp500_brl_levels = []
    for idx in range(len(month_keys)):
        sp = sp500_levels[idx] if idx < len(sp500_levels) else None
        fx = usdbrl_levels[idx] if idx < len(usdbrl_levels) else None
        sp500_brl_levels.append((sp * fx) if (sp is not None and fx is not None) else None)
    sp500_brl_series = _cumulative_pct_from_levels(sp500_brl_levels)

    def _round_series(values):
        return [None if value is None else round(float(value), 2) for value in values]

    result = {
        "labels": labels,
        "datasets": [
            {"label": "Rentabilidade", "values": _round_series(portfolio_series), "color": "#6f8fe7"},
            {"label": "CDI", "values": _round_series(cdi_series), "color": "#e2a72e"},
            {"label": "IBOV", "values": _round_series(ibov_series), "color": "#8e939b"},
            {"label": "S&P500 BRL", "values": _round_series(sp500_brl_series), "color": "#b8bdc6"},
        ],
        "range_key": normalized_range,
        "scope_key": normalized_scope,
    }
    _memory_cache_set(_BENCHMARK_CACHE, cache_key, result, cache_ttl)
    return result

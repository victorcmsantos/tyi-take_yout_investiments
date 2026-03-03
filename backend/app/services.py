import csv
import io
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import current_app, has_request_context

from .auth import get_current_user
from .db import get_db
from .openclaw_client import OpenClawError, invoke_tool

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

_FX_CACHE = {"usdbrl": None, "expires_at": 0.0}
_BCB_SERIES_CACHE = {}
_COINGECKO_CACHE = {}
_TWELVE_DATA_CACHE = {}
_ALPHA_VANTAGE_CACHE = {}
_YAHOO_MONTHLY_CACHE = {}
_BENCHMARK_CACHE = {}
_LOGGER = logging.getLogger(__name__)
_BRAPI_DIAG = {
    "missing_token_logged": False,
    "empty_payload_tickers": set(),
    "empty_results_tickers": set(),
}
_MARKET_DATA_PROVIDER_CAPABILITIES = {
    "alpha_vantage": {"metrics", "profile", "history"},
    "brapi": {"metrics", "profile", "history"},
    "coingecko": {"metrics", "profile", "history"},
    "twelve_data": {"metrics", "history"},
    "google": {"metrics"},
    "yahoo": {"metrics", "profile", "history"},
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


def _serialize_asset(asset):
    if not asset:
        return None
    item = dict(asset)
    item["market_data"] = _market_data_meta_from_asset(item)
    return item


def _mark_asset_market_data_failed(ticker: str, error_message: str):
    db = get_db()
    db.execute(
        """
        UPDATE assets
        SET
            market_data_status = 'stale',
            market_data_last_attempt_at = ?,
            market_data_last_error = ?
        WHERE ticker = ?
        """,
        (_now_iso(), (error_message or "").strip(), (ticker or "").strip().upper()),
    )
    db.commit()


def get_top_assets():
    db = get_db()
    rows = db.execute("SELECT * FROM assets ORDER BY market_cap_bi DESC").fetchall()
    return [_serialize_asset(row) for row in rows]


def _current_user_id():
    user = get_current_user()
    if not user or user.get("is_admin"):
        return None
    return int(user["id"])


def get_asset(ticker: str):
    db = get_db()
    row = db.execute(
        "SELECT * FROM assets WHERE ticker = ?",
        (ticker.upper(),),
    ).fetchone()
    return _serialize_asset(row)

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


def upsert_asset_enrichment(ticker: str, payload: dict | None, raw_reply: str):
    if not ticker:
        return False
    db = get_db()
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
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
    db.commit()
    return True


def _extract_json_from_text(text: str):
    if not text:
        return None
    raw = text.strip()
    if raw.startswith("```"):
        # Strip fenced code blocks if present.
        raw = raw.strip("`")
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    candidate = raw[start : end + 1]
    try:
        return json.loads(candidate)
    except Exception:
        return None


def _normalize_enrichment_list(value):
    if not isinstance(value, list):
        return []
    items = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _normalize_asset_enrichment_payload(payload):
    if not isinstance(payload, dict):
        return None
    return {
        "resumo": str(payload.get("resumo") or "").strip(),
        "modelo_de_negocio": str(payload.get("modelo_de_negocio") or "").strip(),
        "tese": _normalize_enrichment_list(payload.get("tese")),
        "riscos": _normalize_enrichment_list(payload.get("riscos")),
        "dividendos": str(payload.get("dividendos") or "").strip(),
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


def enrich_asset_with_openclaw(ticker: str):
    ticker_norm = (ticker or "").strip().upper()
    if not ticker_norm:
        return False, "Ticker e obrigatorio.", None

    asset = get_asset(ticker_norm)
    if not asset:
        return False, "Ativo nao encontrado.", None

    prompt = (
        f"Ticker: {ticker_norm}. "
        f"Nome: {asset.get('name') or ''}. "
        f"Setor: {asset.get('sector') or ''}. "
        "Responda APENAS JSON valido com as chaves resumo, modelo_de_negocio, tese, riscos, dividendos, observacoes. "
        "Se nao souber, use string vazia ou lista vazia. "
        "Seja conciso e nao invente numeros precisos."
    )

    try:
        result = invoke_tool(
            "sessions_send",
            {
                "sessionKey": "main",
                "message": prompt,
                "timeoutSeconds": 120,
            },
            timeout_seconds=150,
        )
    except OpenClawError as exc:
        return False, str(exc), None

    if not isinstance(result, dict):
        return False, "Resposta inesperada do OpenClaw.", None

    reply = _extract_openclaw_reply(result)
    parsed = _normalize_asset_enrichment_payload(_extract_json_from_text(reply))
    if not parsed:
        upsert_asset_enrichment(ticker_norm, {}, reply)
        return True, "OpenClaw respondeu, mas nao retornou JSON valido. Exibindo resposta bruta.", get_asset_enrichment(ticker_norm)

    upsert_asset_enrichment(ticker_norm, parsed, reply)
    return True, "OK", get_asset_enrichment(ticker_norm)


def get_portfolios():
    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None and not has_request_context():
        rows = db.execute("SELECT id, name FROM portfolios ORDER BY id ASC").fetchall()
    elif current_user_id is None:
        return []
    else:
        rows = db.execute(
            "SELECT id, name FROM portfolios WHERE user_id = ? ORDER BY id ASC",
            (current_user_id,),
        ).fetchall()
    return [dict(row) for row in rows]


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
    if isinstance(raw_ids, int):
        raw_values = [raw_ids]
    elif isinstance(raw_ids, (list, tuple, set)):
        raw_values = list(raw_ids)
    else:
        raw_values = [raw_ids]

    portfolios = get_portfolios()
    valid_ids = {int(item["id"]) for item in portfolios}
    result = []

    for value in raw_values:
        if value in (None, ""):
            continue
        try:
            pid = int(value)
        except (TypeError, ValueError):
            continue
        if pid in valid_ids and pid not in result:
            result.append(pid)

    if not result:
        default_portfolio_id = get_default_portfolio_id()
        result = [default_portfolio_id] if default_portfolio_id is not None else []
    return result


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
    default_portfolio_id = get_default_portfolio_id()
    if raw_portfolio_id in (None, ""):
        return default_portfolio_id
    try:
        pid = int(raw_portfolio_id)
    except (TypeError, ValueError):
        return default_portfolio_id
    return pid if get_portfolio(pid) else default_portfolio_id


def create_portfolio(name: str):
    clean_name = (name or "").strip()
    if not clean_name:
        return False, "Nome da carteira e obrigatorio."
    current_user_id = _current_user_id()
    if current_user_id is None:
        return False, "Usuario sem contexto de carteira."
    db = get_db()
    existing = db.execute(
        "SELECT id FROM portfolios WHERE user_id = ? AND LOWER(name) = LOWER(?)",
        (current_user_id, clean_name),
    ).fetchone()
    if existing:
        return False, "Ja existe uma carteira com esse nome."
    db.execute("INSERT INTO portfolios (user_id, name) VALUES (?, ?)", (current_user_id, clean_name))
    db.commit()
    row = db.execute(
        "SELECT id FROM portfolios WHERE user_id = ? AND name = ?",
        (current_user_id, clean_name),
    ).fetchone()
    return True, int(row["id"])


def delete_portfolio(portfolio_id):
    try:
        pid = int(portfolio_id)
    except (TypeError, ValueError):
        return False, "Carteira invalida."

    db = get_db()
    current_user_id = _current_user_id()
    if current_user_id is None:
        return False, "Usuario sem contexto de carteira."
    portfolio = db.execute(
        "SELECT id, name FROM portfolios WHERE id = ? AND user_id = ?",
        (pid, current_user_id),
    ).fetchone()
    if not portfolio:
        return False, "Carteira nao encontrada."

    total_row = db.execute(
        "SELECT COUNT(*) AS total FROM portfolios WHERE user_id = ?",
        (current_user_id,),
    ).fetchone()
    total_portfolios = int(total_row["total"]) if total_row else 0
    if total_portfolios <= 1:
        return False, "Nao e possivel remover a unica carteira. Crie outra primeiro."

    tx_row = db.execute(
        "SELECT COUNT(*) AS total FROM transactions WHERE portfolio_id = ?",
        (pid,),
    ).fetchone()
    in_row = db.execute(
        "SELECT COUNT(*) AS total FROM incomes WHERE portfolio_id = ?",
        (pid,),
    ).fetchone()
    tx_total = int(tx_row["total"]) if tx_row else 0
    in_total = int(in_row["total"]) if in_row else 0
    if tx_total > 0 or in_total > 0:
        return (
            False,
            "Carteira com lancamentos nao pode ser removida. Remova transacoes/proventos primeiro.",
        )

    db.execute("DELETE FROM chart_snapshot_monthly_class WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM chart_snapshot_monthly_ticker WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM fixed_income_snapshot_items WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM fixed_income_snapshot_summary WHERE portfolio_id = ?", (pid,))
    db.execute("DELETE FROM portfolios WHERE id = ?", (pid,))
    db.commit()
    _clear_benchmark_cache()
    return True, portfolio["name"]


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


def _http_get_json(url: str, headers=None, timeout: float = 8.0):
    request_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
    }
    if headers:
        request_headers.update(headers)
    request = Request(
        url,
        headers=request_headers,
    )
    for _ in range(2):
        try:
            with urlopen(request, timeout=timeout) as response:
                body = response.read()
            if not body:
                continue
            return json.loads(body.decode("utf-8"))
        except (URLError, TimeoutError, ValueError):
            time.sleep(0.15)
            continue
        except Exception:
            time.sleep(0.15)
            continue
    return None


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

    payload = _http_get_json(
        (
            f"{_get_coingecko_base_url()}/coins/markets"
            f"?vs_currency=usd&symbols={symbol}&price_change_percentage=24h,7d,30d"
        ),
        headers=_get_coingecko_headers(),
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

    payload = _http_get_json(
        f"{_get_coingecko_base_url()}/coins/{coin_id}/market_chart?vs_currency=usd&days={cfg['days']}",
        headers=_get_coingecko_headers(),
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


def _get_brapi_token():
    return (os.getenv("BRAPI_TOKEN") or "").strip()


def _get_brapi_headers():
    token = _get_brapi_token()
    if not token:
        return None
    return {"Authorization": f"Bearer {token}"}


def _get_brapi_base_url():
    return (os.getenv("BRAPI_BASE_URL") or "https://brapi.dev/api").rstrip("/")


def _fetch_brapi_quote_result(ticker: str, range_key: str = None, interval: str = None, modules=None):
    if not _is_brazilian_market_ticker(ticker):
        return None

    headers = _get_brapi_headers()
    if not headers:
        if _should_log_market_sources() and not _BRAPI_DIAG["missing_token_logged"]:
            _get_app_logger().warning(
                "BRAPI desabilitado: BRAPI_TOKEN ausente (ticker=%s)",
                (ticker or "").strip().upper() or "?",
            )
            _BRAPI_DIAG["missing_token_logged"] = True
        return None

    params = []
    if range_key:
        params.append(f"range={range_key}")
    if interval:
        params.append(f"interval={interval}")
    if modules:
        params.append(f"modules={','.join(modules)}")
    query = ("?" + "&".join(params)) if params else ""

    payload = _http_get_json(
        f"{_get_brapi_base_url()}/quote/{(ticker or '').strip().upper()}{query}",
        headers=headers,
        timeout=12.0,
    )
    if not payload:
        if _should_log_market_sources():
            normalized = (ticker or "").strip().upper() or "?"
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
        return None
    try:
        results = payload.get("results") or []
        if not results:
            if _should_log_market_sources():
                normalized = (ticker or "").strip().upper() or "?"
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
            return None
        return results[0] or None
    except Exception:
        return None


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
    if len(prices) >= 8:
        base_7 = _to_number(prices[-8])
        last = _to_number(prices[-1])
        if base_7 not in (None, 0) and last is not None:
            variation_7d = ((last / base_7) - 1) * 100

    metrics = {
        "price": price,
        "pl": pl,
        "pvp": pvp,
        "dy": dy,
        "variation_day": variation_day,
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
    history, history_source = _fetch_market_history(ticker, range_key)
    if _is_truthy_env("MARKET_DATA_LOG_SOURCES", "0"):
        logger = _LOGGER
        try:
            logger = current_app.logger
        except Exception:
            pass
        logger.info(
            "market_history_source ticker=%s history_source=%s providers=%s range_key=%s points=%s",
            (ticker or "").strip().upper(),
            history_source or "none",
            _market_data_provider_label(ticker),
            history.get("range_key") or (range_key or "1y").lower(),
            len(history.get("prices") or []),
        )
    return history


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
    if not metrics:
        return False
    return any(
        metrics.get(field) is not None
        for field in (
            "price",
            "dy",
            "pl",
            "pvp",
            "variation_day",
            "variation_7d",
            "variation_30d",
            "market_cap_bi",
        )
    )


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
        "crypto": ["coingecko", "yahoo"],
        "us": ["twelve_data", "alpha_vantage", "yahoo"],
        "br": ["brapi", "yahoo", "google"],
    }
    return list(defaults.get(class_key, ["yahoo"]))


def _market_data_providers_from_env(ticker: str = ""):
    configured = []
    class_key = _market_data_class_key(ticker)

    class_specific_env = {
        "crypto": "MARKET_DATA_PROVIDERS_CRYPTO",
        "us": "MARKET_DATA_PROVIDERS_US",
        "br": "MARKET_DATA_PROVIDERS_BR",
    }
    class_specific_value = os.getenv(class_specific_env.get(class_key, ""), "")
    configured.extend(_providers_from_csv(class_specific_value))

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
    return order


def _market_data_provider_order(capability: str, ticker: str = ""):
    order = []
    for provider in _market_data_providers_from_env(ticker):
        capabilities = _MARKET_DATA_PROVIDER_CAPABILITIES.get(provider, set())
        if capability in capabilities and provider not in order:
            order.append(provider)
    return order


def _market_data_provider_label(ticker: str = ""):
    return ",".join(_market_data_providers_from_env(ticker))


def _is_truthy_env(name: str, default: str = "0"):
    value = (os.getenv(name, default) or default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _fetch_market_profile(ticker: str):
    for provider in _market_data_provider_order("profile", ticker):
        try:
            if provider == "alpha_vantage":
                profile = _fetch_alpha_vantage_profile(ticker)
            elif provider == "twelve_data":
                profile = None
            elif provider == "coingecko":
                profile = _fetch_coingecko_profile(ticker)
            elif provider == "brapi":
                profile = _fetch_brapi_profile(ticker)
            elif provider == "yahoo":
                profile = _fetch_yahoo_profile(ticker)
            else:
                profile = None
        except Exception:
            profile = None
        if profile and any((profile.get("name"), profile.get("sector"))):
            return profile, provider
    return {}, None


def _fetch_market_metrics(ticker: str):
    for provider in _market_data_provider_order("metrics", ticker):
        try:
            if provider == "alpha_vantage":
                metrics = _fetch_alpha_vantage_metrics(ticker)
            elif provider == "twelve_data":
                metrics = _fetch_twelve_data_metrics(ticker)
            elif provider == "coingecko":
                metrics = _fetch_coingecko_metrics(ticker)
            elif provider == "brapi":
                metrics = _fetch_brapi_metrics(ticker)
            elif provider == "google":
                metrics = _fetch_google_metrics(ticker)
            else:
                metrics = _fetch_yahoo_metrics(ticker)
        except Exception:
            metrics = None
        if _has_market_metrics(metrics):
            return metrics, provider
    return {}, None


def _fetch_market_history(ticker: str, range_key: str):
    for provider in _market_data_provider_order("history", ticker):
        try:
            if provider == "alpha_vantage":
                history = _fetch_alpha_vantage_history(ticker, range_key)
            elif provider == "twelve_data":
                history = _fetch_twelve_data_history(ticker, range_key)
            elif provider == "coingecko":
                history = _fetch_coingecko_history(ticker, range_key)
            elif provider == "brapi":
                history = _fetch_brapi_history(ticker, range_key)
            elif provider == "yahoo":
                history = _get_yahoo_asset_price_history(ticker, range_key)
            else:
                history = None
        except Exception:
            history = None
        if history and history.get("prices"):
            return history, provider
    return {
        "range_key": _history_config(range_key)[0],
        "labels": [],
        "prices": [],
        "change_pct": None,
    }, None


def refresh_asset_market_data(ticker: str):
    asset = get_asset(ticker)
    if not asset:
        return False

    profile, profile_source = _fetch_market_profile(ticker)
    metrics, metrics_source = _fetch_market_metrics(ticker)
    if not metrics and not profile:
        _mark_asset_market_data_failed(ticker, "Nenhum provider retornou dados de mercado.")
        return False
    has_market_metrics = _has_market_metrics(metrics)
    attempted_at = _now_iso()
    market_data_status = "fresh" if has_market_metrics else "stale"
    market_data_updated_at = attempted_at if has_market_metrics else asset.get("market_data_updated_at")
    market_data_source = metrics_source or asset.get("market_data_source", "")
    market_data_last_error = "" if has_market_metrics else "Atualizacao sem metricas novas."

    db = get_db()
    name = asset["name"]
    sector = asset["sector"]
    logo_url = asset.get("logo_url", "")
    if profile:
        # Sempre prioriza perfil retornado pelo provider quando houver valor.
        # Isso evita ativo ficar preso com nome/setor antigo apos importacoes.
        name = profile.get("name") or name
        sector = profile.get("sector") or sector
        logo_url = profile.get("logo_url") or logo_url

    db.execute(
        """
        UPDATE assets
        SET
            name = ?,
            sector = ?,
            price = ?,
            dy = ?,
            pl = ?,
            pvp = ?,
            variation_day = ?,
            variation_7d = ?,
            variation_30d = ?,
            market_cap_bi = ?,
            logo_url = ?,
            market_data_status = ?,
            market_data_source = ?,
            market_data_updated_at = ?,
            market_data_last_attempt_at = ?,
            market_data_last_error = ?
        WHERE ticker = ?
        """,
        (
            name,
            sector,
            metrics.get("price") if metrics.get("price") is not None else asset["price"],
            metrics.get("dy") if metrics.get("dy") is not None else asset["dy"],
            metrics.get("pl") if metrics.get("pl") is not None else asset["pl"],
            metrics.get("pvp") if metrics.get("pvp") is not None else asset["pvp"],
            metrics.get("variation_day")
            if metrics.get("variation_day") is not None
            else asset["variation_day"],
            metrics.get("variation_7d")
            if metrics.get("variation_7d") is not None
            else asset.get("variation_7d", 0.0),
            metrics.get("variation_30d")
            if metrics.get("variation_30d") is not None
            else asset.get("variation_30d", 0.0),
            metrics.get("market_cap_bi")
            if metrics.get("market_cap_bi") is not None
            else asset["market_cap_bi"],
            logo_url,
            market_data_status,
            market_data_source,
            market_data_updated_at,
            attempted_at,
            market_data_last_error,
            ticker.upper(),
        ),
    )
    db.commit()
    if _is_truthy_env("MARKET_DATA_LOG_SOURCES", "0"):
        logger = _LOGGER
        try:
            logger = current_app.logger
        except Exception:
            pass
        logger.info(
            "market_data_source ticker=%s providers=%s metrics_source=%s profile_source=%s price=%s dy=%s pl=%s pvp=%s variation_day=%s variation_7d=%s variation_30d=%s market_cap_bi=%s",
            ticker.upper(),
            _market_data_provider_label(ticker),
            metrics_source or "none",
            profile_source or "none",
            metrics.get("price"),
            metrics.get("dy"),
            metrics.get("pl"),
            metrics.get("pvp"),
            metrics.get("variation_day"),
            metrics.get("variation_7d"),
            metrics.get("variation_30d"),
            metrics.get("market_cap_bi"),
        )
    # Sucesso de "atualizacao Yahoo" significa ter recebido cotacao/indicadores.
    # Atualizacao apenas de nome/setor nao conta como sync completo de mercado.
    return has_market_metrics


def refresh_all_assets_market_data(attempts: int = 3):
    db = get_db()
    rows = db.execute("SELECT ticker FROM assets").fetchall()
    tickers = [row["ticker"] for row in rows]
    if not tickers:
        return []
    return refresh_market_data_for_tickers(tickers, attempts=attempts)


def refresh_market_data_for_tickers(tickers, attempts: int = 2):
    unique_tickers = []
    for ticker in tickers:
        clean = (ticker or "").strip().upper()
        if clean and clean not in unique_tickers:
            unique_tickers.append(clean)

    failed = set(unique_tickers)
    for attempt in range(attempts):
        if not failed:
            break
        next_failed = set()
        for ticker in list(failed):
            try:
                ok = refresh_asset_market_data(ticker)
            except Exception as exc:
                _mark_asset_market_data_failed(ticker, str(exc))
                ok = False
            if not ok:
                next_failed.add(ticker)
            # Pequeno jitter reduz chance de bloqueio/rate-limit em lote.
            time.sleep(0.12 + random.random() * 0.12)
        failed = next_failed
        if failed and attempt < attempts - 1:
            # Backoff progressivo entre rodadas.
            time.sleep(0.5 * (attempt + 1))
    return sorted(failed)


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


def _current_shares(ticker: str, portfolio_id: int):
    db = get_db()
    row = db.execute(
        """
        SELECT COALESCE(SUM(CASE WHEN tx_type = 'buy' THEN shares ELSE -shares END), 0) AS shares
        FROM transactions
        WHERE ticker = ? AND portfolio_id = ?
        """,
        (ticker, portfolio_id),
    ).fetchone()
    return float(row["shares"] or 0.0)


def _transaction_exists(portfolio_id: int, ticker: str, tx_type: str, shares: float, price: float, date: str):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM transactions
        WHERE portfolio_id = ?
          AND ticker = ?
          AND tx_type = ?
          AND ABS(shares - ?) < 0.000000001
          AND ABS(price - ?) < 0.000001
          AND date = ?
        LIMIT 1
        """,
        (portfolio_id, ticker, tx_type, shares, price, date),
    ).fetchone()
    return row is not None


def _income_exists(portfolio_id: int, ticker: str, income_type: str, amount: float, date: str):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM incomes
        WHERE portfolio_id = ?
          AND ticker = ?
          AND income_type = ?
          AND ABS(amount - ?) < 0.000001
          AND date = ?
        LIMIT 1
        """,
        (portfolio_id, ticker, income_type, amount, date),
    ).fetchone()
    return row is not None


def add_transaction(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    ticker = (form_data.get("ticker") or "").strip().upper()
    tx_type = (form_data.get("tx_type") or "").strip().lower()

    if not ticker:
        return False, "Ticker e obrigatorio."
    if tx_type not in {"buy", "sell"}:
        return False, "Tipo de transacao invalido."

    shares = _parse_float(form_data.get("shares"))
    if shares is None:
        return False, "Quantidade precisa ser numerica."
    if shares <= 0:
        return False, "Quantidade precisa ser maior que zero."

    price = _parse_float(form_data.get("price"))
    if price is None or price <= 0:
        return False, "Preco precisa ser numerico e maior que zero."
    ok_conversion, converted_price, conversion_error = _convert_usd_to_brl_if_needed(ticker, price)
    if not ok_conversion:
        return False, conversion_error
    price = converted_price

    transaction_date = _parse_date(form_data.get("date"))
    if transaction_date is None:
        return False, "Data invalida. Use o formato YYYY-MM-DD."

    if _transaction_exists(portfolio_id, ticker, tx_type, shares, price, transaction_date):
        return False, "Transacao duplicada: ja existe um registro com esses mesmos dados."

    db = get_db()
    if tx_type == "sell":
        if shares - _current_shares(ticker, portfolio_id) > 0.000000001:
            return False, "Venda maior que a quantidade em carteira."

    asset = get_asset(ticker)
    if not asset:
        if tx_type == "sell":
            return False, "Nao existe posicao para esse ticker."
        profile, _ = _fetch_market_profile(ticker)
        name = (
            profile.get("name")
            or (form_data.get("name") or "").strip()
            or ticker
        )
        sector = (
            profile.get("sector")
            or (form_data.get("sector") or "").strip()
            or "Nao informado"
        )
        db.execute(
            """
            INSERT INTO assets (
                ticker, name, sector, price, dy, pl, pvp, variation_day, market_cap_bi
            ) VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0)
            """,
            (ticker, name, sector, price),
        )
    else:
        should_update_profile = asset["name"] == ticker or asset["sector"] == "Nao informado"
        if should_update_profile:
            profile, _ = _fetch_market_profile(ticker)
            name = profile.get("name") or asset["name"]
            sector = profile.get("sector") or asset["sector"]
            db.execute(
                "UPDATE assets SET name = ?, sector = ? WHERE ticker = ?",
                (name, sector, ticker),
            )

    db.execute(
        """
        INSERT INTO transactions (portfolio_id, ticker, tx_type, shares, price, date)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (portfolio_id, ticker, tx_type, shares, price, transaction_date),
    )
    db.commit()
    invalidate_chart_snapshots([portfolio_id])

    return True, "Transacao registrada com sucesso."


def import_transactions_csv(file_bytes, target_portfolio_id: int):
    if not file_bytes:
        return False, "Arquivo CSV vazio.", 0, []

    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False, "Nao foi possivel ler o CSV (use UTF-8).", 0, []

    sample = text[:2048]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except Exception:
        if "\t" in sample:
            delimiter = "\t"
        else:
            delimiter = ";" if ";" in sample and "," not in sample else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        return False, "CSV sem cabecalho.", 0, []

    header_map = {
        "ticker": "ticker",
        "ativo": "ticker",
        "tx_type": "tx_type",
        "tipo/tx_type": "tx_type",
        "tipo": "tx_type",
        "shares": "shares",
        "quantidade/shares": "shares",
        "quantidade": "shares",
        "qtd": "shares",
        "price": "price",
        "preco/price": "price",
        "preço/price": "price",
        "preco": "price",
        "preço": "price",
        "date": "date",
        "data/date": "date",
        "data": "date",
        "name": "name",
        "nome": "name",
        "sector": "sector",
        "setor": "sector",
        "amount": "amount",
        "valor": "amount",
        "valor/amount": "amount",
        "provento": "amount",
    }

    normalized_fields = {}
    for field in reader.fieldnames:
        key = (field or "").strip().lower()
        mapped = header_map.get(key)
        if mapped:
            normalized_fields[field] = mapped

    required = {"ticker", "tx_type", "date"}
    if not required.issubset(set(normalized_fields.values())):
        return (
            False,
            "CSV precisa ter colunas: ticker, tipo/tx_type e data/date.",
            0,
            [],
        )

    imported = 0
    errors = []
    csv_tickers = set()
    line_number = 1

    for row in reader:
        line_number += 1
        payload = {"target_portfolio_id": str(target_portfolio_id)}
        for original, mapped in normalized_fields.items():
            payload[mapped] = (row.get(original) or "").strip()
        ticker = (payload.get("ticker") or "").strip().upper()
        if ticker:
            csv_tickers.add(ticker)

        tx_type = payload.get("tx_type", "").lower()
        if tx_type == "compra":
            payload["tx_type"] = "buy"
        elif tx_type == "venda":
            payload["tx_type"] = "sell"

        tx_type = payload.get("tx_type", "").lower()
        if tx_type in {"dividendo", "jcp", "aluguel"}:
            income_payload = {
                "target_portfolio_id": str(target_portfolio_id),
                "ticker": payload.get("ticker"),
                "income_type": tx_type,
                "amount": payload.get("amount") or payload.get("price"),
                "date": payload.get("date"),
            }
            ok, message = add_income(income_payload)
        else:
            ok, message = add_transaction(payload)

        if ok:
            imported += 1
        else:
            errors.append(f"Linha {line_number}: {message}")

    failed_refresh = refresh_market_data_for_tickers(sorted(csv_tickers), attempts=2)
    for ticker in failed_refresh:
        errors.append(f"Aviso: nao foi possivel atualizar Yahoo para {ticker}.")

    return True, "Importacao concluida.", imported, errors


def add_income(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    ticker = (form_data.get("ticker") or "").strip().upper()
    income_type = (form_data.get("income_type") or "").strip().lower()

    if not ticker:
        return False, "Ticker e obrigatorio."
    if income_type not in {"dividendo", "jcp", "aluguel"}:
        return False, "Tipo de provento invalido."

    amount = _parse_float(form_data.get("amount"))
    if amount is None or amount <= 0:
        return False, "Valor do provento precisa ser numerico e maior que zero."
    ok_conversion, converted_amount, conversion_error = _convert_usd_to_brl_if_needed(ticker, amount)
    if not ok_conversion:
        return False, conversion_error
    amount = converted_amount

    income_date = _parse_date(form_data.get("date"))
    if income_date is None:
        return False, "Data invalida. Use o formato YYYY-MM-DD."

    if _income_exists(portfolio_id, ticker, income_type, amount, income_date):
        return False, "Provento duplicado: ja existe um registro com esses mesmos dados."

    if not get_asset(ticker):
        return False, "Ticker nao cadastrado. Lance uma transacao primeiro."

    db = get_db()
    db.execute(
        """
        INSERT INTO incomes (portfolio_id, ticker, income_type, amount, date)
        VALUES (?, ?, ?, ?, ?)
        """,
        (portfolio_id, ticker, income_type, amount, income_date),
    )
    db.commit()
    invalidate_chart_snapshots([portfolio_id])
    return True, "Provento registrado com sucesso."


def add_fixed_income(form_data: dict):
    portfolio_id = resolve_portfolio_id(
        form_data.get("target_portfolio_id") or form_data.get("portfolio_id")
    )
    distributor = (form_data.get("distributor") or "").strip()
    issuer = (form_data.get("issuer") or "").strip()
    investment_type = (form_data.get("investment_type") or "").strip().upper()
    rate_type = (form_data.get("rate_type") or "").strip().upper()
    annual_rate_legacy = _parse_float(form_data.get("annual_rate"))
    rate_fixed = _parse_float(form_data.get("juros_fixo"))
    rate_ipca = _parse_float(form_data.get("ipca"))
    rate_cdi = _parse_float(form_data.get("cdi"))
    rate_fixed = 0.0 if rate_fixed is None else rate_fixed
    rate_ipca = 0.0 if rate_ipca is None else rate_ipca
    rate_cdi = 0.0 if rate_cdi is None else rate_cdi
    date_aporte = _parse_date(form_data.get("date_aporte"))
    maturity_date = _parse_date(form_data.get("maturity_date"))
    aporte = _parse_float(form_data.get("aporte"))
    reinvested = _parse_float(form_data.get("reinvested"))

    if not distributor:
        return False, "Distribuidor e obrigatorio."
    if not issuer:
        return False, "Emissor e obrigatorio."
    if not investment_type:
        return False, "Investimento e obrigatorio."
    if rate_type not in {"FIXO", "FIXO+IPCA", "IPCA", "CDI", "FIXO+CDI"}:
        return False, "Tipo de taxa invalido."
    expected_sets = {
        "FIXO": {"FIXO"},
        "IPCA": {"IPCA"},
        "CDI": {"CDI"},
        "FIXO+IPCA": {"FIXO", "IPCA"},
        "FIXO+CDI": {"FIXO", "CDI"},
    }
    positive_set = set()
    if rate_fixed > 0:
        positive_set.add("FIXO")
    if rate_ipca > 0:
        positive_set.add("IPCA")
    if rate_cdi > 0:
        positive_set.add("CDI")

    expected = expected_sets[rate_type]
    annual_rate = None
    if positive_set:
        if positive_set != expected:
            return (
                False,
                (
                    f"Para o tipo {rate_type}, preencha somente: "
                    f"{', '.join(sorted(expected))}."
                ),
            )
        component_rates = {"FIXO": rate_fixed, "IPCA": rate_ipca, "CDI": rate_cdi}
        annual_rate = sum(component_rates[key] for key in expected)
    else:
        # Compatibilidade para layout antigo (sem componentes) apenas para tipos simples.
        if rate_type in {"FIXO+IPCA", "FIXO+CDI"}:
            return False, f"Para o tipo {rate_type}, informe os percentuais de cada componente."
        if annual_rate_legacy is None or annual_rate_legacy < 0:
            return False, "Taxa anual invalida."
        annual_rate = annual_rate_legacy

    if annual_rate is None or annual_rate < 0:
        return False, "Taxa anual invalida."
    if aporte is None or aporte <= 0:
        return False, "Aporte invalido."
    if reinvested is None:
        reinvested = 0.0
    if reinvested < 0:
        return False, "Reinvestido nao pode ser negativo."
    if not date_aporte:
        return False, "Data de aporte invalida."
    if not maturity_date:
        return False, "Data final invalida."
    if maturity_date < date_aporte:
        return False, "Data final nao pode ser menor que data de aporte."
    if _fixed_income_exists(
        portfolio_id,
        distributor,
        issuer,
        investment_type,
        rate_type,
        annual_rate,
        rate_fixed,
        rate_ipca,
        rate_cdi,
        date_aporte,
        aporte,
        reinvested,
        maturity_date,
    ):
        return False, "Registro duplicado: ja existe uma renda fixa com os mesmos dados."

    db = get_db()
    db.execute(
        """
        INSERT INTO fixed_incomes (
            portfolio_id, distributor, issuer, investment_type, rate_type, annual_rate,
            rate_fixed, rate_ipca, rate_cdi,
            date_aporte, aporte, reinvested, maturity_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            portfolio_id,
            distributor,
            issuer,
            investment_type,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            aporte,
            reinvested,
            maturity_date,
        ),
    )
    db.commit()
    invalidate_fixed_income_snapshot([portfolio_id])
    invalidate_chart_snapshots([portfolio_id])
    return True, "Renda fixa cadastrada com sucesso."


def _fixed_income_exists(
    portfolio_id: int,
    distributor: str,
    issuer: str,
    investment_type: str,
    rate_type: str,
    annual_rate: float,
    rate_fixed: float,
    rate_ipca: float,
    rate_cdi: float,
    date_aporte: str,
    aporte: float,
    reinvested: float,
    maturity_date: str,
):
    db = get_db()
    row = db.execute(
        """
        SELECT id
        FROM fixed_incomes
        WHERE portfolio_id = ?
          AND distributor = ?
          AND issuer = ?
          AND investment_type = ?
          AND rate_type = ?
          AND ABS(annual_rate - ?) < 0.000001
          AND ABS(rate_fixed - ?) < 0.000001
          AND ABS(rate_ipca - ?) < 0.000001
          AND ABS(rate_cdi - ?) < 0.000001
          AND date_aporte = ?
          AND ABS(aporte - ?) < 0.000001
          AND ABS(reinvested - ?) < 0.000001
          AND maturity_date = ?
        LIMIT 1
        """,
        (
            portfolio_id,
            distributor,
            issuer,
            investment_type,
            rate_type,
            annual_rate,
            rate_fixed,
            rate_ipca,
            rate_cdi,
            date_aporte,
            aporte,
            reinvested,
            maturity_date,
        ),
    ).fetchone()
    return row is not None


def import_fixed_incomes_csv(file_bytes, target_portfolio_id: int):
    if not file_bytes:
        return False, "Arquivo CSV vazio.", 0, []

    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return False, "Nao foi possivel ler o CSV (use UTF-8).", 0, []

    sample = text[:2048]
    delimiter = ","
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
        delimiter = dialect.delimiter
    except Exception:
        if "\t" in sample:
            delimiter = "\t"
        else:
            delimiter = ";" if ";" in sample and "," not in sample else ","

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    if not reader.fieldnames:
        return False, "CSV sem cabecalho.", 0, []

    header_map = {
        "distributor": "distributor",
        "distribuidor": "distributor",
        "issuer": "issuer",
        "emissor": "issuer",
        "investment_type": "investment_type",
        "investimento": "investment_type",
        "rate_type": "rate_type",
        "tipo_taxa": "rate_type",
        "tipo taxa": "rate_type",
        "tax_type": "rate_type",
        "tipo": "rate_type",
        "annual_rate": "annual_rate",
        "taxa_anual": "annual_rate",
        "taxa anual": "annual_rate",
        "juros_fixo": "annual_rate",
        "juros fixo": "annual_rate",
        "jurosfixo": "annual_rate",
        "juros_fixo_%": "annual_rate",
        "juros fixo %": "annual_rate",
        "juros_fixo_csv": "juros_fixo",
        "juros fixo csv": "juros_fixo",
        "juros_fixo_col": "juros_fixo",
        "juros fixo col": "juros_fixo",
        "juros_fixo_valor": "juros_fixo",
        "juros fixo valor": "juros_fixo",
        "jurosfixocsv": "juros_fixo",
        "jurosfixocol": "juros_fixo",
        "jurosfixovalor": "juros_fixo",
        "juros_fixo": "juros_fixo",
        "juros fixo": "juros_fixo",
        "ipca": "ipca",
        "cdi": "cdi",
        "date_aporte": "date_aporte",
        "data_aporte": "date_aporte",
        "data aporte": "date_aporte",
        "aporte_date": "date_aporte",
        "maturity_date": "maturity_date",
        "data_final": "maturity_date",
        "data final": "maturity_date",
        "vencimento": "maturity_date",
        "aporte": "aporte",
        "applied": "aporte",
        "reinvested": "reinvested",
        "reinvestido": "reinvested",
    }

    normalized_fields = {}
    for field in reader.fieldnames:
        key = (field or "").strip().lower()
        mapped = header_map.get(key)
        if mapped:
            normalized_fields[field] = mapped

    required = {
        "distributor",
        "issuer",
        "investment_type",
        "rate_type",
        "date_aporte",
        "maturity_date",
        "aporte",
        "reinvested",
    }
    if not required.issubset(set(normalized_fields.values())):
        return (
            False,
            (
                "CSV de renda fixa precisa ter colunas: Distribuidor, Emissor, Investimento, "
                "tipo, data aporte, aporte, Reinvestido, data final, Juros Fixo, IPCA e CDI."
            ),
            0,
            [],
        )

    has_rate_cols = {"juros_fixo", "ipca", "cdi"}.issubset(set(normalized_fields.values()))
    has_legacy_rate = {"rate_type", "annual_rate"}.issubset(set(normalized_fields.values()))
    if not has_rate_cols and not has_legacy_rate:
        return (
            False,
            "CSV precisa informar as colunas de taxa (Juros Fixo, IPCA, CDI) ou (tipo taxa, taxa anual).",
            0,
            [],
        )

    imported = 0
    errors = []
    line_number = 1
    for row in reader:
        line_number += 1
        payload = {"target_portfolio_id": str(target_portfolio_id)}
        for original, mapped in normalized_fields.items():
            payload[mapped] = (row.get(original) or "").strip()

        # Novo padrao: escolhe automaticamente o tipo de taxa pela coluna preenchida.
        rate_type_raw = (payload.get("rate_type") or "").strip().upper()
        rate_type_map = {
            "FIXO": "FIXO",
            "FIXO+IPCA": "FIXO+IPCA",
            "FIXO + IPCA": "FIXO+IPCA",
            "CDI": "CDI",
            "IPCA": "IPCA",
            "FIXO+CDI": "FIXO+CDI",
            "FIXO + CDI": "FIXO+CDI",
        }
        payload["rate_type"] = rate_type_map.get(rate_type_raw, rate_type_raw)
        if payload["rate_type"] not in {"FIXO", "FIXO+IPCA", "IPCA", "CDI", "FIXO+CDI"}:
            errors.append(
                f"Linha {line_number}: tipo invalido. Use FIXO, FIXO+IPCA, IPCA, CDI ou FIXO+CDI."
            )
            continue

        juros_fixo = _parse_float(payload.get("juros_fixo"))
        ipca = _parse_float(payload.get("ipca"))
        cdi = _parse_float(payload.get("cdi"))
        rate_candidates = [
            ("FIXO", juros_fixo if juros_fixo is not None else 0.0),
            ("IPCA", ipca if ipca is not None else 0.0),
            ("CDI", cdi if cdi is not None else 0.0),
        ]
        positive_rates = {rtype for rtype, rate in rate_candidates if rate > 0}
        if positive_rates:
            expected_sets = {
                "FIXO": {"FIXO"},
                "IPCA": {"IPCA"},
                "CDI": {"CDI"},
                "FIXO+IPCA": {"FIXO", "IPCA"},
                "FIXO+CDI": {"FIXO", "CDI"},
            }
            expected = expected_sets[payload["rate_type"]]
            if positive_rates != expected:
                errors.append(
                    (
                        f"Linha {line_number}: tipo '{payload['rate_type']}' nao bate com as colunas de taxa preenchidas "
                        f"(esperado {', '.join(sorted(expected))})."
                    )
                )
                continue

            rate_values = {
                "FIXO": juros_fixo if juros_fixo is not None else 0.0,
                "IPCA": ipca if ipca is not None else 0.0,
                "CDI": cdi if cdi is not None else 0.0,
            }
            payload["annual_rate"] = sum(rate_values[key] for key in expected)
        elif "annual_rate" in payload and "rate_type" in payload:
            # Compatibilidade com layout antigo.
            pass
        else:
            errors.append(
                f"Linha {line_number}: informe a taxa correspondente ao tipo em Juros Fixo, IPCA ou CDI."
            )
            continue

        ok, message = add_fixed_income(payload)
        if not ok:
            errors.append(f"Linha {line_number}: {message}")
            continue

        imported += 1

    return True, "Importacao concluida.", imported, errors


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
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            fi.id,
            fi.portfolio_id,
            fi.distributor,
            fi.issuer,
            fi.investment_type,
            fi.rate_type,
            fi.annual_rate,
            fi.rate_fixed,
            fi.rate_ipca,
            fi.rate_cdi,
            fi.date_aporte,
            fi.aporte,
            fi.reinvested,
            fi.maturity_date,
            p.name AS portfolio_name
        FROM fixed_incomes fi
        JOIN portfolios p ON p.id = fi.portfolio_id
        WHERE fi.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY fi.date_aporte DESC, fi.id DESC
        """,
        tuple(pids),
    ).fetchall()
    items = [_fixed_income_projection(dict(row)) for row in rows]
    return _sort_fixed_income_items(items, sort_by=sort_by, sort_dir=sort_dir)


def _sort_fixed_income_items(items, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    valid_dirs = {"asc", "desc"}
    direction = sort_dir if sort_dir in valid_dirs else "desc"
    key_name = (sort_by or "date_aporte").strip()
    if key_name not in {
        "portfolio_name",
        "distributor",
        "issuer",
        "investment_type",
        "rate_type",
        "annual_rate",
        "date_aporte",
        "maturity_date",
        "active_applied_value",
        "elapsed_days",
        "total_days",
        "current_gross_value",
        "total_received",
        "rendimento",
        "final_gross_value",
    }:
        key_name = "date_aporte"

    def _sort_key(item):
        value = item.get(key_name)
        if value is None:
            return (1, "")
        if isinstance(value, (int, float)):
            return (0, float(value))
        return (0, str(value).lower())

    sorted_items = list(items or [])
    sorted_items.sort(key=_sort_key, reverse=(direction == "desc"))
    return sorted_items


def delete_fixed_incomes(fixed_income_ids, portfolio_ids):
    ids = []
    for raw_id in fixed_income_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM fixed_incomes
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    invalidate_fixed_income_snapshot(pids)
    invalidate_chart_snapshots(pids)
    return cursor.rowcount or 0


def get_fixed_income_summary(portfolio_ids):
    payload = get_fixed_income_payload_cached(portfolio_ids)
    return payload.get("summary", get_fixed_income_summary_from_items(payload.get("items") or []))


def get_fixed_income_summary_from_items(items):
    items = items or []
    return {
        "applied_total": round(sum(item["active_applied_value"] for item in items), 2),
        "current_total": round(sum(item["current_gross_value"] for item in items), 2),
        "income_total": round(sum(item["current_income"] for item in items), 2),
        "final_total": round(
            sum(item["final_gross_value"] for item in items if not item["is_matured"]),
            2,
        ),
        "total_received": round(sum(item["total_received"] for item in items), 2),
        "rendimento_recebido_total": round(sum(item["rendimento"] for item in items), 2),
        "count": len(items),
    }


def _snapshot_now():
    return datetime.now().isoformat(timespec="seconds")


def _snapshot_age_seconds(iso_text: str):
    try:
        created = datetime.fromisoformat((iso_text or "").strip())
    except (TypeError, ValueError):
        return None
    return max((datetime.now() - created).total_seconds(), 0.0)


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
    if portfolio_ids is None:
        pids = _all_portfolio_ids()
    else:
        pids = normalize_portfolio_ids(portfolio_ids)
    if not pids:
        return {"portfolios": 0, "items": 0}

    db = get_db()
    total_items = 0
    stamp = _snapshot_now()
    for pid in pids:
        items = get_fixed_incomes([pid], sort_by="date_aporte", sort_dir="desc")
        summary = get_fixed_income_summary_from_items(items)
        total_items += len(items)
        try:
            db.execute(
                "DELETE FROM fixed_income_snapshot_items WHERE portfolio_id = ?",
                (pid,),
            )
            db.execute(
                """
                INSERT INTO fixed_income_snapshot_summary (portfolio_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(portfolio_id) DO UPDATE SET
                  payload_json = excluded.payload_json,
                  updated_at = excluded.updated_at
                """,
                (pid, json.dumps(summary, ensure_ascii=False), stamp),
            )
            for item in items:
                db.execute(
                    """
                    INSERT INTO fixed_income_snapshot_items (portfolio_id, fixed_income_id, payload_json, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(portfolio_id, fixed_income_id) DO UPDATE SET
                      payload_json = excluded.payload_json,
                      updated_at = excluded.updated_at
                    """,
                    (pid, int(item["id"]), json.dumps(item, ensure_ascii=False), stamp),
                )
        except Exception:
            db.rollback()
            raise
    db.commit()
    return {"portfolios": len(pids), "items": total_items}


def get_fixed_income_payload_cached(portfolio_ids, sort_by: str = "date_aporte", sort_dir: str = "desc"):
    pids = normalize_portfolio_ids(portfolio_ids)
    max_age_seconds = int(current_app.config.get("FIXED_INCOME_SNAPSHOT_MAX_AGE_SECONDS", 900))
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()

    try:
        summary_rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM fixed_income_snapshot_summary
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()
        if len(summary_rows) != len(pids):
            raise RuntimeError("snapshot_miss")

        summary_map = {}
        for row in summary_rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            summary_map[int(row["portfolio_id"])] = json.loads(row["payload_json"] or "{}")

        item_rows = db.execute(
            """
            SELECT portfolio_id, payload_json, updated_at
            FROM fixed_income_snapshot_items
            WHERE portfolio_id IN ("""
            + placeholders
            + """)
            """,
            tuple(pids),
        ).fetchall()

        items = []
        for row in item_rows:
            age = _snapshot_age_seconds(row["updated_at"])
            if age is None or age > max_age_seconds:
                raise RuntimeError("snapshot_stale")
            items.append(json.loads(row["payload_json"] or "{}"))

        summary = {
            "applied_total": 0.0,
            "current_total": 0.0,
            "income_total": 0.0,
            "final_total": 0.0,
            "total_received": 0.0,
            "rendimento_recebido_total": 0.0,
            "count": 0,
        }
        for pid in pids:
            part = summary_map.get(int(pid), {})
            summary["applied_total"] += float(part.get("applied_total", 0.0))
            summary["current_total"] += float(part.get("current_total", 0.0))
            summary["income_total"] += float(part.get("income_total", 0.0))
            summary["final_total"] += float(part.get("final_total", 0.0))
            summary["total_received"] += float(part.get("total_received", 0.0))
            summary["rendimento_recebido_total"] += float(part.get("rendimento_recebido_total", 0.0))
            summary["count"] += int(part.get("count", 0))
        for key in ("applied_total", "current_total", "income_total", "final_total", "total_received", "rendimento_recebido_total"):
            summary[key] = round(summary[key], 2)

        return {
            "items": _sort_fixed_income_items(items, sort_by=sort_by, sort_dir=sort_dir),
            "summary": summary,
            "snapshot": True,
        }
    except Exception:
        items = get_fixed_incomes(pids, sort_by=sort_by, sort_dir=sort_dir)
        summary = get_fixed_income_summary_from_items(items)
        return {"items": items, "summary": summary, "snapshot": False}


def get_transactions(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            t.id,
            t.ticker,
            t.tx_type,
            t.shares,
            t.price,
            t.date,
            (t.shares * t.price) AS total_value,
            p.name AS portfolio_name
        FROM transactions t
        JOIN portfolios p ON p.id = t.portfolio_id
        WHERE t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date DESC, t.id DESC
        """,
        tuple(pids),
    ).fetchall()
    return [dict(row) for row in rows]


def delete_transactions(transaction_ids, portfolio_ids):
    ids = []
    for raw_id in transaction_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM transactions
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    invalidate_chart_snapshots(pids)
    return cursor.rowcount


def get_incomes(portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            i.id,
            i.ticker,
            i.income_type,
            i.amount,
            i.date,
            p.name AS portfolio_name
        FROM incomes i
        JOIN portfolios p ON p.id = i.portfolio_id
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date DESC, i.id DESC
        """,
        tuple(pids),
    ).fetchall()
    return [dict(row) for row in rows]


def delete_incomes(income_ids, portfolio_ids):
    ids = []
    for raw_id in income_ids:
        try:
            parsed = int(raw_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in ids:
            ids.append(parsed)

    if not ids:
        return 0

    pids = normalize_portfolio_ids(portfolio_ids)
    ids_placeholders = ",".join(["?"] * len(ids))
    pids_placeholders = ",".join(["?"] * len(pids))

    db = get_db()
    cursor = db.execute(
        """
        DELETE FROM incomes
        WHERE id IN ("""
        + ids_placeholders
        + """)
          AND portfolio_id IN ("""
        + pids_placeholders
        + """)
        """,
        tuple(ids + pids),
    )
    db.commit()
    invalidate_chart_snapshots(pids)
    return cursor.rowcount or 0


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
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            t.ticker,
            t.tx_type,
            t.shares,
            t.price,
            t.date,
            (t.shares * t.price) AS total_value,
            p.name AS portfolio_name
        FROM transactions t
        JOIN portfolios p ON p.id = t.portfolio_id
        WHERE t.ticker = ? AND t.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY t.date DESC, t.id DESC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()
    return [dict(row) for row in rows]


def get_asset_incomes(ticker: str, portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    rows = db.execute(
        """
        SELECT
            i.ticker,
            i.income_type,
            i.amount,
            i.date,
            p.name AS portfolio_name
        FROM incomes i
        JOIN portfolios p ON p.id = i.portfolio_id
        WHERE i.ticker = ? AND i.portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY i.date DESC, i.id DESC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()
    return [dict(row) for row in rows]


def get_asset_position_summary(ticker: str, portfolio_ids):
    pids = normalize_portfolio_ids(portfolio_ids)
    placeholders = ",".join(["?"] * len(pids))
    db = get_db()
    asset = get_asset(ticker)
    if not asset:
        return {
            "shares": 0,
            "avg_price": 0.0,
            "total_value": 0.0,
            "market_value": 0.0,
            "open_pnl_value": 0.0,
            "open_pnl_pct": 0.0,
            "total_incomes": 0.0,
            "incomes_current_month": 0.0,
            "incomes_3m": 0.0,
            "incomes_12m": 0.0,
        }

    rows = db.execute(
        """
        SELECT tx_type, shares, price
        FROM transactions
        WHERE ticker = ? AND portfolio_id IN ("""
        + placeholders
        + """)
        ORDER BY date ASC, id ASC
        """,
        tuple([ticker.upper()] + pids),
    ).fetchall()

    shares = 0
    total_cost = 0.0

    for row in rows:
        tx_type = row["tx_type"]
        tx_shares = row["shares"]
        tx_price = row["price"]

        if tx_type == "buy":
            total_cost += tx_shares * tx_price
            shares += tx_shares
            continue

        if shares <= 0:
            continue

        avg_price = total_cost / shares
        sell_shares = min(tx_shares, shares)
        total_cost -= avg_price * sell_shares
        shares -= sell_shares

    avg_price = (total_cost / shares) if shares > 0 else 0.0
    total_value = total_cost
    market_value = shares * asset["price"]
    open_pnl_value = market_value - total_value
    open_pnl_pct = (open_pnl_value / total_value) * 100 if total_value > 0 else 0.0
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

    income_row = db.execute(
        """
        SELECT
            COALESCE(SUM(amount), 0) AS total_incomes,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_current_month,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_3m,
            COALESCE(SUM(CASE WHEN date >= ? THEN amount ELSE 0 END), 0) AS incomes_12m
        FROM incomes
        WHERE ticker = ? AND portfolio_id IN ("""
        + placeholders
        + """)
        """,
        tuple(
            [
                current_month_start.strftime("%Y-%m-%d"),
                start_3m.strftime("%Y-%m-%d"),
                start_12m.strftime("%Y-%m-%d"),
                ticker.upper(),
            ]
            + pids
        ),
    ).fetchone()
    total_incomes = float(income_row["total_incomes"]) if income_row else 0.0
    incomes_current_month = float(income_row["incomes_current_month"]) if income_row else 0.0
    incomes_3m = float(income_row["incomes_3m"]) if income_row else 0.0
    incomes_12m = float(income_row["incomes_12m"]) if income_row else 0.0

    return {
        "shares": shares,
        "avg_price": round(avg_price, 2),
        "total_value": round(total_value, 2),
        "market_value": round(market_value, 2),
        "open_pnl_value": round(open_pnl_value, 2),
        "open_pnl_pct": round(open_pnl_pct, 2),
        "total_incomes": round(total_incomes, 2),
        "incomes_current_month": round(incomes_current_month, 2),
        "incomes_3m": round(incomes_3m, 2),
        "incomes_12m": round(incomes_12m, 2),
    }


def get_sectors_summary():
    db = get_db()
    rows = db.execute(
        """
        SELECT
            sector,
            COUNT(*) AS assets_count,
            ROUND(AVG(dy), 2) AS avg_dy,
            ROUND(SUM(market_cap_bi), 2) AS market_cap_bi
        FROM assets
        GROUP BY sector
        ORDER BY market_cap_bi DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


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

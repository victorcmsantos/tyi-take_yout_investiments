"""Market data services."""

import os
import random
import time
from datetime import datetime, timedelta
from math import isfinite

from flask import current_app

from ..db import get_db
from . import _legacy as legacy

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

_UPCOMING_INCOME_CACHE = {}


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _market_data_stale_after_seconds():
    raw = os.getenv("MARKET_DATA_STALE_AFTER_SECONDS") or "43200"
    try:
        return max(int(raw), 60)
    except (TypeError, ValueError):
        return 43200


def _market_data_class_key_from_asset(asset):
    ticker = str((asset or {}).get("ticker") or "").strip().upper()
    sector = str((asset or {}).get("sector") or "").strip().lower()
    if legacy._is_crypto_ticker(ticker):
        return "crypto"
    if legacy._is_us_stock_ticker(ticker):
        return "us"
    if ticker.endswith("11") or "fii" in sector or "fundo imobili" in sector:
        return "fiis"
    return "br"


def _market_data_stale_after_seconds_for_class(class_key: str):
    default_seconds = _market_data_stale_after_seconds()
    env_name = {
        "br": "MARKET_DATA_STALE_AFTER_SECONDS_BR",
        "fiis": "MARKET_DATA_STALE_AFTER_SECONDS_FIIS",
        "us": "MARKET_DATA_STALE_AFTER_SECONDS_US",
        "crypto": "MARKET_DATA_STALE_AFTER_SECONDS_CRYPTO",
    }.get(str(class_key or "").strip().lower())
    if not env_name:
        return default_seconds
    raw = os.getenv(env_name, "").strip()
    if not raw:
        return default_seconds
    try:
        return max(int(raw), 60)
    except (TypeError, ValueError):
        return default_seconds


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


def _upcoming_income_cache_ttl_seconds():
    raw = (os.getenv("UPCOMING_INCOME_CACHE_TTL_SECONDS") or "1800").strip()
    try:
        return max(int(raw), 60)
    except (TypeError, ValueError):
        return 1800


def _upcoming_income_db_cache_ttl_seconds():
    raw = (
        os.getenv("UPCOMING_INCOME_DB_CACHE_TTL_SECONDS")
        or os.getenv("UPCOMING_INCOME_CACHE_TTL_SECONDS")
        or "1800"
    ).strip()
    try:
        return max(int(raw), 60)
    except (TypeError, ValueError):
        return 1800


def _upcoming_income_cache_get(cache_key):
    cached = _UPCOMING_INCOME_CACHE.get(cache_key)
    if not cached:
        return None
    expires_at = float(cached.get("expires_at") or 0.0)
    if expires_at <= time.time():
        _UPCOMING_INCOME_CACHE.pop(cache_key, None)
        return None
    payload = cached.get("payload") or []
    return [dict(item) for item in payload if isinstance(item, dict)]


def _upcoming_income_cache_set(cache_key, payload):
    _UPCOMING_INCOME_CACHE[cache_key] = {
        "expires_at": time.time() + _upcoming_income_cache_ttl_seconds(),
        "payload": [dict(item) for item in (payload or []) if isinstance(item, dict)],
    }
    if len(_UPCOMING_INCOME_CACHE) > 500:
        now_ts = time.time()
        expired_keys = [
            key
            for key, value in _UPCOMING_INCOME_CACHE.items()
            if float((value or {}).get("expires_at") or 0.0) <= now_ts
        ]
        for key in expired_keys:
            _UPCOMING_INCOME_CACHE.pop(key, None)


def _coerce_date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "to_pydatetime"):
        try:
            return value.to_pydatetime().date()
        except Exception:
            return None
    if hasattr(value, "date") and callable(value.date):
        try:
            return value.date()
        except Exception:
            return None
    raw = str(value).strip()
    if not raw:
        return None
    if " " in raw:
        raw = raw.split(" ", 1)[0]
    if "T" in raw:
        raw = raw.split("T", 1)[0]
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return None


def _coerce_float(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(number):
        return None
    return number


def _upcoming_income_currency(symbol: str):
    return "BRL" if str(symbol or "").strip().upper().endswith(".SA") else "USD"


def _upcoming_income_db_cache_get(ticker: str, max_items: int):
    db = get_db()
    state_row = db.execute(
        """
        SELECT fetched_at, has_events
        FROM upcoming_income_cache_state
        WHERE ticker = ?
        """,
        (ticker,),
    ).fetchone()
    if not state_row:
        return False, []

    fetched_dt = _parse_iso_datetime(state_row["fetched_at"])
    if fetched_dt is None:
        return False, []
    age_seconds = (datetime.utcnow() - fetched_dt).total_seconds()
    if age_seconds > float(_upcoming_income_db_cache_ttl_seconds()):
        return False, []

    has_events = int(state_row["has_events"] or 0) > 0
    if not has_events:
        return True, []

    rows = db.execute(
        """
        SELECT
            ticker,
            symbol,
            income_type,
            ex_date,
            payment_date,
            amount,
            currency,
            source
        FROM upcoming_income_cache_events
        WHERE ticker = ?
        ORDER BY
            COALESCE(ex_date, '9999-12-31') ASC,
            COALESCE(payment_date, '9999-12-31') ASC
        LIMIT ?
        """,
        (ticker, int(max_items)),
    ).fetchall()
    events = []
    for row in rows:
        amount = _coerce_float(row["amount"])
        events.append(
            {
                "ticker": str(row["ticker"] or "").strip().upper(),
                "symbol": str(row["symbol"] or "").strip().upper(),
                "income_type": str(row["income_type"] or "dividendo").strip().lower(),
                "ex_date": row["ex_date"],
                "payment_date": row["payment_date"],
                "amount": round(float(amount), 6) if amount is not None else None,
                "currency": str(row["currency"] or "BRL").strip().upper() or "BRL",
                "source": str(row["source"] or "").strip(),
            }
        )
    return True, events


def _upcoming_income_db_cache_set(ticker: str, events):
    db = get_db()
    now_iso = _now_iso()
    normalized_ticker = str(ticker or "").strip().upper()
    safe_events = [item for item in (events or []) if isinstance(item, dict)]

    db.execute(
        "DELETE FROM upcoming_income_cache_events WHERE ticker = ?",
        (normalized_ticker,),
    )
    for event in safe_events:
        amount = _coerce_float(event.get("amount"))
        db.execute(
            """
            INSERT INTO upcoming_income_cache_events (
                ticker,
                symbol,
                income_type,
                ex_date,
                payment_date,
                amount,
                currency,
                source,
                fetched_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_ticker,
                str(event.get("symbol") or "").strip().upper(),
                str(event.get("income_type") or "dividendo").strip().lower(),
                event.get("ex_date"),
                event.get("payment_date"),
                float(amount) if amount is not None else None,
                str(event.get("currency") or "BRL").strip().upper() or "BRL",
                str(event.get("source") or "").strip(),
                now_iso,
            ),
        )

    db.execute(
        """
        INSERT INTO upcoming_income_cache_state (
            ticker,
            fetched_at,
            has_events
        )
        VALUES (?, ?, ?)
        ON CONFLICT(ticker) DO UPDATE SET
            fetched_at = excluded.fetched_at,
            has_events = excluded.has_events
        """,
        (normalized_ticker, now_iso, 1 if safe_events else 0),
    )

    if random.random() < 0.02:
        cutoff_dt = datetime.utcnow() - timedelta(seconds=max(_upcoming_income_db_cache_ttl_seconds() * 6, 86400))
        cutoff_iso = cutoff_dt.replace(microsecond=0).isoformat() + "Z"
        db.execute(
            """
            DELETE FROM upcoming_income_cache_state
            WHERE fetched_at < ?
            """,
            (cutoff_iso,),
        )
        db.execute(
            """
            DELETE FROM upcoming_income_cache_events
            WHERE ticker NOT IN (
                SELECT ticker FROM upcoming_income_cache_state
            )
            """
        )
    db.commit()


def _upcoming_income_events_from_yfinance(symbol: str, ticker: str, max_items: int):
    if yf is None:
        return []

    events = []
    seen = set()
    today = datetime.utcnow().date()
    currency = _upcoming_income_currency(symbol)
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_symbol = str(symbol or "").strip().upper()

    try:
        yf_ticker = yf.Ticker(normalized_symbol)
    except Exception:
        return []

    # Declared future dividends with amount when available.
    try:
        actions = yf_ticker.actions
    except Exception:
        actions = None
    if actions is not None and hasattr(actions, "iterrows"):
        try:
            iterator = actions.iterrows()
        except Exception:
            iterator = []
        for index, row in iterator:
            ex_date = _coerce_date(index)
            if ex_date is None or ex_date < today:
                continue
            amount = _coerce_float(getattr(row, "get", lambda *_: None)("Dividends"))
            if amount is None or amount <= 0:
                continue
            ex_iso = ex_date.isoformat()
            key = (ex_iso, None, round(amount, 6))
            if key in seen:
                continue
            seen.add(key)
            events.append(
                {
                    "ticker": normalized_ticker,
                    "symbol": normalized_symbol,
                    "income_type": "dividendo",
                    "ex_date": ex_iso,
                    "payment_date": None,
                    "amount": round(amount, 6),
                    "currency": currency,
                    "source": "yfinance_actions",
                }
            )

    # Calendar usually provides the next expected ex-dividend date.
    try:
        calendar = yf_ticker.calendar
    except Exception:
        calendar = None
    if calendar is None:
        calendar = {}
    if not isinstance(calendar, dict) and hasattr(calendar, "to_dict"):
        try:
            calendar = calendar.to_dict()
        except Exception:
            calendar = {}
    if isinstance(calendar, dict):
        ex_date = _coerce_date(calendar.get("Ex-Dividend Date"))
        payment_date = _coerce_date(calendar.get("Dividend Date"))
        if ex_date is not None and ex_date >= today:
            ex_iso = ex_date.isoformat()
            pay_iso = payment_date.isoformat() if payment_date is not None else None
            existing = next((item for item in events if item.get("ex_date") == ex_iso), None)
            if existing is not None:
                if not existing.get("payment_date") and pay_iso:
                    existing["payment_date"] = pay_iso
            else:
                key = (ex_iso, pay_iso, None)
                if key not in seen:
                    seen.add(key)
                    events.append(
                        {
                            "ticker": normalized_ticker,
                            "symbol": normalized_symbol,
                            "income_type": "dividendo",
                            "ex_date": ex_iso,
                            "payment_date": pay_iso,
                            "amount": None,
                            "currency": currency,
                            "source": "yfinance_calendar",
                        }
                    )

    events.sort(key=lambda item: ((item.get("ex_date") or "9999-12-31"), (item.get("payment_date") or "9999-12-31")))
    return events[:max_items]


def get_asset_upcoming_incomes(
    ticker: str,
    max_items: int = 8,
    allow_live_fetch: bool = True,
    refresh_if_empty_cache: bool = True,
):
    normalized_ticker = str(ticker or "").strip().upper()
    if not normalized_ticker:
        return []
    try:
        max_count = max(1, min(int(max_items), 20))
    except (TypeError, ValueError):
        max_count = 8

    cache_key = (normalized_ticker, max_count)
    cached = _upcoming_income_cache_get(cache_key)
    if cached is not None:
        if cached or (not allow_live_fetch) or (not refresh_if_empty_cache):
            return cached
    try:
        db_hit, db_cached = _upcoming_income_db_cache_get(normalized_ticker, max_count)
    except Exception:
        current_app.logger.exception(
            "Falha ao ler cache compartilhado de proventos futuros para %s.",
            normalized_ticker,
        )
        db_hit, db_cached = False, []
    if db_hit:
        _upcoming_income_cache_set(cache_key, db_cached)
        if db_cached or (not allow_live_fetch) or (not refresh_if_empty_cache):
            return db_cached
    if not allow_live_fetch:
        return []

    symbols = list(legacy._candidate_yahoo_symbols(normalized_ticker))
    if legacy._is_brazilian_market_ticker(normalized_ticker):
        br_symbols = [symbol for symbol in symbols if str(symbol or "").strip().upper().endswith(".SA")]
        if br_symbols:
            symbols = br_symbols

    events = []
    for symbol in symbols:
        events = _upcoming_income_events_from_yfinance(symbol, normalized_ticker, max_count)
        if events:
            break

    try:
        _upcoming_income_db_cache_set(normalized_ticker, events)
    except Exception:
        current_app.logger.exception(
            "Falha ao atualizar cache compartilhado de proventos futuros para %s.",
            normalized_ticker,
        )
    _upcoming_income_cache_set(cache_key, events)
    return events


def prefetch_upcoming_incomes_for_portfolios(
    portfolio_ids=None,
    max_items_per_ticker: int = 8,
    limit_tickers: int | None = None,
):
    pids = legacy.normalize_portfolio_ids(portfolio_ids or [])
    if not pids:
        return {
            "portfolio_ids": [],
            "tickers_selected": 0,
            "tickers_with_events": 0,
            "events_found": 0,
        }

    placeholders = ",".join(["?"] * len(pids))
    rows = get_db().execute(
        """
        SELECT
            ticker,
            SUM(CASE WHEN tx_type = 'buy' THEN shares ELSE -shares END) AS shares
        FROM transactions
        WHERE portfolio_id IN ("""
        + placeholders
        + """)
        GROUP BY ticker
        HAVING shares > 0
        ORDER BY ticker ASC
        """,
        tuple(pids),
    ).fetchall()

    tickers = []
    for row in rows:
        ticker = str(row["ticker"] or "").strip().upper()
        if not ticker:
            continue
        if not legacy._is_brazilian_market_ticker(ticker):
            continue
        tickers.append(ticker)

    if limit_tickers is not None:
        try:
            safe_limit = int(limit_tickers)
        except (TypeError, ValueError):
            safe_limit = 0
        if safe_limit > 0:
            tickers = tickers[:safe_limit]

    tickers_with_events = 0
    events_found = 0
    for ticker in tickers:
        events = get_asset_upcoming_incomes(
            ticker,
            max_items=max_items_per_ticker,
            allow_live_fetch=True,
            refresh_if_empty_cache=True,
        )
        if events:
            tickers_with_events += 1
            events_found += len(events)

    return {
        "portfolio_ids": pids,
        "tickers_selected": len(tickers),
        "tickers_with_events": int(tickers_with_events),
        "events_found": int(events_found),
    }


def _market_data_meta_from_asset(asset):
    if not asset:
        return {
            "status": "unknown",
            "source": "",
            "updated_at": None,
            "last_attempt_at": None,
            "last_error": "",
            "age_seconds": None,
            "class_key": "br",
            "stale_after_seconds": _market_data_stale_after_seconds(),
            "is_stale": True,
            "is_live": False,
            "stale_reason": "missing_asset",
            "providers_tried": [],
            "fallback_used": False,
        }

    class_key = _market_data_class_key_from_asset(asset)
    stale_after_seconds = _market_data_stale_after_seconds_for_class(class_key)
    updated_at = asset.get("market_data_updated_at")
    updated_dt = _parse_iso_datetime(updated_at)
    age_seconds = None
    if updated_dt is not None:
        age_seconds = max(int((datetime.utcnow() - updated_dt).total_seconds()), 0)

    status = (asset.get("market_data_status") or "unknown").strip().lower()
    is_stale = status in {"stale", "failed", "unknown"} or updated_dt is None
    if age_seconds is not None and age_seconds > stale_after_seconds:
        is_stale = True
    stale_reason = "fresh"
    if status in {"stale", "failed", "unknown"}:
        stale_reason = f"status_{status}"
    elif updated_dt is None:
        stale_reason = "missing_updated_at"
    elif age_seconds is not None and age_seconds > stale_after_seconds:
        stale_reason = "stale_ttl_expired"

    providers_tried = [
        item.strip()
        for item in str(asset.get("market_data_provider_trace") or "").split(",")
        if item and item.strip()
    ]

    return {
        "status": status or "unknown",
        "source": (asset.get("market_data_source") or "").strip(),
        "updated_at": updated_at,
        "last_attempt_at": asset.get("market_data_last_attempt_at"),
        "last_error": (asset.get("market_data_last_error") or "").strip(),
        "age_seconds": age_seconds,
        "class_key": class_key,
        "stale_after_seconds": stale_after_seconds,
        "is_stale": bool(is_stale),
        "is_live": not bool(is_stale),
        "stale_reason": stale_reason,
        "providers_tried": providers_tried,
        "fallback_used": bool(int(asset.get("market_data_fallback_used") or 0)),
    }


def _serialize_asset(asset):
    if not asset:
        return None
    item = dict(asset)
    item["market_data"] = _market_data_meta_from_asset(item)
    return item


def _record_market_data_sync_audit(
    *,
    ticker: str,
    success: bool,
    scope: str,
    providers_tried: str,
    metrics_source: str,
    profile_source: str,
    fallback_used: bool,
    market_data_status: str,
    error_message: str,
    price,
    attempted_at: str | None = None,
):
    db = get_db()
    db.execute(
        """
        INSERT INTO market_data_sync_audit (
            ticker,
            attempted_at,
            success,
            scope,
            providers_tried,
            metrics_source,
            profile_source,
            fallback_used,
            market_data_status,
            error_message,
            price
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(ticker or "").strip().upper(),
            attempted_at or _now_iso(),
            1 if success else 0,
            str(scope or "asset").strip().lower() or "asset",
            str(providers_tried or "").strip(),
            str(metrics_source or "").strip(),
            str(profile_source or "").strip(),
            1 if fallback_used else 0,
            str(market_data_status or "unknown").strip().lower() or "unknown",
            str(error_message or "").strip(),
            float(price) if price is not None else None,
        ),
    )
    db.commit()


def _mark_asset_market_data_failed(
    ticker: str,
    error_message: str,
    *,
    providers_tried: str = "",
    metrics_source: str = "",
    profile_source: str = "",
    fallback_used: bool = False,
):
    db = get_db()
    attempted_at = _now_iso()
    db.execute(
        """
        UPDATE assets
        SET
            market_data_status = 'stale',
            market_data_last_attempt_at = ?,
            market_data_last_error = ?
        WHERE ticker = ?
        """,
        (attempted_at, (error_message or "").strip(), (ticker or "").strip().upper()),
    )
    db.commit()
    _record_market_data_sync_audit(
        ticker=ticker,
        success=False,
        scope="asset",
        providers_tried=providers_tried,
        metrics_source=metrics_source,
        profile_source=profile_source,
        fallback_used=fallback_used,
        market_data_status="stale",
        error_message=(error_message or "").strip(),
        price=None,
        attempted_at=attempted_at,
    )


def get_top_assets():
    db = get_db()
    rows = db.execute("SELECT * FROM assets ORDER BY market_cap_bi DESC").fetchall()
    return [_serialize_asset(row) for row in rows]


def get_asset(ticker: str):
    db = get_db()
    row = db.execute(
        "SELECT * FROM assets WHERE ticker = ?",
        ((ticker or "").upper(),),
    ).fetchone()
    return _serialize_asset(row)


def _asset_price_history_cache_ttl_seconds(range_key: str):
    mapping = {
        "1d": 60,
        "7d": 300,
        "30d": 900,
        "6m": 1800,
        "1y": 3600,
        "5y": 21600,
    }
    return mapping.get((range_key or "1y").strip().lower(), 900)


def get_asset_price_history(ticker: str, range_key: str = "1y"):
    normalized_range = legacy._history_config(range_key)[0]
    cache_key = ((ticker or "").strip().upper(), normalized_range)
    cached = legacy._memory_cache_get(legacy._ASSET_PRICE_HISTORY_CACHE, cache_key)
    if cached is not None:
        return dict(cached)

    history, history_source = legacy._fetch_market_history(ticker, range_key)
    legacy._memory_cache_set(
        legacy._ASSET_PRICE_HISTORY_CACHE,
        cache_key,
        dict(history),
        _asset_price_history_cache_ttl_seconds(normalized_range),
    )
    if legacy._is_truthy_env("MARKET_DATA_LOG_SOURCES", "0"):
        logger = legacy._LOGGER
        try:
            logger = current_app.logger
        except Exception:
            pass
        logger.info(
            "market_history_source ticker=%s history_source=%s providers=%s range_key=%s points=%s",
            (ticker or "").strip().upper(),
            history_source or "none",
            legacy._market_data_provider_label(ticker),
            history.get("range_key") or normalized_range,
            len(history.get("prices") or []),
        )
    return history


def refresh_asset_market_data(
    ticker: str,
    include_scanner_br: bool = True,
    preferred_provider: str | None = None,
):
    asset = get_asset(ticker)
    if not asset:
        return False
    providers_tried = legacy._market_data_provider_label(
        ticker,
        include_scanner_br=include_scanner_br,
    )
    profile_source = None
    metrics_source = None

    if str(preferred_provider or "").strip().lower() == "market_scanner":
        profile = legacy._fetch_market_scanner_profile(ticker) or {}
        profile_source = "market_scanner" if profile else None
        metrics = legacy._fetch_market_scanner_metrics(ticker) or {}
        metrics_source = "market_scanner" if legacy._has_market_metrics(metrics) else None
    else:
        profile, profile_source = legacy._fetch_market_profile(
            ticker,
            include_scanner_br=include_scanner_br,
        )
        metrics, metrics_source = legacy._fetch_market_metrics(
            ticker,
            include_scanner_br=include_scanner_br,
        )
    if not metrics and not profile:
        _mark_asset_market_data_failed(
            ticker,
            "Nenhum provider retornou dados de mercado.",
            providers_tried=providers_tried,
            metrics_source=str(metrics_source or ""),
            profile_source=str(profile_source or ""),
            fallback_used=False,
        )
        return False
    has_market_metrics = legacy._has_market_metrics(metrics)
    attempted_at = _now_iso()
    market_data_status = "fresh" if has_market_metrics else "stale"
    market_data_updated_at = attempted_at if has_market_metrics else asset.get("market_data_updated_at")
    market_data_source = metrics_source or asset.get("market_data_source", "")
    market_data_last_error = "" if has_market_metrics else "Atualizacao sem metricas novas."
    metrics_order = legacy._market_data_provider_order(
        "metrics",
        ticker,
        include_scanner_br=include_scanner_br,
    )
    fallback_used = bool(
        has_market_metrics
        and metrics_source
        and metrics_order
        and str(metrics_source).strip().lower() != str(metrics_order[0]).strip().lower()
    )

    db = get_db()
    name = asset["name"]
    sector = asset["sector"]
    logo_url = asset.get("logo_url", "")
    if profile:
        name = profile.get("name") or name
        sector = profile.get("sector") or sector
        logo_url = profile.get("logo_url") or logo_url

    baseline_metrics = {
        "price": metrics.get("price") if metrics.get("price") is not None else asset["price"],
        "dy": metrics.get("dy") if metrics.get("dy") is not None else asset["dy"],
        "pl": metrics.get("pl") if metrics.get("pl") is not None else asset["pl"],
        "pvp": metrics.get("pvp") if metrics.get("pvp") is not None else asset["pvp"],
        "variation_day": (
            metrics.get("variation_day")
            if metrics.get("variation_day") is not None
            else asset["variation_day"]
        ),
        "variation_7d": (
            metrics.get("variation_7d")
            if metrics.get("variation_7d") is not None
            else asset.get("variation_7d", 0.0)
        ),
        "variation_30d": (
            metrics.get("variation_30d")
            if metrics.get("variation_30d") is not None
            else asset.get("variation_30d", 0.0)
        ),
        "market_cap_bi": (
            metrics.get("market_cap_bi")
            if metrics.get("market_cap_bi") is not None
            else asset["market_cap_bi"]
        ),
    }
    legacy._upsert_asset_metric_baseline(db, ticker.upper(), baseline_metrics, updated_at=attempted_at)
    from . import scanner  # local import to avoid package import-cycle during bootstrap

    metric_formula_map = scanner._get_metric_formula_map(db)
    applied_metrics = scanner._apply_metric_formulas_to_values(baseline_metrics, metric_formula_map)

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
            market_data_last_error = ?,
            market_data_provider_trace = ?,
            market_data_fallback_used = ?
        WHERE ticker = ?
        """,
        (
            name,
            sector,
            applied_metrics["price"],
            applied_metrics["dy"],
            applied_metrics["pl"],
            applied_metrics["pvp"],
            applied_metrics["variation_day"],
            applied_metrics["variation_7d"],
            applied_metrics["variation_30d"],
            applied_metrics["market_cap_bi"],
            logo_url,
            market_data_status,
            market_data_source,
            market_data_updated_at,
            attempted_at,
            market_data_last_error,
            providers_tried,
            1 if fallback_used else 0,
            ticker.upper(),
        ),
    )
    db.commit()
    _record_market_data_sync_audit(
        ticker=ticker,
        success=bool(has_market_metrics),
        scope="asset",
        providers_tried=providers_tried,
        metrics_source=str(metrics_source or ""),
        profile_source=str(profile_source or ""),
        fallback_used=fallback_used,
        market_data_status=market_data_status,
        error_message=market_data_last_error,
        price=applied_metrics.get("price"),
        attempted_at=attempted_at,
    )
    if legacy._is_truthy_env("MARKET_DATA_LOG_SOURCES", "0"):
        logger = legacy._LOGGER
        try:
            logger = current_app.logger
        except Exception:
            pass
        logger.info(
            "market_data_source ticker=%s providers=%s metrics_source=%s profile_source=%s price=%s dy=%s pl=%s pvp=%s variation_day=%s variation_7d=%s variation_30d=%s market_cap_bi=%s",
            ticker.upper(),
            legacy._market_data_provider_label(
                ticker,
                include_scanner_br=include_scanner_br,
            ),
            metrics_source or "none",
            profile_source or "none",
            applied_metrics.get("price"),
            applied_metrics.get("dy"),
            applied_metrics.get("pl"),
            applied_metrics.get("pvp"),
            applied_metrics.get("variation_day"),
            applied_metrics.get("variation_7d"),
            applied_metrics.get("variation_30d"),
            applied_metrics.get("market_cap_bi"),
        )
    return has_market_metrics


def refresh_market_data_for_tickers(
    tickers,
    attempts: int = 2,
    include_scanner_br: bool = True,
):
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
        current_batch = sorted(failed)
        legacy._prefetch_brapi_market_data_for_tickers(current_batch)
        for ticker in current_batch:
            try:
                ok = refresh_asset_market_data(
                    ticker,
                    include_scanner_br=include_scanner_br,
                )
            except Exception as exc:
                _mark_asset_market_data_failed(ticker, str(exc))
                ok = False
            if not ok:
                next_failed.add(ticker)
            time.sleep(0.12 + random.random() * 0.12)
        failed = next_failed
        if failed and attempt < attempts - 1:
            time.sleep(0.5 * (attempt + 1))
    return sorted(failed)


def _normalize_scope_key(scope_key: str):
    key = str(scope_key or "all").strip().lower()
    if key in {"", "all", "todos"}:
        return "all"
    if key in {"br", "b3", "brasil"}:
        return "br"
    if key in {"us", "usa", "eua"}:
        return "us"
    if key in {"crypto", "cripto"}:
        return "crypto"
    raise ValueError("Escopo invalido. Use: all, br, us, crypto.")


def _ticker_matches_scope(ticker: str, scope_key: str):
    if scope_key == "all":
        return True
    return legacy._market_data_class_key(ticker) == scope_key


def refresh_assets_market_data(
    scope_key: str = "all",
    stale_only: bool = False,
    attempts: int = 3,
    include_scanner_br: bool = True,
):
    normalized_scope = _normalize_scope_key(scope_key)
    failed = _refresh_assets_market_data_by_scope(
        attempts=attempts,
        stale_only=stale_only,
        scope_key=normalized_scope,
        include_scanner_br=include_scanner_br,
    )
    return {
        "scope": normalized_scope,
        "stale_only": bool(stale_only),
        "selected_count": len(failed["selected"]),
        "failed": list(failed["failed"]),
    }


def refresh_all_assets_market_data(
    attempts: int = 3,
    scope_key: str = "all",
    include_scanner_br: bool = True,
):
    result = refresh_assets_market_data(
        scope_key=scope_key,
        stale_only=False,
        attempts=attempts,
        include_scanner_br=include_scanner_br,
    )
    return result["failed"]


def refresh_stale_assets_market_data(
    attempts: int = 3,
    scope_key: str = "all",
    include_scanner_br: bool = True,
):
    result = refresh_assets_market_data(
        scope_key=scope_key,
        stale_only=True,
        attempts=attempts,
        include_scanner_br=include_scanner_br,
    )
    return result["failed"]


def _refresh_assets_market_data_by_scope(
    attempts: int = 3,
    stale_only: bool = False,
    scope_key: str = "all",
    include_scanner_br: bool = True,
):
    db = get_db()
    rows = db.execute(
        """
        SELECT
            ticker,
            market_data_status,
            market_data_source,
            market_data_updated_at,
            market_data_last_attempt_at,
            market_data_last_error
        FROM assets
        """
    ).fetchall()
    tickers = []
    for row in rows:
        item = dict(row)
        ticker = str(item.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        if not _ticker_matches_scope(ticker, scope_key):
            continue
        if stale_only and not _market_data_meta_from_asset(item)["is_stale"]:
            continue
        tickers.append(ticker)
    if not tickers:
        return {"selected": [], "failed": []}
    failed = refresh_market_data_for_tickers(
        tickers,
        attempts=attempts,
        include_scanner_br=include_scanner_br,
    )
    return {"selected": tickers, "failed": failed}


__all__ = [
    "get_asset",
    "get_asset_upcoming_incomes",
    "get_asset_price_history",
    "get_top_assets",
    "prefetch_upcoming_incomes_for_portfolios",
    "refresh_assets_market_data",
    "refresh_all_assets_market_data",
    "refresh_asset_market_data",
    "refresh_stale_assets_market_data",
]

"""Market data services."""

import os
import random
import time
from datetime import datetime

from flask import current_app

from ..db import get_db
from . import _legacy as legacy


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


def refresh_asset_market_data(ticker: str, include_scanner_br: bool = True):
    asset = get_asset(ticker)
    if not asset:
        return False

    profile, profile_source = legacy._fetch_market_profile(
        ticker,
        include_scanner_br=include_scanner_br,
    )
    metrics, metrics_source = legacy._fetch_market_metrics(
        ticker,
        include_scanner_br=include_scanner_br,
    )
    if not metrics and not profile:
        _mark_asset_market_data_failed(ticker, "Nenhum provider retornou dados de mercado.")
        return False
    has_market_metrics = legacy._has_market_metrics(metrics)
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
            market_data_last_error = ?
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
            ticker.upper(),
        ),
    )
    db.commit()
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
    "get_asset_price_history",
    "get_top_assets",
    "refresh_assets_market_data",
    "refresh_all_assets_market_data",
    "refresh_asset_market_data",
    "refresh_stale_assets_market_data",
]

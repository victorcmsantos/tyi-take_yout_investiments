"""Compatibility helpers extracted from the legacy services module."""

import os

from . import _legacy as legacy


def _prefetch_brapi_market_data_for_tickers(tickers):
    candidates = []
    seen = set()
    for item in tickers or []:
        ticker = legacy._normalize_brapi_symbol(item)
        if not ticker or ticker in seen:
            continue
        if not legacy._is_brazilian_market_ticker(ticker):
            continue
        if "brapi" not in legacy._market_data_providers_from_env(ticker):
            continue
        seen.add(ticker)
        candidates.append(ticker)
    if not candidates:
        return

    legacy._fetch_brapi_quote_results_batch(candidates)
    legacy._fetch_brapi_quote_results_batch(candidates, modules=["summaryProfile"])
    _, history_cfg = legacy._history_config_for_brapi("30d")
    legacy._fetch_brapi_quote_results_batch(
        candidates,
        range_key=history_cfg["range"],
        interval=history_cfg["interval"],
    )


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


def _market_data_provider_label(ticker: str = ""):
    return ",".join(legacy._market_data_providers_from_env(ticker))


def _is_truthy_env(name: str, default: str = "0"):
    value = (os.getenv(name, default) or default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _fetch_market_profile(ticker: str):
    for provider in legacy._market_data_provider_order("profile", ticker):
        try:
            if provider == "alpha_vantage":
                profile = legacy._fetch_alpha_vantage_profile(ticker)
            elif provider == "twelve_data":
                profile = None
            elif provider == "coingecko":
                profile = legacy._fetch_coingecko_profile(ticker)
            elif provider == "brapi":
                profile = legacy._fetch_brapi_profile(ticker)
            elif provider == "yahoo":
                profile = legacy._fetch_yahoo_profile(ticker)
            else:
                profile = None
        except Exception:
            profile = None
        if profile and any((profile.get("name"), profile.get("sector"))):
            return profile, provider
    return {}, None


def _fetch_market_metrics(ticker: str):
    for provider in legacy._market_data_provider_order("metrics", ticker):
        try:
            if provider == "alpha_vantage":
                metrics = legacy._fetch_alpha_vantage_metrics(ticker)
            elif provider == "twelve_data":
                metrics = legacy._fetch_twelve_data_metrics(ticker)
            elif provider == "coingecko":
                metrics = legacy._fetch_coingecko_metrics(ticker)
            elif provider == "brapi":
                metrics = legacy._fetch_brapi_metrics(ticker)
            elif provider == "google":
                metrics = legacy._fetch_google_metrics(ticker)
            else:
                metrics = legacy._fetch_yahoo_metrics(ticker)
        except Exception:
            metrics = None
        if _has_market_metrics(metrics):
            return metrics, provider
    return {}, None


def _fetch_market_history(ticker: str, range_key: str):
    for provider in legacy._market_data_provider_order("history", ticker):
        try:
            if provider == "alpha_vantage":
                history = legacy._fetch_alpha_vantage_history(ticker, range_key)
            elif provider == "twelve_data":
                history = legacy._fetch_twelve_data_history(ticker, range_key)
            elif provider == "coingecko":
                history = legacy._fetch_coingecko_history(ticker, range_key)
            elif provider == "brapi":
                history = legacy._fetch_brapi_history(ticker, range_key)
            elif provider == "yahoo":
                history = legacy._get_yahoo_asset_price_history(ticker, range_key)
            else:
                history = None
        except Exception:
            history = None
        if history and history.get("prices"):
            return history, provider
    return {
        "range_key": legacy._history_config(range_key)[0],
        "labels": [],
        "prices": [],
        "change_pct": None,
    }, None

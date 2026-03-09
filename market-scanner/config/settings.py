"""Application settings and environment parsing."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    dotenv_path = ROOT_DIR / ".env"
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _get_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _get_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    return float(raw)


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


def _get_list(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(slots=True)
class MetricSettings:
    rsi_length: int = 14
    volume_window: int = 20
    breakout_window: int = 20
    sma_short_length: int = 21
    sma_long_length: int = 200
    bollinger_length: int = 20
    bollinger_std: float = 2.0
    atr_length: int = 14
    momentum_length: int = 20
    trend_length: int = 14
    range_window: int = 20
    higher_high_window: int = 10
    volatility_short_window: int = 20
    volatility_long_window: int = 60
    momentum_90_length: int = 90
    high_52w_window: int = 252
    relative_strength_window: int = 90


@dataclass(slots=True)
class SignalRuleSettings:
    rsi_threshold: float = 55.0
    volume_spike_threshold: float = 1.8
    breakout_threshold: float = 0.0
    min_score: float = 0.0
    min_triggered_metrics: int = 1


@dataclass(slots=True)
class ScoringSettings:
    momentum_weight: float = 0.30
    trend_strength_weight: float = 0.25
    breakout_strength_weight: float = 0.20
    volume_spike_weight: float = 0.15
    volatility_contraction_weight: float = 0.10


@dataclass(slots=True)
class TradeLevelSettings:
    entry_band_atr_multiplier: float = 0.5
    target_atr_multiplier: float = 2.0
    stop_atr_multiplier: float = 1.0
    fallback_entry_pct: float = 0.01
    fallback_target_pct: float = 0.04
    fallback_stop_pct: float = 0.02


@dataclass(slots=True)
class AppSettings:
    app_env: str = "production"
    log_level: str = "INFO"
    database_url: str = "sqlite:///./data/market_scanner.db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    scan_interval_hours: int = 3
    price_interval: str = "1d"
    price_period: str = "1y"
    download_batch_size: int = 50
    brapi_base_url: str = "https://brapi.dev/api"
    brapi_token: str = ""
    brapi_timeout_seconds: int = 30
    brapi_max_tickers_per_request: int = 10
    benchmark_symbol: str = "^BVSP"
    active_signal_hours: int = 72
    ticker_cache_ttl_hours: int = 24
    auto_discover_b3_tickers: bool = True
    start_scheduler_with_api: bool = True
    immediate_scan_on_startup: bool = True
    b3_page_size: int = 200
    manual_tickers: list[str] = field(default_factory=list)
    signal_rules: SignalRuleSettings = field(default_factory=SignalRuleSettings)
    scoring: ScoringSettings = field(default_factory=ScoringSettings)
    trade_levels: TradeLevelSettings = field(default_factory=TradeLevelSettings)
    metrics: MetricSettings = field(default_factory=MetricSettings)

    @property
    def data_dir(self) -> Path:
        path = ROOT_DIR / "data"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def templates_dir(self) -> Path:
        return ROOT_DIR / "dashboard" / "templates"

    @property
    def static_dir(self) -> Path:
        return ROOT_DIR / "dashboard" / "static"

    @property
    def ticker_cache_file(self) -> Path:
        return self.data_dir / "b3_tickers_cache.json"


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    _load_dotenv()
    return AppSettings(
        app_env=_get_env("APP_ENV", "production"),
        log_level=_get_env("LOG_LEVEL", "INFO").upper(),
        database_url=_get_env("DATABASE_URL", "sqlite:///./data/market_scanner.db"),
        api_host=_get_env("API_HOST", "0.0.0.0"),
        api_port=_get_int("API_PORT", 8000),
        scan_interval_hours=_get_int("SCAN_INTERVAL_HOURS", 3),
        price_interval=_get_env("PRICE_INTERVAL", "1d"),
        price_period=_get_env("PRICE_PERIOD", "1y"),
        download_batch_size=_get_int("DOWNLOAD_BATCH_SIZE", 50),
        brapi_base_url=_get_env("BRAPI_BASE_URL", "https://brapi.dev/api"),
        brapi_token=_get_env("BRAPI_TOKEN", ""),
        brapi_timeout_seconds=_get_int("BRAPI_TIMEOUT_SECONDS", 30),
        brapi_max_tickers_per_request=_get_int("BRAPI_MAX_TICKERS_PER_REQUEST", 10),
        benchmark_symbol=_get_env("BENCHMARK_SYMBOL", "^BVSP"),
        active_signal_hours=_get_int("ACTIVE_SIGNAL_HOURS", 72),
        ticker_cache_ttl_hours=_get_int("TICKER_CACHE_TTL_HOURS", 24),
        auto_discover_b3_tickers=_get_bool("AUTO_DISCOVER_B3_TICKERS", True),
        start_scheduler_with_api=_get_bool("START_SCHEDULER_WITH_API", True),
        immediate_scan_on_startup=_get_bool("IMMEDIATE_SCAN_ON_STARTUP", True),
        b3_page_size=_get_int("B3_PAGE_SIZE", 200),
        manual_tickers=_get_list("MANUAL_TICKERS", []),
        signal_rules=SignalRuleSettings(
            rsi_threshold=_get_float("RULE_RSI_THRESHOLD", 55.0),
            volume_spike_threshold=_get_float("RULE_VOLUME_SPIKE_THRESHOLD", 1.8),
            breakout_threshold=_get_float("RULE_BREAKOUT_THRESHOLD", 0.0),
            min_score=_get_float("RULE_MIN_SCORE", 0.0),
            min_triggered_metrics=_get_int("RULE_MIN_TRIGGERED_METRICS", 1),
        ),
        scoring=ScoringSettings(
            momentum_weight=_get_float("SCORE_MOMENTUM_WEIGHT", 0.30),
            trend_strength_weight=_get_float("SCORE_TREND_STRENGTH_WEIGHT", 0.25),
            breakout_strength_weight=_get_float("SCORE_BREAKOUT_STRENGTH_WEIGHT", 0.20),
            volume_spike_weight=_get_float("SCORE_VOLUME_SPIKE_WEIGHT", 0.15),
            volatility_contraction_weight=_get_float(
                "SCORE_VOLATILITY_CONTRACTION_WEIGHT",
                0.10,
            ),
        ),
        trade_levels=TradeLevelSettings(
            entry_band_atr_multiplier=_get_float("TRADE_ENTRY_BAND_ATR_MULTIPLIER", 0.5),
            target_atr_multiplier=_get_float("TRADE_TARGET_ATR_MULTIPLIER", 2.0),
            stop_atr_multiplier=_get_float("TRADE_STOP_ATR_MULTIPLIER", 1.0),
            fallback_entry_pct=_get_float("TRADE_FALLBACK_ENTRY_PCT", 0.01),
            fallback_target_pct=_get_float("TRADE_FALLBACK_TARGET_PCT", 0.04),
            fallback_stop_pct=_get_float("TRADE_FALLBACK_STOP_PCT", 0.02),
        ),
        metrics=MetricSettings(
            rsi_length=_get_int("METRIC_RSI_LENGTH", 14),
            volume_window=_get_int("METRIC_VOLUME_WINDOW", 20),
            breakout_window=_get_int("METRIC_BREAKOUT_WINDOW", 20),
            sma_short_length=_get_int("METRIC_SMA_SHORT_LENGTH", 21),
            sma_long_length=_get_int("METRIC_SMA_LONG_LENGTH", 200),
            bollinger_length=_get_int("METRIC_BOLLINGER_LENGTH", 20),
            bollinger_std=_get_float("METRIC_BOLLINGER_STD", 2.0),
            atr_length=_get_int("METRIC_ATR_LENGTH", 14),
            momentum_length=_get_int("METRIC_MOMENTUM_LENGTH", 20),
            trend_length=_get_int("METRIC_TREND_LENGTH", 14),
            range_window=_get_int("METRIC_RANGE_WINDOW", 20),
            higher_high_window=_get_int("METRIC_HIGHER_HIGH_WINDOW", 10),
            volatility_short_window=_get_int("METRIC_VOLATILITY_SHORT_WINDOW", 20),
            volatility_long_window=_get_int("METRIC_VOLATILITY_LONG_WINDOW", 60),
            momentum_90_length=_get_int("METRIC_MOMENTUM_90_LENGTH", 90),
            high_52w_window=_get_int("METRIC_HIGH_52W_WINDOW", 252),
            relative_strength_window=_get_int("METRIC_RELATIVE_STRENGTH_WINDOW", 90),
        ),
    )

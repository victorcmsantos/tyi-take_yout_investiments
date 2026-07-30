"""Application settings and environment parsing."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
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
    # Gate exigente para virar uma shortlist (antes 1 metrica/score 0 disparava
    # quase tudo; 25 ainda deixava ~150). min_score filtra criacao E exibicao.
    min_score: float = 45.0
    min_triggered_metrics: int = 3
    # Fundamentos (dispara como metrica quando favoravel).
    dividend_yield_threshold: float = 0.05  # DY 12m > 5%
    price_earnings_max: float = 15.0  # 0 < P/L < 15 (barato)


@dataclass(slots=True)
class ScoringSettings:
    momentum_weight: float = 0.12
    momentum_90_weight: float = 0.08
    trend_strength_weight: float = 0.12
    distance_from_sma200_weight: float = 0.06
    relative_strength_weight: float = 0.08
    distance_52w_high_weight: float = 0.06
    higher_high_weight: float = 0.05
    breakout_strength_weight: float = 0.10
    bollinger_position_weight: float = 0.05
    vwap_distance_weight: float = 0.03
    volume_spike_weight: float = 0.08
    volatility_contraction_weight: float = 0.06
    atr_percent_weight: float = 0.04
    range_expansion_weight: float = 0.03
    rsi_weight: float = 0.04
    # Bonus fundamentalista ADITIVO (pontos, nao peso) sobre o score tecnico;
    # so aplica quando o dado existe. Total final e limitado a 100.
    dividend_yield_points: float = 10.0
    price_earnings_points: float = 8.0


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
    fundamentals_enabled: bool = True
    fundamentals_ttl_hours: int = 24
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_thread_id: str = ""
    alerts_enabled: bool = True
    alert_min_score: float = 60.0
    alert_max_per_scan: int = 10
    # Robo diario de paper trading do setup3 (compra 1x/dia os sinais acionados).
    setup3_robot_enabled: bool = False
    setup3_robot_budget: float = 5000.0
    setup3_robot_owner_uid: int = 3
    setup3_robot_hour: int = 16
    setup3_robot_minute: int = 0
    setup3_robot_min_score: float = 0.0
    setup3_robot_today_only: bool = True
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


def _metric_overrides_file(settings: AppSettings) -> Path:
    return settings.data_dir / "metric_overrides.json"


def save_metric_overrides(settings: AppSettings) -> None:
    """Persiste os parametros de metricas (editados no Metrics Lab) para
    sobreviverem a reinicios do container."""
    try:
        _metric_overrides_file(settings).write_text(
            json.dumps(asdict(settings.metrics), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        pass


def load_metric_overrides(settings: AppSettings) -> None:
    """Aplica sobre settings.metrics os parametros persistidos, se houver."""
    path = _metric_overrides_file(settings)
    if not path.exists():
        return
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    for key, value in (data or {}).items():
        if not hasattr(settings.metrics, key):
            continue
        current = getattr(settings.metrics, key)
        try:
            setattr(settings.metrics, key, int(value) if isinstance(current, int) else float(value))
        except (TypeError, ValueError):
            continue


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    _load_dotenv()
    settings = AppSettings(
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
        fundamentals_enabled=_get_bool("SCANNER_FUNDAMENTALS_ENABLED", True),
        fundamentals_ttl_hours=_get_int("SCANNER_FUNDAMENTALS_TTL_HOURS", 24),
        telegram_bot_token=_get_env("TELEGRAM_BOT_TOKEN", ""),
        telegram_chat_id=_get_env("TELEGRAM_CHAT_ID", ""),
        telegram_thread_id=_get_env("TELEGRAM_THREAD_ID", ""),
        alerts_enabled=_get_bool("SCANNER_ALERTS_ENABLED", True),
        alert_min_score=_get_float("SCANNER_ALERT_MIN_SCORE", 60.0),
        alert_max_per_scan=_get_int("SCANNER_ALERT_MAX_PER_SCAN", 10),
        setup3_robot_enabled=_get_bool("SETUP3_ROBOT_ENABLED", False),
        setup3_robot_budget=_get_float("SETUP3_ROBOT_BUDGET", 5000.0),
        setup3_robot_owner_uid=_get_int("SETUP3_ROBOT_OWNER_UID", 3),
        setup3_robot_hour=_get_int("SETUP3_ROBOT_HOUR", 16),
        setup3_robot_minute=_get_int("SETUP3_ROBOT_MINUTE", 0),
        setup3_robot_min_score=_get_float("SETUP3_ROBOT_MIN_SCORE", 0.0),
        setup3_robot_today_only=_get_bool("SETUP3_ROBOT_TODAY_ONLY", True),
        manual_tickers=_get_list("MANUAL_TICKERS", []),
        signal_rules=SignalRuleSettings(
            rsi_threshold=_get_float("RULE_RSI_THRESHOLD", 55.0),
            volume_spike_threshold=_get_float("RULE_VOLUME_SPIKE_THRESHOLD", 1.8),
            breakout_threshold=_get_float("RULE_BREAKOUT_THRESHOLD", 0.0),
            min_score=_get_float("RULE_MIN_SCORE", 45.0),
            min_triggered_metrics=_get_int("RULE_MIN_TRIGGERED_METRICS", 3),
            dividend_yield_threshold=_get_float("RULE_DIVIDEND_YIELD_THRESHOLD", 0.05),
            price_earnings_max=_get_float("RULE_PRICE_EARNINGS_MAX", 15.0),
        ),
        scoring=ScoringSettings(
            momentum_weight=_get_float("SCORE_MOMENTUM_WEIGHT", 0.12),
            momentum_90_weight=_get_float("SCORE_MOMENTUM_90_WEIGHT", 0.08),
            trend_strength_weight=_get_float("SCORE_TREND_STRENGTH_WEIGHT", 0.12),
            distance_from_sma200_weight=_get_float("SCORE_DISTANCE_SMA200_WEIGHT", 0.06),
            relative_strength_weight=_get_float("SCORE_RELATIVE_STRENGTH_WEIGHT", 0.08),
            distance_52w_high_weight=_get_float("SCORE_DISTANCE_52W_HIGH_WEIGHT", 0.06),
            higher_high_weight=_get_float("SCORE_HIGHER_HIGH_WEIGHT", 0.05),
            breakout_strength_weight=_get_float("SCORE_BREAKOUT_STRENGTH_WEIGHT", 0.10),
            bollinger_position_weight=_get_float("SCORE_BOLLINGER_POSITION_WEIGHT", 0.05),
            vwap_distance_weight=_get_float("SCORE_VWAP_DISTANCE_WEIGHT", 0.03),
            volume_spike_weight=_get_float("SCORE_VOLUME_SPIKE_WEIGHT", 0.08),
            volatility_contraction_weight=_get_float("SCORE_VOLATILITY_CONTRACTION_WEIGHT", 0.06),
            atr_percent_weight=_get_float("SCORE_ATR_PERCENT_WEIGHT", 0.04),
            range_expansion_weight=_get_float("SCORE_RANGE_EXPANSION_WEIGHT", 0.03),
            rsi_weight=_get_float("SCORE_RSI_WEIGHT", 0.04),
            dividend_yield_points=_get_float("SCORE_DIVIDEND_YIELD_POINTS", 10.0),
            price_earnings_points=_get_float("SCORE_PRICE_EARNINGS_POINTS", 8.0),
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
    load_metric_overrides(settings)
    return settings

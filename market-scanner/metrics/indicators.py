"""Indicator registry for scanner metrics."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Callable

import numpy as np
import pandas as pd
import pandas_ta as ta

from config.settings import MetricSettings


MetricComputer = Callable[[pd.DataFrame, MetricSettings], pd.Series]


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """Configurable metric descriptor."""

    key: str
    label: str
    computer: MetricComputer
    details: str = ""
    formula_template: str = ""
    parameters: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MetricParameterMeta:
    """Editable parameter metadata for UI forms."""

    key: str
    label: str
    description: str
    min_value: float
    max_value: float
    step: float = 1.0


PARAMETER_METADATA: dict[str, MetricParameterMeta] = {
    "rsi_length": MetricParameterMeta(
        key="rsi_length",
        label="Período RSI",
        description="Janela usada no cálculo do RSI.",
        min_value=2,
        max_value=100,
    ),
    "volume_window": MetricParameterMeta(
        key="volume_window",
        label="Janela Volume",
        description="Período da média de volume para detectar spikes.",
        min_value=2,
        max_value=200,
    ),
    "breakout_window": MetricParameterMeta(
        key="breakout_window",
        label="Janela Breakout",
        description="Quantidade de candles para o topo de referência do breakout.",
        min_value=2,
        max_value=250,
    ),
    "sma_long_length": MetricParameterMeta(
        key="sma_long_length",
        label="SMA Longa",
        description="Período da média longa usada na distância da tendência.",
        min_value=20,
        max_value=400,
    ),
    "bollinger_length": MetricParameterMeta(
        key="bollinger_length",
        label="Janela Bollinger",
        description="Período da média central das Bandas de Bollinger.",
        min_value=5,
        max_value=200,
    ),
    "bollinger_std": MetricParameterMeta(
        key="bollinger_std",
        label="Desvio Bollinger",
        description="Multiplicador de desvio padrão das bandas.",
        min_value=0.5,
        max_value=5.0,
        step=0.1,
    ),
    "atr_length": MetricParameterMeta(
        key="atr_length",
        label="Período ATR",
        description="Janela do Average True Range.",
        min_value=2,
        max_value=100,
    ),
    "momentum_length": MetricParameterMeta(
        key="momentum_length",
        label="Janela Momentum",
        description="Período usado no percentual de variação do fechamento.",
        min_value=2,
        max_value=120,
    ),
    "trend_length": MetricParameterMeta(
        key="trend_length",
        label="Período ADX",
        description="Janela do ADX usada na força de tendência.",
        min_value=2,
        max_value=120,
    ),
    "range_window": MetricParameterMeta(
        key="range_window",
        label="Janela Range",
        description="Período médio para expansão de range.",
        min_value=2,
        max_value=120,
    ),
    "higher_high_window": MetricParameterMeta(
        key="higher_high_window",
        label="Janela Higher High",
        description="Número de candles para taxa de topos ascendentes.",
        min_value=2,
        max_value=120,
    ),
    "volatility_short_window": MetricParameterMeta(
        key="volatility_short_window",
        label="Volatilidade Curta",
        description="Janela curta de desvio padrão dos retornos.",
        min_value=2,
        max_value=120,
    ),
    "volatility_long_window": MetricParameterMeta(
        key="volatility_long_window",
        label="Volatilidade Longa",
        description="Janela longa de desvio padrão dos retornos.",
        min_value=5,
        max_value=250,
    ),
    "momentum_90_length": MetricParameterMeta(
        key="momentum_90_length",
        label="Janela Momentum 90",
        description="Período usado no momentum de médio prazo.",
        min_value=10,
        max_value=260,
    ),
    "high_52w_window": MetricParameterMeta(
        key="high_52w_window",
        label="Janela 52W High",
        description="Janela de candles para máxima anual.",
        min_value=100,
        max_value=400,
    ),
    "relative_strength_window": MetricParameterMeta(
        key="relative_strength_window",
        label="Janela Força Relativa",
        description="Janela de comparação de retorno contra o Ibovespa.",
        min_value=10,
        max_value=260,
    ),
}


def compute_rsi(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    return ta.rsi(frame["close"], length=settings.rsi_length)


def compute_volume_spike(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    baseline = frame["volume"].rolling(settings.volume_window).mean()
    return frame["volume"] / baseline


def compute_breakout_20(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    rolling_high = frame["high"].shift(1).rolling(settings.breakout_window).max()
    return (frame["close"] - rolling_high) / rolling_high


def compute_distance_from_sma200(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    return (frame["close"] - frame["sma_200"]) / frame["sma_200"]


def compute_bollinger_position(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    bands = ta.bbands(
        frame["close"],
        length=settings.bollinger_length,
        std=settings.bollinger_std,
    )
    if bands is None or bands.empty:
        return pd.Series(index=frame.index, dtype=float)
    lower = bands.iloc[:, 0]
    upper = bands.iloc[:, 2]
    width = (upper - lower).replace(0, np.nan)
    return (frame["close"] - lower) / width


def compute_atr_percent(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    atr = ta.atr(
        high=frame["high"],
        low=frame["low"],
        close=frame["close"],
        length=settings.atr_length,
    )
    return atr / frame["close"]


def compute_momentum(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    return frame["close"].pct_change(settings.momentum_length)


def compute_trend_strength(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    adx = ta.adx(
        high=frame["high"],
        low=frame["low"],
        close=frame["close"],
        length=settings.trend_length,
    )
    if adx is None or adx.empty:
        return pd.Series(index=frame.index, dtype=float)
    return adx.iloc[:, 0] / 100.0


def compute_vwap_distance(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    return (frame["close"] - frame["vwap"]) / frame["vwap"]


def compute_range_expansion(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    true_range = frame["high"] - frame["low"]
    range_average = true_range.rolling(settings.range_window).mean()
    return true_range / range_average


def compute_higher_high_score(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    higher_high = (frame["high"] > frame["high"].shift(1)).astype(float)
    return higher_high.rolling(settings.higher_high_window).mean()


def compute_volatility_compression(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    short_vol = frame["close"].pct_change().rolling(settings.volatility_short_window).std()
    long_vol = frame["close"].pct_change().rolling(settings.volatility_long_window).std()
    return short_vol / long_vol


def compute_momentum_90(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    return frame["close"].pct_change(settings.momentum_90_length)


def compute_distance_52w_high(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    window = settings.high_52w_window
    min_periods = min(window, max(100, int(window * 0.7)))
    rolling_52w_high = frame["high"].rolling(window, min_periods=min_periods).max()
    return (frame["close"] - rolling_52w_high) / rolling_52w_high


def compute_relative_strength_vs_ibov(frame: pd.DataFrame, settings: MetricSettings) -> pd.Series:
    if "ibov_close" not in frame.columns:
        return pd.Series(index=frame.index, dtype=float)
    ticker_return = frame["close"].pct_change(settings.relative_strength_window, fill_method=None)
    ibov_return = frame["ibov_close"].pct_change(settings.relative_strength_window, fill_method=None)
    return ((1.0 + ticker_return) / (1.0 + ibov_return)) - 1.0


def default_metric_definitions() -> list[MetricDefinition]:
    """Return the default registry of supported metrics."""

    return [
        MetricDefinition(
            "rsi",
            "RSI",
            compute_rsi,
            details="Índice de força relativa para medir sobrecompra/sobrevenda.",
            formula_template="RSI({rsi_length}) = 100 - 100/(1 + média_ganhos/média_perdas)",
            parameters=("rsi_length",),
        ),
        MetricDefinition(
            "volume_spike",
            "Volume Spike",
            compute_volume_spike,
            details="Compara volume atual com média móvel do volume.",
            formula_template="volume_spike = volume / SMA(volume, {volume_window})",
            parameters=("volume_window",),
        ),
        MetricDefinition(
            "breakout_20",
            "Breakout 20",
            compute_breakout_20,
            details="Mede rompimento do fechamento acima da máxima histórica recente.",
            formula_template=(
                "breakout_20 = (close - rolling_max(high.shift(1), {breakout_window})) "
                "/ rolling_max(high.shift(1), {breakout_window})"
            ),
            parameters=("breakout_window",),
        ),
        MetricDefinition(
            "distance_from_sma200",
            "Distance from SMA200",
            compute_distance_from_sma200,
            details="Distância percentual do preço em relação à média longa.",
            formula_template="distance_from_sma200 = (close - SMA({sma_long_length})) / SMA({sma_long_length})",
            parameters=("sma_long_length",),
        ),
        MetricDefinition(
            "bollinger_position",
            "Bollinger Position",
            compute_bollinger_position,
            details="Posição relativa do fechamento dentro das Bandas de Bollinger.",
            formula_template=(
                "bollinger_position = (close - BB_low({bollinger_length},{bollinger_std})) "
                "/ (BB_up({bollinger_length},{bollinger_std}) - BB_low({bollinger_length},{bollinger_std}))"
            ),
            parameters=("bollinger_length", "bollinger_std"),
        ),
        MetricDefinition(
            "atr_percent",
            "ATR Percent",
            compute_atr_percent,
            details="Volatilidade média do ativo normalizada pelo preço de fechamento.",
            formula_template="atr_percent = ATR({atr_length}) / close",
            parameters=("atr_length",),
        ),
        MetricDefinition(
            "momentum",
            "Momentum",
            compute_momentum,
            details="Retorno percentual do fechamento em uma janela fixa.",
            formula_template="momentum = pct_change(close, {momentum_length})",
            parameters=("momentum_length",),
        ),
        MetricDefinition(
            "trend_strength",
            "Trend Strength",
            compute_trend_strength,
            details="Força da tendência usando ADX normalizado em 0-1.",
            formula_template="trend_strength = ADX({trend_length}) / 100",
            parameters=("trend_length",),
        ),
        MetricDefinition(
            "vwap_distance",
            "VWAP Distance",
            compute_vwap_distance,
            details="Distância percentual entre fechamento e VWAP acumulada.",
            formula_template="vwap_distance = (close - vwap) / vwap",
            parameters=(),
        ),
        MetricDefinition(
            "range_expansion",
            "Range Expansion",
            compute_range_expansion,
            details="Mede expansão de range diário versus média da janela.",
            formula_template="range_expansion = (high - low) / SMA(high - low, {range_window})",
            parameters=("range_window",),
        ),
        MetricDefinition(
            "higher_high_score",
            "Higher High Score",
            compute_higher_high_score,
            details="Frequência de topos ascendentes na janela.",
            formula_template="higher_high_score = rolling_mean(high > high.shift(1), {higher_high_window})",
            parameters=("higher_high_window",),
        ),
        MetricDefinition(
            "volatility_compression",
            "Volatility Compression",
            compute_volatility_compression,
            details="Razão entre volatilidade curta e longa dos retornos.",
            formula_template=(
                "volatility_compression = std(pct_change(close), {volatility_short_window}) "
                "/ std(pct_change(close), {volatility_long_window})"
            ),
            parameters=("volatility_short_window", "volatility_long_window"),
        ),
        MetricDefinition(
            "momentum_90",
            "Momentum 90",
            compute_momentum_90,
            details="Retorno percentual de médio prazo do fechamento.",
            formula_template="momentum_90 = pct_change(close, {momentum_90_length})",
            parameters=("momentum_90_length",),
        ),
        MetricDefinition(
            "distance_52w_high",
            "Distance 52W High",
            compute_distance_52w_high,
            details="Distância percentual do preço atual para a máxima de 52 semanas.",
            formula_template=(
                "distance_52w_high = (close - rolling_max(high, {high_52w_window})) "
                "/ rolling_max(high, {high_52w_window})"
            ),
            parameters=("high_52w_window",),
        ),
        MetricDefinition(
            "relative_strength_vs_ibov",
            "Relative Strength vs IBOV",
            compute_relative_strength_vs_ibov,
            details="Força relativa do ativo comparada ao retorno do Ibovespa na mesma janela.",
            formula_template=(
                "relative_strength_vs_ibov = ((1 + ret(close,{relative_strength_window})) "
                "/ (1 + ret(ibov_close,{relative_strength_window}))) - 1"
            ),
            parameters=("relative_strength_window",),
        ),
    ]


def build_metric_catalog(settings: MetricSettings) -> list[dict[str, object]]:
    """Build UI-friendly metric descriptors with formulas and editable params."""

    settings_payload = asdict(settings)
    catalog: list[dict[str, object]] = []
    for definition in default_metric_definitions():
        formatted_formula = definition.formula_template.format(**settings_payload)
        parameter_rows: list[dict[str, object]] = []
        for parameter_key in definition.parameters:
            meta = PARAMETER_METADATA[parameter_key]
            parameter_rows.append(
                {
                    "key": parameter_key,
                    "label": meta.label,
                    "description": meta.description,
                    "value": settings_payload[parameter_key],
                    "min": meta.min_value,
                    "max": meta.max_value,
                    "step": meta.step,
                }
            )
        catalog.append(
            {
                "key": definition.key,
                "label": definition.label,
                "details": definition.details,
                "formula": formatted_formula,
                "parameters": parameter_rows,
            }
        )
    return catalog

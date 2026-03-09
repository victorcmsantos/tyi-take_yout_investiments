"""Metric engine for computing technical indicators."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import isnan

import pandas as pd

from config.settings import AppSettings
from metrics.indicators import MetricDefinition, default_metric_definitions


@dataclass(slots=True)
class MetricComputation:
    """Latest metric snapshot plus the decorated price frame."""

    timestamp: datetime
    metrics: dict[str, float]
    helpers: dict[str, float]
    labels: dict[str, str]
    frame: pd.DataFrame


class MetricEngine:
    """Compute configured metrics for a price frame."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.registry: list[MetricDefinition] = default_metric_definitions()

    def compute(
        self,
        prices: pd.DataFrame,
        benchmark_prices: pd.DataFrame | None = None,
    ) -> MetricComputation | None:
        """Compute metrics and helper series from a normalized OHLCV frame."""

        if prices.empty or len(prices.index) < self.settings.metrics.sma_long_length:
            return None

        frame = prices.copy().sort_index()
        frame["sma_21"] = frame["close"].rolling(self.settings.metrics.sma_short_length).mean()
        frame["sma_200"] = frame["close"].rolling(self.settings.metrics.sma_long_length).mean()
        typical_price = (frame["high"] + frame["low"] + frame["close"]) / 3.0
        cumulative_volume = frame["volume"].cumsum().replace(0, pd.NA)
        frame["vwap"] = (typical_price * frame["volume"]).cumsum() / cumulative_volume
        if benchmark_prices is not None and not benchmark_prices.empty:
            benchmark_close = benchmark_prices["close"].sort_index()
            benchmark_close.index = benchmark_close.index.normalize()
            benchmark_close = benchmark_close[~benchmark_close.index.duplicated(keep="last")]
            ticker_dates = pd.Series(frame.index.normalize(), index=frame.index)
            frame["ibov_close"] = ticker_dates.map(benchmark_close).ffill()

        metric_values: dict[str, float] = {}
        labels: dict[str, str] = {}
        for metric in self.registry:
            frame[metric.key] = metric.computer(frame, self.settings.metrics)
            latest_value = frame[metric.key].iloc[-1]
            if latest_value is None or pd.isna(latest_value) or (isinstance(latest_value, float) and isnan(latest_value)):
                continue
            metric_values[metric.key] = float(latest_value)
            labels[metric.key] = metric.label

        latest = frame.iloc[-1]
        helpers = {
            "close": float(latest["close"]),
            "sma_21": float(latest["sma_21"]),
            "sma_200": float(latest["sma_200"]),
        }
        return MetricComputation(
            timestamp=frame.index[-1].to_pydatetime(),
            metrics=metric_values,
            helpers=helpers,
            labels=labels,
            frame=frame,
        )

    def to_rows(self, symbol: str, computation: MetricComputation) -> list[dict[str, object]]:
        """Transform a metric snapshot into database rows."""

        return [
            {
                "ticker": symbol,
                "timestamp": computation.timestamp,
                "metric_name": key,
                "metric_value": value,
            }
            for key, value in computation.metrics.items()
        ]

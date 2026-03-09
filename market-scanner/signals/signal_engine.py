"""Buy signal evaluation."""

from __future__ import annotations

from dataclasses import dataclass

from config.settings import AppSettings
from metrics.metric_engine import MetricComputation
from signals.scoring import ScoreBreakdown, ScoringEngine


@dataclass(slots=True)
class SignalDecision:
    """Triggered signal payload."""

    ticker: str
    timestamp: object
    price: float
    score: float
    metrics_triggered: list[str]
    breakdown: ScoreBreakdown


class SignalEngine:
    """Evaluate buy conditions against the latest metric snapshot."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self.scoring_engine = ScoringEngine(settings)

    def evaluate(self, ticker: str, computation: MetricComputation) -> SignalDecision | None:
        metrics = computation.metrics
        helpers = computation.helpers

        triggered_metrics = [
            label
            for key, label in computation.labels.items()
            if key in metrics and self._is_positive_metric(key, metrics[key], helpers)
        ]
        if len(triggered_metrics) < self.settings.signal_rules.min_triggered_metrics:
            return None

        breakdown = self.scoring_engine.score(metrics)
        if breakdown.total < self.settings.signal_rules.min_score:
            return None

        return SignalDecision(
            ticker=ticker,
            timestamp=computation.timestamp,
            price=helpers["close"],
            score=breakdown.total,
            metrics_triggered=sorted(set(triggered_metrics)),
            breakdown=breakdown,
        )

    def _is_positive_metric(
        self,
        metric_name: str,
        value: float,
        helpers: dict[str, float],
    ) -> bool:
        if metric_name == "rsi":
            return value > self.settings.signal_rules.rsi_threshold
        if metric_name == "volume_spike":
            return value > self.settings.signal_rules.volume_spike_threshold
        if metric_name == "breakout_20":
            return value > self.settings.signal_rules.breakout_threshold
        if metric_name == "momentum":
            return value > 0
        if metric_name == "trend_strength":
            return value > 0.2
        if metric_name == "distance_from_sma200":
            return value > 0
        if metric_name == "bollinger_position":
            return value > 0.65
        if metric_name == "atr_percent":
            return value < 0.06
        if metric_name == "vwap_distance":
            return value > 0
        if metric_name == "range_expansion":
            return value > 1.1
        if metric_name == "higher_high_score":
            return value > 0.6
        if metric_name == "volatility_compression":
            return value < 1.0
        if metric_name == "momentum_90":
            return value > 0
        if metric_name == "distance_52w_high":
            return value > -0.15
        if metric_name == "relative_strength_vs_ibov":
            return value > 0
        return helpers.get("close", 0.0) > helpers.get("sma_21", 0.0)

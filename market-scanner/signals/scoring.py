"""Signal scoring utilities."""

from __future__ import annotations

from dataclasses import dataclass

from config.settings import AppSettings


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(value, upper))


def _normalize(value: float | None, lower: float, upper: float, inverse: bool = False) -> float:
    if value is None:
        return 0.0
    if upper == lower:
        return 0.0
    ratio = _clamp((value - lower) / (upper - lower))
    return 1.0 - ratio if inverse else ratio


@dataclass(slots=True)
class ScoreBreakdown:
    """Final signal score and component contributions."""

    total: float
    components: dict[str, float]


class ScoringEngine:
    """Weighted metric scoring."""

    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings

    def score(self, metrics: dict[str, float]) -> ScoreBreakdown:
        momentum = _normalize(metrics.get("momentum"), 0.0, 0.25)
        trend_strength = _normalize(metrics.get("trend_strength"), 0.15, 0.6)
        breakout_strength = _normalize(metrics.get("breakout_20"), 0.0, 0.12)
        volume_spike = _normalize(metrics.get("volume_spike"), 1.0, 3.0)
        volatility_contraction = _normalize(
            metrics.get("volatility_compression"),
            0.4,
            1.1,
            inverse=True,
        )

        components = {
            "momentum": momentum * self.settings.scoring.momentum_weight * 100.0,
            "trend_strength": trend_strength
            * self.settings.scoring.trend_strength_weight
            * 100.0,
            "breakout_strength": breakout_strength
            * self.settings.scoring.breakout_strength_weight
            * 100.0,
            "volume_spike": volume_spike
            * self.settings.scoring.volume_spike_weight
            * 100.0,
            "volatility_contraction": volatility_contraction
            * self.settings.scoring.volatility_contraction_weight
            * 100.0,
        }
        total = round(sum(components.values()), 2)
        return ScoreBreakdown(total=total, components=components)


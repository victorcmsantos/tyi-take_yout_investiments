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
        s = self.settings.scoring
        # (metrica_no_score, valor, low, high, inverse, peso). As 15 metricas
        # contribuem para o ranking (antes so 5 pesavam). Pesos somam ~1.0.
        parts = {
            "momentum": (_normalize(metrics.get("momentum"), 0.0, 0.25), s.momentum_weight),
            "momentum_90": (_normalize(metrics.get("momentum_90"), 0.0, 0.5), s.momentum_90_weight),
            "trend_strength": (_normalize(metrics.get("trend_strength"), 0.15, 0.6), s.trend_strength_weight),
            "distance_from_sma200": (_normalize(metrics.get("distance_from_sma200"), 0.0, 0.3), s.distance_from_sma200_weight),
            "relative_strength": (_normalize(metrics.get("relative_strength_vs_ibov"), 0.0, 0.3), s.relative_strength_weight),
            "distance_52w_high": (_normalize(metrics.get("distance_52w_high"), -0.3, 0.0), s.distance_52w_high_weight),
            "higher_high": (_normalize(metrics.get("higher_high_score"), 0.4, 1.0), s.higher_high_weight),
            "breakout_strength": (_normalize(metrics.get("breakout_20"), 0.0, 0.12), s.breakout_strength_weight),
            "bollinger_position": (_normalize(metrics.get("bollinger_position"), 0.5, 1.0), s.bollinger_position_weight),
            "vwap_distance": (_normalize(metrics.get("vwap_distance"), 0.0, 0.05), s.vwap_distance_weight),
            "volume_spike": (_normalize(metrics.get("volume_spike"), 1.0, 3.0), s.volume_spike_weight),
            "volatility_contraction": (_normalize(metrics.get("volatility_compression"), 0.4, 1.1, inverse=True), s.volatility_contraction_weight),
            "atr_quality": (_normalize(metrics.get("atr_percent"), 0.02, 0.08, inverse=True), s.atr_percent_weight),
            "range_expansion": (_normalize(metrics.get("range_expansion"), 1.0, 2.0), s.range_expansion_weight),
            "rsi": (_normalize(metrics.get("rsi"), 45.0, 70.0), s.rsi_weight),
        }
        components = {key: round(norm * weight * 100.0, 2) for key, (norm, weight) in parts.items()}

        # Bonus fundamentalista ADITIVO (so quando o dado existe): DY alto e P/L
        # baixo somam pontos por cima do score tecnico. Total final limitado a 100.
        dividend_yield = metrics.get("dividend_yield")
        if dividend_yield is not None:
            components["dividend_yield"] = round(
                _normalize(dividend_yield, 0.03, 0.09) * s.dividend_yield_points, 2
            )
        price_earnings = metrics.get("price_earnings")
        if price_earnings is not None and price_earnings > 0:
            components["price_earnings"] = round(
                _normalize(price_earnings, 4.0, 20.0, inverse=True) * s.price_earnings_points, 2
            )

        total = round(min(100.0, sum(components.values())), 2)
        return ScoreBreakdown(total=total, components=components)


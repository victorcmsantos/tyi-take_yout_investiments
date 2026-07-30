"""Regras puras de avaliacao de trade (sem dependencia de banco).

Isolado aqui para ser testavel sem SQLAlchemy.
"""

from __future__ import annotations


def candle_is_after_entry(candle_timestamp, signal_timestamp) -> bool:
    """True se o candle deve avaliar stop/alvo do trade.

    So candles POSTERIORES ao candle de entrada sao elegiveis. O candle de
    entrada nao dispara saida: seu high/low ja aconteceu (em parte antes da
    entrada), entao avalia-lo seria look-ahead -- um trade aberto no fechamento
    do dia seria "stopado" pelo minimo do proprio dia, que e passado.

    Sem timestamps (None), mantem o comportamento antigo (elegivel) para nao
    quebrar chamadas legadas.
    """
    if candle_timestamp is None or signal_timestamp is None:
        return True
    return candle_timestamp > signal_timestamp

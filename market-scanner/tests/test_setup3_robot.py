"""Testes dos helpers puros do robo setup3.

Rodavel sem dependencias do scanner: stuba `loguru` para permitir importar o
modulo (que so usa o logger dentro de run_daily_setup3_robot). Uso:

    python tests/test_setup3_robot.py
"""

import sys
import types
from datetime import date
from pathlib import Path

# Stub minimo de loguru para importar o modulo sem a dependencia instalada.
if "loguru" not in sys.modules:
    stub = types.ModuleType("loguru")
    stub.logger = types.SimpleNamespace(
        info=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        exception=lambda *a, **k: None,
    )
    sys.modules["loguru"] = stub

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scheduler.setup3_robot import (  # noqa: E402
    compute_quantity,
    is_setup3,
    select_setup3_signals,
)
from database.trade_rules import candle_is_after_entry  # noqa: E402


def test_candle_is_after_entry():
    entry = date(2026, 7, 30)
    nxt = date(2026, 7, 31)
    assert candle_is_after_entry(nxt, entry) is True       # candle seguinte avalia
    assert candle_is_after_entry(entry, entry) is False     # candle de entrada NAO avalia
    assert candle_is_after_entry(date(2026, 7, 29), entry) is False
    assert candle_is_after_entry(None, entry) is True       # legado
    assert candle_is_after_entry(nxt, None) is True         # legado


def test_is_setup3():
    assert is_setup3(["Volatility Compression", "Breakout 20"]) is True
    assert is_setup3(["volatility compression", "breakout 20"]) is True  # case-insensitive
    assert is_setup3(["Breakout 20", "Volume Spike"]) is False           # sem compressao
    assert is_setup3(["Volatility Compression", "RSI"]) is False          # sem breakout
    assert is_setup3([]) is False
    assert is_setup3(None) is False


def test_compute_quantity():
    assert compute_quantity(25.0, 5000.0) == 200
    assert compute_quantity(33.33, 5000.0) == 150   # round(150.01)
    assert compute_quantity(10.0, 5000.0) == 500
    assert compute_quantity(0.0, 5000.0) == 0
    assert compute_quantity(-5.0, 5000.0) == 0
    assert compute_quantity(30.0, 0.0) == 0
    assert compute_quantity("abc", 5000.0) == 0


def _sig(ticker, metrics, created_at, score=70.0, price=25.0):
    return {
        "ticker": ticker,
        "metrics_triggered": metrics,
        "created_at": created_at,
        "score": score,
        "price": price,
    }


def test_select_setup3_signals():
    today = date(2026, 7, 30)
    signals = [
        _sig("AAAA3", ["Volatility Compression", "Breakout 20"], "2026-07-30T14:30:00"),  # ok hoje
        _sig("BBBB3", ["Volatility Compression", "Breakout 20"], "2026-07-29T14:30:00"),  # ontem
        _sig("CCCC3", ["Breakout 20", "Volume Spike"], "2026-07-30T14:30:00"),            # nao setup3
        _sig("DDDD3", ["Volatility Compression", "Breakout 20"], "2026-07-30T10:00:00", score=40.0),  # score baixo
    ]

    # today_only=True, sem piso de score: setup3 de hoje -> AAAA3 e DDDD3
    picked = select_setup3_signals(signals, today=today, today_only=True, min_score=0.0)
    assert [s["ticker"] for s in picked] == ["AAAA3", "DDDD3"]

    # today_only=False: soma BBBB3 (setup3 de ontem)
    picked = select_setup3_signals(signals, today=today, today_only=False, min_score=0.0)
    assert sorted(s["ticker"] for s in picked) == ["AAAA3", "BBBB3", "DDDD3"]

    # min_score=50 corta DDDD3 (score 40); sobra AAAA3
    picked = select_setup3_signals(signals, today=today, today_only=True, min_score=50.0)
    assert [s["ticker"] for s in picked] == ["AAAA3"]


def main():
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    if failures:
        print(f"\n{failures} teste(s) falharam")
        sys.exit(1)
    print("\ntodos os testes passaram")


if __name__ == "__main__":
    main()

"""Robo diario de paper trading do 'setup3'.

Uma vez por dia (apos o horario configurado, dentro do pregao B3), abre um paper
trade de ~budget reais em cada acao cujo sinal do scanner foi acionado no
"setup3" hoje -- isto e, o `metrics_triggered` do sinal contem um rotulo com
"volatility compression" E outro com "breakout" (mesma regra do filtro do
frontend em ScannerPage.jsx). A SAIDA e automatica: o proprio scan fecha o
trade quando bate o stop (1xATR) ou o alvo (2xATR). O objetivo e validar o
setup em producao por alguns meses e decidir se presta.

Os trades sao gravados direto no banco (mesma sessao do scanner). As notes
levam o marcador de dono `[[TYI_UID:<uid>]]` para o trade aparecer na tela
Swing Trade do usuario (o backend filtra por esse marcador).

Os helpers puros (is_setup3 / select_setup3_signals / compute_quantity) nao
importam SQLAlchemy -- os imports de banco sao preguicosos dentro de
`run_daily_setup3_robot` para manter os helpers testaveis isoladamente.
"""

from __future__ import annotations

from datetime import date, datetime

from loguru import logger


def _has_keyword(metrics, needle: str) -> bool:
    needle = needle.lower()
    return any(needle in str(metric or "").lower() for metric in (metrics or []))


def is_setup3(metrics) -> bool:
    """Setup 3 = 'Volatility Compression' + 'Breakout' no metrics_triggered."""
    return _has_keyword(metrics, "volatility compression") and _has_keyword(metrics, "breakout")


def compute_quantity(price: float, budget: float) -> int:
    """Quantidade inteira que aproxima o orcamento (~budget) por acao."""
    try:
        price = float(price)
        budget = float(budget)
    except (TypeError, ValueError):
        return 0
    if price <= 0 or budget <= 0:
        return 0
    return int(round(budget / price))


def _created_on(created_at_iso: str, target_day: date) -> bool:
    try:
        return datetime.fromisoformat(str(created_at_iso)).date() == target_day
    except (TypeError, ValueError):
        return False


def select_setup3_signals(signals, *, today: date, today_only: bool, min_score: float):
    """Filtra os sinais que qualificam para o robo (setup3 [+ acionados hoje])."""
    selected = []
    for signal in signals or []:
        if float(signal.get("score") or 0.0) < float(min_score):
            continue
        if not is_setup3(signal.get("metrics_triggered")):
            continue
        if today_only and not _created_on(signal.get("created_at"), today):
            continue
        selected.append(signal)
    return selected


def run_daily_setup3_robot(session_factory, settings, *, today: date | None = None) -> dict:
    """Abre um paper trade de ~budget em cada ticker do setup3 acionado hoje.

    Retorna um resumo com contagens. Idempotente no dia: tickers com trade
    ABERTO sao pulados (o create tambem tem trava anti-duplicado interna).
    """
    # Imports de banco preguicosos (mantem os helpers acima livres de SQLAlchemy).
    from database.db import (
        create_trade_from_signal,
        list_active_signals,
        list_open_trade_tickers,
        session_scope,
    )

    if today is None:
        today = datetime.utcnow().date()

    budget = float(settings.setup3_robot_budget)
    owner_uid = int(settings.setup3_robot_owner_uid)
    result = {
        "matched": 0,
        "opened": 0,
        "skipped_open": 0,
        "skipped_qty": 0,
        "errors": 0,
        "tickers": [],
    }

    with session_scope(session_factory) as session:
        signals = list_active_signals(
            session,
            settings.active_signal_hours,
            trade_level_settings=settings.trade_levels,
            min_score=settings.setup3_robot_min_score,
        )
        selected = select_setup3_signals(
            signals,
            today=today,
            today_only=settings.setup3_robot_today_only,
            min_score=settings.setup3_robot_min_score,
        )
        result["matched"] = len(selected)
        open_tickers = list_open_trade_tickers(session)

        for signal in selected:
            ticker = str(signal.get("ticker") or "").strip().upper()
            price = float(signal.get("price") or 0.0)
            if ticker in open_tickers:
                result["skipped_open"] += 1
                continue
            quantity = compute_quantity(price, budget)
            if quantity < 1:
                result["skipped_qty"] += 1
                logger.warning(
                    f"setup3 robot: {ticker} pulado (preco {price} -> qty {quantity} < 1)"
                )
                continue
            notes = f"[[TYI_UID:{owner_uid}]] robo-setup3 {today.isoformat()}"
            try:
                trade = create_trade_from_signal(
                    session,
                    ticker,
                    active_hours=settings.active_signal_hours,
                    trade_level_settings=settings.trade_levels,
                    quantity=float(quantity),
                    notes=notes,
                )
            except ValueError as exc:
                result["errors"] += 1
                logger.warning(f"setup3 robot: falha ao abrir {ticker}: {exc}")
                continue
            # Se ja existia trade aberto (trava interna), o create devolve o
            # existente sem criar outro -- contabiliza como pulado.
            if ticker in open_tickers:
                result["skipped_open"] += 1
                continue
            open_tickers.add(ticker)
            result["opened"] += 1
            result["tickers"].append(ticker)
            logger.info(
                f"setup3 robot: aberto {ticker} qty={quantity} "
                f"~R${round(quantity * price, 2)} (entry {price})"
            )

    logger.info(
        "setup3 robot run "
        f"day={today.isoformat()} matched={result['matched']} "
        f"opened={result['opened']} skipped_open={result['skipped_open']} "
        f"skipped_qty={result['skipped_qty']} errors={result['errors']}"
    )
    return result

"""Database engine, sessions, and repository helpers."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator, Sequence

from sqlalchemy import create_engine, desc, event, func, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker

from config.settings import AppSettings, TradeLevelSettings
from database.models import Base, Metric, Price, Signal, TickerCatalog, Trade


OPEN_TRADE_STATUS = "OPEN"
SUCCESS_TRADE_STATUS = "TARGET_HIT"
FAILURE_TRADE_STATUS = "STOP_HIT"
MANUAL_PROFIT_STATUS = "CLOSED_PROFIT"
MANUAL_LOSS_STATUS = "CLOSED_LOSS"


def _ticker_symbol_candidates(symbol: str) -> list[str]:
    value = str(symbol or "").strip().upper()
    if not value:
        return []
    candidates = [value]
    if value.endswith(".SA"):
        base = value.removesuffix(".SA")
        if base:
            candidates.append(base)
    elif re.fullmatch(r"[A-Z]{4}\d{1,2}[A-Z]?", value):
        candidates.append(f"{value}.SA")
    deduped: list[str] = []
    for item in candidates:
        if item and item not in deduped:
            deduped.append(item)
    return deduped


def create_session_factory(settings: AppSettings) -> sessionmaker[Session]:
    """Build the SQLAlchemy engine and session factory."""

    _ensure_sqlite_directory(settings.database_url)
    connect_args = {}
    if settings.database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False, "timeout": 30}
    engine = create_engine(
        settings.database_url,
        echo=False,
        future=True,
        connect_args=connect_args,
    )
    if settings.database_url.startswith("sqlite"):
        @event.listens_for(engine, "connect")
        def _sqlite_configure(connection, _):
            cursor = connection.cursor()
            try:
                cursor.execute("PRAGMA foreign_keys = ON")
                cursor.execute("PRAGMA busy_timeout = 30000")
                cursor.execute("PRAGMA journal_mode = WAL")
                cursor.execute("PRAGMA synchronous = NORMAL")
            finally:
                cursor.close()
    Base.metadata.create_all(engine)
    _run_sqlite_migrations(engine, settings.database_url)
    return sessionmaker(bind=engine, expire_on_commit=False, class_=Session)


def _ensure_sqlite_directory(database_url: str) -> None:
    if not database_url.startswith("sqlite:///"):
        return
    raw_path = database_url.removeprefix("sqlite:///")
    if raw_path == ":memory:":
        return
    path = Path(raw_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    path.parent.mkdir(parents=True, exist_ok=True)


def _run_sqlite_migrations(engine, database_url: str) -> None:
    """Apply lightweight schema additions for existing SQLite databases."""

    if not database_url.startswith("sqlite"):
        return
    with engine.begin() as connection:
        trade_columns = {
            row[1]
            for row in connection.execute(text("PRAGMA table_info(trades)")).fetchall()
        }
        if trade_columns and "quantity" not in trade_columns:
            connection.execute(
                text("ALTER TABLE trades ADD COLUMN quantity FLOAT NOT NULL DEFAULT 1.0")
            )
        if trade_columns and "invested_amount" not in trade_columns:
            connection.execute(text("ALTER TABLE trades ADD COLUMN invested_amount FLOAT"))


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Provide a transactional scope around a set of operations."""

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_prices(
    session: Session,
    ticker: str,
    interval: str,
    prices: Sequence[dict[str, float | datetime]],
) -> None:
    """Insert or update historical price rows."""

    if not prices:
        return
    statement = sqlite_insert(Price).values(
        [{**row, "ticker": ticker, "interval": interval} for row in prices]
    )
    update_map = {
        "open": statement.excluded.open,
        "high": statement.excluded.high,
        "low": statement.excluded.low,
        "close": statement.excluded.close,
        "volume": statement.excluded.volume,
    }
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["ticker", "timestamp", "interval"],
            set_=update_map,
        )
    )


def upsert_metrics(
    session: Session,
    ticker: str,
    interval: str,
    metrics: Sequence[dict[str, float | datetime | str]],
) -> None:
    """Insert or update metric rows."""

    if not metrics:
        return
    statement = sqlite_insert(Metric).values(
        [{**row, "ticker": ticker, "interval": interval} for row in metrics]
    )
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["ticker", "timestamp", "interval", "metric_name"],
            set_={"metric_value": statement.excluded.metric_value},
        )
    )


def upsert_signal(
    session: Session,
    ticker: str,
    timestamp: datetime,
    price: float,
    score: float,
    metrics_triggered: list[str],
) -> None:
    """Insert or update a signal row."""

    statement = sqlite_insert(Signal).values(
        {
            "ticker": ticker,
            "timestamp": timestamp,
            "signal_type": "BUY",
            "price": price,
            "score": score,
            "metrics_triggered": json.dumps(metrics_triggered),
            "created_at": datetime.utcnow(),
        }
    )
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["ticker", "timestamp", "signal_type"],
            set_={
                "price": statement.excluded.price,
                "score": statement.excluded.score,
                "metrics_triggered": statement.excluded.metrics_triggered,
                "created_at": statement.excluded.created_at,
            },
        )
    )


def upsert_ticker_catalog(
    session: Session,
    rows: Sequence[dict[str, object]],
) -> None:
    """Insert or update official ticker catalog rows."""

    if not rows:
        return
    now = datetime.utcnow()
    payload = [{**row, "last_verified_at": row.get("last_verified_at", now)} for row in rows]
    statement = sqlite_insert(TickerCatalog).values(payload)
    session.execute(
        statement.on_conflict_do_update(
            index_elements=["ticker"],
            set_={
                "yahoo_symbol": statement.excluded.yahoo_symbol,
                "issuer_code": statement.excluded.issuer_code,
                "issuer_name": statement.excluded.issuer_name,
                "trading_name": statement.excluded.trading_name,
                "specification": statement.excluded.specification,
                "isin": statement.excluded.isin,
                "source": statement.excluded.source,
                "is_active": statement.excluded.is_active,
                "yahoo_supported": statement.excluded.yahoo_supported,
                "last_verified_at": statement.excluded.last_verified_at,
            },
        )
    )


def mark_missing_tickers_inactive(
    session: Session,
    active_tickers: Sequence[str],
) -> None:
    """Mark catalog tickers not present in the latest discovery as inactive."""

    statement = update(TickerCatalog).values(is_active=False)
    if active_tickers:
        statement = statement.where(TickerCatalog.ticker.not_in(list(active_tickers)))
    session.execute(statement)


def touch_ticker_scan_status(
    session: Session,
    tickers: Sequence[str],
    *,
    yahoo_supported: bool,
    scanned_at: datetime,
) -> None:
    """Update scan metadata for a set of tickers."""

    if not tickers:
        return
    session.execute(
        update(TickerCatalog)
        .where(TickerCatalog.ticker.in_(list(tickers)))
        .values(
            yahoo_supported=yahoo_supported,
            last_verified_at=scanned_at,
            last_scan_at=scanned_at if yahoo_supported else TickerCatalog.last_scan_at,
        )
    )


def has_backend_assets_table(session: Session) -> bool:
    """Return whether the shared SQLite DB contains the backend assets table."""

    row = session.execute(
        text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'assets' LIMIT 1")
    ).first()
    return row is not None


def list_backend_br_asset_symbols(session: Session) -> list[str]:
    """Return BR symbols from backend assets normalized as Yahoo `.SA` symbols."""

    if not has_backend_assets_table(session):
        return []

    rows = session.execute(text("SELECT ticker FROM assets")).fetchall()
    symbols: list[str] = []
    seen: set[str] = set()
    for row in rows:
        raw = str(row[0] or "").strip().upper()
        if not raw:
            continue
        base = raw.removesuffix(".SA")
        if not re.fullmatch(r"[A-Z]{4}\d{1,2}[A-Z]?", base):
            continue
        yahoo_symbol = f"{base}.SA"
        if yahoo_symbol in seen:
            continue
        seen.add(yahoo_symbol)
        symbols.append(yahoo_symbol)
    return sorted(symbols)


def update_backend_asset_market_snapshot(
    session: Session,
    *,
    ticker: str,
    price: float,
    variation_day: float | None,
    variation_7d: float | None,
    variation_30d: float | None,
    updated_at: datetime,
) -> int:
    """Propagate scanner price snapshot into backend assets when sharing one DB."""

    updated_iso = updated_at.replace(microsecond=0).isoformat() + "Z"
    result = session.execute(
        text(
            """
            UPDATE assets
            SET
                price = :price,
                variation_day = :variation_day,
                variation_7d = :variation_7d,
                variation_30d = :variation_30d,
                market_data_status = 'fresh',
                market_data_source = 'market_scanner',
                market_data_updated_at = :updated_iso,
                market_data_last_attempt_at = :updated_iso,
                market_data_last_error = ''
            WHERE ticker = :ticker
            """
        ),
        {
            "ticker": str(ticker or "").strip().upper(),
            "price": float(price),
            "variation_day": float(variation_day or 0.0),
            "variation_7d": float(variation_7d or 0.0),
            "variation_30d": float(variation_30d or 0.0),
            "updated_iso": updated_iso,
        },
    )
    return int(result.rowcount or 0)


def create_trade_from_signal(
    session: Session,
    ticker: str,
    *,
    active_hours: int,
    trade_level_settings: TradeLevelSettings | None = None,
    quantity: float = 1.0,
    invested_amount: float | None = None,
    notes: str = "",
) -> dict[str, object]:
    """Persist a new tracked trade from the latest available signal."""

    ticker_candidates = _ticker_symbol_candidates(ticker)
    if not ticker_candidates:
        raise ValueError("Ticker is required")
    normalized_ticker = ticker_candidates[0]
    normalized_quantity = float(quantity)
    if normalized_quantity <= 0:
        raise ValueError("Quantity must be greater than zero")
    existing_open_trade = session.scalar(
        select(Trade)
        .where(Trade.ticker.in_(ticker_candidates), Trade.status == OPEN_TRADE_STATUS)
        .order_by(desc(Trade.opened_at))
        .limit(1)
    )
    if existing_open_trade is not None:
        return _serialize_trade(existing_open_trade)

    cutoff = datetime.utcnow() - timedelta(hours=active_hours)
    signal = session.scalar(
        select(Signal)
        .where(Signal.ticker.in_(ticker_candidates), Signal.created_at >= cutoff)
        .order_by(desc(Signal.created_at), desc(Signal.score))
        .limit(1)
    )
    if signal is None:
        signal = session.scalar(
            select(Signal)
            .where(Signal.ticker.in_(ticker_candidates))
            .order_by(desc(Signal.created_at), desc(Signal.score))
            .limit(1)
        )
    if signal is None:
        raise ValueError(f"No signal available for ticker {normalized_ticker}")
    normalized_ticker = signal.ticker

    atr_percent = _load_latest_metric_map(
        session,
        [normalized_ticker],
        metric_name="atr_percent",
    ).get(normalized_ticker)
    trade_levels = _build_trade_levels(
        signal.price,
        atr_percent,
        trade_level_settings=trade_level_settings,
    )
    opened_at = datetime.utcnow()
    trade = Trade(
        ticker=normalized_ticker,
        signal_timestamp=signal.timestamp,
        status=OPEN_TRADE_STATUS,
        entry_price=signal.price,
        quantity=normalized_quantity,
        invested_amount=(
            float(invested_amount)
            if invested_amount is not None
            else float(signal.price) * normalized_quantity
        ),
        entry_region_low=float(trade_levels["entry_region"]["low"]),
        entry_region_high=float(trade_levels["entry_region"]["high"]),
        objective_price=float(trade_levels["objective_price"]),
        stop_price=float(trade_levels["stop_price"]),
        score=signal.score,
        metrics_triggered=signal.metrics_triggered,
        notes=notes.strip(),
        opened_at=opened_at,
        last_price=signal.price,
        last_checked_at=opened_at,
    )
    session.add(trade)
    session.flush()
    return _serialize_trade(trade)


def update_trade(
    session: Session,
    trade_id: int,
    *,
    quantity: float | None = None,
    invested_amount: float | None = None,
    objective_price: float | None = None,
    stop_price: float | None = None,
    notes: str | None = None,
) -> dict[str, object]:
    """Update editable trade fields while the trade is still open."""

    trade = session.get(Trade, trade_id)
    if trade is None:
        raise ValueError(f"Trade {trade_id} was not found")
    if trade.status != OPEN_TRADE_STATUS:
        raise ValueError("Only open trades can be edited")

    if quantity is not None:
        if float(quantity) <= 0:
            raise ValueError("Quantity must be greater than zero")
        trade.quantity = float(quantity)
    if invested_amount is not None:
        if float(invested_amount) <= 0:
            raise ValueError("Invested amount must be greater than zero")
        trade.invested_amount = float(invested_amount)
    if objective_price is not None:
        if float(objective_price) <= 0:
            raise ValueError("Objective price must be greater than zero")
        trade.objective_price = float(objective_price)
    if stop_price is not None:
        if float(stop_price) <= 0:
            raise ValueError("Stop price must be greater than zero")
        trade.stop_price = float(stop_price)
    if (
        objective_price is not None or stop_price is not None
    ) and trade.objective_price <= trade.stop_price:
        raise ValueError("Objective price must be above stop price")
    if notes is not None:
        trade.notes = notes.strip()

    session.flush()
    return _serialize_trade(trade)


def update_open_trades_for_ticker(
    session: Session,
    ticker: str,
    *,
    high: float,
    low: float,
    close: float,
) -> None:
    """Update tracked trades using the latest candle for a ticker."""

    open_trades = session.scalars(
        select(Trade)
        .where(Trade.ticker == ticker, Trade.status == OPEN_TRADE_STATUS)
        .order_by(desc(Trade.opened_at))
    ).all()
    checked_at = datetime.utcnow()
    for trade in open_trades:
        trade.last_price = close
        trade.last_checked_at = checked_at
        # Same-candle target+stop ambiguity is resolved conservatively as stop-first.
        if low <= trade.stop_price:
            trade.status = FAILURE_TRADE_STATUS
            trade.exit_price = trade.stop_price
            trade.exit_reason = "stop_hit"
            trade.closed_at = checked_at
        elif high >= trade.objective_price:
            trade.status = SUCCESS_TRADE_STATUS
            trade.exit_price = trade.objective_price
            trade.exit_reason = "target_hit"
            trade.closed_at = checked_at


def close_trade(
    session: Session,
    trade_id: int,
    *,
    exit_price: float | None = None,
) -> dict[str, object]:
    """Manually close an open trade using the provided or latest price."""

    trade = session.get(Trade, trade_id)
    if trade is None:
        raise ValueError(f"Trade {trade_id} was not found")
    if trade.status != OPEN_TRADE_STATUS:
        return _serialize_trade(trade)

    latest_price = exit_price
    if latest_price is None:
        latest_price = trade.last_price
    if latest_price is None:
        latest_row = session.scalar(
            select(Price)
            .where(Price.ticker == trade.ticker)
            .order_by(desc(Price.timestamp))
            .limit(1)
        )
        latest_price = None if latest_row is None else latest_row.close
    if latest_price is None:
        raise ValueError(f"No price available to close trade {trade_id}")

    closed_at = datetime.utcnow()
    trade.exit_price = float(latest_price)
    trade.last_price = float(latest_price)
    trade.last_checked_at = closed_at
    trade.closed_at = closed_at
    trade.exit_reason = "manual_close"
    trade.status = MANUAL_PROFIT_STATUS if float(latest_price) >= trade.entry_price else MANUAL_LOSS_STATUS
    session.flush()
    return _serialize_trade(trade)


def list_trades(
    session: Session,
    *,
    limit: int = 500,
) -> dict[str, object]:
    """Return active trades, full attempt history, and a compact summary."""

    trades = session.scalars(select(Trade).order_by(desc(Trade.opened_at)).limit(limit)).all()
    active = [_serialize_trade(trade) for trade in trades if trade.status == OPEN_TRADE_STATUS]
    history = [_serialize_trade(trade) for trade in trades if trade.status != OPEN_TRADE_STATUS]
    status_counts = {
        "open": sum(1 for trade in trades if trade.status == OPEN_TRADE_STATUS),
        "success": sum(1 for trade in trades if trade.status == SUCCESS_TRADE_STATUS),
        "failure": sum(1 for trade in trades if trade.status == FAILURE_TRADE_STATUS),
        "closed_profit": sum(1 for trade in trades if trade.status == MANUAL_PROFIT_STATUS),
        "closed_loss": sum(1 for trade in trades if trade.status == MANUAL_LOSS_STATUS),
    }
    open_invested_amount = round(sum(trade["invested_amount"] for trade in active), 2)
    open_pnl_amount = round(sum(trade["current_pnl_amount"] for trade in active), 2)
    return {
        "active": active,
        "history": history,
        "tracked_tickers": sorted({str(trade["ticker"]) for trade in active}),
        "summary": {
            "tracked_count": len(active),
            "history_count": len(history),
            "open_invested_amount": open_invested_amount,
            "open_pnl_amount": open_pnl_amount,
            **status_counts,
        },
    }


def list_active_signals(
    session: Session,
    active_hours: int,
    limit: int = 200,
    trade_level_settings: TradeLevelSettings | None = None,
) -> list[dict[str, object]]:
    """Return recent signals sorted by score descending."""

    cutoff = datetime.utcnow() - timedelta(hours=active_hours)
    rows = session.scalars(
        select(Signal)
        .where(Signal.created_at >= cutoff)
        .order_by(desc(Signal.created_at), desc(Signal.score))
    ).all()
    latest_by_ticker: dict[str, Signal] = {}
    for row in rows:
        latest_by_ticker.setdefault(row.ticker, row)
    ordered_rows = sorted(latest_by_ticker.values(), key=lambda item: item.score, reverse=True)[:limit]
    atr_by_ticker = _load_latest_metric_map(
        session,
        [row.ticker for row in ordered_rows],
        metric_name="atr_percent",
    )
    return [
        {
            "ticker": row.ticker,
            "timestamp": row.timestamp.isoformat(),
            "price": round(row.price, 4),
            "score": round(row.score, 2),
            "metrics_triggered": json.loads(row.metrics_triggered),
            "created_at": row.created_at.isoformat(),
            "trade_levels": _build_trade_levels(
                row.price,
                atr_by_ticker.get(row.ticker),
                trade_level_settings=trade_level_settings,
            ),
        }
        for row in ordered_rows
    ]


def get_signal_matrix(
    session: Session,
    active_hours: int,
    limit: int = 200,
    trade_level_settings: TradeLevelSettings | None = None,
) -> dict[str, object]:
    """Return a ticker x metric matrix for active signals."""

    signals = list_active_signals(
        session,
        active_hours=active_hours,
        limit=limit,
        trade_level_settings=trade_level_settings,
    )
    metric_frequency: dict[str, int] = defaultdict(int)
    for signal in signals:
        for metric in signal["metrics_triggered"]:
            metric_frequency[str(metric)] += 1

    metric_columns = sorted(
        metric_frequency.keys(),
        key=lambda metric: (-metric_frequency[metric], metric),
    )
    rows = [
        {
            "ticker": signal["ticker"],
            "price": signal["price"],
            "score": signal["score"],
            "triggered_count": len(signal["metrics_triggered"]),
            "metrics_triggered": signal["metrics_triggered"],
            "cells": {
                metric: metric in signal["metrics_triggered"] for metric in metric_columns
            },
        }
        for signal in sorted(
            signals,
            key=lambda item: (-len(item["metrics_triggered"]), -float(item["score"]), str(item["ticker"])),
        )
    ]
    return {
        "columns": metric_columns,
        "rows": rows,
    }


def _load_latest_metric_map(
    session: Session,
    tickers: Sequence[str],
    *,
    metric_name: str,
) -> dict[str, float]:
    """Load the latest value of a metric for each ticker."""

    if not tickers:
        return {}
    rows = session.scalars(
        select(Metric)
        .where(Metric.ticker.in_(list(tickers)), Metric.metric_name == metric_name)
        .order_by(desc(Metric.timestamp))
    ).all()
    values: dict[str, float] = {}
    for row in rows:
        values.setdefault(row.ticker, float(row.metric_value))
    return values


def _build_trade_levels(
    price: float,
    atr_percent: float | None,
    *,
    trade_level_settings: TradeLevelSettings | None = None,
) -> dict[str, object]:
    """Build suggested entry, target, and stop levels from ATR or fallbacks."""

    settings = trade_level_settings or TradeLevelSettings()
    if atr_percent is not None and atr_percent > 0:
        atr_value = price * atr_percent
        entry_half_band = atr_value * settings.entry_band_atr_multiplier
        stop_distance = atr_value * settings.stop_atr_multiplier
        target_distance = atr_value * settings.target_atr_multiplier
        method = "atr"
    else:
        entry_half_band = price * settings.fallback_entry_pct
        stop_distance = price * settings.fallback_stop_pct
        target_distance = price * settings.fallback_target_pct
        method = "fallback_pct"

    objective_price = round(price + target_distance, 4)
    stop_price = round(max(price - stop_distance, 0.0), 4)
    potential_gain_pct = round((objective_price / price - 1.0) * 100.0, 2) if price else 0.0
    risk_pct = round((1.0 - stop_price / price) * 100.0, 2) if price else 0.0
    risk_reward_ratio = round(potential_gain_pct / risk_pct, 2) if risk_pct > 0 else None

    return {
        "entry_region": {
            "low": round(price - entry_half_band, 4),
            "high": round(price + entry_half_band, 4),
        },
        "objective_price": objective_price,
        "stop_price": stop_price,
        "potential_gain_pct": potential_gain_pct,
        "risk_pct": risk_pct,
        "risk_reward_ratio": risk_reward_ratio,
        "method": method,
    }


def _serialize_trade(trade: Trade) -> dict[str, object]:
    """Convert a tracked trade to API/template payload."""

    metrics_triggered = json.loads(trade.metrics_triggered)
    quantity = float(trade.quantity or 1.0)
    invested_amount = (
        float(trade.invested_amount)
        if trade.invested_amount is not None
        else float(trade.entry_price) * quantity
    )
    reference_price = trade.exit_price if trade.exit_price is not None else trade.last_price
    current_price = trade.last_price if trade.last_price is not None else trade.entry_price
    current_market_value = float(current_price) * quantity if current_price is not None else 0.0
    exit_market_value = float(reference_price) * quantity if reference_price is not None else 0.0
    current_pnl_amount = round(current_market_value - invested_amount, 2)
    realized_pnl_amount = round(exit_market_value - invested_amount, 2) if trade.exit_price is not None else None
    pnl_pct = (
        round((exit_market_value / invested_amount - 1.0) * 100.0, 2)
        if trade.exit_price is not None and invested_amount > 0
        else 0.0
    )
    current_pnl_pct = (
        round((current_market_value / invested_amount - 1.0) * 100.0, 2)
        if current_price is not None and invested_amount > 0
        else 0.0
    )
    return {
        "id": trade.id,
        "ticker": trade.ticker,
        "signal_timestamp": None
        if trade.signal_timestamp is None
        else trade.signal_timestamp.isoformat(),
        "status": trade.status,
        "status_label": _get_trade_status_label(trade.status),
        "status_tone": _get_trade_status_tone(trade.status),
        "entry_price": round(trade.entry_price, 4),
        "quantity": round(quantity, 6),
        "invested_amount": round(invested_amount, 2),
        "entry_region": {
            "low": round(trade.entry_region_low, 4),
            "high": round(trade.entry_region_high, 4),
        },
        "objective_price": round(trade.objective_price, 4),
        "stop_price": round(trade.stop_price, 4),
        "score": round(trade.score, 2),
        "metrics_triggered": metrics_triggered,
        "notes": trade.notes,
        "opened_at": trade.opened_at.isoformat(),
        "closed_at": None if trade.closed_at is None else trade.closed_at.isoformat(),
        "exit_price": None if trade.exit_price is None else round(trade.exit_price, 4),
        "exit_reason": trade.exit_reason,
        "last_price": None if trade.last_price is None else round(trade.last_price, 4),
        "last_checked_at": None
        if trade.last_checked_at is None
        else trade.last_checked_at.isoformat(),
        "current_market_value": round(current_market_value, 2),
        "current_pnl_amount": current_pnl_amount,
        "current_pnl_pct": current_pnl_pct,
        "realized_pnl_amount": realized_pnl_amount,
        "realized_pnl_pct": pnl_pct if trade.exit_price is not None else None,
        "risk_reward_ratio": round(
            (trade.objective_price - trade.entry_price) / (trade.entry_price - trade.stop_price),
            2,
        )
        if trade.entry_price > trade.stop_price
        else None,
    }


def _get_trade_status_label(status: str) -> str:
    labels = {
        OPEN_TRADE_STATUS: "Em andamento",
        SUCCESS_TRADE_STATUS: "Alvo atingido",
        FAILURE_TRADE_STATUS: "Stop atingido",
        MANUAL_PROFIT_STATUS: "Encerrada com lucro",
        MANUAL_LOSS_STATUS: "Encerrada com loss",
    }
    return labels.get(status, status.replace("_", " ").title())


def _get_trade_status_tone(status: str) -> str:
    if status in {SUCCESS_TRADE_STATUS, MANUAL_PROFIT_STATUS}:
        return "positive"
    if status in {FAILURE_TRADE_STATUS, MANUAL_LOSS_STATUS}:
        return "negative"
    return "neutral"


def get_ticker_details(session: Session, symbol: str) -> dict[str, object]:
    """Return the latest price, metrics, and signals for a ticker."""

    symbol_candidates = _ticker_symbol_candidates(symbol)
    if not symbol_candidates:
        symbol_candidates = [str(symbol or "").strip().upper()]
    symbol_set = set(symbol_candidates)
    base_candidates = {
        item.removesuffix(".SA")
        for item in symbol_candidates
        if item
    }

    catalog_row = session.scalar(
        select(TickerCatalog)
        .where(TickerCatalog.ticker.in_(list(base_candidates)))
        .order_by(desc(TickerCatalog.last_scan_at), desc(TickerCatalog.last_verified_at))
        .limit(1)
    )
    latest_price = session.scalar(
        select(Price)
        .where(Price.ticker.in_(list(symbol_set)))
        .order_by(desc(Price.timestamp))
        .limit(1)
    )
    latest_signal = session.scalar(
        select(Signal)
        .where(Signal.ticker.in_(list(symbol_set)))
        .order_by(desc(Signal.created_at))
        .limit(1)
    )
    resolved_symbol = (
        str(latest_price.ticker)
        if latest_price is not None
        else (
            str(latest_signal.ticker)
            if latest_signal is not None
            else (
                f"{catalog_row.ticker}.SA"
                if catalog_row is not None
                else symbol_candidates[0]
            )
        )
    )

    metric_rows = session.scalars(
        select(Metric)
        .where(Metric.ticker == resolved_symbol)
        .order_by(desc(Metric.timestamp))
        .limit(200)
    ).all()

    latest_metrics: dict[str, float] = {}
    if metric_rows:
        latest_timestamp = metric_rows[0].timestamp
        for row in metric_rows:
            if row.timestamp != latest_timestamp:
                continue
            latest_metrics[row.metric_name] = round(row.metric_value, 6)

    return {
        "ticker": resolved_symbol,
        "catalog": None
        if catalog_row is None
        else {
            "ticker": catalog_row.ticker,
            "yahoo_symbol": catalog_row.yahoo_symbol,
            "issuer_code": catalog_row.issuer_code,
            "issuer_name": catalog_row.issuer_name,
            "trading_name": catalog_row.trading_name,
            "specification": catalog_row.specification,
            "isin": catalog_row.isin,
            "source": catalog_row.source,
            "is_active": catalog_row.is_active,
            "yahoo_supported": catalog_row.yahoo_supported,
            "discovered_at": catalog_row.discovered_at.isoformat(),
            "last_verified_at": catalog_row.last_verified_at.isoformat(),
            "last_scan_at": None
            if catalog_row.last_scan_at is None
            else catalog_row.last_scan_at.isoformat(),
        },
        "latest_price": None if latest_price is None else round(latest_price.close, 4),
        "latest_price_timestamp": None
        if latest_price is None
        else latest_price.timestamp.isoformat(),
        "latest_metrics": latest_metrics,
        "latest_signal": None
        if latest_signal is None
        else {
            "timestamp": latest_signal.timestamp.isoformat(),
            "price": round(latest_signal.price, 4),
            "score": round(latest_signal.score, 2),
            "metrics_triggered": json.loads(latest_signal.metrics_triggered),
        },
    }


def get_metric_history(
    session: Session,
    symbol: str,
    limit: int = 500,
) -> dict[str, list[dict[str, object]]]:
    """Return metric history grouped by metric name."""

    rows = session.scalars(
        select(Metric)
        .where(Metric.ticker == symbol)
        .order_by(desc(Metric.timestamp))
        .limit(limit)
    ).all()
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        grouped[row.metric_name].append(
            {
                "timestamp": row.timestamp.isoformat(),
                "value": round(row.metric_value, 6),
            }
        )
    return dict(grouped)


def get_signal_summary(session: Session) -> dict[str, object]:
    """Return aggregate counts for dashboard context."""

    latest_signal_time = session.scalar(select(func.max(Signal.created_at)))
    signal_count = session.scalar(select(func.count(func.distinct(Signal.ticker)))) or 0
    return {
        "signal_count": signal_count,
        "latest_signal_time": None
        if latest_signal_time is None
        else latest_signal_time.isoformat(),
    }

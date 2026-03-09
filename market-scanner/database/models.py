"""SQLAlchemy ORM models."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base declarative model."""


class TickerCatalog(Base):
    """Official B3 ticker universe persisted locally."""

    __tablename__ = "tickers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    yahoo_symbol: Mapped[str] = mapped_column(String(32), index=True)
    issuer_code: Mapped[str] = mapped_column(String(16), index=True)
    issuer_name: Mapped[str] = mapped_column(String(128))
    trading_name: Mapped[str] = mapped_column(String(128), default="")
    specification: Mapped[str] = mapped_column(String(32))
    isin: Mapped[str] = mapped_column(String(32))
    source: Mapped[str] = mapped_column(String(64), default="b3_official_cotahist")
    is_active: Mapped[bool] = mapped_column(default=True, index=True)
    yahoo_supported: Mapped[bool] = mapped_column(default=True, index=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_verified_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_scan_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)


class Price(Base):
    """Historical OHLCV prices."""

    __tablename__ = "prices"
    __table_args__ = (
        UniqueConstraint("ticker", "timestamp", "interval", name="uq_prices_ticker_ts_int"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    interval: Mapped[str] = mapped_column(String(16), default="1d", index=True)
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)


class Metric(Base):
    """Stored metric values."""

    __tablename__ = "metrics"
    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "timestamp",
            "interval",
            "metric_name",
            name="uq_metrics_ticker_ts_int_name",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    interval: Mapped[str] = mapped_column(String(16), default="1d", index=True)
    metric_name: Mapped[str] = mapped_column(String(64), index=True)
    metric_value: Mapped[float] = mapped_column(Float)


class Signal(Base):
    """Triggered buy signals."""

    __tablename__ = "signals"
    __table_args__ = (
        UniqueConstraint(
            "ticker",
            "timestamp",
            "signal_type",
            name="uq_signals_ticker_ts_type",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)
    signal_type: Mapped[str] = mapped_column(String(16), default="BUY", index=True)
    price: Mapped[float] = mapped_column(Float)
    score: Mapped[float] = mapped_column(Float, index=True)
    metrics_triggered: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)


class Trade(Base):
    """User-confirmed trade attempts tracked against target and stop levels."""

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ticker: Mapped[str] = mapped_column(String(32), index=True)
    signal_timestamp: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String(24), default="OPEN", index=True)
    entry_price: Mapped[float] = mapped_column(Float)
    quantity: Mapped[float] = mapped_column(Float, default=1.0)
    invested_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    entry_region_low: Mapped[float] = mapped_column(Float)
    entry_region_high: Mapped[float] = mapped_column(Float)
    objective_price: Mapped[float] = mapped_column(Float)
    stop_price: Mapped[float] = mapped_column(Float)
    score: Mapped[float] = mapped_column(Float, default=0.0, index=True)
    metrics_triggered: Mapped[str] = mapped_column(Text)
    notes: Mapped[str] = mapped_column(Text, default="")
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    exit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    exit_reason: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)

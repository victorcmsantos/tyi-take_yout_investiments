"""Daemon scheduler and market scan orchestration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, time, timezone
from threading import Event, Lock, Thread
from typing import Iterable
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger
from sqlalchemy.orm import Session, sessionmaker

from collector.brapi_client import BRAPIClient
from collector.ticker_provider import B3ListedTicker, B3TickerProvider
from config.settings import AppSettings
from database.db import (
    has_backend_assets_table,
    mark_missing_tickers_inactive,
    session_scope,
    touch_ticker_scan_status,
    update_backend_asset_market_snapshot,
    update_open_trades_for_ticker,
    upsert_metrics,
    upsert_prices,
    upsert_signal,
    upsert_ticker_catalog,
)
from metrics.metric_engine import MetricEngine
from signals.signal_engine import SignalEngine


@dataclass(slots=True)
class ScanSummary:
    """Execution summary for a market scan."""

    tickers_loaded: int
    tickers_processed: int
    signals_triggered: int


class MarketScannerDaemon:
    """Orchestrates market scans and scheduler lifecycle."""

    MARKET_TIMEZONE = "America/Sao_Paulo"
    MARKET_CRON_DAY_OF_WEEK = "mon-fri"
    MARKET_CRON_HOUR = "10-17"
    MARKET_CRON_MINUTE = "0,30"

    def __init__(
        self,
        settings: AppSettings,
        session_factory: sessionmaker[Session],
        ticker_provider: B3TickerProvider | None = None,
        brapi_client: BRAPIClient | None = None,
        metric_engine: MetricEngine | None = None,
        signal_engine: SignalEngine | None = None,
    ) -> None:
        self.settings = settings
        self.session_factory = session_factory
        self.brapi_client = brapi_client or BRAPIClient(settings)
        self.ticker_provider = ticker_provider or B3TickerProvider(settings)
        self.metric_engine = metric_engine or MetricEngine(settings)
        self.signal_engine = signal_engine or SignalEngine(settings)
        self.market_timezone = ZoneInfo(self.MARKET_TIMEZONE)
        self.market_calendar = xcals.get_calendar("BVMF")
        self.scheduler = BackgroundScheduler(timezone=self.market_timezone)
        self._scan_lock = Lock()
        self._stop_event = Event()

    def start(self) -> None:
        """Start the APScheduler daemon."""

        if not self.scheduler.running:
            self.scheduler.add_job(
                self.scan_market,
                trigger="cron",
                day_of_week=self.MARKET_CRON_DAY_OF_WEEK,
                hour=self.MARKET_CRON_HOUR,
                minute=self.MARKET_CRON_MINUTE,
                id="scan_market",
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )
            self.scheduler.start()
            logger.info(
                "Market scanner scheduler started",
                timezone=self.MARKET_TIMEZONE,
                cron_day_of_week=self.MARKET_CRON_DAY_OF_WEEK,
                cron_hour=self.MARKET_CRON_HOUR,
                cron_minute=self.MARKET_CRON_MINUTE,
            )
        if self.settings.immediate_scan_on_startup:
            Thread(
                target=self.scan_market,
                kwargs={"force": True},
                daemon=True,
                name="market-scanner-startup-scan",
            ).start()

    def stop(self) -> None:
        """Stop the scheduler cleanly."""

        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
            logger.info("Market scanner scheduler stopped")
        self._stop_event.set()

    def run_forever(self) -> None:
        """Run the daemon in blocking mode."""

        self.start()
        try:
            self._stop_event.wait()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received; shutting down daemon")
            self.stop()

    def scan_market(self, force: bool = False) -> ScanSummary:
        """Fetch prices, compute metrics, evaluate signals, and persist results."""

        with self._scan_lock:
            return self._scan_market_internal(force=force, requested_symbols=None)

    def scan_ticker(self, symbol: str, force: bool = True) -> ScanSummary:
        """Run a manual scan for a single ticker."""

        normalized = self._normalize_symbol(symbol)
        if not normalized:
            raise ValueError("Ticker invalido para scan manual.")
        with self._scan_lock:
            return self._scan_market_internal(force=force, requested_symbols=[normalized])

    def _scan_market_internal(
        self,
        force: bool = False,
        requested_symbols: list[str] | None = None,
    ) -> ScanSummary:
        if not force and not self._is_market_open():
            now_local = datetime.now(timezone.utc).astimezone(self.market_timezone)
            logger.info(
                "Skipping market scan outside B3 session",
                now=now_local.isoformat(),
            )
            return ScanSummary(tickers_loaded=0, tickers_processed=0, signals_triggered=0)

        full_catalog = self.ticker_provider.load_catalog()
        catalog = self._catalog_for_requested_symbols(full_catalog, requested_symbols)
        tickers = [record.yahoo_symbol for record in catalog]
        if requested_symbols and not tickers:
            logger.warning(
                "Requested symbols are not available in catalog",
                requested_symbols=requested_symbols,
            )
            return ScanSummary(tickers_loaded=0, tickers_processed=0, signals_triggered=0)
        benchmark_symbol = self.settings.benchmark_symbol.strip() or "^BVSP"
        benchmark_frame = None
        try:
            benchmark_batch = self.brapi_client.download_batch(
                [benchmark_symbol],
                period=self.settings.price_period,
                interval=self.settings.price_interval,
            )
            benchmark_frame = benchmark_batch.get(benchmark_symbol)
            if benchmark_frame is None and benchmark_batch:
                benchmark_frame = next(iter(benchmark_batch.values()))
        except Exception as exc:
            logger.warning(
                "Benchmark download failed; relative metrics may be unavailable",
                benchmark=benchmark_symbol,
                error=str(exc),
            )
        scan_started_at = datetime.utcnow()
        with session_scope(self.session_factory) as session:
            upsert_ticker_catalog(
                session,
                [
                    {
                        "ticker": record.ticker,
                        "yahoo_symbol": record.yahoo_symbol,
                        "issuer_code": record.issuer_code,
                        "issuer_name": record.issuer_name,
                        "trading_name": record.trading_name,
                        "specification": record.specification,
                        "isin": record.isin,
                        "source": record.source,
                        "is_active": record.is_active,
                        "yahoo_supported": record.yahoo_supported,
                        "discovered_at": record.discovered_at or scan_started_at,
                        "last_verified_at": record.last_verified_at or scan_started_at,
                    }
                    for record in catalog
                ],
            )
            if not requested_symbols:
                mark_missing_tickers_inactive(session, [record.ticker for record in catalog])
        logger.info("Starting market scan", tickers=len(tickers))

        processed = 0
        triggered = 0
        for batch in self._chunks(tickers, self.settings.download_batch_size):
            try:
                data = self.brapi_client.download_batch(
                    batch,
                    period=self.settings.price_period,
                    interval=self.settings.price_interval,
                )
                missing = sorted(set(batch) - set(data.keys()))
                if missing:
                    logger.warning(
                        "Some official B3 tickers were not returned by BRAPI",
                        missing_count=len(missing),
                        sample_missing=missing[:10],
                    )
            except Exception as exc:
                logger.exception("Batch download failed", error=str(exc), batch_size=len(batch))
                with session_scope(self.session_factory) as session:
                    touch_ticker_scan_status(
                        session,
                        [ticker.removesuffix(".SA") for ticker in batch],
                        yahoo_supported=False,
                        scanned_at=scan_started_at,
                    )
                continue

            with session_scope(self.session_factory) as session:
                can_sync_backend_assets = has_backend_assets_table(session)
                touch_ticker_scan_status(
                    session,
                    [ticker.removesuffix(".SA") for ticker in data.keys()],
                    yahoo_supported=True,
                    scanned_at=scan_started_at,
                )
                touch_ticker_scan_status(
                    session,
                    [ticker.removesuffix(".SA") for ticker in missing],
                    yahoo_supported=False,
                    scanned_at=scan_started_at,
                )
                for ticker, frame in data.items():
                    try:
                        price_rows = [
                            {
                                "timestamp": timestamp.to_pydatetime(),
                                "open": float(row["open"]),
                                "high": float(row["high"]),
                                "low": float(row["low"]),
                                "close": float(row["close"]),
                                "volume": float(row["volume"]),
                            }
                            for timestamp, row in frame.iterrows()
                        ]
                        upsert_prices(session, ticker, self.settings.price_interval, price_rows)
                        latest_bar = frame.iloc[-1]
                        update_open_trades_for_ticker(
                            session,
                            ticker,
                            high=float(latest_bar["high"]),
                            low=float(latest_bar["low"]),
                            close=float(latest_bar["close"]),
                        )

                        if can_sync_backend_assets:
                            closes = [float(value) for value in frame["close"].tolist() if value is not None]
                            variation_day = 0.0
                            variation_7d = 0.0
                            variation_30d = 0.0
                            if len(closes) >= 2 and closes[-2] != 0:
                                variation_day = ((closes[-1] / closes[-2]) - 1.0) * 100.0
                            if len(closes) >= 8 and closes[-8] != 0:
                                variation_7d = ((closes[-1] / closes[-8]) - 1.0) * 100.0
                            if len(closes) >= 31 and closes[-31] != 0:
                                variation_30d = ((closes[-1] / closes[-31]) - 1.0) * 100.0
                            update_backend_asset_market_snapshot(
                                session,
                                ticker=ticker.removesuffix(".SA"),
                                price=float(latest_bar["close"]),
                                variation_day=variation_day,
                                variation_7d=variation_7d,
                                variation_30d=variation_30d,
                                updated_at=datetime.utcnow(),
                            )

                        computation = self.metric_engine.compute(
                            frame,
                            benchmark_prices=benchmark_frame,
                        )
                        if computation is None:
                            logger.debug("Skipping ticker with insufficient history", ticker=ticker)
                            continue

                        upsert_metrics(
                            session,
                            ticker,
                            self.settings.price_interval,
                            self.metric_engine.to_rows(ticker, computation),
                        )

                        decision = self.signal_engine.evaluate(ticker, computation)
                        if decision is not None:
                            upsert_signal(
                                session,
                                ticker=decision.ticker,
                                timestamp=decision.timestamp,
                                price=decision.price,
                                score=decision.score,
                                metrics_triggered=decision.metrics_triggered,
                            )
                            triggered += 1
                        processed += 1
                    except Exception as exc:
                        logger.exception("Ticker processing failed", ticker=ticker, error=str(exc))

        logger.info(
            "Market scan completed",
            tickers_loaded=len(tickers),
            tickers_processed=processed,
            signals_triggered=triggered,
        )
        return ScanSummary(
            tickers_loaded=len(tickers),
            tickers_processed=processed,
            signals_triggered=triggered,
        )

    def _catalog_for_requested_symbols(
        self,
        catalog: list[B3ListedTicker],
        requested_symbols: list[str] | None,
    ) -> list[B3ListedTicker]:
        if not requested_symbols:
            return list(catalog)

        by_yahoo = {record.yahoo_symbol.upper(): record for record in catalog}
        by_ticker = {record.ticker.upper(): record for record in catalog}
        selected: list[B3ListedTicker] = []
        seen: set[str] = set()
        now = datetime.utcnow()

        for raw_symbol in requested_symbols:
            normalized = self._normalize_symbol(raw_symbol)
            if not normalized:
                continue
            base = normalized.removesuffix(".SA")
            record = by_yahoo.get(normalized) or by_ticker.get(base)
            if record is None and re.fullmatch(r"[A-Z]{4}\d{1,2}[A-Z]?", base):
                record = B3ListedTicker(
                    ticker=base,
                    yahoo_symbol=f"{base}.SA",
                    issuer_code=base[:4],
                    issuer_name=base[:4],
                    trading_name=base[:4],
                    specification="MANUAL",
                    isin="",
                    source="manual_runtime",
                    is_active=True,
                    yahoo_supported=True,
                    discovered_at=now,
                    last_verified_at=now,
                )
            if record is None:
                continue
            key = record.yahoo_symbol.upper()
            if key in seen:
                continue
            seen.add(key)
            selected.append(record)
        return selected

    def _normalize_symbol(self, symbol: str) -> str:
        raw = str(symbol or "").strip().upper()
        if not raw:
            return ""
        if raw.endswith(".SA"):
            return raw
        if re.fullmatch(r"[A-Z]{4}\d{1,2}[A-Z]?", raw):
            return f"{raw}.SA"
        return raw

    def _chunks(self, values: list[str], size: int) -> Iterable[list[str]]:
        for index in range(0, len(values), size):
            yield values[index : index + size]

    def _is_market_open(self, when: datetime | None = None) -> bool:
        """Return whether B3 is open for trading at the given instant."""

        reference = when or datetime.now(timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        reference_utc = reference.astimezone(timezone.utc)
        try:
            minute = pd.Timestamp(reference_utc).floor("min")
            return bool(self.market_calendar.is_open_on_minute(minute, ignore_breaks=True))
        except Exception as exc:
            logger.warning("Failed to evaluate B3 market calendar; using time fallback", error=str(exc))

        # Fallback window for resilience in case calendar rules become unavailable.
        local_time = reference_utc.astimezone(self.market_timezone)
        if local_time.weekday() >= 5:
            return False
        return time(10, 0) <= local_time.timetz().replace(tzinfo=None) < time(17, 0)

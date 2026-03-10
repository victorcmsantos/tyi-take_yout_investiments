"""FastAPI application and dashboard routes."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from config.settings import AppSettings, get_settings
from database.db import (
    close_trade,
    create_trade_from_signal,
    create_session_factory,
    get_metric_history,
    get_signal_matrix,
    get_signal_summary,
    get_ticker_details,
    list_active_signals,
    list_trades,
    session_scope,
    update_trade,
)
from metrics.indicators import build_metric_catalog
from scheduler.daemon import MarketScannerDaemon


class CreateTradeRequest(BaseModel):
    ticker: str = Field(min_length=1)
    quantity: float = 1.0
    invested_amount: float | None = None
    notes: str = ""


class CloseTradeRequest(BaseModel):
    exit_price: float | None = None


class UpdateTradeRequest(BaseModel):
    quantity: float | None = None
    invested_amount: float | None = None
    objective_price: float | None = None
    stop_price: float | None = None
    notes: str | None = None


class UpdateMetricRequest(BaseModel):
    parameters: dict[str, float] = Field(default_factory=dict)


def create_app(settings: AppSettings | None = None) -> FastAPI:
    """Application factory."""

    app_settings = settings or get_settings()
    session_factory = create_session_factory(app_settings)
    templates = Jinja2Templates(directory=str(app_settings.templates_dir))
    daemon = MarketScannerDaemon(app_settings, session_factory)
    style_version = int((app_settings.static_dir / "styles.css").stat().st_mtime)
    dashboard_js_version = int((app_settings.static_dir / "dashboard.js").stat().st_mtime)
    operations_js_version = int((app_settings.static_dir / "operations.js").stat().st_mtime)
    metrics_js_version = int((app_settings.static_dir / "metrics.js").stat().st_mtime)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if app_settings.start_scheduler_with_api:
            daemon.start()
        try:
            yield
        finally:
            if app_settings.start_scheduler_with_api:
                daemon.stop()

    app = FastAPI(title="Market Scanner", lifespan=lifespan)
    app.mount("/static", StaticFiles(directory=str(app_settings.static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/dashboard")

    @app.get("/signals")
    def signals() -> list[dict[str, object]]:
        with session_scope(session_factory) as session:
            payload = list_active_signals(
                session,
                app_settings.active_signal_hours,
                trade_level_settings=app_settings.trade_levels,
            )
        return payload

    @app.get("/signal-matrix")
    def signal_matrix() -> dict[str, object]:
        with session_scope(session_factory) as session:
            payload = get_signal_matrix(
                session,
                app_settings.active_signal_hours,
                trade_level_settings=app_settings.trade_levels,
            )
        return payload

    @app.get("/trades")
    def trades() -> dict[str, object]:
        with session_scope(session_factory) as session:
            payload = list_trades(session)
        return payload

    @app.post("/trades")
    def create_trade(payload: CreateTradeRequest) -> dict[str, object]:
        try:
            with session_scope(session_factory) as session:
                return create_trade_from_signal(
                    session,
                    payload.ticker,
                    active_hours=app_settings.active_signal_hours,
                    trade_level_settings=app_settings.trade_levels,
                    quantity=payload.quantity,
                    invested_amount=payload.invested_amount,
                    notes=payload.notes,
                )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.patch("/trades/{trade_id}")
    def update_trade_endpoint(trade_id: int, payload: UpdateTradeRequest) -> dict[str, object]:
        try:
            with session_scope(session_factory) as session:
                return update_trade(
                    session,
                    trade_id,
                    quantity=payload.quantity,
                    invested_amount=payload.invested_amount,
                    objective_price=payload.objective_price,
                    stop_price=payload.stop_price,
                    notes=payload.notes,
                )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/trades/{trade_id}/close")
    def close_trade_endpoint(trade_id: int, payload: CloseTradeRequest) -> dict[str, object]:
        try:
            with session_scope(session_factory) as session:
                return close_trade(session, trade_id, exit_price=payload.exit_price)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/ticker/{symbol}")
    def ticker_details(symbol: str) -> dict[str, object]:
        with session_scope(session_factory) as session:
            payload = get_ticker_details(session, symbol.upper())
        return payload

    @app.post("/scan")
    def scan_now() -> dict[str, object]:
        summary = daemon.scan_market(force=True)
        return {
            "scan_summary": {
                "tickers_loaded": summary.tickers_loaded,
                "tickers_processed": summary.tickers_processed,
                "signals_triggered": summary.signals_triggered,
            }
        }

    @app.post("/scan/ticker/{symbol}")
    def scan_single_ticker(symbol: str) -> dict[str, object]:
        try:
            summary = daemon.scan_ticker(symbol, force=True)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ticker": str(symbol or "").upper(),
            "scan_summary": {
                "tickers_loaded": summary.tickers_loaded,
                "tickers_processed": summary.tickers_processed,
                "signals_triggered": summary.signals_triggered,
            },
        }

    @app.get("/scan/status")
    def scan_status() -> dict[str, object]:
        return daemon.get_scan_status()

    @app.get("/metrics/history/{symbol}")
    def metrics_history(symbol: str) -> dict[str, list[dict[str, object]]]:
        with session_scope(session_factory) as session:
            return get_metric_history(session, symbol.upper())

    @app.get("/metrics/catalog")
    def metrics_catalog() -> list[dict[str, object]]:
        return build_metric_catalog(app_settings.metrics)

    @app.patch("/metrics/catalog/{metric_key}")
    def update_metric(metric_key: str, payload: UpdateMetricRequest) -> dict[str, object]:
        registry = {metric.key: metric for metric in daemon.metric_engine.registry}
        metric = registry.get(metric_key)
        if metric is None:
            raise HTTPException(status_code=404, detail=f"Metric {metric_key} not found")
        if not payload.parameters:
            raise HTTPException(status_code=400, detail="No parameters were provided")
        editable_keys = set(metric.parameters)
        if not editable_keys:
            raise HTTPException(status_code=400, detail="This metric has no editable parameters")

        updated_parameters: dict[str, float] = {}
        for key, raw_value in payload.parameters.items():
            if key not in editable_keys:
                raise HTTPException(
                    status_code=400,
                    detail=f"Parameter {key} is not editable for metric {metric_key}",
                )
            current_value = getattr(app_settings.metrics, key)
            numeric_value = float(raw_value)
            if numeric_value <= 0:
                raise HTTPException(status_code=400, detail=f"Parameter {key} must be > 0")

            if isinstance(current_value, int):
                if not numeric_value.is_integer():
                    raise HTTPException(
                        status_code=400,
                        detail=f"Parameter {key} must be an integer value",
                    )
                cast_value = int(numeric_value)
            else:
                cast_value = float(numeric_value)
            setattr(app_settings.metrics, key, cast_value)
            updated_parameters[key] = cast_value

        return {
            "metric_key": metric_key,
            "updated_parameters": updated_parameters,
            "scan_summary": None,
            "catalog": build_metric_catalog(app_settings.metrics),
        }

    @app.get("/dashboard")
    def dashboard(request: Request):
        with session_scope(session_factory) as session:
            summary = get_signal_summary(session)
            signals_payload = list_active_signals(
                session,
                app_settings.active_signal_hours,
                trade_level_settings=app_settings.trade_levels,
            )
            trades_payload = list_trades(session)
            matrix_payload = get_signal_matrix(
                session,
                app_settings.active_signal_hours,
                trade_level_settings=app_settings.trade_levels,
            )
        context = {
            "request": request,
            "settings": app_settings,
            "summary": summary,
            "signals": signals_payload,
            "matrix": matrix_payload,
            "tracked_tickers": trades_payload["tracked_tickers"],
            "highlight_score": app_settings.signal_rules.min_score + 15,
            "style_version": style_version,
            "dashboard_js_version": dashboard_js_version,
        }
        return templates.TemplateResponse("dashboard.html", context)

    @app.get("/operations")
    def operations(request: Request):
        with session_scope(session_factory) as session:
            trades_payload = list_trades(session)
        context = {
            "request": request,
            "settings": app_settings,
            "trade_data": trades_payload,
            "style_version": style_version,
            "operations_js_version": operations_js_version,
        }
        return templates.TemplateResponse("operations.html", context)

    @app.get("/metrics-lab")
    def metrics_lab(request: Request):
        context = {
            "request": request,
            "metric_catalog": build_metric_catalog(app_settings.metrics),
            "style_version": style_version,
            "metrics_js_version": metrics_js_version,
        }
        return templates.TemplateResponse("metrics.html", context)

    return app


def create_api_only_app() -> FastAPI:
    """Factory with scheduler disabled, useful for external process managers."""

    settings = replace(get_settings(), start_scheduler_with_api=False)
    return create_app(settings)

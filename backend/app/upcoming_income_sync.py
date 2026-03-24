import os
import time
from threading import Event, Thread

from .observability import init_job_status, mark_job_finished, mark_job_started
from .runtime_lock import should_run_background_jobs
from .services import prefetch_upcoming_incomes_for_portfolios


def _as_bool(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _run_sync_once(app):
    with app.app_context():
        mark_job_started(app, "upcoming_income_sync")
        try:
            try:
                limit_tickers = int(app.config.get("UPCOMING_INCOME_SYNC_MAX_TICKERS_PER_RUN", 0))
            except (TypeError, ValueError):
                limit_tickers = 0
            result = prefetch_upcoming_incomes_for_portfolios(
                max_items_per_ticker=int(app.config.get("UPCOMING_INCOME_SYNC_MAX_ITEMS_PER_TICKER", 8)),
                limit_tickers=limit_tickers if limit_tickers > 0 else None,
            )
            mark_job_finished(app, "upcoming_income_sync", result=result)
            app.logger.info(
                "Agenda de proventos futuros pre-aquecida: %s ticker(s), %s com evento, %s evento(s).",
                int(result.get("tickers_selected", 0)),
                int(result.get("tickers_with_events", 0)),
                int(result.get("events_found", 0)),
            )
        except Exception as exc:
            mark_job_finished(app, "upcoming_income_sync", error=exc)
            app.logger.exception("Falha no pre-aquecimento da agenda de proventos futuros.")


def _sync_loop(app, stop_event: Event):
    interval = int(app.config.get("UPCOMING_INCOME_SYNC_INTERVAL_SECONDS", 1800))
    warmup = bool(app.config.get("UPCOMING_INCOME_SYNC_WARMUP_ON_STARTUP", True))

    if warmup and not stop_event.is_set():
        app.extensions["upcoming_income_sync_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["upcoming_income_sync_last_run"] = time.time()
        finally:
            app.extensions["upcoming_income_sync_running"] = False

    while not stop_event.is_set():
        stop_event.wait(interval)
        if stop_event.is_set():
            break
        app.extensions["upcoming_income_sync_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["upcoming_income_sync_last_run"] = time.time()
        finally:
            app.extensions["upcoming_income_sync_running"] = False


def start_upcoming_income_sync(app):
    enabled_default = _as_bool(os.getenv("UPCOMING_INCOME_SYNC_ENABLED", "1"))
    try:
        interval_default = max(int(os.getenv("UPCOMING_INCOME_SYNC_INTERVAL_SECONDS", "1800")), 300)
    except (TypeError, ValueError):
        interval_default = 1800
    warmup_default = _as_bool(os.getenv("UPCOMING_INCOME_SYNC_WARMUP_ON_STARTUP", "1"))
    try:
        max_items_default = max(int(os.getenv("UPCOMING_INCOME_SYNC_MAX_ITEMS_PER_TICKER", "8")), 1)
    except (TypeError, ValueError):
        max_items_default = 8
    try:
        max_tickers_default = int(os.getenv("UPCOMING_INCOME_SYNC_MAX_TICKERS_PER_RUN", "0"))
    except (TypeError, ValueError):
        max_tickers_default = 0

    app.config.setdefault("UPCOMING_INCOME_SYNC_ENABLED", enabled_default)
    app.config.setdefault("UPCOMING_INCOME_SYNC_INTERVAL_SECONDS", interval_default)
    app.config.setdefault("UPCOMING_INCOME_SYNC_WARMUP_ON_STARTUP", warmup_default)
    app.config.setdefault("UPCOMING_INCOME_SYNC_MAX_ITEMS_PER_TICKER", max_items_default)
    app.config.setdefault("UPCOMING_INCOME_SYNC_MAX_TICKERS_PER_RUN", max_tickers_default)
    app.config.setdefault("UPCOMING_INCOME_SYNC_MAX_AGE_SECONDS", interval_default * 2)
    app.extensions.setdefault("upcoming_income_sync_last_run", 0.0)
    app.extensions.setdefault("upcoming_income_sync_running", False)

    should_start = app.config["UPCOMING_INCOME_SYNC_ENABLED"] and should_run_background_jobs(app)
    init_job_status(
        app,
        "upcoming_income_sync",
        interval_seconds=app.config["UPCOMING_INCOME_SYNC_INTERVAL_SECONDS"],
        max_age_seconds=app.config["UPCOMING_INCOME_SYNC_MAX_AGE_SECONDS"],
        enabled=should_start,
        configured_enabled=app.config["UPCOMING_INCOME_SYNC_ENABLED"],
    )

    if not should_start:
        return

    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    if app.extensions.get("upcoming_income_sync_started"):
        return

    stop_event = Event()
    worker = Thread(target=_sync_loop, args=(app, stop_event), daemon=True)
    worker.start()

    app.extensions["upcoming_income_sync_started"] = True
    app.extensions["upcoming_income_sync_stop_event"] = stop_event
    app.extensions["upcoming_income_sync_thread"] = worker

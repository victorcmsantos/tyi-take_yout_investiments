import os
import time
from threading import Event, Thread

from .observability import init_job_status, mark_job_finished, mark_job_started
from .runtime_lock import should_run_background_jobs
from .services import refresh_stale_assets_market_data


def _as_bool(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _run_sync_once(app):
    with app.app_context():
        mark_job_started(app, "market_sync")
        try:
            scope_key = str(app.config.get("MARKET_SYNC_SCOPE", "all") or "all").strip().lower()
            include_scanner_br = not bool(app.config.get("MARKET_SYNC_FORCE_LIVE_BR", False))
            failed = refresh_stale_assets_market_data(
                attempts=5,
                scope_key=scope_key,
                include_scanner_br=include_scanner_br,
            )
            if failed:
                app.logger.warning(
                    "Atualizacao de mercado (scope=%s) com falha temporaria para %s ticker(s): %s. Nova tentativa no proximo ciclo.",
                    scope_key,
                    len(failed),
                    ", ".join(failed[:10]),
                )
            mark_job_finished(
                app,
                "market_sync",
                result={
                    "scope": scope_key,
                    "include_scanner_br": include_scanner_br,
                    "failed_tickers": len(failed),
                    "sample": failed[:10],
                },
            )
        except Exception as exc:
            mark_job_finished(app, "market_sync", error=exc)
            app.logger.exception("Falha na atualizacao de mercado.")


def _sync_loop(app, stop_event: Event):
    interval = int(app.config.get("MARKET_SYNC_INTERVAL_SECONDS", 300))
    # Aguarda o primeiro intervalo antes da primeira execucao para nao
    # degradar latencia das primeiras requisicoes apos subir o app.
    stop_event.wait(interval)
    while not stop_event.is_set():
        app.extensions["market_sync_manual_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["market_sync_last_run"] = time.time()
        finally:
            app.extensions["market_sync_manual_running"] = False
        stop_event.wait(interval)


def trigger_market_sync_if_due(app, force: bool = False, blocking: bool = False):
    if not app.config.get("MARKET_SYNC_ENABLED", True):
        return False

    interval = int(app.config.get("MARKET_SYNC_INTERVAL_SECONDS", 300))
    now = time.time()
    last_run = float(app.extensions.get("market_sync_last_run", 0.0))
    running = bool(app.extensions.get("market_sync_manual_running", False))

    if running:
        return False
    if not force and (now - last_run) < interval:
        return False

    if blocking:
        app.extensions["market_sync_manual_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["market_sync_last_run"] = time.time()
        finally:
            app.extensions["market_sync_manual_running"] = False
        return True

    def _worker():
        app.extensions["market_sync_manual_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["market_sync_last_run"] = time.time()
        finally:
            app.extensions["market_sync_manual_running"] = False

    Thread(target=_worker, daemon=True).start()
    return True


def start_market_sync(app):
    enabled_default = str(os.getenv("MARKET_SYNC_ENABLED", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    try:
        interval_default = max(int(os.getenv("MARKET_SYNC_INTERVAL_SECONDS", "300")), 60)
    except (TypeError, ValueError):
        interval_default = 300
    scope_default = str(os.getenv("MARKET_SYNC_SCOPE", "all") or "all").strip().lower()
    force_live_br_default = _as_bool(os.getenv("MARKET_SYNC_FORCE_LIVE_BR", "0"))
    app.config.setdefault("MARKET_SYNC_ENABLED", enabled_default)
    app.config.setdefault("MARKET_SYNC_INTERVAL_SECONDS", interval_default)
    app.config.setdefault("MARKET_SYNC_SCOPE", scope_default)
    app.config.setdefault("MARKET_SYNC_FORCE_LIVE_BR", force_live_br_default)
    app.extensions.setdefault("market_sync_last_run", 0.0)
    app.extensions.setdefault("market_sync_manual_running", False)
    should_start = app.config["MARKET_SYNC_ENABLED"] and should_run_background_jobs(app)
    init_job_status(
        app,
        "market_sync",
        interval_seconds=app.config["MARKET_SYNC_INTERVAL_SECONDS"],
        max_age_seconds=app.config["MARKET_SYNC_INTERVAL_SECONDS"] * 2,
        enabled=should_start,
    )

    if not should_start:
        return

    # Evita thread duplicada no processo pai do reloader do Flask.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    if app.extensions.get("market_sync_started"):
        return

    stop_event = Event()
    worker = Thread(target=_sync_loop, args=(app, stop_event), daemon=True)
    worker.start()

    app.extensions["market_sync_started"] = True
    app.extensions["market_sync_stop_event"] = stop_event
    app.extensions["market_sync_thread"] = worker

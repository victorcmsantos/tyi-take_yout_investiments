import os
import time
from threading import Event, Thread

from .services import rebuild_chart_snapshots


def _run_sync_once(app):
    with app.app_context():
        try:
            result = rebuild_chart_snapshots()
            app.logger.info(
                "Snapshot de graficos atualizado: %s carteira(s).",
                int(result.get("portfolios", 0)),
            )
        except Exception:
            app.logger.exception("Falha ao atualizar snapshot de graficos.")


def _sync_loop(app, stop_event: Event):
    interval = int(app.config.get("CHART_SNAPSHOT_INTERVAL_SECONDS", 300))
    warmup = bool(app.config.get("CHART_SNAPSHOT_WARMUP_ON_STARTUP", True))

    if warmup and not stop_event.is_set():
        app.extensions["chart_snapshot_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["chart_snapshot_last_run"] = time.time()
        finally:
            app.extensions["chart_snapshot_running"] = False

    while not stop_event.is_set():
        stop_event.wait(interval)
        if stop_event.is_set():
            break
        app.extensions["chart_snapshot_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["chart_snapshot_last_run"] = time.time()
        finally:
            app.extensions["chart_snapshot_running"] = False


def start_chart_sync(app):
    app.config.setdefault("CHART_SNAPSHOT_ENABLED", True)
    app.config.setdefault("CHART_SNAPSHOT_INTERVAL_SECONDS", 300)
    app.config.setdefault("CHART_SNAPSHOT_WARMUP_ON_STARTUP", True)
    app.config.setdefault("CHART_SNAPSHOT_MAX_AGE_SECONDS", 900)
    app.config.setdefault("BENCHMARK_CACHE_TTL_SECONDS", 900)
    app.config.setdefault("YAHOO_MONTHLY_CACHE_TTL_SECONDS", 21600)
    app.extensions.setdefault("chart_snapshot_last_run", 0.0)
    app.extensions.setdefault("chart_snapshot_running", False)

    if not app.config["CHART_SNAPSHOT_ENABLED"]:
        return

    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    if app.extensions.get("chart_snapshot_started"):
        return

    stop_event = Event()
    worker = Thread(target=_sync_loop, args=(app, stop_event), daemon=True)
    worker.start()

    app.extensions["chart_snapshot_started"] = True
    app.extensions["chart_snapshot_stop_event"] = stop_event
    app.extensions["chart_snapshot_thread"] = worker

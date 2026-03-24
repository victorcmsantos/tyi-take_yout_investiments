import os
import time
from threading import Event, Thread

from .observability import init_job_status, mark_job_finished, mark_job_started
from .runtime_lock import should_run_background_jobs
from .services import rebuild_fixed_income_snapshots


def _run_sync_once(app):
    with app.app_context():
        mark_job_started(app, "fixed_income_snapshot")
        try:
            result = rebuild_fixed_income_snapshots()
            mark_job_finished(app, "fixed_income_snapshot", result=result)
            app.logger.info(
                "Snapshot renda fixa atualizado: %s carteira(s), %s item(ns).",
                int(result.get("portfolios", 0)),
                int(result.get("items", 0)),
            )
        except Exception as exc:
            mark_job_finished(app, "fixed_income_snapshot", error=exc)
            app.logger.exception("Falha ao atualizar snapshot de renda fixa.")


def _sync_loop(app, stop_event: Event):
    interval = int(app.config.get("FIXED_INCOME_SNAPSHOT_INTERVAL_SECONDS", 300))
    warmup = bool(app.config.get("FIXED_INCOME_SNAPSHOT_WARMUP_ON_STARTUP", True))

    if warmup and not stop_event.is_set():
        app.extensions["fixed_income_snapshot_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["fixed_income_snapshot_last_run"] = time.time()
        finally:
            app.extensions["fixed_income_snapshot_running"] = False

    while not stop_event.is_set():
        stop_event.wait(interval)
        if stop_event.is_set():
            break
        app.extensions["fixed_income_snapshot_running"] = True
        try:
            _run_sync_once(app)
            app.extensions["fixed_income_snapshot_last_run"] = time.time()
        finally:
            app.extensions["fixed_income_snapshot_running"] = False


def start_fixed_income_sync(app):
    app.config.setdefault("FIXED_INCOME_SNAPSHOT_ENABLED", True)
    app.config.setdefault("FIXED_INCOME_SNAPSHOT_INTERVAL_SECONDS", 300)
    app.config.setdefault("FIXED_INCOME_SNAPSHOT_WARMUP_ON_STARTUP", True)
    app.config.setdefault("FIXED_INCOME_SNAPSHOT_MAX_AGE_SECONDS", 900)
    app.extensions.setdefault("fixed_income_snapshot_last_run", 0.0)
    app.extensions.setdefault("fixed_income_snapshot_running", False)
    should_start = app.config["FIXED_INCOME_SNAPSHOT_ENABLED"] and should_run_background_jobs(app)
    init_job_status(
        app,
        "fixed_income_snapshot",
        interval_seconds=app.config["FIXED_INCOME_SNAPSHOT_INTERVAL_SECONDS"],
        max_age_seconds=app.config["FIXED_INCOME_SNAPSHOT_MAX_AGE_SECONDS"],
        enabled=should_start,
        configured_enabled=app.config["FIXED_INCOME_SNAPSHOT_ENABLED"],
    )

    if not should_start:
        return

    # Evita thread duplicada no processo pai do reloader do Flask.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    if app.extensions.get("fixed_income_snapshot_started"):
        return

    stop_event = Event()
    worker = Thread(target=_sync_loop, args=(app, stop_event), daemon=True)
    worker.start()

    app.extensions["fixed_income_snapshot_started"] = True
    app.extensions["fixed_income_snapshot_stop_event"] = stop_event
    app.extensions["fixed_income_snapshot_thread"] = worker

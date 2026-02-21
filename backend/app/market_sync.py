import os
import time
from threading import Event, Thread

from .services import refresh_all_assets_market_data


def _run_sync_once(app):
    with app.app_context():
        try:
            failed = refresh_all_assets_market_data(attempts=5)
            if failed:
                app.logger.warning(
                    "Atualizacao Yahoo com falha temporaria para %s ticker(s): %s. Nova tentativa no proximo ciclo.",
                    len(failed),
                    ", ".join(failed[:10]),
                )
        except Exception:
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
    app.config.setdefault("MARKET_SYNC_ENABLED", True)
    app.config.setdefault("MARKET_SYNC_INTERVAL_SECONDS", 300)
    app.extensions.setdefault("market_sync_last_run", 0.0)
    app.extensions.setdefault("market_sync_manual_running", False)

    if not app.config["MARKET_SYNC_ENABLED"]:
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

import json
import logging
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from threading import Lock

from flask import current_app, g, has_app_context, has_request_context, request
from werkzeug.exceptions import HTTPException

from .db import get_db, list_database_backups
from .notifications import notify_event, telegram_status_payload


class JsonLogFormatter(logging.Formatter):
    def format(self, record):
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        event = getattr(record, "event", None)
        if event:
            payload["event"] = event
        details = getattr(record, "details", None)
        if isinstance(details, dict):
            payload.update(details)
        if has_request_context():
            payload.setdefault("request_id", getattr(g, "request_id", None))
            payload.setdefault("method", request.method)
            payload.setdefault("path", request.path)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def configure_observability(app):
    _configure_structured_logging(app)
    _init_metrics(app)
    _register_request_hooks(app)
    _register_error_handlers(app)


def _configure_structured_logging(app):
    formatter = JsonLogFormatter()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

    app.logger.handlers.clear()
    app.logger.propagate = True


def _init_metrics(app):
    app.extensions.setdefault("route_metrics", {})
    app.extensions.setdefault("route_metrics_lock", Lock())
    app.extensions.setdefault(
        "observability_started_at",
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
    )


def _job_state_to_persisted_payload(state):
    result = state.get("last_result")
    if result is None:
        result_json = None
    else:
        try:
            result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
        except TypeError:
            result_json = json.dumps(str(result), ensure_ascii=False)
    return {
        "job_name": str(state.get("name") or "").strip(),
        "configured_enabled": 1 if bool(state.get("configured_enabled")) else 0,
        "enabled": 1 if bool(state.get("enabled")) else 0,
        "running": 1 if bool(state.get("running")) else 0,
        "interval_seconds": int(state.get("interval_seconds") or 0),
        "max_age_seconds": int(state.get("max_age_seconds") or 0),
        "created_at": state.get("created_at"),
        "last_started_at": state.get("last_started_at"),
        "last_finished_at": state.get("last_finished_at"),
        "last_success_at": state.get("last_success_at"),
        "last_error_at": state.get("last_error_at"),
        "last_duration_ms": state.get("last_duration_ms"),
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "last_result_json": result_json,
        "last_error": state.get("last_error"),
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _persist_job_status(app, state):
    payload = _job_state_to_persisted_payload(state)
    if not payload["job_name"]:
        return
    sql = """
    INSERT INTO background_job_status (
      job_name,
      configured_enabled,
      enabled,
      running,
      interval_seconds,
      max_age_seconds,
      created_at,
      last_started_at,
      last_finished_at,
      last_success_at,
      last_error_at,
      last_duration_ms,
      consecutive_failures,
      last_result_json,
      last_error,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(job_name) DO UPDATE SET
      configured_enabled = excluded.configured_enabled,
      enabled = excluded.enabled,
      running = excluded.running,
      interval_seconds = excluded.interval_seconds,
      max_age_seconds = excluded.max_age_seconds,
      last_started_at = excluded.last_started_at,
      last_finished_at = excluded.last_finished_at,
      last_success_at = excluded.last_success_at,
      last_error_at = excluded.last_error_at,
      last_duration_ms = excluded.last_duration_ms,
      consecutive_failures = excluded.consecutive_failures,
      last_result_json = excluded.last_result_json,
      last_error = excluded.last_error,
      updated_at = excluded.updated_at
    """
    params = (
        payload["job_name"],
        payload["configured_enabled"],
        payload["enabled"],
        payload["running"],
        payload["interval_seconds"],
        payload["max_age_seconds"],
        payload["created_at"],
        payload["last_started_at"],
        payload["last_finished_at"],
        payload["last_success_at"],
        payload["last_error_at"],
        payload["last_duration_ms"],
        payload["consecutive_failures"],
        payload["last_result_json"],
        payload["last_error"],
        payload["updated_at"],
    )
    if has_app_context():
        db = get_db()
        db.execute(sql, params)
        db.commit()
        return

    connection = sqlite3.connect(app.config["DATABASE"])
    try:
        connection.execute(sql, params)
        connection.commit()
    finally:
        connection.close()


def _hydrate_job_state_from_row(row):
    result = None
    raw_result = row["last_result_json"]
    if raw_result:
        try:
            result = json.loads(raw_result)
        except (TypeError, ValueError):
            result = raw_result
    return {
        "name": row["job_name"],
        "created_at": row["created_at"],
        "configured_enabled": bool(row["configured_enabled"]),
        "enabled": bool(row["enabled"]),
        "running": bool(row["running"]),
        "interval_seconds": int(row["interval_seconds"] or 0),
        "max_age_seconds": int(row["max_age_seconds"] or 0),
        "last_started_at": row["last_started_at"],
        "last_finished_at": row["last_finished_at"],
        "last_success_at": row["last_success_at"],
        "last_error_at": row["last_error_at"],
        "last_duration_ms": row["last_duration_ms"],
        "consecutive_failures": int(row["consecutive_failures"] or 0),
        "last_result": result,
        "last_error": row["last_error"],
    }


def _register_request_hooks(app):
    @app.before_request
    def _before_request_metrics():
        g.request_started_at = time.perf_counter()
        g.request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]

    @app.after_request
    def _after_request_metrics(response):
        duration_ms = round((time.perf_counter() - g.request_started_at) * 1000, 2)
        response.headers["X-Request-ID"] = g.request_id
        response.headers["X-Response-Time-Ms"] = str(duration_ms)
        _record_request_metric(
            app,
            request.path,
            request.method,
            response.status_code,
            duration_ms,
        )
        app.logger.info(
            "request_complete",
            extra={
                "event": "request_complete",
                "details": {
                    "request_id": g.request_id,
                    "method": request.method,
                    "path": request.path,
                    "query_string": request.query_string.decode("utf-8", errors="ignore"),
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "remote_addr": request.headers.get("X-Forwarded-For", request.remote_addr),
                },
            },
        )
        return response


def _register_error_handlers(app):
    @app.errorhandler(Exception)
    def _handle_api_exception(exc):
        is_api_request = request.path.startswith("/api/")
        if isinstance(exc, HTTPException):
            status = exc.code or 500
            message = exc.description
        else:
            status = 500
            message = "Erro interno no servidor."

        log_payload = {
            "event": "request_failed",
            "details": {
                "request_id": getattr(g, "request_id", None),
                "method": request.method,
                "path": request.path,
                "status_code": status,
            },
        }
        if status >= 500:
            app.logger.exception("request_failed", extra=log_payload)
        else:
            app.logger.warning("request_failed", extra=log_payload)

        if is_api_request:
            return {"ok": False, "error": message}, status
        if isinstance(exc, HTTPException):
            return exc
        return "Internal Server Error", 500


def _record_request_metric(app, path, method, status_code, duration_ms):
    metrics = app.extensions["route_metrics"]
    lock = app.extensions["route_metrics_lock"]
    key = f"{method} {path}"
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with lock:
        current = metrics.setdefault(
            key,
            {
                "method": method,
                "path": path,
                "count": 0,
                "errors_4xx": 0,
                "errors_5xx": 0,
                "total_duration_ms": 0.0,
                "avg_duration_ms": 0.0,
                "max_duration_ms": 0.0,
                "last_status_code": None,
                "last_seen_at": None,
            },
        )
        current["count"] += 1
        current["total_duration_ms"] += duration_ms
        current["avg_duration_ms"] = round(current["total_duration_ms"] / current["count"], 2)
        current["max_duration_ms"] = round(max(current["max_duration_ms"], duration_ms), 2)
        current["last_status_code"] = status_code
        current["last_seen_at"] = now
        if 400 <= status_code < 500:
            current["errors_4xx"] += 1
        elif status_code >= 500:
            current["errors_5xx"] += 1


def init_job_status(app, job_name, interval_seconds, max_age_seconds, enabled=True, configured_enabled=None):
    configured_flag = bool(enabled) if configured_enabled is None else bool(configured_enabled)
    app.extensions.setdefault("job_statuses", {})
    state = app.extensions["job_statuses"].setdefault(
        job_name,
        {
            "name": job_name,
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "configured_enabled": configured_flag,
            "enabled": bool(enabled),
            "running": False,
            "interval_seconds": int(interval_seconds),
            "max_age_seconds": int(max_age_seconds),
            "last_started_at": None,
            "last_finished_at": None,
            "last_success_at": None,
            "last_error_at": None,
            "last_duration_ms": None,
            "consecutive_failures": 0,
            "last_result": None,
            "last_error": None,
        },
    )
    state["configured_enabled"] = configured_flag
    state["enabled"] = bool(enabled)
    state["interval_seconds"] = int(interval_seconds)
    state["max_age_seconds"] = int(max_age_seconds)
    if bool(enabled) or not configured_flag:
        _persist_job_status(app, state)


def mark_job_started(app, job_name):
    state = app.extensions["job_statuses"][job_name]
    state["running"] = True
    state["last_started_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["_started_perf"] = time.perf_counter()
    _persist_job_status(app, state)


def mark_job_finished(app, job_name, result=None, error=None):
    state = app.extensions["job_statuses"][job_name]
    previous_failures = int(state.get("consecutive_failures") or 0)
    started_perf = state.pop("_started_perf", None)
    duration_ms = None
    if started_perf is not None:
        duration_ms = round((time.perf_counter() - started_perf) * 1000, 2)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["running"] = False
    state["last_finished_at"] = now
    state["last_duration_ms"] = duration_ms
    if error is None:
        state["last_success_at"] = now
        state["last_result"] = result
        state["last_error"] = None
        state["consecutive_failures"] = 0
        if previous_failures > 0:
            notify_event(
                "job_recovered",
                f"Job recuperado: {job_name}",
                details={
                    "job": job_name,
                    "duration_ms": duration_ms,
                    "result": result,
                },
                dedupe_key=f"job:recovered:{job_name}",
                min_interval_seconds=60,
            )
    else:
        state["last_error_at"] = now
        state["last_error"] = str(error)
        state["consecutive_failures"] += 1
        notify_event(
            "job_failed",
            f"Falha no job: {job_name}",
            details={
                "job": job_name,
                "consecutive_failures": int(state.get("consecutive_failures") or 0),
                "duration_ms": duration_ms,
                "error": str(error),
            },
            dedupe_key=f"job:failed:{job_name}",
            min_interval_seconds=300,
        )
    _persist_job_status(app, state)


def get_job_statuses(app):
    try:
        rows = get_db().execute(
            """
            SELECT
              job_name,
              configured_enabled,
              enabled,
              running,
              interval_seconds,
              max_age_seconds,
              created_at,
              last_started_at,
              last_finished_at,
              last_success_at,
              last_error_at,
              last_duration_ms,
              consecutive_failures,
              last_result_json,
              last_error
            FROM background_job_status
            ORDER BY job_name ASC
            """
        ).fetchall()
    except Exception:
        rows = []

    if rows:
        statuses = []
        for row in rows:
            item = _hydrate_job_state_from_row(row)
            item["stale"] = _is_job_stale(item)
            statuses.append(item)
        return statuses

    statuses = []
    for state in app.extensions.get("job_statuses", {}).values():
        item = dict(state)
        item.pop("_started_perf", None)
        item["stale"] = _is_job_stale(item)
        statuses.append(item)
    return sorted(statuses, key=lambda item: item["name"])


def _is_job_stale(state):
    if not state.get("enabled"):
        return False
    if state.get("running"):
        return False
    max_age_seconds = int(state.get("max_age_seconds") or 0)
    if max_age_seconds <= 0:
        return False
    last_success_at = state.get("last_success_at")
    if not last_success_at:
        reference_at = state.get("last_finished_at") or state.get("created_at")
        if not reference_at:
            return False
        reference_time = datetime.fromisoformat(reference_at)
        age_seconds = (datetime.now(timezone.utc) - reference_time).total_seconds()
        return age_seconds > max_age_seconds
    last_success = datetime.fromisoformat(last_success_at)
    age_seconds = (datetime.now(timezone.utc) - last_success).total_seconds()
    return age_seconds > max_age_seconds


def get_route_metrics(app):
    metrics = app.extensions.get("route_metrics", {})
    return [dict(value) for _, value in sorted(metrics.items(), key=lambda item: item[0])]


def get_provider_circuit_statuses():
    now_epoch = time.time()
    rows = []
    try:
        rows = get_db().execute(
            """
            SELECT provider, disabled_until, status_code, updated_at
            FROM api_provider_circuit_state
            ORDER BY provider ASC
            """
        ).fetchall()
    except Exception:
        return []

    payload = []
    for row in rows:
        until_epoch = float(row["disabled_until"] or 0.0)
        is_active = until_epoch > now_epoch
        remaining = max(int(until_epoch - now_epoch), 0) if is_active else 0
        payload.append(
            {
                "provider": str(row["provider"] or "").strip().lower(),
                "active": bool(is_active),
                "disabled_until_epoch": until_epoch,
                "remaining_seconds": remaining,
                "status_code": row["status_code"],
                "updated_at": row["updated_at"],
            }
        )
    return payload


def get_provider_usage_statuses():
    try:
        from .services import _legacy as legacy_market
    except Exception:
        return []

    statuses = []
    for provider in ("brapi", "coingecko"):
        try:
            status = legacy_market._provider_usage_status(provider)
        except Exception:
            status = {"provider": provider, "windows": {}}
        statuses.append(status)
    return statuses


def build_health_payload():
    db_status = _database_status()
    backups = list_database_backups()
    job_statuses = get_job_statuses(current_app)
    provider_circuits = get_provider_circuit_statuses()
    provider_usage = get_provider_usage_statuses()
    budget_exhausted = False
    for provider in provider_usage:
        for window_payload in (provider.get("windows") or {}).values():
            limit = int(window_payload.get("limit") or 0)
            remaining = window_payload.get("remaining")
            if limit > 0 and remaining is not None and int(remaining) <= 0:
                budget_exhausted = True
                break
        if budget_exhausted:
            break
    degraded = (
        not db_status["ok"]
        or any(item["consecutive_failures"] > 0 for item in job_statuses)
        or any(item["stale"] for item in job_statuses)
        or any(item.get("active") for item in provider_circuits)
        or budget_exhausted
    )
    current_status = "degraded" if degraded else "ok"
    previous_status = current_app.extensions.get("health_last_status")
    current_app.extensions["health_last_status"] = current_status
    if previous_status and previous_status != current_status:
        notify_event(
            "health_degraded" if current_status == "degraded" else "health_recovered",
            f"Saude do sistema: {current_status.upper()}",
            details={
                "status": current_status,
                "database_ok": bool(db_status.get("ok")),
                "jobs_failed": sum(1 for item in job_statuses if int(item.get("consecutive_failures") or 0) > 0),
                "jobs_stale": sum(1 for item in job_statuses if bool(item.get("stale"))),
                "active_circuits": sum(1 for item in provider_circuits if bool(item.get("active"))),
                "budget_exhausted": bool(budget_exhausted),
            },
            dedupe_key=f"health:status:{current_status}",
            min_interval_seconds=120,
        )
    return {
        "status": current_status,
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "database": db_status,
        "backups": {
            "count": len(backups),
            "latest": backups[0] if backups else None,
        },
        "jobs": job_statuses,
        "provider_circuits": provider_circuits,
        "provider_usage": provider_usage,
        "telegram": telegram_status_payload(),
        "metrics": {
            "routes_tracked": len(current_app.extensions.get("route_metrics", {})),
        },
    }


def _database_status():
    try:
        row = get_db().execute("SELECT 1 AS ok").fetchone()
        return {"ok": bool(row and row["ok"] == 1), "engine": "sqlite"}
    except Exception as exc:
        return {"ok": False, "engine": "sqlite", "error": str(exc)}

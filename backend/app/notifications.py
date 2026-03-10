import json
import os
import time
from datetime import datetime, timezone
from threading import Lock, Thread
from urllib import error as urlerror
from urllib import request as urlrequest

from flask import current_app, has_app_context


_DEDUP_CACHE = {}
_DEDUP_LOCK = Lock()
_DEFAULT_EVENTS = {
    "startup",
    "manual_scan_started",
    "manual_scan_success",
    "manual_scan_failed",
    "sync_ticker_success",
    "sync_ticker_failed",
    "provider_circuit_open",
    "provider_circuit_closed",
    "provider_budget_exhausted",
    "job_failed",
    "job_recovered",
    "health_degraded",
    "health_recovered",
    "admin_test",
    "swing_trade_opened",
    "swing_trade_updated",
    "swing_trade_closed",
    "swing_trade_failed",
}
_PROFILE_EVENTS = {
    "prod": {
        "job_failed",
        "job_recovered",
        "provider_circuit_open",
        "provider_circuit_closed",
        "provider_budget_exhausted",
        "manual_scan_failed",
        "health_degraded",
        "health_recovered",
        "admin_test",
        "swing_trade_opened",
        "swing_trade_updated",
        "swing_trade_closed",
        "swing_trade_failed",
    },
    "debug": {
        "startup",
        "manual_scan_started",
        "manual_scan_success",
        "manual_scan_failed",
        "sync_ticker_success",
        "sync_ticker_failed",
        "provider_circuit_open",
        "provider_circuit_closed",
        "provider_budget_exhausted",
        "job_failed",
        "job_recovered",
        "health_degraded",
        "health_recovered",
        "admin_test",
        "swing_trade_opened",
        "swing_trade_updated",
        "swing_trade_closed",
        "swing_trade_failed",
    },
}


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iso_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _settings():
    enabled = _as_bool(os.getenv("TELEGRAM_ENABLED", "0"))
    token = str(os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = str(os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    thread_id = _safe_int(os.getenv("TELEGRAM_THREAD_ID"))
    profile_raw = str(os.getenv("TELEGRAM_NOTIFY_PROFILE") or "prod").strip().lower()
    profile_alias = {"production": "prod", "verbose": "debug"}
    profile = profile_alias.get(profile_raw, profile_raw)
    if profile not in _PROFILE_EVENTS:
        profile = "prod"
    prefix = str(os.getenv("TELEGRAM_MESSAGE_PREFIX") or "TYI").strip() or "TYI"
    timeout_seconds = _safe_int(os.getenv("TELEGRAM_TIMEOUT_SECONDS"))
    default_interval = _safe_int(os.getenv("TELEGRAM_DEFAULT_MIN_INTERVAL_SECONDS"))
    raw_events = str(os.getenv("TELEGRAM_NOTIFY_EVENTS") or "").strip()
    events = {
        item.strip().lower()
        for item in raw_events.split(",")
        if item and item.strip()
    }
    if not events:
        events = set(_PROFILE_EVENTS.get(profile) or _DEFAULT_EVENTS)
    return {
        "enabled": bool(enabled),
        "token": token,
        "chat_id": chat_id,
        "thread_id": thread_id if thread_id and thread_id > 0 else None,
        "profile": profile,
        "prefix": prefix,
        "timeout_seconds": max(int(timeout_seconds or 8), 3),
        "default_interval": max(int(default_interval or 120), 0),
        "events": events,
    }


def telegram_status_payload():
    cfg = _settings()
    return {
        "enabled": bool(cfg["enabled"]),
        "configured": bool(cfg["token"] and cfg["chat_id"]),
        "bot_configured": bool(cfg["token"]),
        "chat_configured": bool(cfg["chat_id"]),
        "thread_id": cfg["thread_id"],
        "notify_profile": str(cfg.get("profile") or "prod"),
        "notify_events": sorted(cfg["events"]),
        "default_min_interval_seconds": int(cfg["default_interval"]),
        "updated_at": _iso_now(),
    }


def _event_allowed(event_key: str, cfg: dict):
    event = str(event_key or "").strip().lower()
    if not event:
        return True
    events = set(cfg.get("events") or set())
    if "all" in events or "*" in events:
        return True
    return event in events


def _dedupe_allows_send(dedupe_key: str, min_interval_seconds: int):
    key = str(dedupe_key or "").strip().lower()
    interval = max(int(min_interval_seconds or 0), 0)
    if not key or interval <= 0:
        return True

    now = time.time()
    with _DEDUP_LOCK:
        last_sent = float(_DEDUP_CACHE.get(key) or 0.0)
        if last_sent > 0 and (now - last_sent) < interval:
            return False
        _DEDUP_CACHE[key] = now
        if len(_DEDUP_CACHE) > 2048:
            threshold = now - 86400
            stale = [item for item, ts in _DEDUP_CACHE.items() if float(ts) < threshold]
            for item in stale:
                _DEDUP_CACHE.pop(item, None)
    return True


def _dispatch_telegram(cfg: dict, text: str):
    if not cfg.get("token") or not cfg.get("chat_id"):
        return {
            "sent": False,
            "error": "TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID ausentes.",
            "status_code": None,
            "response": None,
        }

    payload = {
        "chat_id": cfg["chat_id"],
        "text": (str(text or "") or "").strip()[:3900],
        "disable_web_page_preview": True,
    }
    if cfg.get("thread_id"):
        payload["message_thread_id"] = int(cfg["thread_id"])

    endpoint = f"https://api.telegram.org/bot{cfg['token']}/sendMessage"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request_obj = urlrequest.Request(
        endpoint,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urlrequest.urlopen(request_obj, timeout=float(cfg.get("timeout_seconds") or 8)) as response:
            status_code = int(response.getcode() or 200)
            raw = response.read().decode("utf-8", errors="ignore")
        parsed = None
        if raw:
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"raw": raw}
        ok = bool(status_code < 400 and (not isinstance(parsed, dict) or parsed.get("ok", True)))
        return {
            "sent": ok,
            "error": None if ok else "Telegram retornou resposta invalida.",
            "status_code": status_code,
            "response": parsed,
        }
    except urlerror.HTTPError as exc:
        body_text = ""
        try:
            body_text = exc.read().decode("utf-8", errors="ignore")
        except Exception:
            body_text = ""
        return {
            "sent": False,
            "error": f"HTTPError {exc.code}",
            "status_code": int(exc.code),
            "response": body_text,
        }
    except Exception as exc:
        return {
            "sent": False,
            "error": str(exc),
            "status_code": None,
            "response": None,
        }


def send_telegram_text(
    text: str,
    *,
    event_key: str = "custom",
    dedupe_key: str | None = None,
    min_interval_seconds: int | None = None,
    asynchronous: bool = True,
    force: bool = False,
):
    cfg = _settings()
    if not cfg["enabled"] and not force:
        return {
            "queued": False,
            "sent": False,
            "reason": "disabled",
            "event": str(event_key or "").strip().lower(),
        }
    if not _event_allowed(event_key, cfg) and not force:
        return {
            "queued": False,
            "sent": False,
            "reason": "event_not_allowed",
            "event": str(event_key or "").strip().lower(),
        }

    interval = (
        int(cfg["default_interval"])
        if min_interval_seconds is None
        else max(int(min_interval_seconds), 0)
    )
    dedupe = dedupe_key if dedupe_key else f"event:{str(event_key or '').strip().lower()}"
    if not force and not _dedupe_allows_send(dedupe, interval):
        return {
            "queued": False,
            "sent": False,
            "deduplicated": True,
            "reason": "deduplicated",
            "event": str(event_key or "").strip().lower(),
        }

    if not cfg.get("token") or not cfg.get("chat_id"):
        return {
            "queued": False,
            "sent": False,
            "reason": "missing_configuration",
            "event": str(event_key or "").strip().lower(),
        }

    if asynchronous:
        def _worker():
            result = _dispatch_telegram(cfg, text)
            if not result.get("sent") and has_app_context():
                current_app.logger.warning(
                    "Falha ao enviar notificacao Telegram: event=%s status=%s error=%s",
                    event_key,
                    result.get("status_code"),
                    result.get("error"),
                )

        Thread(target=_worker, daemon=True, name=f"telegram-{event_key}").start()
        return {
            "queued": True,
            "sent": False,
            "reason": "queued",
            "event": str(event_key or "").strip().lower(),
        }

    result = _dispatch_telegram(cfg, text)
    response = {
        "queued": False,
        "sent": bool(result.get("sent")),
        "reason": "sent" if result.get("sent") else "failed",
        "event": str(event_key or "").strip().lower(),
        "status_code": result.get("status_code"),
        "error": result.get("error"),
    }
    if result.get("response") is not None:
        response["response"] = result.get("response")
    return response


def _format_detail_value(value):
    if isinstance(value, bool):
        return "sim" if value else "nao"
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list)):
        try:
            text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            text = str(value)
        return text[:500]
    return str(value)


def notify_event(
    event_key: str,
    title: str,
    *,
    details: dict | None = None,
    dedupe_key: str | None = None,
    min_interval_seconds: int | None = None,
    asynchronous: bool = True,
    force: bool = False,
):
    cfg = _settings()
    prefix = str(cfg.get("prefix") or "TYI").strip() or "TYI"
    event = str(event_key or "").strip().lower() or "event"
    lines = [f"[{prefix}] {title}", f"Evento: {event}", f"Quando: {_iso_now()}"]
    if isinstance(details, dict) and details:
        for key in sorted(details.keys()):
            value = _format_detail_value(details.get(key))
            lines.append(f"{key}: {value}")
    text = "\n".join(lines)
    return send_telegram_text(
        text,
        event_key=event,
        dedupe_key=dedupe_key,
        min_interval_seconds=min_interval_seconds,
        asynchronous=asynchronous,
        force=force,
    )

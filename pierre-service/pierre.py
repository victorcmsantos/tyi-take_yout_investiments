"""Read-only client for the Pierre Finance API with a self-contained SQLite
cache (TTL + stale-while-revalidate). Used only inside the pierre-service
container; the API key never leaves it.
"""

import json
import logging
import os
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime

log = logging.getLogger("pierre")


class PierreNotConfigured(RuntimeError):
    pass


class PierreError(RuntimeError):
    pass


CACHE_PATH = os.getenv("PIERRE_CACHE_DB", "/data/pierre_cache.db")


def _base_url():
    return os.getenv("PIERRE_BASE_URL", "https://www.pierre.finance").rstrip("/")


def _api_key():
    key = (os.getenv("PIERRE_API_KEY") or "").strip()
    if not key:
        raise PierreNotConfigured("PIERRE_API_KEY nao configurada.")
    return key


def _ttl_seconds():
    try:
        return int(os.getenv("PIERRE_CACHE_TTL_SECONDS", "300"))
    except ValueError:
        return 300


def _timeout_seconds():
    try:
        return float(os.getenv("PIERRE_TIMEOUT_SECONDS", "15"))
    except ValueError:
        return 15.0


def _now_iso():
    return datetime.now().isoformat(timespec="seconds")


def _age_seconds(iso_text):
    try:
        created = datetime.fromisoformat((iso_text or "").strip())
    except (TypeError, ValueError):
        return None
    return max((datetime.now() - created).total_seconds(), 0.0)


# --- cache --------------------------------------------------------------------

_DB_LOCK = threading.Lock()


def _db():
    os.makedirs(os.path.dirname(CACHE_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(CACHE_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pierre_cache (
          cache_key TEXT PRIMARY KEY,
          payload_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    return conn


def _cache_key(path, params):
    items = sorted((str(k), str(v)) for k, v in (params or {}).items() if v not in (None, ""))
    return path + "?" + urllib.parse.urlencode(items)


def _cache_read(key):
    try:
        with _DB_LOCK, _db() as conn:
            row = conn.execute(
                "SELECT payload_json, updated_at FROM pierre_cache WHERE cache_key = ?",
                (key,),
            ).fetchone()
    except Exception:
        return None
    if row is None:
        return None
    try:
        return json.loads(row["payload_json"]), _age_seconds(row["updated_at"])
    except (TypeError, ValueError):
        return None


def _cache_write(key, payload):
    try:
        with _DB_LOCK, _db() as conn:
            conn.execute(
                """
                INSERT INTO pierre_cache (cache_key, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                  payload_json = excluded.payload_json, updated_at = excluded.updated_at
                """,
                (key, json.dumps(payload, ensure_ascii=False), _now_iso()),
            )
            conn.commit()
    except Exception:
        pass


# --- live HTTP + read-through --------------------------------------------------

_REFRESH_INFLIGHT = set()
_REFRESH_LOCK = threading.Lock()

# Pierre's API rate-limits aggressively and answers throttled calls with
# 401 invalid_api_key ("try again later"), not 429. Retrying right away only
# burns the quota, so background refreshes back off (delays below) and, after
# any throttled response, ALL refresh attempts pause for a shared cooldown.
_REFRESH_RETRY_DELAYS = (0, 300, 900, 1800)  # seconds before attempts 1..4
_THROTTLE_COOLDOWN = 600
_THROTTLE_LOCK = threading.Lock()
_throttled_until = 0.0


def _mark_throttled():
    global _throttled_until
    with _THROTTLE_LOCK:
        _throttled_until = max(_throttled_until, time.time() + _THROTTLE_COOLDOWN)


def _throttle_remaining():
    with _THROTTLE_LOCK:
        return max(_throttled_until - time.time(), 0.0)


def _live_get(path, params):
    url = _base_url() + path
    query = urllib.parse.urlencode([(k, v) for k, v in (params or {}).items() if v not in (None, "")])
    if query:
        url = url + "?" + query
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {_api_key()}",
            "Accept": "application/json",
            "User-Agent": "tyi-pierre-service",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 429):
            _mark_throttled()
        raise


def _refresh_async(key, path, params):
    with _REFRESH_LOCK:
        if key in _REFRESH_INFLIGHT:
            return
        _REFRESH_INFLIGHT.add(key)

    def _worker():
        try:
            for attempt, delay in enumerate(_REFRESH_RETRY_DELAYS, start=1):
                wait = delay + _throttle_remaining()
                if wait:
                    time.sleep(wait)
                try:
                    payload = _live_get(path, params)
                except Exception as exc:
                    log.warning(
                        "refresh de %s falhou (tentativa %d/%d): %s",
                        key, attempt, len(_REFRESH_RETRY_DELAYS), exc,
                    )
                    continue
                if isinstance(payload, dict) and payload.get("success") is not False:
                    _cache_write(key, payload)
                    if attempt > 1:
                        log.warning("refresh de %s ok na tentativa %d", key, attempt)
                    return
                log.warning("refresh de %s retornou success=false; mantendo cache", key)
        finally:
            with _REFRESH_LOCK:
                _REFRESH_INFLIGHT.discard(key)

    threading.Thread(target=_worker, name="pierre-refresh", daemon=True).start()


def _get(path, params=None):
    key = _cache_key(path, params)
    cached = _cache_read(key)
    if cached is not None:
        payload, age = cached
        if age is not None and age > _ttl_seconds():
            _refresh_async(key, path, params)
        return payload
    try:
        payload = _live_get(path, params)
    except Exception as exc:
        stale = _cache_read(key)
        if stale is not None:
            return stale[0]
        raise PierreError(f"Pierre indisponivel e sem cache: {exc}") from exc
    if isinstance(payload, dict) and payload.get("success") is not False:
        _cache_write(key, payload)
    return payload


# --- public read API ----------------------------------------------------------

def get_accounts():
    return _get("/tools/api/get-accounts")


def get_balance():
    return _get("/tools/api/get-balance")


def get_transactions(start_date=None, end_date=None, account_type=None, fmt="structured"):
    return _get(
        "/tools/api/get-transactions",
        {"startDate": start_date, "endDate": end_date, "accountType": account_type, "format": fmt},
    )


def get_bills(account_id=None):
    return _get("/tools/api/get-bills", {"accountId": account_id})


def get_installments(start_date=None, end_date=None):
    return _get("/tools/api/get-installments", {"startDate": start_date, "endDate": end_date})


def is_configured():
    try:
        _api_key()
        return True
    except PierreNotConfigured:
        return False

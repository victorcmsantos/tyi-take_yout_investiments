"""Read-only client for the Pierre Finance API with a self-contained SQLite
cache (TTL + stale-while-revalidate). Used only inside the pierre-service
container; the API key never leaves it.
"""

import json
import os
import sqlite3
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime


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
    with urllib.request.urlopen(request, timeout=_timeout_seconds()) as response:
        return json.loads(response.read().decode("utf-8"))


def _refresh_async(key, path, params):
    with _REFRESH_LOCK:
        if key in _REFRESH_INFLIGHT:
            return
        _REFRESH_INFLIGHT.add(key)

    def _worker():
        try:
            payload = _live_get(path, params)
            if isinstance(payload, dict) and payload.get("success") is not False:
                _cache_write(key, payload)
        except Exception:
            pass
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

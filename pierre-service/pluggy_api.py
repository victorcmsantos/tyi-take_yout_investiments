"""Read-only client for the Pluggy API (direct Open Finance aggregator — the
same source Pierre uses upstream, without Pierre's rate limits).

Auth: POST /auth with clientId/clientSecret issues an apiKey valid ~2h; it is
cached in-process and renewed on expiry or on a 401/403. Item IDs (one per
connected institution) come from PLUGGY_ITEM_IDS, comma-separated.

Transactions use GET /v2/transactions (cursor pagination via `next`; the v1
endpoint returns 410 Gone). Date filters are applied client-side: pages come
newest-first, so pagination stops once a page's oldest date is older than the
requested floor.
"""

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("pluggy")

BASE_URL = "https://api.pluggy.ai"
_API_KEY_TTL = 100 * 60  # Pluggy issues ~2h keys; renew comfortably earlier.


class PluggyNotConfigured(RuntimeError):
    pass


class PluggyError(RuntimeError):
    pass


def _credentials():
    client_id = (os.getenv("PLUGGY_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("PLUGGY_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret:
        raise PluggyNotConfigured("PLUGGY_CLIENT_ID/PLUGGY_CLIENT_SECRET nao configurados.")
    return client_id, client_secret


def item_ids():
    raw = os.getenv("PLUGGY_ITEM_IDS") or ""
    return [p.strip() for p in raw.split(",") if p.strip()]


def _timeout_seconds():
    try:
        return float(os.getenv("PLUGGY_TIMEOUT_SECONDS", "30"))
    except ValueError:
        return 30.0


def is_configured():
    try:
        _credentials()
        return True
    except PluggyNotConfigured:
        return False


# --- auth (apiKey cached in-process) -------------------------------------------

_AUTH_LOCK = threading.Lock()
_api_key = None
_api_key_expires = 0.0


def _request(url, headers, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=_timeout_seconds()) as response:
        return json.loads(response.read().decode("utf-8"))


def _auth_key(force=False):
    global _api_key, _api_key_expires
    with _AUTH_LOCK:
        if not force and _api_key and time.time() < _api_key_expires:
            return _api_key
        client_id, client_secret = _credentials()
        try:
            payload = _request(
                BASE_URL + "/auth",
                {"Content-Type": "application/json"},
                {"clientId": client_id, "clientSecret": client_secret},
            )
        except urllib.error.HTTPError as exc:
            raise PluggyError(f"Pluggy auth falhou: HTTP {exc.code}") from exc
        key = payload.get("apiKey")
        if not key:
            raise PluggyError("Pluggy auth nao retornou apiKey.")
        _api_key = key
        _api_key_expires = time.time() + _API_KEY_TTL
        return key


def _get(path, params=None, raw_query=None):
    """raw_query: query string pronta (ex.: o campo `next` da paginação v2,
    que já vem como '?accountId=...&after=...')."""
    if raw_query is not None:
        query = raw_query.lstrip("?")
    else:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v not in (None, "")})
    url = BASE_URL + path + (f"?{query}" if query else "")
    for attempt in (1, 2):
        key = _auth_key(force=attempt == 2)
        try:
            return _request(url, {"X-API-KEY": key, "Accept": "application/json"})
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403) and attempt == 1:
                continue  # apiKey expirou no meio do caminho; renova e tenta 1x
            detail = ""
            try:
                detail = exc.read().decode()[:200]
            except Exception:
                pass
            raise PluggyError(f"Pluggy GET {path} falhou: HTTP {exc.code} {detail}") from exc
        except Exception as exc:
            raise PluggyError(f"Pluggy GET {path} falhou: {exc}") from exc


# --- public read API ------------------------------------------------------------

def get_item(item_id):
    return _get(f"/items/{item_id}")


def get_accounts(item_id):
    return _get("/accounts", {"itemId": item_id}).get("results", [])


def get_transactions(account_id, date_from=None, date_to=None, max_pages=20):
    """All transactions for an account, newest-first, filtered by [date_from,
    date_to] (YYYY-MM-DD). Cursor pagination; stops early once a page is
    entirely older than date_from."""
    results = []
    next_query = None
    for _ in range(max_pages):
        if next_query:
            page = _get("/v2/transactions", raw_query=next_query)
        else:
            page = _get("/v2/transactions", {"accountId": account_id})
        rows = page.get("results", [])
        for t in rows:
            d = str(t.get("date") or "")[:10]
            if date_to and d > date_to:
                continue
            if date_from and d < date_from:
                continue
            results.append(t)
        next_query = page.get("next")
        if not next_query or not rows:
            break
        oldest = min(str(t.get("date") or "")[:10] for t in rows)
        if date_from and oldest < date_from:
            break
    return results


def get_investments(item_id):
    return _get("/investments", {"itemId": item_id}).get("results", [])


def status():
    """Connectivity summary for the configured items — used by /pluggy-status."""
    out = {"configured": is_configured(), "items": []}
    if not out["configured"]:
        return out
    for item_id in item_ids():
        entry = {"id": item_id}
        try:
            it = get_item(item_id)
            accs = get_accounts(item_id)
            entry.update({
                "connector": ((it.get("connector") or {}).get("name")),
                "status": it.get("status"),
                "last_updated": it.get("lastUpdatedAt") or it.get("updatedAt"),
                "accounts": [
                    {
                        "id": a.get("id"),
                        "type": a.get("type"),
                        "name": a.get("name"),
                        "number": a.get("number"),
                        "balance": a.get("balance"),
                    }
                    for a in accs
                ],
            })
        except Exception as exc:  # noqa: BLE001 - status é diagnóstico
            entry["error"] = str(exc)
        out["items"].append(entry)
    return out

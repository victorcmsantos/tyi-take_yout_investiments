"""Auth-gated proxy to the internal pierre-service, mounted under /api/pierre.

Auth is enforced globally for /api/* in create_app(); this layer only forwards
the (read-only) request to the internal Pierre microservice and relays its JSON
response. The Pierre API key lives only inside pierre-service.
"""

import urllib.error
import urllib.request

from flask import Blueprint, current_app, jsonify, request

pierre_bp = Blueprint("pierre", __name__)

# Read-only data endpoints (GET only) and the writable overrides endpoint.
_ALLOWED = {
    "status", "accounts", "balance", "transactions", "bills", "installments",
    "cashflow", "overview", "ledger", "overrides",
    "manual-accounts", "manual-transactions", "recurring-items", "settings",
    "pluggy-status", "pluggy-transactions",
}
_WRITABLE = {"overrides", "manual-accounts", "manual-transactions", "recurring-items", "settings"}


def _service_url():
    return str(current_app.config.get("PIERRE_SERVICE_URL", "http://pierre-service:8000")).rstrip("/")


def _timeout():
    try:
        return float(current_app.config.get("PIERRE_TIMEOUT_SECONDS", 20))
    except (TypeError, ValueError):
        return 20.0


def _proxy(endpoint):
    qs = request.query_string.decode()
    url = f"{_service_url()}/{endpoint}" + (f"?{qs}" if qs else "")
    headers = {"Accept": "application/json"}
    data = None
    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        data = request.get_data() or b""
        headers["Content-Type"] = request.headers.get("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, method=request.method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=_timeout()) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        body = exc.read()
        status = exc.code
    except Exception as exc:  # noqa: BLE001 - service unreachable
        return jsonify({"ok": False, "error": f"pierre-service indisponivel: {exc}"}), 502
    return current_app.response_class(body, status=status, mimetype="application/json")


@pierre_bp.route("/<endpoint>", methods=["GET", "POST", "DELETE"])
def proxy(endpoint):
    if endpoint not in _ALLOWED:
        return jsonify({"ok": False, "error": "Endpoint Pierre desconhecido."}), 404
    if request.method != "GET" and endpoint not in _WRITABLE:
        return jsonify({"ok": False, "error": "Operação não permitida."}), 405
    return _proxy(endpoint)

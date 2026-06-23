"""Auth-gated proxy to the internal pierre-service, mounted under /api/pierre.

Auth is enforced globally for /api/* in create_app(); this layer only forwards
the (read-only) request to the internal Pierre microservice and relays its JSON
response. The Pierre API key lives only inside pierre-service.
"""

import urllib.error
import urllib.request

from flask import Blueprint, current_app, jsonify, request

pierre_bp = Blueprint("pierre", __name__)

# Read-only endpoints we allow forwarding to pierre-service.
_ALLOWED = {"status", "accounts", "balance", "transactions", "bills", "installments", "cashflow", "overview"}


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
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
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


@pierre_bp.route("/<endpoint>", methods=["GET"])
def proxy(endpoint):
    if endpoint not in _ALLOWED:
        return jsonify({"ok": False, "error": "Endpoint Pierre desconhecido."}), 404
    return _proxy(endpoint)

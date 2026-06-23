"""pierre-service: internal-only Flask app that proxies the Pierre Finance API
with caching. Not exposed to the host; the TYI backend calls it server-to-server
over the private network. No auth here — auth is enforced by the backend.
"""

from datetime import date

from flask import Flask, jsonify, request

import cashflow
import overview
import pierre

app = Flask(__name__)


def _ok(payload=None, status=200):
    return jsonify({"ok": True, "data": payload}), status


def _err(message, status=502):
    return jsonify({"ok": False, "error": message}), status


def _guard(fn):
    try:
        return _ok(fn())
    except pierre.PierreNotConfigured:
        return _err("Integracao Pierre nao configurada (PIERRE_API_KEY).", status=503)
    except pierre.PierreError as exc:
        return _err(str(exc), status=502)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao consultar Pierre: {exc}", status=502)


@app.get("/health")
def health():
    return _ok({"service": "pierre", "configured": pierre.is_configured()})


@app.get("/status")
def status():
    return _ok({"configured": pierre.is_configured()})


@app.get("/accounts")
def accounts():
    return _guard(pierre.get_accounts)


@app.get("/balance")
def balance():
    return _guard(pierre.get_balance)


@app.get("/transactions")
def transactions():
    return _guard(
        lambda: pierre.get_transactions(
            start_date=request.args.get("startDate"),
            end_date=request.args.get("endDate"),
            account_type=request.args.get("accountType"),
            fmt=request.args.get("format", "structured"),
        )
    )


@app.get("/bills")
def bills():
    return _guard(lambda: pierre.get_bills(request.args.get("accountId")))


@app.get("/installments")
def installments():
    return _guard(
        lambda: pierre.get_installments(
            start_date=request.args.get("startDate"),
            end_date=request.args.get("endDate"),
        )
    )


@app.get("/overview")
def overview_route():
    def _build():
        raw = request.args.get("month") or ""
        try:
            year, month = (int(p) for p in raw.split("-")[:2])
        except (ValueError, TypeError):
            today = date.today()
            year, month = today.year, today.month
        return overview.build_overview(year, month)

    return _guard(_build)


@app.get("/cashflow")
def cashflow_route():
    def _build():
        payload = pierre.get_transactions(
            start_date=request.args.get("startDate"),
            end_date=request.args.get("endDate"),
            account_type=request.args.get("accountType"),
            fmt="structured",
        )
        return cashflow.summarize_cashflow(payload)

    return _guard(_build)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

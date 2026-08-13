"""pierre-service: internal-only Flask app that proxies the Pierre Finance API
with caching. Not exposed to the host; the TYI backend calls it server-to-server
over the private network. No auth here — auth is enforced by the backend.
"""

import json
from datetime import date

from flask import Flask, jsonify, request

import cashflow
import datasource
import manual
import overrides
import overview
import pierre
import pluggy_api
import recurring
import settings

app = Flask(__name__)


@app.before_request
def _pick_finance_source():
    # ?source=pluggy|pierre força a fonte nesta requisição (default: env/pierre)
    datasource.set_request_source(request.args.get("source"))


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


def _guard_pluggy(fn):
    try:
        return _ok(fn())
    except pluggy_api.PluggyNotConfigured:
        return _err("Integracao Pluggy nao configurada (PLUGGY_CLIENT_ID/SECRET).", status=503)
    except pluggy_api.PluggyError as exc:
        return _err(str(exc), status=502)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao consultar Pluggy: {exc}", status=502)


@app.get("/pluggy-status")
def pluggy_status():
    return _guard_pluggy(pluggy_api.status)


@app.get("/inter-status")
def inter_status():
    import inter_api

    try:
        return _ok(inter_api.status())
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao consultar Inter: {exc}", status=502)


@app.get("/pluggy-transactions")
def pluggy_transactions():
    account_id = request.args.get("accountId")
    if not account_id:
        return _err("accountId obrigatorio.", status=400)
    return _guard_pluggy(
        lambda: pluggy_api.get_transactions(
            account_id,
            date_from=request.args.get("from"),
            date_to=request.args.get("to"),
        )
    )


@app.get("/accounts")
def accounts():
    return _guard(datasource.get_accounts)


@app.get("/balance")
def balance():
    return _guard(pierre.get_balance)


@app.get("/transactions")
def transactions():
    return _guard(
        lambda: datasource.get_transactions(
            start_date=request.args.get("startDate"),
            end_date=request.args.get("endDate"),
            account_type=request.args.get("accountType"),
            fmt=request.args.get("format", "structured"),
        )
    )


@app.get("/bills")
def bills():
    return _guard(lambda: datasource.get_bills(request.args.get("accountId")))


@app.get("/installments")
def installments():
    return _guard(
        lambda: datasource.get_installments(
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


@app.get("/ledger")
def ledger_route():
    def _build():
        raw = request.args.get("month") or ""
        try:
            year, month = (int(p) for p in raw.split("-")[:2])
        except (ValueError, TypeError):
            today = date.today()
            year, month = today.year, today.month
        return overview.build_ledger(year, month)

    return _guard(_build)


@app.get("/accounts-activity")
def accounts_activity_route():
    def _build():
        raw = request.args.get("month") or ""
        try:
            year, month = (int(p) for p in raw.split("-")[:2])
        except (ValueError, TypeError):
            today = date.today()
            year, month = today.year, today.month
        return overview.build_accounts_activity(year, month)

    return _guard(_build)


@app.get("/cashflow")
def cashflow_route():
    def _build():
        # inject first, then overrides, so recategorization rules apply to manual
        # transactions too (e.g. a manual ECS transfer marked as Investimentos).
        payload = manual.inject(datasource.get_transactions(
            start_date=request.args.get("startDate"),
            end_date=request.args.get("endDate"),
            account_type=request.args.get("accountType"),
            fmt="structured",
        ), request.args.get("startDate") or "", request.args.get("endDate") or "")
        payload = overrides.apply(payload)
        return cashflow.summarize_cashflow(payload)

    return _guard(_build)


@app.get("/overrides")
def overrides_list():
    return _guard(overrides.list_overrides)


@app.post("/overrides")
def overrides_add():
    body = request.get_json(silent=True) or {}
    try:
        return _ok(overrides.add_override(body.get("pattern"), body.get("category")))
    except ValueError as exc:
        return _err(str(exc), status=400)
    except Exception as exc:  # noqa: BLE001
        return _err(f"Falha ao salvar regra: {exc}", status=500)


@app.delete("/overrides")
def overrides_delete():
    overrides.delete_override(request.args.get("pattern"))
    return _ok({"deleted": request.args.get("pattern")})


# --- manual accounts / transactions -------------------------------------------

def _int_arg(name):
    try:
        return int(request.args.get(name))
    except (TypeError, ValueError):
        return None


@app.get("/manual-accounts")
def manual_accounts_list():
    return _guard(manual.list_accounts)


@app.post("/manual-accounts")
def manual_accounts_add():
    body = request.get_json(silent=True) or {}
    try:
        if body.get("id"):
            return _ok(manual.update_account(body.get("id"), body.get("name"), body.get("logo")))
        return _ok(manual.add_account(body.get("name"), body.get("logo")))
    except ValueError as exc:
        return _err(str(exc), status=400)


@app.delete("/manual-accounts")
def manual_accounts_delete():
    manual.delete_account(_int_arg("id"))
    return _ok({"deleted": _int_arg("id")})


@app.get("/manual-transactions")
def manual_tx_list():
    return _guard(lambda: manual.list_transactions(_int_arg("account_id")))


@app.post("/manual-transactions")
def manual_tx_add():
    body = request.get_json(silent=True) or {}
    try:
        if body.get("id"):
            return _ok(manual.update_transaction(
                body.get("id"), body.get("date"), body.get("description"),
                body.get("category"), body.get("amount"), body.get("flow"),
            ))
        return _ok(manual.add_transaction(
            body.get("account_id"), body.get("date"), body.get("description"),
            body.get("category"), body.get("amount"), body.get("flow"),
        ))
    except ValueError as exc:
        return _err(str(exc), status=400)


@app.delete("/manual-transactions")
def manual_tx_delete():
    manual.delete_transaction(_int_arg("id"))
    return _ok({"deleted": _int_arg("id")})


# --- recurring commitments ----------------------------------------------------

@app.get("/recurring-items")
def recurring_list():
    return _guard(recurring.list_items)


@app.post("/recurring-items")
def recurring_add():
    body = request.get_json(silent=True) or {}
    try:
        if body.get("id"):
            return _ok(recurring.update_item(body.get("id"), **{k: v for k, v in body.items() if k != "id"}))
        return _ok(recurring.add_item(
            body.get("description"), body.get("bucket"), body.get("amount"),
            category=body.get("category", ""), bank=body.get("bank", ""),
            kind=body.get("kind", "fixed"), total_installments=body.get("total_installments", 0),
            start_month=body.get("start_month", ""),
        ))
    except ValueError as exc:
        return _err(str(exc), status=400)


@app.delete("/recurring-items")
def recurring_delete():
    from_month = request.args.get("from_month")
    if from_month:
        return _ok(recurring.deactivate_from(_int_arg("id"), from_month))
    recurring.delete_item(_int_arg("id"))
    return _ok({"deleted": _int_arg("id")})


@app.get("/settings")
def settings_get():
    data = dict(settings.all_settings())
    data["card_closing_day"] = settings.card_closing_day()
    data["card_closing_days"] = settings.card_closing_days()
    data["card_closing_by_last4"] = settings.card_closing_by_last4()
    data["card_closing_overrides"] = settings.card_closing_overrides()
    return _ok(data)


@app.post("/settings")
def settings_set():
    body = request.get_json(silent=True) or {}
    for k, v in body.items():
        # dict/list values (e.g. the per-card closing map) are stored as JSON so
        # settings.card_closing_by_last4() can parse them back.
        if isinstance(v, (dict, list)):
            v = json.dumps(v, ensure_ascii=False)
        settings.set_value(k, v)
    return _ok({"card_closing_day": settings.card_closing_day()})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

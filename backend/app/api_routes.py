import json
import os
import re
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

from flask import Blueprint, current_app, jsonify, request, send_file

from .auth import (
    create_user_account,
    get_current_user,
    list_users,
    login_user,
    logout_current_user,
    require_admin_user,
    set_user_active_state,
)
from .db import create_database_backups, list_database_backups, resolve_database_backup_path
from .observability import build_health_payload, get_route_metrics
from .services import (
    add_fixed_income,
    add_income,
    add_transaction,
    create_portfolio,
    delete_portfolio,
    delete_fixed_incomes,
    delete_incomes,
    delete_transactions,
    get_asset,
    get_asset_enrichment,
    get_asset_enrichment_history,
    get_asset_incomes,
    get_asset_position_summary,
    get_asset_price_history,
    get_asset_transactions,
    get_benchmark_comparison,
    get_fixed_income_summary,
    get_fixed_income_payload_cached,
    get_fixed_incomes,
    get_incomes,
    get_monthly_class_summary,
    get_monthly_ticker_summary,
    get_metric_formulas_catalog,
    get_portfolio_snapshot,
    get_portfolios,
    get_sectors_summary,
    get_top_assets,
    get_transactions,
    import_fixed_incomes_csv,
    import_transactions_csv,
    normalize_portfolio_ids,
    refresh_all_assets_market_data,
    refresh_asset_market_data,
    resolve_portfolio_id,
    enrich_asset_with_openclaw,
    enrich_assets_with_openclaw_batch,
    update_metric_formula,
)


api_bp = Blueprint("api", __name__)
_SCANNER_USER_NOTE_PATTERN = re.compile(r"^\[\[TYI_UID:(\d+)\]\]\s*")


def _selected_portfolio_ids_from_request():
    raw_ids = request.args.getlist("portfolio_id")
    if not raw_ids:
        single = request.args.get("portfolio_id")
        if single:
            raw_ids = [single]
    return normalize_portfolio_ids(raw_ids)


def _json_ok(payload=None, status=200):
    return jsonify({"ok": True, "data": payload}), status


def _json_error(message, status=400, details=None):
    payload = {"ok": False, "error": message}
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status


def _as_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _scanner_note_with_user(notes, user_id: int):
    clean = str(notes or "").strip()
    # Remove qualquer marcador anterior para evitar spoof de ownership.
    clean = _SCANNER_USER_NOTE_PATTERN.sub("", clean).strip()
    if clean:
        return f"[[TYI_UID:{int(user_id)}]] {clean}"
    return f"[[TYI_UID:{int(user_id)}]]"


def _scanner_user_id_from_notes(notes):
    match = _SCANNER_USER_NOTE_PATTERN.match(str(notes or ""))
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _scanner_strip_user_marker(notes):
    return _SCANNER_USER_NOTE_PATTERN.sub("", str(notes or "")).strip()


def _scanner_trade_visible_for_user(trade: dict, user: dict):
    owner_id = _scanner_user_id_from_notes((trade or {}).get("notes"))
    if owner_id is None:
        # Trades antigos sem marcador ficam acessiveis apenas para admin.
        return bool(user.get("is_admin"))
    return int(owner_id) == int(user["id"])


def _scanner_trade_to_client(trade: dict):
    payload = dict(trade or {})
    payload["notes"] = _scanner_strip_user_marker(payload.get("notes"))
    return payload


def _scanner_summary_from_trades(active, history):
    all_trades = list(active or []) + list(history or [])
    status_counts = {
        "open": sum(1 for trade in all_trades if str(trade.get("status")) == "OPEN"),
        "success": sum(1 for trade in all_trades if str(trade.get("status")) == "TARGET_HIT"),
        "failure": sum(1 for trade in all_trades if str(trade.get("status")) == "STOP_HIT"),
        "closed_profit": sum(1 for trade in all_trades if str(trade.get("status")) == "CLOSED_PROFIT"),
        "closed_loss": sum(1 for trade in all_trades if str(trade.get("status")) == "CLOSED_LOSS"),
    }
    open_invested_amount = round(
        sum(float((trade or {}).get("invested_amount") or 0.0) for trade in (active or [])),
        2,
    )
    open_pnl_amount = round(
        sum(float((trade or {}).get("current_pnl_amount") or 0.0) for trade in (active or [])),
        2,
    )
    return {
        "tracked_count": len(active or []),
        "history_count": len(history or []),
        "open_invested_amount": open_invested_amount,
        "open_pnl_amount": open_pnl_amount,
        **status_counts,
    }


def _scanner_filter_trades_payload_for_user(payload, user: dict):
    source = payload if isinstance(payload, dict) else {}
    raw_active = source.get("active") if isinstance(source.get("active"), list) else []
    raw_history = source.get("history") if isinstance(source.get("history"), list) else []
    visible_active = [
        _scanner_trade_to_client(item)
        for item in raw_active
        if isinstance(item, dict) and _scanner_trade_visible_for_user(item, user)
    ]
    visible_history = [
        _scanner_trade_to_client(item)
        for item in raw_history
        if isinstance(item, dict) and _scanner_trade_visible_for_user(item, user)
    ]
    return {
        "active": visible_active,
        "history": visible_history,
        "tracked_tickers": sorted({str(item.get("ticker") or "").upper() for item in visible_active if item.get("ticker")}),
        "summary": _scanner_summary_from_trades(visible_active, visible_history),
    }


def _scanner_prepare_trade_payload(payload, user: dict, force_notes: bool = False):
    prepared = dict(payload or {})
    prepared["user_id"] = int(user["id"])
    if force_notes or "notes" in prepared:
        prepared["notes"] = _scanner_note_with_user(prepared.get("notes"), int(user["id"]))
    return prepared


def _market_scanner_base_url():
    return (os.getenv("MARKET_SCANNER_BASE_URL") or "http://market-scanner:8000").rstrip("/")


def _market_scanner_timeout_seconds():
    raw = (os.getenv("MARKET_SCANNER_TIMEOUT_SECONDS") or "8").strip()
    try:
        return max(float(raw), 1.0)
    except (TypeError, ValueError):
        return 8.0


def _market_scanner_get(path: str):
    return _market_scanner_request("GET", path)


def _market_scanner_request(method: str, path: str, payload=None):
    user = get_current_user()
    query_items = [
        (key, value)
        for key, value in request.args.items(multi=True)
        if key not in {"user_id", "scanner_user_id"}
    ]
    if user and user.get("id") is not None:
        query_items.append(("user_id", str(int(user["id"]))))
    query = urlparse.urlencode(query_items, doseq=True)
    url = f"{_market_scanner_base_url()}{path}"
    if query:
        url = f"{url}?{query}"

    body = None
    headers = {"Accept": "application/json"}
    if user and user.get("id") is not None:
        headers["X-TYI-User-Id"] = str(int(user["id"]))
        headers["X-TYI-Username"] = str(user.get("username") or "")
    if payload is not None:
        if user and isinstance(payload, dict):
            payload = {**payload, "user_id": int(user["id"])}
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urlrequest.Request(url, data=body, headers=headers, method=method.upper())
    try:
        with urlrequest.urlopen(req, timeout=_market_scanner_timeout_seconds()) as response:
            status = int(response.getcode() or 200)
            raw_body = response.read().decode("utf-8", errors="replace")
    except urlerror.HTTPError as exc:
        raw_body = exc.read().decode("utf-8", errors="replace")
        return False, int(exc.code or 502), _market_scanner_parse_body(raw_body)
    except Exception as exc:
        return False, 503, str(exc)

    return True, status, _market_scanner_parse_body(raw_body)


def _market_scanner_parse_body(raw_body: str):
    try:
        return json.loads(raw_body) if raw_body else None
    except json.JSONDecodeError:
        return {"raw": raw_body}


def _market_scanner_proxy_get(path: str):
    return _market_scanner_proxy("GET", path)


def _market_scanner_proxy(method: str, path: str, payload=None):
    ok, status, response_payload = _market_scanner_request(method, path, payload=payload)
    if not ok:
        message = _market_scanner_error_message(response_payload, status)
        return _json_error(
            message,
            status=status,
            details={"upstream": response_payload},
        )
    return _json_ok(response_payload, status=status)


def _market_scanner_error_message(payload, status: int):
    if isinstance(payload, dict):
        detail = payload.get("detail") or payload.get("error")
        if isinstance(detail, str) and detail.strip():
            return detail.strip()
    if isinstance(payload, str) and payload.strip():
        return payload.strip()[:300]
    if status >= 500:
        return "Market Scanner indisponivel."
    return "Falha ao processar requisicao no Market Scanner."


def _build_charts_core_payload(portfolio_ids):
    portfolio = get_portfolio_snapshot(portfolio_ids, sort_by="name", sort_dir="asc")
    fixed_income_payload = get_fixed_income_payload_cached(
        portfolio_ids, sort_by="date_aporte", sort_dir="desc"
    )
    fixed_income = fixed_income_payload["summary"]
    fixed_income_items = fixed_income_payload["items"]
    monthly_class_summary = get_monthly_class_summary(portfolio_ids)

    category_labels = {
        "br_stocks": "Acoes BR",
        "us_stocks": "Acoes US",
        "crypto": "Cripto",
        "fiis": "FIIs",
    }
    category_chart = {"labels": [], "values": []}
    for key in ("br_stocks", "us_stocks", "crypto", "fiis"):
        value = float(portfolio["group_totals"].get(key, 0.0))
        if value <= 0:
            continue
        category_chart["labels"].append(category_labels[key])
        category_chart["values"].append(round(value, 2))

    top_positions = sorted(
        portfolio["positions"], key=lambda item: item.get("value", 0.0), reverse=True
    )[:10]
    top_assets_chart = {
        "labels": [item["ticker"] for item in top_positions],
        "values": [round(float(item.get("value", 0.0)), 2) for item in top_positions],
    }

    allocation_by_group_charts = []
    for key in ("br_stocks", "us_stocks", "fiis", "crypto"):
        items = portfolio["grouped_positions"].get(key, [])
        if not items:
            continue
        group_total = sum(float(item.get("value", 0.0)) for item in items)
        if group_total <= 0:
            continue
        allocation_by_group_charts.append(
            {
                "id": f"chart-allocation-{key}",
                "title": f"Composicao por ativo - {category_labels[key]}",
                "labels": [item["ticker"] for item in items],
                "values": [round(float(item.get("value", 0.0)), 2) for item in items],
                "weights": [
                    round((float(item.get("value", 0.0)) / group_total) * 100, 2)
                    for item in items
                ],
            }
        )

    cards_chart = {
        "labels": ["Patrimonio", "Investido em aberto", "Proventos totais"],
        "values": [
            round(float(portfolio["total_value"]), 2),
            round(float(portfolio["invested_value"]), 2),
            round(float(portfolio["total_incomes"]), 2),
        ],
    }

    cri_keywords = ("CRI", "CRA", "DEB")
    cri_result = 0.0
    for item in fixed_income_items:
        investment_name = (item.get("investment_type") or "").upper()
        if any(key in investment_name for key in cri_keywords):
            cri_result += float(item.get("current_income", 0.0))

    result_by_category_chart = {
        "labels": ["US", "FIIs", "BR", "Cripto", "CRI/CRA/DEB"],
        "values": [
            round(float(portfolio["group_summaries"]["us_stocks"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["fiis"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["br_stocks"]["open_pnl_value"]), 2),
            round(float(portfolio["group_summaries"]["crypto"]["open_pnl_value"]), 2),
            round(float(cri_result), 2),
        ],
    }

    fixed_income_by_investment = {}
    fixed_income_by_distributor = {}
    fixed_income_by_issuer = {}
    for item in fixed_income_items:
        value = float(item.get("active_applied_value", 0.0))
        income_value = float(item.get("current_income", 0.0))
        investment_type = (item.get("investment_type") or "Nao informado").strip()
        distributor = (item.get("distributor") or "Nao informado").strip()
        issuer = (item.get("issuer") or "Nao informado").strip()
        if value > 0:
            fixed_income_by_investment[investment_type] = (
                fixed_income_by_investment.get(investment_type, 0.0) + value
            )
            fixed_income_by_distributor[distributor] = (
                fixed_income_by_distributor.get(distributor, 0.0) + value
            )
        if value > 0 or income_value != 0:
            state = fixed_income_by_issuer.get(issuer, {"investment": 0.0, "income": 0.0})
            state["investment"] += max(value, 0.0)
            state["income"] += income_value
            fixed_income_by_issuer[issuer] = state

    investment_pairs = sorted(
        fixed_income_by_investment.items(), key=lambda pair: pair[1], reverse=True
    )
    distributor_pairs = sorted(
        fixed_income_by_distributor.items(), key=lambda pair: pair[1], reverse=True
    )
    issuer_pairs = sorted(
        fixed_income_by_issuer.items(), key=lambda pair: pair[1]["investment"], reverse=True
    )

    monthly_income_rows = list(monthly_class_summary)
    last_income_idx = -1
    for idx, row in enumerate(monthly_income_rows):
        fii_income = float(row.get("fii_incomes", 0.0))
        acoes_income = float(row.get("br_incomes", 0.0))
        if (fii_income + acoes_income) != 0:
            last_income_idx = idx
    if last_income_idx >= 0:
        monthly_income_rows = monthly_income_rows[: last_income_idx + 1]
        monthly_income_rows = [
            row
            for row in monthly_income_rows
            if (float(row.get("fii_incomes", 0.0)) + float(row.get("br_incomes", 0.0))) != 0
        ]

    return {
        "category_chart": category_chart,
        "top_assets_chart": top_assets_chart,
        "allocation_by_group_charts": allocation_by_group_charts,
        "cards_chart": cards_chart,
        "result_by_category_chart": result_by_category_chart,
        "classes_chart": {
            "labels": ["Renda Variavel", "Renda Fixa"],
            "values": [
                round(float(portfolio["total_value"]), 2),
                round(float(fixed_income["current_total"]), 2),
            ],
        },
        "fixed_income_investment_chart": {
            "labels": [label for label, _ in investment_pairs],
            "values": [round(value, 2) for _, value in investment_pairs],
        },
        "fixed_income_distributor_chart": {
            "labels": [label for label, _ in distributor_pairs],
            "values": [round(value, 2) for _, value in distributor_pairs],
        },
        "fixed_income_issuer_chart": {
            "labels": [label for label, _ in issuer_pairs],
            "investment_values": [
                round(float(payload["investment"]), 2) for _, payload in issuer_pairs
            ],
            "income_values": [
                round(float(payload["income"]), 2) for _, payload in issuer_pairs
            ],
        },
        "monthly_class_summary": monthly_class_summary,
        "monthly_income_chart": {
            "labels": [row.get("label", "") for row in monthly_income_rows],
            "fii_values": [
                round(float(row.get("fii_incomes", 0.0)), 2) for row in monthly_income_rows
            ],
            "acoes_values": [
                round(float(row.get("br_incomes", 0.0)), 2) for row in monthly_income_rows
            ],
        },
        "snapshot": {
            "fixed_income": bool(fixed_income_payload.get("snapshot")),
        },
    }


@api_bp.route("/health", methods=["GET"])
def health():
    payload = build_health_payload()
    status_code = 200 if payload["status"] == "ok" else 503
    return _json_ok(payload, status=status_code)


@api_bp.route("/scanner/health", methods=["GET"])
def scanner_health():
    return _market_scanner_proxy_get("/signal-matrix")


@api_bp.route("/scanner/signals", methods=["GET"])
def scanner_signals():
    return _market_scanner_proxy_get("/signals")


@api_bp.route("/scanner/signal-matrix", methods=["GET"])
def scanner_signal_matrix():
    return _market_scanner_proxy_get("/signal-matrix")


@api_bp.route("/scanner/trades", methods=["GET"])
def scanner_trades():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    ok, status, response_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(response_payload, status)
        return _json_error(message, status=status, details={"upstream": response_payload})
    return _json_ok(_scanner_filter_trades_payload_for_user(response_payload, user), status=status)


@api_bp.route("/scanner/trades", methods=["POST"])
def scanner_create_trade():
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)
    payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(payload, user, force_notes=True)
    return _market_scanner_proxy("POST", "/trades", payload=payload)


@api_bp.route("/scanner/trades/<int:trade_id>", methods=["PATCH"])
def scanner_update_trade(trade_id: int):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, status, trades_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(trades_payload, status)
        return _json_error(message, status=status, details={"upstream": trades_payload})

    all_trades = []
    if isinstance(trades_payload, dict):
        all_trades.extend(item for item in (trades_payload.get("active") or []) if isinstance(item, dict))
        all_trades.extend(item for item in (trades_payload.get("history") or []) if isinstance(item, dict))
    target = next((item for item in all_trades if int(item.get("id") or 0) == int(trade_id)), None)
    if target is None:
        return _json_error("Trade nao encontrado.", status=404)
    if not _scanner_trade_visible_for_user(target, user):
        return _json_error("Trade nao pertence ao usuario atual.", status=403)

    payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(payload, user, force_notes=("notes" in payload))
    return _market_scanner_proxy("PATCH", f"/trades/{trade_id}", payload=payload)


@api_bp.route("/scanner/trades/<int:trade_id>/close", methods=["POST"])
def scanner_close_trade(trade_id: int):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, status, trades_payload = _market_scanner_request("GET", "/trades")
    if not ok:
        message = _market_scanner_error_message(trades_payload, status)
        return _json_error(message, status=status, details={"upstream": trades_payload})

    all_trades = []
    if isinstance(trades_payload, dict):
        all_trades.extend(item for item in (trades_payload.get("active") or []) if isinstance(item, dict))
        all_trades.extend(item for item in (trades_payload.get("history") or []) if isinstance(item, dict))
    target = next((item for item in all_trades if int(item.get("id") or 0) == int(trade_id)), None)
    if target is None:
        return _json_error("Trade nao encontrado.", status=404)
    if not _scanner_trade_visible_for_user(target, user):
        return _json_error("Trade nao pertence ao usuario atual.", status=403)

    payload = request.get_json(silent=True) or request.form.to_dict()
    payload = _scanner_prepare_trade_payload(payload, user, force_notes=False)
    return _market_scanner_proxy("POST", f"/trades/{trade_id}/close", payload=payload)


@api_bp.route("/scanner/ticker/<symbol>", methods=["GET"])
def scanner_ticker(symbol: str):
    safe_symbol = urlparse.quote(symbol.upper(), safe="")
    return _market_scanner_proxy_get(f"/ticker/{safe_symbol}")


@api_bp.route("/scanner/metrics/catalog", methods=["GET"])
def scanner_metrics_catalog():
    return _market_scanner_proxy_get("/metrics/catalog")


@api_bp.route("/scanner/metrics/catalog/<metric_key>", methods=["PATCH"])
def scanner_metrics_update(metric_key: str):
    safe_key = urlparse.quote(str(metric_key or "").strip(), safe="")
    payload = request.get_json(silent=True) or request.form.to_dict()
    return _market_scanner_proxy("PATCH", f"/metrics/catalog/{safe_key}", payload=payload)


@api_bp.route("/auth/me", methods=["GET"])
def auth_me():
    user = get_current_user()
    return _json_ok({"authenticated": bool(user), "user": user})


@api_bp.route("/auth/login", methods=["POST"])
def auth_login():
    payload = request.get_json(silent=True) or request.form.to_dict()
    user = login_user(payload.get("username", ""), payload.get("password", ""))
    if not user:
        return _json_error("Usuario ou senha invalidos.", status=401)
    return _json_ok({"authenticated": True, "user": user})


@api_bp.route("/auth/logout", methods=["POST"])
def auth_logout():
    logout_current_user()
    return _json_ok({"authenticated": False})


@api_bp.route("/admin/users", methods=["GET", "POST"])
def admin_users():
    admin = require_admin_user()
    if request.method == "GET":
        return _json_ok({"users": list_users(), "current_user_id": admin["id"]})

    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, user = create_user_account(
        payload.get("username", ""),
        payload.get("password", ""),
        _as_bool(payload.get("is_admin")),
    )
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "user": user}, status=201)


@api_bp.route("/admin/users/<int:user_id>/status", methods=["POST"])
def admin_user_status(user_id: int):
    admin = require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, user = set_user_active_state(
        user_id,
        _as_bool(payload.get("is_active")),
        acting_user_id=admin["id"],
    )
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "user": user})


@api_bp.route("/admin/openclaw/enrich-assets", methods=["POST"])
def admin_openclaw_enrich_assets():
    require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    tickers = payload.get("tickers") or []
    if isinstance(tickers, str):
        tickers = [item.strip() for item in tickers.split(",")]
    elif not isinstance(tickers, list):
        tickers = []

    raw_limit = payload.get("limit")
    limit = None
    try:
        parsed_limit = int(raw_limit)
        if parsed_limit > 0:
            limit = parsed_limit
    except (TypeError, ValueError):
        limit = None

    result = enrich_assets_with_openclaw_batch(
        tickers=tickers,
        only_missing=_as_bool(payload.get("only_missing", True)),
        limit=limit,
    )
    return _json_ok(result)


@api_bp.route("/admin/metric-formulas", methods=["GET"])
def admin_metric_formulas():
    require_admin_user()
    return _json_ok(get_metric_formulas_catalog())


@api_bp.route("/admin/metric-formulas/<metric_key>", methods=["POST"])
def admin_metric_formula_save(metric_key: str):
    require_admin_user()
    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message, result = update_metric_formula(metric_key, payload.get("formula"))
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message, "result": result, "catalog": get_metric_formulas_catalog()})


@api_bp.route("/metrics", methods=["GET"])
def metrics():
    return _json_ok({"routes": get_route_metrics(current_app)})


@api_bp.route("/backup/database", methods=["GET", "POST"])
def backup_database_endpoint():
    require_admin_user()
    if request.method == "GET":
        return _json_ok({"backups": list_database_backups()})

    try:
        result = create_database_backups(reason="api")
    except Exception as exc:
        return _json_error(f"Nao foi possivel gerar backup: {exc}", status=500)
    backups = result.get("backups") or []
    primary = next((item for item in backups if item.get("database_key") == "backend"), None)
    return _json_ok({"backup": primary, **result}, status=201)


@api_bp.route("/backup/database/<path:filename>", methods=["GET"])
def backup_database_download(filename: str):
    require_admin_user()
    path = resolve_database_backup_path(filename)
    if not path:
        return _json_error("Backup nao encontrado.", status=404)
    return send_file(
        path,
        as_attachment=True,
        download_name=path.name,
        mimetype="application/x-sqlite3",
    )


@api_bp.route("/portfolios", methods=["GET", "POST", "DELETE"])
def portfolios():
    if request.method == "GET":
        return _json_ok(get_portfolios())

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, result = create_portfolio(payload.get("name"))
        if not ok:
            return _json_error(result, status=400)
        return _json_ok({"portfolio_id": result}, status=201)

    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, result = delete_portfolio(payload.get("portfolio_id"))
    if not ok:
        return _json_error(result, status=400)
    return _json_ok({"removed_name": result})


@api_bp.route("/assets", methods=["GET"])
def assets():
    return _json_ok(get_top_assets())


@api_bp.route("/assets/<ticker>", methods=["GET"])
def asset_detail(ticker):
    portfolio_ids = _selected_portfolio_ids_from_request()
    asset = get_asset(ticker)
    if not asset:
        return _json_error("Ativo nao encontrado.", status=404)
    payload = {
        "asset": asset,
        "enrichment": get_asset_enrichment(ticker),
        "enrichment_history": get_asset_enrichment_history(ticker),
        "position": get_asset_position_summary(ticker, portfolio_ids),
        "transactions": get_asset_transactions(ticker, portfolio_ids),
        "incomes": get_asset_incomes(ticker, portfolio_ids),
    }
    return _json_ok(payload)


@api_bp.route("/assets/<ticker>/price-history", methods=["GET"])
def asset_price_history(ticker: str):
    chart_range = (request.args.get("range") or "1y").lower()
    asset = get_asset(ticker)
    if not asset:
        return _json_error("Ativo nao encontrado.", status=404)
    return _json_ok(get_asset_price_history(ticker, chart_range))


@api_bp.route("/assets/<ticker>/enrich/openclaw", methods=["POST"])
def asset_enrich_openclaw(ticker: str):
    user = get_current_user()
    if not user:
        return _json_error("Nao autenticado.", status=401)

    ok, message, enrichment = enrich_asset_with_openclaw(ticker)
    if not ok:
        return _json_error(message, status=502)
    return _json_ok(
        {
            "message": message,
            "enrichment": enrichment,
            "enrichment_history": get_asset_enrichment_history(ticker),
        }
    )


@api_bp.route("/portfolio/snapshot", methods=["GET"])
def portfolio_snapshot():
    portfolio_ids = _selected_portfolio_ids_from_request()
    sort_by = request.args.get("sort_by", "name")
    sort_dir = request.args.get("sort_dir", "asc")
    payload = get_portfolio_snapshot(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)
    return _json_ok(payload)


@api_bp.route("/sectors", methods=["GET"])
def sectors():
    return _json_ok(get_sectors_summary())


@api_bp.route("/transactions", methods=["GET", "POST", "DELETE"])
def transactions():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        return _json_ok(get_transactions(portfolio_ids))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_transaction(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    transaction_ids = ids.get("transaction_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_transactions(transaction_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhuma transacao removida.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/incomes", methods=["GET", "POST", "DELETE"])
def incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        return _json_ok(get_incomes(portfolio_ids))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_income(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    income_ids = ids.get("income_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_incomes(income_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhum provento removido.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/fixed-incomes", methods=["GET", "POST", "DELETE"])
def fixed_incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        sort_by = request.args.get("sort_by", "date_aporte")
        sort_dir = request.args.get("sort_dir", "desc")
        return _json_ok(get_fixed_income_payload_cached(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir))

    if request.method == "POST":
        payload = request.get_json(silent=True) or request.form.to_dict()
        ok, message = add_fixed_income(payload)
        if not ok:
            return _json_error(message, status=400)
        return _json_ok({"message": message}, status=201)

    ids = request.get_json(silent=True) or {}
    fixed_income_ids = ids.get("fixed_income_ids") or []
    portfolio_ids = _selected_portfolio_ids_from_request()
    removed = delete_fixed_incomes(fixed_income_ids, portfolio_ids)
    if removed <= 0:
        return _json_error("Nenhum registro removido.", status=400)
    return _json_ok({"removed": removed})


@api_bp.route("/imports/transactions-csv", methods=["POST"])
def import_transactions_csv_endpoint():
    target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
    file = request.files.get("csv_file")
    file_bytes = file.read() if file else b""
    ok, message, imported, errors = import_transactions_csv(file_bytes, target_portfolio_id)
    if not ok:
        return _json_error(message, status=400, details={"imported": imported, "errors": errors})
    return _json_ok(
        {
            "message": message,
            "imported": imported,
            "errors": errors,
            "target_portfolio_id": target_portfolio_id,
        }
    )


@api_bp.route("/imports/fixed-incomes-csv", methods=["POST"])
def import_fixed_incomes_csv_endpoint():
    target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
    file = request.files.get("fixed_income_csv_file")
    file_bytes = file.read() if file else b""
    ok, message, imported, errors = import_fixed_incomes_csv(file_bytes, target_portfolio_id)
    if not ok:
        return _json_error(message, status=400, details={"imported": imported, "errors": errors})
    return _json_ok(
        {
            "message": message,
            "imported": imported,
            "errors": errors,
            "target_portfolio_id": target_portfolio_id,
        }
    )


@api_bp.route("/charts/monthly-class-summary", methods=["GET"])
def charts_monthly_class_summary():
    portfolio_ids = _selected_portfolio_ids_from_request()
    return _json_ok(get_monthly_class_summary(portfolio_ids))


@api_bp.route("/charts/benchmark", methods=["GET"])
def charts_benchmark():
    portfolio_ids = _selected_portfolio_ids_from_request()
    range_key = (request.args.get("range") or "12m").strip().lower()
    scope_key = (request.args.get("scope") or "all").strip().lower()
    return _json_ok(get_benchmark_comparison(portfolio_ids, range_key=range_key, scope_key=scope_key))


@api_bp.route("/charts/core", methods=["GET"])
def charts_core():
    portfolio_ids = _selected_portfolio_ids_from_request()
    return _json_ok(_build_charts_core_payload(portfolio_ids))


@api_bp.route("/charts/ticker-summary", methods=["GET"])
def charts_ticker_summary():
    portfolio_ids = _selected_portfolio_ids_from_request()
    months = request.args.get("months", 8)
    return _json_ok(get_monthly_ticker_summary(portfolio_ids, months=months))


@api_bp.route("/charts/dashboard", methods=["GET"])
def charts_dashboard():
    portfolio_ids = _selected_portfolio_ids_from_request()
    benchmark_range = (request.args.get("range") or "12m").strip().lower()
    benchmark_scope = (request.args.get("scope") or "all").strip().lower()
    payload = _build_charts_core_payload(portfolio_ids)
    payload["monthly_ticker_summary"] = get_monthly_ticker_summary(portfolio_ids, months=8)
    payload["benchmark_chart"] = get_benchmark_comparison(
        portfolio_ids,
        range_key=benchmark_range,
        scope_key=benchmark_scope,
    )
    payload["benchmark_range"] = payload["benchmark_chart"].get("range_key", "12m")
    payload["benchmark_scope"] = payload["benchmark_chart"].get("scope_key", "all")
    return _json_ok(payload)


@api_bp.route("/sync/market-data", methods=["POST"])
def sync_market_data_all():
    try:
        failed = refresh_all_assets_market_data()
    except Exception:
        failed = ["erro"]
    return _json_ok({"failed": failed, "failed_count": len(failed), "success": len(failed) == 0})


@api_bp.route("/sync/market-data/<ticker>", methods=["POST"])
def sync_market_data_ticker(ticker):
    ok = False
    try:
        ok = refresh_asset_market_data(ticker)
    except Exception:
        ok = False
    if not ok:
        return _json_error("Nao foi possivel atualizar este ticker agora.", status=503)
    return _json_ok({"ticker": ticker.upper(), "success": True})

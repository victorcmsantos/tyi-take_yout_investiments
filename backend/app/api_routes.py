from flask import Blueprint, jsonify, request

from .services import (
    add_fixed_income,
    add_income,
    add_transaction,
    delete_fixed_incomes,
    delete_transactions,
    get_asset,
    get_asset_incomes,
    get_asset_position_summary,
    get_asset_price_history,
    get_asset_transactions,
    get_benchmark_comparison,
    get_fixed_income_summary,
    get_fixed_incomes,
    get_incomes,
    get_monthly_class_summary,
    get_portfolio_snapshot,
    get_portfolios,
    get_sectors_summary,
    get_top_assets,
    get_transactions,
    normalize_portfolio_ids,
    refresh_all_assets_market_data,
    refresh_asset_market_data,
)


api_bp = Blueprint("api", __name__)


def _selected_portfolio_ids_from_request():
    raw_ids = request.args.getlist("portfolio_id")
    if not raw_ids:
        single = request.args.get("portfolio_id")
        if single:
            raw_ids = [single]
    return normalize_portfolio_ids(raw_ids)


def _json_ok(payload=None, status=200):
    return jsonify({"ok": True, "data": payload}), status


def _json_error(message, status=400):
    return jsonify({"ok": False, "error": message}), status


@api_bp.route("/health", methods=["GET"])
def health():
    return _json_ok({"status": "ok"})


@api_bp.route("/portfolios", methods=["GET"])
def portfolios():
    return _json_ok(get_portfolios())


@api_bp.route("/assets", methods=["GET"])
def assets():
    return _json_ok(get_top_assets())


@api_bp.route("/assets/<ticker>", methods=["GET"])
def asset_detail(ticker):
    portfolio_ids = _selected_portfolio_ids_from_request()
    chart_range = (request.args.get("range") or "1y").lower()
    asset = get_asset(ticker)
    if not asset:
        return _json_error("Ativo nao encontrado.", status=404)
    payload = {
        "asset": asset,
        "position": get_asset_position_summary(ticker, portfolio_ids),
        "transactions": get_asset_transactions(ticker, portfolio_ids),
        "incomes": get_asset_incomes(ticker, portfolio_ids),
        "price_history": get_asset_price_history(ticker, chart_range),
    }
    return _json_ok(payload)


@api_bp.route("/portfolio/snapshot", methods=["GET"])
def portfolio_snapshot():
    portfolio_ids = _selected_portfolio_ids_from_request()
    sort_by = request.args.get("sort_by", "value")
    sort_dir = request.args.get("sort_dir", "desc")
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


@api_bp.route("/incomes", methods=["GET", "POST"])
def incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        return _json_ok(get_incomes(portfolio_ids))

    payload = request.get_json(silent=True) or request.form.to_dict()
    ok, message = add_income(payload)
    if not ok:
        return _json_error(message, status=400)
    return _json_ok({"message": message}, status=201)


@api_bp.route("/fixed-incomes", methods=["GET", "POST", "DELETE"])
def fixed_incomes():
    if request.method == "GET":
        portfolio_ids = _selected_portfolio_ids_from_request()
        sort_by = request.args.get("sort_by", "date_aporte")
        sort_dir = request.args.get("sort_dir", "desc")
        payload = {
            "items": get_fixed_incomes(portfolio_ids, sort_by=sort_by, sort_dir=sort_dir),
            "summary": get_fixed_income_summary(portfolio_ids),
        }
        return _json_ok(payload)

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


@api_bp.route("/sync/yahoo", methods=["POST"])
def sync_yahoo_all():
    try:
        failed = refresh_all_assets_market_data()
    except Exception:
        failed = ["erro"]
    return _json_ok({"failed": failed, "failed_count": len(failed), "success": len(failed) == 0})


@api_bp.route("/sync/yahoo/<ticker>", methods=["POST"])
def sync_yahoo_ticker(ticker):
    ok = False
    try:
        ok = refresh_asset_market_data(ticker)
    except Exception:
        ok = False
    if not ok:
        return _json_error("Nao foi possivel atualizar este ticker agora.", status=503)
    return _json_ok({"ticker": ticker.upper(), "success": True})

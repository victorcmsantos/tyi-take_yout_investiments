from flask import Blueprint, jsonify, request

from .services import (
    add_fixed_income,
    add_income,
    add_transaction,
    create_portfolio,
    delete_portfolio,
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
    import_fixed_incomes_csv,
    import_transactions_csv,
    normalize_portfolio_ids,
    refresh_all_assets_market_data,
    refresh_asset_market_data,
    resolve_portfolio_id,
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


def _json_error(message, status=400, details=None):
    payload = {"ok": False, "error": message}
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status


@api_bp.route("/health", methods=["GET"])
def health():
    return _json_ok({"status": "ok"})


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


@api_bp.route("/charts/dashboard", methods=["GET"])
def charts_dashboard():
    portfolio_ids = _selected_portfolio_ids_from_request()
    benchmark_range = (request.args.get("range") or "12m").strip().lower()
    benchmark_scope = (request.args.get("scope") or "all").strip().lower()
    raw_annual_metrics = [item.strip().lower() for item in request.args.getlist("annual_metric") if item]
    valid_annual_metrics = {"invested", "incomes"}
    annual_selected_metrics = [item for item in raw_annual_metrics if item in valid_annual_metrics] or ["invested", "incomes"]
    raw_annual_categories = [item.strip().lower() for item in request.args.getlist("annual_category") if item]
    valid_annual_categories = {"br", "us", "fii", "cripto", "fixa"}
    annual_selected_categories = [item for item in raw_annual_categories if item in valid_annual_categories] or [
        "br",
        "us",
        "fii",
        "cripto",
        "fixa",
    ]

    portfolio = get_portfolio_snapshot(portfolio_ids, sort_by="value", sort_dir="desc")
    fixed_income = get_fixed_income_summary(portfolio_ids)
    fixed_income_items = get_fixed_incomes(portfolio_ids, sort_by="date_aporte", sort_dir="desc")
    monthly_class_summary = get_monthly_class_summary(portfolio_ids)
    benchmark_chart = get_benchmark_comparison(
        portfolio_ids,
        range_key=benchmark_range,
        scope_key=benchmark_scope,
    )

    month_order = [
        ("Jan", "jan"),
        ("Fev", "fev"),
        ("Mar", "mar"),
        ("Abr", "abr"),
        ("Mai", "mai"),
        ("Jun", "jun"),
        ("Jul", "jul"),
        ("Ago", "ago"),
        ("Set", "set"),
        ("Out", "out"),
        ("Nov", "nov"),
        ("Dez", "dez"),
    ]

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
            fixed_income_by_investment[investment_type] = fixed_income_by_investment.get(investment_type, 0.0) + value
            fixed_income_by_distributor[distributor] = fixed_income_by_distributor.get(distributor, 0.0) + value
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
    fixed_income_investment_chart = {
        "labels": [label for label, _ in investment_pairs],
        "values": [round(value, 2) for _, value in investment_pairs],
    }
    fixed_income_distributor_chart = {
        "labels": [label for label, _ in distributor_pairs],
        "values": [round(value, 2) for _, value in distributor_pairs],
    }
    issuer_pairs = sorted(
        fixed_income_by_issuer.items(), key=lambda pair: pair[1]["investment"], reverse=True
    )
    fixed_income_issuer_chart = {
        "labels": [label for label, _ in issuer_pairs],
        "investment_values": [round(float(payload["investment"]), 2) for _, payload in issuer_pairs],
        "income_values": [round(float(payload["income"]), 2) for _, payload in issuer_pairs],
    }

    classes_chart = {
        "labels": ["Renda Variavel", "Renda Fixa"],
        "values": [
            round(float(portfolio["total_value"]), 2),
            round(float(fixed_income["current_total"]), 2),
        ],
    }

    annual_invested_map = {}
    for row in monthly_class_summary:
        raw_label = (row.get("label") or "").strip().lower()
        if "/" not in raw_label:
            continue
        month_key, year_short = raw_label.split("/", 1)
        try:
            year = 2000 + int(year_short)
        except ValueError:
            continue
        if year not in annual_invested_map:
            annual_invested_map[year] = {
                key: {"invested": 0.0, "incomes": 0.0} for _, key in month_order
            }
        if month_key in annual_invested_map[year]:
            invested_value = 0.0
            incomes_value = 0.0
            if "br" in annual_selected_categories:
                invested_value += float(row.get("br_invested", 0.0))
                incomes_value += float(row.get("br_incomes", 0.0))
            if "us" in annual_selected_categories:
                invested_value += float(row.get("us_invested", 0.0))
                incomes_value += float(row.get("us_incomes", 0.0))
            if "fii" in annual_selected_categories:
                invested_value += float(row.get("fii_invested", 0.0))
                incomes_value += float(row.get("fii_incomes", 0.0))
            if "cripto" in annual_selected_categories:
                invested_value += float(row.get("cripto_invested", 0.0))
                incomes_value += float(row.get("cripto_incomes", 0.0))
            if "fixa" in annual_selected_categories:
                invested_value += float(row.get("fixa_invested", 0.0))
                incomes_value += float(row.get("fixa_incomes", 0.0))
            annual_invested_map[year][month_key]["invested"] = invested_value
            annual_invested_map[year][month_key]["incomes"] = incomes_value

    annual_invested_summary = {"months": [label for label, _ in month_order], "years": []}
    for year in sorted(annual_invested_map.keys()):
        month_invested_values = [annual_invested_map[year][key]["invested"] for _, key in month_order]
        month_income_values = [annual_invested_map[year][key]["incomes"] for _, key in month_order]
        annual_invested_summary["years"].append(
            {
                "label": str(year),
                "invested_total": round(sum(month_invested_values), 2),
                "incomes_total": round(sum(month_income_values), 2),
                "invested_values": [round(value, 2) for value in month_invested_values],
                "incomes_values": [round(value, 2) for value in month_income_values],
            }
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
    monthly_income_chart = {
        "labels": [row.get("label", "") for row in monthly_income_rows],
        "fii_values": [round(float(row.get("fii_incomes", 0.0)), 2) for row in monthly_income_rows],
        "acoes_values": [round(float(row.get("br_incomes", 0.0)), 2) for row in monthly_income_rows],
    }

    payload = {
        "category_chart": category_chart,
        "top_assets_chart": top_assets_chart,
        "allocation_by_group_charts": allocation_by_group_charts,
        "cards_chart": cards_chart,
        "result_by_category_chart": result_by_category_chart,
        "classes_chart": classes_chart,
        "fixed_income_investment_chart": fixed_income_investment_chart,
        "fixed_income_distributor_chart": fixed_income_distributor_chart,
        "fixed_income_issuer_chart": fixed_income_issuer_chart,
        "monthly_class_summary": monthly_class_summary,
        "annual_invested_summary": annual_invested_summary,
        "annual_selected_metrics": annual_selected_metrics,
        "annual_selected_categories": annual_selected_categories,
        "benchmark_chart": benchmark_chart,
        "benchmark_range": benchmark_chart.get("range_key", "12m"),
        "benchmark_scope": benchmark_chart.get("scope_key", "all"),
        "monthly_income_chart": monthly_income_chart,
    }
    return _json_ok(payload)


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

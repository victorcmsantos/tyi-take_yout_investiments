from flask import Blueprint, abort, redirect, render_template, request, url_for

from .services import (
    add_income,
    add_fixed_income,
    create_portfolio,
    add_transaction,
    delete_portfolio,
    delete_fixed_incomes,
    delete_transactions,
    import_fixed_incomes_csv,
    import_transactions_csv,
    get_asset,
    get_asset_incomes,
    get_asset_price_history,
    get_asset_position_summary,
    get_asset_transactions,
    get_income_totals_by_ticker,
    get_monthly_class_summary,
    get_fixed_income_summary,
    get_fixed_incomes,
    get_incomes,
    get_portfolio,
    get_portfolios,
    get_portfolio_snapshot,
    normalize_portfolio_ids,
    resolve_portfolio_id,
    refresh_all_assets_market_data,
    refresh_asset_market_data,
    get_sectors_summary,
    get_top_assets,
    get_transactions,
)

main_bp = Blueprint("main", __name__)


def _selected_portfolio_ids():
    raw_values = request.form.getlist("portfolio_id") if request.method == "POST" else request.args.getlist("portfolio_id")
    if not raw_values:
        single = request.form.get("portfolio_id") if request.method == "POST" else request.args.get("portfolio_id")
        raw_values = [single]
    return normalize_portfolio_ids(raw_values)


def _active_portfolio_id(selected_portfolio_ids):
    return selected_portfolio_ids[0] if selected_portfolio_ids else resolve_portfolio_id(None)


def _base_context(selected_portfolio_ids):
    active_portfolio_id = _active_portfolio_id(selected_portfolio_ids)
    return {
        "portfolios": get_portfolios(),
        "selected_portfolio_ids": selected_portfolio_ids,
        "active_portfolio_id": active_portfolio_id,
        "current_portfolio": get_portfolio(active_portfolio_id),
    }


@main_bp.route("/")
def home():
    selected_portfolio_ids = _selected_portfolio_ids()
    sync = request.args.get("sync")
    failed = request.args.get("failed", "0")
    sync_message = None
    sync_status = None
    if sync == "ok":
        sync_message = "Consulta ao Yahoo concluida com sucesso (cotacoes/indicadores recebidos)."
        sync_status = "ok"
    elif sync == "partial":
        try:
            failed_count = max(0, int(failed))
        except ValueError:
            failed_count = 0
        sync_message = (
            f"Atualizacao concluida com pendencias: {failed_count} ticker(s) falharam no Yahoo."
            if failed_count > 0
            else "Atualizacao concluida com pendencias no Yahoo."
        )
        sync_status = "partial"

    assets = get_top_assets()
    incomes_by_ticker, incomes_total = get_income_totals_by_ticker(selected_portfolio_ids)
    assets_with_income = []
    for asset in assets:
        item = dict(asset)
        item["total_incomes"] = round(incomes_by_ticker.get(asset["ticker"], 0.0), 2)
        assets_with_income.append(item)

    if not assets:
        return render_template(
            "index.html",
            assets=[],
            highlights=None,
            incomes_total=0.0,
            sync_message=sync_message,
            sync_status=sync_status,
            **_base_context(selected_portfolio_ids),
        )
    highlights = {
        "highest_dy": max(assets, key=lambda item: item["dy"]),
        "highest_gain": max(assets, key=lambda item: item["variation_day"]),
        "largest_cap": max(assets, key=lambda item: item["market_cap_bi"]),
    }
    return render_template(
        "index.html",
        assets=assets_with_income,
        highlights=highlights,
        incomes_total=incomes_total,
        sync_message=sync_message,
        sync_status=sync_status,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/atualizar-yahoo", methods=["POST"])
def atualizar_yahoo():
    selected_portfolio_ids = _selected_portfolio_ids()
    try:
        failed = refresh_all_assets_market_data()
    except Exception:
        failed = ["erro"]

    if failed:
        return redirect(
            url_for(
                "main.home",
                sync="partial",
                failed=len(failed),
                portfolio_id=selected_portfolio_ids,
            )
        )
    return redirect(
        url_for(
            "main.home",
            sync="ok",
            portfolio_id=selected_portfolio_ids,
        )
    )


@main_bp.route("/ativo/<ticker>")
def ativo(ticker):
    selected_portfolio_ids = _selected_portfolio_ids()
    chart_range = (request.args.get("range") or "1y").lower()
    sync = request.args.get("sync")
    sync_message = None
    sync_status = None
    if sync == "ok":
        sync_message = "Consulta ao Yahoo concluida com sucesso (cotacoes/indicadores recebidos)."
        sync_status = "ok"
    elif sync == "partial":
        sync_message = "Cotacao/indicadores atualizados, mas nome/setor nao vieram do Yahoo."
        sync_status = "partial"
    elif sync == "fail":
        sync_message = "Nao foi possivel atualizar agora. Tente novamente."
        sync_status = "fail"

    refresh_asset_market_data(ticker)
    asset = get_asset(ticker)
    if not asset:
        abort(404)
    transactions = get_asset_transactions(ticker, selected_portfolio_ids)
    incomes = get_asset_incomes(ticker, selected_portfolio_ids)
    position = get_asset_position_summary(ticker, selected_portfolio_ids)
    price_history = get_asset_price_history(ticker, chart_range)
    chart_ranges = [
        {"key": "1d", "label": "1 DIA"},
        {"key": "7d", "label": "7 DIAS"},
        {"key": "30d", "label": "30 DIAS"},
        {"key": "6m", "label": "6 MESES"},
        {"key": "1y", "label": "1 ANO"},
        {"key": "5y", "label": "5 ANOS"},
    ]
    range_label_map = {item["key"]: item["label"] for item in chart_ranges}
    price_history["range_label"] = range_label_map.get(price_history.get("range_key"), "PERIODO")
    return render_template(
        "ativo.html",
        asset=asset,
        transactions=transactions,
        incomes=incomes,
        position=position,
        price_history=price_history,
        chart_ranges=chart_ranges,
        sync_message=sync_message,
        sync_status=sync_status,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/ativo/<ticker>/atualizar-yahoo", methods=["POST"])
def atualizar_ativo_yahoo(ticker):
    selected_portfolio_ids = _selected_portfolio_ids()
    ok = False
    try:
        ok = refresh_asset_market_data(ticker)
    except Exception:
        ok = False

    if not ok:
        return redirect(
            url_for("main.ativo", ticker=ticker, sync="fail", portfolio_id=selected_portfolio_ids)
        )

    asset = get_asset(ticker)
    if asset and asset["name"] != ticker.upper() and asset["sector"] != "Nao informado":
        return redirect(url_for("main.ativo", ticker=ticker, sync="ok", portfolio_id=selected_portfolio_ids))
    return redirect(url_for("main.ativo", ticker=ticker, sync="partial", portfolio_id=selected_portfolio_ids))


@main_bp.route("/transacoes/nova", methods=["GET", "POST"])
def nova_transacao():
    selected_portfolio_ids = _selected_portfolio_ids()
    active_portfolio_id = _active_portfolio_id(selected_portfolio_ids)
    error = None
    import_message = None
    import_errors = []
    remove_message = None
    fixed_income_error = None
    fixed_income_message = None
    fixed_income_import_errors = []

    removed = request.args.get("removed")
    remove_error = request.args.get("remove_error")
    if removed:
        remove_message = f"{removed} transacao(oes) removida(s)."
    elif remove_error == "none":
        error = "Selecione ao menos uma transacao para remover."

    if request.method == "POST":
        action = request.form.get("action", "manual")
        if action == "import_csv":
            target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
            file = request.files.get("csv_file")
            file_bytes = file.read() if file else b""
            ok, message, imported, import_errors = import_transactions_csv(file_bytes, target_portfolio_id)
            if ok:
                try:
                    refresh_all_assets_market_data()
                except Exception:
                    pass
                import_message = f"Importacao finalizada. {imported} transacoes importadas."
            else:
                error = message
        elif action == "fixed_income":
            ok, message = add_fixed_income(request.form)
            if ok:
                fixed_income_message = message
            else:
                fixed_income_error = message
        elif action == "import_fixed_income_csv":
            target_portfolio_id = resolve_portfolio_id(request.form.get("target_portfolio_id"))
            file = request.files.get("fixed_income_csv_file")
            file_bytes = file.read() if file else b""
            ok, message, imported, fixed_income_import_errors = import_fixed_incomes_csv(
                file_bytes, target_portfolio_id
            )
            if ok:
                fixed_income_message = f"Importacao de renda fixa finalizada. {imported} registro(s) importado(s)."
            else:
                fixed_income_error = message
        else:
            ok, message = add_transaction(request.form)
            if ok:
                try:
                    refresh_asset_market_data(request.form.get("ticker", ""))
                except Exception:
                    pass
                return redirect(url_for("main.carteira", portfolio_id=selected_portfolio_ids))
            error = message

    return render_template(
        "nova_transacao.html",
        error=error,
        import_message=import_message,
        import_errors=import_errors,
        remove_message=remove_message,
        fixed_income_error=fixed_income_error,
        fixed_income_message=fixed_income_message,
        fixed_income_import_errors=fixed_income_import_errors,
        transactions=get_transactions(selected_portfolio_ids),
        **_base_context(selected_portfolio_ids),
        target_portfolio_id=active_portfolio_id,
    )


@main_bp.route("/transacoes/remover", methods=["POST"])
def remover_transacoes():
    selected_portfolio_ids = _selected_portfolio_ids()
    transaction_ids = request.form.getlist("transaction_ids")
    removed = delete_transactions(transaction_ids, selected_portfolio_ids)
    if removed <= 0:
        return redirect(url_for("main.nova_transacao", portfolio_id=selected_portfolio_ids, remove_error="none"))
    return redirect(url_for("main.nova_transacao", portfolio_id=selected_portfolio_ids, removed=removed))


@main_bp.route("/proventos/novo", methods=["GET", "POST"])
def novo_provento():
    selected_portfolio_ids = _selected_portfolio_ids()
    active_portfolio_id = _active_portfolio_id(selected_portfolio_ids)
    error = None

    if request.method == "POST":
        ok, message = add_income(request.form)
        if ok:
            return redirect(url_for("main.novo_provento", portfolio_id=selected_portfolio_ids))
        error = message

    return render_template(
        "novo_provento.html",
        error=error,
        incomes=get_incomes(selected_portfolio_ids),
        **_base_context(selected_portfolio_ids),
        target_portfolio_id=active_portfolio_id,
    )


@main_bp.route("/renda-fixa")
def renda_fixa():
    selected_portfolio_ids = _selected_portfolio_ids()
    sort_by = request.args.get("sort_by", "date_aporte")
    sort_dir = request.args.get("sort_dir", "desc")
    removed = request.args.get("removed")
    remove_error = request.args.get("remove_error")
    remove_message = None
    error = None
    if removed:
        remove_message = f"{removed} registro(s) de renda fixa removido(s)."
    elif remove_error == "none":
        error = "Selecione ao menos um registro de renda fixa para remover."

    fixed_incomes = get_fixed_incomes(selected_portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)
    prefixado_items = [item for item in fixed_incomes if (item.get("rate_type") or "").upper() == "FIXO"]
    posfixado_items = [item for item in fixed_incomes if (item.get("rate_type") or "").upper() != "FIXO"]

    def _group_summary(items):
        return {
            "count": len(items),
            "applied_total": round(sum(float(item.get("active_applied_value", 0.0)) for item in items), 2),
            "current_total": round(sum(float(item.get("current_gross_value", 0.0)) for item in items), 2),
            "income_total": round(sum(float(item.get("current_income", 0.0)) for item in items), 2),
            "total_received": round(sum(float(item.get("total_received", 0.0)) for item in items), 2),
            "rendimento_recebido_total": round(sum(float(item.get("rendimento", 0.0)) for item in items), 2),
        }

    fixed_income_groups = [
        {
            "key": "prefixado",
            "title": "Juros Prefixado",
            "items": prefixado_items,
            "summary": _group_summary(prefixado_items),
        },
        {
            "key": "posfixado",
            "title": "Juros Pos-fixado",
            "items": posfixado_items,
            "summary": _group_summary(posfixado_items),
        },
    ]

    return render_template(
        "renda_fixa.html",
        error=error,
        remove_message=remove_message,
        summary=get_fixed_income_summary(selected_portfolio_ids),
        fixed_incomes=fixed_incomes,
        fixed_income_groups=fixed_income_groups,
        sort_by=sort_by,
        sort_dir=sort_dir,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/renda-fixa/remover", methods=["POST"])
def remover_renda_fixa():
    selected_portfolio_ids = _selected_portfolio_ids()
    fixed_income_ids = request.form.getlist("fixed_income_ids")
    removed = delete_fixed_incomes(fixed_income_ids, selected_portfolio_ids)
    if removed <= 0:
        return redirect(url_for("main.renda_fixa", portfolio_id=selected_portfolio_ids, remove_error="none"))
    return redirect(url_for("main.renda_fixa", portfolio_id=selected_portfolio_ids, removed=removed))


@main_bp.route("/graficos")
def graficos():
    selected_portfolio_ids = _selected_portfolio_ids()
    portfolio = get_portfolio_snapshot(selected_portfolio_ids)
    fixed_income = get_fixed_income_summary(selected_portfolio_ids)
    fixed_income_items = get_fixed_incomes(selected_portfolio_ids)
    monthly_class_summary = get_monthly_class_summary(selected_portfolio_ids)
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
    category_chart = {
        "labels": [],
        "values": [],
    }
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
            annual_invested_map[year] = {key: 0.0 for _, key in month_order}
        if month_key in annual_invested_map[year]:
            annual_invested_map[year][month_key] = float(row.get("total_invested", 0.0))

    annual_invested_summary = {
        "months": [label for label, _ in month_order],
        "years": [],
    }
    for year in sorted(annual_invested_map.keys()):
        month_values = [annual_invested_map[year][key] for _, key in month_order]
        annual_invested_summary["years"].append(
            {
                "label": str(year),
                "total": round(sum(month_values), 2),
                "values": [round(value, 2) for value in month_values],
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

    return render_template(
        "graficos.html",
        category_chart=category_chart,
        top_assets_chart=top_assets_chart,
        allocation_by_group_charts=allocation_by_group_charts,
        cards_chart=cards_chart,
        result_by_category_chart=result_by_category_chart,
        classes_chart=classes_chart,
        fixed_income_investment_chart=fixed_income_investment_chart,
        fixed_income_distributor_chart=fixed_income_distributor_chart,
        fixed_income_issuer_chart=fixed_income_issuer_chart,
        monthly_class_summary=monthly_class_summary,
        annual_invested_summary=annual_invested_summary,
        monthly_income_chart=monthly_income_chart,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/setores")
def setores():
    selected_portfolio_ids = _selected_portfolio_ids()
    sectors = get_sectors_summary()
    return render_template(
        "setores.html",
        sectors=sectors,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/carteira")
def carteira():
    selected_portfolio_ids = _selected_portfolio_ids()
    sort_by = request.args.get("sort_by", "value")
    sort_dir = request.args.get("sort_dir", "desc")
    sync = request.args.get("sync")
    failed = request.args.get("failed", "0")
    sync_message = None
    sync_status = None

    if sync == "ok":
        sync_message = "Consulta ao Yahoo concluida com sucesso (cotacoes/indicadores recebidos)."
        sync_status = "ok"
    elif sync == "partial":
        try:
            failed_count = max(0, int(failed))
        except ValueError:
            failed_count = 0
        sync_message = (
            f"Atualizacao concluida com pendencias: {failed_count} ticker(s) falharam no Yahoo."
            if failed_count > 0
            else "Atualizacao concluida com pendencias no Yahoo."
        )
        sync_status = "partial"

    portfolio = get_portfolio_snapshot(selected_portfolio_ids, sort_by=sort_by, sort_dir=sort_dir)
    return render_template(
        "carteira.html",
        portfolio=portfolio,
        sort_by=portfolio["sort_by"],
        sort_dir=portfolio["sort_dir"],
        sync_message=sync_message,
        sync_status=sync_status,
        **_base_context(selected_portfolio_ids),
    )


@main_bp.route("/carteira/atualizar-yahoo", methods=["POST"])
def carteira_atualizar_yahoo():
    selected_portfolio_ids = _selected_portfolio_ids()
    try:
        failed = refresh_all_assets_market_data()
    except Exception:
        failed = ["erro"]

    if failed:
        return redirect(
            url_for(
                "main.carteira",
                sync="partial",
                failed=len(failed),
                portfolio_id=selected_portfolio_ids,
            )
        )
    return redirect(
        url_for(
            "main.carteira",
            sync="ok",
            portfolio_id=selected_portfolio_ids,
        )
    )


@main_bp.route("/carteiras", methods=["GET", "POST"])
def carteiras():
    selected_portfolio_ids = _selected_portfolio_ids()
    error = None
    success = None

    if request.method == "POST":
        action = (request.form.get("action") or "create").strip().lower()
        if action == "delete":
            ok, result = delete_portfolio(request.form.get("portfolio_id"))
            if ok:
                success = f"Carteira '{result}' removida com sucesso."
            else:
                error = result
        else:
            ok, result = create_portfolio(request.form.get("name"))
            if ok:
                return redirect(url_for("main.carteira", portfolio_id=[result]))
            error = result

    return render_template(
        "carteiras.html",
        error=error,
        success=success,
        **_base_context(selected_portfolio_ids),
    )

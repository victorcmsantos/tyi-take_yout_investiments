"""Service layer split by domain with backward-compatible exports."""

from . import _legacy as _legacy
from .market_data import (
    get_asset,
    get_asset_upcoming_incomes,
    get_asset_price_history,
    get_top_assets,
    prefetch_upcoming_incomes_for_portfolios,
    refresh_assets_market_data,
    refresh_all_assets_market_data,
    refresh_asset_market_data,
    refresh_stale_assets_market_data,
)
from .openclaw import (
    enrich_asset_with_openclaw,
    enrich_assets_with_openclaw_batch,
    get_asset_enrichment,
    get_asset_enrichment_history,
)
from .portfolio import (
    add_fixed_income,
    add_income,
    add_transaction,
    create_portfolio,
    delete_fixed_incomes,
    delete_incomes,
    delete_portfolio,
    delete_transactions,
    get_asset_incomes,
    get_asset_position_summary,
    get_asset_transactions,
    get_benchmark_comparison,
    get_fixed_income_payload_cached,
    get_fixed_income_summary,
    get_fixed_incomes,
    get_incomes,
    get_monthly_class_summary,
    get_monthly_ticker_summary,
    get_portfolio_snapshot,
    get_portfolios,
    get_sectors_summary,
    get_transactions,
    import_fixed_incomes_csv,
    import_transactions_csv,
    normalize_portfolio_ids,
    rebuild_chart_snapshots,
    rebuild_fixed_income_snapshots,
    resolve_portfolio_id,
)
from .scanner import get_metric_formulas_catalog, update_metric_formula

__all__ = [
    "add_fixed_income",
    "add_income",
    "add_transaction",
    "create_portfolio",
    "delete_fixed_incomes",
    "delete_incomes",
    "delete_portfolio",
    "delete_transactions",
    "enrich_asset_with_openclaw",
    "enrich_assets_with_openclaw_batch",
    "get_asset",
    "get_asset_upcoming_incomes",
    "get_asset_enrichment",
    "get_asset_enrichment_history",
    "get_asset_incomes",
    "get_asset_position_summary",
    "get_asset_price_history",
    "get_asset_transactions",
    "get_benchmark_comparison",
    "get_fixed_income_payload_cached",
    "get_fixed_income_summary",
    "get_fixed_incomes",
    "get_incomes",
    "get_metric_formulas_catalog",
    "get_monthly_class_summary",
    "get_monthly_ticker_summary",
    "get_portfolio_snapshot",
    "get_portfolios",
    "get_sectors_summary",
    "get_top_assets",
    "get_transactions",
    "import_fixed_incomes_csv",
    "import_transactions_csv",
    "normalize_portfolio_ids",
    "prefetch_upcoming_incomes_for_portfolios",
    "rebuild_chart_snapshots",
    "rebuild_fixed_income_snapshots",
    "refresh_assets_market_data",
    "refresh_all_assets_market_data",
    "refresh_asset_market_data",
    "refresh_stale_assets_market_data",
    "resolve_portfolio_id",
    "update_metric_formula",
]


def __getattr__(name):
    # Backward compatibility for legacy/private symbols while the split
    # is rolled out progressively.
    try:
        return getattr(_legacy, name)
    except AttributeError as exc:
        raise AttributeError(f"module '{__name__}' has no attribute '{name}'") from exc

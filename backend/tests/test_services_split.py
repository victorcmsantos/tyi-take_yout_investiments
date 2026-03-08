import unittest

from app import services
from app.services import _legacy
from app.services import market_data, openclaw, portfolio, scanner


class ServicesSplitExportsTest(unittest.TestCase):
    def test_public_contract_is_available(self):
        expected = [
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
            "rebuild_chart_snapshots",
            "rebuild_fixed_income_snapshots",
            "refresh_all_assets_market_data",
            "refresh_asset_market_data",
            "resolve_portfolio_id",
            "update_metric_formula",
        ]
        for name in expected:
            with self.subTest(name=name):
                self.assertTrue(callable(getattr(services, name)))

    def test_domain_modules_wiring(self):
        self.assertEqual(market_data.get_asset.__module__, "app.services.market_data")
        self.assertEqual(market_data.refresh_asset_market_data.__module__, "app.services.market_data")
        self.assertEqual(portfolio.get_portfolios.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.normalize_portfolio_ids.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_transactions.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.delete_transactions.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_incomes.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.delete_incomes.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.add_transaction.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.add_income.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.import_transactions_csv.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.add_fixed_income.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.import_fixed_incomes_csv.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.delete_fixed_incomes.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_fixed_incomes.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_fixed_income_summary.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_fixed_income_payload_cached.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.rebuild_fixed_income_snapshots.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_asset_transactions.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_asset_incomes.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_asset_position_summary.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_sectors_summary.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_benchmark_comparison.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_portfolio_snapshot.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_monthly_class_summary.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.get_monthly_ticker_summary.__module__, "app.services.portfolio")
        self.assertEqual(portfolio.rebuild_chart_snapshots.__module__, "app.services.portfolio")
        self.assertEqual(openclaw.enrich_asset_with_openclaw.__module__, "app.services.openclaw")
        self.assertEqual(scanner.update_metric_formula.__module__, "app.services.scanner")

    def test_market_data_meta_default(self):
        meta = market_data._market_data_meta_from_asset(None)
        self.assertEqual(meta["status"], "unknown")
        self.assertTrue(meta["is_stale"])
        self.assertFalse(meta["is_live"])

    def test_scanner_formula_validation_blocks_unsafe_name(self):
        with self.assertRaises(ValueError):
            scanner._validate_metric_formula_expression("__import__('os').system('id')")

    def test_openclaw_payload_normalization(self):
        payload = openclaw._normalize_asset_enrichment_payload(
            {
                "resumo": " ok ",
                "tese": [" um ", "", None],
                "riscos": "texto-invalido",
                "acao_sugerida": " segurar ",
            }
        )
        self.assertEqual(payload["resumo"], "ok")
        self.assertEqual(payload["tese"], ["um"])
        self.assertEqual(payload["riscos"], [])
        self.assertEqual(payload["acao_sugerida"], "segurar")

    def test_portfolio_normalize_ids(self):
        original_get_portfolios = portfolio.get_portfolios
        original_get_default = portfolio.get_default_portfolio_id
        try:
            portfolio.get_portfolios = lambda: [{"id": 1}, {"id": 2}]
            portfolio.get_default_portfolio_id = lambda: 1
            result = portfolio.normalize_portfolio_ids(["2", "2", "x", "", None, 1])
            self.assertEqual(result, [2, 1])
            self.assertEqual(portfolio.normalize_portfolio_ids(["x"]), [1])
        finally:
            portfolio.get_portfolios = original_get_portfolios
            portfolio.get_default_portfolio_id = original_get_default

    def test_legacy_private_symbol_is_still_resolvable(self):
        self.assertIs(services._parse_float, _legacy._parse_float)

    def test_legacy_wrappers_delegate_to_split_modules(self):
        original_refresh = market_data.refresh_market_data_for_tickers
        original_enrichment = openclaw.get_asset_enrichment
        original_transactions = portfolio.get_transactions
        try:
            market_data.refresh_market_data_for_tickers = lambda tickers, attempts=2: [
                f"failed:{','.join(tickers)}:{attempts}"
            ]
            openclaw.get_asset_enrichment = lambda ticker: {"ticker": ticker, "payload": {"ok": True}}
            portfolio.get_transactions = lambda portfolio_ids: [{"portfolio_ids": list(portfolio_ids)}]

            self.assertEqual(
                _legacy.refresh_market_data_for_tickers(["PETR4", "VALE3"], attempts=4),
                ["failed:PETR4,VALE3:4"],
            )
            self.assertEqual(
                _legacy.get_asset_enrichment("PETR4"),
                {"ticker": "PETR4", "payload": {"ok": True}},
            )
            self.assertEqual(
                _legacy.get_transactions([1, 2]),
                [{"portfolio_ids": [1, 2]}],
            )
        finally:
            market_data.refresh_market_data_for_tickers = original_refresh
            openclaw.get_asset_enrichment = original_enrichment
            portfolio.get_transactions = original_transactions


if __name__ == "__main__":
    unittest.main()

import os
import unittest

from app import services
from app.services import _legacy
from app.services import legacy_compat
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
            market_data.refresh_market_data_for_tickers = lambda tickers, attempts=2, include_scanner_br=True: [
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

    def test_legacy_market_helpers_delegate_to_legacy_compat(self):
        original_fetch_market_profile = legacy_compat._fetch_market_profile
        original_provider_label = legacy_compat._market_data_provider_label
        try:
            legacy_compat._fetch_market_profile = (
                lambda ticker, include_scanner_br=True: ({"name": ticker}, "compat")
            )
            legacy_compat._market_data_provider_label = (
                lambda ticker="", include_scanner_br=True: f"compat:{ticker}"
            )
            self.assertEqual(_legacy._fetch_market_profile("PETR4"), ({"name": "PETR4"}, "compat"))
            self.assertEqual(_legacy._market_data_provider_label("PETR4"), "compat:PETR4")
        finally:
            legacy_compat._fetch_market_profile = original_fetch_market_profile
            legacy_compat._market_data_provider_label = original_provider_label

    def test_coingecko_enters_cooldown_after_rate_limit(self):
        original_http_get_json_with_status = _legacy._http_get_json_with_status
        original_cooldown = os.environ.get("COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS")
        try:
            call_count = {"value": 0}

            def _fake_http_get_json_with_status(url, headers=None, timeout=8.0, attempts=2):
                call_count["value"] += 1
                return None, 429

            _legacy._http_get_json_with_status = _fake_http_get_json_with_status
            os.environ["COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS"] = "120"
            _legacy._COINGECKO_CACHE.clear()
            _legacy._COINGECKO_CIRCUIT["until"] = 0.0
            _legacy._COINGECKO_CIRCUIT["status_code"] = None

            self.assertIsNone(_legacy._fetch_coingecko_market_item("BTC-USD"))
            self.assertIsNone(_legacy._fetch_coingecko_market_item("BTC-USD"))
            self.assertEqual(call_count["value"], 1)
            self.assertGreater(float(_legacy._COINGECKO_CIRCUIT.get("until", 0.0) or 0.0), 0.0)
        finally:
            _legacy._http_get_json_with_status = original_http_get_json_with_status
            if original_cooldown is None:
                os.environ.pop("COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS", None)
            else:
                os.environ["COINGECKO_RATE_LIMIT_COOLDOWN_SECONDS"] = original_cooldown
            _legacy._COINGECKO_CIRCUIT["until"] = 0.0
            _legacy._COINGECKO_CIRCUIT["status_code"] = None
            _legacy._COINGECKO_CACHE.clear()

    def test_refresh_stale_assets_market_data_filters_fresh_assets(self):
        original_get_db = market_data.get_db
        original_refresh = market_data.refresh_market_data_for_tickers
        now_iso = market_data._now_iso()

        class _FakeCursor:
            def fetchall(self):
                return [
                    {
                        "ticker": "BTC-USD",
                        "market_data_status": "stale",
                        "market_data_source": "coingecko",
                        "market_data_updated_at": None,
                        "market_data_last_attempt_at": None,
                        "market_data_last_error": "limite",
                    },
                    {
                        "ticker": "PETR4",
                        "market_data_status": "fresh",
                        "market_data_source": "brapi",
                        "market_data_updated_at": now_iso,
                        "market_data_last_attempt_at": now_iso,
                        "market_data_last_error": "",
                    },
                ]

        class _FakeDb:
            def execute(self, _query):
                return _FakeCursor()

        try:
            market_data.get_db = lambda: _FakeDb()
            market_data.refresh_market_data_for_tickers = (
                lambda tickers, attempts=2, include_scanner_br=True: list(tickers)
            )
            self.assertEqual(market_data.refresh_stale_assets_market_data(attempts=2), ["BTC-USD"])
        finally:
            market_data.get_db = original_get_db
            market_data.refresh_market_data_for_tickers = original_refresh

    def test_refresh_assets_market_data_scope_br_filters_universe(self):
        original_get_db = market_data.get_db
        original_refresh = market_data.refresh_market_data_for_tickers

        class _FakeCursor:
            def fetchall(self):
                return [
                    {
                        "ticker": "PETR4",
                        "market_data_status": "stale",
                        "market_data_source": "market_scanner",
                        "market_data_updated_at": None,
                        "market_data_last_attempt_at": None,
                        "market_data_last_error": "",
                    },
                    {
                        "ticker": "AAPL",
                        "market_data_status": "stale",
                        "market_data_source": "yahoo",
                        "market_data_updated_at": None,
                        "market_data_last_attempt_at": None,
                        "market_data_last_error": "",
                    },
                    {
                        "ticker": "BTC-USD",
                        "market_data_status": "stale",
                        "market_data_source": "coingecko",
                        "market_data_updated_at": None,
                        "market_data_last_attempt_at": None,
                        "market_data_last_error": "",
                    },
                ]

        class _FakeDb:
            def execute(self, _query):
                return _FakeCursor()

        captured = {"tickers": None, "include_scanner_br": None}

        def _fake_refresh(tickers, attempts=2, include_scanner_br=True):
            captured["tickers"] = list(tickers)
            captured["include_scanner_br"] = include_scanner_br
            return []

        try:
            market_data.get_db = lambda: _FakeDb()
            market_data.refresh_market_data_for_tickers = _fake_refresh
            result = market_data.refresh_assets_market_data(
                scope_key="br",
                stale_only=False,
                attempts=2,
                include_scanner_br=False,
            )
            self.assertEqual(result["scope"], "br")
            self.assertEqual(result["selected_count"], 1)
            self.assertEqual(captured["tickers"], ["PETR4"])
            self.assertFalse(captured["include_scanner_br"])
            self.assertEqual(result["failed"], [])
        finally:
            market_data.get_db = original_get_db
            market_data.refresh_market_data_for_tickers = original_refresh

    def test_brapi_cache_ttl_uses_env_override(self):
        original_ttl = os.environ.get("BRAPI_QUOTE_CACHE_TTL_SECONDS")
        try:
            os.environ["BRAPI_QUOTE_CACHE_TTL_SECONDS"] = "180"
            self.assertEqual(_legacy._brapi_quote_cache_ttl_seconds(), 180.0)
            os.environ["BRAPI_QUOTE_CACHE_TTL_SECONDS"] = "1"
            self.assertEqual(_legacy._brapi_quote_cache_ttl_seconds(), 5.0)
            os.environ["BRAPI_QUOTE_CACHE_TTL_SECONDS"] = "invalid"
            self.assertEqual(_legacy._brapi_quote_cache_ttl_seconds(), 120.0)
        finally:
            if original_ttl is None:
                os.environ.pop("BRAPI_QUOTE_CACHE_TTL_SECONDS", None)
            else:
                os.environ["BRAPI_QUOTE_CACHE_TTL_SECONDS"] = original_ttl

    def test_market_scanner_provider_is_prepended_for_br_assets(self):
        original_providers = os.environ.get("MARKET_DATA_PROVIDERS")
        original_br = os.environ.get("MARKET_DATA_PROVIDERS_BR")
        original_scanner_flag = os.environ.get("MARKET_DATA_USE_SCANNER_BR")
        try:
            os.environ["MARKET_DATA_PROVIDERS"] = "brapi,yahoo"
            os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            os.environ["MARKET_DATA_USE_SCANNER_BR"] = "1"
            providers = _legacy._market_data_providers_from_env("PETR4")
            self.assertGreaterEqual(len(providers), 1)
            self.assertEqual(providers[0], "market_scanner")
            self.assertIn("brapi", providers)
        finally:
            if original_providers is None:
                os.environ.pop("MARKET_DATA_PROVIDERS", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS"] = original_providers
            if original_br is None:
                os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS_BR"] = original_br
            if original_scanner_flag is None:
                os.environ.pop("MARKET_DATA_USE_SCANNER_BR", None)
            else:
                os.environ["MARKET_DATA_USE_SCANNER_BR"] = original_scanner_flag

    def test_market_scanner_provider_can_be_disabled_by_env(self):
        original_providers = os.environ.get("MARKET_DATA_PROVIDERS")
        original_br = os.environ.get("MARKET_DATA_PROVIDERS_BR")
        original_scanner_flag = os.environ.get("MARKET_DATA_USE_SCANNER_BR")
        try:
            os.environ["MARKET_DATA_PROVIDERS"] = "brapi,yahoo"
            os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            os.environ["MARKET_DATA_USE_SCANNER_BR"] = "0"
            providers = _legacy._market_data_providers_from_env("PETR4")
            self.assertNotIn("market_scanner", providers)
            self.assertIn("brapi", providers)
        finally:
            if original_providers is None:
                os.environ.pop("MARKET_DATA_PROVIDERS", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS"] = original_providers
            if original_br is None:
                os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS_BR"] = original_br
            if original_scanner_flag is None:
                os.environ.pop("MARKET_DATA_USE_SCANNER_BR", None)
            else:
                os.environ["MARKET_DATA_USE_SCANNER_BR"] = original_scanner_flag

    def test_market_scanner_provider_can_be_skipped_for_manual_live_refresh(self):
        original_providers = os.environ.get("MARKET_DATA_PROVIDERS")
        original_br = os.environ.get("MARKET_DATA_PROVIDERS_BR")
        original_scanner_flag = os.environ.get("MARKET_DATA_USE_SCANNER_BR")
        try:
            os.environ["MARKET_DATA_PROVIDERS"] = "brapi,yahoo"
            os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            os.environ["MARKET_DATA_USE_SCANNER_BR"] = "1"
            providers = _legacy._market_data_providers_from_env(
                "PETR4",
                include_scanner_br=False,
            )
            self.assertNotIn("market_scanner", providers)
            self.assertIn("brapi", providers)
        finally:
            if original_providers is None:
                os.environ.pop("MARKET_DATA_PROVIDERS", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS"] = original_providers
            if original_br is None:
                os.environ.pop("MARKET_DATA_PROVIDERS_BR", None)
            else:
                os.environ["MARKET_DATA_PROVIDERS_BR"] = original_br
            if original_scanner_flag is None:
                os.environ.pop("MARKET_DATA_USE_SCANNER_BR", None)
            else:
                os.environ["MARKET_DATA_USE_SCANNER_BR"] = original_scanner_flag


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest
from pathlib import Path

from app import create_app
from app.auth import create_user_account
from app.services import market_data
import app.api_routes as api_routes


class SyncHealthFeaturesTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.db_path = root / 'test_sync_health.db'
        self.backup_dir = root / 'backups'
        self.secret_file = root / '.flask-secret'
        self.bootstrap_file = root / 'admin-bootstrap.txt'
        self.bg_lock = root / '.background-jobs.lock'
        self.startup_lock = root / '.db-startup.lock'

        self.original_env = {
            'DATABASE': os.environ.get('DATABASE'),
            'DATABASE_BACKUP_DIR': os.environ.get('DATABASE_BACKUP_DIR'),
            'AUTH_SECRET_KEY_FILE': os.environ.get('AUTH_SECRET_KEY_FILE'),
            'ADMIN_BOOTSTRAP_FILE': os.environ.get('ADMIN_BOOTSTRAP_FILE'),
            'BACKGROUND_JOBS_LOCK_FILE': os.environ.get('BACKGROUND_JOBS_LOCK_FILE'),
            'DATABASE_STARTUP_LOCK_FILE': os.environ.get('DATABASE_STARTUP_LOCK_FILE'),
            'MARKET_DATA_STALE_AFTER_SECONDS': os.environ.get('MARKET_DATA_STALE_AFTER_SECONDS'),
            'MARKET_DATA_STALE_AFTER_SECONDS_CRYPTO': os.environ.get('MARKET_DATA_STALE_AFTER_SECONDS_CRYPTO'),
        }
        os.environ['DATABASE'] = str(self.db_path)
        os.environ['DATABASE_BACKUP_DIR'] = str(self.backup_dir)
        os.environ['AUTH_SECRET_KEY_FILE'] = str(self.secret_file)
        os.environ['ADMIN_BOOTSTRAP_FILE'] = str(self.bootstrap_file)
        os.environ['BACKGROUND_JOBS_LOCK_FILE'] = str(self.bg_lock)
        os.environ['DATABASE_STARTUP_LOCK_FILE'] = str(self.startup_lock)

        self.app = create_app()
        with self.app.app_context():
            ok, _, self.user = create_user_account('sync_viewer', 'sync-pass-123', role='trader')
            self.assertTrue(ok)

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmpdir.cleanup()

    def test_market_data_meta_uses_class_specific_ttl(self):
        os.environ['MARKET_DATA_STALE_AFTER_SECONDS'] = '43200'
        os.environ['MARKET_DATA_STALE_AFTER_SECONDS_CRYPTO'] = '120'
        asset = {
            'ticker': 'BTC-USD',
            'sector': 'Cripto',
            'market_data_status': 'fresh',
            'market_data_updated_at': None,
            'market_data_source': 'coingecko',
            'market_data_last_attempt_at': None,
            'market_data_last_error': '',
            'market_data_provider_trace': 'yahoo,coingecko',
            'market_data_fallback_used': 1,
        }
        meta = market_data._market_data_meta_from_asset(asset)
        self.assertEqual(meta['class_key'], 'crypto')
        self.assertEqual(int(meta['stale_after_seconds']), 120)
        self.assertTrue(meta['is_stale'])
        self.assertTrue(meta['fallback_used'])
        self.assertEqual(meta['providers_tried'], ['yahoo', 'coingecko'])

    def test_scanner_trade_reconciliation_flags_divergence(self):
        reconciled = api_routes._scanner_reconcile_trade_item(
            {
                'id': 10,
                'ticker': 'PETR4.SA',
                'status': 'OPEN',
                'quantity': 100,
                'invested_amount': 4000,
                'entry_price': 40,
                'last_price': 42,
                'current_pnl_amount': 0,
                'current_pnl_pct': 0,
            }
        )
        self.assertAlmostEqual(float(reconciled['current_pnl_amount']), 200.0, places=2)
        self.assertTrue(bool((reconciled.get('pnl_reconciliation') or {}).get('divergence')))

    def test_sync_status_endpoint_returns_payload(self):
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess['user_id'] = int(self.user['id'])
            response = client.get('/api/sync/status')
            self.assertEqual(response.status_code, 200)
            payload = response.get_json(silent=True) or {}
            data = payload.get('data') or {}
            self.assertIn('health', data)
            self.assertIn('scanner', data)
            self.assertIn('stale_assets_total', data)
            self.assertIn('telegram', data.get('health') or {})

    def test_sync_stale_market_data_endpoint_returns_summary(self):
        calls = []
        original_refresh = api_routes.refresh_assets_market_data

        def _fake_refresh_assets_market_data(**kwargs):
            calls.append(dict(kwargs))
            return {
                'scope': 'all',
                'stale_only': True,
                'selected_count': 3,
                'failed': ['PETR4.SA'],
            }

        api_routes.refresh_assets_market_data = _fake_refresh_assets_market_data
        try:
            with self.app.test_client() as client:
                with client.session_transaction() as sess:
                    sess['user_id'] = int(self.user['id'])
                response = client.post('/api/sync/market-data/stale', json={'attempts': 2})
                self.assertEqual(response.status_code, 200)
                payload = response.get_json(silent=True) or {}
                data = payload.get('data') or {}
                self.assertEqual(data.get('mode'), 'manual_stale_sync')
                self.assertEqual(int(data.get('selected_count') or 0), 3)
                self.assertEqual(int(data.get('updated_count') or 0), 2)
                self.assertEqual(int(data.get('failed_count') or 0), 1)
                self.assertEqual(data.get('failed') or [], ['PETR4.SA'])
                self.assertEqual(len(calls), 1)
                self.assertTrue(bool(calls[0].get('stale_only')))
        finally:
            api_routes.refresh_assets_market_data = original_refresh

    def test_variable_income_daily_chart_endpoint_returns_payload(self):
        calls = []
        original_chart_fn = api_routes.get_variable_income_value_daily_series

        def _fake_daily_chart(portfolio_ids, range_key='90d'):
            calls.append({
                'portfolio_ids': list(portfolio_ids or []),
                'range_key': str(range_key or ''),
            })
            return {
                'range_key': '90d',
                'labels': ['10/03', '11/03'],
                'values': [1000.0, 1015.5],
                'points_count': 2,
                'included_tickers': ['PETR4.SA'],
                'missing_tickers': [],
                'current_total_value': 1015.5,
                'generated_at': '2026-03-12T00:00:00Z',
            }

        api_routes.get_variable_income_value_daily_series = _fake_daily_chart
        try:
            with self.app.test_client() as client:
                with client.session_transaction() as sess:
                    sess['user_id'] = int(self.user['id'])
                response = client.get('/api/charts/variable-income-value-daily?range=90d')
                self.assertEqual(response.status_code, 200)
                payload = response.get_json(silent=True) or {}
                data = payload.get('data') or {}
                self.assertEqual(data.get('range_key'), '90d')
                self.assertEqual(data.get('labels') or [], ['10/03', '11/03'])
                self.assertEqual(data.get('values') or [], [1000.0, 1015.5])
                self.assertEqual(int(data.get('points_count') or 0), 2)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0]['range_key'], '90d')
        finally:
            api_routes.get_variable_income_value_daily_series = original_chart_fn

    def test_build_charts_core_payload_uses_current_income_for_cri_cra_deb(self):
        original_snapshot = api_routes.get_portfolio_snapshot
        original_fixed_income = api_routes.get_fixed_income_payload_cached
        original_monthly_summary = api_routes.get_monthly_class_summary

        api_routes.get_portfolio_snapshot = lambda *args, **kwargs: {
            'positions': [],
            'grouped_positions': {
                'br_stocks': [],
                'us_stocks': [],
                'fiis': [],
                'crypto': [],
            },
            'group_totals': {
                'br_stocks': 0.0,
                'us_stocks': 0.0,
                'fiis': 0.0,
                'crypto': 0.0,
            },
            'group_summaries': {
                'us_stocks': {'open_pnl_value': 100.0},
                'fiis': {'open_pnl_value': 200.0},
                'br_stocks': {'open_pnl_value': 300.0},
                'crypto': {'open_pnl_value': -50.0},
            },
            'total_value': 0.0,
            'invested_value': 0.0,
            'total_incomes': 0.0,
        }
        api_routes.get_fixed_income_payload_cached = lambda *args, **kwargs: {
            'summary': {'current_total': 0.0},
            'items': [
                {
                    'investment_type': 'CRI',
                    'open_pnl_value': 1500.0,
                    'current_income': 9000.0,
                    'active_applied_value': 0.0,
                },
                {
                    'investment_type': 'DEBEN INCENTIVADA',
                    'open_pnl_value': 250.0,
                    'current_income': 7000.0,
                    'active_applied_value': 0.0,
                },
                {
                    'investment_type': 'TESOURO',
                    'open_pnl_value': 3333.0,
                    'current_income': 8888.0,
                    'active_applied_value': 0.0,
                },
                {
                    'investment_type': 'CDB',
                    'open_pnl_value': 99999.0,
                    'current_income': 99999.0,
                    'active_applied_value': 0.0,
                },
            ],
        }
        api_routes.get_monthly_class_summary = lambda *args, **kwargs: []

        try:
            payload = api_routes._build_charts_core_payload([1])
            self.assertEqual(
                payload['result_by_category_chart']['values'],
                [100.0, 200.0, 300.0, -50.0, 16000.0],
            )
        finally:
            api_routes.get_portfolio_snapshot = original_snapshot
            api_routes.get_fixed_income_payload_cached = original_fixed_income
            api_routes.get_monthly_class_summary = original_monthly_summary

    def test_build_charts_core_payload_ignores_non_cri_cra_deb_fixed_income(self):
        original_snapshot = api_routes.get_portfolio_snapshot
        original_fixed_income = api_routes.get_fixed_income_payload_cached
        original_monthly_summary = api_routes.get_monthly_class_summary

        api_routes.get_portfolio_snapshot = lambda *args, **kwargs: {
            'positions': [],
            'grouped_positions': {
                'br_stocks': [],
                'us_stocks': [],
                'fiis': [],
                'crypto': [],
            },
            'group_totals': {
                'br_stocks': 0.0,
                'us_stocks': 0.0,
                'fiis': 0.0,
                'crypto': 0.0,
            },
            'group_summaries': {
                'us_stocks': {'open_pnl_value': 0.0},
                'fiis': {'open_pnl_value': 0.0},
                'br_stocks': {'open_pnl_value': 0.0},
                'crypto': {'open_pnl_value': 0.0},
            },
            'total_value': 0.0,
            'invested_value': 0.0,
            'total_incomes': 0.0,
        }
        api_routes.get_fixed_income_payload_cached = lambda *args, **kwargs: {
            'summary': {'current_total': 0.0},
            'items': [
                {
                    'investment_type': 'CRA',
                    'current_income': 9000.0,
                    'active_applied_value': 0.0,
                },
                {
                    'investment_type': 'DEBEN INCENTIVADA',
                    'current_income': 7000.0,
                    'active_applied_value': 0.0,
                },
            ],
        }
        api_routes.get_monthly_class_summary = lambda *args, **kwargs: []

        try:
            payload = api_routes._build_charts_core_payload([1])
            self.assertEqual(
                payload['result_by_category_chart']['values'],
                [0.0, 0.0, 0.0, 0.0, 16000.0],
            )
        finally:
            api_routes.get_portfolio_snapshot = original_snapshot
            api_routes.get_fixed_income_payload_cached = original_fixed_income
            api_routes.get_monthly_class_summary = original_monthly_summary


if __name__ == '__main__':
    unittest.main()

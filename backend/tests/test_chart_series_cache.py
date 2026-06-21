import os
import tempfile
import unittest
from pathlib import Path

from app import create_app
from app.auth import create_user_account
from app.db import get_db
from app.services import _legacy, portfolio


class ChartSeriesCacheTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.original_env = {
            key: os.environ.get(key)
            for key in (
                'DATABASE',
                'DATABASE_BACKUP_DIR',
                'AUTH_SECRET_KEY_FILE',
                'ADMIN_BOOTSTRAP_FILE',
                'BACKGROUND_JOBS_LOCK_FILE',
                'DATABASE_STARTUP_LOCK_FILE',
            )
        }
        os.environ['DATABASE'] = str(root / 'test_charts.db')
        os.environ['DATABASE_BACKUP_DIR'] = str(root / 'backups')
        os.environ['AUTH_SECRET_KEY_FILE'] = str(root / '.flask-secret')
        os.environ['ADMIN_BOOTSTRAP_FILE'] = str(root / 'admin-bootstrap.txt')
        os.environ['BACKGROUND_JOBS_LOCK_FILE'] = str(root / '.bg.lock')
        os.environ['DATABASE_STARTUP_LOCK_FILE'] = str(root / '.db.lock')
        self.app = create_app()

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmpdir.cleanup()

    def _seed(self):
        ok, _msg, user = create_user_account('charts_user', 'charts-pass-123', role='trader')
        self.assertTrue(ok)
        db = get_db()
        cur = db.execute(
            "INSERT INTO portfolios (name, user_id) VALUES ('Teste', ?)",
            (user['id'],),
        )
        pid = int(cur.lastrowid)
        db.execute(
            """
            INSERT INTO assets (ticker, name, sector, price)
            VALUES ('ITUB4', 'Itau', 'Bancos', 30.0)
            """
        )
        db.execute(
            """
            INSERT INTO transactions (portfolio_id, ticker, tx_type, shares, price, date)
            VALUES (?, 'ITUB4', 'buy', 100, 25.0, '2026-01-05')
            """,
            (pid,),
        )
        db.commit()
        return pid

    def test_series_use_db_cache_and_warm(self):
        with self.app.app_context():
            pid = self._seed()

            # cold call computes and persists into chart_series_cache
            first = _legacy.get_patrimony_open_pnl_by_type_series([pid], range_key='12m')
            self.assertIn('datasets', first)
            rows = get_db().execute(
                "SELECT cache_key FROM chart_series_cache"
            ).fetchall()
            keys = {row['cache_key'] for row in rows}
            self.assertIn(f'patrimony_open_pnl_by_type|{pid}|12m', keys)

            # second call returns the same payload (read-through path)
            second = _legacy.get_patrimony_open_pnl_by_type_series([pid], range_key='12m')
            self.assertEqual(first, second)

            # daily series also cached
            _legacy.get_variable_income_value_daily_series([pid], range_key='90d')
            keys2 = {
                row['cache_key']
                for row in get_db().execute(
                    "SELECT cache_key FROM chart_series_cache"
                ).fetchall()
            }
            self.assertIn(f'variable_income_value_daily|{pid}|90d', keys2)

            # rebuild warms the heavy series for each portfolio
            get_db().execute("DELETE FROM chart_series_cache")
            get_db().commit()
            portfolio.rebuild_chart_snapshots([pid])
            warmed = {
                row['cache_key']
                for row in get_db().execute(
                    "SELECT cache_key FROM chart_series_cache"
                ).fetchall()
            }
            self.assertIn(f'patrimony_open_pnl_by_type|{pid}|12m', warmed)
            self.assertIn(f'patrimony_open_pnl_by_type|{pid}|24m', warmed)
            self.assertIn(f'variable_income_value_daily|{pid}|90d', warmed)

    def test_invalidate_clears_series_cache(self):
        with self.app.app_context():
            pid = self._seed()
            _legacy.get_patrimony_open_pnl_by_type_series([pid], range_key='12m')
            self.assertTrue(
                get_db().execute("SELECT COUNT(*) c FROM chart_series_cache").fetchone()['c'] > 0
            )
            _legacy.invalidate_chart_snapshots([pid])
            self.assertEqual(
                get_db().execute("SELECT COUNT(*) c FROM chart_series_cache").fetchone()['c'], 0
            )


if __name__ == '__main__':
    unittest.main()

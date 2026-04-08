import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from app import create_app
from app.auth import create_user_account
from app.db import get_db
from app.services import portfolio


class FixedIncomeSnapshotProjectionVersionTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.db_path = root / "test_fixed_income_snapshot.db"
        self.backup_dir = root / "backups"
        self.secret_file = root / ".flask-secret"
        self.bootstrap_file = root / "admin-bootstrap.txt"
        self.bg_lock = root / ".background-jobs.lock"
        self.startup_lock = root / ".db-startup.lock"

        self.original_env = {
            "DATABASE": os.environ.get("DATABASE"),
            "DATABASE_BACKUP_DIR": os.environ.get("DATABASE_BACKUP_DIR"),
            "AUTH_SECRET_KEY_FILE": os.environ.get("AUTH_SECRET_KEY_FILE"),
            "ADMIN_BOOTSTRAP_FILE": os.environ.get("ADMIN_BOOTSTRAP_FILE"),
            "BACKGROUND_JOBS_LOCK_FILE": os.environ.get("BACKGROUND_JOBS_LOCK_FILE"),
            "DATABASE_STARTUP_LOCK_FILE": os.environ.get("DATABASE_STARTUP_LOCK_FILE"),
        }
        os.environ["DATABASE"] = str(self.db_path)
        os.environ["DATABASE_BACKUP_DIR"] = str(self.backup_dir)
        os.environ["AUTH_SECRET_KEY_FILE"] = str(self.secret_file)
        os.environ["ADMIN_BOOTSTRAP_FILE"] = str(self.bootstrap_file)
        os.environ["BACKGROUND_JOBS_LOCK_FILE"] = str(self.bg_lock)
        os.environ["DATABASE_STARTUP_LOCK_FILE"] = str(self.startup_lock)

        self.app = create_app()
        with self.app.app_context():
            ok, _, self.user = create_user_account("snapshot_user", "snapshot-pass-123", role="trader")
            self.assertTrue(ok)
            portfolios = portfolio.get_portfolios()
            self.assertTrue(portfolios)
            self.portfolio_id = int(portfolios[0]["id"])

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmpdir.cleanup()

    def test_cached_fixed_income_payload_recomputes_when_projection_version_missing(self):
        with self.app.app_context():
            db = get_db()

            # Insert one fixed income record.
            db.execute(
                """
                INSERT INTO fixed_incomes (
                  portfolio_id, distributor, issuer, investment_type,
                  rate_type, annual_rate, rate_fixed, rate_ipca, rate_cdi,
                  date_aporte, aporte, reinvested, maturity_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.portfolio_id,
                    "Banco Teste",
                    "Emissor Teste",
                    "CDB",
                    "FIXO",
                    10.0,
                    10.0,
                    0.0,
                    0.0,
                    "2025-01-01",
                    1000.0,
                    0.0,
                    "2027-01-01",
                ),
            )
            fixed_income_id = int(db.execute("SELECT MAX(id) AS id FROM fixed_incomes").fetchone()["id"])

            # Create a fresh snapshot entry without projection_version to simulate legacy cache.
            now = datetime.now().isoformat(timespec="seconds")
            db.execute(
                """
                                INSERT INTO fixed_income_snapshot_summary (portfolio_id, payload_json, updated_at)
                                VALUES (?, ?, ?)
                                ON CONFLICT(portfolio_id) DO UPDATE SET
                                    payload_json = excluded.payload_json,
                                    updated_at = excluded.updated_at
                """,
                (self.portfolio_id, "{}", now),
            )
            db.execute(
                """
                INSERT INTO fixed_income_snapshot_items (portfolio_id, fixed_income_id, payload_json, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    self.portfolio_id,
                    fixed_income_id,
                    '{"investment_type":"CDB","rate_type":"FIXO","annual_rate":10.0}',
                    now,
                ),
            )
            db.commit()

            payload = portfolio.get_fixed_income_payload_cached([self.portfolio_id])
            self.assertFalse(bool(payload.get("snapshot")), "Expected recomputation when projection_version is missing")
            items = payload.get("items") or []
            self.assertEqual(len(items), 1)
            expected_version = int(getattr(portfolio.legacy, "FIXED_INCOME_PROJECTION_VERSION", 1))
            self.assertEqual(int(items[0].get("projection_version") or 0), expected_version)
            self.assertIn("open_pnl_value", items[0])


if __name__ == "__main__":
    unittest.main()

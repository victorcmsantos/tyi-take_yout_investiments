import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from app import create_app
from app import api_routes
from app.auth import create_user_account
from app.db import get_db


def _build_scanner_db(path: Path):
    connection = sqlite3.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ticker TEXT,
              timestamp TEXT,
              signal_type TEXT,
              price REAL,
              score REAL,
              metrics_triggered TEXT,
              created_at TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ticker TEXT,
              status TEXT,
              opened_at TEXT,
              closed_at TEXT,
              last_checked_at TEXT
            )
            """
        )
        connection.execute(
            """
            INSERT INTO signals (ticker, timestamp, signal_type, price, score, metrics_triggered, created_at)
            VALUES ('PETR4.SA', '2026-03-08T10:00:00+00:00', 'BUY', 35.2, 72.1, '[]', '2026-03-08T10:00:00+00:00')
            """
        )
        connection.execute(
            """
            INSERT INTO trades (ticker, status, opened_at, closed_at, last_checked_at)
            VALUES ('VALE3.SA', 'OPEN', '2026-03-08T11:00:00+00:00', NULL, '2026-03-08T12:15:00+00:00')
            """
        )
        connection.commit()
    finally:
        connection.close()


class ScannerAdminObservabilityTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.db_path = root / "test_scanner_admin.db"
        self.backup_dir = root / "backups"
        self.secret_file = root / ".flask-secret"
        self.bootstrap_file = root / "admin-bootstrap.txt"
        self.bg_lock = root / ".background-jobs.lock"
        self.startup_lock = root / ".db-startup.lock"
        self.scanner_db_path = root / "market_scanner.db"
        _build_scanner_db(self.scanner_db_path)

        self.original_env = {
            "DATABASE": os.environ.get("DATABASE"),
            "DATABASE_BACKUP_DIR": os.environ.get("DATABASE_BACKUP_DIR"),
            "AUTH_SECRET_KEY_FILE": os.environ.get("AUTH_SECRET_KEY_FILE"),
            "ADMIN_BOOTSTRAP_FILE": os.environ.get("ADMIN_BOOTSTRAP_FILE"),
            "BACKGROUND_JOBS_LOCK_FILE": os.environ.get("BACKGROUND_JOBS_LOCK_FILE"),
            "DATABASE_STARTUP_LOCK_FILE": os.environ.get("DATABASE_STARTUP_LOCK_FILE"),
            "MARKET_SCANNER_DATABASE_PATH": os.environ.get("MARKET_SCANNER_DATABASE_PATH"),
        }
        os.environ["DATABASE"] = str(self.db_path)
        os.environ["DATABASE_BACKUP_DIR"] = str(self.backup_dir)
        os.environ["AUTH_SECRET_KEY_FILE"] = str(self.secret_file)
        os.environ["ADMIN_BOOTSTRAP_FILE"] = str(self.bootstrap_file)
        os.environ["BACKGROUND_JOBS_LOCK_FILE"] = str(self.bg_lock)
        os.environ["DATABASE_STARTUP_LOCK_FILE"] = str(self.startup_lock)
        os.environ["MARKET_SCANNER_DATABASE_PATH"] = str(self.scanner_db_path)

        self.app = create_app()
        with self.app.app_context():
            ok, _, user = create_user_account("audit_tester", "audit-pass-123", role="trader")
            self.assertTrue(ok)
            self.audit_user = user

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmpdir.cleanup()

    def test_scanner_status_payload_reports_db_metrics(self):
        with self.app.app_context():
            payload = api_routes._scanner_db_status_payload()
        self.assertTrue(bool(payload.get("exists")))
        self.assertTrue(bool(payload.get("db_accessible")))
        self.assertGreater(int(payload.get("size_bytes") or 0), 0)
        self.assertGreaterEqual(int(payload.get("table_count") or 0), 2)
        self.assertTrue(bool(payload.get("last_data_update_at")))

    def test_scanner_trade_audit_is_recorded_and_exposed(self):
        with self.app.app_context():
            api_routes._log_scanner_trade_audit(
                action="create",
                user=self.audit_user,
                trade_id=77,
                ticker="PETR4.SA",
                request_payload={"ticker": "PETR4.SA", "quantity": 1},
                response_payload={"id": 77, "ok": True},
                success=True,
                upstream_status=201,
                error_message="",
            )
            row = get_db().execute(
                "SELECT action, username, trade_id, ticker, success, upstream_status FROM scanner_trade_audit ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["action"]), "create")
            self.assertEqual(str(row["username"]), "audit_tester")
            self.assertEqual(int(row["trade_id"]), 77)
            self.assertEqual(str(row["ticker"]), "PETR4.SA")
            self.assertEqual(int(row["success"]), 1)
            self.assertEqual(int(row["upstream_status"]), 201)

        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["user_id"] = 1  # admin bootstrap
            response = client.get("/api/admin/scanner/audit?limit=10")
            self.assertEqual(response.status_code, 200)
            data = response.get_json(silent=True) or {}
            payload = data.get("data") or {}
            items = payload.get("items") or []
            self.assertGreaterEqual(len(items), 1)
            self.assertEqual(items[0].get("action"), "create")


if __name__ == "__main__":
    unittest.main()

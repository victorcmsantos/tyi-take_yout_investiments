import os
import tempfile
import unittest
from pathlib import Path

from app import create_app
from app.auth import create_user_account, set_user_role
from app.db import get_db


class ViewerRoleAccessTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.db_path = root / "test_auth_roles.db"
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
            ok, _, viewer = create_user_account("viewer_user", "viewer-pass-123", role="viewer")
            self.assertTrue(ok)
            ok, _, trader = create_user_account("trader_user", "trader-pass-123", role="trader")
            self.assertTrue(ok)
            self.viewer_id = int(viewer["id"])
            self.trader_id = int(trader["id"])

    def tearDown(self):
        for key, value in self.original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmpdir.cleanup()

    def test_viewer_can_read_but_cannot_write(self):
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["user_id"] = self.viewer_id

            read_response = client.get("/api/portfolios")
            self.assertEqual(read_response.status_code, 200)

            write_response = client.post("/api/portfolios", json={"name": "Viewer Attempt"})
            self.assertEqual(write_response.status_code, 403)
            body = write_response.get_json(silent=True) or {}
            self.assertIn("somente leitura", str(body.get("error", "")).lower())

    def test_trader_can_write(self):
        with self.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["user_id"] = self.trader_id

            write_response = client.post("/api/portfolios", json={"name": "Carteira Trader"})
            self.assertEqual(write_response.status_code, 201)
            body = write_response.get_json(silent=True) or {}
            self.assertTrue(bool(body.get("ok")))

    def test_role_is_persisted_for_new_users(self):
        with self.app.app_context():
            row = get_db().execute(
                "SELECT username, role, is_admin FROM users WHERE username = ?",
                ("viewer_user",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(str(row["role"]), "viewer")
            self.assertEqual(int(row["is_admin"]), 0)

    def test_set_user_role_updates_admin_flag(self):
        with self.app.app_context():
            ok, _, updated = set_user_role(self.viewer_id, "admin")
            self.assertTrue(ok)
            self.assertEqual(str(updated["role"]), "admin")
            self.assertTrue(bool(updated["is_admin"]))


if __name__ == "__main__":
    unittest.main()

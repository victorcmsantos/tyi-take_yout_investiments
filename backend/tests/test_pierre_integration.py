import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app import create_app
from app.auth import create_user_account


class _FakeResponse:
    def __init__(self, body, status=200):
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.status = status

    def read(self):
        return self._body


@contextmanager
def _fake_urlopen(body, status=200):
    def _open(req, timeout=None):
        @contextmanager
        def _cm():
            yield _FakeResponse(body, status)
        return _cm()

    with patch("app.pierre_routes.urllib.request.urlopen", side_effect=_open):
        yield


class PierreProxyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.orig = {k: os.environ.get(k) for k in (
            "DATABASE", "DATABASE_BACKUP_DIR", "AUTH_SECRET_KEY_FILE",
            "ADMIN_BOOTSTRAP_FILE", "BACKGROUND_JOBS_LOCK_FILE",
            "DATABASE_STARTUP_LOCK_FILE",
        )}
        os.environ["DATABASE"] = str(root / "t.db")
        os.environ["DATABASE_BACKUP_DIR"] = str(root / "b")
        os.environ["AUTH_SECRET_KEY_FILE"] = str(root / ".s")
        os.environ["ADMIN_BOOTSTRAP_FILE"] = str(root / ".a")
        os.environ["BACKGROUND_JOBS_LOCK_FILE"] = str(root / ".bg")
        os.environ["DATABASE_STARTUP_LOCK_FILE"] = str(root / ".db")
        self.app = create_app()
        with self.app.app_context():
            create_user_account("pierre_user", "pierre-pass-123", role="trader")
        self.client = self.app.test_client()

    def tearDown(self):
        for k, v in self.orig.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self.tmp.cleanup()

    def _login(self):
        self.client.post("/api/auth/login", json={"username": "pierre_user", "password": "pierre-pass-123"})

    def test_requires_auth(self):
        self.assertEqual(self.client.get("/api/pierre/status").status_code, 401)

    def test_proxies_service_response(self):
        self._login()
        upstream = {"ok": True, "data": {"configured": True}}
        with _fake_urlopen(upstream):
            resp = self.client.get("/api/pierre/status")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(json.loads(resp.data), upstream)

    def test_forwards_query_and_relays_accounts(self):
        self._login()
        upstream = {"ok": True, "data": [{"id": "1", "type": "BANK"}]}
        with _fake_urlopen(upstream):
            resp = self.client.get("/api/pierre/transactions?startDate=2026-06-01&accountType=BANK")
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(json.loads(resp.data)["ok"])

    def test_unknown_endpoint_404(self):
        self._login()
        self.assertEqual(self.client.get("/api/pierre/wipe-everything").status_code, 404)

    def test_service_unreachable_returns_502(self):
        self._login()
        with patch("app.pierre_routes.urllib.request.urlopen", side_effect=OSError("no route")):
            resp = self.client.get("/api/pierre/accounts")
        self.assertEqual(resp.status_code, 502)
        self.assertFalse(json.loads(resp.data)["ok"])


if __name__ == "__main__":
    unittest.main()

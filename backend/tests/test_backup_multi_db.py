import sqlite3
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from app import db


def _init_sqlite_file(path: Path):
    connection = sqlite3.connect(str(path))
    try:
        connection.execute("CREATE TABLE IF NOT EXISTS sample (id INTEGER PRIMARY KEY, name TEXT)")
        connection.execute("INSERT INTO sample (name) VALUES (?)", (path.stem,))
        connection.commit()
    finally:
        connection.close()


class MultiDatabaseBackupTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        root = Path(self.tmpdir.name)
        self.backend_db_path = root / "investments.db"
        self.scanner_db_path = root / "market_scanner.db"
        self.backup_dir = root / "backups"
        _init_sqlite_file(self.backend_db_path)
        _init_sqlite_file(self.scanner_db_path)

        self.app = Flask(__name__)
        self.app.config["DATABASE"] = str(self.backend_db_path)
        self.app.config["DATABASE_BACKUP_DIR"] = str(self.backup_dir)
        self.app.config["DATABASE_BACKUP_MAX_FILES"] = 5
        self.app.config["MARKET_SCANNER_DATABASE_PATH"] = str(self.scanner_db_path)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_create_database_backups_covers_backend_and_scanner(self):
        with self.app.app_context():
            result = db.create_database_backups(reason="test")

        backups = result.get("backups") or []
        keys = {item.get("database_key") for item in backups}
        self.assertEqual(keys, {"backend", "market_scanner"})
        self.assertFalse(bool(result.get("partial")))
        for item in backups:
            self.assertTrue(Path(item["path"]).is_file())

    def test_list_and_resolve_backups_from_both_sources(self):
        with self.app.app_context():
            db.create_database_backups(reason="test")
            listed = db.list_database_backups()
            keys = {item.get("database_key") for item in listed}
            self.assertEqual(keys, {"backend", "market_scanner"})

            first = listed[0]
            resolved = db.resolve_database_backup_path(first["filename"])
            self.assertIsNotNone(resolved)
            self.assertTrue(resolved.is_file())

    def test_missing_optional_scanner_db_keeps_backend_backup(self):
        with self.app.app_context():
            self.app.config["MARKET_SCANNER_DATABASE_PATH"] = str(
                Path(self.tmpdir.name) / "missing_scanner.db"
            )
            result = db.create_database_backups(reason="test")

        backups = result.get("backups") or []
        failures = result.get("failures") or []
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].get("database_key"), "backend")
        self.assertTrue(bool(result.get("partial")))
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].get("database_key"), "market_scanner")


if __name__ == "__main__":
    unittest.main()

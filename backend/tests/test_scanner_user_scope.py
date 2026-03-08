import unittest

from app.api_routes import (
    _scanner_filter_trades_payload_for_user,
    _scanner_note_with_user,
    _scanner_prepare_trade_payload,
    _scanner_strip_user_marker,
    _scanner_trade_visible_for_user,
    _scanner_user_id_from_notes,
)


class ScannerUserScopeTest(unittest.TestCase):
    def test_user_marker_round_trip(self):
        marked = _scanner_note_with_user("Entrada por setup", 7)
        self.assertEqual(_scanner_user_id_from_notes(marked), 7)
        self.assertEqual(_scanner_strip_user_marker(marked), "Entrada por setup")

    def test_prepare_trade_payload_enforces_owner(self):
        user = {"id": 3, "username": "alice", "is_admin": False}
        payload = _scanner_prepare_trade_payload({"ticker": "PETR4", "notes": "teste"}, user, force_notes=True)
        self.assertEqual(payload["user_id"], 3)
        self.assertEqual(_scanner_user_id_from_notes(payload["notes"]), 3)
        self.assertEqual(_scanner_strip_user_marker(payload["notes"]), "teste")

    def test_trade_visibility(self):
        user = {"id": 2, "is_admin": False}
        admin = {"id": 1, "is_admin": True}
        own_trade = {"notes": _scanner_note_with_user("x", 2)}
        other_trade = {"notes": _scanner_note_with_user("x", 9)}
        legacy_trade = {"notes": "sem marcador"}

        self.assertTrue(_scanner_trade_visible_for_user(own_trade, user))
        self.assertFalse(_scanner_trade_visible_for_user(other_trade, user))
        self.assertFalse(_scanner_trade_visible_for_user(legacy_trade, user))
        self.assertTrue(_scanner_trade_visible_for_user(legacy_trade, admin))

    def test_filter_trades_payload_by_user(self):
        user = {"id": 2, "is_admin": False}
        payload = {
            "active": [
                {
                    "id": 10,
                    "ticker": "PETR4",
                    "status": "OPEN",
                    "invested_amount": 1000,
                    "current_pnl_amount": 25,
                    "notes": _scanner_note_with_user("minha", 2),
                },
                {
                    "id": 11,
                    "ticker": "VALE3",
                    "status": "OPEN",
                    "invested_amount": 1500,
                    "current_pnl_amount": -10,
                    "notes": _scanner_note_with_user("outra", 8),
                },
            ],
            "history": [
                {
                    "id": 12,
                    "ticker": "BBAS3",
                    "status": "TARGET_HIT",
                    "invested_amount": 500,
                    "current_pnl_amount": 0,
                    "notes": _scanner_note_with_user("fechada", 2),
                }
            ],
        }
        filtered = _scanner_filter_trades_payload_for_user(payload, user)
        self.assertEqual(len(filtered["active"]), 1)
        self.assertEqual(len(filtered["history"]), 1)
        self.assertEqual(filtered["tracked_tickers"], ["PETR4"])
        self.assertEqual(filtered["summary"]["tracked_count"], 1)
        self.assertEqual(filtered["summary"]["history_count"], 1)
        self.assertEqual(filtered["summary"]["open"], 1)
        self.assertEqual(filtered["summary"]["success"], 1)
        self.assertEqual(filtered["summary"]["open_invested_amount"], 1000.0)
        self.assertEqual(filtered["active"][0]["notes"], "minha")


if __name__ == "__main__":
    unittest.main()

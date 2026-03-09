import unittest

from app import api_routes


class ScannerLiveOverlayTest(unittest.TestCase):
    def setUp(self):
        self._original_fetch_batch = api_routes.legacy_market._fetch_brapi_quote_results_batch

    def tearDown(self):
        api_routes.legacy_market._fetch_brapi_quote_results_batch = self._original_fetch_batch

    def test_apply_live_quote_overrides_on_signal_rows(self):
        def _fake_fetch_batch(symbols, range_key=None, interval=None, modules=None):
            self.assertIn("PETR4", symbols)
            return {
                "PETR4": {
                    "regularMarketPrice": 43.84,
                    "regularMarketTime": "2026-03-09T14:32:28.000Z",
                }
            }

        api_routes.legacy_market._fetch_brapi_quote_results_batch = _fake_fetch_batch
        rows = [
            {
                "ticker": "PETR4.SA",
                "price": 42.11,
                "timestamp": "2026-03-06T03:00:00",
            },
            {
                "ticker": "AAPL",
                "price": 210.55,
                "timestamp": "2026-03-09T14:30:00.000Z",
            },
        ]
        patched = api_routes._scanner_apply_live_quote_overrides(rows)

        self.assertEqual(patched[0]["price"], 43.84)
        self.assertEqual(patched[0]["timestamp"], "2026-03-09T14:32:28.000Z")
        self.assertEqual(patched[1]["price"], 210.55)
        self.assertEqual(patched[1]["timestamp"], "2026-03-09T14:30:00.000Z")

    def test_apply_live_quote_overrides_to_ticker_payload(self):
        api_routes.legacy_market._fetch_brapi_quote_results_batch = (
            lambda symbols, range_key=None, interval=None, modules=None: {
                "PETR4": {
                    "regularMarketPrice": 44.01,
                    "regularMarketTime": "2026-03-09T14:35:00.000Z",
                }
            }
        )
        payload = {
            "ticker": "PETR4.SA",
            "latest_price": 42.11,
            "latest_price_timestamp": "2026-03-06T03:00:00",
            "latest_signal": {
                "timestamp": "2026-03-06T03:00:00",
                "price": 42.11,
                "score": 43.0,
            },
        }

        patched = api_routes._scanner_apply_live_quote_overrides_to_ticker_payload(payload)

        self.assertEqual(patched["latest_price"], 44.01)
        self.assertEqual(patched["latest_price_timestamp"], "2026-03-09T14:35:00.000Z")
        self.assertEqual(patched["latest_signal"]["price"], 44.01)
        self.assertEqual(patched["latest_signal"]["timestamp"], "2026-03-09T14:35:00.000Z")


if __name__ == "__main__":
    unittest.main()

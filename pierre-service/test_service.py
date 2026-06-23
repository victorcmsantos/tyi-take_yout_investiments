import os
import tempfile
import unittest
from unittest.mock import patch

import cashflow
import pierre

SAMPLE_TX = {
    "success": True,
    "data": {
        "type": "structured",
        "transactions": {
            "accounts": {
                "Bank": {
                    "received": [
                        {"date": "2026-06-01", "category": "Salário", "amount": 30000.0, "type": "CREDIT"},
                        {"date": "2026-06-02", "category": "Transferência mesma titularidade", "amount": 4000.0, "type": "CREDIT"},
                    ],
                    "bank_transfer": [
                        {"date": "2026-06-03", "category": "Seguros", "amount": 1000.0, "type": "DEBIT"},
                        {"date": "2026-06-04", "category": "Transferência - PIX", "amount": 2000.0, "type": "DEBIT"},
                        {"date": "2026-06-05", "category": "Pagamento de cartão de crédito", "amount": 8000.0, "type": "DEBIT"},
                        {"date": "2026-06-06", "category": "Transferência mesma titularidade", "amount": 500.0, "type": "DEBIT"},
                    ],
                    "credit_cards": {},
                },
                "Card": {
                    "received": [],
                    "bank_transfer": [],
                    "credit_cards": {
                        "Visa": {
                            "purchases": [
                                {"date": "2026-06-07", "category": "Supermercado", "amount": 1500.0, "type": "DEBIT"},
                                {"date": "2026-06-08", "category": "Restaurantes", "amount": 500.0, "type": "DEBIT"},
                            ],
                            "payments": [
                                {"date": "2026-06-09", "category": "Pagamento de cartão de crédito", "amount": 8000.0},
                            ],
                        }
                    },
                },
            }
        },
        "summary": {"period": "1 mes", "total_received": 30000.0, "total_spent": 12000.0, "total_bank_transfer": 10500.0},
    },
}


class CashflowTest(unittest.TestCase):
    def test_buckets_and_sobra(self):
        out = cashflow.summarize_cashflow(SAMPLE_TX)
        b = out["buckets"]
        self.assertEqual(b["receita"], 30000.0)       # salário; internal transfer excluded
        self.assertEqual(b["cartao"], 2000.0)         # card purchases (1500 + 500)
        self.assertEqual(b["despfixa"], 1000.0)       # Seguros
        self.assertEqual(b["despavulsa"], 2000.0)     # PIX sent
        # card settlement + internal transfers excluded from expenses
        self.assertEqual(b["sobra"], 30000.0 - 2000.0 - 1000.0 - 2000.0)
        buckets_used = {c["bucket"] for c in out["categories"]}
        self.assertEqual(buckets_used, {"receita", "cartao", "fixa", "avulsa"})


class CacheTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._orig = pierre.CACHE_PATH
        pierre.CACHE_PATH = os.path.join(self.tmp.name, "cache.db")
        os.environ["PIERRE_API_KEY"] = "sk-test"

    def tearDown(self):
        pierre.CACHE_PATH = self._orig
        os.environ.pop("PIERRE_API_KEY", None)
        self.tmp.cleanup()

    def test_read_through_and_stale_fallback(self):
        payload = {"success": True, "data": [1, 2, 3]}
        with patch.object(pierre, "_live_get", return_value=payload) as live:
            self.assertEqual(pierre.get_accounts(), payload)
            self.assertEqual(pierre.get_accounts(), payload)  # served from cache
            self.assertEqual(live.call_count, 1)
        # live fails but cache exists -> stale served
        with patch.object(pierre, "_live_get", side_effect=RuntimeError("502")):
            self.assertEqual(pierre.get_accounts(), payload)
        # different key with no cache + failure -> raises
        with patch.object(pierre, "_live_get", side_effect=RuntimeError("502")):
            with self.assertRaises(pierre.PierreError):
                pierre.get_balance()


if __name__ == "__main__":
    unittest.main()

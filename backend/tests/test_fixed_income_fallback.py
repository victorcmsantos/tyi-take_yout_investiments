import unittest
from datetime import date
from unittest.mock import patch

from app.services import _legacy


def _project(item, ref):
    # Force the BCB series to be unavailable so the fallback path is exercised.
    with patch.object(_legacy, "_compound_from_bcb_series", return_value=(1.0, False)):
        return _legacy._fixed_income_projection_at_date(item, ref)


class FixedIncomeFallbackTest(unittest.TestCase):
    REF = date(2026, 6, 21)

    def _item(self, **over):
        base = {
            "date_aporte": "2024-01-16",
            "maturity_date": "2028-12-22",
            "aporte": 33000.0,
            "reinvested": 0.0,
            "annual_rate": 113.0,
            "rate_fixed": 0.0,
            "rate_ipca": 0.0,
            "rate_cdi": 113.0,
            "rate_type": "CDI",
        }
        base.update(over)
        return base

    def test_cdi_fallback_is_percent_of_index_not_absolute(self):
        # "113% do CDI" must not be treated as 113% a.a. (which gave ~x6.3).
        proj = _project(self._item(), self.REF)
        mult = proj["current_gross_value"] / proj["applied_value"]
        # ~2.4y at 1.13 * 10.5% a.a. -> ~1.30x, definitely well under 2x.
        self.assertGreater(mult, 1.05)
        self.assertLess(mult, 1.6)

    def test_ipca_fallback_is_percent_of_index_not_absolute(self):
        # FIXO 5.5% + 100% do IPCA must not explode to ~x37.
        item = self._item(
            date_aporte="2021-08-15",
            maturity_date="2026-12-15",
            aporte=3370.26,
            annual_rate=105.5,
            rate_fixed=5.5,
            rate_ipca=100.0,
            rate_cdi=0.0,
            rate_type="FIXO+IPCA",
        )
        proj = _project(item, self.REF)
        mult = proj["current_gross_value"] / proj["applied_value"]
        # fixed ~1.055^4.85 * ipca ~1.045^4.85 -> ~1.6x, far from the x37 bug.
        self.assertGreater(mult, 1.2)
        self.assertLess(mult, 2.2)


if __name__ == "__main__":
    unittest.main()

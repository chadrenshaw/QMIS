import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISPersistenceFilterTests(unittest.TestCase):
    def test_build_persistence_metadata_distinguishes_transient_emerging_and_persistent(self) -> None:
        from qmis.signals.persistence import build_persistence_metadata

        transient = build_persistence_metadata(observed_windows=[30], family="divergences")
        persistent = build_persistence_metadata(observed_windows=[30, 90], family="divergences")
        entrenched = build_persistence_metadata(observed_windows=[30, 90, 365], family="factors")

        self.assertEqual(transient["persistence_windows"], 1)
        self.assertEqual(transient["persistence_label"], "transient")
        self.assertFalse(transient["passes_filter"])
        self.assertEqual(persistent["required_windows"], 2)
        self.assertEqual(persistent["persistence_label"], "persistent")
        self.assertTrue(persistent["passes_filter"])
        self.assertEqual(entrenched["persistence_label"], "entrenched")
        self.assertTrue(entrenched["passes_filter"])

    def test_annotate_factor_persistence_marks_nonpersistent_factors_without_dropping_them(self) -> None:
        from qmis.signals.persistence import annotate_factor_persistence

        ts = pd.Timestamp("2026-03-08")
        factors = [
            {
                "ts": ts,
                "factor_name": "liquidity",
                "component_rank": 1,
                "strength": 0.84,
                "direction": "tightening",
                "summary": "Liquidity tightening",
                "supporting_assets": ["fed_balance_sheet", "yield_3m", "reverse_repo_usage"],
            },
            {
                "ts": ts,
                "factor_name": "volatility",
                "component_rank": 2,
                "strength": 0.43,
                "direction": "stressed",
                "summary": "Volatility pressure",
                "supporting_assets": ["vix", "sp500_above_200dma", "new_lows"],
            },
        ]
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": -2.0, "pct_change_90d": -3.0, "pct_change_365d": -5.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "yield_3m", "pct_change_30d": 0.3, "pct_change_90d": 0.5, "pct_change_365d": 1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": 5.0, "pct_change_90d": 8.0, "pct_change_365d": 10.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "vix", "pct_change_30d": 3.0, "pct_change_90d": -1.0, "pct_change_365d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": -2.0, "pct_change_90d": 1.0, "pct_change_365d": 2.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": 6.0, "pct_change_90d": -4.0, "pct_change_365d": -7.0, "trend_label": "UP"},
            ]
        )

        annotated = annotate_factor_persistence(factors, features)

        self.assertEqual(len(annotated), 2)
        self.assertTrue(annotated[0]["passes_filter"])
        self.assertEqual(annotated[0]["persistence_label"], "entrenched")
        self.assertFalse(annotated[1]["passes_filter"])
        self.assertEqual(annotated[1]["persistence_label"], "transient")


if __name__ == "__main__":
    unittest.main()

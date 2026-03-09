import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISScoringTests(unittest.TestCase):
    def _build_latest_feature_frame(self) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-08")
        rows = [
            ("gold", "UP"),
            ("oil", "UP"),
            ("yield_10y", "UP"),
            ("copper", "UP"),
            ("sp500", "UP"),
            ("pmi", "UP"),
            ("fed_balance_sheet", "UP"),
            ("m2_money_supply", "UP"),
            ("reverse_repo_usage", "DOWN"),
            ("dollar_index", "DOWN"),
            ("vix", "SIDEWAYS"),
        ]
        return pd.DataFrame(
            {
                "ts": [ts] * len(rows),
                "series_name": [row[0] for row in rows],
                "trend_label": [row[1] for row in rows],
            }
        )

    def test_compute_macro_scores_uses_feature_trends_and_yield_curve(self) -> None:
        from qmis.signals.scoring import compute_macro_scores

        feature_frame = self._build_latest_feature_frame()
        signal_snapshot = {"yield_10y": 4.2, "yield_3m": 3.8}

        scores = compute_macro_scores(feature_frame, signal_snapshot)

        self.assertEqual(scores["inflation_score"], 3)
        self.assertEqual(scores["growth_score"], 3)
        self.assertEqual(scores["liquidity_score"], 4)
        self.assertEqual(scores["risk_score"], 0)
        self.assertAlmostEqual(scores["yield_curve"], 0.4, places=6)
        self.assertEqual(scores["yield_curve_state"], "NORMAL")


if __name__ == "__main__":
    unittest.main()

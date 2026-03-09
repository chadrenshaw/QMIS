import math
import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISTrendFeatureTests(unittest.TestCase):
    def test_compute_percent_change_windows(self) -> None:
        from qmis.features.trends import compute_percent_change_windows

        series = pd.Series(
            [100.0, 102.0, 105.0, 110.0, 120.0],
            index=pd.date_range("2026-03-01", periods=5, freq="D"),
        )

        result = compute_percent_change_windows(series, windows=(1, 3))

        self.assertAlmostEqual(result.loc[result.index[-1], "pct_change_1d"], 9.0909090909, places=6)
        self.assertAlmostEqual(result.loc[result.index[-1], "pct_change_3d"], 17.6470588235, places=6)

    def test_compute_rolling_slope_and_trend_label(self) -> None:
        from qmis.features.trends import classify_trend, compute_rolling_slope

        series = pd.Series(
            [10.0, 11.0, 12.0, 13.0, 14.0],
            index=pd.date_range("2026-03-01", periods=5, freq="D"),
        )

        slope = compute_rolling_slope(series, window=5)
        self.assertAlmostEqual(float(slope.iloc[-1]), 1.0, places=6)
        self.assertEqual(classify_trend(6.5), "UP")
        self.assertEqual(classify_trend(-5.5), "DOWN")
        self.assertEqual(classify_trend(2.0), "SIDEWAYS")


if __name__ == "__main__":
    unittest.main()

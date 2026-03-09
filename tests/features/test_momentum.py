import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISMomentumFeatureTests(unittest.TestCase):
    def test_compute_moving_average(self) -> None:
        from qmis.features.momentum import compute_moving_average

        series = pd.Series(
            [10.0, 20.0, 30.0, 40.0],
            index=pd.date_range("2026-03-01", periods=4, freq="D"),
        )

        moving_average = compute_moving_average(series, window=3)

        self.assertTrue(pd.isna(moving_average.iloc[1]))
        self.assertAlmostEqual(float(moving_average.iloc[-1]), 30.0, places=6)


if __name__ == "__main__":
    unittest.main()

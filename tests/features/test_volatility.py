import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISVolatilityFeatureTests(unittest.TestCase):
    def test_compute_rolling_zscore_volatility_and_drawdown(self) -> None:
        from qmis.features.volatility import (
            compute_drawdown,
            compute_rolling_volatility,
            compute_rolling_zscore,
        )

        series = pd.Series(
            [100.0, 105.0, 110.0, 90.0, 95.0],
            index=pd.date_range("2026-03-01", periods=5, freq="D"),
        )

        zscore = compute_rolling_zscore(series, window=3)
        volatility = compute_rolling_volatility(series, window=3)
        drawdown = compute_drawdown(series, window=3)

        self.assertAlmostEqual(float(zscore.iloc[2]), 1.0, places=6)
        self.assertAlmostEqual(float(volatility.iloc[-1]), 0.1348151337, places=6)
        self.assertAlmostEqual(float(drawdown.iloc[3]), -18.1818181818, places=6)


if __name__ == "__main__":
    unittest.main()

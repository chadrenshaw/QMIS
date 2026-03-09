import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISCycleTests(unittest.TestCase):
    def test_detect_dominant_cycles_matches_lunar_like_period(self) -> None:
        from qmis.signals.cycles import detect_dominant_cycles

        dates = pd.date_range("2024-01-01", periods=365, freq="D")
        signal = np.sin(2 * np.pi * np.arange(len(dates)) / 29.53)
        frame = pd.DataFrame(
            {
                "ts": dates,
                "series_name": ["btc"] * len(dates),
                "value": signal,
            }
        )

        cycles = detect_dominant_cycles(frame, top_n=1)

        self.assertEqual(len(cycles), 1)
        row = cycles.iloc[0].to_dict()
        self.assertEqual(row["series_name"], "btc")
        self.assertAlmostEqual(row["period_days"], 29.53, delta=1.5)
        self.assertEqual(row["matched_cycle"], "lunar_period")
        self.assertIn(row["confidence_label"], {"statistically_significant", "validated"})


if __name__ == "__main__":
    unittest.main()

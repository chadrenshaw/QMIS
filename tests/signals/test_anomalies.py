import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISAnomalyTests(unittest.TestCase):
    def test_detect_relationship_anomalies_flags_broken_history(self) -> None:
        from qmis.signals.anomalies import detect_relationship_anomalies

        relationships = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-03-08"),
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": -0.82,
                    "p_value": 0.0001,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": pd.Timestamp("2026-03-08"),
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.05,
                    "p_value": 0.62,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
            ]
        )

        anomalies = detect_relationship_anomalies(relationships)

        self.assertEqual(len(anomalies), 1)
        row = anomalies.iloc[0].to_dict()
        self.assertEqual(row["series_x"], "gold")
        self.assertEqual(row["series_y"], "yield_10y")
        self.assertEqual(row["anomaly_type"], "relationship_break")
        self.assertEqual(row["current_state"], "broken")
        self.assertEqual(row["historical_state"], "stable")


if __name__ == "__main__":
    unittest.main()

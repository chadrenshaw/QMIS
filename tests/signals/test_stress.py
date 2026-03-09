import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISStressTests(unittest.TestCase):
    def _signals(self, include_credit: bool = False) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        rows = [
            {"ts": ts, "source": "test", "category": "market", "series_name": "vix", "value": 31.5, "unit": "index_points", "metadata": "{}"},
            {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.05, "unit": "percent", "metadata": "{}"},
            {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 4.85, "unit": "percent", "metadata": "{}"},
            {"ts": ts, "source": "test", "category": "breadth", "series_name": "sp500_above_200dma", "value": 43.0, "unit": "percent", "metadata": "{}"},
            {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_lows", "value": 110.0, "unit": "count", "metadata": "{}"},
        ]
        if include_credit:
            rows.append({"ts": ts, "source": "test", "category": "market", "series_name": "HYG", "value": 71.0, "unit": "usd", "metadata": "{}"})
        return pd.DataFrame(rows)

    def _features(self, include_credit: bool = False) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        rows = [
            {"ts": ts, "series_name": "vix", "pct_change_30d": 28.0, "pct_change_90d": 22.0, "pct_change_365d": 12.0, "zscore_30d": 2.1, "volatility_30d": 0.5, "slope_30d": 0.4, "drawdown_90d": -3.0, "trend_label": "UP"},
            {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": -18.0, "pct_change_90d": -22.0, "pct_change_365d": -10.0, "zscore_30d": -1.9, "volatility_30d": 0.3, "slope_30d": -0.6, "drawdown_90d": -14.0, "trend_label": "DOWN"},
            {"ts": ts, "series_name": "new_lows", "pct_change_30d": 52.0, "pct_change_90d": 64.0, "pct_change_365d": 70.0, "zscore_30d": 2.0, "volatility_30d": 0.6, "slope_30d": 0.7, "drawdown_90d": -1.0, "trend_label": "UP"},
        ]
        if include_credit:
            rows.append({"ts": ts, "series_name": "HYG", "pct_change_30d": -8.0, "pct_change_90d": -10.0, "pct_change_365d": -5.0, "zscore_30d": -1.5, "volatility_30d": 0.4, "slope_30d": -0.4, "drawdown_90d": -9.0, "trend_label": "DOWN"})
        return pd.DataFrame(rows)

    def _relationships(self) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        return pd.DataFrame(
            [
                {"ts": ts, "series_x": "gold", "series_y": "yield_10y", "window_days": 365, "lag_days": 0, "correlation": -0.82, "p_value": 0.0001, "relationship_state": "stable", "confidence_label": "validated"},
                {"ts": ts, "series_x": "gold", "series_y": "yield_10y", "window_days": 30, "lag_days": 0, "correlation": -0.09, "p_value": 0.62, "relationship_state": "broken", "confidence_label": "likely_spurious"},
                {"ts": ts, "series_x": "BTCUSD", "series_y": "yield_3m", "window_days": 365, "lag_days": 0, "correlation": -0.75, "p_value": 0.0004, "relationship_state": "stable", "confidence_label": "validated"},
                {"ts": ts, "series_x": "BTCUSD", "series_y": "yield_3m", "window_days": 30, "lag_days": 0, "correlation": -0.03, "p_value": 0.70, "relationship_state": "broken", "confidence_label": "likely_spurious"},
                {"ts": ts, "series_x": "sp500", "series_y": "vix", "window_days": 365, "lag_days": 0, "correlation": -0.88, "p_value": 0.0002, "relationship_state": "stable", "confidence_label": "validated"},
                {"ts": ts, "series_x": "sp500", "series_y": "vix", "window_days": 30, "lag_days": 0, "correlation": -0.12, "p_value": 0.54, "relationship_state": "broken", "confidence_label": "likely_spurious"},
            ]
        )

    def test_build_market_stress_snapshot_computes_high_stress_and_missing_inputs(self) -> None:
        from qmis.signals.stress import build_market_stress_snapshot

        snapshot = build_market_stress_snapshot(
            signals=self._signals(include_credit=False),
            features=self._features(include_credit=False),
            relationships=self._relationships(),
        )

        self.assertIn(snapshot["stress_level"], {"HIGH", "CRITICAL"})
        self.assertGreater(float(snapshot["stress_score"]), 50.0)
        self.assertIn("credit", snapshot["missing_inputs"])
        self.assertIn("vix_level", snapshot["components"])
        self.assertIn("anomaly_pressure", snapshot["components"])

    def test_materialize_market_stress_persists_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.stress import materialize_market_stress
        from qmis.storage import connect_db

        signals = self._signals(include_credit=True)
        features = self._features(include_credit=True)
        relationships = self._relationships()

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                for table_name, payload in (("signals", signals), ("features", features), ("relationships", relationships)):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            inserted_rows = materialize_market_stress(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT stress_score, stress_level, summary, components, missing_inputs
                    FROM stress_snapshots
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        self.assertIn(str(persisted.iloc[0]["stress_level"]), {"HIGH", "CRITICAL"})
        self.assertIn("credit", json.loads(persisted.iloc[0]["components"]).keys())
        self.assertEqual(json.loads(persisted.iloc[0]["missing_inputs"]), [])


if __name__ == "__main__":
    unittest.main()

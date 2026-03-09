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


class QMISBreadthHealthTests(unittest.TestCase):
    def _signals(self, *, above_200dma: float, ad_line: float, new_highs: float, new_lows: float) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        return pd.DataFrame(
            [
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "sp500_above_200dma", "value": above_200dma, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "advance_decline_line", "value": ad_line, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_highs", "value": new_highs, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_lows", "value": new_lows, "unit": "count", "metadata": "{}"},
            ]
        )

    def _features(
        self,
        *,
        ad_trend: str,
        ad_zscore: float,
        above_trend: str,
        above_zscore: float,
        highs_trend: str,
        lows_trend: str,
    ) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        return pd.DataFrame(
            [
                {"ts": ts, "series_name": "advance_decline_line", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": ad_zscore, "volatility_30d": 0.2, "slope_30d": 0.2, "drawdown_90d": -1.0, "trend_label": ad_trend},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": above_zscore, "volatility_30d": 0.2, "slope_30d": 0.2, "drawdown_90d": -1.0, "trend_label": above_trend},
                {"ts": ts, "series_name": "new_highs", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": 0.8 if highs_trend == "UP" else -0.8 if highs_trend == "DOWN" else 0.0, "volatility_30d": 0.3, "slope_30d": 0.1, "drawdown_90d": -1.0, "trend_label": highs_trend},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": 0.8 if lows_trend == "UP" else -0.8 if lows_trend == "DOWN" else 0.0, "volatility_30d": 0.3, "slope_30d": 0.1, "drawdown_90d": -1.0, "trend_label": lows_trend},
            ]
        )

    def test_build_breadth_health_scores_strong_state(self) -> None:
        from qmis.signals.breadth import build_breadth_health

        snapshot = build_breadth_health(
            signals=self._signals(above_200dma=74.0, ad_line=2200.0, new_highs=82.0, new_lows=9.0),
            features=self._features(
                ad_trend="UP",
                ad_zscore=1.4,
                above_trend="UP",
                above_zscore=1.1,
                highs_trend="UP",
                lows_trend="DOWN",
            ),
        )

        self.assertEqual(snapshot["breadth_state"], "STRONG")
        self.assertGreater(snapshot["breadth_score"], 60.0)
        self.assertEqual(snapshot["missing_inputs"], [])
        self.assertIn("new_highs_vs_lows", snapshot["components"])

    def test_build_breadth_health_scores_weakening_state(self) -> None:
        from qmis.signals.breadth import build_breadth_health

        snapshot = build_breadth_health(
            signals=self._signals(above_200dma=56.0, ad_line=980.0, new_highs=31.0, new_lows=27.0),
            features=self._features(
                ad_trend="SIDEWAYS",
                ad_zscore=0.1,
                above_trend="DOWN",
                above_zscore=-0.2,
                highs_trend="DOWN",
                lows_trend="UP",
            ),
        )

        self.assertEqual(snapshot["breadth_state"], "WEAKENING")
        self.assertGreater(snapshot["breadth_score"], 40.0)
        self.assertLess(snapshot["breadth_score"], 60.0)

    def test_build_breadth_health_scores_fragile_state_and_missing_inputs(self) -> None:
        from qmis.signals.breadth import build_breadth_health

        signals = self._signals(above_200dma=34.0, ad_line=-420.0, new_highs=8.0, new_lows=76.0)
        features = self._features(
            ad_trend="DOWN",
            ad_zscore=-1.5,
            above_trend="DOWN",
            above_zscore=-1.3,
            highs_trend="DOWN",
            lows_trend="UP",
        ).loc[lambda frame: frame["series_name"] != "advance_decline_line"].reset_index(drop=True)

        snapshot = build_breadth_health(signals=signals, features=features)

        self.assertEqual(snapshot["breadth_state"], "FRAGILE")
        self.assertLess(snapshot["breadth_score"], 40.0)
        self.assertEqual(snapshot["missing_inputs"], ["advance_decline_line"])

    def test_materialize_breadth_health_persists_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.breadth import materialize_breadth_health
        from qmis.storage import connect_db

        signals = self._signals(above_200dma=74.0, ad_line=2200.0, new_highs=82.0, new_lows=9.0)
        features = self._features(
            ad_trend="UP",
            ad_zscore=1.4,
            above_trend="UP",
            above_zscore=1.1,
            highs_trend="UP",
            lows_trend="DOWN",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                for table_name, payload in (("signals", signals), ("features", features)):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            inserted_rows = materialize_breadth_health(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT breadth_score, breadth_state, summary, components, missing_inputs
                    FROM breadth_snapshots
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        self.assertEqual(str(persisted.iloc[0]["breadth_state"]), "STRONG")
        self.assertGreater(float(persisted.iloc[0]["breadth_score"]), 60.0)
        self.assertEqual(json.loads(persisted.iloc[0]["missing_inputs"]), [])
        self.assertIn("advance_decline_line", json.loads(persisted.iloc[0]["components"]))


if __name__ == "__main__":
    unittest.main()

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


class QMISLiquidityStateTests(unittest.TestCase):
    def _feature_frame(self, ts: pd.Timestamp, *, include_real_yields: bool = True) -> pd.DataFrame:
        rows = [
            {
                "ts": ts,
                "series_name": "fed_balance_sheet",
                "pct_change_30d": 3.2,
                "pct_change_90d": 5.8,
                "pct_change_365d": 8.4,
                "zscore_30d": 1.4,
                "volatility_30d": 0.1,
                "slope_30d": 0.2,
                "drawdown_90d": -1.5,
                "trend_label": "UP",
            },
            {
                "ts": ts,
                "series_name": "m2_money_supply",
                "pct_change_30d": 1.1,
                "pct_change_90d": 2.4,
                "pct_change_365d": 5.2,
                "zscore_30d": 0.8,
                "volatility_30d": 0.1,
                "slope_30d": 0.1,
                "drawdown_90d": -0.8,
                "trend_label": "UP",
            },
            {
                "ts": ts,
                "series_name": "reverse_repo_usage",
                "pct_change_30d": -11.0,
                "pct_change_90d": -18.0,
                "pct_change_365d": -34.0,
                "zscore_30d": -1.3,
                "volatility_30d": 0.2,
                "slope_30d": -0.4,
                "drawdown_90d": -10.0,
                "trend_label": "DOWN",
            },
            {
                "ts": ts,
                "series_name": "dollar_index",
                "pct_change_30d": -1.9,
                "pct_change_90d": -2.8,
                "pct_change_365d": -1.2,
                "zscore_30d": -0.7,
                "volatility_30d": 0.1,
                "slope_30d": -0.1,
                "drawdown_90d": -2.0,
                "trend_label": "DOWN",
            },
        ]
        if include_real_yields:
            rows.append(
                {
                    "ts": ts,
                    "series_name": "real_yields",
                    "pct_change_30d": -6.0,
                    "pct_change_90d": -9.0,
                    "pct_change_365d": -14.0,
                    "zscore_30d": -1.1,
                    "volatility_30d": 0.2,
                    "slope_30d": -0.3,
                    "drawdown_90d": -4.0,
                    "trend_label": "DOWN",
                }
            )
        return pd.DataFrame(rows)

    def _signal_frame(self, ts: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ts": ts, "source": "fred", "category": "liquidity", "series_name": "fed_balance_sheet", "value": 6_820_000.0, "unit": "millions_usd", "metadata": "{}"},
                {"ts": ts, "source": "fred", "category": "macro", "series_name": "m2_money_supply", "value": 21_820.0, "unit": "billions_usd", "metadata": "{}"},
                {"ts": ts, "source": "fred", "category": "liquidity", "series_name": "reverse_repo_usage", "value": 145.0, "unit": "billions_usd", "metadata": "{}"},
                {"ts": ts, "source": "yfinance", "category": "liquidity", "series_name": "dollar_index", "value": 101.2, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "fred", "category": "macro", "series_name": "real_yields", "value": 1.63, "unit": "percent", "metadata": "{}"},
            ]
        )

    def test_build_liquidity_state_scores_expanding_conditions(self) -> None:
        from qmis.signals.liquidity import build_liquidity_state

        ts = pd.Timestamp("2026-03-08")
        snapshot = build_liquidity_state(signals=self._signal_frame(ts), features=self._feature_frame(ts))

        self.assertEqual(snapshot["liquidity_state"], "EXPANDING")
        self.assertGreater(snapshot["liquidity_score"], 60.0)
        self.assertEqual(snapshot["missing_inputs"], [])
        self.assertIn("fed balance sheet", snapshot["summary"].lower())
        self.assertIn("real_yields", snapshot["components"])
        self.assertGreater(snapshot["components"]["reverse_repo_usage"]["score"], 0.0)

    def test_build_liquidity_state_handles_missing_inputs_and_tightening(self) -> None:
        from qmis.signals.liquidity import build_liquidity_state

        ts = pd.Timestamp("2026-03-08")
        features = self._feature_frame(ts, include_real_yields=False).copy()
        features.loc[features["series_name"] == "fed_balance_sheet", ["zscore_30d", "trend_label"]] = [-1.4, "DOWN"]
        features.loc[features["series_name"] == "m2_money_supply", ["zscore_30d", "trend_label"]] = [-0.9, "DOWN"]
        features.loc[features["series_name"] == "reverse_repo_usage", ["zscore_30d", "trend_label"]] = [1.1, "UP"]
        features.loc[features["series_name"] == "dollar_index", ["zscore_30d", "trend_label"]] = [0.8, "UP"]
        signals = self._signal_frame(ts).loc[lambda frame: frame["series_name"] != "real_yields"].reset_index(drop=True)

        snapshot = build_liquidity_state(signals=signals, features=features)

        self.assertEqual(snapshot["liquidity_state"], "TIGHTENING")
        self.assertLess(snapshot["liquidity_score"], 40.0)
        self.assertEqual(snapshot["missing_inputs"], ["real_yields"])

    def test_materialize_liquidity_state_persists_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.liquidity import materialize_liquidity_state
        from qmis.storage import connect_db

        ts = pd.Timestamp("2026-03-08")
        signals = self._signal_frame(ts)
        features = self._feature_frame(ts)

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                connection.register("signals_df", signals)
                connection.execute(
                    """
                    INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
                    SELECT ts, source, category, series_name, value, unit, metadata
                    FROM signals_df
                    """
                )
                connection.unregister("signals_df")
                connection.register("features_df", features)
                connection.execute(
                    """
                    INSERT INTO features (
                        ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                        zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
                    )
                    SELECT
                        ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                        zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
                    FROM features_df
                    """
                )
                connection.unregister("features_df")

            inserted_rows = materialize_liquidity_state(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT liquidity_score, liquidity_state, summary, components, missing_inputs
                    FROM liquidity_snapshots
                    """
                ).fetchone()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(persisted[1], "EXPANDING")
        self.assertGreater(float(persisted[0]), 60.0)
        self.assertEqual(json.loads(persisted[4]), [])
        self.assertIn("dollar_index", json.loads(persisted[3]))


if __name__ == "__main__":
    unittest.main()

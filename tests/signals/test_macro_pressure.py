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


class QMISMacroPressureTests(unittest.TestCase):
    def _signals(self) -> pd.DataFrame:
        timestamps = pd.to_datetime(["2026-03-08", "2026-03-09"])
        payload = {
            "high_yield_spread": [4.2, 5.8],
            "baa_corporate_spread": [2.3, 3.1],
            "vix": [18.5, 31.0],
            "vix3m": [19.0, 26.0],
            "vix6m": [19.5, 27.0],
            "sp500_above_200dma": [62.0, 41.0],
            "advance_decline_line": [1200.0, -250.0],
            "new_highs": [84.0, 18.0],
            "new_lows": [12.0, 132.0],
            "yield_10y": [4.2, 4.05],
            "yield_2y": [4.3, 4.35],
            "yield_3m": [4.35, 4.8],
            "fed_balance_sheet": [7050.0, 6880.0],
            "m2_money_supply": [21900.0, 21720.0],
            "reverse_repo_usage": [145.0, 305.0],
            "dollar_index": [102.5, 106.0],
            "real_yields": [1.55, 2.05],
        }
        rows: list[dict[str, object]] = []
        for series_name, values in payload.items():
            if series_name in {"vix", "vix3m", "vix6m"}:
                category = "market"
                unit = "index_points"
            elif series_name in {"sp500_above_200dma"}:
                category = "breadth"
                unit = "percent"
            elif series_name in {"advance_decline_line", "new_highs", "new_lows"}:
                category = "breadth"
                unit = "count"
            else:
                category = "macro"
                unit = "percent" if "yield" in series_name or "spread" in series_name else "usd_billions"
            if series_name == "dollar_index":
                unit = "index_points"
            for ts, value in zip(timestamps, values, strict=True):
                rows.append(
                    {
                        "ts": ts,
                        "source": "test",
                        "category": category,
                        "series_name": series_name,
                        "value": value,
                        "unit": unit,
                        "metadata": "{}",
                    }
                )
        return pd.DataFrame(rows)

    def _features(self) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-09")
        return pd.DataFrame(
            [
                {"ts": ts, "series_name": "high_yield_spread", "pct_change_30d": 14.0, "pct_change_90d": 18.0, "pct_change_365d": 11.0, "zscore_30d": 2.0, "volatility_30d": 0.2, "slope_30d": 0.6, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "baa_corporate_spread", "pct_change_30d": 11.0, "pct_change_90d": 16.0, "pct_change_365d": 9.0, "zscore_30d": 1.7, "volatility_30d": 0.2, "slope_30d": 0.4, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "vix", "pct_change_30d": 32.0, "pct_change_90d": 28.0, "pct_change_365d": 18.0, "zscore_30d": 2.3, "volatility_30d": 0.5, "slope_30d": 0.7, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": -22.0, "pct_change_90d": -24.0, "pct_change_365d": -12.0, "zscore_30d": -1.8, "volatility_30d": 0.4, "slope_30d": -0.6, "drawdown_90d": -11.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "advance_decline_line", "pct_change_30d": -18.0, "pct_change_90d": -20.0, "pct_change_365d": -15.0, "zscore_30d": -1.5, "volatility_30d": 0.4, "slope_30d": -0.5, "drawdown_90d": -10.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_highs", "pct_change_30d": -35.0, "pct_change_90d": -42.0, "pct_change_365d": -24.0, "zscore_30d": -1.9, "volatility_30d": 0.4, "slope_30d": -0.8, "drawdown_90d": -6.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": 80.0, "pct_change_90d": 95.0, "pct_change_365d": 60.0, "zscore_30d": 2.2, "volatility_30d": 0.6, "slope_30d": 0.9, "drawdown_90d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "yield_10y", "pct_change_30d": -0.5, "pct_change_90d": -0.2, "pct_change_365d": 0.4, "zscore_30d": 0.1, "volatility_30d": 0.1, "slope_30d": -0.1, "drawdown_90d": -0.3, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "yield_2y", "pct_change_30d": 0.7, "pct_change_90d": 1.1, "pct_change_365d": 0.8, "zscore_30d": 0.4, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "yield_3m", "pct_change_30d": 1.2, "pct_change_90d": 1.4, "pct_change_365d": 0.9, "zscore_30d": 0.5, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": -3.0, "pct_change_90d": -5.0, "pct_change_365d": -7.0, "zscore_30d": -1.1, "volatility_30d": 0.1, "slope_30d": -0.3, "drawdown_90d": -1.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "m2_money_supply", "pct_change_30d": -1.2, "pct_change_90d": -1.5, "pct_change_365d": -0.8, "zscore_30d": -0.5, "volatility_30d": 0.1, "slope_30d": -0.2, "drawdown_90d": -0.8, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": 22.0, "pct_change_90d": 35.0, "pct_change_365d": 41.0, "zscore_30d": 1.7, "volatility_30d": 0.2, "slope_30d": 0.5, "drawdown_90d": -0.6, "trend_label": "UP"},
                {"ts": ts, "series_name": "dollar_index", "pct_change_30d": 3.2, "pct_change_90d": 4.6, "pct_change_365d": 6.1, "zscore_30d": 1.1, "volatility_30d": 0.1, "slope_30d": 0.3, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "real_yields", "pct_change_30d": 1.3, "pct_change_90d": 1.8, "pct_change_365d": 2.2, "zscore_30d": 1.2, "volatility_30d": 0.1, "slope_30d": 0.3, "drawdown_90d": -0.2, "trend_label": "UP"},
            ]
        )

    def test_build_macro_pressure_snapshot_identifies_high_pressure(self) -> None:
        from qmis.signals.macro_pressure import build_macro_pressure_snapshot, classify_macro_pressure

        snapshot = build_macro_pressure_snapshot(
            signals=self._signals(),
            features=self._features(),
        )

        self.assertGreaterEqual(float(snapshot["mpi_score"]), 70.0)
        self.assertEqual(snapshot["pressure_level"], classify_macro_pressure(float(snapshot["mpi_score"])))
        self.assertEqual(set(snapshot["components"]), {"credit_stress", "volatility_stress", "breadth_stress", "liquidity_stress", "yield_curve_stress"})
        self.assertEqual(snapshot["pressure_level"], "SEVERE PRESSURE")
        self.assertEqual(snapshot["missing_inputs"], [])
        self.assertGreaterEqual(len(snapshot["primary_contributors"]), 3)
        self.assertIn("volatility", " ".join(snapshot["primary_contributors"]).lower())
        self.assertIn("breadth", " ".join(snapshot["primary_contributors"]).lower())

    def test_materialize_macro_pressure_persists_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.macro_pressure import materialize_macro_pressure
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("signals", self._signals()),
                    ("features", self._features()),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            inserted_rows = materialize_macro_pressure(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT mpi_score, pressure_level, summary, components, primary_contributors, missing_inputs
                    FROM macro_pressure_snapshots
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        row = persisted.iloc[0]
        self.assertGreaterEqual(float(row["mpi_score"]), 70.0)
        self.assertEqual(str(row["pressure_level"]), "SEVERE PRESSURE")
        self.assertEqual(set(json.loads(row["components"]).keys()), {"credit_stress", "volatility_stress", "breadth_stress", "liquidity_stress", "yield_curve_stress"})
        self.assertGreaterEqual(len(json.loads(row["primary_contributors"])), 3)
        self.assertEqual(json.loads(row["missing_inputs"]), [])


if __name__ == "__main__":
    unittest.main()

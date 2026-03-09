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


class QMISPredictiveSignalTests(unittest.TestCase):
    def _build_signal_frame(self) -> pd.DataFrame:
        timestamps = pd.to_datetime(["2026-03-07", "2026-03-08"])
        rows: list[dict[str, object]] = []
        payload = {
            "yield_10y": [4.00, 4.10],
            "yield_2y": [4.20, 4.35],
            "yield_3m": [4.60, 4.55],
            "high_yield_spread": [4.10, 4.80],
            "baa_corporate_spread": [2.20, 2.60],
            "financial_conditions_index": [0.20, 0.60],
            "fed_balance_sheet": [6900.0, 6800.0],
            "m2_money_supply": [21850.0, 21800.0],
            "reverse_repo_usage": [180.0, 250.0],
            "dollar_index": [103.0, 105.0],
            "real_yields": [1.60, 1.90],
            "breakeven_inflation_10y": [2.30, 2.10],
            "vix": [18.0, 24.0],
            "vix3m": [19.5, 22.0],
            "vix6m": [20.0, 22.5],
            "pmi": [50.5, 49.0],
        }
        for series_name, values in payload.items():
            unit = "percent" if "yield" in series_name or "spread" in series_name else "index_points"
            if series_name in {"fed_balance_sheet", "m2_money_supply", "reverse_repo_usage"}:
                unit = "usd_billions"
            for ts, value in zip(timestamps, values, strict=True):
                rows.append(
                    {
                        "ts": ts,
                        "source": "test",
                        "category": "macro" if series_name not in {"vix", "vix3m", "vix6m"} else "market",
                        "series_name": series_name,
                        "value": value,
                        "unit": unit,
                        "metadata": "{}",
                    }
                )
        return pd.DataFrame(rows)

    def _build_feature_frame(self) -> pd.DataFrame:
        ts = pd.Timestamp("2026-03-08")
        return pd.DataFrame(
            [
                {"ts": ts, "series_name": "yield_10y", "pct_change_30d": 1.5, "pct_change_90d": 1.0, "pct_change_365d": 0.5, "zscore_30d": 0.5, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -0.1, "trend_label": "UP"},
                {"ts": ts, "series_name": "yield_2y", "pct_change_30d": 1.8, "pct_change_90d": 1.2, "pct_change_365d": 0.8, "zscore_30d": 0.7, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -0.1, "trend_label": "UP"},
                {"ts": ts, "series_name": "yield_3m", "pct_change_30d": -0.2, "pct_change_90d": -0.1, "pct_change_365d": 0.2, "zscore_30d": -0.1, "volatility_30d": 0.1, "slope_30d": 0.0, "drawdown_90d": -0.1, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": -3.5, "pct_change_90d": -5.0, "pct_change_365d": -8.0, "zscore_30d": -0.9, "volatility_30d": 0.1, "slope_30d": -0.3, "drawdown_90d": -1.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "m2_money_supply", "pct_change_30d": -1.0, "pct_change_90d": -1.5, "pct_change_365d": 0.5, "zscore_30d": -0.3, "volatility_30d": 0.1, "slope_30d": -0.1, "drawdown_90d": -0.5, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": 15.0, "pct_change_90d": 22.0, "pct_change_365d": 31.0, "zscore_30d": 1.4, "volatility_30d": 0.2, "slope_30d": 0.4, "drawdown_90d": -0.5, "trend_label": "UP"},
                {"ts": ts, "series_name": "dollar_index", "pct_change_30d": 2.0, "pct_change_90d": 3.2, "pct_change_365d": 4.6, "zscore_30d": 0.8, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "real_yields", "pct_change_30d": 1.0, "pct_change_90d": 1.4, "pct_change_365d": 1.9, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "pmi", "pct_change_30d": -1.5, "pct_change_90d": -2.1, "pct_change_365d": -0.8, "zscore_30d": -0.9, "volatility_30d": 0.1, "slope_30d": -0.2, "drawdown_90d": -1.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "sp500", "pct_change_30d": 1.2, "pct_change_90d": 3.5, "pct_change_365d": 11.0, "zscore_30d": 0.4, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -1.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "semiconductor_index", "pct_change_30d": -2.0, "pct_change_90d": -4.0, "pct_change_365d": 8.0, "zscore_30d": -0.8, "volatility_30d": 0.2, "slope_30d": -0.2, "drawdown_90d": -4.1, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "small_caps", "pct_change_30d": -1.5, "pct_change_90d": -2.0, "pct_change_365d": 6.0, "zscore_30d": -0.5, "volatility_30d": 0.2, "slope_30d": -0.1, "drawdown_90d": -3.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "bank_stocks", "pct_change_30d": -2.1, "pct_change_90d": -3.8, "pct_change_365d": 5.0, "zscore_30d": -0.7, "volatility_30d": 0.2, "slope_30d": -0.2, "drawdown_90d": -3.2, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "transportation_index", "pct_change_30d": -1.2, "pct_change_90d": -2.5, "pct_change_365d": 4.5, "zscore_30d": -0.4, "volatility_30d": 0.2, "slope_30d": -0.1, "drawdown_90d": -2.8, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "copper", "pct_change_30d": 4.1, "pct_change_90d": 6.5, "pct_change_365d": 12.0, "zscore_30d": 0.8, "volatility_30d": 0.2, "slope_30d": 0.3, "drawdown_90d": -2.1, "trend_label": "UP"},
                {"ts": ts, "series_name": "oil", "pct_change_30d": 5.4, "pct_change_90d": 7.0, "pct_change_365d": 15.0, "zscore_30d": 0.9, "volatility_30d": 0.3, "slope_30d": 0.3, "drawdown_90d": -2.3, "trend_label": "UP"},
                {"ts": ts, "series_name": "agriculture_index", "pct_change_30d": 2.2, "pct_change_90d": 3.1, "pct_change_365d": 10.0, "zscore_30d": 0.4, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -1.1, "trend_label": "UP"},
                {"ts": ts, "series_name": "commodity_index", "pct_change_30d": 3.0, "pct_change_90d": 4.2, "pct_change_365d": 9.2, "zscore_30d": 0.6, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -1.4, "trend_label": "UP"},
            ]
        )

    def test_build_predictive_snapshot_classifies_forward_macro_signals(self) -> None:
        from qmis.signals.predictive import build_predictive_snapshot

        snapshot = build_predictive_snapshot(
            signals=self._build_signal_frame(),
            features=self._build_feature_frame(),
        )

        forward_signals = snapshot["forward_macro_signals"]
        self.assertEqual(forward_signals["yield_curve"]["state"], "Inverted")
        self.assertEqual(forward_signals["credit_spreads"]["state"], "Widening")
        self.assertEqual(forward_signals["financial_conditions"]["state"], "Tightening")
        self.assertEqual(forward_signals["real_rates"]["state"], "Rising")
        self.assertEqual(forward_signals["global_liquidity"]["state"], "Contracting")
        self.assertEqual(forward_signals["volatility_term_structure"]["state"], "Backwardation")
        self.assertEqual(forward_signals["manufacturing_momentum"]["state"], "Weakening")
        self.assertEqual(forward_signals["leadership_rotation"]["state"], "Defensive")
        self.assertEqual(forward_signals["commodity_pressure"]["state"], "Inflationary")
        self.assertEqual(snapshot["missing_inputs"], [])
        self.assertIn("recession risk", snapshot["summary"].lower())

    def test_materialize_predictive_signals_persists_latest_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.predictive import materialize_predictive_signals
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("signals", self._build_signal_frame()),
                    ("features", self._build_feature_frame()),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            inserted_rows = materialize_predictive_signals(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute("SELECT * FROM predictive_snapshots").fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        row = persisted.iloc[0].to_dict()
        forward_signals = json.loads(row["forward_macro_signals"])
        self.assertEqual(forward_signals["yield_curve"]["state"], "Inverted")
        self.assertEqual(forward_signals["global_liquidity"]["state"], "Contracting")
        self.assertEqual(json.loads(row["missing_inputs"]), [])


if __name__ == "__main__":
    unittest.main()

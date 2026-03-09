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


class QMISRegimeTests(unittest.TestCase):
    def test_determine_regime_covers_all_spec_labels(self) -> None:
        from qmis.signals.regime import determine_regime

        cases = [
            ({"inflation_score": 2, "growth_score": 2, "liquidity_score": 2, "risk_score": 3}, "CRISIS / RISK-OFF"),
            ({"inflation_score": 2, "growth_score": 2, "liquidity_score": 2, "risk_score": 1}, "INFLATIONARY EXPANSION"),
            ({"inflation_score": 0, "growth_score": 2, "liquidity_score": 2, "risk_score": 1}, "DISINFLATION"),
            ({"inflation_score": 1, "growth_score": 1, "liquidity_score": 1, "risk_score": 2}, "RECESSION RISK"),
            ({"inflation_score": 1, "growth_score": 1, "liquidity_score": 3, "risk_score": 1}, "LIQUIDITY EXPANSION"),
            ({"inflation_score": 1, "growth_score": 1, "liquidity_score": 1, "risk_score": 1}, "LIQUIDITY WITHDRAWAL"),
            ({"inflation_score": 1, "growth_score": 3, "liquidity_score": 3, "risk_score": 0}, "SPECULATIVE BUBBLE"),
            ({"inflation_score": 1, "growth_score": 2, "liquidity_score": 2, "risk_score": 1}, "NEUTRAL"),
        ]

        for scores, expected in cases:
            with self.subTest(expected=expected):
                regime_label, confidence = determine_regime(scores)
                self.assertEqual(regime_label, expected)
                self.assertGreaterEqual(confidence, 0.0)
                self.assertLessEqual(confidence, 1.0)

    def test_materialize_regime_replaces_regime_rows_from_latest_inputs(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.regime import materialize_regime
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            ts = pd.Timestamp("2026-03-08")
            feature_payload = pd.DataFrame(
                {
                    "ts": [ts] * 11,
                    "series_name": [
                        "gold",
                        "oil",
                        "yield_10y",
                        "copper",
                        "sp500",
                        "pmi",
                        "fed_balance_sheet",
                        "m2_money_supply",
                        "reverse_repo_usage",
                        "dollar_index",
                        "vix",
                    ],
                    "pct_change_30d": [6.0] * 11,
                    "pct_change_90d": [6.0] * 11,
                    "pct_change_365d": [6.0] * 11,
                    "zscore_30d": [1.0] * 11,
                    "volatility_30d": [0.1] * 11,
                    "slope_30d": [1.0] * 11,
                    "drawdown_90d": [0.0] * 11,
                    "trend_label": [
                        "UP",
                        "UP",
                        "UP",
                        "UP",
                        "UP",
                        "UP",
                        "UP",
                        "UP",
                        "DOWN",
                        "DOWN",
                        "SIDEWAYS",
                    ],
                }
            )
            signal_payload = pd.DataFrame(
                {
                    "ts": [ts, ts],
                    "source": ["test", "test"],
                    "category": ["macro", "macro"],
                    "series_name": ["yield_10y", "yield_3m"],
                    "value": [4.2, 3.8],
                    "unit": ["percent", "percent"],
                    "metadata": ["{}", "{}"],
                }
            )
            with connect_db(db_path) as connection:
                connection.register("features_df", feature_payload)
                connection.execute("INSERT INTO features SELECT * FROM features_df")
                connection.unregister("features_df")
                connection.register("signals_df", signal_payload)
                connection.execute("INSERT INTO signals SELECT * FROM signals_df")
                connection.unregister("signals_df")

            inserted_rows = materialize_regime(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute("SELECT * FROM regimes").fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        row = persisted.iloc[0].to_dict()
        self.assertEqual(row["inflation_score"], 3)
        self.assertEqual(row["growth_score"], 3)
        self.assertEqual(row["liquidity_score"], 4)
        self.assertEqual(row["risk_score"], 0)
        self.assertEqual(row["regime_label"], "SPECULATIVE BUBBLE")
        self.assertGreaterEqual(row["confidence"], 0.0)
        self.assertLessEqual(row["confidence"], 1.0)


if __name__ == "__main__":
    unittest.main()

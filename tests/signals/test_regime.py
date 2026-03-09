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
            breadth_payload = pd.DataFrame(
                [
                    {
                        "ts": ts,
                        "breadth_score": 74.0,
                        "breadth_state": "STRONG",
                        "summary": "Breadth is strong.",
                        "components": '{"sp500_above_200dma": {"score": 0.7}}',
                        "missing_inputs": "[]",
                    }
                ]
            )
            liquidity_payload = pd.DataFrame(
                [
                    {
                        "ts": ts,
                        "liquidity_score": 68.0,
                        "liquidity_state": "EXPANDING",
                        "summary": "Liquidity is expanding.",
                        "components": '{"fed_balance_sheet": {"score": 0.4}}',
                        "missing_inputs": "[]",
                    }
                ]
            )
            stress_payload = pd.DataFrame(
                [
                    {
                        "ts": ts,
                        "stress_score": 24.0,
                        "stress_level": "LOW",
                        "summary": "Stress is low.",
                        "components": '{"breadth": 0.1}',
                        "missing_inputs": "[]",
                    }
                ]
            )
            factor_payload = pd.DataFrame(
                [
                    {
                        "ts": ts,
                        "factor_name": "liquidity",
                        "component_rank": 1,
                        "strength": 0.72,
                        "direction": "expanding",
                        "summary": "Liquidity is supporting markets.",
                        "supporting_assets": '["fed_balance_sheet", "reverse_repo_usage"]',
                        "loadings": '{"fed_balance_sheet": 0.8}',
                    },
                    {
                        "ts": ts,
                        "factor_name": "crypto",
                        "component_rank": 2,
                        "strength": 0.42,
                        "direction": "bullish",
                        "summary": "Crypto is firm.",
                        "supporting_assets": '["BTCUSD", "ETHUSD"]',
                        "loadings": '{"BTCUSD": 0.7}',
                    },
                ]
            )
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("features", feature_payload),
                    ("signals", signal_payload),
                    ("breadth_snapshots", breadth_payload),
                    ("liquidity_snapshots", liquidity_payload),
                    ("stress_snapshots", stress_payload),
                    ("factors", factor_payload),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

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
        probabilities = json.loads(row["regime_probabilities"])
        self.assertIn("SPECULATIVE BUBBLE", probabilities)
        self.assertAlmostEqual(sum(probabilities.values()), 100.0, places=2)
        drivers = json.loads(row["regime_drivers"])
        self.assertIn("SPECULATIVE BUBBLE", drivers)
        self.assertTrue(drivers["SPECULATIVE BUBBLE"])

    def test_build_regime_probabilities_handles_mixed_signals(self) -> None:
        from qmis.signals.regime import build_regime_probabilities

        probabilities, drivers = build_regime_probabilities(
            scores={
                "inflation_score": 2,
                "growth_score": 1,
                "liquidity_score": 1,
                "risk_score": 2,
            },
            breadth_health={"breadth_state": "WEAKENING", "breadth_score": 48.0},
            liquidity_environment={"liquidity_state": "TIGHTENING", "liquidity_score": 34.0},
            market_stress={"stress_level": "HIGH", "stress_score": 67.0},
            factors=[
                {"factor_name": "liquidity", "direction": "tightening", "strength": 0.78},
                {"factor_name": "volatility", "direction": "stressed", "strength": 0.66},
            ],
        )

        self.assertAlmostEqual(sum(probabilities.values()), 100.0, places=2)
        self.assertGreater(probabilities["RECESSION RISK"], 0.0)
        self.assertGreater(probabilities["LIQUIDITY WITHDRAWAL"], 0.0)
        self.assertGreater(probabilities["CRISIS / RISK-OFF"], 0.0)
        self.assertLess(probabilities["SPECULATIVE BUBBLE"], probabilities["RECESSION RISK"])
        self.assertTrue(drivers["RECESSION RISK"])


if __name__ == "__main__":
    unittest.main()

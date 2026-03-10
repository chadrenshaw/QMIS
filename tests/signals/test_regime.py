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

    def test_materialize_regime_builds_current_regime_from_latest_feature_per_series(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.regime import materialize_regime
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            ts = pd.Timestamp("2026-03-08")
            crypto_ts = pd.Timestamp("2026-03-08 12:15:00")
            feature_payload = pd.DataFrame(
                [
                    {"ts": ts, "series_name": "gold", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "oil", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts - pd.Timedelta(days=2), "series_name": "yield_10y", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "copper", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "sp500", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts - pd.Timedelta(days=30), "series_name": "pmi", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts - pd.Timedelta(days=4), "series_name": "fed_balance_sheet", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts - pd.Timedelta(days=65), "series_name": "m2_money_supply", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "dollar_index", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "vix", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": 0.0, "volatility_30d": 0.1, "slope_30d": 0.0, "drawdown_90d": 0.0, "trend_label": "SIDEWAYS"},
                    {"ts": crypto_ts, "series_name": "BTC_dominance", "pct_change_30d": 3.0, "pct_change_90d": 4.0, "pct_change_365d": 5.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 0.4, "drawdown_90d": -1.0, "trend_label": "UP"},
                    {"ts": crypto_ts, "series_name": "crypto_market_cap", "pct_change_30d": 3.0, "pct_change_90d": 4.0, "pct_change_365d": 5.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 0.4, "drawdown_90d": -1.0, "trend_label": "UP"},
                ]
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
                predictive_persisted = connection.execute("SELECT * FROM predictive_snapshots").fetchdf()
                macro_pressure_persisted = connection.execute("SELECT * FROM macro_pressure_snapshots").fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertGreaterEqual(len(persisted), 1)
        row = persisted.sort_values("ts").iloc[-1].to_dict()
        self.assertEqual(row["inflation_score"], 3)
        self.assertEqual(row["growth_score"], 3)
        self.assertEqual(row["liquidity_score"], 4)
        self.assertEqual(row["risk_score"], 0)
        self.assertEqual(row["regime_label"], "SPECULATIVE BUBBLE")
        self.assertGreaterEqual(row["confidence"], 0.0)
        self.assertLessEqual(row["confidence"], 1.0)
        probabilities = json.loads(row["regime_probabilities"])
        self.assertIn("LIQUIDITY EXPANSION", probabilities)
        self.assertAlmostEqual(sum(probabilities.values()), 100.0, places=2)
        drivers = json.loads(row["regime_drivers"])
        self.assertIn("LIQUIDITY EXPANSION", drivers)
        self.assertTrue(drivers["LIQUIDITY EXPANSION"])
        bayesian_evidence = json.loads(row["bayesian_evidence"])
        self.assertIn("LIQUIDITY EXPANSION", bayesian_evidence)
        forecast = json.loads(row["forward_regime_forecast"])
        self.assertEqual(set(forecast), {"30d", "90d", "180d"})
        self.assertIn("top_regime", forecast["30d"])
        self.assertIn("distribution", forecast["90d"])
        self.assertEqual(len(predictive_persisted), 1)
        self.assertEqual(len(macro_pressure_persisted), 1)
        forward_signals = json.loads(predictive_persisted.iloc[0]["forward_macro_signals"])
        self.assertEqual(forward_signals["yield_curve"]["state"], "Normal")

    def test_materialize_regime_preserves_history_across_distinct_runs(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.regime import materialize_regime
        from qmis.storage import connect_db

        def feature_rows(ts: pd.Timestamp, *, gold_trend: str, oil_trend: str) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"ts": ts, "series_name": "gold", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": gold_trend},
                    {"ts": ts, "series_name": "oil", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": oil_trend},
                    {"ts": ts, "series_name": "yield_10y", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "copper", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "sp500", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "pmi", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "m2_money_supply", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "dollar_index", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "vix", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": 0.0, "volatility_30d": 0.1, "slope_30d": 0.0, "drawdown_90d": 0.0, "trend_label": "SIDEWAYS"},
                ]
            )

        def signal_rows(ts: pd.Timestamp, yield_10y: float, yield_3m: float) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": yield_10y, "unit": "percent", "metadata": "{}"},
                    {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": yield_3m, "unit": "percent", "metadata": "{}"},
                ]
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            ts_one = pd.Timestamp("2026-03-08")
            ts_two = pd.Timestamp("2026-03-09")
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("features", feature_rows(ts_one, gold_trend="UP", oil_trend="UP")),
                    ("signals", signal_rows(ts_one, 4.2, 3.8)),
                    ("breadth_snapshots", pd.DataFrame([{"ts": ts_one, "breadth_score": 74.0, "breadth_state": "STRONG", "summary": "Breadth is strong.", "components": "{}", "missing_inputs": "[]"}])),
                    ("liquidity_snapshots", pd.DataFrame([{"ts": ts_one, "liquidity_score": 68.0, "liquidity_state": "EXPANDING", "summary": "Liquidity is expanding.", "components": "{}", "missing_inputs": "[]"}])),
                    ("stress_snapshots", pd.DataFrame([{"ts": ts_one, "stress_score": 24.0, "stress_level": "LOW", "summary": "Stress is low.", "components": "{}", "missing_inputs": "[]"}])),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            materialize_regime(db_path=db_path)

            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("features", feature_rows(ts_two, gold_trend="DOWN", oil_trend="UP")),
                    ("signals", signal_rows(ts_two, 4.0, 4.3)),
                    ("breadth_snapshots", pd.DataFrame([{"ts": ts_two, "breadth_score": 44.0, "breadth_state": "WEAKENING", "summary": "Breadth is weakening.", "components": "{}", "missing_inputs": "[]"}])),
                    ("liquidity_snapshots", pd.DataFrame([{"ts": ts_two, "liquidity_score": 42.0, "liquidity_state": "TIGHTENING", "summary": "Liquidity is tightening.", "components": "{}", "missing_inputs": "[]"}])),
                    ("stress_snapshots", pd.DataFrame([{"ts": ts_two, "stress_score": 61.0, "stress_level": "HIGH", "summary": "Stress is high.", "components": "{}", "missing_inputs": "[]"}])),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            materialize_regime(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    "SELECT ts, inflation_score, growth_score, liquidity_score, risk_score FROM regimes ORDER BY ts"
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(len(persisted), 2)
        self.assertEqual(list(persisted["ts"].dt.strftime("%Y-%m-%d")), ["2026-03-08", "2026-03-09"])

    def test_materialize_regime_backfills_historical_rows_on_first_run(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.regime import materialize_regime
        from qmis.storage import connect_db

        def feature_rows(ts: pd.Timestamp, *, gold_trend: str, oil_trend: str) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"ts": ts, "series_name": "gold", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": gold_trend},
                    {"ts": ts, "series_name": "oil", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": oil_trend},
                    {"ts": ts, "series_name": "yield_10y", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "copper", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "sp500", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "pmi", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "m2_money_supply", "pct_change_30d": 6.0, "pct_change_90d": 6.0, "pct_change_365d": 6.0, "zscore_30d": 1.0, "volatility_30d": 0.1, "slope_30d": 1.0, "drawdown_90d": 0.0, "trend_label": "UP"},
                    {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "dollar_index", "pct_change_30d": -6.0, "pct_change_90d": -6.0, "pct_change_365d": -6.0, "zscore_30d": -1.0, "volatility_30d": 0.1, "slope_30d": -1.0, "drawdown_90d": 0.0, "trend_label": "DOWN"},
                    {"ts": ts, "series_name": "vix", "pct_change_30d": 0.0, "pct_change_90d": 0.0, "pct_change_365d": 0.0, "zscore_30d": 0.0, "volatility_30d": 0.1, "slope_30d": 0.0, "drawdown_90d": 0.0, "trend_label": "SIDEWAYS"},
                ]
            )

        def signal_rows(ts: pd.Timestamp, yield_10y: float, yield_3m: float) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": yield_10y, "unit": "percent", "metadata": "{}"},
                    {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": yield_3m, "unit": "percent", "metadata": "{}"},
                ]
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            ts_one = pd.Timestamp("2026-03-08")
            ts_two = pd.Timestamp("2026-03-09 12:15:00")
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("features", feature_rows(ts_one, gold_trend="UP", oil_trend="UP")),
                    ("features", feature_rows(ts_two, gold_trend="DOWN", oil_trend="UP")),
                    ("signals", signal_rows(ts_one, 4.2, 3.8)),
                    ("signals", signal_rows(ts_two, 4.0, 4.3)),
                    ("breadth_snapshots", pd.DataFrame([{"ts": ts_two, "breadth_score": 44.0, "breadth_state": "WEAKENING", "summary": "Breadth is weakening.", "components": "{}", "missing_inputs": "[]"}])),
                    ("liquidity_snapshots", pd.DataFrame([{"ts": ts_two, "liquidity_score": 42.0, "liquidity_state": "TIGHTENING", "summary": "Liquidity is tightening.", "components": "{}", "missing_inputs": "[]"}])),
                    ("stress_snapshots", pd.DataFrame([{"ts": ts_two, "stress_score": 61.0, "stress_level": "HIGH", "summary": "Stress is high.", "components": "{}", "missing_inputs": "[]"}])),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            materialize_regime(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    "SELECT ts, inflation_score, growth_score, liquidity_score, risk_score FROM regimes ORDER BY ts"
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(len(persisted), 2)
        self.assertEqual(list(persisted["ts"].dt.strftime("%Y-%m-%d")), ["2026-03-08", "2026-03-09"])
        self.assertNotEqual(
            list(persisted.iloc[0][["inflation_score", "growth_score", "liquidity_score", "risk_score"]]),
            list(persisted.iloc[1][["inflation_score", "growth_score", "liquidity_score", "risk_score"]]),
        )

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
        self.assertGreater(probabilities["STAGFLATION RISK"], 0.0)
        self.assertLess(probabilities["LIQUIDITY EXPANSION"], probabilities["RECESSION RISK"])
        self.assertTrue(drivers["RECESSION RISK"])

    def test_build_regime_probabilities_uses_predictive_macro_evidence(self) -> None:
        from qmis.signals.regime import build_regime_probabilities

        probabilities, drivers = build_regime_probabilities(
            scores={
                "inflation_score": 1,
                "growth_score": 2,
                "liquidity_score": 2,
                "risk_score": 1,
            },
            breadth_health={"breadth_state": "STRONG", "breadth_score": 68.0},
            liquidity_environment={"liquidity_state": "NEUTRAL", "liquidity_score": 50.0},
            market_stress={"stress_level": "MODERATE", "stress_score": 44.0},
            predictive_snapshot={
                "forward_macro_signals": {
                    "yield_curve": {"state": "Inverted", "summary": "Yield curve remains inverted."},
                    "credit_spreads": {"state": "Widening", "summary": "Credit spreads are widening."},
                    "financial_conditions": {"state": "Tightening", "summary": "Conditions are tightening."},
                    "real_rates": {"state": "Rising", "summary": "Real rates are rising."},
                    "global_liquidity": {"state": "Contracting", "summary": "Liquidity is contracting."},
                    "volatility_term_structure": {"state": "Backwardation", "summary": "Volatility curve is backwardated."},
                    "manufacturing_momentum": {"state": "Weakening", "summary": "Manufacturing is weakening."},
                    "leadership_rotation": {"state": "Defensive", "summary": "Leadership is defensive."},
                    "commodity_pressure": {"state": "Inflationary", "summary": "Commodity pressure is inflationary."},
                }
            },
        )

        self.assertGreater(probabilities["RECESSION RISK"], probabilities["LIQUIDITY EXPANSION"])
        self.assertGreater(probabilities["LIQUIDITY WITHDRAWAL"], probabilities["NEUTRAL"])
        self.assertIn("yield curve inversion", " ".join(drivers["RECESSION RISK"]))
        self.assertIn("global liquidity contracting", " ".join(drivers["LIQUIDITY WITHDRAWAL"]))


if __name__ == "__main__":
    unittest.main()

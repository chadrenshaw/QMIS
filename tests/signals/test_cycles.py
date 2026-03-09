import sys
import unittest
import tempfile
import json
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

    def _cycle_inputs(self, *, end_date: str = "2026-03-08", peak: bool = False) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object], pd.Timestamp]:
        ts = pd.Timestamp(end_date)
        dates = pd.date_range(end=ts, periods=4018, freq="D")
        base = 95.0 + 75.0 * np.sin(np.linspace(-np.pi, np.pi * 1.2, len(dates)))
        if peak:
            tail = np.concatenate([np.linspace(165.0, 210.0, 45), np.linspace(210.0, 207.0, 45)])
            base[-90:] = tail
        else:
            base[-90:] = np.linspace(95.0, 188.0, 90)
        sunspots = pd.DataFrame(
            {
                "ts": dates,
                "source": ["test"] * len(dates),
                "category": ["astronomy"] * len(dates),
                "series_name": ["sunspot_number"] * len(dates),
                "value": base,
                "unit": ["count"] * len(dates),
                "metadata": ["{}"] * len(dates),
            }
        )
        latest_signals = pd.DataFrame(
            [
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "lunar_cycle_day", "value": 20.0, "unit": "days", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "sunspot_number", "value": float(base[-1]), "unit": "count", "metadata": "{}"},
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": 2.0, "pct_change_90d": 4.0, "pct_change_365d": 8.0, "zscore_30d": 0.5, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "m2_money_supply", "pct_change_30d": 1.0, "pct_change_90d": 2.0, "pct_change_365d": 5.0, "zscore_30d": 0.3, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -0.5, "trend_label": "UP"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": -6.0, "pct_change_90d": -10.0, "pct_change_365d": -18.0, "zscore_30d": -0.8, "volatility_30d": 0.2, "slope_30d": -0.3, "drawdown_90d": -8.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "real_yields", "pct_change_30d": -0.3, "pct_change_90d": -0.6, "pct_change_365d": -1.1, "zscore_30d": -0.2, "volatility_30d": 0.1, "slope_30d": -0.1, "drawdown_90d": -0.2, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "dollar_index", "pct_change_30d": -1.4, "pct_change_90d": -2.8, "pct_change_365d": -4.0, "zscore_30d": -0.5, "volatility_30d": 0.1, "slope_30d": -0.1, "drawdown_90d": -1.0, "trend_label": "DOWN"},
            ]
        )
        liquidity_environment = {
            "ts": ts,
            "liquidity_score": 67.0,
            "liquidity_state": "EXPANDING",
            "summary": "Liquidity is expanding as balance-sheet support improves and real yields soften.",
            "components": {
                "fed_balance_sheet": {"score": 0.4},
                "m2_money_supply": {"score": 0.2},
                "reverse_repo_usage": {"score": 0.6},
                "real_yields": {"score": 0.2},
                "dollar_index": {"score": 0.2},
            },
            "missing_inputs": [],
        }
        signals = pd.concat([sunspots, latest_signals], ignore_index=True)
        return signals, features, liquidity_environment, ts

    def test_build_cycle_snapshots_classifies_environmental_cycles(self) -> None:
        from qmis.signals.cycles import build_cycle_snapshots

        signals, features, liquidity_environment, ts = self._cycle_inputs()

        cycles = build_cycle_snapshots(signals=signals, features=features, liquidity_environment=liquidity_environment, as_of=ts)

        self.assertEqual(set(cycles["cycle_name"]), {"solar_cycle", "lunar_cycle", "macro_liquidity_cycle"})
        solar = cycles.loc[cycles["cycle_name"] == "solar_cycle"].iloc[0].to_dict()
        lunar = cycles.loc[cycles["cycle_name"] == "lunar_cycle"].iloc[0].to_dict()
        liquidity = cycles.loc[cycles["cycle_name"] == "macro_liquidity_cycle"].iloc[0].to_dict()
        self.assertEqual(solar["phase"], "rising")
        self.assertFalse(bool(solar["is_turning_point"]))
        self.assertTrue(bool(solar["alert_on_transition"]))
        self.assertEqual(lunar["phase"], "waning_gibbous")
        self.assertFalse(bool(lunar["alert_on_transition"]))
        self.assertEqual(liquidity["phase"], "expanding")
        self.assertIn("liquidity", liquidity["summary"].lower())

    def test_build_cycle_snapshots_marks_solar_phase_transitions(self) -> None:
        from qmis.signals.cycles import build_cycle_snapshots

        signals, features, liquidity_environment, ts = self._cycle_inputs(peak=True)
        previous_cycles = pd.DataFrame(
            [
                {
                    "ts": ts - pd.Timedelta(days=1),
                    "cycle_name": "solar_cycle",
                    "phase": "rising",
                    "strength": 0.71,
                    "is_turning_point": False,
                    "transition_from": None,
                    "alert_on_transition": True,
                    "summary": "Solar activity is rising.",
                    "supporting_signals": json.dumps(["sunspot_number"]),
                    "metadata": json.dumps({}),
                }
            ]
        )

        cycles = build_cycle_snapshots(
            signals=signals,
            features=features,
            liquidity_environment=liquidity_environment,
            previous_cycles=previous_cycles,
            as_of=ts,
        )

        solar = cycles.loc[cycles["cycle_name"] == "solar_cycle"].iloc[0].to_dict()
        self.assertEqual(solar["phase"], "peak")
        self.assertTrue(bool(solar["is_turning_point"]))
        self.assertEqual(solar["transition_from"], "rising")

    def test_materialize_cycle_snapshots_persists_latest_rows(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db
        from qmis.signals.cycles import materialize_cycle_snapshots

        signals, features, liquidity_environment, _ = self._cycle_inputs()

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                for table_name, payload in (
                    ("signals", signals),
                    ("features", features),
                    ("liquidity_snapshots", pd.DataFrame([{
                        "ts": liquidity_environment["ts"],
                        "liquidity_score": liquidity_environment["liquidity_score"],
                        "liquidity_state": liquidity_environment["liquidity_state"],
                        "summary": liquidity_environment["summary"],
                        "components": json.dumps(liquidity_environment["components"]),
                        "missing_inputs": json.dumps(liquidity_environment["missing_inputs"]),
                    }])),
                ):
                    connection.register(f"{table_name}_df", payload)
                    connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                    connection.unregister(f"{table_name}_df")

            inserted_rows = materialize_cycle_snapshots(db_path=db_path)

            connection = __import__("duckdb").connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT cycle_name, phase, transition_from, alert_on_transition
                    FROM cycle_snapshots
                    ORDER BY cycle_name
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 3)
        self.assertEqual(len(persisted), 3)
        self.assertEqual(set(persisted["cycle_name"]), {"solar_cycle", "lunar_cycle", "macro_liquidity_cycle"})


if __name__ == "__main__":
    unittest.main()

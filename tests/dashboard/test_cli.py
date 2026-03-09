import io
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from rich.console import Console


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISDashboardCLITests(unittest.TestCase):
    def _seed_dashboard_state(self, db_path: Path) -> None:
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db

        bootstrap_database(db_path)
        ts = pd.Timestamp("2026-03-08")

        signals = pd.DataFrame(
            [
                {"ts": ts, "source": "test", "category": "market", "series_name": "gold", "value": 2150.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "oil", "value": 84.5, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "copper", "value": 4.1, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 95000.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "vix", "value": 19.0, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.2, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 3.8, "unit": "percent", "metadata": "{}"},
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "gold", "pct_change_30d": 7.0, "pct_change_90d": 12.0, "pct_change_365d": 20.0, "zscore_30d": 1.1, "volatility_30d": 0.1, "slope_30d": 0.3, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "oil", "pct_change_30d": 8.0, "pct_change_90d": 10.0, "pct_change_365d": 18.0, "zscore_30d": 1.0, "volatility_30d": 0.2, "slope_30d": 0.2, "drawdown_90d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "copper", "pct_change_30d": -3.0, "pct_change_90d": 2.0, "pct_change_365d": 7.0, "zscore_30d": 0.3, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -3.0, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "BTCUSD", "pct_change_30d": 11.0, "pct_change_90d": 14.0, "pct_change_365d": 65.0, "zscore_30d": 1.6, "volatility_30d": 0.4, "slope_30d": 1.0, "drawdown_90d": -8.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "vix", "pct_change_30d": -2.0, "pct_change_90d": -4.0, "pct_change_365d": 1.0, "zscore_30d": -0.4, "volatility_30d": 0.3, "slope_30d": -0.1, "drawdown_90d": -5.0, "trend_label": "SIDEWAYS"},
            ]
        )
        regimes = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "inflation_score": 3,
                    "growth_score": 1,
                    "liquidity_score": 2,
                    "risk_score": 2,
                    "regime_label": "STAGFLATION RISK",
                    "confidence": 0.82,
                }
            ]
        )
        relationships = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "copper",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.74,
                    "p_value": 0.0003,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
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
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.03,
                    "p_value": 0.82,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
                {
                    "ts": ts,
                    "series_x": "sunspot_number",
                    "series_y": "BTCUSD",
                    "window_days": 365,
                    "lag_days": 28,
                    "correlation": 0.63,
                    "p_value": 0.01,
                    "relationship_state": "exploratory",
                    "confidence_label": "exploratory",
                },
            ]
        )

        with connect_db(db_path) as connection:
            for table_name, payload in (
                ("signals", signals),
                ("features", features),
                ("regimes", regimes),
                ("relationships", relationships),
            ):
                connection.register(f"{table_name}_df", payload)
                connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                connection.unregister(f"{table_name}_df")

    def test_load_dashboard_snapshot_reads_derived_state(self) -> None:
        from qmis.alerts.engine import materialize_alerts
        from qmis.dashboard.cli import load_dashboard_snapshot

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            self._seed_dashboard_state(db_path)
            materialize_alerts(db_path=db_path)

            snapshot = load_dashboard_snapshot(db_path=db_path)

        self.assertEqual(snapshot["regime"]["regime_label"], "STAGFLATION RISK")
        self.assertEqual(snapshot["scores"]["inflation_score"], 3)
        self.assertAlmostEqual(snapshot["yield_curve"], 0.4, places=6)
        self.assertEqual(snapshot["yield_curve_state"], "NORMAL")
        self.assertEqual(snapshot["trend_summary"]["gold"]["trend_label"], "UP")
        self.assertEqual(len(snapshot["top_relationships"]), 2)
        self.assertEqual(len(snapshot["lead_lag_relationships"]), 1)
        self.assertEqual(len(snapshot["anomalies"]), 1)
        self.assertEqual(snapshot["alert_summary"]["status"], "active")
        self.assertGreaterEqual(len(snapshot["alerts"]), 2)

    def test_render_dashboard_writes_required_sections(self) -> None:
        from qmis.alerts.engine import materialize_alerts
        from qmis.dashboard.cli import load_dashboard_snapshot, render_dashboard

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            self._seed_dashboard_state(db_path)
            materialize_alerts(db_path=db_path)
            snapshot = load_dashboard_snapshot(db_path=db_path)

            buffer = io.StringIO()
            console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
            render_dashboard(snapshot, console=console)

        output = buffer.getvalue()
        self.assertIn("GLOBAL MACRO DASHBOARD", output)
        self.assertIn("Gold Trend", output)
        self.assertIn("Macro Scores", output)
        self.assertIn("Current Regime", output)
        self.assertIn("STAGFLATION RISK", output)
        self.assertIn("Top Relationships", output)
        self.assertIn("Anomalies", output)
        self.assertIn("Lead-Lag", output)
        self.assertIn("Alerts", output)


if __name__ == "__main__":
    unittest.main()

import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISAPITests(unittest.TestCase):
    def _seed_state(self, db_path: Path) -> None:
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db

        bootstrap_database(db_path)
        ts = pd.Timestamp("2026-03-08")
        prev_ts = pd.Timestamp("2026-03-07")

        signals = pd.DataFrame(
            [
                {"ts": prev_ts, "source": "test", "category": "market", "series_name": "gold", "value": 2100.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "gold", "value": 2150.0, "unit": "usd", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "market", "series_name": "oil", "value": 82.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "oil", "value": 84.5, "unit": "usd", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 93500.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 95000.0, "unit": "usd", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "breadth", "series_name": "sp500_above_200dma", "value": 64.0, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "sp500_above_200dma", "value": 58.0, "unit": "percent", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "breadth", "series_name": "advance_decline_line", "value": 1240.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "advance_decline_line", "value": 910.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_highs", "value": 28.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_lows", "value": 25.0, "unit": "count", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "liquidity", "series_name": "fed_balance_sheet", "value": 7050.0, "unit": "billions_usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "liquidity", "series_name": "fed_balance_sheet", "value": 7100.0, "unit": "billions_usd", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.1, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.2, "unit": "percent", "metadata": "{}"},
                {"ts": prev_ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 3.7, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 3.8, "unit": "percent", "metadata": "{}"},
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "oil", "pct_change_30d": 6.0, "pct_change_90d": 9.0, "pct_change_365d": 18.0, "zscore_30d": 1.0, "volatility_30d": 0.2, "slope_30d": 0.25, "drawdown_90d": -1.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "gold", "pct_change_30d": 7.0, "pct_change_90d": 12.0, "pct_change_365d": 20.0, "zscore_30d": 1.1, "volatility_30d": 0.1, "slope_30d": 0.3, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "BTCUSD", "pct_change_30d": 8.0, "pct_change_90d": 14.0, "pct_change_365d": 52.0, "zscore_30d": 1.3, "volatility_30d": 0.4, "slope_30d": 0.45, "drawdown_90d": -6.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": 1.0, "pct_change_90d": 2.0, "pct_change_365d": 4.0, "zscore_30d": 0.4, "volatility_30d": 0.05, "slope_30d": 0.1, "drawdown_90d": -0.4, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "yield_10y", "pct_change_30d": 0.3, "pct_change_90d": 0.5, "pct_change_365d": 0.8, "zscore_30d": 0.2, "volatility_30d": 0.05, "slope_30d": 0.1, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": -4.0, "pct_change_90d": -6.0, "pct_change_365d": 3.0, "zscore_30d": -0.3, "volatility_30d": 0.2, "slope_30d": -0.1, "drawdown_90d": -4.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "advance_decline_line", "pct_change_30d": -3.0, "pct_change_90d": -4.0, "pct_change_365d": 2.0, "zscore_30d": -0.2, "volatility_30d": 0.2, "slope_30d": -0.1, "drawdown_90d": -3.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_highs", "pct_change_30d": -8.0, "pct_change_90d": -10.0, "pct_change_365d": 4.0, "zscore_30d": -0.5, "volatility_30d": 0.2, "slope_30d": -0.2, "drawdown_90d": -5.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": 9.0, "pct_change_90d": 11.0, "pct_change_365d": -3.0, "zscore_30d": 0.6, "volatility_30d": 0.3, "slope_30d": 0.2, "drawdown_90d": -1.0, "trend_label": "UP"},
            ]
        )
        regimes = pd.DataFrame(
            [
                {
                    "ts": prev_ts,
                    "inflation_score": 2,
                    "growth_score": 2,
                    "liquidity_score": 2,
                    "risk_score": 1,
                    "regime_label": "INFLATIONARY EXPANSION",
                    "confidence": 0.76,
                    "regime_probabilities": '{"INFLATIONARY EXPANSION": 52.0, "DISINFLATION": 18.0, "NEUTRAL": 30.0}',
                    "regime_drivers": '{"INFLATIONARY EXPANSION": ["positive growth and liquidity"]}',
                },
                {
                    "ts": ts,
                    "inflation_score": 3,
                    "growth_score": 1,
                    "liquidity_score": 2,
                    "risk_score": 2,
                    "regime_label": "STAGFLATION RISK",
                    "confidence": 0.82,
                    "regime_probabilities": '{"LIQUIDITY WITHDRAWAL": 29.0, "RECESSION RISK": 25.0, "INFLATIONARY EXPANSION": 18.0, "CRISIS / RISK-OFF": 12.0, "NEUTRAL": 16.0}',
                    "regime_drivers": '{"LIQUIDITY WITHDRAWAL": ["liquidity tightening", "risk elevated"]}',
                }
            ]
        )
        relationships = pd.DataFrame(
            [
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
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": -0.33,
                    "p_value": 0.04,
                    "relationship_state": "weakening",
                    "confidence_label": "tentative",
                },
                {
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.05,
                    "p_value": 0.62,
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
        stress = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "stress_score": 61.0,
                    "stress_level": "HIGH",
                    "summary": "Market stress is HIGH with elevated volatility and breadth deterioration.",
                    "components": '{"vix_level": 0.75, "yield_curve": 0.20, "breadth": 0.66, "anomaly_pressure": 0.40}',
                    "missing_inputs": '["credit"]',
                }
            ]
        )
        liquidity_environment = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "liquidity_score": 58.4,
                    "liquidity_state": "NEUTRAL",
                    "summary": "Liquidity is neutral as balance-sheet expansion is offset by firm real yields.",
                    "components": '{"fed_balance_sheet": {"score": 0.18}, "m2_money_supply": {"score": 0.11}, "reverse_repo_usage": {"score": 0.42}, "dollar_index": {"score": -0.10}, "real_yields": {"score": -0.22}}',
                    "missing_inputs": '[]',
                }
            ]
        )
        breadth_health = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "breadth_score": 51.2,
                    "breadth_state": "WEAKENING",
                    "summary": "Breadth is weakening as participation slips and highs no longer dominate lows.",
                    "components": '{"advance_decline_line": {"score": -0.18}, "sp500_above_200dma": {"score": -0.10}, "new_highs_vs_lows": {"score": -0.06}}',
                    "missing_inputs": '[]',
                }
            ]
        )

        with connect_db(db_path) as connection:
            for table_name, payload in (
                ("signals", signals),
                ("features", features),
                ("stress_snapshots", stress),
                ("breadth_snapshots", breadth_health),
                ("liquidity_snapshots", liquidity_environment),
                ("regimes", regimes),
                ("relationships", relationships),
            ):
                connection.register(f"{table_name}_df", payload)
                connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                connection.unregister(f"{table_name}_df")

    def test_read_only_api_endpoints_return_duckdb_state(self) -> None:
        from qmis.alerts.engine import materialize_alerts
        from qmis.api import create_app

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            self._seed_state(db_path)
            materialize_alerts(db_path=db_path)
            client = TestClient(create_app(db_path=db_path))

            health = client.get("/health")
            regime = client.get("/regime/latest")
            signals = client.get("/signals/latest")
            relationships = client.get("/relationships")
            anomalies = client.get("/anomalies")
            divergences = client.get("/divergences")
            alerts = client.get("/alerts")
            dashboard = client.get("/dashboard")

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")

        self.assertEqual(regime.status_code, 200)
        self.assertEqual(regime.json()["regime_label"], "STAGFLATION RISK")
        self.assertEqual(regime.json()["regime_probabilities"]["LIQUIDITY WITHDRAWAL"], 29.0)

        self.assertEqual(signals.status_code, 200)
        self.assertIn("gold", signals.json()["signals"])

        self.assertEqual(relationships.status_code, 200)
        self.assertEqual(len(relationships.json()["relationships"]), 4)

        self.assertEqual(anomalies.status_code, 200)
        self.assertEqual(anomalies.json()["anomalies"][0]["anomaly_type"], "relationship_break")
        self.assertIn(True, [bool(row["passes_filter"]) for row in anomalies.json()["anomalies"]])
        self.assertEqual(divergences.status_code, 200)
        self.assertEqual(divergences.json()["divergences"][0]["title"], "Gold Rising With Yields")
        self.assertEqual(divergences.json()["divergences"][0]["persistence_label"], "persistent")
        self.assertTrue(divergences.json()["divergences"][0]["passes_filter"])

        self.assertEqual(alerts.status_code, 200)
        self.assertEqual(alerts.json()["summary"]["status"], "active")
        self.assertGreaterEqual(len(alerts.json()["alerts"]), 3)

        self.assertEqual(dashboard.status_code, 200)
        self.assertEqual(dashboard.json()["scores"]["inflation_score"], 3)
        self.assertEqual(dashboard.json()["yield_curve_state"], "NORMAL")
        self.assertEqual(dashboard.json()["freshness"]["status"], "fresh")
        self.assertIn("gold", dashboard.json()["signal_history"])
        self.assertEqual(len(dashboard.json()["signal_history"]["gold"]), 2)
        self.assertEqual(len(dashboard.json()["score_history"]), 2)
        self.assertEqual(dashboard.json()["regime"]["regime_probabilities"]["LIQUIDITY WITHDRAWAL"], 29.0)
        self.assertEqual(dashboard.json()["alert_summary"]["status"], "active")
        self.assertGreaterEqual(len(dashboard.json()["alerts"]), 3)
        self.assertEqual(dashboard.json()["market_stress"]["stress_level"], "HIGH")
        self.assertEqual(dashboard.json()["market_stress"]["missing_inputs"], ["credit"])
        self.assertEqual(dashboard.json()["breadth_health"]["breadth_state"], "WEAKENING")
        self.assertEqual(dashboard.json()["liquidity_environment"]["liquidity_state"], "NEUTRAL")
        self.assertIn("narrative", dashboard.json())
        self.assertIn("gold", dashboard.json()["narrative"]["text"].lower())
        self.assertEqual(dashboard.json()["divergences"][0]["title"], "Gold Rising With Yields")
        self.assertEqual(dashboard.json()["divergences"][0]["persistence_label"], "persistent")
        self.assertTrue(dashboard.json()["divergences"][0]["passes_filter"])
        self.assertIn(True, [bool(row["passes_filter"]) for row in dashboard.json()["anomalies"]])
        self.assertIn("market", dashboard.json()["signal_groups"])
        self.assertIn("gold", dashboard.json()["signal_groups"]["market"])

    def test_create_app_serves_built_frontend_assets_from_same_process(self) -> None:
        from qmis.api import create_app

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            db_path = temp_root / "qmis.duckdb"
            dist_dir = temp_root / "dist"
            assets_dir = dist_dir / "assets"
            assets_dir.mkdir(parents=True)
            (dist_dir / "index.html").write_text("<!doctype html><html><body>QMIS Dashboard</body></html>", encoding="utf-8")
            (assets_dir / "app.js").write_text("console.log('qmis');", encoding="utf-8")

            client = TestClient(create_app(db_path=db_path, web_dist_dir=dist_dir))
            root = client.get("/")
            asset = client.get("/assets/app.js")
            health = client.get("/health")

        self.assertEqual(root.status_code, 200)
        self.assertIn("QMIS Dashboard", root.text)
        self.assertEqual(asset.status_code, 200)
        self.assertIn("console.log('qmis');", asset.text)
        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json()["status"], "ok")


if __name__ == "__main__":
    unittest.main()

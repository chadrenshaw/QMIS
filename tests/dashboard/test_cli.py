import io
import json
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
                {"ts": ts, "source": "test", "category": "market", "series_name": "sp500", "value": 6100.0, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 95000.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "ETHUSD", "value": 5100.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "BTC_dominance", "value": 58.0, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "sp500_above_200dma", "value": 71.0, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "advance_decline_line", "value": 1880.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_highs", "value": 68.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "breadth", "series_name": "new_lows", "value": 14.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "vix", "value": 19.0, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.2, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 3.8, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "pmi", "value": 52.1, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "liquidity", "series_name": "fed_balance_sheet", "value": 7420.0, "unit": "usd_billions", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "liquidity", "series_name": "reverse_repo_usage", "value": 311.0, "unit": "usd_billions", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "solar_longitude", "value": 348.06, "unit": "degrees", "metadata": "{}"},
                {
                    "ts": ts,
                    "source": "test",
                    "category": "astronomy",
                    "series_name": "zodiac_index",
                    "value": 11.0,
                    "unit": "index",
                    "metadata": json.dumps({"zodiac_sign": "Pisces"}),
                },
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "lunar_cycle_day", "value": 20.0, "unit": "days", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "lunar_illumination", "value": 72.09, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "sunspot_number", "value": 156.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "solar_flare_count", "value": 4.0, "unit": "count", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "astronomy", "series_name": "solar_flux_f107", "value": 122.0, "unit": "sfu", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "natural", "series_name": "geomagnetic_kp", "value": 5.0, "unit": "index_points", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "natural", "series_name": "earthquake_count", "value": 21.0, "unit": "count", "metadata": "{}"},
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "gold", "pct_change_30d": 7.0, "pct_change_90d": 12.0, "pct_change_365d": 20.0, "zscore_30d": 1.1, "volatility_30d": 0.1, "slope_30d": 0.3, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "oil", "pct_change_30d": 8.0, "pct_change_90d": 10.0, "pct_change_365d": 18.0, "zscore_30d": 1.0, "volatility_30d": 0.2, "slope_30d": 0.2, "drawdown_90d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "copper", "pct_change_30d": -3.0, "pct_change_90d": 2.0, "pct_change_365d": 7.0, "zscore_30d": 0.3, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -3.0, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "sp500", "pct_change_30d": 4.0, "pct_change_90d": 8.0, "pct_change_365d": 16.0, "zscore_30d": 0.7, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -2.5, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "BTCUSD", "pct_change_30d": 11.0, "pct_change_90d": 14.0, "pct_change_365d": 65.0, "zscore_30d": 1.6, "volatility_30d": 0.4, "slope_30d": 1.0, "drawdown_90d": -8.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "ETHUSD", "pct_change_30d": 9.0, "pct_change_90d": 16.0, "pct_change_365d": 70.0, "zscore_30d": 1.4, "volatility_30d": 0.5, "slope_30d": 0.9, "drawdown_90d": -10.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "BTC_dominance", "pct_change_30d": -6.0, "pct_change_90d": -2.0, "pct_change_365d": 4.0, "zscore_30d": -1.1, "volatility_30d": 0.2, "slope_30d": -0.2, "drawdown_90d": -5.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": 6.0, "pct_change_90d": 9.0, "pct_change_365d": 18.0, "zscore_30d": 0.9, "volatility_30d": 0.2, "slope_30d": 0.3, "drawdown_90d": -4.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "advance_decline_line", "pct_change_30d": 8.0, "pct_change_90d": 10.0, "pct_change_365d": 20.0, "zscore_30d": 1.2, "volatility_30d": 0.2, "slope_30d": 0.4, "drawdown_90d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "new_highs", "pct_change_30d": 16.0, "pct_change_90d": 18.0, "pct_change_365d": 30.0, "zscore_30d": 1.0, "volatility_30d": 0.3, "slope_30d": 0.3, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": -14.0, "pct_change_90d": -18.0, "pct_change_365d": -24.0, "zscore_30d": -0.9, "volatility_30d": 0.3, "slope_30d": -0.2, "drawdown_90d": -2.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "vix", "pct_change_30d": -2.0, "pct_change_90d": -4.0, "pct_change_365d": 1.0, "zscore_30d": -0.4, "volatility_30d": 0.3, "slope_30d": -0.1, "drawdown_90d": -5.0, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "pmi", "pct_change_30d": 7.0, "pct_change_90d": 10.0, "pct_change_365d": 12.0, "zscore_30d": 1.2, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -1.5, "trend_label": "UP"},
                {"ts": ts, "series_name": "yield_10y", "pct_change_30d": 0.2, "pct_change_90d": 0.4, "pct_change_365d": 0.8, "zscore_30d": 0.5, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -0.2, "trend_label": "UP"},
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": 5.0, "pct_change_90d": 7.0, "pct_change_365d": 14.0, "zscore_30d": 0.8, "volatility_30d": 0.1, "slope_30d": 0.1, "drawdown_90d": -2.0, "trend_label": "SIDEWAYS"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": -8.0, "pct_change_90d": -10.0, "pct_change_365d": -30.0, "zscore_30d": -1.4, "volatility_30d": 0.2, "slope_30d": -0.4, "drawdown_90d": -12.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "sunspot_number", "pct_change_30d": 13.0, "pct_change_90d": 18.0, "pct_change_365d": 42.0, "zscore_30d": 1.7, "volatility_30d": 0.4, "slope_30d": 0.7, "drawdown_90d": -6.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "solar_flare_count", "pct_change_30d": 10.0, "pct_change_90d": 15.0, "pct_change_365d": 25.0, "zscore_30d": 1.5, "volatility_30d": 0.3, "slope_30d": 0.4, "drawdown_90d": -4.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "geomagnetic_kp", "pct_change_30d": 6.0, "pct_change_90d": 4.0, "pct_change_365d": 9.0, "zscore_30d": 0.6, "volatility_30d": 0.2, "slope_30d": 0.1, "drawdown_90d": -3.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "earthquake_count", "pct_change_30d": -1.0, "pct_change_90d": 3.0, "pct_change_365d": 5.0, "zscore_30d": 0.1, "volatility_30d": 0.2, "slope_30d": 0.0, "drawdown_90d": -2.0, "trend_label": "SIDEWAYS"},
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
                    "regime_probabilities": json.dumps(
                        {
                            "LIQUIDITY WITHDRAWAL": 34.0,
                            "RECESSION RISK": 28.0,
                            "INFLATIONARY EXPANSION": 16.0,
                            "CRISIS / RISK-OFF": 12.0,
                            "NEUTRAL": 10.0,
                        }
                    ),
                    "regime_drivers": json.dumps(
                        {
                            "LIQUIDITY WITHDRAWAL": ["tightening liquidity factor", "mixed growth"],
                            "RECESSION RISK": ["soft growth score", "elevated volatility"],
                        }
                    ),
                }
            ]
        )
        factors = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "factor_name": "liquidity",
                    "component_rank": 1,
                    "strength": 0.84,
                    "direction": "tightening",
                    "summary": "Fed balance sheet, yield_3m, and reverse_repo_usage are driving a tightening liquidity regime.",
                    "supporting_assets": json.dumps(["fed_balance_sheet", "yield_3m", "reverse_repo_usage"]),
                    "loadings": json.dumps({"fed_balance_sheet": -0.77, "yield_3m": 0.74, "reverse_repo_usage": 0.72}),
                },
                {
                    "ts": ts,
                    "factor_name": "crypto",
                    "component_rank": 2,
                    "strength": 0.63,
                    "direction": "bullish",
                    "summary": "BTCUSD and ETHUSD remain tightly linked inside a crypto-led cycle.",
                    "supporting_assets": json.dumps(["BTCUSD", "ETHUSD", "BTC_dominance"]),
                    "loadings": json.dumps({"BTCUSD": 0.88, "ETHUSD": 0.86, "BTC_dominance": -0.42}),
                },
                {
                    "ts": ts,
                    "factor_name": "volatility",
                    "component_rank": 3,
                    "strength": 0.47,
                    "direction": "stressed",
                    "summary": "VIX and breadth deterioration are reinforcing a volatility regime.",
                    "supporting_assets": json.dumps(["vix", "sp500_above_200dma", "new_lows"]),
                    "loadings": json.dumps({"vix": 0.83, "sp500_above_200dma": -0.71, "new_lows": 0.66}),
                },
            ]
        )
        stress = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "stress_score": 68.0,
                    "stress_level": "HIGH",
                    "summary": "Market stress is HIGH with VIX pressure, inverted curve stress, and weak breadth.",
                    "components": json.dumps(
                        {
                            "vix_level": 0.82,
                            "vix_spike": 0.55,
                            "yield_curve": 0.53,
                            "breadth": 0.64,
                            "anomaly_pressure": 0.60,
                        }
                    ),
                    "missing_inputs": json.dumps(["credit"]),
                }
            ]
        )
        liquidity_environment = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "liquidity_score": 63.5,
                    "liquidity_state": "EXPANDING",
                    "summary": "Liquidity is expanding as reverse repo drains and real yields ease.",
                    "components": json.dumps(
                        {
                            "fed_balance_sheet": {"score": 0.22, "weight": 0.3},
                            "m2_money_supply": {"score": 0.31, "weight": 0.2},
                            "reverse_repo_usage": {"score": 0.64, "weight": 0.2},
                            "dollar_index": {"score": 0.28, "weight": 0.15},
                            "real_yields": {"score": 0.35, "weight": 0.15},
                        }
                    ),
                    "missing_inputs": json.dumps([]),
                }
            ]
        )
        breadth_health = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "breadth_score": 71.2,
                    "breadth_state": "STRONG",
                    "summary": "Breadth is strong with broad participation and positive high-low expansion.",
                    "components": json.dumps(
                        {
                            "advance_decline_line": {"score": 0.58, "weight": 0.3},
                            "sp500_above_200dma": {"score": 0.64, "weight": 0.4},
                            "new_highs_vs_lows": {"score": 0.72, "weight": 0.3},
                        }
                    ),
                    "missing_inputs": json.dumps([]),
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
                    "series_x": "BTCUSD",
                    "series_y": "ETHUSD",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.91,
                    "p_value": 0.0002,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "fed_balance_sheet",
                    "series_y": "BTCUSD",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.79,
                    "p_value": 0.0004,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "vix",
                    "series_y": "sp500_above_200dma",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": -0.76,
                    "p_value": 0.001,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": -0.34,
                    "p_value": 0.03,
                    "relationship_state": "weakening",
                    "confidence_label": "tentative",
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
                    "series_x": "BTCUSD",
                    "series_y": "yield_10y",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": -0.72,
                    "p_value": 0.0006,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "BTCUSD",
                    "series_y": "yield_10y",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.08,
                    "p_value": 0.41,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
                {
                    "ts": ts,
                    "series_x": "ETHUSD",
                    "series_y": "yield_3m",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": -0.71,
                    "p_value": 0.0008,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "ETHUSD",
                    "series_y": "yield_3m",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": 0.02,
                    "p_value": 0.77,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
                {
                    "ts": ts,
                    "series_x": "sunspot_number",
                    "series_y": "BTCUSD",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.82,
                    "p_value": 0.01,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
            ]
        )

        with connect_db(db_path) as connection:
            for table_name, payload in (
                ("signals", signals),
                ("features", features),
                ("factors", factors),
                ("stress_snapshots", stress),
                ("breadth_snapshots", breadth_health),
                ("liquidity_snapshots", liquidity_environment),
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
        self.assertEqual(snapshot["regime"]["regime_probabilities"]["LIQUIDITY WITHDRAWAL"], 34.0)
        self.assertEqual(snapshot["scores"]["inflation_score"], 3)
        self.assertAlmostEqual(snapshot["yield_curve"], 0.4, places=6)
        self.assertEqual(snapshot["yield_curve_state"], "NORMAL")
        self.assertEqual(snapshot["trend_summary"]["gold"]["trend_label"], "UP")
        self.assertIn("breadth", snapshot["signal_groups"])
        self.assertIn("astronomy", snapshot["signal_groups"])
        self.assertIn("natural", snapshot["signal_groups"])
        self.assertEqual(snapshot["signal_summary"]["zodiac_index"]["metadata"]["zodiac_sign"], "Pisces")
        self.assertEqual(len(snapshot["top_relationships"]), 3)
        self.assertEqual(len(snapshot["lead_lag_relationships"]), 0)
        self.assertEqual(len(snapshot["anomalies"]), 3)
        self.assertEqual(snapshot["divergences"][0]["title"], "Gold Rising With Yields")
        self.assertEqual(snapshot["alert_summary"]["status"], "active")
        self.assertGreaterEqual(len(snapshot["alerts"]), 2)
        self.assertEqual(snapshot["factors"][0]["factor_name"], "liquidity")
        self.assertEqual(snapshot["factors"][0]["direction"], "tightening")
        self.assertEqual(snapshot["market_stress"]["stress_level"], "HIGH")
        self.assertEqual(snapshot["market_stress"]["missing_inputs"], ["credit"])
        self.assertEqual(snapshot["breadth_health"]["breadth_state"], "STRONG")
        self.assertEqual(snapshot["liquidity_environment"]["liquidity_state"], "EXPANDING")
        self.assertEqual(
            snapshot["intelligence"]["global_state_line"],
            "Regime: STAGFLATION RISK | Volatility: MODERATE | Liquidity: EXPANDING | Growth: STABLE | Inflation: HOT",
        )
        self.assertIn("Sun: Pisces", snapshot["intelligence"]["cosmic_state_line"])
        self.assertEqual(snapshot["intelligence"]["market_drivers"][0]["title"], "Liquidity Tightening")
        self.assertEqual(snapshot["intelligence"]["market_stress"]["stress_level"], "HIGH")
        self.assertEqual(snapshot["intelligence"]["breadth_health"]["breadth_state"], "STRONG")
        self.assertEqual(snapshot["intelligence"]["liquidity_environment"]["liquidity_state"], "EXPANDING")
        self.assertEqual(snapshot["intelligence"]["regime_probabilities"][0]["label"], "LIQUIDITY WITHDRAWAL")
        self.assertEqual(snapshot["intelligence"]["divergences"][0]["title"], "Gold Rising With Yields")
        self.assertEqual(snapshot["intelligence"]["relationship_shifts"][0]["title"], "Crypto vs Macro Decoupling")
        self.assertEqual(snapshot["intelligence"]["risk_monitor"]["breadth_risk"]["level"], "LOW")
        self.assertEqual(snapshot["intelligence"]["risk_monitor"]["divergence_risk"]["level"], "MODERATE")
        self.assertEqual(snapshot["intelligence"]["risk_monitor"]["systemic_risk"]["level"], "HIGH")

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
        self.assertIn("OPERATOR INTELLIGENCE SNAPSHOT", output)
        self.assertIn("GLOBAL STATE", output)
        self.assertIn("Regime: STAGFLATION RISK", output)
        self.assertIn("MARKET PULSE", output)
        self.assertIn("COSMIC STATE", output)
        self.assertIn("Sun: Pisces", output)
        self.assertIn("Moon: Waning Gibbous", output)
        self.assertIn("Solar: ELEVATED", output)
        self.assertIn("MARKET STRESS", output)
        self.assertIn("HIGH", output)
        self.assertIn("REGIME PROBABILITIES", output)
        self.assertIn("LIQUIDITY WITHDRAWAL", output)
        self.assertIn("BREADTH HEALTH", output)
        self.assertIn("STRONG", output)
        self.assertIn("LIQUIDITY ENVIRONMENT", output)
        self.assertIn("EXPANDING", output)
        self.assertIn("PRIMARY MARKET DRIVERS", output)
        self.assertIn("Liquidity Tightening", output)
        self.assertIn("Crypto Cycle", output)
        self.assertIn("CROSS-MARKET DIVERGENCES", output)
        self.assertIn("Gold Rising With Yields", output)
        self.assertIn("RELATIONSHIP SHIFTS", output)
        self.assertIn("Crypto vs Macro Decoupling", output)
        self.assertIn("RISK MONITOR", output)
        self.assertIn("divergence_risk", output)
        self.assertIn("systemic_risk", output)
        self.assertIn("WARNING SIGNALS", output)
        self.assertIn("EXPERIMENTAL SIGNALS", output)
        self.assertIn("STAGFLATION RISK", output)
        self.assertIn("sunspot_number", output)


if __name__ == "__main__":
    unittest.main()

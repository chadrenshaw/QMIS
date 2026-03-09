import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class OperatorInterpreterTests(unittest.TestCase):
    def _snapshot(self) -> dict[str, object]:
        return {
            "signal_summary": {
                "solar_longitude": {
                    "value": 348.06,
                    "unit": "degrees",
                    "category": "astronomy",
                    "source": "derived_ephemeris",
                    "metadata": {},
                },
                "zodiac_index": {
                    "value": 11.0,
                    "unit": "index",
                    "category": "astronomy",
                    "source": "derived_ephemeris",
                    "metadata": {"zodiac_sign": "Pisces"},
                },
                "lunar_cycle_day": {
                    "value": 20.0,
                    "unit": "days",
                    "category": "astronomy",
                    "source": "derived_ephemeris",
                    "metadata": {},
                },
                "lunar_illumination": {
                    "value": 72.09,
                    "unit": "percent",
                    "category": "astronomy",
                    "source": "derived_ephemeris",
                    "metadata": {},
                },
                "sunspot_number": {
                    "value": 21.0,
                    "unit": "count",
                    "category": "astronomy",
                    "source": "noaa_swpc",
                    "metadata": {},
                },
                "solar_flare_count": {
                    "value": 8.0,
                    "unit": "count",
                    "category": "astronomy",
                    "source": "noaa_swpc",
                    "metadata": {},
                },
                "solar_flux_f107": {
                    "value": 122.0,
                    "unit": "sfu",
                    "category": "astronomy",
                    "source": "noaa_swpc",
                    "metadata": {},
                },
                "geomagnetic_kp": {
                    "value": 2.0,
                    "unit": "index_points",
                    "category": "natural",
                    "source": "noaa_swpc",
                    "metadata": {},
                },
                "earthquake_count": {
                    "value": 251.0,
                    "unit": "count",
                    "category": "natural",
                    "source": "derived_natural",
                    "metadata": {},
                },
                "global_temperature_anomaly": {
                    "value": 0.47,
                    "unit": "celsius_anomaly",
                    "category": "natural",
                    "source": "derived_natural",
                    "metadata": {},
                },
                "solar_wind_speed": {
                    "value": 407.32,
                    "unit": "km_per_s",
                    "category": "natural",
                    "source": "derived_natural",
                    "metadata": {},
                },
                "vix": {
                    "value": 29.49,
                    "unit": "index_points",
                    "category": "market",
                    "source": "yfinance",
                    "metadata": {},
                },
                "fed_balance_sheet": {
                    "value": 6628894.0,
                    "unit": "millions_usd",
                    "category": "liquidity",
                    "source": "fred",
                    "metadata": {},
                },
                "reverse_repo_usage": {
                    "value": 1.51,
                    "unit": "billions_usd",
                    "category": "liquidity",
                    "source": "fred",
                    "metadata": {},
                },
                "m2_money_supply": {
                    "value": 22442.1,
                    "unit": "billions_usd",
                    "category": "macro",
                    "source": "fred",
                    "metadata": {},
                },
                "yield_10y": {
                    "value": 4.13,
                    "unit": "percent",
                    "category": "macro",
                    "source": "fred",
                    "metadata": {},
                },
                "yield_3m": {
                    "value": 3.70,
                    "unit": "percent",
                    "category": "macro",
                    "source": "fred",
                    "metadata": {},
                },
                "pmi": {
                    "value": 49.2,
                    "unit": "index_points",
                    "category": "macro",
                    "source": "fred",
                    "metadata": {},
                },
                "sp500_above_200dma": {
                    "value": 48.11,
                    "unit": "percent",
                    "category": "breadth",
                    "source": "derived_breadth",
                    "metadata": {},
                },
                "BTCUSD": {
                    "value": 66642.79,
                    "unit": "usd",
                    "category": "crypto",
                    "source": "yfinance",
                    "metadata": {},
                },
                "ETHUSD": {
                    "value": 1962.5,
                    "unit": "usd",
                    "category": "crypto",
                    "source": "yfinance",
                    "metadata": {},
                },
            },
            "trend_summary": {
                "vix": {"trend_label": "UP"},
                "fed_balance_sheet": {"trend_label": "SIDEWAYS"},
                "reverse_repo_usage": {"trend_label": "DOWN"},
                "m2_money_supply": {"trend_label": "UP"},
                "pmi": {"trend_label": "DOWN"},
                "sp500_above_200dma": {"trend_label": "DOWN"},
                "BTCUSD": {"trend_label": "DOWN"},
                "ETHUSD": {"trend_label": "SIDEWAYS"},
            },
            "scores": {
                "inflation_score": 2,
                "growth_score": 1,
                "liquidity_score": 0,
                "risk_score": 2,
            },
            "regime": {
                "regime_label": "LIQUIDITY WITHDRAWAL",
                "confidence": 0.51,
            },
            "yield_curve": 0.43,
            "yield_curve_state": "NORMAL",
            "relationships": [
                {
                    "series_x": "BTCUSD",
                    "series_y": "ETHUSD",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.99,
                    "p_value": 0.0001,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "series_x": "fed_balance_sheet",
                    "series_y": "BTCUSD",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.81,
                    "p_value": 0.002,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "series_x": "vix",
                    "series_y": "sp500_above_200dma",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": -0.78,
                    "p_value": 0.004,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "series_x": "sunspot_number",
                    "series_y": "BTCUSD",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.82,
                    "p_value": 0.01,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "series_x": "geomagnetic_kp",
                    "series_y": "BTCUSD",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.52,
                    "p_value": 0.03,
                    "relationship_state": "exploratory",
                    "confidence_label": "exploratory",
                },
            ],
            "anomalies": [
                {
                    "series_x": "BTCUSD",
                    "series_y": "yield_10y",
                    "anomaly_type": "relationship_break",
                    "historical_state": "stable",
                    "current_state": "broken",
                },
                {
                    "series_x": "ETHUSD",
                    "series_y": "yield_3m",
                    "anomaly_type": "relationship_break",
                    "historical_state": "stable",
                    "current_state": "broken",
                },
                {
                    "series_x": "sp500_above_200dma",
                    "series_y": "vix",
                    "anomaly_type": "relationship_break",
                    "historical_state": "stable",
                    "current_state": "broken",
                },
            ],
            "alerts": [
                {
                    "severity": "critical",
                    "alert_type": "relationship_break",
                    "title": "Relationship break detected",
                    "message": "BTCUSD vs yield_10y degraded from stable to broken.",
                },
                {
                    "severity": "critical",
                    "alert_type": "threshold",
                    "title": "Threshold breached",
                    "message": "VIX reached 29.49, above the stress threshold.",
                },
            ],
        }

    def test_interpreter_builds_briefing_summaries(self) -> None:
        from qmis.signals.interpreter import build_operator_snapshot

        snapshot = self._snapshot()
        intelligence = build_operator_snapshot(snapshot)

        self.assertEqual(
            intelligence["global_state_line"],
            "Regime: LIQUIDITY WITHDRAWAL | Volatility: HIGH | Liquidity: TIGHT | Growth: WEAK | Inflation: ELEVATED",
        )
        self.assertIn("Sun: Pisces", intelligence["cosmic_state_line"])
        self.assertIn("Moon: Waning Gibbous", intelligence["cosmic_state_line"])
        self.assertIn("Solar: ELEVATED", intelligence["cosmic_state_line"])
        self.assertEqual([item["label"] for item in intelligence["market_pulse"]], ["Equities", "Crypto", "Energy", "Volatility", "Dollar", "Rates"])
        self.assertEqual(intelligence["market_pulse"][1]["state"], "DOWN")
        self.assertEqual(intelligence["risk_monitor"]["volatility_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["liquidity_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["growth_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["systemic_risk"]["level"], "CRITICAL")
        self.assertTrue(intelligence["experimental_signals"]["visible"])

    def test_interpreter_groups_drivers_shifts_and_warnings(self) -> None:
        from qmis.signals.interpreter import build_operator_snapshot

        snapshot = self._snapshot()
        intelligence = build_operator_snapshot(snapshot)

        self.assertEqual(len(intelligence["market_drivers"]), 3)
        self.assertIn("Crypto factor", intelligence["market_drivers"][0]["title"])
        self.assertEqual(intelligence["relationship_shifts"][0]["title"], "Crypto vs Macro Decoupling")
        self.assertEqual(len(intelligence["warning_signals"]), 3)
        self.assertIn("rising volatility", " ".join(item["title"].lower() for item in intelligence["warning_signals"]))
        self.assertIn("breadth deterioration", " ".join(item["title"].lower() for item in intelligence["warning_signals"]))


if __name__ == "__main__":
    unittest.main()

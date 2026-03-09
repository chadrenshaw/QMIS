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
                "advance_decline_line": {
                    "value": -420.0,
                    "unit": "count",
                    "category": "breadth",
                    "source": "derived_breadth",
                    "metadata": {},
                },
                "new_highs": {
                    "value": 12.0,
                    "unit": "count",
                    "category": "breadth",
                    "source": "derived_breadth",
                    "metadata": {},
                },
                "new_lows": {
                    "value": 88.0,
                    "unit": "count",
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
                "advance_decline_line": {"trend_label": "DOWN"},
                "new_highs": {"trend_label": "DOWN"},
                "new_lows": {"trend_label": "UP"},
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
            "factors": [
                {
                    "factor_name": "liquidity",
                    "component_rank": 1,
                    "strength": 0.82,
                    "direction": "tightening",
                    "summary": "Fed balance sheet contraction and short-rate pressure are dominating cross-asset moves.",
                    "supporting_assets": ["fed_balance_sheet", "yield_3m", "reverse_repo_usage"],
                },
                {
                    "factor_name": "crypto",
                    "component_rank": 2,
                    "strength": 0.71,
                    "direction": "bullish",
                    "summary": "BTCUSD and ETHUSD remain tightly linked inside a crypto-led cycle.",
                    "supporting_assets": ["BTCUSD", "ETHUSD", "crypto_market_cap"],
                },
                {
                    "factor_name": "volatility",
                    "component_rank": 3,
                    "strength": 0.54,
                    "direction": "stressed",
                    "summary": "VIX and weak breadth continue to reinforce a volatility regime.",
                    "supporting_assets": ["vix", "sp500_above_200dma", "new_lows"],
                },
            ],
            "market_stress": {
                "stress_score": 74.0,
                "stress_level": "HIGH",
                "summary": "Market stress is HIGH with elevated VIX, inverted rates, and breadth deterioration.",
                "components": {
                    "vix_level": 0.72,
                    "vix_spike": 0.70,
                    "yield_curve": 0.53,
                    "breadth": 0.68,
                    "anomaly_pressure": 0.80,
                },
                "missing_inputs": ["credit"],
            },
            "liquidity_environment": {
                "liquidity_score": 31.5,
                "liquidity_state": "TIGHTENING",
                "summary": "Liquidity is tightening as real yields and the dollar firm while balance-sheet support fades.",
                "components": {
                    "fed_balance_sheet": {"score": -0.42, "weight": 0.3},
                    "m2_money_supply": {"score": 0.15, "weight": 0.2},
                    "reverse_repo_usage": {"score": 0.48, "weight": 0.2},
                    "dollar_index": {"score": -0.32, "weight": 0.15},
                    "real_yields": {"score": -0.36, "weight": 0.15},
                },
                "missing_inputs": [],
            },
            "breadth_health": {
                "breadth_score": 28.4,
                "breadth_state": "FRAGILE",
                "summary": "Breadth is fragile as participation narrows and new lows dominate.",
                "components": {
                    "advance_decline_line": {"score": -0.55, "weight": 0.3},
                    "sp500_above_200dma": {"score": -0.62, "weight": 0.4},
                    "new_highs_vs_lows": {"score": -0.71, "weight": 0.3},
                },
                "missing_inputs": [],
            },
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
            "Regime: LIQUIDITY WITHDRAWAL | Volatility: HIGH | Liquidity: TIGHTENING | Growth: WEAK | Inflation: ELEVATED",
        )
        self.assertIn("Sun: Pisces", intelligence["cosmic_state_line"])
        self.assertIn("Moon: Waning Gibbous", intelligence["cosmic_state_line"])
        self.assertIn("Solar: ELEVATED", intelligence["cosmic_state_line"])
        self.assertEqual([item["label"] for item in intelligence["market_pulse"]], ["Equities", "Crypto", "Energy", "Volatility", "Dollar", "Rates"])
        self.assertEqual(intelligence["market_pulse"][1]["state"], "DOWN")
        self.assertEqual(intelligence["risk_monitor"]["volatility_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["liquidity_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["breadth_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["growth_risk"]["level"], "HIGH")
        self.assertEqual(intelligence["risk_monitor"]["systemic_risk"]["level"], "CRITICAL")
        self.assertEqual(intelligence["market_stress"]["stress_level"], "HIGH")
        self.assertEqual(intelligence["liquidity_environment"]["liquidity_state"], "TIGHTENING")
        self.assertEqual(intelligence["breadth_health"]["breadth_state"], "FRAGILE")
        self.assertIn("breadth", intelligence["market_stress"]["summary"].lower())
        self.assertTrue(intelligence["experimental_signals"]["visible"])

    def test_interpreter_groups_drivers_shifts_and_warnings(self) -> None:
        from qmis.signals.interpreter import build_operator_snapshot

        snapshot = self._snapshot()
        intelligence = build_operator_snapshot(snapshot)

        self.assertEqual(len(intelligence["market_drivers"]), 3)
        self.assertEqual(intelligence["market_drivers"][0]["title"], "Liquidity Tightening")
        self.assertEqual(intelligence["market_drivers"][1]["title"], "Crypto Cycle")
        self.assertEqual(intelligence["relationship_shifts"][0]["title"], "Crypto vs Macro Decoupling")
        self.assertEqual(len(intelligence["warning_signals"]), 3)
        self.assertIn("rising volatility", " ".join(item["title"].lower() for item in intelligence["warning_signals"]))
        self.assertIn("breadth deterioration", " ".join(item["title"].lower() for item in intelligence["warning_signals"]))


if __name__ == "__main__":
    unittest.main()

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISAlertRuleTests(unittest.TestCase):
    def test_evaluate_alert_rules_emits_expected_alert_classes(self) -> None:
        from qmis.alerts.rules import evaluate_alert_rules

        latest_regime = {
            "ts": pd.Timestamp("2026-03-08"),
            "inflation_score": 3,
            "growth_score": 1,
            "liquidity_score": 2,
            "risk_score": 3,
            "regime_label": "CRISIS / RISK-OFF",
            "confidence": 0.9,
        }
        previous_regime = {
            "ts": pd.Timestamp("2026-03-07"),
            "inflation_score": 2,
            "growth_score": 2,
            "liquidity_score": 2,
            "risk_score": 1,
            "regime_label": "INFLATIONARY EXPANSION",
            "confidence": 0.74,
        }
        latest_signals = {
            "yield_10y": {"ts": pd.Timestamp("2026-03-08"), "value": 4.1, "unit": "percent"},
            "yield_3m": {"ts": pd.Timestamp("2026-03-08"), "value": 4.4, "unit": "percent"},
            "vix": {"ts": pd.Timestamp("2026-03-08"), "value": 32.0, "unit": "index_points"},
        }
        relationships = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-03-08"),
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
        anomalies = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-03-08"),
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "anomaly_type": "relationship_break",
                    "historical_state": "stable",
                    "current_state": "broken",
                    "historical_window_days": 365,
                    "current_window_days": 30,
                    "historical_correlation": -0.82,
                    "current_correlation": -0.05,
                }
            ]
        )
        cycles = pd.DataFrame(
            [
                {
                    "series_name": "BTCUSD",
                    "period_days": 29.4,
                    "frequency": 1 / 29.4,
                    "power": 17.0,
                    "relative_power": 0.48,
                    "matched_cycle": "lunar_period",
                    "confidence_label": "validated",
                }
            ]
        )

        alerts = evaluate_alert_rules(
            latest_regime=latest_regime,
            previous_regime=previous_regime,
            latest_signals=latest_signals,
            relationships=relationships,
            anomalies=anomalies,
            cycles=cycles,
        )

        self.assertEqual(set(alerts["alert_type"]), {"regime_change", "threshold", "correlation_discovery", "relationship_break", "cycle_match"})
        self.assertIn("regime_change:INFLATIONARY EXPANSION->CRISIS / RISK-OFF", set(alerts["dedupe_key"]))
        self.assertIn("threshold:yield_curve_inversion", set(alerts["dedupe_key"]))
        self.assertIn("threshold:vix_stress", set(alerts["dedupe_key"]))
        self.assertIn("correlation:sunspot_number:BTCUSD:365:28", set(alerts["dedupe_key"]))
        self.assertIn("relationship_break:gold:yield_10y:30", set(alerts["dedupe_key"]))
        self.assertIn("cycle:BTCUSD:lunar_period", set(alerts["dedupe_key"]))


if __name__ == "__main__":
    unittest.main()

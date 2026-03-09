import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISDivergenceTests(unittest.TestCase):
    def test_detect_cross_market_divergences_ranks_canonical_templates(self) -> None:
        from qmis.signals.divergence import detect_cross_market_divergences

        ts = pd.Timestamp("2026-03-08")
        relationships = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": -0.84,
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
                    "correlation": -0.36,
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
                    "correlation": 0.11,
                    "p_value": 0.40,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
                {
                    "ts": ts,
                    "series_x": "sp500",
                    "series_y": "copper",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.77,
                    "p_value": 0.0003,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "sp500",
                    "series_y": "copper",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.21,
                    "p_value": 0.08,
                    "relationship_state": "weakening",
                    "confidence_label": "tentative",
                },
                {
                    "ts": ts,
                    "series_x": "sp500",
                    "series_y": "copper",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.18,
                    "p_value": 0.51,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
                {
                    "ts": ts,
                    "series_x": "BTCUSD",
                    "series_y": "fed_balance_sheet",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": 0.73,
                    "p_value": 0.0004,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "BTCUSD",
                    "series_y": "fed_balance_sheet",
                    "window_days": 90,
                    "lag_days": 0,
                    "correlation": 0.28,
                    "p_value": 0.09,
                    "relationship_state": "weakening",
                    "confidence_label": "tentative",
                },
                {
                    "ts": ts,
                    "series_x": "BTCUSD",
                    "series_y": "fed_balance_sheet",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.06,
                    "p_value": 0.67,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "gold", "trend_label": "UP", "pct_change_30d": 7.2},
                {"ts": ts, "series_name": "yield_10y", "trend_label": "UP", "pct_change_30d": 0.4},
                {"ts": ts, "series_name": "sp500", "trend_label": "UP", "pct_change_30d": 4.6},
                {"ts": ts, "series_name": "copper", "trend_label": "DOWN", "pct_change_30d": -5.1},
                {"ts": ts, "series_name": "BTCUSD", "trend_label": "UP", "pct_change_30d": 10.5},
                {"ts": ts, "series_name": "fed_balance_sheet", "trend_label": "DOWN", "pct_change_30d": -1.1},
            ]
        )

        divergences = detect_cross_market_divergences(relationships=relationships, features=features)

        self.assertEqual(
            list(divergences["divergence_key"]),
            ["gold_vs_yields", "equities_vs_copper", "crypto_vs_liquidity"],
        )
        self.assertEqual(divergences.iloc[0]["title"], "Gold Rising With Yields")
        self.assertEqual(divergences.iloc[0]["observed_direction"], "same_direction_moves")
        self.assertEqual(divergences.iloc[0]["persistence_windows"], 2)
        crypto_row = divergences.loc[divergences["divergence_key"] == "crypto_vs_liquidity"].iloc[0]
        self.assertEqual(crypto_row["title"], "Crypto Decoupling From Liquidity")
        self.assertEqual(crypto_row["observed_direction"], "opposite_moves")
        self.assertGreater(float(divergences.iloc[0]["strength"]), float(divergences.iloc[2]["strength"]))
        self.assertIn("365d", str(divergences.iloc[0]["summary"]))
        self.assertIn("30d", str(divergences.iloc[0]["summary"]))

    def test_detect_cross_market_divergences_keeps_single_window_noise_for_debugging(self) -> None:
        from qmis.signals.divergence import detect_cross_market_divergences

        ts = pd.Timestamp("2026-03-08")
        relationships = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "series_x": "sp500",
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
                    "series_x": "sp500",
                    "series_y": "copper",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": 0.48,
                    "p_value": 0.06,
                    "relationship_state": "weakening",
                    "confidence_label": "tentative",
                },
            ]
        )
        features = pd.DataFrame(
            [
                {"ts": ts, "series_name": "sp500", "trend_label": "UP", "pct_change_30d": 2.3},
                {"ts": ts, "series_name": "copper", "trend_label": "DOWN", "pct_change_30d": -1.4},
            ]
        )

        divergences = detect_cross_market_divergences(relationships=relationships, features=features)

        self.assertEqual(len(divergences), 1)
        self.assertEqual(divergences.iloc[0]["divergence_key"], "equities_vs_copper")
        self.assertEqual(divergences.iloc[0]["persistence_label"], "transient")
        self.assertFalse(bool(divergences.iloc[0]["passes_filter"]))


if __name__ == "__main__":
    unittest.main()

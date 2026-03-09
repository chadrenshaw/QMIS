import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class BayesianRegimeModelTests(unittest.TestCase):
    def _bearish_signals(self) -> dict[str, object]:
        return {
            "scores": {
                "inflation_score": 2,
                "growth_score": 1,
                "liquidity_score": 1,
                "risk_score": 2,
            },
            "breadth_health": {"breadth_state": "WEAKENING", "breadth_score": 46.0},
            "liquidity_environment": {"liquidity_state": "TIGHTENING", "liquidity_score": 37.0},
            "market_stress": {"stress_level": "HIGH", "stress_score": 68.0},
            "macro_pressure": {"pressure_level": "SEVERE PRESSURE", "mpi_score": 78.0},
            "predictive_snapshot": {
                "forward_macro_signals": {
                    "yield_curve": {"state": "Inverted"},
                    "credit_spreads": {"state": "Widening"},
                    "financial_conditions": {"state": "Tightening"},
                    "real_rates": {"state": "Rising"},
                    "global_liquidity": {"state": "Contracting"},
                    "volatility_term_structure": {"state": "Backwardation"},
                    "manufacturing_momentum": {"state": "Weakening"},
                    "leadership_rotation": {"state": "Defensive"},
                    "commodity_pressure": {"state": "Inflationary"},
                }
            },
        }

    def _constructive_signals(self) -> dict[str, object]:
        return {
            "scores": {
                "inflation_score": 0,
                "growth_score": 2,
                "liquidity_score": 3,
                "risk_score": 0,
            },
            "breadth_health": {"breadth_state": "STRONG", "breadth_score": 72.0},
            "liquidity_environment": {"liquidity_state": "EXPANDING", "liquidity_score": 68.0},
            "market_stress": {"stress_level": "LOW", "stress_score": 22.0},
            "macro_pressure": {"pressure_level": "LOW PRESSURE", "mpi_score": 18.0},
            "predictive_snapshot": {
                "forward_macro_signals": {
                    "yield_curve": {"state": "Normal"},
                    "credit_spreads": {"state": "Narrowing"},
                    "financial_conditions": {"state": "Loosening"},
                    "real_rates": {"state": "Falling"},
                    "global_liquidity": {"state": "Expanding"},
                    "volatility_term_structure": {"state": "Contango"},
                    "manufacturing_momentum": {"state": "Improving"},
                    "leadership_rotation": {"state": "Cyclical"},
                    "commodity_pressure": {"state": "Mixed"},
                }
            },
        }

    def test_update_regime_probabilities_returns_normalized_posterior(self) -> None:
        from qmis.models.bayesian_regime import BAYESIAN_REGIMES, update_regime_probabilities

        posterior, evidence = update_regime_probabilities(self._bearish_signals())

        self.assertEqual(set(posterior), set(BAYESIAN_REGIMES))
        self.assertAlmostEqual(sum(posterior.values()), 100.0, places=2)
        self.assertIn("RECESSION RISK", evidence)
        self.assertTrue(evidence["RECESSION RISK"])

    def test_update_regime_probabilities_lifts_bearish_regimes_when_macro_turns_defensive(self) -> None:
        from qmis.models.bayesian_regime import update_regime_probabilities

        posterior, _ = update_regime_probabilities(self._bearish_signals())

        self.assertGreater(posterior["RECESSION RISK"], posterior["LIQUIDITY EXPANSION"])
        self.assertGreater(posterior["LIQUIDITY WITHDRAWAL"], posterior["NEUTRAL"])
        self.assertGreater(posterior["STAGFLATION RISK"], posterior["DISINFLATION"])

    def test_update_regime_probabilities_lifts_constructive_regimes_when_liquidity_and_growth_improve(self) -> None:
        from qmis.models.bayesian_regime import update_regime_probabilities

        posterior, _ = update_regime_probabilities(self._constructive_signals())

        self.assertGreater(posterior["LIQUIDITY EXPANSION"], posterior["LIQUIDITY WITHDRAWAL"])
        self.assertGreater(posterior["DISINFLATION"], posterior["RECESSION RISK"])

    def test_update_regime_probabilities_uses_macro_pressure_evidence(self) -> None:
        from qmis.models.bayesian_regime import update_regime_probabilities

        posterior, evidence = update_regime_probabilities(self._bearish_signals())

        self.assertGreater(posterior["RECESSION RISK"], posterior["DISINFLATION"])
        self.assertIn("macro pressure elevated", " ".join(evidence["RECESSION RISK"]))

    def test_forecast_regime_projects_multiple_horizons_from_transition_matrix(self) -> None:
        from qmis.models.bayesian_regime import (
            compute_regime_transition_probabilities,
            forecast_regime,
        )

        posterior = {
            "LIQUIDITY EXPANSION": 10.0,
            "LIQUIDITY WITHDRAWAL": 46.0,
            "RECESSION RISK": 20.0,
            "STAGFLATION RISK": 12.0,
            "DISINFLATION": 4.0,
            "NEUTRAL": 8.0,
        }
        transition_matrix = compute_regime_transition_probabilities()

        forecast_30d = forecast_regime(posterior, 30, transition_matrix=transition_matrix)
        forecast_90d = forecast_regime(posterior, 90, transition_matrix=transition_matrix)
        forecast_180d = forecast_regime(posterior, 180, transition_matrix=transition_matrix)

        self.assertEqual(forecast_30d["horizon_days"], 30)
        self.assertEqual(forecast_90d["horizon_days"], 90)
        self.assertEqual(forecast_180d["horizon_days"], 180)
        self.assertAlmostEqual(sum(forecast_30d["distribution"].values()), 100.0, places=2)
        self.assertAlmostEqual(sum(forecast_90d["distribution"].values()), 100.0, places=2)
        self.assertAlmostEqual(sum(forecast_180d["distribution"].values()), 100.0, places=2)
        self.assertEqual(forecast_90d["top_regime"], "RECESSION RISK")
        self.assertIn(forecast_180d["top_regime"], {"RECESSION RISK", "LIQUIDITY EXPANSION"})


if __name__ == "__main__":
    unittest.main()

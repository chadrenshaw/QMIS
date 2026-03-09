import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISNarrativeTests(unittest.TestCase):
    def _snapshot(self) -> dict[str, object]:
        return {
            "regime": {
                "regime_label": "LIQUIDITY WITHDRAWAL",
                "regime_probabilities": {"LIQUIDITY WITHDRAWAL": 33.0, "RECESSION RISK": 27.0},
            },
            "market_stress": {
                "stress_level": "HIGH",
                "stress_score": 74.0,
                "summary": "Market stress is HIGH with elevated VIX, inverted rates, and breadth deterioration.",
            },
            "liquidity_environment": {
                "liquidity_state": "TIGHTENING",
                "summary": "Liquidity is tightening as real yields and the dollar firm while balance-sheet support fades.",
            },
            "breadth_health": {
                "breadth_state": "FRAGILE",
                "summary": "Breadth is fragile as participation narrows and new lows dominate.",
            },
            "factors": [
                {
                    "factor_name": "liquidity",
                    "direction": "tightening",
                    "summary": "Strong liquidity driver (tightening) led by fed_balance_sheet, yield_3m, reverse_repo_usage.",
                    "passes_filter": True,
                },
                {
                    "factor_name": "crypto",
                    "direction": "bullish",
                    "summary": "Strong crypto driver (bullish) led by BTCUSD, ETHUSD, crypto_market_cap.",
                    "passes_filter": True,
                },
            ],
            "divergences": [
                {
                    "title": "Crypto Decoupling From Liquidity",
                    "summary": "Crypto is moving opposite to core liquidity proxies.",
                    "passes_filter": True,
                }
            ],
        }

    def test_build_market_narrative_generates_grounded_summary(self) -> None:
        from qmis.signals.narrative import build_market_narrative

        narrative = build_market_narrative(self._snapshot())

        self.assertIn("liquidity tightening", narrative["text"].lower())
        self.assertIn("crypto", narrative["text"].lower())
        self.assertLessEqual(len(narrative["sentences"]), 3)
        self.assertEqual(narrative["evidence"][0]["kind"], "factor")
        self.assertEqual(narrative["evidence"][1]["kind"], "divergence")

    def test_build_market_narrative_adapts_when_factor_context_changes(self) -> None:
        from qmis.signals.narrative import build_market_narrative

        snapshot = self._snapshot()
        snapshot["factors"] = [
            {
                "factor_name": "volatility",
                "direction": "stressed",
                "summary": "Moderate volatility driver (stressed) led by vix, sp500_above_200dma, new_lows.",
                "passes_filter": True,
            }
        ]
        snapshot["divergences"] = []

        narrative = build_market_narrative(snapshot)

        self.assertIn("volatility", narrative["text"].lower())
        self.assertNotIn("crypto-specific", narrative["text"].lower())


if __name__ == "__main__":
    unittest.main()

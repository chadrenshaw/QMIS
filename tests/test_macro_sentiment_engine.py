import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "macro_sentiment_engine.py"
SPEC = importlib.util.spec_from_file_location("macro_sentiment_engine", SCRIPT_PATH)
macro_sentiment_engine = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(macro_sentiment_engine)


class MacroSentimentEngineTests(unittest.TestCase):
    def test_calculate_trends_classifies_multiple_timeframes(self) -> None:
        history = pd.DataFrame(
            {
                "Close": [100.0] * 240 + [95.0] * 9 + [101.0, 106.0, 112.0, 118.0, 124.0, 130.0, 136.0]
            }
        )

        trends = macro_sentiment_engine.calculate_trends({"TEST": history})

        self.assertEqual(trends["TEST"]["12m"]["direction"], "UP")
        self.assertEqual(trends["TEST"]["3m"]["direction"], "UP")
        self.assertEqual(trends["TEST"]["1m"]["direction"], "UP")
        self.assertGreater(trends["TEST"]["3m"]["slope"], 0)

    def test_calculate_macro_scores_uses_expected_rules(self) -> None:
        trends = {
            "GC=F": {"3m": {"direction": "UP"}},
            "CL=F": {"3m": {"direction": "UP"}},
            "^TNX": {"3m": {"direction": "UP"}},
            "TIP": {"3m": {"direction": "UP"}},
            "^GSPC": {"3m": {"direction": "DOWN"}},
            "HG=F": {"3m": {"direction": "UP"}},
            "HYG": {"3m": {"direction": "DOWN"}},
            "^VIX": {"3m": {"direction": "UP"}},
        }

        scores = macro_sentiment_engine.calculate_macro_scores(trends, yield_curve=-0.25)

        self.assertEqual(scores["inflation_score"], 4)
        self.assertEqual(scores["growth_score"], 2)
        self.assertEqual(scores["risk_score"], 4)
        self.assertEqual(scores["yield_curve_state"], "INVERTED")

    def test_determine_regime_prioritizes_risk_off(self) -> None:
        regime = macro_sentiment_engine.determine_regime(
            inflation_score=4,
            growth_score=3,
            risk_score=3,
            yields_falling=False,
        )

        self.assertEqual(regime, "CRISIS / RISK-OFF")

    def test_generate_alerts_emits_requested_conditions(self) -> None:
        trends = {
            "CL=F": {"3m": {"direction": "UP", "percent_change": 12.5}},
            "^GSPC": {"3m": {"direction": "DOWN"}},
            "^VIX": {"3m": {"direction": "UP"}},
            "GC=F": {"3m": {"direction": "UP"}},
            "^TNX": {"3m": {"direction": "DOWN"}},
            "HYG": {"3m": {"direction": "DOWN", "percent_change": -6.5}},
        }
        latest_values = {"^VIX": 28.2}

        alerts = macro_sentiment_engine.generate_alerts(
            trends=trends,
            latest_values=latest_values,
            yield_curve=-0.40,
        )

        self.assertIn("Yield Curve Inversion Detected", alerts)
        self.assertIn("Elevated VIX Above 25", alerts)
        self.assertIn("Rising Oil Prices", alerts)
        self.assertIn("S&P 500 Falling While VIX Rising", alerts)
        self.assertIn("Gold Rising While Yields Falling", alerts)
        self.assertIn("Credit Proxy Collapsing", alerts)

    def test_fetch_market_data_does_not_pass_requests_session_into_yfinance(self) -> None:
        class DummySession:
            def __init__(self) -> None:
                self.closed = False

            class DummyResponse:
                def close(self) -> None:
                    return None

            def get(self, *_args, **_kwargs) -> "DummySession.DummyResponse":
                return self.DummyResponse()

            def close(self) -> None:
                self.closed = True

        dummy_session = DummySession()
        raw = pd.DataFrame(
            {
                ("GC=F", "Close"): [100.0, 101.0],
            },
            index=pd.date_range("2025-01-01", periods=2, freq="D"),
        )
        raw.columns = pd.MultiIndex.from_tuples(raw.columns)
        dummy_history = pd.DataFrame({"Close": [100.0, 101.0]})

        with (
            mock.patch.object(macro_sentiment_engine, "build_http_session", return_value=dummy_session),
            mock.patch.object(macro_sentiment_engine.yf, "download", return_value=raw) as mock_download,
            mock.patch.object(
                macro_sentiment_engine,
                "extract_ticker_history",
                return_value=dummy_history,
            ),
        ):
            histories = macro_sentiment_engine.fetch_market_data(period="1mo")

        self.assertNotIn("session", mock_download.call_args.kwargs)
        self.assertTrue(dummy_session.closed)
        self.assertIn("GC=F", histories)

    def test_normalize_market_value_handles_current_and_legacy_yield_scales(self) -> None:
        self.assertEqual(macro_sentiment_engine.normalize_market_value("^TNX", 4.13), 4.13)
        self.assertAlmostEqual(macro_sentiment_engine.normalize_market_value("^IRX", 35.7), 3.57)

    def test_apply_signal_snapshot_baselines_first_run_without_new_alerts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "macro_state.db"
            snapshot = {
                "oil_3m_trend": {
                    "value": "SIDEWAYS",
                    "label": "Oil 3M Trend",
                    "severity": "warning",
                    "tags": ["oil", "macro"],
                },
                "regime": {
                    "value": "NEUTRAL / MIXED",
                    "label": "Macro Regime",
                    "severity": "critical",
                    "tags": ["regime", "macro"],
                },
            }

            new_events, active_events = macro_sentiment_engine.apply_signal_snapshot(
                db_path=db_path,
                signal_snapshot=snapshot,
                now=pd.Timestamp("2026-03-08T12:00:00Z"),
            )

        self.assertEqual(new_events, [])
        self.assertEqual(active_events, [])

    def test_apply_signal_snapshot_creates_recent_transition_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "macro_state.db"
            base_snapshot = {
                "oil_3m_trend": {
                    "value": "SIDEWAYS",
                    "label": "Oil 3M Trend",
                    "severity": "warning",
                    "tags": ["oil", "macro"],
                },
                "yield_curve_state": {
                    "value": "NORMAL",
                    "label": "Yield Curve",
                    "severity": "critical",
                    "tags": ["rates", "macro"],
                },
            }
            changed_snapshot = {
                "oil_3m_trend": {
                    "value": "UP",
                    "label": "Oil 3M Trend",
                    "severity": "warning",
                    "tags": ["oil", "macro"],
                },
                "yield_curve_state": {
                    "value": "INVERTED",
                    "label": "Yield Curve",
                    "severity": "critical",
                    "tags": ["rates", "macro"],
                },
            }

            macro_sentiment_engine.apply_signal_snapshot(
                db_path=db_path,
                signal_snapshot=base_snapshot,
                now=pd.Timestamp("2026-03-08T12:00:00Z"),
            )
            new_events, active_events = macro_sentiment_engine.apply_signal_snapshot(
                db_path=db_path,
                signal_snapshot=changed_snapshot,
                now=pd.Timestamp("2026-03-08T13:00:00Z"),
            )
            _, later_active_events = macro_sentiment_engine.apply_signal_snapshot(
                db_path=db_path,
                signal_snapshot=changed_snapshot,
                now=pd.Timestamp("2026-03-08T20:00:00Z"),
            )
            _, expired_active_events = macro_sentiment_engine.apply_signal_snapshot(
                db_path=db_path,
                signal_snapshot=changed_snapshot,
                now=pd.Timestamp("2026-03-09T14:01:00Z"),
            )

        self.assertEqual(len(new_events), 2)
        self.assertEqual({event["signal_key"] for event in new_events}, {"oil_3m_trend", "yield_curve_state"})
        self.assertTrue(all(event["from_value"] != event["to_value"] for event in new_events))
        self.assertEqual(len(active_events), 2)
        self.assertEqual(len(later_active_events), 2)
        self.assertEqual(expired_active_events, [])

    def test_send_ntfy_summary_posts_single_consolidated_message(self) -> None:
        class DummyResponse:
            def raise_for_status(self) -> None:
                return None

        events = [
            {
                "signal_key": "oil_3m_trend",
                "label": "Oil 3M Trend",
                "from_value": "SIDEWAYS",
                "to_value": "UP",
                "severity": "warning",
                "tags": ["oil", "macro"],
                "message": "Oil 3M Trend changed: SIDEWAYS -> UP",
            },
            {
                "signal_key": "yield_curve_state",
                "label": "Yield Curve",
                "from_value": "NORMAL",
                "to_value": "INVERTED",
                "severity": "critical",
                "tags": ["rates", "macro"],
                "message": "Yield Curve changed: NORMAL -> INVERTED",
            },
        ]

        with mock.patch.object(
            macro_sentiment_engine.requests,
            "post",
            return_value=DummyResponse(),
        ) as mock_post:
            macro_sentiment_engine.send_ntfy_summary(
                topic_url="https://ntfy.chadlee.org/markets",
                events=events,
                current_regime="STAGFLATION RISK",
            )

        self.assertEqual(mock_post.call_count, 1)
        self.assertEqual(mock_post.call_args.args[0], "https://ntfy.chadlee.org/markets")
        payload = mock_post.call_args.kwargs["data"]
        self.assertIn("Oil 3M Trend changed: SIDEWAYS -> UP", payload)
        self.assertIn("Yield Curve changed: NORMAL -> INVERTED", payload)
        self.assertIn("Current regime: STAGFLATION RISK", payload)

    def test_parse_args_uses_repo_db_path_and_rejects_state_db_flag(self) -> None:
        args = macro_sentiment_engine.parse_args([])

        self.assertEqual(
            args.no_ntfy,
            False,
        )
        self.assertEqual(
            macro_sentiment_engine.DEFAULT_STATE_DB_PATH,
            macro_sentiment_engine.ROOT_DIR / "db" / "macro_sentiment_engine.db",
        )

        with self.assertRaises(SystemExit):
            macro_sentiment_engine.parse_args(
                ["--state-db-path", "/tmp/override.db"]
            )


if __name__ == "__main__":
    unittest.main()

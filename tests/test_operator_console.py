import io
import sys
import unittest
from pathlib import Path
from unittest import mock

from rich.console import Console


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISOperatorConsoleTests(unittest.TestCase):
    def test_dry_run_describes_refresh_and_render_steps(self) -> None:
        script_path = REPO_ROOT / "scripts" / "run_operator_console.py"
        result = __import__("subprocess").run(
            [sys.executable, str(script_path), "--dry-run"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("operator console dry-run", result.stdout.lower())
        self.assertIn("collectors=all", result.stdout.lower())
        self.assertIn("dashboard=render", result.stdout.lower())

    def test_main_refreshes_pipeline_then_renders_dashboard(self) -> None:
        from scripts import run_operator_console

        snapshot = {
            "trend_summary": {},
            "signal_summary": {},
            "signal_groups": {},
            "grouped_signals": {},
            "signal_history": {},
            "scores": {},
            "score_history": [],
            "regime": None,
            "yield_curve": None,
            "yield_curve_state": "UNKNOWN",
            "freshness": {"status": "empty"},
            "latest_snapshot_ts": None,
            "top_relationships": [],
            "lead_lag_relationships": [],
            "anomalies": [],
            "alert_summary": {"status": "clear", "message": "No active alerts"},
            "alerts": [],
        }

        with (
            mock.patch.object(run_operator_console, "run_market_collector", return_value=2) as market,
            mock.patch.object(run_operator_console, "run_crypto_collector", return_value=3) as crypto,
            mock.patch.object(run_operator_console, "run_breadth_collector", return_value=1) as breadth,
            mock.patch.object(run_operator_console, "run_macro_collector", return_value=4) as macro,
            mock.patch.object(run_operator_console, "run_liquidity_collector", return_value=2) as liquidity,
            mock.patch.object(run_operator_console, "run_solar_collector", return_value=2) as solar,
            mock.patch.object(run_operator_console, "run_astronomy_collector", return_value=2) as astronomy,
            mock.patch.object(run_operator_console, "run_natural_collector", return_value=2) as natural,
            mock.patch.object(run_operator_console, "materialize_features", return_value=12) as features,
            mock.patch.object(run_operator_console, "materialize_regime", return_value=1) as regime,
            mock.patch.object(run_operator_console, "materialize_relationships", return_value=5) as relationships,
            mock.patch.object(run_operator_console, "materialize_lead_lag_relationships", return_value=2) as lead_lag,
            mock.patch.object(run_operator_console, "materialize_alerts", return_value=3) as alerts,
            mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot) as load_snapshot,
            mock.patch.object(run_operator_console, "render_dashboard") as render_dashboard,
        ):
            console = Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120)
            exit_code = run_operator_console.main([], console=console)

        self.assertEqual(exit_code, 0)
        for mocked in (
            market,
            crypto,
            breadth,
            macro,
            liquidity,
            solar,
            astronomy,
            natural,
            features,
            regime,
            relationships,
            lead_lag,
            alerts,
            load_snapshot,
            render_dashboard,
        ):
            mocked.assert_called()

    def test_main_can_render_existing_state_without_refresh(self) -> None:
        from scripts import run_operator_console

        snapshot = {
            "trend_summary": {},
            "signal_summary": {},
            "signal_groups": {},
            "grouped_signals": {},
            "signal_history": {},
            "scores": {},
            "score_history": [],
            "regime": None,
            "yield_curve": None,
            "yield_curve_state": "UNKNOWN",
            "freshness": {"status": "empty"},
            "latest_snapshot_ts": None,
            "top_relationships": [],
            "lead_lag_relationships": [],
            "anomalies": [],
            "alert_summary": {"status": "clear", "message": "No active alerts"},
            "alerts": [],
        }

        with (
            mock.patch.object(run_operator_console, "run_market_collector") as market,
            mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot) as load_snapshot,
            mock.patch.object(run_operator_console, "render_dashboard") as render_dashboard,
        ):
            console = Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120)
            exit_code = run_operator_console.main(["--no-refresh"], console=console)

        self.assertEqual(exit_code, 0)
        market.assert_not_called()
        load_snapshot.assert_called_once()
        render_dashboard.assert_called_once()

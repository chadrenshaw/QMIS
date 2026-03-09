import io
import sys
import tempfile
import unittest
from contextlib import ExitStack
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pandas as pd
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

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(run_operator_console, "_latest_collector_run_timestamp", return_value=None))
            stack.enter_context(mock.patch.object(run_operator_console, "_latest_signal_timestamp", return_value=None))
            market = stack.enter_context(mock.patch.object(run_operator_console, "run_market_collector", return_value=2))
            crypto = stack.enter_context(mock.patch.object(run_operator_console, "run_crypto_collector", return_value=3))
            breadth = stack.enter_context(mock.patch.object(run_operator_console, "run_breadth_collector", return_value=1))
            macro = stack.enter_context(mock.patch.object(run_operator_console, "run_macro_collector", return_value=4))
            liquidity = stack.enter_context(mock.patch.object(run_operator_console, "run_liquidity_collector", return_value=2))
            solar = stack.enter_context(mock.patch.object(run_operator_console, "run_solar_collector", return_value=2))
            astronomy = stack.enter_context(mock.patch.object(run_operator_console, "run_astronomy_collector", return_value=2))
            natural = stack.enter_context(mock.patch.object(run_operator_console, "run_natural_collector", return_value=2))
            features = stack.enter_context(mock.patch.object(run_operator_console, "materialize_features", return_value=12))
            regime = stack.enter_context(mock.patch.object(run_operator_console, "materialize_regime", return_value=1))
            breadth_health = stack.enter_context(mock.patch.object(run_operator_console, "materialize_breadth_health", return_value=1))
            liquidity_state = stack.enter_context(mock.patch.object(run_operator_console, "materialize_liquidity_state", return_value=1))
            factors = stack.enter_context(mock.patch.object(run_operator_console, "materialize_factors", return_value=3))
            relationships = stack.enter_context(mock.patch.object(run_operator_console, "materialize_relationships", return_value=5))
            stress = stack.enter_context(mock.patch.object(run_operator_console, "materialize_market_stress", return_value=1))
            cycles = stack.enter_context(mock.patch.object(run_operator_console, "materialize_cycle_snapshots", return_value=3))
            lead_lag = stack.enter_context(mock.patch.object(run_operator_console, "materialize_lead_lag_relationships", return_value=2))
            alerts = stack.enter_context(mock.patch.object(run_operator_console, "materialize_alerts", return_value=3))
            load_snapshot = stack.enter_context(mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot))
            render_dashboard = stack.enter_context(mock.patch.object(run_operator_console, "render_dashboard"))
            console = Console(file=io.StringIO(), force_terminal=False, color_system=None, width=120)
            exit_code = run_operator_console.main(["--force-refresh"], console=console)

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
            breadth_health,
            liquidity_state,
            factors,
            relationships,
            stress,
            cycles,
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

    def test_main_logs_calls_and_skips_fresh_collectors(self) -> None:
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
        market_runner = mock.Mock(return_value=2)
        macro_runner = mock.Mock(return_value=4)
        specs = (
            {
                "name": "market",
                "source": "yfinance",
                "series_names": ("gold",),
                "max_age": pd.Timedelta(minutes=15),
                "timeout_seconds": 45,
                "runner": market_runner,
            },
            {
                "name": "macro",
                "source": "fred",
                "series_names": ("yield_10y",),
                "max_age": pd.Timedelta(hours=12),
                "timeout_seconds": 45,
                "runner": macro_runner,
            },
        )

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(run_operator_console, "_collector_specs", return_value=specs))
            stack.enter_context(mock.patch.object(run_operator_console, "_latest_collector_run_timestamp", return_value=None))
            stack.enter_context(
                mock.patch.object(
                    run_operator_console,
                    "_latest_signal_timestamp",
                    side_effect=[pd.Timestamp.now("UTC"), pd.Timestamp.now("UTC") - pd.Timedelta(days=2)],
                )
            )
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_features", return_value=12))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_regime", return_value=1))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_breadth_health", return_value=1))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_liquidity_state", return_value=1))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_factors", return_value=4))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_relationships", return_value=5))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_market_stress", return_value=1))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_cycle_snapshots", return_value=3))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_lead_lag_relationships", return_value=2))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_alerts", return_value=3))
            stack.enter_context(mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot))
            stack.enter_context(mock.patch.object(run_operator_console, "render_dashboard"))
            buffer = io.StringIO()
            console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
            exit_code = run_operator_console.main([], console=console)

        self.assertEqual(exit_code, 0)
        market_runner.assert_not_called()
        macro_runner.assert_called_once()
        output = buffer.getvalue()
        self.assertIn("Skipping market", output)
        self.assertIn("Calling macro", output)

    def test_main_continues_after_collector_timeout(self) -> None:
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
        specs = (
            {
                "name": "macro",
                "source": "fred",
                "series_names": ("yield_10y",),
                "max_age": pd.Timedelta(hours=12),
                "timeout_seconds": 1,
                "runner": mock.Mock(side_effect=TimeoutError("fred timed out")),
            },
        )

        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(run_operator_console, "_collector_specs", return_value=specs))
            stack.enter_context(mock.patch.object(run_operator_console, "_latest_collector_run_timestamp", return_value=None))
            stack.enter_context(mock.patch.object(run_operator_console, "_latest_signal_timestamp", return_value=None))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_features", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_regime", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_breadth_health", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_liquidity_state", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_factors", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_relationships", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_market_stress", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_cycle_snapshots", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_lead_lag_relationships", return_value=0))
            stack.enter_context(mock.patch.object(run_operator_console, "materialize_alerts", return_value=0))
            load_snapshot = stack.enter_context(mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot))
            render_dashboard = stack.enter_context(mock.patch.object(run_operator_console, "render_dashboard"))
            buffer = io.StringIO()
            console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
            exit_code = run_operator_console.main([], console=console)

        self.assertEqual(exit_code, 0)
        self.assertIn("Timed out macro", buffer.getvalue())
        load_snapshot.assert_called_once()
        render_dashboard.assert_called_once()

    def test_main_skips_collectors_using_recent_collector_run_metadata(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db
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
        market_runner = mock.Mock(return_value=2)
        specs = (
            {
                "name": "market",
                "source": "yfinance",
                "series_names": ("gold",),
                "max_age": pd.Timedelta(minutes=15),
                "timeout_seconds": 45,
                "runner": market_runner,
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO collector_runs (collector_name, source, collected_at, status, row_count, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    ["market", "yfinance", datetime.now(UTC).replace(tzinfo=None), "success", 2120, "recent run"],
                )

            with (
                mock.patch.object(run_operator_console, "_collector_specs", return_value=specs),
                mock.patch.object(run_operator_console, "load_config") as load_config,
                mock.patch.object(run_operator_console, "materialize_features", return_value=0),
                mock.patch.object(run_operator_console, "materialize_regime", return_value=0),
                mock.patch.object(run_operator_console, "materialize_breadth_health", return_value=0),
                mock.patch.object(run_operator_console, "materialize_liquidity_state", return_value=0),
                mock.patch.object(run_operator_console, "materialize_factors", return_value=0),
                mock.patch.object(run_operator_console, "materialize_relationships", return_value=0),
                mock.patch.object(run_operator_console, "materialize_market_stress", return_value=0),
                mock.patch.object(run_operator_console, "materialize_cycle_snapshots", return_value=0),
                mock.patch.object(run_operator_console, "materialize_lead_lag_relationships", return_value=0),
                mock.patch.object(run_operator_console, "materialize_alerts", return_value=0),
                mock.patch.object(run_operator_console, "load_dashboard_snapshot", return_value=snapshot),
                mock.patch.object(run_operator_console, "render_dashboard"),
            ):
                load_config.return_value = mock.Mock(
                    repo_root=REPO_ROOT,
                    db_path=db_path,
                )
                buffer = io.StringIO()
                console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
                exit_code = run_operator_console.main([], console=console)

        self.assertEqual(exit_code, 0)
        market_runner.assert_not_called()
        self.assertIn("Skipping market", buffer.getvalue())

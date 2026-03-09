import contextlib
import importlib.util
import io
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _load_script_module(script_name: str):
    script_path = REPO_ROOT / "scripts" / script_name
    spec = importlib.util.spec_from_file_location(f"test_{script_name.replace('.py', '')}", script_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class QMISSchedulingEntrypointTests(unittest.TestCase):
    def test_schedule_manifest_lists_spec_jobs(self) -> None:
        from qmis.scheduling import build_schedule_manifest

        manifest = build_schedule_manifest(REPO_ROOT)

        self.assertEqual(set(manifest), {"collectors", "analysis", "alerts"})
        self.assertEqual(manifest["collectors"][0]["cadence"], "market-15m")
        self.assertIn("scripts/run_collectors.py --cadence market-15m", manifest["collectors"][0]["command"])
        self.assertEqual(manifest["analysis"][0]["cadence"], "daily")
        self.assertIn("scripts/run_analysis.py --cadence daily", manifest["analysis"][0]["command"])
        self.assertEqual(manifest["alerts"][0]["cadence"], "daily")

    def test_run_collectors_market_15m_executes_only_market_group(self) -> None:
        module = _load_script_module("run_collectors.py")
        config = SimpleNamespace(repo_root=REPO_ROOT, db_path=Path("/tmp/qmis.duckdb"))

        with (
            patch.object(module, "load_config", return_value=config),
            patch.object(module, "get_logger"),
            patch.object(module, "run_market_collector", return_value=10) as market,
            patch.object(module, "run_crypto_collector", return_value=20) as crypto,
            patch.object(module, "run_breadth_collector", return_value=30) as breadth,
            patch.object(module, "run_macro_collector", return_value=40) as macro,
            patch.object(module, "run_liquidity_collector", return_value=50) as liquidity,
            patch.object(module, "run_solar_collector", return_value=60) as solar,
            patch.object(module, "run_astronomy_collector", return_value=70) as astronomy,
            patch.object(module, "run_natural_collector", return_value=80) as natural,
        ):
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = module.main(["--cadence", "market-15m"])

        self.assertEqual(exit_code, 0)
        market.assert_called_once()
        crypto.assert_called_once()
        breadth.assert_called_once()
        macro.assert_not_called()
        liquidity.assert_not_called()
        solar.assert_not_called()
        astronomy.assert_not_called()
        natural.assert_not_called()
        self.assertIn("market-15m", stdout.getvalue())

    def test_run_analysis_and_alerts_support_daily_cadence_and_list_jobs(self) -> None:
        analysis_module = _load_script_module("run_analysis.py")
        alerts_module = _load_script_module("run_alerts.py")
        config = SimpleNamespace(repo_root=REPO_ROOT, db_path=Path("/tmp/qmis.duckdb"))

        with (
            patch.object(analysis_module, "load_config", return_value=config),
            patch.object(analysis_module, "get_logger"),
        ):
            analysis_output = io.StringIO()
            with contextlib.redirect_stdout(analysis_output):
                analysis_exit = analysis_module.main(["--dry-run", "--cadence", "daily"])
        self.assertEqual(analysis_exit, 0)
        self.assertIn("daily", analysis_output.getvalue())

        with (
            patch.object(alerts_module, "load_config", return_value=config),
            patch.object(alerts_module, "get_logger"),
        ):
            alerts_output = io.StringIO()
            with contextlib.redirect_stdout(alerts_output):
                alerts_exit = alerts_module.main(["--list-jobs"])
        self.assertEqual(alerts_exit, 0)
        self.assertIn("scripts/run_alerts.py --cadence daily", alerts_output.getvalue())


if __name__ == "__main__":
    unittest.main()

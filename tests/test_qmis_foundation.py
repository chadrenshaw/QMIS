import subprocess
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISFoundationTests(unittest.TestCase):
    def test_load_config_uses_repo_root_paths(self) -> None:
        from qmis.config import load_config

        config = load_config()

        self.assertEqual(config.repo_root, REPO_ROOT)
        self.assertEqual(config.db_path, REPO_ROOT / "db" / "qmis.duckdb")
        self.assertEqual(config.log_dir, REPO_ROOT / "logs")

    def test_namespace_packages_import(self) -> None:
        import qmis.alerts
        import qmis.collectors
        import qmis.dashboard
        import qmis.features
        import qmis.signals

        self.assertEqual(qmis.collectors.__name__, "qmis.collectors")
        self.assertEqual(qmis.features.__name__, "qmis.features")
        self.assertEqual(qmis.signals.__name__, "qmis.signals")
        self.assertEqual(qmis.alerts.__name__, "qmis.alerts")
        self.assertEqual(qmis.dashboard.__name__, "qmis.dashboard")

    def test_runtime_scripts_support_dry_run(self) -> None:
        for script_name in ("run_collectors.py", "run_analysis.py", "run_alerts.py", "run_dashboard.py"):
            script_path = REPO_ROOT / "scripts" / script_name
            result = subprocess.run(
                [sys.executable, str(script_path), "--dry-run"],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, msg=f"{script_name} failed: {result.stderr}")
            self.assertIn("dry-run", result.stdout.lower())


if __name__ == "__main__":
    unittest.main()

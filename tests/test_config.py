import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISConfigTests(unittest.TestCase):
    def test_load_config_supports_env_overrides_for_runtime_paths(self) -> None:
        from qmis.config import load_config

        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = Path(temp_dir) / "runtime"
            web_dist_dir = Path(temp_dir) / "web-dist"

            with patch.dict(
                os.environ,
                {
                    "QMIS_DATA_ROOT": str(data_root),
                    "QMIS_WEB_DIST_DIR": str(web_dist_dir),
                },
                clear=False,
            ):
                config = load_config()

        self.assertEqual(config.repo_root, REPO_ROOT)
        self.assertEqual(config.data_root, data_root)
        self.assertEqual(config.db_path, data_root / "db" / "qmis.duckdb")
        self.assertEqual(config.log_dir, data_root / "logs")
        self.assertEqual(config.data_dir, data_root / "data")
        self.assertEqual(config.web_dist_dir, web_dist_dir)


if __name__ == "__main__":
    unittest.main()

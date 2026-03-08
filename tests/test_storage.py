import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISStorageTests(unittest.TestCase):
    def test_connect_db_creates_parent_directory_and_file(self) -> None:
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "nested" / "qmis.duckdb"

            connection = connect_db(db_path)
            try:
                self.assertTrue(db_path.parent.exists())
                self.assertTrue(db_path.exists())
                self.assertIsInstance(connection, duckdb.DuckDBPyConnection)
            finally:
                connection.close()

    def test_get_default_db_path_uses_repo_local_qmis_db(self) -> None:
        from qmis.storage import get_default_db_path

        self.assertEqual(get_default_db_path(), REPO_ROOT / "db" / "qmis.duckdb")


if __name__ == "__main__":
    unittest.main()

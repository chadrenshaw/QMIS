import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISPersistenceTests(unittest.TestCase):
    def test_replace_signal_rows_replaces_existing_series_history_instead_of_appending(self) -> None:
        from qmis.collectors._persistence import replace_signal_rows
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db

        payload = pd.DataFrame(
            [
                {
                    "ts": pd.Timestamp("2026-03-05").to_pydatetime(),
                    "source": "fred",
                    "category": "macro",
                    "series_name": "yield_10y",
                    "value": 4.10,
                    "unit": "percent",
                    "metadata": '{"series_id":"DGS10"}',
                },
                {
                    "ts": pd.Timestamp("2026-03-06").to_pydatetime(),
                    "source": "fred",
                    "category": "macro",
                    "series_name": "yield_10y",
                    "value": 4.13,
                    "unit": "percent",
                    "metadata": '{"series_id":"DGS10"}',
                },
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)

            with connect_db(db_path) as connection:
                first_inserted = replace_signal_rows(connection, payload, "replacement_df")
            with connect_db(db_path) as connection:
                second_inserted = replace_signal_rows(connection, payload, "replacement_df")

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT ts, source, category, series_name, value, unit, metadata
                    FROM signals
                    ORDER BY ts
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(first_inserted, 2)
        self.assertEqual(second_inserted, 2)
        self.assertEqual(len(persisted), 2)


if __name__ == "__main__":
    unittest.main()

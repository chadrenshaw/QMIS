import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISSchemaTests(unittest.TestCase):
    def test_bootstrap_database_creates_all_required_tables(self) -> None:
        from qmis.schema import bootstrap_database

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            bootstrap_database(db_path)

            connection = __import__("duckdb").connect(str(db_path), read_only=True)
            try:
                tables = {
                    row[0]
                    for row in connection.execute("SHOW TABLES").fetchall()
                }
            finally:
                connection.close()

        self.assertEqual(tables, {"features", "regimes", "relationships", "signals"})

    def test_bootstrap_database_applies_spec_columns(self) -> None:
        from qmis.schema import bootstrap_database

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)

            connection = __import__("duckdb").connect(str(db_path), read_only=True)
            try:
                signals_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('signals')").fetchall()
                }
                features_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('features')").fetchall()
                }
                relationships_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('relationships')").fetchall()
                }
                regimes_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('regimes')").fetchall()
                }
            finally:
                connection.close()

        self.assertEqual(
            signals_columns,
            {"ts", "source", "category", "series_name", "value", "unit", "metadata"},
        )
        self.assertEqual(
            features_columns,
            {
                "ts",
                "series_name",
                "pct_change_30d",
                "pct_change_90d",
                "pct_change_365d",
                "zscore_30d",
                "volatility_30d",
                "slope_30d",
                "drawdown_90d",
                "trend_label",
            },
        )
        self.assertEqual(
            relationships_columns,
            {
                "ts",
                "series_x",
                "series_y",
                "window_days",
                "lag_days",
                "correlation",
                "p_value",
                "relationship_state",
            },
        )
        self.assertEqual(
            regimes_columns,
            {
                "ts",
                "inflation_score",
                "growth_score",
                "liquidity_score",
                "risk_score",
                "regime_label",
                "confidence",
            },
        )


if __name__ == "__main__":
    unittest.main()

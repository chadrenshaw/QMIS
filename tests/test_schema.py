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

        self.assertEqual(tables, {"alerts", "collector_runs", "factors", "features", "regimes", "relationships", "signals", "stress_snapshots"})

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
                factors_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('factors')").fetchall()
                }
                stress_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('stress_snapshots')").fetchall()
                }
                relationships_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('relationships')").fetchall()
                }
                regimes_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('regimes')").fetchall()
                }
                alerts_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('alerts')").fetchall()
                }
                collector_runs_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info('collector_runs')").fetchall()
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
            factors_columns,
            {
                "ts",
                "factor_name",
                "component_rank",
                "strength",
                "direction",
                "summary",
                "supporting_assets",
                "loadings",
            },
        )
        self.assertEqual(
            stress_columns,
            {
                "ts",
                "stress_score",
                "stress_level",
                "summary",
                "components",
                "missing_inputs",
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
                "confidence_label",
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
        self.assertEqual(
            alerts_columns,
            {
                "ts",
                "alert_type",
                "severity",
                "rule_key",
                "dedupe_key",
                "title",
                "message",
                "source_table",
                "series_name",
                "series_x",
                "series_y",
                "metadata",
            },
        )
        self.assertEqual(
            collector_runs_columns,
            {
                "collector_name",
                "source",
                "collected_at",
                "status",
                "row_count",
                "message",
            },
        )


if __name__ == "__main__":
    unittest.main()

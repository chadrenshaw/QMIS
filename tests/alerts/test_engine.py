import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISAlertEngineTests(unittest.TestCase):
    def _seed_state(self, db_path: Path) -> None:
        from qmis.schema import bootstrap_database
        from qmis.storage import connect_db

        bootstrap_database(db_path)
        ts_prev = pd.Timestamp("2026-03-07")
        ts = pd.Timestamp("2026-03-08")

        signals = pd.DataFrame(
            [
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_10y", "value": 4.1, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "macro", "series_name": "yield_3m", "value": 4.4, "unit": "percent", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "market", "series_name": "vix", "value": 32.0, "unit": "index_points", "metadata": "{}"},
                {"ts": ts_prev, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 90000.0, "unit": "usd", "metadata": "{}"},
                {"ts": ts, "source": "test", "category": "crypto", "series_name": "BTCUSD", "value": 95000.0, "unit": "usd", "metadata": "{}"},
            ]
        )
        regimes = pd.DataFrame(
            [
                {
                    "ts": ts_prev,
                    "inflation_score": 2,
                    "growth_score": 2,
                    "liquidity_score": 2,
                    "risk_score": 1,
                    "regime_label": "INFLATIONARY EXPANSION",
                    "confidence": 0.74,
                    "regime_probabilities": '{"INFLATIONARY EXPANSION": 58.0, "LIQUIDITY EXPANSION": 22.0, "NEUTRAL": 20.0}',
                    "regime_drivers": '{"INFLATIONARY EXPANSION": ["growth and inflation both firm"]}',
                },
                {
                    "ts": ts,
                    "inflation_score": 3,
                    "growth_score": 1,
                    "liquidity_score": 2,
                    "risk_score": 3,
                    "regime_label": "CRISIS / RISK-OFF",
                    "confidence": 0.9,
                    "regime_probabilities": '{"CRISIS / RISK-OFF": 47.0, "RECESSION RISK": 26.0, "LIQUIDITY WITHDRAWAL": 17.0, "NEUTRAL": 10.0}',
                    "regime_drivers": '{"CRISIS / RISK-OFF": ["risk elevated", "growth deteriorating"]}',
                },
            ]
        )
        relationships = pd.DataFrame(
            [
                {
                    "ts": ts,
                    "series_x": "sunspot_number",
                    "series_y": "BTCUSD",
                    "window_days": 365,
                    "lag_days": 28,
                    "correlation": 0.63,
                    "p_value": 0.01,
                    "relationship_state": "exploratory",
                    "confidence_label": "exploratory",
                },
                {
                    "ts": ts_prev,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 365,
                    "lag_days": 0,
                    "correlation": -0.82,
                    "p_value": 0.0001,
                    "relationship_state": "stable",
                    "confidence_label": "validated",
                },
                {
                    "ts": ts,
                    "series_x": "gold",
                    "series_y": "yield_10y",
                    "window_days": 30,
                    "lag_days": 0,
                    "correlation": -0.05,
                    "p_value": 0.62,
                    "relationship_state": "broken",
                    "confidence_label": "likely_spurious",
                },
            ]
        )

        with connect_db(db_path) as connection:
            for table_name, payload in (
                ("signals", signals),
                ("regimes", regimes),
                ("relationships", relationships),
            ):
                connection.register(f"{table_name}_df", payload)
                connection.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_df")
                connection.unregister(f"{table_name}_df")

    def test_materialize_alerts_persists_deduplicated_alert_rows(self) -> None:
        from qmis.alerts.engine import materialize_alerts
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            self._seed_state(db_path)

            first_count = materialize_alerts(db_path=db_path)
            second_count = materialize_alerts(db_path=db_path)

            with connect_db(db_path, read_only=True) as connection:
                rows = connection.execute(
                    """
                    SELECT alert_type, dedupe_key, severity, title
                    FROM alerts
                    ORDER BY dedupe_key
                    """
                ).fetchall()

        self.assertEqual(first_count, second_count)
        self.assertEqual(len(rows), first_count)
        self.assertGreaterEqual(first_count, 5)
        self.assertEqual(len({row[1] for row in rows}), len(rows))
        self.assertIn(("threshold", "threshold:vix_stress", "critical", "Threshold breached"), rows)


if __name__ == "__main__":
    unittest.main()

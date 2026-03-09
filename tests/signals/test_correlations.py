import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISCorrelationTests(unittest.TestCase):
    def _build_signal_frame(self) -> pd.DataFrame:
        dates = pd.date_range("2020-01-01", periods=2000, freq="D")
        trend = np.linspace(100.0, 400.0, num=len(dates))
        rng = np.random.default_rng(7)

        gold = trend + np.sin(np.arange(len(dates)) / 30.0)
        copper = gold * 1.2 + np.cos(np.arange(len(dates)) / 25.0)
        sunspot_number = gold * 0.8 + np.sin(np.arange(len(dates)) / 15.0)
        btc = sunspot_number * 3.0 + np.cos(np.arange(len(dates)) / 20.0)
        vix = -gold.copy()
        vix[-30:] = 150.0 + rng.normal(0.0, 6.0, size=30)

        rows: list[dict[str, object]] = []
        series_payload = {
            "gold": ("market", gold, "{}"),
            "copper": ("market", copper, "{}"),
            "sunspot_number": ("natural", sunspot_number, '{"exploratory": true}'),
            "btc": ("crypto", btc, "{}"),
            "vix": ("market", vix, "{}"),
        }
        for series_name, (category, values, metadata) in series_payload.items():
            for ts, value in zip(dates, values, strict=True):
                rows.append(
                    {
                        "ts": ts,
                        "source": "test",
                        "category": category,
                        "series_name": series_name,
                        "value": float(value),
                        "unit": "index",
                        "metadata": metadata,
                    }
                )
        return pd.DataFrame(rows)

    def _select_row(self, frame: pd.DataFrame, series_a: str, series_b: str, window_days: int) -> dict:
        mask = (
            (frame["window_days"] == window_days)
            & (
                ((frame["series_x"] == series_a) & (frame["series_y"] == series_b))
                | ((frame["series_x"] == series_b) & (frame["series_y"] == series_a))
            )
        )
        row = frame.loc[mask]
        self.assertEqual(len(row), 1)
        return row.iloc[0].to_dict()

    def test_build_relationship_frame_assigns_confidence_and_states(self) -> None:
        from qmis.signals.correlations import build_relationship_frame

        signals = self._build_signal_frame()

        relationships = build_relationship_frame(signals)

        stable_row = self._select_row(relationships, "gold", "copper", 1825)
        self.assertGreater(stable_row["correlation"], 0.95)
        self.assertLess(stable_row["p_value"], 0.05)
        self.assertEqual(stable_row["relationship_state"], "stable")
        self.assertEqual(stable_row["confidence_label"], "validated")

        exploratory_row = self._select_row(relationships, "btc", "sunspot_number", 365)
        self.assertGreater(exploratory_row["correlation"], 0.6)
        self.assertEqual(exploratory_row["relationship_state"], "exploratory")
        self.assertEqual(exploratory_row["confidence_label"], "exploratory")

        broken_row = self._select_row(relationships, "gold", "vix", 30)
        self.assertLess(abs(broken_row["correlation"]), 0.6)
        self.assertEqual(broken_row["relationship_state"], "broken")
        self.assertEqual(broken_row["confidence_label"], "likely_spurious")

    def test_materialize_relationships_replaces_rows_in_duckdb(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.correlations import materialize_relationships
        from qmis.storage import connect_db

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                payload = self._build_signal_frame()
                connection.register("signals_df", payload)
                connection.execute(
                    """
                    INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
                    SELECT ts, source, category, series_name, value, unit, metadata
                    FROM signals_df
                    """
                )
                connection.unregister("signals_df")

            inserted_rows = materialize_relationships(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT *
                    FROM relationships
                    ORDER BY window_days, series_x, series_y
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertGreater(inserted_rows, 0)
        self.assertGreater(len(persisted), 0)
        stable_row = self._select_row(persisted, "gold", "copper", 365)
        self.assertEqual(stable_row["relationship_state"], "stable")
        self.assertEqual(stable_row["confidence_label"], "validated")


if __name__ == "__main__":
    unittest.main()

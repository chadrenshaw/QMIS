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


class QMISLeadLagTests(unittest.TestCase):
    def _build_signal_frame(self) -> pd.DataFrame:
        dates = pd.date_range("2024-01-01", periods=500, freq="D")
        base = np.sin(np.arange(len(dates)) / 12.0) + np.linspace(0.0, 1.0, len(dates))
        lag_days = 14
        follower = np.concatenate([np.full(lag_days, base[0]), base[:-lag_days]])

        rows: list[dict[str, object]] = []
        for series_name, values in {"sunspot_number": base, "btc": follower}.items():
            for ts, value in zip(dates, values, strict=True):
                rows.append(
                    {
                        "ts": ts,
                        "source": "test",
                        "category": "natural" if series_name == "sunspot_number" else "crypto",
                        "series_name": series_name,
                        "value": float(value),
                        "unit": "index",
                        "metadata": '{"exploratory": true}' if series_name == "sunspot_number" else "{}",
                    }
                )
        return pd.DataFrame(rows)

    def test_build_lead_lag_frame_finds_best_positive_lag(self) -> None:
        from qmis.signals.leadlag import build_lead_lag_frame

        lead_lag = build_lead_lag_frame(self._build_signal_frame(), windows=(365,), max_lag=30)

        self.assertEqual(len(lead_lag), 1)
        row = lead_lag.iloc[0].to_dict()
        self.assertEqual({row["series_x"], row["series_y"]}, {"sunspot_number", "btc"})
        self.assertGreaterEqual(row["lag_days"], 12)
        self.assertLessEqual(row["lag_days"], 16)
        self.assertGreater(abs(row["correlation"]), 0.9)
        self.assertEqual(row["relationship_state"], "exploratory")
        self.assertEqual(row["confidence_label"], "exploratory")

    def test_materialize_lead_lag_relationships_persists_nonzero_lags(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.leadlag import materialize_lead_lag_relationships
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

            inserted_rows = materialize_lead_lag_relationships(db_path=db_path, windows=(365,), max_lag=30)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    "SELECT * FROM relationships WHERE lag_days <> 0"
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 1)
        self.assertEqual(len(persisted), 1)
        self.assertGreater(abs(float(persisted.iloc[0]["correlation"])), 0.9)


if __name__ == "__main__":
    unittest.main()

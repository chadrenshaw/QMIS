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


class QMISFeatureMaterializationTests(unittest.TestCase):
    def _build_signal_frame(self) -> pd.DataFrame:
        dates = pd.date_range("2025-01-01", periods=370, freq="D")
        return pd.DataFrame(
            {
                "ts": list(dates) * 2,
                "source": ["test"] * (len(dates) * 2),
                "category": ["market"] * len(dates) + ["macro"] * len(dates),
                "series_name": ["gold"] * len(dates) + ["yield_10y"] * len(dates),
                "value": [100.0 + i for i in range(len(dates))] + [4.0 + i * 0.01 for i in range(len(dates))],
                "unit": ["usd"] * len(dates) + ["percent"] * len(dates),
                "metadata": ["{}"] * (len(dates) * 2),
            }
        )

    def test_materialize_features_replaces_feature_rows_from_signals(self) -> None:
        from qmis.features.normalization import materialize_features
        from qmis.schema import bootstrap_database
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

            inserted_rows = materialize_features(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT *
                    FROM features
                    WHERE ts = '2026-01-05 00:00:00'
                    ORDER BY series_name
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 740)
        self.assertEqual(len(persisted), 2)
        gold_row = persisted.loc[persisted["series_name"] == "gold"].iloc[0].to_dict()
        self.assertAlmostEqual(gold_row["pct_change_30d"], 6.8337129841, places=6)
        self.assertAlmostEqual(gold_row["pct_change_90d"], 23.7467018470, places=6)
        self.assertAlmostEqual(gold_row["pct_change_365d"], 350.9615384615, places=6)
        self.assertEqual(gold_row["trend_label"], "UP")
        self.assertFalse(pd.isna(gold_row["zscore_30d"]))
        self.assertFalse(pd.isna(gold_row["volatility_30d"]))
        self.assertFalse(pd.isna(gold_row["slope_30d"]))
        self.assertAlmostEqual(gold_row["drawdown_90d"], 0.0, places=6)


if __name__ == "__main__":
    unittest.main()

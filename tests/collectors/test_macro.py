import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISMacroCollectorTests(unittest.TestCase):
    def _build_macro_series(self) -> dict[str, pd.Series]:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        return {
            "DGS10": pd.Series([4.20, 4.25], index=index),
            "DGS3MO": pd.Series([4.55, 4.50], index=index),
            "M2SL": pd.Series([21800.0, 21820.0], index=index),
            "WALCL": pd.Series([6765000.0, 6768000.0], index=index),
            "RRPONTSYD": pd.Series([180.0, None], index=index),
            "BSCICP02USM460S": pd.Series([51.2, 51.7], index=index),
        }

    def test_normalize_macro_signals_flattens_fred_series_payload(self) -> None:
        from qmis.collectors.macro import normalize_macro_signals

        signals = normalize_macro_signals(self._build_macro_series())

        self.assertEqual(len(signals), 11)
        self.assertEqual(
            set(signals["series_name"]),
            {
                "yield_10y",
                "yield_3m",
                "m2_money_supply",
                "fed_balance_sheet",
                "reverse_repo_usage",
                "pmi",
            },
        )
        self.assertTrue((signals["source"] == "fred").all())
        self.assertTrue((signals["category"] == "macro").all())
        self.assertEqual(
            set(signals["unit"]),
            {"billions_usd", "index_points", "millions_usd", "percent"},
        )
        reverse_repo_rows = signals.loc[signals["series_name"] == "reverse_repo_usage"]
        self.assertEqual(len(reverse_repo_rows), 1)
        self.assertEqual(reverse_repo_rows.iloc[0]["value"], 180.0)

        metadata = json.loads(signals.loc[signals["series_name"] == "pmi"].iloc[0]["metadata"])
        self.assertEqual(metadata["series_id"], "BSCICP02USM460S")
        self.assertEqual(metadata["indicator_name"], "PMI")
        self.assertEqual(metadata["source_type"], "fred")

    def test_run_macro_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.macro import run_macro_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.macro.fetch_macro_series",
                return_value=self._build_macro_series(),
            ):
                inserted_rows = run_macro_collector(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT source, category, series_name, value, unit
                    FROM signals
                    ORDER BY ts, series_name
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 11)
        self.assertEqual(len(persisted), 11)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "fred",
                "category": "macro",
                "series_name": "fed_balance_sheet",
                "value": 6765000.0,
                "unit": "millions_usd",
            },
        )

    def test_fetch_series_with_csv_fallback_accepts_fred_graph_shape(self) -> None:
        from qmis.collectors.macro import _fetch_series_with_csv_fallback

        response = mock.Mock()
        response.text = "\n".join(
            [
                "observation_date,DGS10",
                "2026-03-05,4.20",
                "2026-03-06,4.25",
            ]
        )
        response.raise_for_status.return_value = None
        session = mock.Mock()
        session.get.return_value = response

        series = _fetch_series_with_csv_fallback("DGS10", session=session)

        self.assertEqual(series.name, "DGS10")
        self.assertEqual(list(series.index.strftime("%Y-%m-%d")), ["2026-03-05", "2026-03-06"])
        self.assertEqual(list(series.values), [4.2, 4.25])


if __name__ == "__main__":
    unittest.main()

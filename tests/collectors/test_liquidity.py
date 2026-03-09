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


class QMISLiquidityCollectorTests(unittest.TestCase):
    def _build_macro_series(self) -> dict[str, pd.Series]:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        return {
            "M2SL": pd.Series([21800.0, 21820.0], index=index),
            "WALCL": pd.Series([6765000.0, 6768000.0], index=index),
            "RRPONTSYD": pd.Series([180.0, None], index=index),
        }

    def _build_market_download(self) -> pd.DataFrame:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        frame = pd.DataFrame(
            {
                ("DX-Y.NYB", "Close"): [103.2, 103.5],
            },
            index=index,
        )
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def test_normalize_liquidity_signals_flattens_macro_and_market_inputs(self) -> None:
        from qmis.collectors.liquidity import normalize_liquidity_signals

        signals = normalize_liquidity_signals(
            macro_series_payloads=self._build_macro_series(),
            market_download=self._build_market_download(),
        )

        self.assertEqual(len(signals), 7)
        self.assertEqual(
            set(signals["series_name"]),
            {
                "m2_money_supply",
                "fed_balance_sheet",
                "reverse_repo_usage",
                "dollar_index",
            },
        )
        self.assertTrue((signals["category"] == "liquidity").all())
        self.assertEqual(
            set(signals["source"]),
            {"fred", "yfinance"},
        )
        self.assertEqual(
            set(signals["unit"]),
            {"billions_usd", "millions_usd", "index_points"},
        )

        dollar_index_rows = signals.loc[signals["series_name"] == "dollar_index"].sort_values("ts")
        self.assertEqual(list(dollar_index_rows["value"]), [103.2, 103.5])
        self.assertEqual(json.loads(dollar_index_rows.iloc[0]["metadata"])["ticker"], "DX-Y.NYB")

        reverse_repo_rows = signals.loc[signals["series_name"] == "reverse_repo_usage"]
        self.assertEqual(len(reverse_repo_rows), 1)
        self.assertEqual(reverse_repo_rows.iloc[0]["value"], 180.0)

    def test_run_liquidity_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.liquidity import run_liquidity_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.liquidity.fetch_macro_series",
                return_value=self._build_macro_series(),
            ), mock.patch(
                "qmis.collectors.liquidity.fetch_market_download",
                return_value=self._build_market_download(),
            ):
                inserted_rows = run_liquidity_collector(db_path=db_path)

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

        self.assertEqual(inserted_rows, 7)
        self.assertEqual(len(persisted), 7)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "yfinance",
                "category": "liquidity",
                "series_name": "dollar_index",
                "value": 103.2,
                "unit": "index_points",
            },
        )


if __name__ == "__main__":
    unittest.main()

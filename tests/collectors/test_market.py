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


class QMISMarketCollectorTests(unittest.TestCase):
    def _build_raw_download_frame(self) -> pd.DataFrame:
        index = pd.to_datetime(["2026-03-06", "2026-03-07"])
        frame = pd.DataFrame(
            {
                ("GC=F", "Close"): [2050.5, 2061.25],
                ("CL=F", "Close"): [78.2, 79.4],
                ("HG=F", "Close"): [4.11, 4.13],
                ("^GSPC", "Close"): [5099.0, 5102.5],
                ("^VIX", "Close"): [18.4, 17.9],
                ("DX-Y.NYB", "Close"): [103.2, 103.4],
            },
            index=index,
        )
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def test_normalize_market_signals_flattens_yfinance_download(self) -> None:
        from qmis.collectors.market import normalize_market_signals

        signals = normalize_market_signals(self._build_raw_download_frame())

        self.assertEqual(len(signals), 12)
        self.assertEqual(
            set(signals["series_name"]),
            {"gold", "oil", "copper", "sp500", "vix", "dollar_index"},
        )
        self.assertTrue((signals["source"] == "yfinance").all())
        self.assertTrue((signals["category"] == "market").all())
        self.assertEqual(
            set(signals["unit"]),
            {"usd", "index_points"},
        )
        gold_rows = signals.loc[signals["series_name"] == "gold"].sort_values("ts")
        self.assertEqual(list(gold_rows["value"]), [2050.5, 2061.25])
        self.assertIn('"ticker": "GC=F"', gold_rows.iloc[0]["metadata"])

    def test_run_market_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.market import run_market_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch("qmis.collectors.market.yf.download", return_value=self._build_raw_download_frame()):
                inserted_rows = run_market_collector(db_path=db_path, period="1mo")

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

        self.assertEqual(inserted_rows, 12)
        self.assertEqual(len(persisted), 12)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "yfinance",
                "category": "market",
                "series_name": "copper",
                "value": 4.11,
                "unit": "usd",
            },
        )


if __name__ == "__main__":
    unittest.main()

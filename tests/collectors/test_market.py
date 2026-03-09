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
                ("^VIX3M", "Close"): [19.6, 19.1],
                ("^VIX6M", "Close"): [20.4, 20.0],
                ("DX-Y.NYB", "Close"): [103.2, 103.4],
                ("SMH", "Close"): [251.2, 255.8],
                ("IWM", "Close"): [204.4, 206.0],
                ("KBE", "Close"): [48.2, 48.9],
                ("IYT", "Close"): [66.5, 67.1],
                ("DBA", "Close"): [27.2, 27.6],
                ("DBC", "Close"): [24.1, 24.5],
            },
            index=index,
        )
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def test_normalize_market_signals_flattens_yfinance_download(self) -> None:
        from qmis.collectors.market import normalize_market_signals

        signals = normalize_market_signals(self._build_raw_download_frame())

        self.assertEqual(len(signals), 28)
        self.assertEqual(
            set(signals["series_name"]),
            {
                "gold",
                "oil",
                "copper",
                "sp500",
                "vix",
                "vix3m",
                "vix6m",
                "dollar_index",
                "semiconductor_index",
                "small_caps",
                "bank_stocks",
                "transportation_index",
                "agriculture_index",
                "commodity_index",
            },
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
        volatility_rows = signals.loc[signals["series_name"] == "vix3m"].sort_values("ts")
        self.assertEqual(list(volatility_rows["value"]), [19.6, 19.1])
        leadership_rows = signals.loc[signals["series_name"] == "semiconductor_index"].sort_values("ts")
        self.assertEqual(list(leadership_rows["value"]), [251.2, 255.8])

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

        self.assertEqual(inserted_rows, 28)
        self.assertEqual(len(persisted), 28)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "yfinance",
                "category": "market",
                "series_name": "agriculture_index",
                "value": 27.2,
                "unit": "usd",
            },
        )
        latest_vix6m = persisted.loc[persisted["series_name"] == "vix6m"].iloc[-1]
        self.assertEqual(
            latest_vix6m.to_dict(),
            {
                "source": "yfinance",
                "category": "market",
                "series_name": "vix6m",
                "value": 20.0,
                "unit": "index_points",
            },
        )
        latest_transportation = persisted.loc[persisted["series_name"] == "transportation_index"].iloc[-1]
        self.assertEqual(
            latest_transportation.to_dict(),
            {
                "source": "yfinance",
                "category": "market",
                "series_name": "transportation_index",
                "value": 67.1,
                "unit": "usd",
            },
        )


if __name__ == "__main__":
    unittest.main()

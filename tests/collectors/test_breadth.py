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


class QMISBreadthCollectorTests(unittest.TestCase):
    def _build_constituents(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "Symbol": ["AAA", "BBB", "CCC"],
                "Security": ["Alpha", "Beta", "Gamma"],
            }
        )

    def _build_market_download(self) -> pd.DataFrame:
        index = pd.to_datetime(
            ["2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06"]
        )
        frame = pd.DataFrame(
            {
                ("AAA", "Close"): [10.0, 11.0, 12.0, 11.0, 13.0],
                ("BBB", "Close"): [10.0, 9.0, 8.0, 9.0, 7.0],
                ("CCC", "Close"): [10.0, 10.0, 11.0, 12.0, 13.0],
            },
            index=index,
        )
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def _build_batch_download(self, symbols: list[str]) -> pd.DataFrame:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        payload: dict[tuple[str, str], list[float]] = {}
        for offset, symbol in enumerate(symbols, start=1):
            payload[(symbol, "Close")] = [10.0 + offset, 11.0 + offset]
        frame = pd.DataFrame(payload, index=index)
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def test_calculate_breadth_signals_derives_spec_metrics(self) -> None:
        from qmis.collectors.breadth import calculate_breadth_signals

        signals = calculate_breadth_signals(
            raw_download=self._build_market_download(),
            constituents=self._build_constituents(),
            moving_average_window=3,
            high_low_window=3,
        )

        self.assertEqual(len(signals), 12)
        self.assertEqual(
            set(signals["series_name"]),
            {"sp500_above_200dma", "advance_decline_line", "new_highs", "new_lows"},
        )
        self.assertTrue((signals["category"] == "breadth").all())
        self.assertTrue((signals["source"] == "derived_breadth").all())

        latest = signals.loc[signals["ts"] == pd.Timestamp("2026-03-06").to_pydatetime()]
        latest_values = dict(zip(latest["series_name"], latest["value"]))
        self.assertAlmostEqual(latest_values["sp500_above_200dma"], 66.6666666667, places=6)
        self.assertEqual(latest_values["advance_decline_line"], 3.0)
        self.assertEqual(latest_values["new_highs"], 2.0)
        self.assertEqual(latest_values["new_lows"], 1.0)

        metadata = json.loads(latest.loc[latest["series_name"] == "sp500_above_200dma"].iloc[0]["metadata"])
        self.assertEqual(metadata["constituents_source"], "datasets_s_and_p_500_companies")
        self.assertEqual(metadata["prices_source"], "yfinance")

    def test_run_breadth_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.breadth import run_breadth_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.breadth.fetch_sp500_constituents",
                return_value=self._build_constituents(),
            ), mock.patch(
                "qmis.collectors.breadth.fetch_breadth_market_download",
                return_value=self._build_market_download(),
            ):
                inserted_rows = run_breadth_collector(
                    db_path=db_path,
                    moving_average_window=3,
                    high_low_window=3,
                )

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
                "source": "derived_breadth",
                "category": "breadth",
                "series_name": "advance_decline_line",
                "value": 1.0,
                "unit": "count",
            },
        )

    def test_fetch_breadth_market_download_uses_fast_threaded_primary_path(self) -> None:
        from qmis.collectors.breadth import fetch_breadth_market_download

        symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]
        with mock.patch(
            "qmis.collectors.breadth.fetch_market_download",
            return_value=self._build_batch_download(symbols),
        ) as fetch_download:
            combined = fetch_breadth_market_download(symbols, period="30d", interval="1d", chunk_size=2)

        fetch_download.assert_called_once_with(
            period="30d",
            interval="1d",
            tickers=symbols,
            threads=True,
            timeout_seconds=10,
        )
        self.assertEqual(set(combined.columns.get_level_values(0)), set(symbols))

    def test_fetch_breadth_market_download_falls_back_to_safe_batches_after_primary_failure(self) -> None:
        from qmis.collectors.breadth import fetch_breadth_market_download

        symbols = ["AAA", "BBB", "CCC", "DDD", "EEE"]
        with mock.patch(
            "qmis.collectors.breadth.fetch_market_download",
            side_effect=[
                RuntimeError("yfinance primary failed"),
                self._build_batch_download(["AAA", "BBB"]),
                self._build_batch_download(["CCC", "DDD"]),
                self._build_batch_download(["EEE"]),
            ],
        ) as fetch_download:
            combined = fetch_breadth_market_download(symbols, period="30d", interval="1d", chunk_size=2)

        self.assertEqual(fetch_download.call_count, 4)
        self.assertEqual(fetch_download.call_args_list[0].kwargs["threads"], True)
        self.assertEqual(
            [call.kwargs["tickers"] for call in fetch_download.call_args_list[1:]],
            [["AAA", "BBB"], ["CCC", "DDD"], ["EEE"]],
        )
        self.assertTrue(all(call.kwargs["threads"] is False for call in fetch_download.call_args_list[1:]))
        self.assertTrue(all(call.kwargs["timeout_seconds"] == 10 for call in fetch_download.call_args_list))
        self.assertEqual(set(combined.columns.get_level_values(0)), set(symbols))


if __name__ == "__main__":
    unittest.main()

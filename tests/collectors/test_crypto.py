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


class QMISCryptoCollectorTests(unittest.TestCase):
    def _build_market_download(self) -> pd.DataFrame:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        frame = pd.DataFrame(
            {
                ("BTC-USD", "Close"): [90500.0, 91250.0],
                ("ETH-USD", "Close"): [3200.5, 3255.25],
            },
            index=index,
        )
        frame.columns = pd.MultiIndex.from_tuples(frame.columns)
        return frame

    def _build_global_metrics_payload(self) -> dict[str, object]:
        return {
            "fetched_at": pd.Timestamp("2026-03-06T16:00:00Z"),
            "data": {
                "total_market_cap": {"usd": 3200000000000.0},
                "market_cap_percentage": {"btc": 58.4},
            },
        }

    def test_normalize_crypto_signals_flattens_price_and_global_metrics(self) -> None:
        from qmis.collectors.crypto import normalize_crypto_signals

        signals = normalize_crypto_signals(
            market_download=self._build_market_download(),
            global_metrics_payload=self._build_global_metrics_payload(),
        )

        self.assertEqual(len(signals), 6)
        self.assertEqual(
            set(signals["series_name"]),
            {"BTCUSD", "ETHUSD", "BTC_dominance", "crypto_market_cap"},
        )
        self.assertTrue((signals["category"] == "crypto").all())
        self.assertEqual(set(signals["source"]), {"yfinance", "coingecko"})
        self.assertEqual(set(signals["unit"]), {"pct", "usd", "usd_market_cap"})

        btc_rows = signals.loc[signals["series_name"] == "BTCUSD"].sort_values("ts")
        self.assertEqual(list(btc_rows["value"]), [90500.0, 91250.0])
        self.assertEqual(json.loads(btc_rows.iloc[0]["metadata"])["ticker"], "BTC-USD")

        dominance_rows = signals.loc[signals["series_name"] == "BTC_dominance"]
        self.assertEqual(len(dominance_rows), 1)
        dominance_metadata = json.loads(dominance_rows.iloc[0]["metadata"])
        self.assertEqual(dominance_rows.iloc[0]["value"], 58.4)
        self.assertEqual(dominance_metadata["api"], "global")

    def test_run_crypto_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.crypto import run_crypto_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.crypto.fetch_crypto_market_download",
                return_value=self._build_market_download(),
            ), mock.patch(
                "qmis.collectors.crypto.fetch_crypto_global_metrics",
                return_value=self._build_global_metrics_payload(),
            ):
                inserted_rows = run_crypto_collector(db_path=db_path)

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

        self.assertEqual(inserted_rows, 6)
        self.assertEqual(len(persisted), 6)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "yfinance",
                "category": "crypto",
                "series_name": "BTCUSD",
                "value": 90500.0,
                "unit": "usd",
            },
        )


if __name__ == "__main__":
    unittest.main()

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import duckdb
import pandas as pd
import requests


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISMacroCollectorTests(unittest.TestCase):
    def _build_macro_series(self) -> dict[str, pd.Series]:
        index = pd.to_datetime(["2026-03-05", "2026-03-06"])
        return {
            "DGS10": pd.Series([4.20, 4.25], index=index),
            "DGS2": pd.Series([4.05, 4.10], index=index),
            "DGS3MO": pd.Series([4.55, 4.50], index=index),
            "BAMLH0A0HYM2": pd.Series([3.85, 4.10], index=index),
            "BAA10YM": pd.Series([2.20, 2.35], index=index),
            "STLFSI4": pd.Series([0.11, 0.23], index=index),
            "M2SL": pd.Series([21800.0, 21820.0], index=index),
            "WALCL": pd.Series([6765000.0, 6768000.0], index=index),
            "RRPONTSYD": pd.Series([180.0, None], index=index),
            "DFII10": pd.Series([1.85, 1.92], index=index),
            "T10YIE": pd.Series([2.31, 2.28], index=index),
            "BSCICP02USM460S": pd.Series([51.2, 51.7], index=index),
        }

    def test_normalize_macro_signals_flattens_fred_series_payload(self) -> None:
        from qmis.collectors.macro import normalize_macro_signals

        signals = normalize_macro_signals(self._build_macro_series())

        self.assertEqual(len(signals), 23)
        self.assertEqual(
            set(signals["series_name"]),
            {
                "yield_10y",
                "yield_2y",
                "yield_3m",
                "high_yield_spread",
                "baa_corporate_spread",
                "financial_conditions_index",
                "m2_money_supply",
                "fed_balance_sheet",
                "reverse_repo_usage",
                "real_yields",
                "breakeven_inflation_10y",
                "pmi",
            },
        )
        self.assertTrue((signals["source"] == "fred").all())
        self.assertTrue((signals["category"] == "macro").all())
        self.assertEqual(
            set(signals["unit"]),
            {"billions_usd", "index_points", "millions_usd", "percent", "spread_points"},
        )
        reverse_repo_rows = signals.loc[signals["series_name"] == "reverse_repo_usage"]
        self.assertEqual(len(reverse_repo_rows), 1)
        self.assertEqual(reverse_repo_rows.iloc[0]["value"], 180.0)
        credit_rows = signals.loc[signals["series_name"] == "high_yield_spread"].sort_values("ts")
        self.assertEqual(list(credit_rows["value"]), [3.85, 4.1])

        metadata = json.loads(signals.loc[signals["series_name"] == "pmi"].iloc[0]["metadata"])
        self.assertEqual(metadata["series_id"], "BSCICP02USM460S")
        self.assertEqual(metadata["indicator_name"], "PMI")
        self.assertEqual(metadata["source_type"], "fred")

    def test_normalize_macro_signals_returns_empty_schema_when_all_series_are_missing(self) -> None:
        from qmis.collectors.macro import SIGNAL_COLUMNS, normalize_macro_signals

        signals = normalize_macro_signals({})

        self.assertTrue(signals.empty)
        self.assertEqual(list(signals.columns), SIGNAL_COLUMNS)

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

        self.assertEqual(inserted_rows, 23)
        self.assertEqual(len(persisted), 23)
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "fred",
                "category": "macro",
                "series_name": "baa_corporate_spread",
                "value": 2.2,
                "unit": "spread_points",
            },
        )
        latest_financial_conditions = persisted.loc[persisted["series_name"] == "financial_conditions_index"].iloc[-1]
        self.assertEqual(
            latest_financial_conditions.to_dict(),
            {
                "source": "fred",
                "category": "macro",
                "series_name": "financial_conditions_index",
                "value": 0.23,
                "unit": "index_points",
            },
        )

    def test_run_macro_collector_returns_zero_rows_when_all_fetches_are_skipped(self) -> None:
        from qmis.collectors.macro import run_macro_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.macro.fetch_macro_series",
                return_value={},
            ):
                inserted_rows = run_macro_collector(db_path=db_path)

        self.assertEqual(inserted_rows, 0)
        self.assertFalse(db_path.exists())

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

    def test_fetch_series_with_fred_api_parses_json_observations(self) -> None:
        from qmis.collectors.macro import _fetch_series_with_fred_api

        response = mock.Mock()
        response.json.return_value = {
            "observations": [
                {"date": "2026-03-05", "value": "4.20"},
                {"date": "2026-03-06", "value": "4.25"},
                {"date": "2026-03-07", "value": "."},
            ]
        }
        response.raise_for_status.return_value = None
        session = mock.Mock()
        session.get.return_value = response

        series = _fetch_series_with_fred_api("DGS10", api_key="secret", session=session, timeout_seconds=7)

        session.get.assert_called_once()
        self.assertEqual(series.name, "DGS10")
        self.assertEqual(list(series.index.strftime("%Y-%m-%d")), ["2026-03-05", "2026-03-06", "2026-03-07"])
        self.assertEqual(list(series.iloc[:2]), [4.2, 4.25])
        self.assertTrue(pd.isna(series.iloc[2]))

    def test_fetch_macro_series_uses_treasury_for_yields_without_fred_key(self) -> None:
        from qmis.collectors.macro import fetch_macro_series

        treasury_payload = {
            "DGS10": pd.Series([4.20, 4.25], index=pd.to_datetime(["2026-03-05", "2026-03-06"])),
            "DGS3MO": pd.Series([4.55, 4.50], index=pd.to_datetime(["2026-03-05", "2026-03-06"])),
        }

        with mock.patch.dict("os.environ", {}, clear=True), mock.patch(
            "qmis.collectors.macro._fetch_treasury_yield_series",
            return_value=treasury_payload,
        ) as fetch_treasury, mock.patch(
            "qmis.collectors.macro._fetch_series_with_fred_api"
        ) as fetch_fred_api, mock.patch(
            "qmis.collectors.macro._fetch_series_with_csv_fallback"
        ) as fetch_csv:
            payload = fetch_macro_series(series_ids=["DGS10", "DGS3MO"])

        fetch_treasury.assert_called_once()
        fetch_fred_api.assert_not_called()
        fetch_csv.assert_not_called()
        self.assertEqual(set(payload), {"DGS10", "DGS3MO"})

    def test_fetch_macro_series_skips_fred_only_series_without_api_key(self) -> None:
        from qmis.collectors.macro import fetch_macro_series

        with mock.patch.dict("os.environ", {}, clear=True), mock.patch(
            "qmis.collectors.macro._fetch_treasury_yield_series",
            return_value={},
        ), mock.patch(
            "qmis.collectors.macro._fetch_series_with_fred_api"
        ) as fetch_fred_api, mock.patch(
            "qmis.collectors.macro._fetch_series_with_csv_fallback"
        ) as fetch_csv:
            payload = fetch_macro_series(series_ids=["WALCL"])

        fetch_fred_api.assert_not_called()
        fetch_csv.assert_not_called()
        self.assertEqual(payload, {})

    def test_fetch_macro_series_prefers_fred_api_when_key_is_available(self) -> None:
        from qmis.collectors.macro import fetch_macro_series

        fred_series = pd.Series([4.20, 4.25], index=pd.to_datetime(["2026-03-05", "2026-03-06"]))
        with mock.patch.dict("os.environ", {"FRED_API_KEY": "secret"}, clear=True), mock.patch(
            "qmis.collectors.macro._fetch_treasury_yield_series"
        ) as fetch_treasury, mock.patch(
            "qmis.collectors.macro._fetch_series_with_fred_api",
            side_effect=[fred_series.rename("DGS10"), fred_series.rename("DGS3MO")],
        ) as fetch_fred_api:
            payload = fetch_macro_series(series_ids=["DGS10", "DGS3MO"])

        fetch_treasury.assert_not_called()
        self.assertEqual(fetch_fred_api.call_count, 2)
        self.assertEqual(set(payload), {"DGS10", "DGS3MO"})

    def test_run_macro_collector_uses_shared_http_session_and_timeout(self) -> None:
        from qmis.collectors.macro import run_macro_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            fake_session = mock.MagicMock(spec=requests.Session)
            fake_session.__enter__.return_value = fake_session
            fake_session.__exit__.return_value = None
            with mock.patch("qmis.collectors.macro.requests.Session", return_value=fake_session), mock.patch(
                "qmis.collectors.macro.fetch_macro_series",
                return_value=self._build_macro_series(),
            ) as fetch_macro_series:
                run_macro_collector(db_path=db_path)

        fetch_macro_series.assert_called_once_with(session=fake_session, timeout_seconds=10)


if __name__ == "__main__":
    unittest.main()

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


class QMISNaturalCollectorTests(unittest.TestCase):
    def _build_payloads(self) -> dict[str, object]:
        return {
            "earthquake_feed": {
                "metadata": {"count": 3},
                "features": [
                    {"properties": {"time": 1772928000000, "mag": 1.2}},
                    {"properties": {"time": 1772931600000, "mag": 2.1}},
                    {"properties": {"time": 1772935200000, "mag": 0.9}},
                ],
            },
            "temperature_anomaly_timeseries": "\n".join(
                [
                    "2025 12 0.71 0.01 0.0 0.0 0.0 0.0 0.0 0.0",
                    "2026  1 0.84 0.01 0.0 0.0 0.0 0.0 0.0 0.0",
                    "2026  2 0.91 0.01 0.0 0.0 0.0 0.0 0.0 0.0",
                ]
            ),
            "geomagnetic_payload": [
                {"time_tag": "2026-03-08T12:00:00", "kp_index": 3},
                {"time_tag": "2026-03-08T18:00:00", "kp_index": 5},
            ],
            "solar_wind_csv": "\n".join(
                [
                    "2026-03-08T00:00:00Z,2.55,171115,604.4",
                    "2026-03-08T00:01:00Z,2.50,168477,607.2",
                    "2026-03-08T00:02:00Z,2.07,180823,615.9",
                ]
            ),
        }

    def test_normalize_natural_signals_maps_selected_exploratory_sources(self) -> None:
        from qmis.collectors.natural import normalize_natural_signals

        signals = normalize_natural_signals(self._build_payloads())

        self.assertEqual(len(signals), 4)
        self.assertEqual(
            set(signals["series_name"]),
            {
                "earthquake_count",
                "global_temperature_anomaly",
                "geomagnetic_activity",
                "solar_wind_speed",
            },
        )
        self.assertTrue((signals["category"] == "natural").all())

        values = dict(zip(signals["series_name"], signals["value"]))
        self.assertEqual(values["earthquake_count"], 3.0)
        self.assertEqual(values["global_temperature_anomaly"], 0.91)
        self.assertEqual(values["geomagnetic_activity"], 5.0)
        self.assertAlmostEqual(values["solar_wind_speed"], (604.4 + 607.2 + 615.9) / 3.0, places=6)

        metadata = {
            row["series_name"]: json.loads(row["metadata"])
            for _, row in signals.iterrows()
        }
        self.assertTrue(all(item["exploratory"] for item in metadata.values()))
        self.assertEqual(metadata["earthquake_count"]["source_provider"], "usgs")
        self.assertEqual(metadata["global_temperature_anomaly"]["source_provider"], "noaa_ncei")
        self.assertEqual(metadata["geomagnetic_activity"]["source_provider"], "noaa_swpc")
        self.assertEqual(metadata["solar_wind_speed"]["source_provider"], "nasa_iswa_hapi")

    def test_run_natural_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.natural import run_natural_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.natural.fetch_natural_payloads",
                return_value=self._build_payloads(),
            ):
                inserted_rows = run_natural_collector(db_path=db_path)

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

        self.assertEqual(inserted_rows, 4)
        self.assertEqual(len(persisted), 4)
        earthquake_row = persisted.loc[persisted["series_name"] == "earthquake_count"].iloc[0].to_dict()
        self.assertEqual(
            earthquake_row,
            {
                "source": "derived_natural",
                "category": "natural",
                "series_name": "earthquake_count",
                "value": 3.0,
                "unit": "count",
            },
        )


if __name__ == "__main__":
    unittest.main()

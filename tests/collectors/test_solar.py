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


class QMISSolarCollectorTests(unittest.TestCase):
    def _build_noaa_payloads(self) -> dict[str, list[dict[str, object]]]:
        return {
            "sunspot_report": [
                {
                    "Obsdate": "2026-03-08T00:00:00",
                    "Region": 4378,
                    "Numspot": 2,
                },
                {
                    "Obsdate": "2026-03-08T00:00:00",
                    "Region": 4378,
                    "Numspot": 3,
                },
                {
                    "Obsdate": "2026-03-08T00:00:00",
                    "Region": 4381,
                    "Numspot": 4,
                },
            ],
            "solar_radio_flux": [
                {
                    "time_tag": "2026-03-08T12:00:00",
                    "details": [{"frequency": 2695, "flux": 141}],
                },
                {
                    "time_tag": "2026-03-08T17:45:00",
                    "details": [{"frequency": 2695, "flux": 142}],
                },
            ],
            "planetary_k_index_1m": [
                {"time_tag": "2026-03-08T12:00:00", "kp_index": 3},
                {"time_tag": "2026-03-08T18:00:00", "kp_index": 4},
            ],
            "edited_events": [
                {"begin_datetime": "2026-03-08T01:10:00", "type": "XRA"},
                {"begin_datetime": "2026-03-08T05:30:00", "type": "RBR"},
                {"begin_datetime": "2026-03-08T17:17:00", "type": "XRA"},
            ],
        }

    def test_normalize_solar_signals_aggregates_noaa_payloads(self) -> None:
        from qmis.collectors.solar import normalize_solar_signals

        signals = normalize_solar_signals(self._build_noaa_payloads())

        self.assertEqual(len(signals), 4)
        self.assertEqual(
            set(signals["series_name"]),
            {"sunspot_number", "solar_flux_f107", "geomagnetic_kp", "solar_flare_count"},
        )
        self.assertEqual(set(signals["category"]), {"astronomy", "natural"})
        self.assertEqual(set(signals["source"]), {"noaa_swpc"})

        values = dict(zip(signals["series_name"], signals["value"]))
        self.assertEqual(values["sunspot_number"], 7.0)
        self.assertEqual(values["solar_flux_f107"], 142.0)
        self.assertEqual(values["geomagnetic_kp"], 4.0)
        self.assertEqual(values["solar_flare_count"], 2.0)

        kp_metadata = json.loads(signals.loc[signals["series_name"] == "geomagnetic_kp"].iloc[0]["metadata"])
        self.assertEqual(kp_metadata["endpoint"], "planetary_k_index_1m")

    def test_run_solar_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.solar import run_solar_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            with mock.patch(
                "qmis.collectors.solar.fetch_solar_payloads",
                return_value=self._build_noaa_payloads(),
            ):
                inserted_rows = run_solar_collector(db_path=db_path)

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
        self.assertEqual(
            persisted.iloc[0].to_dict(),
            {
                "source": "noaa_swpc",
                "category": "natural",
                "series_name": "geomagnetic_kp",
                "value": 4.0,
                "unit": "index_points",
            },
        )


if __name__ == "__main__":
    unittest.main()

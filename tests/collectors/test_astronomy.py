import json
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


class QMISAstronomyCollectorTests(unittest.TestCase):
    def test_calculate_astronomy_signals_returns_required_daily_series(self) -> None:
        from qmis.collectors.astronomy import calculate_astronomy_signals

        signals = calculate_astronomy_signals(pd.Timestamp("2026-03-08T00:00:00Z"))

        self.assertEqual(
            set(signals["series_name"]),
            {
                "lunar_cycle_day",
                "lunar_phase_angle",
                "lunar_illumination",
                "moon_distance",
                "moon_declination",
                "solar_longitude",
                "zodiac_index",
                "earth_axial_tilt",
                "precession_angle",
                "full_moon",
                "new_moon",
            },
        )
        self.assertTrue((signals["category"] == "astronomy").all())
        self.assertTrue((signals["source"] == "derived_ephemeris").all())

        values = dict(zip(signals["series_name"], signals["value"]))
        self.assertGreaterEqual(values["lunar_cycle_day"], 0.0)
        self.assertLessEqual(values["lunar_cycle_day"], 29.53)
        self.assertGreaterEqual(values["lunar_illumination"], 0.0)
        self.assertLessEqual(values["lunar_illumination"], 100.0)
        self.assertGreaterEqual(values["solar_longitude"], 0.0)
        self.assertLess(values["solar_longitude"], 360.0)
        self.assertIn(values["zodiac_index"], range(12))
        self.assertIn(values["full_moon"], {0.0, 1.0})
        self.assertIn(values["new_moon"], {0.0, 1.0})

        zodiac_metadata = json.loads(signals.loc[signals["series_name"] == "zodiac_index"].iloc[0]["metadata"])
        self.assertIn(zodiac_metadata["zodiac_sign"], {
            "Aries",
            "Taurus",
            "Gemini",
            "Cancer",
            "Leo",
            "Virgo",
            "Libra",
            "Scorpio",
            "Sagittarius",
            "Capricorn",
            "Aquarius",
            "Pisces",
        })

    def test_run_astronomy_collector_persists_rows_into_signals_table(self) -> None:
        from qmis.collectors.astronomy import run_astronomy_collector

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            inserted_rows = run_astronomy_collector(
                db_path=db_path,
                ts=pd.Timestamp("2026-03-08T00:00:00Z"),
            )

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT source, category, series_name, unit
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
                "source": "derived_ephemeris",
                "category": "astronomy",
                "series_name": "earth_axial_tilt",
                "unit": "degrees",
            },
        )


if __name__ == "__main__":
    unittest.main()

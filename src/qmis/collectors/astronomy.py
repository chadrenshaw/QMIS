"""Astronomy signal collector for QMIS."""

from __future__ import annotations

import json
import math
from datetime import timedelta
from pathlib import Path

import ephem
import pandas as pd

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


SYNODIC_MONTH_DAYS = 29.53
AU_IN_KM = 149_597_870.7
ZODIAC_SIGNS = [
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
]


def _normalize_timestamp(ts: pd.Timestamp | None = None) -> pd.Timestamp:
    timestamp = ts or pd.Timestamp.utcnow()
    normalized = pd.Timestamp(timestamp)
    if normalized.tzinfo is not None:
        normalized = normalized.tz_convert("UTC").tz_localize(None)
    return normalized.normalize()


def _julian_centuries(ts: pd.Timestamp) -> float:
    delta_days = (ts - pd.Timestamp("2000-01-01T12:00:00")).total_seconds() / 86400.0
    return delta_days / 36525.0


def _earth_axial_tilt_degrees(ts: pd.Timestamp) -> float:
    centuries = _julian_centuries(ts)
    arcseconds = 84381.448 - 46.8150 * centuries - 0.00059 * centuries**2 + 0.001813 * centuries**3
    return arcseconds / 3600.0


def _precession_angle_degrees(ts: pd.Timestamp) -> float:
    centuries = _julian_centuries(ts)
    arcseconds = 5028.796195 * centuries + 1.1054348 * centuries**2
    return arcseconds / 3600.0


def _zodiac_index_and_sign(solar_longitude: float) -> tuple[int, str]:
    longitude = solar_longitude % 360.0
    zodiac_index = int(math.floor(longitude / 30.0)) % 12
    return zodiac_index, ZODIAC_SIGNS[zodiac_index]


def calculate_astronomy_signals(ts: pd.Timestamp | None = None) -> pd.DataFrame:
    """Derive the spec-defined daily astronomy signals for a single UTC date."""
    signal_ts = _normalize_timestamp(ts)
    ephem_date = ephem.Date(signal_ts.to_pydatetime())

    moon = ephem.Moon(ephem_date)
    sun = ephem.Sun(ephem_date)
    moon_ecliptic = ephem.Ecliptic(moon)
    sun_ecliptic = ephem.Ecliptic(sun)

    phase_angle = (math.degrees(float(moon_ecliptic.lon - sun_ecliptic.lon)) + 360.0) % 360.0
    lunar_cycle_day = (phase_angle / 360.0) * SYNODIC_MONTH_DAYS
    lunar_illumination = float(moon.phase)
    moon_distance = float(moon.earth_distance) * AU_IN_KM
    moon_declination = math.degrees(float(moon.dec))
    solar_longitude = math.degrees(float(sun_ecliptic.lon)) % 360.0
    zodiac_index, zodiac_sign = _zodiac_index_and_sign(solar_longitude)

    day_start = ephem.Date(signal_ts.to_pydatetime())
    day_end = ephem.Date((signal_ts + pd.Timedelta(days=1)).to_pydatetime())
    new_moon_date = ephem.previous_new_moon(day_end).datetime().date()
    full_moon_date = ephem.previous_full_moon(day_end).datetime().date()

    rows = [
        ("lunar_cycle_day", lunar_cycle_day, "days", {}),
        ("lunar_phase_angle", phase_angle, "degrees", {}),
        ("lunar_illumination", lunar_illumination, "percent", {}),
        ("moon_distance", moon_distance, "kilometers", {}),
        ("moon_declination", moon_declination, "degrees", {}),
        ("solar_longitude", solar_longitude, "degrees", {}),
        ("zodiac_index", float(zodiac_index), "index", {"zodiac_sign": zodiac_sign}),
        ("earth_axial_tilt", _earth_axial_tilt_degrees(signal_ts), "degrees", {}),
        ("precession_angle", _precession_angle_degrees(signal_ts), "degrees", {}),
        ("full_moon", 1.0 if full_moon_date == signal_ts.date() else 0.0, "boolean", {}),
        ("new_moon", 1.0 if new_moon_date == signal_ts.date() else 0.0, "boolean", {}),
    ]

    payload = [
        {
            "ts": signal_ts.to_pydatetime(),
            "source": "derived_ephemeris",
            "category": "astronomy",
            "series_name": series_name,
            "value": float(value),
            "unit": unit,
            "metadata": json.dumps(metadata, sort_keys=True),
        }
        for series_name, value, unit, metadata in rows
    ]
    return pd.DataFrame(payload).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_astronomy_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized astronomy signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        payload = signals.copy()
        connection.register("astronomy_signals_df", payload)
        connection.execute(
            """
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM astronomy_signals_df
            """
        )
        connection.unregister("astronomy_signals_df")
    return int(len(signals))


def run_astronomy_collector(db_path: Path | None = None, ts: pd.Timestamp | None = None) -> int:
    """Derive and persist the spec-defined astronomy signals."""
    signals = calculate_astronomy_signals(ts=ts)
    return persist_astronomy_signals(signals, db_path=db_path)

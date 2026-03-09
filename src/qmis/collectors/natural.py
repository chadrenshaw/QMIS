"""Exploratory natural signal collector for QMIS."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


USGS_EARTHQUAKE_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
NOAA_GLOBAL_TEMP_URL = (
    "https://www.ncei.noaa.gov/data/noaa-global-surface-temperature/v5/access/timeseries/"
    "aravg.mon.land_ocean.90S.90N.v5.0.0.202212.asc"
)
NOAA_GEOMAGNETIC_URL = "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json"
NASA_SOLAR_WIND_URL = "https://iswa.gsfc.nasa.gov/IswaSystemWebApp/hapi/data"


def fetch_natural_payloads(
    session: requests.Session | None = None,
    timeout_seconds: int = 30,
) -> dict[str, object]:
    """Fetch the selected exploratory natural-signal payloads."""
    http = session or requests.Session()

    earthquake_feed = http.get(USGS_EARTHQUAKE_URL, timeout=timeout_seconds)
    earthquake_feed.raise_for_status()

    temperature_feed = http.get(NOAA_GLOBAL_TEMP_URL, timeout=timeout_seconds)
    temperature_feed.raise_for_status()

    geomagnetic_feed = http.get(NOAA_GEOMAGNETIC_URL, timeout=timeout_seconds)
    geomagnetic_feed.raise_for_status()

    now = pd.Timestamp.utcnow()
    one_day_ago = now - pd.Timedelta(days=1)
    solar_wind_feed = http.get(
        NASA_SOLAR_WIND_URL,
        params={
            "id": "ace_swepam_P1M",
            "time.min": one_day_ago.isoformat().replace("+00:00", "Z"),
            "time.max": now.isoformat().replace("+00:00", "Z"),
        },
        timeout=timeout_seconds,
    )
    solar_wind_feed.raise_for_status()

    return {
        "earthquake_feed": earthquake_feed.json(),
        "temperature_anomaly_timeseries": temperature_feed.text,
        "geomagnetic_payload": geomagnetic_feed.json(),
        "solar_wind_csv": solar_wind_feed.text,
    }


def _exploratory_metadata(source_provider: str, endpoint: str, aggregation: str) -> str:
    return json.dumps(
        {
            "exploratory": True,
            "source_provider": source_provider,
            "endpoint": endpoint,
            "aggregation": aggregation,
        },
        sort_keys=True,
    )


def normalize_natural_signals(payloads: dict[str, object]) -> pd.DataFrame:
    """Normalize selected exploratory natural inputs into QMIS signal rows."""
    rows: list[dict[str, Any]] = []

    earthquake_payload = payloads.get("earthquake_feed", {})
    earthquake_features = earthquake_payload.get("features", []) if isinstance(earthquake_payload, dict) else []
    if earthquake_features:
        earthquake_times = [
            pd.to_datetime(feature.get("properties", {}).get("time"), unit="ms", utc=True)
            for feature in earthquake_features
            if feature.get("properties", {}).get("time") is not None
        ]
        if earthquake_times:
            signal_ts = max(earthquake_times).tz_convert("UTC").tz_localize(None).normalize()
            rows.append(
                {
                    "ts": signal_ts.to_pydatetime(),
                    "source": "derived_natural",
                    "category": "natural",
                    "series_name": "earthquake_count",
                    "value": float(len(earthquake_features)),
                    "unit": "count",
                    "metadata": _exploratory_metadata("usgs", "all_day.geojson", "daily_count"),
                }
            )

    temperature_text = str(payloads.get("temperature_anomaly_timeseries", "")).strip()
    if temperature_text:
        frame = pd.read_csv(StringIO(temperature_text), sep=r"\s+", header=None)
        if len(frame.columns) >= 3:
            latest = frame.iloc[-1]
            signal_ts = pd.Timestamp(year=int(latest.iloc[0]), month=int(latest.iloc[1]), day=1)
            rows.append(
                {
                    "ts": signal_ts.to_pydatetime(),
                    "source": "derived_natural",
                    "category": "natural",
                    "series_name": "global_temperature_anomaly",
                    "value": float(latest.iloc[2]),
                    "unit": "celsius_anomaly",
                    "metadata": _exploratory_metadata(
                        "noaa_ncei",
                        "noaa-global-surface-temperature-timeseries",
                        "latest_monthly_value",
                    ),
                }
            )

    geomagnetic_payload = payloads.get("geomagnetic_payload", [])
    geomagnetic_frame = pd.DataFrame(geomagnetic_payload if isinstance(geomagnetic_payload, list) else [])
    if not geomagnetic_frame.empty and "time_tag" in geomagnetic_frame.columns:
        geomagnetic_frame["signal_date"] = pd.to_datetime(geomagnetic_frame["time_tag"]).dt.normalize()
        geomagnetic_frame["kp_index"] = pd.to_numeric(geomagnetic_frame["kp_index"], errors="coerce")
        grouped = geomagnetic_frame.dropna(subset=["signal_date", "kp_index"]).groupby("signal_date")["kp_index"].max()
        if not grouped.empty:
            signal_date = grouped.index[-1]
            rows.append(
                {
                    "ts": pd.Timestamp(signal_date).to_pydatetime(),
                    "source": "derived_natural",
                    "category": "natural",
                    "series_name": "geomagnetic_activity",
                    "value": float(grouped.iloc[-1]),
                    "unit": "index_points",
                    "metadata": _exploratory_metadata("noaa_swpc", "planetary_k_index_1m", "daily_max"),
                }
            )

    solar_wind_text = str(payloads.get("solar_wind_csv", "")).strip()
    if solar_wind_text:
        frame = pd.read_csv(
            StringIO(solar_wind_text),
            header=None,
            names=["time_tag", "proton_density", "ion_temperature", "bulk_speed"],
        )
        frame["time_tag"] = pd.to_datetime(frame["time_tag"], utc=True)
        frame["bulk_speed"] = pd.to_numeric(frame["bulk_speed"], errors="coerce")
        clean = frame.dropna(subset=["time_tag", "bulk_speed"])
        if not clean.empty:
            signal_ts = clean["time_tag"].max().tz_convert("UTC").tz_localize(None).normalize()
            rows.append(
                {
                    "ts": signal_ts.to_pydatetime(),
                    "source": "derived_natural",
                    "category": "natural",
                    "series_name": "solar_wind_speed",
                    "value": float(clean["bulk_speed"].mean()),
                    "unit": "km_per_s",
                    "metadata": _exploratory_metadata("nasa_iswa_hapi", "ace_swepam_P1M", "daily_mean"),
                }
            )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_natural_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized natural signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        payload = signals.copy()
        connection.register("natural_signals_df", payload)
        connection.execute(
            """
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM natural_signals_df
            """
        )
        connection.unregister("natural_signals_df")
    return int(len(signals))


def run_natural_collector(db_path: Path | None = None) -> int:
    """Fetch and persist the selected exploratory natural signals."""
    payloads = fetch_natural_payloads()
    signals = normalize_natural_signals(payloads)
    return persist_natural_signals(signals, db_path=db_path)

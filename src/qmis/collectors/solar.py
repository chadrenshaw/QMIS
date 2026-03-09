"""Solar activity collector for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.collectors._persistence import replace_signal_rows
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


SOLAR_ENDPOINTS = {
    "sunspot_report": "https://services.swpc.noaa.gov/json/sunspot_report.json",
    "solar_radio_flux": "https://services.swpc.noaa.gov/json/solar-radio-flux.json",
    "planetary_k_index_1m": "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
    "edited_events": "https://services.swpc.noaa.gov/json/edited_events.json",
}


def fetch_solar_payloads(
    session: requests.Session | None = None,
    timeout_seconds: int = 30,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch the live NOAA SWPC solar payloads used by the solar collector."""
    http = session or requests.Session()
    payloads: dict[str, list[dict[str, Any]]] = {}

    for name, url in SOLAR_ENDPOINTS.items():
        response = http.get(url, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        payloads[name] = payload if isinstance(payload, list) else []

    return payloads


def _empty_signal_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=["ts", "source", "category", "series_name", "value", "unit", "metadata"])


def normalize_solar_signals(payloads: dict[str, list[dict[str, Any]]]) -> pd.DataFrame:
    """Aggregate NOAA solar payloads into daily QMIS signals."""
    rows: list[dict[str, Any]] = []

    sunspot_frame = pd.DataFrame(payloads.get("sunspot_report", []))
    if not sunspot_frame.empty and {"Obsdate", "Region", "Numspot"} <= set(sunspot_frame.columns):
        sunspot_frame["signal_date"] = pd.to_datetime(sunspot_frame["Obsdate"]).dt.normalize()
        sunspot_frame["Numspot"] = pd.to_numeric(sunspot_frame["Numspot"], errors="coerce")
        grouped = (
            sunspot_frame.dropna(subset=["signal_date", "Numspot"])
            .groupby(["signal_date", "Region"], dropna=False)["Numspot"]
            .max()
            .groupby("signal_date")
            .sum()
        )
        for signal_date, value in grouped.items():
            rows.append(
                {
                    "ts": pd.Timestamp(signal_date).to_pydatetime(),
                    "source": "noaa_swpc",
                    "category": "astronomy",
                    "series_name": "sunspot_number",
                    "value": float(value),
                    "unit": "count",
                    "metadata": json.dumps(
                        {"endpoint": "sunspot_report", "aggregation": "sum_max_numspot_by_region"},
                        sort_keys=True,
                    ),
                }
            )

    flux_frame = pd.DataFrame(payloads.get("solar_radio_flux", []))
    if not flux_frame.empty and "time_tag" in flux_frame.columns:
        flux_frame["signal_date"] = pd.to_datetime(flux_frame["time_tag"]).dt.normalize()
        flux_frame = flux_frame.sort_values("time_tag")
        latest_by_day = flux_frame.groupby("signal_date", as_index=False).tail(1)
        for _, record in latest_by_day.iterrows():
            details = record.get("details") or []
            f107_value = next(
                (
                    detail.get("flux")
                    for detail in details
                    if detail.get("frequency") == 2695 and detail.get("flux") is not None
                ),
                None,
            )
            if f107_value is None:
                continue
            rows.append(
                {
                    "ts": pd.Timestamp(record["signal_date"]).to_pydatetime(),
                    "source": "noaa_swpc",
                    "category": "astronomy",
                    "series_name": "solar_flux_f107",
                    "value": float(f107_value),
                    "unit": "sfu",
                    "metadata": json.dumps(
                        {"endpoint": "solar_radio_flux", "frequency_mhz": 2695, "aggregation": "latest_daily"},
                        sort_keys=True,
                    ),
                }
            )

    kp_frame = pd.DataFrame(payloads.get("planetary_k_index_1m", []))
    if not kp_frame.empty and "time_tag" in kp_frame.columns:
        kp_frame["signal_date"] = pd.to_datetime(kp_frame["time_tag"]).dt.normalize()
        kp_series = pd.to_numeric(kp_frame.get("kp_index"), errors="coerce")
        if kp_series.isna().all() and "estimated_kp" in kp_frame.columns:
            kp_series = pd.to_numeric(kp_frame["estimated_kp"], errors="coerce")
        kp_frame["kp_value"] = kp_series
        grouped = kp_frame.dropna(subset=["signal_date", "kp_value"]).groupby("signal_date")["kp_value"].max()
        for signal_date, value in grouped.items():
            rows.append(
                {
                    "ts": pd.Timestamp(signal_date).to_pydatetime(),
                    "source": "noaa_swpc",
                    "category": "natural",
                    "series_name": "geomagnetic_kp",
                    "value": float(value),
                    "unit": "index_points",
                    "metadata": json.dumps(
                        {"endpoint": "planetary_k_index_1m", "aggregation": "daily_max"},
                        sort_keys=True,
                    ),
                }
            )

    events_frame = pd.DataFrame(payloads.get("edited_events", []))
    if not events_frame.empty and {"begin_datetime", "type"} <= set(events_frame.columns):
        events_frame["signal_date"] = pd.to_datetime(events_frame["begin_datetime"]).dt.normalize()
        flare_events = events_frame.loc[events_frame["type"] == "XRA"]
        grouped = flare_events.dropna(subset=["signal_date"]).groupby("signal_date").size()
        for signal_date, value in grouped.items():
            rows.append(
                {
                    "ts": pd.Timestamp(signal_date).to_pydatetime(),
                    "source": "noaa_swpc",
                    "category": "astronomy",
                    "series_name": "solar_flare_count",
                    "value": float(value),
                    "unit": "count",
                    "metadata": json.dumps(
                        {"endpoint": "edited_events", "event_type": "XRA", "aggregation": "daily_count"},
                        sort_keys=True,
                    ),
                }
            )

    if not rows:
        return _empty_signal_frame()

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_solar_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized solar signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        return replace_signal_rows(connection, signals, "solar_signals_df")


def run_solar_collector(db_path: Path | None = None) -> int:
    """Fetch and persist the spec-defined solar activity signals."""
    payloads = fetch_solar_payloads()
    signals = normalize_solar_signals(payloads)
    return persist_solar_signals(signals, db_path=db_path)

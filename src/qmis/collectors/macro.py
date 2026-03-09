"""Macro data collector for QMIS."""

from __future__ import annotations

import json
import os
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.logger import get_logger
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


LOGGER = get_logger("qmis.collectors.macro")
FRED_API_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_GRAPH_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
TREASURY_YIELD_CSV_URL_TEMPLATE = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/{year}/all"
)
TREASURY_YIELD_COLUMN_MAP = {
    "DGS10": "10 Yr",
    "DGS3MO": "3 Mo",
}

MACRO_SERIES: dict[str, dict[str, str]] = {
    "DGS10": {
        "series_name": "yield_10y",
        "indicator_name": "10Y Treasury Yield",
        "unit": "percent",
        "frequency": "daily",
    },
    "DGS3MO": {
        "series_name": "yield_3m",
        "indicator_name": "3M Treasury Yield",
        "unit": "percent",
        "frequency": "daily",
    },
    "M2SL": {
        "series_name": "m2_money_supply",
        "indicator_name": "M2 Money Supply",
        "unit": "billions_usd",
        "frequency": "weekly",
    },
    "WALCL": {
        "series_name": "fed_balance_sheet",
        "indicator_name": "Fed Balance Sheet",
        "unit": "millions_usd",
        "frequency": "weekly",
    },
    "RRPONTSYD": {
        "series_name": "reverse_repo_usage",
        "indicator_name": "Reverse Repo Usage",
        "unit": "billions_usd",
        "frequency": "daily",
    },
    "BSCICP02USM460S": {
        "series_name": "pmi",
        "indicator_name": "PMI",
        "unit": "index_points",
        "frequency": "monthly",
    },
}


def _fetch_series_with_fred_api(
    series_id: str,
    api_key: str,
    session: requests.Session | None = None,
    timeout_seconds: int = 10,
) -> pd.Series:
    http = session or requests.Session()
    response = http.get(
        FRED_API_OBSERVATIONS_URL,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "sort_order": "asc",
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    observations = payload.get("observations", [])
    if not observations:
        return pd.Series(dtype=float, name=series_id)

    frame = pd.DataFrame(observations)
    if "date" not in frame or "value" not in frame:
        raise ValueError(f"Unexpected FRED API payload for {series_id}.")
    return pd.Series(
        pd.to_numeric(frame["value"], errors="coerce").to_numpy(),
        index=pd.to_datetime(frame["date"]),
        name=series_id,
    )


def _fetch_treasury_yield_series(
    session: requests.Session | None = None,
    years: tuple[int, ...] | None = None,
    timeout_seconds: int = 10,
) -> dict[str, pd.Series]:
    http = session or requests.Session()
    target_years = years or (pd.Timestamp.utcnow().year, pd.Timestamp.utcnow().year - 1)
    frames: list[pd.DataFrame] = []

    for year in dict.fromkeys(target_years):
        response = http.get(
            TREASURY_YIELD_CSV_URL_TEMPLATE.format(year=year),
            params={
                "type": "daily_treasury_yield_curve",
                "_format": "csv",
            },
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        frame = pd.read_csv(StringIO(response.text))
        if frame.empty:
            continue
        frames.append(frame)

    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    if "Date" not in combined:
        raise ValueError("Unexpected Treasury yield CSV payload.")

    combined["Date"] = pd.to_datetime(combined["Date"])
    combined = combined.drop_duplicates(subset=["Date"], keep="last").sort_values("Date")

    payload: dict[str, pd.Series] = {}
    for series_id, column_name in TREASURY_YIELD_COLUMN_MAP.items():
        if column_name not in combined:
            raise ValueError(f"Missing Treasury yield column {column_name}.")
        payload[series_id] = pd.Series(
            pd.to_numeric(combined[column_name], errors="coerce").to_numpy(),
            index=combined["Date"],
            name=series_id,
        )
    return payload


def _fetch_series_with_csv_fallback(
    series_id: str,
    session: requests.Session | None = None,
    timeout_seconds: int = 30,
) -> pd.Series:
    http = session or requests.Session()
    response = http.get(
        FRED_GRAPH_CSV_URL,
        params={"id": series_id},
        timeout=timeout_seconds,
    )
    response.raise_for_status()

    frame = pd.read_csv(StringIO(response.text))
    normalized_columns = {column.upper(): column for column in frame.columns}
    date_column = normalized_columns.get("DATE") or normalized_columns.get("OBSERVATION_DATE")
    value_column = normalized_columns.get("VALUE") or normalized_columns.get(series_id.upper())
    if not date_column or not value_column:
        raise ValueError(f"Unexpected FRED CSV payload for {series_id}.")

    series = pd.Series(
        pd.to_numeric(frame[value_column], errors="coerce").to_numpy(),
        index=pd.to_datetime(frame[date_column]),
        name=series_id,
    )
    return series


def fetch_macro_series(
    api_key: str | None = None,
    session: requests.Session | None = None,
    series_ids: list[str] | None = None,
    timeout_seconds: int = 10,
) -> dict[str, pd.Series]:
    """Fetch the spec-defined macro series from FRED."""
    resolved_api_key = api_key or os.getenv("FRED_API_KEY")
    payload: dict[str, pd.Series] = {}
    target_series_ids = series_ids or list(MACRO_SERIES)

    treasury_series_ids = [series_id for series_id in target_series_ids if series_id in TREASURY_YIELD_COLUMN_MAP]
    if treasury_series_ids and not resolved_api_key:
        LOGGER.info("Calling treasury yield curve for %s", ", ".join(treasury_series_ids))
        try:
            treasury_payload = _fetch_treasury_yield_series(
                session=session,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - network failures are logged and skipped
            LOGGER.warning("Skipping Treasury yield fetch due to error: %s", exc)
        else:
            for series_id in treasury_series_ids:
                series = treasury_payload.get(series_id)
                if series is None:
                    LOGGER.warning("Treasury payload did not include %s", series_id)
                    continue
                payload[series_id] = series

    remaining_series_ids = [series_id for series_id in target_series_ids if series_id not in payload]
    if not remaining_series_ids:
        return payload

    if not resolved_api_key:
        LOGGER.warning(
            "Skipping FRED-only series without FRED_API_KEY: %s",
            ", ".join(remaining_series_ids),
        )
        return payload

    for series_id in remaining_series_ids:
        LOGGER.info("Calling fred api series %s", series_id)
        try:
            series = _fetch_series_with_fred_api(
                series_id,
                resolved_api_key,
                session=session,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:  # pragma: no cover - network failures are logged and skipped
            LOGGER.warning("Skipping FRED series %s due to fetch error: %s", series_id, exc)
            continue
        payload[series_id] = series

    return payload


def normalize_macro_signals(series_payloads: dict[str, pd.Series]) -> pd.DataFrame:
    """Normalize FRED series payloads into QMIS signal rows."""
    rows: list[dict[str, Any]] = []

    for series_id, descriptor in MACRO_SERIES.items():
        series = series_payloads.get(series_id)
        if series is None:
            continue

        normalized = pd.to_numeric(series, errors="coerce").dropna().sort_index()
        normalized.index = pd.to_datetime(normalized.index)

        for ts, value in normalized.items():
            rows.append(
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "fred",
                    "category": "macro",
                    "series_name": descriptor["series_name"],
                    "value": float(value),
                    "unit": descriptor["unit"],
                    "metadata": json.dumps(
                        {
                            "series_id": series_id,
                            "indicator_name": descriptor["indicator_name"],
                            "frequency": descriptor["frequency"],
                            "source_type": "fred",
                        },
                        sort_keys=True,
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_macro_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized macro signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        payload = signals.copy()
        connection.register("macro_signals_df", payload)
        connection.execute(
            """
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM macro_signals_df
            """
        )
        connection.unregister("macro_signals_df")
    return int(len(signals))


def run_macro_collector(db_path: Path | None = None) -> int:
    """Fetch and persist the spec-defined macro signals."""
    with requests.Session() as session:
        series_payloads = fetch_macro_series(session=session, timeout_seconds=10)
    signals = normalize_macro_signals(series_payloads)
    return persist_macro_signals(signals, db_path=db_path)

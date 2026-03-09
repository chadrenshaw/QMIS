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
FRED_GRAPH_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

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


def _fetch_series_with_fredapi(series_id: str, api_key: str) -> pd.Series:
    try:
        from fredapi import Fred
    except ImportError as exc:  # pragma: no cover - exercised via fallback path in test environments
        raise RuntimeError("fredapi is not installed.") from exc

    client = Fred(api_key=api_key)
    series = client.get_series(series_id)
    if series is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(series, errors="coerce")


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
) -> dict[str, pd.Series]:
    """Fetch the spec-defined macro series from FRED."""
    resolved_api_key = api_key or os.getenv("FRED_API_KEY")
    payload: dict[str, pd.Series] = {}
    target_series_ids = series_ids or list(MACRO_SERIES)

    for series_id in target_series_ids:
        try:
            if resolved_api_key:
                series = _fetch_series_with_fredapi(series_id, resolved_api_key)
            else:
                series = _fetch_series_with_csv_fallback(series_id, session=session)
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
    series_payloads = fetch_macro_series()
    signals = normalize_macro_signals(series_payloads)
    return persist_macro_signals(signals, db_path=db_path)

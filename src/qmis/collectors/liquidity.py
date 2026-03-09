"""Liquidity data collector for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.collectors._persistence import replace_signal_rows
from qmis.collectors.macro import MACRO_SERIES, fetch_macro_series
from qmis.collectors.market import MARKET_SERIES, _extract_close_frame, fetch_market_download
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


LIQUIDITY_FRED_SERIES = {
    series_id: MACRO_SERIES[series_id]
    for series_id in ("M2SL", "WALCL", "RRPONTSYD")
}

LIQUIDITY_MARKET_SERIES = {
    ticker: MARKET_SERIES[ticker]
    for ticker in ("DX-Y.NYB",)
}


def normalize_liquidity_signals(
    macro_series_payloads: dict[str, pd.Series],
    market_download: pd.DataFrame,
) -> pd.DataFrame:
    """Normalize liquidity source payloads into QMIS signal rows."""
    rows: list[dict[str, Any]] = []

    for series_id, descriptor in LIQUIDITY_FRED_SERIES.items():
        series = macro_series_payloads.get(series_id)
        if series is None:
            continue

        normalized = pd.to_numeric(series, errors="coerce").dropna().sort_index()
        normalized.index = pd.to_datetime(normalized.index)
        for ts, value in normalized.items():
            rows.append(
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "fred",
                    "category": "liquidity",
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

    for ticker, descriptor in LIQUIDITY_MARKET_SERIES.items():
        close_frame = _extract_close_frame(market_download, ticker)
        for ts, row in close_frame.iterrows():
            rows.append(
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "yfinance",
                    "category": "liquidity",
                    "series_name": descriptor["series_name"],
                    "value": float(row["Close"]),
                    "unit": descriptor["unit"],
                    "metadata": json.dumps(
                        {
                            "ticker": ticker,
                            "asset_name": descriptor["asset_name"],
                            "interval": "1d",
                            "source_type": "yfinance",
                        },
                        sort_keys=True,
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_liquidity_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized liquidity signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        return replace_signal_rows(connection, signals, "liquidity_signals_df")


def run_liquidity_collector(db_path: Path | None = None) -> int:
    """Fetch and persist the spec-defined liquidity inputs."""
    with requests.Session() as session:
        macro_series_payloads = fetch_macro_series(
            series_ids=list(LIQUIDITY_FRED_SERIES),
            session=session,
            timeout_seconds=10,
        )
    market_download = fetch_market_download(tickers=list(LIQUIDITY_MARKET_SERIES))
    signals = normalize_liquidity_signals(
        macro_series_payloads=macro_series_payloads,
        market_download=market_download,
    )
    return persist_liquidity_signals(signals, db_path=db_path)

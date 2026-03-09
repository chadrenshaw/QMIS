"""Market breadth collector for QMIS."""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.collectors.market import _extract_close_frame, fetch_market_download
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


SP500_CONSTITUENTS_URL = (
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"
)


def normalize_constituent_symbol(symbol: str) -> str:
    """Normalize public constituent symbols into yfinance-compatible tickers."""
    return symbol.replace(".", "-").strip()


def fetch_sp500_constituents(
    session: requests.Session | None = None,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """Fetch the current S&P 500 constituent list from the selected public CSV source."""
    http = session or requests.Session()
    response = http.get(SP500_CONSTITUENTS_URL, timeout=timeout_seconds)
    response.raise_for_status()

    frame = pd.read_csv(StringIO(response.text))
    if "Symbol" not in frame.columns:
        raise ValueError("S&P 500 constituent CSV is missing the Symbol column.")
    frame["yfinance_symbol"] = frame["Symbol"].map(normalize_constituent_symbol)
    return frame


def fetch_breadth_market_download(
    symbols: list[str],
    period: str = "450d",
    interval: str = "1d",
) -> pd.DataFrame:
    """Fetch constituent price history for breadth calculations."""
    return fetch_market_download(period=period, interval=interval, tickers=symbols)


def _build_close_frame(raw_download: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    series_map: dict[str, pd.Series] = {}
    for symbol in symbols:
        try:
            close_frame = _extract_close_frame(raw_download, symbol)
        except ValueError:
            continue
        series_map[symbol] = close_frame["Close"]

    if not series_map:
        return pd.DataFrame()

    close_df = pd.DataFrame(series_map).sort_index()
    close_df.index = pd.to_datetime(close_df.index)
    return close_df


def calculate_breadth_signals(
    raw_download: pd.DataFrame,
    constituents: pd.DataFrame,
    moving_average_window: int = 200,
    high_low_window: int = 252,
) -> pd.DataFrame:
    """Derive the spec-defined breadth metrics from constituent closes."""
    symbols = constituents["yfinance_symbol"] if "yfinance_symbol" in constituents.columns else constituents["Symbol"]
    close_df = _build_close_frame(raw_download, list(symbols))
    if close_df.empty:
        return pd.DataFrame(columns=["ts", "source", "category", "series_name", "value", "unit", "metadata"])

    rolling_mean = close_df.rolling(window=moving_average_window, min_periods=moving_average_window).mean()
    above_ma = (close_df > rolling_mean).sum(axis=1) / (close_df > rolling_mean).count(axis=1) * 100.0

    rolling_high = close_df.rolling(window=high_low_window, min_periods=high_low_window).max()
    rolling_low = close_df.rolling(window=high_low_window, min_periods=high_low_window).min()
    new_highs = (close_df == rolling_high).sum(axis=1).astype(float)
    new_lows = (close_df == rolling_low).sum(axis=1).astype(float)

    daily_changes = close_df.diff()
    advances = (daily_changes > 0).sum(axis=1)
    declines = (daily_changes < 0).sum(axis=1)
    ad_line = (advances - declines).cumsum().astype(float)

    start_index = max(moving_average_window, high_low_window, 2) - 1
    valid_dates = close_df.index[start_index:]

    rows: list[dict[str, Any]] = []
    metadata_common = {
        "constituents_source": "datasets_s_and_p_500_companies",
        "prices_source": "yfinance",
        "moving_average_window": moving_average_window,
        "high_low_window": high_low_window,
    }

    for ts in valid_dates:
        rows.extend(
            [
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "derived_breadth",
                    "category": "breadth",
                    "series_name": "sp500_above_200dma",
                    "value": float(above_ma.loc[ts]),
                    "unit": "percent",
                    "metadata": json.dumps(metadata_common, sort_keys=True),
                },
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "derived_breadth",
                    "category": "breadth",
                    "series_name": "advance_decline_line",
                    "value": float(ad_line.loc[ts]),
                    "unit": "count",
                    "metadata": json.dumps(metadata_common, sort_keys=True),
                },
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "derived_breadth",
                    "category": "breadth",
                    "series_name": "new_highs",
                    "value": float(new_highs.loc[ts]),
                    "unit": "count",
                    "metadata": json.dumps(metadata_common, sort_keys=True),
                },
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "derived_breadth",
                    "category": "breadth",
                    "series_name": "new_lows",
                    "value": float(new_lows.loc[ts]),
                    "unit": "count",
                    "metadata": json.dumps(metadata_common, sort_keys=True),
                },
            ]
        )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_breadth_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized breadth signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        payload = signals.copy()
        connection.register("breadth_signals_df", payload)
        connection.execute(
            """
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM breadth_signals_df
            """
        )
        connection.unregister("breadth_signals_df")
    return int(len(signals))


def run_breadth_collector(
    db_path: Path | None = None,
    moving_average_window: int = 200,
    high_low_window: int = 252,
) -> int:
    """Fetch and persist the spec-defined breadth signals."""
    constituents = fetch_sp500_constituents().copy()
    if "yfinance_symbol" not in constituents.columns:
        constituents["yfinance_symbol"] = constituents["Symbol"].map(normalize_constituent_symbol)
    symbols = constituents["yfinance_symbol"].dropna().tolist()
    raw_download = fetch_breadth_market_download(symbols)
    signals = calculate_breadth_signals(
        raw_download=raw_download,
        constituents=constituents,
        moving_average_window=moving_average_window,
        high_low_window=high_low_window,
    )
    return persist_breadth_signals(signals, db_path=db_path)

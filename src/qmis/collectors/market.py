"""Market data collector for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

from qmis.collectors._persistence import replace_signal_rows
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


MARKET_SERIES: dict[str, dict[str, str]] = {
    "GC=F": {"series_name": "gold", "asset_name": "Gold Futures", "unit": "usd"},
    "CL=F": {"series_name": "oil", "asset_name": "Oil Futures", "unit": "usd"},
    "HG=F": {"series_name": "copper", "asset_name": "Copper Futures", "unit": "usd"},
    "^GSPC": {"series_name": "sp500", "asset_name": "S&P500", "unit": "index_points"},
    "^VIX": {"series_name": "vix", "asset_name": "VIX Volatility Index", "unit": "index_points"},
    "DX-Y.NYB": {"series_name": "dollar_index", "asset_name": "US Dollar Index", "unit": "index_points"},
}


def fetch_market_download(
    period: str = "400d",
    interval: str = "1d",
    tickers: list[str] | None = None,
    threads: bool = False,
    timeout_seconds: int = 10,
) -> pd.DataFrame:
    """Fetch market history for the spec-defined market assets."""
    return yf.download(
        tickers=tickers or list(MARKET_SERIES.keys()),
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        progress=False,
        threads=threads,
        timeout=timeout_seconds,
        multi_level_index=True,
    )


def _extract_close_frame(raw_download: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(raw_download.columns, pd.MultiIndex):
        if ticker not in raw_download.columns.get_level_values(0):
            raise ValueError(f"Ticker {ticker} missing from yfinance download payload.")
        frame = raw_download[ticker]
    else:
        frame = raw_download

    if "Close" not in frame.columns:
        raise ValueError(f"Ticker {ticker} missing Close column.")

    close_frame = frame[["Close"]].dropna().copy()
    close_frame.index = pd.to_datetime(close_frame.index)
    return close_frame.sort_index()


def normalize_market_signals(raw_download: pd.DataFrame) -> pd.DataFrame:
    """Normalize a yfinance download payload into QMIS signal rows."""
    rows: list[dict[str, Any]] = []

    for ticker, descriptor in MARKET_SERIES.items():
        close_frame = _extract_close_frame(raw_download, ticker)
        for ts, row in close_frame.iterrows():
            rows.append(
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "yfinance",
                    "category": "market",
                    "series_name": descriptor["series_name"],
                    "value": float(row["Close"]),
                    "unit": descriptor["unit"],
                    "metadata": json.dumps(
                        {
                            "ticker": ticker,
                            "asset_name": descriptor["asset_name"],
                            "interval": "1d",
                        },
                        sort_keys=True,
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_market_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized market signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        return replace_signal_rows(connection, signals, "market_signals_df")


def run_market_collector(db_path: Path | None = None, period: str = "400d") -> int:
    """Fetch and persist the spec-defined market signals."""
    raw_download = fetch_market_download(period=period)
    signals = normalize_market_signals(raw_download)
    return persist_market_signals(signals, db_path=db_path)

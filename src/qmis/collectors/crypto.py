"""Crypto data collector for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from qmis.collectors.market import _extract_close_frame, fetch_market_download
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


CRYPTO_PRICE_SERIES: dict[str, dict[str, str]] = {
    "BTC-USD": {"series_name": "BTCUSD", "asset_name": "Bitcoin", "unit": "usd"},
    "ETH-USD": {"series_name": "ETHUSD", "asset_name": "Ethereum", "unit": "usd"},
}

COINGECKO_GLOBAL_URL = "https://api.coingecko.com/api/v3/global"


def fetch_crypto_market_download(period: str = "400d", interval: str = "1d") -> pd.DataFrame:
    """Fetch BTC and ETH history from yfinance."""
    return fetch_market_download(
        period=period,
        interval=interval,
        tickers=list(CRYPTO_PRICE_SERIES.keys()),
    )


def fetch_crypto_global_metrics(
    session: requests.Session | None = None,
    timeout_seconds: int = 30,
) -> dict[str, object]:
    """Fetch total crypto market cap and BTC dominance."""
    http = session or requests.Session()
    response = http.get(COINGECKO_GLOBAL_URL, timeout=timeout_seconds)
    response.raise_for_status()

    payload = response.json()
    fetched_at = pd.Timestamp.utcnow()
    return {
        "fetched_at": fetched_at,
        "data": payload.get("data", {}),
    }


def normalize_crypto_signals(
    market_download: pd.DataFrame,
    global_metrics_payload: dict[str, object],
) -> pd.DataFrame:
    """Normalize crypto source payloads into QMIS signal rows."""
    rows: list[dict[str, Any]] = []

    for ticker, descriptor in CRYPTO_PRICE_SERIES.items():
        close_frame = _extract_close_frame(market_download, ticker)
        for ts, row in close_frame.iterrows():
            rows.append(
                {
                    "ts": pd.Timestamp(ts).to_pydatetime(),
                    "source": "yfinance",
                    "category": "crypto",
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

    fetched_at_timestamp = pd.Timestamp(global_metrics_payload["fetched_at"])
    if fetched_at_timestamp.tzinfo is not None:
        fetched_at_timestamp = fetched_at_timestamp.tz_convert("UTC").tz_localize(None)
    fetched_at = fetched_at_timestamp.to_pydatetime()
    global_data = global_metrics_payload.get("data", {})
    total_market_cap = global_data.get("total_market_cap", {})
    market_cap_percentage = global_data.get("market_cap_percentage", {})

    if "usd" in total_market_cap and total_market_cap["usd"] is not None:
        rows.append(
            {
                "ts": fetched_at,
                "source": "coingecko",
                "category": "crypto",
                "series_name": "crypto_market_cap",
                "value": float(total_market_cap["usd"]),
                "unit": "usd_market_cap",
                "metadata": json.dumps(
                    {
                        "api": "global",
                        "metric": "total_market_cap_usd",
                        "source_type": "coingecko",
                    },
                    sort_keys=True,
                ),
            }
        )

    if "btc" in market_cap_percentage and market_cap_percentage["btc"] is not None:
        rows.append(
            {
                "ts": fetched_at,
                "source": "coingecko",
                "category": "crypto",
                "series_name": "BTC_dominance",
                "value": float(market_cap_percentage["btc"]),
                "unit": "pct",
                "metadata": json.dumps(
                    {
                        "api": "global",
                        "metric": "btc_market_cap_percentage",
                        "source_type": "coingecko",
                    },
                    sort_keys=True,
                ),
            }
        )

    return pd.DataFrame(rows).sort_values(["ts", "series_name"]).reset_index(drop=True)


def persist_crypto_signals(signals: pd.DataFrame, db_path: Path | None = None) -> int:
    """Insert normalized crypto signals into the DuckDB `signals` table."""
    if signals.empty:
        return 0

    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        payload = signals.copy()
        connection.register("crypto_signals_df", payload)
        connection.execute(
            """
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM crypto_signals_df
            """
        )
        connection.unregister("crypto_signals_df")
    return int(len(signals))


def run_crypto_collector(db_path: Path | None = None) -> int:
    """Fetch and persist the spec-defined crypto inputs."""
    market_download = fetch_crypto_market_download()
    global_metrics_payload = fetch_crypto_global_metrics()
    signals = normalize_crypto_signals(
        market_download=market_download,
        global_metrics_payload=global_metrics_payload,
    )
    return persist_crypto_signals(signals, db_path=db_path)

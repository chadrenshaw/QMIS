#!/usr/bin/env python3
"""QMIS standalone macro sentiment engine using live Yahoo Finance market data."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import yfinance as yf
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from scipy.stats import linregress


TICKERS: dict[str, str] = {
    "Gold Futures": "GC=F",
    "Oil Futures": "CL=F",
    "Copper Futures": "HG=F",
    "S&P500": "^GSPC",
    "10 Year Treasury Yield": "^TNX",
    "3 Month Treasury": "^IRX",
    "VIX Volatility Index": "^VIX",
    "US Dollar Index": "DX-Y.NYB",
    "High Yield Bond ETF": "HYG",
    "TIPS ETF": "TIP",
}

TIMEFRAMES: dict[str, int] = {
    "12m": 252,
    "3m": 63,
    "1m": 21,
}

YIELD_TICKERS = {"^TNX", "^IRX"}
PRIMARY_WINDOW = "3m"
ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_LOG_PATH = ROOT_DIR / "logs" / "macro_regime_history.csv"
DEFAULT_STATE_DB_PATH = ROOT_DIR / "db" / "macro_sentiment_engine.db"
DEFAULT_NTFY_TOPIC_URL = "https://ntfy.chadlee.org/markets"
ACTIVE_ALERT_WINDOW_HOURS = 24


class MarketDataError(RuntimeError):
    """Raised when required market data cannot be fetched or normalized."""


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Global macro sentiment dashboard")
    parser.add_argument(
        "--log-path",
        type=Path,
        default=DEFAULT_LOG_PATH,
        help="CSV file used to append regime history",
    )
    parser.add_argument(
        "--no-log",
        action="store_true",
        help="Disable CSV regime history logging",
    )
    parser.add_argument(
        "--ntfy-topic-url",
        default=DEFAULT_NTFY_TOPIC_URL,
        help="ntfy topic URL for consolidated change notifications",
    )
    parser.add_argument(
        "--no-ntfy",
        action="store_true",
        help="Disable ntfy notifications",
    )
    return parser.parse_args(argv)


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "QMIS/1.0 "
                "(https://finance.yahoo.com; contact=local-script)"
            )
        }
    )
    return session


def verify_yahoo_connectivity(session: requests.Session) -> None:
    try:
        response = session.get("https://finance.yahoo.com", timeout=10)
        response.close()
    except requests.RequestException as exc:
        raise MarketDataError(f"Network error while reaching Yahoo Finance: {exc}") from exc


def fetch_market_data(period: str = "14mo") -> dict[str, pd.DataFrame]:
    session = build_http_session()
    tickers = list(TICKERS.values())

    try:
        verify_yahoo_connectivity(session)
        raw = yf.download(
            tickers=tickers,
            period=period,
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
            timeout=20,
            multi_level_index=True,
        )
    except requests.RequestException as exc:
        raise MarketDataError(f"Network error while fetching market data: {exc}") from exc
    except Exception as exc:  # pragma: no cover - depends on upstream transport failures.
        raise MarketDataError(f"Unexpected yfinance failure: {exc}") from exc
    finally:
        session.close()

    if raw.empty:
        raise MarketDataError("Received an empty dataset from Yahoo Finance.")

    histories: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        histories[ticker] = extract_ticker_history(raw, ticker)

    return histories


def extract_ticker_history(raw: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if isinstance(raw.columns, pd.MultiIndex):
        if ticker not in raw.columns.get_level_values(0):
            raise MarketDataError(f"Ticker {ticker} was missing from the download payload.")
        frame = raw[ticker].copy()
    else:
        frame = raw.copy()

    if "Close" not in frame.columns:
        raise MarketDataError(f"Ticker {ticker} did not include a Close column.")

    history = frame[["Close"]].dropna()
    if history.empty:
        raise MarketDataError(f"Ticker {ticker} returned no usable history.")

    history.index = pd.to_datetime(history.index)
    return history.sort_index()


def calculate_percent_change(series: pd.Series) -> float:
    if len(series) < 2:
        return 0.0
    first = float(series.iloc[0])
    last = float(series.iloc[-1])
    if np.isclose(first, 0.0):
        return 0.0
    return ((last / first) - 1.0) * 100.0


def calculate_slope(series: pd.Series) -> float:
    if len(series) < 2:
        return 0.0
    x_axis = np.arange(len(series), dtype=float)
    y_axis = series.astype(float).to_numpy()
    return float(linregress(x_axis, y_axis).slope)


def classify_direction(percent_change: float) -> str:
    if percent_change > 5.0:
        return "UP"
    if percent_change < -5.0:
        return "DOWN"
    return "SIDEWAYS"


def calculate_trends(histories: dict[str, pd.DataFrame]) -> dict[str, dict[str, dict[str, float | str]]]:
    trends: dict[str, dict[str, dict[str, float | str]]] = {}

    for ticker, history in histories.items():
        close = history["Close"].dropna()
        ticker_trends: dict[str, dict[str, float | str]] = {}

        for label, window in TIMEFRAMES.items():
            segment = close.tail(window)
            percent_change = calculate_percent_change(segment)
            slope = calculate_slope(segment)
            ticker_trends[label] = {
                "percent_change": percent_change,
                "slope": slope,
                "direction": classify_direction(percent_change),
            }

        trends[ticker] = ticker_trends

    return trends


def latest_market_values(histories: dict[str, pd.DataFrame]) -> dict[str, float]:
    values: dict[str, float] = {}
    for ticker, history in histories.items():
        raw_value = float(history["Close"].iloc[-1])
        values[ticker] = normalize_market_value(ticker, raw_value)
    return values


def normalize_market_value(ticker: str, value: float) -> float:
    if ticker in YIELD_TICKERS and abs(value) > 20:
        return value / 10.0
    return value


def yield_curve_state(yield_curve: float) -> str:
    return "NORMAL" if yield_curve > 0 else "INVERTED"


def is_up(trends: dict[str, dict[str, dict[str, float | str]]], ticker: str, timeframe: str = PRIMARY_WINDOW) -> bool:
    return trends[ticker][timeframe]["direction"] == "UP"


def is_down(
    trends: dict[str, dict[str, dict[str, float | str]]],
    ticker: str,
    timeframe: str = PRIMARY_WINDOW,
) -> bool:
    return trends[ticker][timeframe]["direction"] == "DOWN"


def summarize_indicator(positive_count: int, total_count: int, inverse: bool = False) -> str:
    negative_count = total_count - positive_count
    if inverse:
        if positive_count >= max(2, total_count - 1):
            return "HIGH"
        if negative_count >= max(2, total_count - 1):
            return "LOW"
        return "MIXED"

    if positive_count >= max(2, total_count - 1):
        return "STRENGTHENING"
    if negative_count >= max(2, total_count - 1):
        return "WEAKENING"
    return "MIXED"


def calculate_macro_scores(
    trends: dict[str, dict[str, dict[str, float | str]]],
    yield_curve: float,
) -> dict[str, Any]:
    inflation_components = {
        "gold": is_up(trends, "GC=F"),
        "oil": is_up(trends, "CL=F"),
        "yields": is_up(trends, "^TNX"),
        "tip": is_up(trends, "TIP"),
    }
    growth_components = {
        "sp500": is_up(trends, "^GSPC"),
        "copper": is_up(trends, "HG=F"),
        "oil": is_up(trends, "CL=F"),
    }
    risk_components = {
        "vix": is_up(trends, "^VIX"),
        "sp500": is_down(trends, "^GSPC"),
        "credit": is_down(trends, "HYG"),
        "yield_curve": yield_curve <= 0,
    }

    inflation_score = sum(inflation_components.values())
    growth_score = sum(growth_components.values())
    risk_score = sum(risk_components.values())

    return {
        "inflation_score": inflation_score,
        "growth_score": growth_score,
        "risk_score": risk_score,
        "yield_curve_state": yield_curve_state(yield_curve),
        "yields_falling": is_down(trends, "^TNX"),
        "credit_risk_indicator": trends["HYG"][PRIMARY_WINDOW]["direction"],
        "inflation_indicator": summarize_indicator(sum(inflation_components.values()), len(inflation_components)),
        "growth_indicator": summarize_indicator(sum(growth_components.values()), len(growth_components)),
        "risk_indicator": summarize_indicator(sum(risk_components.values()), len(risk_components), inverse=True),
    }


def determine_regime(
    inflation_score: int,
    growth_score: int,
    risk_score: int,
    yields_falling: bool,
) -> str:
    if risk_score >= 3:
        return "CRISIS / RISK-OFF"
    if inflation_score >= 3 and growth_score >= 2:
        return "INFLATIONARY EXPANSION"
    if inflation_score >= 2 and growth_score <= 1:
        return "STAGFLATION RISK"
    if growth_score <= 1 and yields_falling:
        return "RECESSION RISK"
    return "NEUTRAL / MIXED"


def generate_alerts(
    trends: dict[str, dict[str, dict[str, float | str]]],
    latest_values: dict[str, float],
    yield_curve: float,
) -> list[str]:
    alerts: list[str] = []

    if yield_curve <= 0:
        alerts.append("Yield Curve Inversion Detected")
    if latest_values["^VIX"] > 25:
        alerts.append("Elevated VIX Above 25")
    if float(trends["CL=F"]["3m"]["percent_change"]) > 10:
        alerts.append("Rising Oil Prices")
    if is_down(trends, "^GSPC") and is_up(trends, "^VIX"):
        alerts.append("S&P 500 Falling While VIX Rising")
    if is_up(trends, "GC=F") and is_down(trends, "^TNX"):
        alerts.append("Gold Rising While Yields Falling")
    if float(trends["HYG"]["3m"]["percent_change"]) <= -5 or is_down(trends, "HYG"):
        alerts.append("Credit Proxy Collapsing")

    return alerts


def build_signal_snapshot(
    trends: dict[str, dict[str, dict[str, float | str]]],
    latest_values: dict[str, float],
    yield_curve: float,
    regime: str,
) -> dict[str, dict[str, Any]]:
    oil_change = float(trends["CL=F"]["3m"]["percent_change"])
    credit_change = float(trends["HYG"]["3m"]["percent_change"])

    return {
        "regime": {
            "value": regime,
            "label": "Macro Regime",
            "severity": "critical",
            "tags": ["macro", "regime"],
        },
        "oil_3m_trend": {
            "value": str(trends["CL=F"]["3m"]["direction"]),
            "label": "Oil 3M Trend",
            "severity": "warning",
            "tags": ["macro", "oil"],
        },
        "sp500_3m_trend": {
            "value": str(trends["^GSPC"]["3m"]["direction"]),
            "label": "S&P 500 3M Trend",
            "severity": "warning",
            "tags": ["macro", "equities"],
        },
        "vix_3m_trend": {
            "value": str(trends["^VIX"]["3m"]["direction"]),
            "label": "VIX 3M Trend",
            "severity": "warning",
            "tags": ["macro", "vix"],
        },
        "credit_3m_trend": {
            "value": str(trends["HYG"]["3m"]["direction"]),
            "label": "Credit Proxy 3M Trend",
            "severity": "critical",
            "tags": ["macro", "credit"],
        },
        "yield_curve_state": {
            "value": yield_curve_state(yield_curve),
            "label": "Yield Curve",
            "severity": "critical",
            "tags": ["macro", "rates"],
        },
        "vix_level": {
            "value": "ABOVE_25" if latest_values["^VIX"] > 25 else "AT_OR_BELOW_25",
            "label": "VIX Level",
            "severity": "warning",
            "tags": ["macro", "vix"],
        },
        "oil_3m_change_band": {
            "value": "ABOVE_10" if oil_change > 10 else "AT_OR_BELOW_10",
            "label": "Oil 3M Change",
            "severity": "warning",
            "tags": ["macro", "oil"],
        },
        "spx_vix_divergence": {
            "value": "ACTIVE" if is_down(trends, "^GSPC") and is_up(trends, "^VIX") else "INACTIVE",
            "label": "S&P 500 / VIX Divergence",
            "severity": "critical",
            "tags": ["macro", "risk"],
        },
        "gold_yields_divergence": {
            "value": "ACTIVE" if is_up(trends, "GC=F") and is_down(trends, "^TNX") else "INACTIVE",
            "label": "Gold / Yields Divergence",
            "severity": "warning",
            "tags": ["macro", "gold", "rates"],
        },
        "credit_stress": {
            "value": "ACTIVE" if credit_change <= -5 or is_down(trends, "HYG") else "INACTIVE",
            "label": "Credit Stress",
            "severity": "critical",
            "tags": ["macro", "credit"],
        },
    }


def ensure_utc_timestamp(value: pd.Timestamp | None = None) -> pd.Timestamp:
    timestamp = value if value is not None else pd.Timestamp.now("UTC")
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def initialize_state_db(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS signal_state (
            signal_key TEXT PRIMARY KEY,
            current_value TEXT NOT NULL,
            label TEXT NOT NULL,
            severity TEXT NOT NULL,
            tags TEXT NOT NULL,
            changed_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL,
            label TEXT NOT NULL,
            severity TEXT NOT NULL,
            tags TEXT NOT NULL,
            from_value TEXT NOT NULL,
            to_value TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            ntfy_sent_at TEXT
        )
        """
    )
    connection.commit()


def serialize_tags(tags: list[str]) -> str:
    return ",".join(dict.fromkeys(tags))


def deserialize_tags(tags: str) -> list[str]:
    return [tag for tag in tags.split(",") if tag]


def row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "signal_key": row["signal_key"],
        "label": row["label"],
        "severity": row["severity"],
        "tags": deserialize_tags(row["tags"]),
        "from_value": row["from_value"],
        "to_value": row["to_value"],
        "message": row["message"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "ntfy_sent_at": row["ntfy_sent_at"],
    }


def apply_signal_snapshot(
    db_path: Path,
    signal_snapshot: dict[str, dict[str, Any]],
    now: pd.Timestamp | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    timestamp = ensure_utc_timestamp(now)
    expires_at = timestamp + pd.Timedelta(hours=ACTIVE_ALERT_WINDOW_HOURS)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        initialize_state_db(connection)

        existing_rows = connection.execute(
            "SELECT signal_key, current_value FROM signal_state"
        ).fetchall()
        existing = {row["signal_key"]: row["current_value"] for row in existing_rows}
        new_events: list[dict[str, Any]] = []

        for signal_key, descriptor in signal_snapshot.items():
            current_value = str(descriptor["value"])
            label = str(descriptor["label"])
            severity = str(descriptor["severity"])
            tags = serialize_tags(list(descriptor["tags"]))
            previous_value = existing.get(signal_key)

            if previous_value is None:
                connection.execute(
                    """
                    INSERT INTO signal_state (
                        signal_key, current_value, label, severity, tags, changed_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (signal_key, current_value, label, severity, tags, timestamp.isoformat(), timestamp.isoformat()),
                )
                continue

            if previous_value != current_value:
                message = f"{label} changed: {previous_value} -> {current_value}"
                cursor = connection.execute(
                    """
                    INSERT INTO alert_events (
                        signal_key, label, severity, tags, from_value, to_value, message, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        signal_key,
                        label,
                        severity,
                        tags,
                        previous_value,
                        current_value,
                        message,
                        timestamp.isoformat(),
                        expires_at.isoformat(),
                    ),
                )
                new_events.append(
                    {
                        "id": cursor.lastrowid,
                        "signal_key": signal_key,
                        "label": label,
                        "severity": severity,
                        "tags": deserialize_tags(tags),
                        "from_value": previous_value,
                        "to_value": current_value,
                        "message": message,
                        "created_at": timestamp.isoformat(),
                        "expires_at": expires_at.isoformat(),
                        "ntfy_sent_at": None,
                    }
                )
                connection.execute(
                    """
                    UPDATE signal_state
                    SET current_value = ?, label = ?, severity = ?, tags = ?, changed_at = ?, last_seen_at = ?
                    WHERE signal_key = ?
                    """,
                    (
                        current_value,
                        label,
                        severity,
                        tags,
                        timestamp.isoformat(),
                        timestamp.isoformat(),
                        signal_key,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE signal_state
                    SET label = ?, severity = ?, tags = ?, last_seen_at = ?
                    WHERE signal_key = ?
                    """,
                    (label, severity, tags, timestamp.isoformat(), signal_key),
                )

        active_rows = connection.execute(
            """
            SELECT id, signal_key, label, severity, tags, from_value, to_value, message, created_at, expires_at, ntfy_sent_at
            FROM alert_events
            WHERE expires_at > ?
            ORDER BY created_at DESC, id DESC
            """,
            (timestamp.isoformat(),),
        ).fetchall()
        connection.commit()

    return new_events, [row_to_event(row) for row in active_rows]


def get_pending_ntfy_events(db_path: Path, now: pd.Timestamp | None = None) -> list[dict[str, Any]]:
    timestamp = ensure_utc_timestamp(now)
    if not db_path.exists():
        return []

    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        initialize_state_db(connection)
        rows = connection.execute(
            """
            SELECT id, signal_key, label, severity, tags, from_value, to_value, message, created_at, expires_at, ntfy_sent_at
            FROM alert_events
            WHERE ntfy_sent_at IS NULL AND expires_at > ?
            ORDER BY created_at ASC, id ASC
            """,
            (timestamp.isoformat(),),
        ).fetchall()

    return [row_to_event(row) for row in rows]


def mark_ntfy_events_sent(db_path: Path, event_ids: list[int], now: pd.Timestamp | None = None) -> None:
    if not event_ids or not db_path.exists():
        return

    timestamp = ensure_utc_timestamp(now).isoformat()
    placeholders = ", ".join("?" for _ in event_ids)

    with sqlite3.connect(db_path) as connection:
        initialize_state_db(connection)
        connection.execute(
            f"UPDATE alert_events SET ntfy_sent_at = ? WHERE id IN ({placeholders})",
            [timestamp, *event_ids],
        )
        connection.commit()


def severity_priority(severity: str) -> str:
    return {"info": "2", "warning": "3", "critical": "4"}.get(severity, "3")


def send_ntfy_summary(
    topic_url: str,
    events: list[dict[str, Any]],
    current_regime: str,
    timeout: int = 10,
) -> None:
    if not events:
        return

    severity_order = {"info": 1, "warning": 2, "critical": 3}
    top_severity = max(events, key=lambda event: severity_order.get(str(event["severity"]), 2))["severity"]
    tags = ["macro", "markets"]
    for event in events:
        tags.extend(event["tags"])

    body_lines = [f"Current regime: {current_regime}", "", "New signal changes:"]
    body_lines.extend(f"- {event['message']}" for event in events)
    body = "\n".join(body_lines)

    response = requests.post(
        topic_url,
        data=body,
        timeout=timeout,
        headers={
            "Title": "Macro Signal Changes",
            "Tags": serialize_tags(tags),
            "Priority": severity_priority(str(top_severity)),
        },
    )
    response.raise_for_status()


def format_recent_event(event: dict[str, Any], now: pd.Timestamp | None = None) -> str:
    timestamp = ensure_utc_timestamp(now)
    created_at = ensure_utc_timestamp(pd.Timestamp(event["created_at"]))
    age = timestamp - created_at
    total_hours = int(age.total_seconds() // 3600)
    total_minutes = int((age.total_seconds() % 3600) // 60)
    age_text = f"{total_hours}h" if total_hours > 0 else f"{total_minutes}m"
    return f"{event['message']} ({age_text} ago)"


def build_sparkline(series: pd.Series, width: int = 16) -> str:
    clean = series.dropna().astype(float)
    if clean.empty:
        return ""

    if len(clean) > width:
        indices = np.linspace(0, len(clean) - 1, width, dtype=int)
        sample = clean.iloc[indices].to_numpy()
    else:
        sample = clean.to_numpy()

    minimum = float(np.min(sample))
    maximum = float(np.max(sample))
    chars = ".-:=+*#%@"

    if np.isclose(maximum, minimum):
        return chars[len(chars) // 2] * len(sample)

    scaled = (sample - minimum) / (maximum - minimum)
    buckets = np.clip((scaled * (len(chars) - 1)).astype(int), 0, len(chars) - 1)
    return "".join(chars[index] for index in buckets)


def format_percent(value: float) -> str:
    return f"{value:+.2f}%"


def format_number(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}f}"


def trend_with_arrow(direction: str) -> str:
    arrows = {"UP": "UP ↑", "DOWN": "DOWN ↓", "SIDEWAYS": "SIDEWAYS →"}
    return arrows.get(direction, direction)


def build_asset_table(
    histories: dict[str, pd.DataFrame],
    trends: dict[str, dict[str, dict[str, float | str]]],
    latest_values: dict[str, float],
) -> Table:
    table = Table(title="Market Trends", box=box.SIMPLE_HEAVY, show_lines=False)
    table.add_column("Asset", style="bold")
    table.add_column("12M")
    table.add_column("3M")
    table.add_column("1M")
    table.add_column("Latest", justify="right")
    table.add_column("Sparkline")

    for asset_name, ticker in TICKERS.items():
        ticker_trends = trends[ticker]
        latest = latest_values[ticker]
        decimals = 2 if ticker in YIELD_TICKERS or ticker == "^VIX" else 2
        table.add_row(
            asset_name,
            trend_with_arrow(str(ticker_trends["12m"]["direction"])),
            trend_with_arrow(str(ticker_trends["3m"]["direction"])),
            trend_with_arrow(str(ticker_trends["1m"]["direction"])),
            format_number(latest, decimals),
            build_sparkline(histories[ticker]["Close"]),
        )

    return table


def build_overview_table(latest_values: dict[str, float], yield_curve: float, scores: dict[str, Any]) -> Table:
    table = Table(title="Macro Overview", box=box.SIMPLE_HEAVY)
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    table.add_row("10Y Yield", f"{latest_values['^TNX']:.2f}%")
    table.add_row("3M Yield", f"{latest_values['^IRX']:.2f}%")
    table.add_row("Yield Curve", f"{yield_curve:+.2f} ({scores['yield_curve_state']})")
    table.add_row("VIX", f"{latest_values['^VIX']:.2f}")
    table.add_row("Dollar Index", f"{latest_values['DX-Y.NYB']:.2f}")
    table.add_row("Inflation Indicator", str(scores["inflation_indicator"]))
    table.add_row("Growth Indicator", str(scores["growth_indicator"]))
    table.add_row("Risk Indicator", str(scores["risk_indicator"]))

    return table


def build_scores_table(scores: dict[str, Any]) -> Table:
    table = Table(title="Macro Scores", box=box.SIMPLE_HEAVY)
    table.add_column("Score", style="bold")
    table.add_column("Value", justify="right")
    table.add_row("Inflation Score", str(scores["inflation_score"]))
    table.add_row("Growth Score", str(scores["growth_score"]))
    table.add_row("Risk Score", str(scores["risk_score"]))
    return table


def print_dashboard(
    histories: dict[str, pd.DataFrame],
    trends: dict[str, dict[str, dict[str, float | str]]],
    latest_values: dict[str, float],
    yield_curve: float,
    scores: dict[str, Any],
    regime: str,
    alerts: list[str],
    recent_events: list[dict[str, Any]],
    console: Console | None = None,
) -> None:
    console = console or Console()
    now = pd.Timestamp.now("UTC")
    console.rule("[bold cyan]GLOBAL MACRO SENTIMENT DASHBOARD")
    console.print(build_asset_table(histories, trends, latest_values))
    console.print(build_overview_table(latest_values, yield_curve, scores))
    console.print(build_scores_table(scores))
    console.print(Panel(Text(regime, justify="center", style="bold magenta"), title="Current Global Regime"))

    if recent_events:
        recent_text = "\n".join(f"CHANGE: {format_recent_event(event, now=now)}" for event in recent_events)
        console.print(Panel(recent_text, title="Recent State Changes (24H)", border_style="yellow"))
    else:
        console.print(Panel("No recent state changes.", title="Recent State Changes (24H)", border_style="green"))

    if alerts:
        alert_text = "\n".join(f"ALERT: {alert}" for alert in alerts)
        console.print(Panel(alert_text, title="Current Conditions", border_style="red"))
    else:
        console.print(Panel("No active alerts.", title="Current Conditions", border_style="green"))


def log_regime_history(
    log_path: Path,
    regime: str,
    scores: dict[str, Any],
    latest_values: dict[str, float],
    yield_curve: float,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "timestamp": pd.Timestamp.now("UTC").isoformat(),
        "regime": regime,
        "inflation_score": scores["inflation_score"],
        "growth_score": scores["growth_score"],
        "risk_score": scores["risk_score"],
        "ten_year_yield": latest_values["^TNX"],
        "three_month_yield": latest_values["^IRX"],
        "yield_curve": yield_curve,
        "vix": latest_values["^VIX"],
        "sp500": latest_values["^GSPC"],
    }

    write_header = not log_path.exists()
    with log_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def analyze_macro_environment() -> dict[str, Any]:
    histories = fetch_market_data()
    trends = calculate_trends(histories)
    latest_values = latest_market_values(histories)
    yield_curve = latest_values["^TNX"] - latest_values["^IRX"]
    scores = calculate_macro_scores(trends, yield_curve)
    regime = determine_regime(
        inflation_score=int(scores["inflation_score"]),
        growth_score=int(scores["growth_score"]),
        risk_score=int(scores["risk_score"]),
        yields_falling=bool(scores["yields_falling"]),
    )
    alerts = generate_alerts(trends, latest_values, yield_curve)
    signal_snapshot = build_signal_snapshot(trends, latest_values, yield_curve, regime)

    return {
        "histories": histories,
        "trends": trends,
        "latest_values": latest_values,
        "yield_curve": yield_curve,
        "scores": scores,
        "regime": regime,
        "alerts": alerts,
        "signal_snapshot": signal_snapshot,
    }


def main() -> int:
    args = parse_args()
    console = Console()

    try:
        payload = analyze_macro_environment()
    except MarketDataError as exc:
        console.print(Panel(str(exc), title="QMIS Macro Engine Error", border_style="red"))
        return 1

    _, recent_events = apply_signal_snapshot(
        db_path=DEFAULT_STATE_DB_PATH,
        signal_snapshot=payload["signal_snapshot"],
    )

    pending_ntfy_events = get_pending_ntfy_events(DEFAULT_STATE_DB_PATH)
    if pending_ntfy_events and not args.no_ntfy:
        try:
            send_ntfy_summary(
                topic_url=args.ntfy_topic_url,
                events=pending_ntfy_events,
                current_regime=str(payload["regime"]),
            )
        except requests.RequestException as exc:
            console.print(Panel(f"Failed to publish ntfy alert: {exc}", title="Notification Warning", border_style="yellow"))
        else:
            mark_ntfy_events_sent(DEFAULT_STATE_DB_PATH, [int(event["id"]) for event in pending_ntfy_events])

    print_dashboard(
        histories=payload["histories"],
        trends=payload["trends"],
        latest_values=payload["latest_values"],
        yield_curve=payload["yield_curve"],
        scores=payload["scores"],
        regime=payload["regime"],
        alerts=payload["alerts"],
        recent_events=recent_events,
        console=console,
    )

    if not args.no_log:
        log_regime_history(
            log_path=args.log_path,
            regime=payload["regime"],
            scores=payload["scores"],
            latest_values=payload["latest_values"],
            yield_curve=payload["yield_curve"],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

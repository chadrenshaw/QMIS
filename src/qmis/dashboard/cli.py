"""Rich CLI dashboard for QMIS derived outputs."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qmis.alerts.engine import load_alert_snapshot
from qmis.schema import bootstrap_database
from qmis.signals.anomalies import detect_relationship_anomalies
from qmis.signals.interpreter import build_operator_snapshot
from qmis.storage import connect_db, get_default_db_path


DASHBOARD_TRENDS = (
    ("gold", "Gold Trend"),
    ("oil", "Oil Trend"),
    ("copper", "Copper Trend"),
    ("BTCUSD", "BTC Trend"),
)

DASHBOARD_HISTORY_SERIES = (
    "gold",
    "oil",
    "BTCUSD",
    "yield_10y",
    "yield_3m",
    "fed_balance_sheet",
    "dollar_index",
)

SIGNAL_GROUP_ORDER = ("market", "breadth", "macro", "liquidity", "crypto", "astronomy", "natural")

CATEGORY_TITLES = {
    "market": "Market Signals",
    "breadth": "Breadth Signals",
    "macro": "Macro Signals",
    "liquidity": "Liquidity Signals",
    "crypto": "Crypto Signals",
    "astronomy": "Astronomy Signals",
    "natural": "Natural Signals",
}


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _latest_rows(connection, table_name: str, value_columns: str = "*"):
    return connection.execute(
        f"""
        SELECT {value_columns}
        FROM (
            SELECT
                {value_columns},
                ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
            FROM {table_name}
        )
        WHERE row_number = 1
        """
    ).fetchdf()


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return None


def _build_freshness(
    latest_signal_ts: Any,
    latest_regime_ts: Any,
    latest_relationship_ts: Any,
) -> dict[str, Any]:
    timestamps = [
        ts for ts in (
            _coerce_timestamp(latest_signal_ts),
            _coerce_timestamp(latest_regime_ts),
            _coerce_timestamp(latest_relationship_ts),
        )
        if ts is not None
    ]
    if not timestamps:
        return {
            "status": "empty",
            "latest_signal_ts": latest_signal_ts,
            "latest_regime_ts": latest_regime_ts,
            "latest_relationship_ts": latest_relationship_ts,
        }

    newest = max(timestamps)
    age_days = (datetime.now(UTC) - newest).days
    status = "fresh" if age_days <= 5 else "stale"
    return {
        "status": status,
        "latest_signal_ts": latest_signal_ts,
        "latest_regime_ts": latest_regime_ts,
        "latest_relationship_ts": latest_relationship_ts,
        "age_days": age_days,
    }


def load_dashboard_snapshot(db_path: Path | None = None) -> dict[str, Any]:
    """Load the latest derived dashboard state from DuckDB."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path, read_only=True) as connection:
        feature_rows = connection.execute(
            """
            SELECT ts, series_name, trend_label
            FROM (
                SELECT
                    ts,
                    series_name,
                    trend_label,
                    ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
                FROM features
            )
            WHERE row_number = 1
            """
        ).fetchdf()
        signal_rows = connection.execute(
            """
            SELECT ts, series_name, value, unit, category, source, metadata
            FROM (
                SELECT
                    ts,
                    series_name,
                    value,
                    unit,
                    category,
                    source,
                    metadata,
                    ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
                FROM signals
            )
            WHERE row_number = 1
            """
        ).fetchdf()
        regime_rows = connection.execute(
            """
            SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence
            FROM regimes
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        relationship_rows = connection.execute(
            """
            SELECT
                ts,
                series_x,
                series_y,
                window_days,
                lag_days,
                correlation,
                p_value,
                relationship_state,
                confidence_label
            FROM relationships
            ORDER BY ABS(correlation) DESC, window_days DESC
            """
        ).fetchdf()
        history_series = connection.execute(
            f"""
            SELECT ts, series_name, value, unit
            FROM signals
            WHERE series_name IN ({", ".join(["?"] * len(DASHBOARD_HISTORY_SERIES))})
            ORDER BY series_name, ts ASC
            """,
            list(DASHBOARD_HISTORY_SERIES),
        ).fetchdf()
        score_history_rows = connection.execute(
            """
            SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence
            FROM regimes
            ORDER BY ts ASC
            """
        ).fetchdf()

    trend_summary = {
        str(row["series_name"]): {
            "ts": row["ts"],
            "trend_label": str(row["trend_label"]),
        }
        for _, row in feature_rows.iterrows()
    }
    signal_summary = {
        str(row["series_name"]): {
            "ts": row["ts"],
            "value": float(row["value"]),
            "unit": str(row["unit"]),
            "category": str(row["category"]),
            "source": str(row["source"]),
            "metadata": _parse_metadata(row.get("metadata")),
        }
        for _, row in signal_rows.iterrows()
    }

    latest_regime = regime_rows.iloc[0].to_dict() if not regime_rows.empty else None
    scores = (
        {
            "inflation_score": int(latest_regime["inflation_score"]),
            "growth_score": int(latest_regime["growth_score"]),
            "liquidity_score": int(latest_regime["liquidity_score"]),
            "risk_score": int(latest_regime["risk_score"]),
        }
        if latest_regime
        else {}
    )

    yield_10y = signal_summary.get("yield_10y", {}).get("value")
    yield_3m = signal_summary.get("yield_3m", {}).get("value")
    yield_curve = None
    yield_curve_state = "UNKNOWN"
    if yield_10y is not None and yield_3m is not None:
        yield_curve = float(yield_10y) - float(yield_3m)
        yield_curve_state = "NORMAL" if yield_curve > 0 else "INVERTED"

    zero_lag = relationship_rows.loc[relationship_rows["lag_days"] == 0].copy()
    lead_lag = relationship_rows.loc[relationship_rows["lag_days"] != 0].copy()
    top_relationships = zero_lag.loc[zero_lag["relationship_state"] != "broken"].head(3).to_dict("records")
    lead_lag_relationships = lead_lag.head(3).to_dict("records")
    anomalies = detect_relationship_anomalies(zero_lag).to_dict("records")
    signal_history: dict[str, list[dict[str, Any]]] = {}
    for series_name, frame in history_series.groupby("series_name"):
        signal_history[str(series_name)] = [
            {
                "ts": row["ts"],
                "value": float(row["value"]),
                "unit": str(row["unit"]),
            }
            for _, row in frame.iterrows()
        ]

    score_history = [
        {
            "ts": row["ts"],
            "inflation_score": int(row["inflation_score"]),
            "growth_score": int(row["growth_score"]),
            "liquidity_score": int(row["liquidity_score"]),
            "risk_score": int(row["risk_score"]),
            "regime_label": str(row["regime_label"]),
            "confidence": float(row["confidence"]),
        }
        for _, row in score_history_rows.iterrows()
    ]
    signal_groups = {
        category: [
            series_name
            for series_name, signal in signal_summary.items()
            if signal.get("category") == category
        ]
        for category in SIGNAL_GROUP_ORDER
        if any(signal.get("category") == category for signal in signal_summary.values())
    }
    grouped_signals = {
        category: [
            {
                "series_name": series_name,
                "trend_label": trend_summary.get(series_name, {}).get("trend_label", "N/A"),
                "value": signal_summary[series_name]["value"],
                "unit": signal_summary[series_name]["unit"],
                "source": signal_summary[series_name]["source"],
                "metadata": signal_summary[series_name]["metadata"],
                "ts": signal_summary[series_name]["ts"],
            }
            for series_name in sorted(signal_groups[category])
        ]
        for category in signal_groups
    }
    freshness = _build_freshness(
        latest_signal_ts=signal_rows["ts"].max() if not signal_rows.empty else None,
        latest_regime_ts=regime_rows["ts"].max() if not regime_rows.empty else None,
        latest_relationship_ts=relationship_rows["ts"].max() if not relationship_rows.empty else None,
    )
    latest_snapshot_ts = max(
        (
            ts
            for ts in (
                signal_rows["ts"].max() if not signal_rows.empty else None,
                regime_rows["ts"].max() if not regime_rows.empty else None,
                relationship_rows["ts"].max() if not relationship_rows.empty else None,
            )
            if ts is not None
        ),
        default=None,
    )
    alert_rows, alert_summary = load_alert_snapshot(db_path=target_path)

    snapshot = {
        "trend_summary": trend_summary,
        "signal_summary": signal_summary,
        "signal_groups": signal_groups,
        "grouped_signals": grouped_signals,
        "signal_history": signal_history,
        "scores": scores,
        "score_history": score_history,
        "regime": latest_regime,
        "yield_curve": yield_curve,
        "yield_curve_state": yield_curve_state,
        "freshness": freshness,
        "latest_snapshot_ts": latest_snapshot_ts,
        "relationships": relationship_rows.to_dict("records"),
        "top_relationships": top_relationships,
        "lead_lag_relationships": lead_lag_relationships,
        "anomalies": anomalies,
        "alert_summary": alert_summary,
        "alerts": alert_rows.to_dict("records"),
    }
    snapshot["intelligence"] = build_operator_snapshot(snapshot)
    return snapshot


def _build_signal_summary_table(snapshot: dict[str, Any]) -> Table:
    table = Table(title="Signal Summary")
    table.add_column("Signal")
    table.add_column("Trend", justify="center")
    table.add_column("Value", justify="right")

    trend_summary = snapshot["trend_summary"]
    signal_summary = snapshot["signal_summary"]
    for series_name, label in DASHBOARD_TRENDS:
        trend = trend_summary.get(series_name, {}).get("trend_label", "N/A")
        value = signal_summary.get(series_name, {}).get("value")
        display_value = f"{value:,.2f}" if value is not None else "N/A"
        table.add_row(label, trend, display_value)

    vix_value = signal_summary.get("vix", {}).get("value")
    table.add_row("Yield Curve", snapshot["yield_curve_state"], f"{snapshot['yield_curve']:.2f}" if snapshot["yield_curve"] is not None else "N/A")
    table.add_row("VIX", "LEVEL", f"{vix_value:,.2f}" if vix_value is not None else "N/A")
    return table


def _format_signal_value(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):,.2f}"


def _build_category_signal_table(category: str, rows: list[dict[str, Any]]) -> Table:
    table = Table(title=CATEGORY_TITLES.get(category, f"{category.title()} Signals"))
    table.add_column("Signal")
    table.add_column("Trend", justify="center")
    table.add_column("Value", justify="right")
    table.add_column("Unit")
    table.add_column("Source")

    if not rows:
        table.add_row("No data", "-", "-", "-", "-")
        return table

    for row in rows:
        table.add_row(
            str(row["series_name"]),
            str(row["trend_label"]),
            _format_signal_value(row.get("value")),
            str(row["unit"]),
            str(row["source"]),
        )
    return table


def _build_scores_table(snapshot: dict[str, Any]) -> Table:
    table = Table(title="Macro Scores")
    table.add_column("Score")
    table.add_column("Value", justify="right")

    scores = snapshot["scores"]
    for label, key in (
        ("Inflation", "inflation_score"),
        ("Growth", "growth_score"),
        ("Liquidity", "liquidity_score"),
        ("Risk", "risk_score"),
    ):
        value = scores.get(key)
        table.add_row(label, str(value) if value is not None else "N/A")
    return table


def _build_relationships_table(rows: list[dict[str, Any]], title: str) -> Table:
    table = Table(title=title)
    table.add_column("Pair")
    table.add_column("Window", justify="right")
    table.add_column("Lag", justify="right")
    table.add_column("Corr", justify="right")
    table.add_column("State")
    table.add_column("Confidence")

    if not rows:
        table.add_row("No data", "-", "-", "-", "-", "-")
        return table

    for row in rows:
        pair = f"{row['series_x']} vs {row['series_y']}"
        table.add_row(
            pair,
            str(int(row["window_days"])),
            str(int(row["lag_days"])),
            f"{float(row['correlation']):.2f}",
            str(row["relationship_state"]),
            str(row["confidence_label"]),
        )
    return table


def _build_anomalies_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(title="Anomalies")
    table.add_column("Pair")
    table.add_column("Type")
    table.add_column("Historical")
    table.add_column("Current")

    if not rows:
        table.add_row("No anomalies", "-", "-", "-")
        return table

    for row in rows:
        pair = f"{row['series_x']} vs {row['series_y']}"
        table.add_row(pair, str(row["anomaly_type"]), str(row["historical_state"]), str(row["current_state"]))
    return table


def _build_alerts_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(title="Alerts")
    table.add_column("Severity")
    table.add_column("Type")
    table.add_column("Title")
    table.add_column("Message")

    if not rows:
        table.add_row("clear", "-", "No active alerts", "-")
        return table

    for row in rows[:8]:
        table.add_row(
            str(row["severity"]),
            str(row["alert_type"]),
            str(row["title"]),
            str(row["message"]),
        )
    return table


def render_world_snapshot(snapshot: dict[str, Any], console: Console) -> None:
    intelligence = snapshot["intelligence"]
    console.print(Panel(intelligence["global_state_line"], title="GLOBAL STATE", expand=False))


def _render_market_pulse(snapshot: dict[str, Any], console: Console) -> None:
    pulse = snapshot["intelligence"]["market_pulse"]
    line = " | ".join(f"{item['label']}: {item['state']}" for item in pulse)
    console.print(Panel(line, title="MARKET PULSE", expand=False))


def _render_cosmic_state(snapshot: dict[str, Any], console: Console) -> None:
    console.print(Panel(snapshot["intelligence"]["cosmic_state_line"], title="COSMIC STATE", expand=False))


def render_market_forces(snapshot: dict[str, Any], console: Console) -> None:
    drivers = snapshot["intelligence"]["market_drivers"]
    body = "\n".join(f"- {driver['title']}: {driver['summary']}" for driver in drivers) if drivers else "- No dominant drivers detected."
    console.print(Panel(body, title="MARKET DRIVERS", expand=False))


def render_relationship_changes(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["relationship_shifts"]
    body = "\n".join(f"- {row['title']}: {row['summary']}" for row in rows[:4]) if rows else "- No material relationship shifts."
    console.print(Panel(body, title="RELATIONSHIP SHIFTS", expand=False))


def render_risk_indicators(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["risk_monitor"]
    body = "\n".join(f"- {key}: {row['level']} | {row['summary']}" for key, row in rows.items())
    console.print(Panel(body, title="RISK MONITOR", expand=False))


def _render_experimental_signals(snapshot: dict[str, Any], console: Console) -> None:
    experimental = snapshot["intelligence"]["experimental_signals"]
    if not experimental["visible"]:
        console.print(Panel(str(experimental["summary"]), title="EXPERIMENTAL SIGNALS", expand=False))
        return

    signal_lines = [
        f"{row['series_name']}: {_format_signal_value(row.get('value'))} {row['unit']}"
        for row in experimental["signals"]
    ]
    correlation_lines = [
        f"{row['pair']} ({row['correlation']:.2f})"
        for row in experimental["correlations"][:4]
    ]
    body = "\n".join(
        ["Signals:"] + [f"- {line}" for line in signal_lines] + ["Correlations:"] + [f"- {line}" for line in correlation_lines]
    )
    console.print(Panel(body, title="EXPERIMENTAL SIGNALS", expand=False))


def render_watchlist(snapshot: dict[str, Any], console: Console) -> None:
    warnings = snapshot["intelligence"]["warning_signals"]
    body = "\n".join(f"- {item['title']}: {item['detail']}" for item in warnings) if warnings else "- No immediate warning signals."
    console.print(Panel(body, title="WARNING SIGNALS", expand=False))


def render_dashboard(snapshot: dict[str, Any], console: Console | None = None) -> None:
    """Render the Rich CLI dashboard for the latest QMIS snapshot."""
    console = console or Console()
    title = Text("OPERATOR INTELLIGENCE SNAPSHOT", style="bold")
    console.print(Panel(title, expand=False))
    render_world_snapshot(snapshot, console)
    _render_market_pulse(snapshot, console)
    _render_cosmic_state(snapshot, console)
    render_market_forces(snapshot, console)
    render_relationship_changes(snapshot, console)
    render_risk_indicators(snapshot, console)
    render_watchlist(snapshot, console)
    _render_experimental_signals(snapshot, console)

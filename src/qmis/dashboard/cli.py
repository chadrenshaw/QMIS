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
    world_state = intelligence["world_state"]

    summary = Table(title="World State Snapshot")
    summary.add_column("Field")
    summary.add_column("Value", justify="right")
    summary.add_row("Sun Sign", str(world_state["sun_sign"]))
    summary.add_row("Solar Longitude", _format_signal_value(world_state.get("solar_longitude")))
    summary.add_row("Zodiac Index", str(world_state.get("zodiac_index", "N/A")))
    summary.add_row("Lunar Phase", str(world_state["lunar_phase"]))
    summary.add_row("Lunar Cycle Day", _format_signal_value(world_state.get("lunar_cycle_day")))
    summary.add_row("Lunar Illumination", _format_signal_value(world_state.get("lunar_illumination")))
    console.print(summary)

    solar_table = Table(title="Solar Activity Metrics")
    solar_table.add_column("Signal")
    solar_table.add_column("Value", justify="right")
    solar_table.add_column("Unit")
    for row in world_state["solar_activity"]:
        solar_table.add_row(str(row["series_name"]), _format_signal_value(row.get("value")), str(row["unit"]))
    if not world_state["solar_activity"]:
        solar_table.add_row("No data", "-", "-")
    console.print(solar_table)

    natural_table = Table(title="Natural Signals")
    natural_table.add_column("Signal")
    natural_table.add_column("Value", justify="right")
    natural_table.add_column("Unit")
    for row in world_state["natural_signals"]:
        natural_table.add_row(str(row["series_name"]), _format_signal_value(row.get("value")), str(row["unit"]))
    if not world_state["natural_signals"]:
        natural_table.add_row("No data", "-", "-")
    console.print(natural_table)


def render_market_forces(snapshot: dict[str, Any], console: Console) -> None:
    forces = snapshot["intelligence"]["market_forces"]
    table = Table(title="Dominant Market Forces")
    table.add_column("Theme")
    table.add_column("Direction")
    table.add_column("Strength", justify="right")
    table.add_column("Lead Pair")

    if not forces:
        table.add_row("No dominant forces", "-", "-", "-")
    else:
        for force in forces[:5]:
            lead_pair = force["pairs"][0]["label"] if force["pairs"] else "-"
            table.add_row(
                str(force["theme"]),
                str(force["direction"]),
                f"{float(force['strength']):.2f}",
                lead_pair,
            )
    console.print(table)


def render_relationship_changes(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["relationship_changes"]
    table = Table(title="Key Relationship Changes")
    table.add_column("Summary")
    table.add_column("Breaks", justify="right")
    table.add_column("Detail")

    if not rows:
        table.add_row("No material relationship breaks", "0", "-")
    else:
        for row in rows[:5]:
            table.add_row(str(row["title"]), str(int(row["count"])), str(row["summary"]))
    console.print(table)


def render_risk_indicators(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["risk_indicators"]
    table = Table(title="Macro Risk Indicators")
    table.add_column("Indicator")
    table.add_column("State")
    table.add_column("Value", justify="right")
    table.add_column("Interpretation")

    for key in ("volatility", "liquidity", "inflation_pressure", "growth_momentum"):
        row = rows.get(key, {})
        value = row.get("value")
        display_value = _format_signal_value(value) if isinstance(value, (int, float)) else str(value or "N/A")
        table.add_row(key, str(row.get("state", "unknown")), display_value, str(row.get("summary", "-")))
    console.print(table)


def _render_significant_correlations(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["significant_correlations"]
    table = Table(title="Significant Correlations")
    table.add_column("Pair")
    table.add_column("Corr", justify="right")
    table.add_column("Window", justify="right")
    table.add_column("p-value", justify="right")

    if not rows:
        table.add_row("No significant correlations", "-", "-", "-")
    else:
        for row in rows[:6]:
            table.add_row(
                str(row["pair"]),
                f"{float(row['correlation']):.2f}",
                str(int(row["window_days"])),
                f"{float(row['p_value']):.4f}",
            )
    console.print(table)


def _render_experimental_signals(snapshot: dict[str, Any], console: Console) -> None:
    experimental = snapshot["intelligence"]["experimental_signals"]
    table = Table(title="Experimental Signals")
    table.add_column("Signal")
    table.add_column("Value", justify="right")
    table.add_column("Unit")

    if not experimental["signals"]:
        table.add_row("No experimental signals", "-", "-")
    else:
        for row in experimental["signals"][:8]:
            table.add_row(str(row["series_name"]), _format_signal_value(row.get("value")), str(row["unit"]))
    console.print(table)

    correlation_table = Table(title="Experimental Correlations")
    correlation_table.add_column("Pair")
    correlation_table.add_column("Corr", justify="right")
    if not experimental["correlations"]:
        correlation_table.add_row("No strong experimental correlations", "-")
    else:
        for row in experimental["correlations"][:4]:
            correlation_table.add_row(str(row["pair"]), f"{float(row['correlation']):.2f}")
    console.print(correlation_table)


def render_watchlist(snapshot: dict[str, Any], console: Console) -> None:
    watchlist = snapshot["intelligence"]["watchlist"]
    table = Table(title="What To Watch")
    table.add_column("Item")
    table.add_column("Why It Matters")

    if not watchlist:
        table.add_row("No watch items", "-")
    else:
        for item in watchlist:
            table.add_row(str(item["title"]), str(item["detail"]))
    console.print(table)


def render_dashboard(snapshot: dict[str, Any], console: Console | None = None) -> None:
    """Render the Rich CLI dashboard for the latest QMIS snapshot."""
    console = console or Console()
    title = Text("OPERATOR INTELLIGENCE SNAPSHOT", style="bold")
    console.print(Panel(title, expand=False))
    regime_label = snapshot["regime"]["regime_label"] if snapshot["regime"] else "N/A"
    confidence = snapshot["regime"]["confidence"] if snapshot["regime"] else None
    regime_text = regime_label if confidence is None else f"{regime_label}\nConfidence: {confidence:.2f}"
    render_world_snapshot(snapshot, console)
    console.print(Panel(regime_text, title="Macro Regime", expand=False))
    render_market_forces(snapshot, console)
    render_relationship_changes(snapshot, console)
    render_risk_indicators(snapshot, console)
    _render_significant_correlations(snapshot, console)
    _render_experimental_signals(snapshot, console)
    render_watchlist(snapshot, console)
    console.print(_build_alerts_table(snapshot["alerts"]))

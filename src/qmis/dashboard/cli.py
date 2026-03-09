"""Rich CLI dashboard for QMIS derived outputs."""

from __future__ import annotations

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

SIGNAL_GROUP_ORDER = ("market", "macro", "liquidity", "crypto", "astronomy", "natural")


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
            SELECT ts, series_name, value, unit, category, source
            FROM (
                SELECT
                    ts,
                    series_name,
                    value,
                    unit,
                    category,
                    source,
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

    return {
        "trend_summary": trend_summary,
        "signal_summary": signal_summary,
        "signal_groups": signal_groups,
        "signal_history": signal_history,
        "scores": scores,
        "score_history": score_history,
        "regime": latest_regime,
        "yield_curve": yield_curve,
        "yield_curve_state": yield_curve_state,
        "freshness": freshness,
        "latest_snapshot_ts": latest_snapshot_ts,
        "top_relationships": top_relationships,
        "lead_lag_relationships": lead_lag_relationships,
        "anomalies": anomalies,
        "alert_summary": alert_summary,
        "alerts": alert_rows.to_dict("records"),
    }


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


def render_dashboard(snapshot: dict[str, Any], console: Console | None = None) -> None:
    """Render the Rich CLI dashboard for the latest QMIS snapshot."""
    console = console or Console()
    title = Text("GLOBAL MACRO DASHBOARD", style="bold")
    console.print(Panel(title, expand=False))

    console.print(_build_signal_summary_table(snapshot))
    console.print(_build_scores_table(snapshot))

    regime_label = snapshot["regime"]["regime_label"] if snapshot["regime"] else "N/A"
    confidence = snapshot["regime"]["confidence"] if snapshot["regime"] else None
    regime_text = regime_label if confidence is None else f"{regime_label}\nConfidence: {confidence:.2f}"
    console.print(Panel(regime_text, title="Current Regime", expand=False))

    console.print(_build_relationships_table(snapshot["top_relationships"], title="Top Relationships"))
    console.print(_build_relationships_table(snapshot["lead_lag_relationships"], title="Lead-Lag Signals"))
    console.print(_build_anomalies_table(snapshot["anomalies"]))
    console.print(_build_alerts_table(snapshot["alerts"]))

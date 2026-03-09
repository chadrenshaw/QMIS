"""Persisted alert materialization for QMIS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from qmis.alerts.rules import evaluate_alert_rules
from qmis.schema import bootstrap_database
from qmis.signals.anomalies import detect_relationship_anomalies
from qmis.storage import connect_db, get_default_db_path


def _latest_regime_rows(connection) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    rows = connection.execute(
        """
        SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence
        FROM regimes
        ORDER BY ts DESC
        LIMIT 2
        """
    ).fetchdf()
    latest = rows.iloc[0].to_dict() if len(rows) >= 1 else None
    previous = rows.iloc[1].to_dict() if len(rows) >= 2 else None
    return latest, previous


def _latest_signals(connection) -> dict[str, dict[str, Any]]:
    rows = connection.execute(
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
        ORDER BY series_name
        """
    ).fetchdf()
    return {
        str(row["series_name"]): {
            "ts": pd.to_datetime(row["ts"]),
            "value": float(row["value"]),
            "unit": str(row["unit"]),
            "category": str(row["category"]),
            "source": str(row["source"]),
        }
        for _, row in rows.iterrows()
    }


def load_alert_snapshot(db_path: Path | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load the persisted alert snapshot and a summary view."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path, read_only=True) as connection:
        alerts = connection.execute(
            """
            SELECT
                ts,
                alert_type,
                severity,
                rule_key,
                dedupe_key,
                title,
                message,
                source_table,
                series_name,
                series_x,
                series_y,
                metadata
            FROM alerts
            ORDER BY
                CASE severity
                    WHEN 'critical' THEN 0
                    WHEN 'warning' THEN 1
                    ELSE 2
                END,
                ts DESC,
                dedupe_key ASC
            """
        ).fetchdf()

    if alerts.empty:
        return alerts, {
            "status": "clear",
            "message": "No active alerts.",
            "updated_at": None,
            "count": 0,
            "alerts": [],
        }

    latest_ts = pd.to_datetime(alerts["ts"]).max()
    severities = alerts["severity"].tolist()
    if "critical" in severities:
        status = "active"
    elif "warning" in severities:
        status = "watch"
    else:
        status = "info"

    summary = {
        "status": status,
        "message": f"{len(alerts)} active alert(s).",
        "updated_at": latest_ts,
        "count": int(len(alerts)),
        "alerts": alerts.to_dict("records"),
    }
    return alerts, summary


def materialize_alerts(db_path: Path | None = None) -> int:
    """Evaluate and replace the current alert snapshot."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        latest_regime, previous_regime = _latest_regime_rows(connection)
        latest_signals = _latest_signals(connection)
        relationships = connection.execute(
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
            ORDER BY series_x, series_y, lag_days, window_days
            """
        ).fetchdf()
        anomalies = detect_relationship_anomalies(relationships)
        cycles = connection.execute(
            """
            SELECT
                ts,
                cycle_name,
                phase,
                strength,
                is_turning_point,
                transition_from,
                alert_on_transition,
                summary,
                supporting_signals,
                metadata
            FROM cycle_snapshots
            ORDER BY cycle_name
            """
        ).fetchdf()

        alerts = evaluate_alert_rules(
            latest_regime=latest_regime,
            previous_regime=previous_regime,
            latest_signals=latest_signals,
            relationships=relationships,
            anomalies=anomalies,
            cycles=cycles,
        )

        connection.execute("DELETE FROM alerts")
        if alerts.empty:
            return 0

        connection.register("alerts_df", alerts)
        connection.execute(
            """
            INSERT INTO alerts (
                ts,
                alert_type,
                severity,
                rule_key,
                dedupe_key,
                title,
                message,
                source_table,
                series_name,
                series_x,
                series_y,
                metadata
            )
            SELECT
                ts,
                alert_type,
                severity,
                rule_key,
                dedupe_key,
                title,
                message,
                source_table,
                series_name,
                series_x,
                series_y,
                metadata
            FROM alerts_df
            """
        )
        connection.unregister("alerts_df")
    return int(len(alerts))

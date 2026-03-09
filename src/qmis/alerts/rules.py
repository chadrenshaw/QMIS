"""Explicit rule catalog for the QMIS alert engine."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd


def _severity_for_regime(regime_label: str) -> str:
    if regime_label in {"CRISIS / RISK-OFF", "RECESSION RISK"}:
        return "critical"
    if regime_label in {"LIQUIDITY WITHDRAWAL", "SPECULATIVE BUBBLE"}:
        return "warning"
    return "info"


def _severity_for_confidence(confidence_label: str) -> str:
    if confidence_label == "validated":
        return "warning"
    if confidence_label == "statistically_significant":
        return "warning"
    return "info"


def _record(
    *,
    ts: Any,
    alert_type: str,
    severity: str,
    rule_key: str,
    dedupe_key: str,
    title: str,
    message: str,
    source_table: str,
    series_name: str | None = None,
    series_x: str | None = None,
    series_y: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "ts": pd.to_datetime(ts),
        "alert_type": alert_type,
        "severity": severity,
        "rule_key": rule_key,
        "dedupe_key": dedupe_key,
        "title": title,
        "message": message,
        "source_table": source_table,
        "series_name": series_name,
        "series_x": series_x,
        "series_y": series_y,
        "metadata": json.dumps(metadata or {}, sort_keys=True),
    }


def build_regime_change_alert(
    latest_regime: dict[str, Any] | None,
    previous_regime: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if not latest_regime or not previous_regime:
        return []
    latest_label = str(latest_regime["regime_label"])
    previous_label = str(previous_regime["regime_label"])
    if latest_label == previous_label:
        return []

    return [
        _record(
            ts=latest_regime["ts"],
            alert_type="regime_change",
            severity=_severity_for_regime(latest_label),
            rule_key="regime_change",
            dedupe_key=f"regime_change:{previous_label}->{latest_label}",
            title="Regime change detected",
            message=f"Macro regime moved from {previous_label} to {latest_label}.",
            source_table="regimes",
            metadata={
                "previous_regime": previous_label,
                "current_regime": latest_label,
                "confidence": latest_regime.get("confidence"),
            },
        )
    ]


def build_threshold_alerts(latest_signals: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    yield_10y = latest_signals.get("yield_10y", {}).get("value")
    yield_3m = latest_signals.get("yield_3m", {}).get("value")
    if yield_10y is not None and yield_3m is not None and float(yield_10y) <= float(yield_3m):
        alerts.append(
            _record(
                ts=latest_signals["yield_10y"]["ts"],
                alert_type="threshold",
                severity="warning",
                rule_key="yield_curve_inversion",
                dedupe_key="threshold:yield_curve_inversion",
                title="Threshold breached",
                message=f"Yield curve inverted to {(float(yield_10y) - float(yield_3m)):.2f}.",
                source_table="signals",
                series_name="yield_curve",
                metadata={"yield_10y": yield_10y, "yield_3m": yield_3m},
            )
        )

    vix = latest_signals.get("vix", {}).get("value")
    if vix is not None and float(vix) >= 25.0:
        alerts.append(
            _record(
                ts=latest_signals["vix"]["ts"],
                alert_type="threshold",
                severity="critical",
                rule_key="vix_stress",
                dedupe_key="threshold:vix_stress",
                title="Threshold breached",
                message=f"VIX reached {float(vix):.2f}, above the stress threshold.",
                source_table="signals",
                series_name="vix",
                metadata={"threshold": 25.0, "value": float(vix)},
            )
        )
    return alerts


def build_correlation_alerts(relationships: pd.DataFrame) -> list[dict[str, Any]]:
    if relationships.empty:
        return []

    alerts: list[dict[str, Any]] = []
    for row in relationships.itertuples(index=False):
        if str(row.relationship_state) not in {"stable", "emerging", "exploratory"}:
            continue
        if str(row.confidence_label) == "likely_spurious":
            continue
        if abs(float(row.correlation)) < 0.6:
            continue

        alerts.append(
            _record(
                ts=row.ts,
                alert_type="correlation_discovery",
                severity=_severity_for_confidence(str(row.confidence_label)),
                rule_key="correlation_detected",
                dedupe_key=f"correlation:{row.series_x}:{row.series_y}:{int(row.window_days)}:{int(row.lag_days)}",
                title="Correlation detected",
                message=(
                    f"{row.series_x} vs {row.series_y} correlation {float(row.correlation):.2f} "
                    f"over {int(row.window_days)}d with lag {int(row.lag_days)}d."
                ),
                source_table="relationships",
                series_x=str(row.series_x),
                series_y=str(row.series_y),
                metadata={
                    "window_days": int(row.window_days),
                    "lag_days": int(row.lag_days),
                    "correlation": float(row.correlation),
                    "confidence_label": str(row.confidence_label),
                },
            )
        )
    return alerts


def build_relationship_break_alerts(anomalies: pd.DataFrame) -> list[dict[str, Any]]:
    if anomalies.empty:
        return []

    alerts: list[dict[str, Any]] = []
    for row in anomalies.itertuples(index=False):
        if hasattr(row, "passes_filter") and not bool(row.passes_filter):
            continue
        severity = "critical" if str(row.anomaly_type) == "relationship_break" else "warning"
        alerts.append(
            _record(
                ts=row.ts,
                alert_type="relationship_break",
                severity=severity,
                rule_key=str(row.anomaly_type),
                dedupe_key=f"{row.anomaly_type}:{row.series_x}:{row.series_y}:{int(row.current_window_days)}",
                title="Relationship break detected",
                message=(
                    f"{row.series_x} vs {row.series_y} degraded from {row.historical_state} "
                    f"to {row.current_state} in the {int(row.current_window_days)}d window."
                ),
                source_table="relationships",
                series_x=str(row.series_x),
                series_y=str(row.series_y),
                metadata={
                    "historical_state": str(row.historical_state),
                    "current_state": str(row.current_state),
                    "historical_window_days": int(row.historical_window_days),
                    "current_window_days": int(row.current_window_days),
                    "persistence_windows": int(getattr(row, "persistence_windows", 0)),
                    "required_windows": int(getattr(row, "required_windows", 0)),
                    "persistence_label": str(getattr(row, "persistence_label", "")),
                },
            )
        )
    return alerts


def build_cycle_alerts(cycles: pd.DataFrame) -> list[dict[str, Any]]:
    if cycles.empty:
        return []

    alerts: list[dict[str, Any]] = []
    for row in cycles.itertuples(index=False):
        if not row.matched_cycle or str(row.confidence_label) == "likely_spurious":
            continue
        alerts.append(
            _record(
                ts=pd.Timestamp.now(tz="UTC").tz_localize(None),
                alert_type="cycle_match",
                severity=_severity_for_confidence(str(row.confidence_label)),
                rule_key="cycle_match",
                dedupe_key=f"cycle:{row.series_name}:{row.matched_cycle}",
                title="Periodicity detected",
                message=(
                    f"{row.series_name} dominant cycle {float(row.period_days):.1f} days "
                    f"matches {row.matched_cycle}."
                ),
                source_table="signals",
                series_name=str(row.series_name),
                metadata={
                    "period_days": float(row.period_days),
                    "matched_cycle": str(row.matched_cycle),
                    "confidence_label": str(row.confidence_label),
                },
            )
        )
    return alerts


def evaluate_alert_rules(
    *,
    latest_regime: dict[str, Any] | None,
    previous_regime: dict[str, Any] | None,
    latest_signals: dict[str, dict[str, Any]],
    relationships: pd.DataFrame,
    anomalies: pd.DataFrame,
    cycles: pd.DataFrame,
) -> pd.DataFrame:
    """Evaluate the full alert rule catalog and return deduplicated rows."""
    rows: list[dict[str, Any]] = []
    rows.extend(build_regime_change_alert(latest_regime, previous_regime))
    rows.extend(build_threshold_alerts(latest_signals))
    rows.extend(build_correlation_alerts(relationships))
    rows.extend(build_relationship_break_alerts(anomalies))
    rows.extend(build_cycle_alerts(cycles))

    frame = pd.DataFrame(
        rows,
        columns=[
            "ts",
            "alert_type",
            "severity",
            "rule_key",
            "dedupe_key",
            "title",
            "message",
            "source_table",
            "series_name",
            "series_x",
            "series_y",
            "metadata",
        ],
    )
    if frame.empty:
        return frame
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.tz_localize(None)
    frame = frame.sort_values(["severity", "ts", "dedupe_key"], ascending=[True, False, True])
    return frame.drop_duplicates(subset=["dedupe_key"], keep="first").reset_index(drop=True)

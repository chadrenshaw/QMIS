"""Composite market stress analysis for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.anomalies import detect_relationship_anomalies
from qmis.storage import connect_db, get_default_db_path


def _latest_signal_values(signals: pd.DataFrame) -> dict[str, float]:
    if signals.empty:
        return {}
    latest = signals.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")
    return {str(row["series_name"]): float(row["value"]) for _, row in latest.iterrows()}


def _latest_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=features.columns)
    latest = features.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    return latest.drop_duplicates(subset=["series_name"], keep="last")


def _feature_value(feature_frame: pd.DataFrame, series_name: str, column: str) -> float | None:
    if feature_frame.empty:
        return None
    row = feature_frame.loc[feature_frame["series_name"] == series_name]
    if row.empty:
        return None
    value = row.iloc[0].get(column)
    return float(value) if value is not None and pd.notna(value) else None


def _feature_trend(feature_frame: pd.DataFrame, series_name: str) -> str | None:
    if feature_frame.empty:
        return None
    row = feature_frame.loc[feature_frame["series_name"] == series_name]
    if row.empty:
        return None
    trend = row.iloc[0].get("trend_label")
    return str(trend) if trend is not None and pd.notna(trend) else None


def _clamp(score: float) -> float:
    return max(0.0, min(1.0, float(score)))


def build_market_stress_snapshot(
    *,
    signals: pd.DataFrame,
    features: pd.DataFrame,
    relationships: pd.DataFrame,
) -> dict[str, Any]:
    """Build the latest composite market stress snapshot."""
    latest_ts = None
    if not signals.empty:
        latest_ts = pd.to_datetime(signals["ts"]).max()
    elif not features.empty:
        latest_ts = pd.to_datetime(features["ts"]).max()
    elif not relationships.empty:
        latest_ts = pd.to_datetime(relationships["ts"]).max()

    signal_values = _latest_signal_values(signals)
    feature_rows = _latest_feature_rows(features)
    anomalies = detect_relationship_anomalies(relationships.loc[relationships["lag_days"] == 0].copy())

    components: dict[str, float] = {}
    missing_inputs: list[str] = []

    vix = signal_values.get("vix")
    if vix is None:
        missing_inputs.extend(["vix_level", "vix_spike"])
    else:
        components["vix_level"] = _clamp((vix - 15.0) / 20.0)
        vix_zscore = _feature_value(feature_rows, "vix", "zscore_30d")
        vix_change = _feature_value(feature_rows, "vix", "pct_change_30d")
        if vix_zscore is None and vix_change is None:
            missing_inputs.append("vix_spike")
        else:
            spike_input = vix_zscore if vix_zscore is not None else (vix_change or 0.0) / 20.0
            components["vix_spike"] = _clamp(spike_input / 2.5 if vix_zscore is not None else spike_input)

    yield_10y = signal_values.get("yield_10y")
    yield_3m = signal_values.get("yield_3m")
    if yield_10y is None or yield_3m is None:
        missing_inputs.append("yield_curve")
    else:
        yield_curve = float(yield_10y) - float(yield_3m)
        components["yield_curve"] = _clamp(abs(min(yield_curve, 0.0)) / 1.5)

    breadth = signal_values.get("sp500_above_200dma")
    if breadth is None:
        missing_inputs.append("breadth")
    else:
        breadth_score = _clamp((60.0 - float(breadth)) / 25.0)
        if _feature_trend(feature_rows, "sp500_above_200dma") == "DOWN":
            breadth_score = _clamp(breadth_score + 0.15)
        new_lows = signal_values.get("new_lows")
        if new_lows is not None:
            breadth_score = _clamp(breadth_score + min(float(new_lows) / 250.0, 0.25))
        components["breadth"] = breadth_score

    components["anomaly_pressure"] = _clamp(len(anomalies) / 4.0)

    hyg = signal_values.get("HYG")
    if hyg is None:
        missing_inputs.append("credit")
    else:
        credit_score = 0.0
        if _feature_trend(feature_rows, "HYG") == "DOWN":
            credit_score += 0.45
        hyg_change = _feature_value(feature_rows, "HYG", "pct_change_30d")
        if hyg_change is not None and hyg_change <= -5.0:
            credit_score += 0.35
        components["credit"] = _clamp(credit_score)

    available_scores = list(components.values())
    stress_score = round((sum(available_scores) / len(available_scores)) * 100.0, 2) if available_scores else 0.0
    if stress_score >= 75.0:
        stress_level = "CRITICAL"
    elif stress_score >= 50.0:
        stress_level = "HIGH"
    elif stress_score >= 25.0:
        stress_level = "MODERATE"
    else:
        stress_level = "LOW"

    top_components = sorted(components.items(), key=lambda item: item[1], reverse=True)
    lead_names = ", ".join(name.replace("_", " ") for name, score in top_components[:3] if score > 0.0)
    summary = (
        f"Market stress is {stress_level} with {lead_names}."
        if lead_names
        else f"Market stress is {stress_level} with limited supporting inputs."
    )

    return {
        "ts": latest_ts,
        "stress_score": stress_score,
        "stress_level": stress_level,
        "summary": summary,
        "components": components,
        "missing_inputs": missing_inputs,
    }


def materialize_market_stress(db_path: Path | None = None) -> int:
    """Recompute and replace the current market stress snapshot."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        signals = connection.execute(
            """
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM signals
            ORDER BY ts, series_name
            """
        ).fetchdf()
        features = connection.execute(
            """
            SELECT ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                   zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
            FROM features
            ORDER BY ts, series_name
            """
        ).fetchdf()
        relationships = connection.execute(
            """
            SELECT ts, series_x, series_y, window_days, lag_days, correlation, p_value, relationship_state, confidence_label
            FROM relationships
            ORDER BY ts, series_x, series_y, window_days
            """
        ).fetchdf()

        snapshot = build_market_stress_snapshot(signals=signals, features=features, relationships=relationships)
        connection.execute("DELETE FROM stress_snapshots")
        if snapshot["ts"] is None:
            return 0

        payload = pd.DataFrame(
            [
                {
                    "ts": snapshot["ts"],
                    "stress_score": float(snapshot["stress_score"]),
                    "stress_level": str(snapshot["stress_level"]),
                    "summary": str(snapshot["summary"]),
                    "components": json.dumps(snapshot["components"], sort_keys=True),
                    "missing_inputs": json.dumps(snapshot["missing_inputs"]),
                }
            ]
        )
        connection.register("stress_df", payload)
        connection.execute(
            """
            INSERT INTO stress_snapshots (
                ts,
                stress_score,
                stress_level,
                summary,
                components,
                missing_inputs
            )
            SELECT
                ts,
                stress_score,
                stress_level,
                summary,
                components,
                missing_inputs
            FROM stress_df
            """
        )
        connection.unregister("stress_df")
    return 1

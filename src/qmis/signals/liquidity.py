"""Composite liquidity environment analysis for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


TREND_BIAS = {
    "UP": 0.15,
    "DOWN": -0.15,
    "SIDEWAYS": 0.0,
}

LIQUIDITY_COMPONENTS: dict[str, dict[str, float | str]] = {
    "fed_balance_sheet": {"weight": 0.30, "orientation": "pro"},
    "m2_money_supply": {"weight": 0.20, "orientation": "pro"},
    "reverse_repo_usage": {"weight": 0.20, "orientation": "inverse"},
    "dollar_index": {"weight": 0.15, "orientation": "inverse"},
    "real_yields": {"weight": 0.15, "orientation": "inverse"},
}


def _latest_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=features.columns)
    latest = features.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    return latest.drop_duplicates(subset=["series_name"], keep="last")


def _latest_signal_values(signals: pd.DataFrame) -> tuple[pd.Timestamp | None, dict[str, float]]:
    if signals.empty:
        return None, {}
    latest = signals.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")
    timestamp = pd.to_datetime(latest["ts"]).max()
    values = {
        str(row["series_name"]): float(row["value"])
        for _, row in latest.iterrows()
        if row.get("value") is not None and pd.notna(row.get("value"))
    }
    return timestamp, values


def _clamp(score: float, *, minimum: float = -1.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, float(score)))


def _component_score(row: pd.Series, orientation: str) -> float:
    zscore_value = row.get("zscore_30d", 0.0)
    zscore = float(zscore_value) if zscore_value is not None and pd.notna(zscore_value) else 0.0
    trend = str(row.get("trend_label") or "SIDEWAYS")
    base_score = _clamp(zscore / 2.5)
    adjusted = _clamp(base_score + TREND_BIAS.get(trend, 0.0))
    if orientation == "inverse":
        adjusted *= -1.0
    return _clamp(adjusted)


def _state_from_score(liquidity_score: float) -> str:
    if liquidity_score >= 60.0:
        return "EXPANDING"
    if liquidity_score <= 40.0:
        return "TIGHTENING"
    return "NEUTRAL"


def build_liquidity_state(*, signals: pd.DataFrame, features: pd.DataFrame) -> dict[str, Any]:
    """Build the latest composite liquidity environment snapshot.

    Weighting:
    - fed_balance_sheet: 30%
    - m2_money_supply: 20%
    - reverse_repo_usage: 20% (inverse)
    - dollar_index: 15% (inverse)
    - real_yields: 15% (inverse)

    Scoring:
    - Each component starts from the latest 30d z-score, clipped to [-1, 1] after scaling.
    - A small trend bias (+/- 0.15) is added from the latest trend label.
    - Inverse components are sign-flipped so higher values imply tighter liquidity.
    - The weighted average is mapped from [-1, 1] to a 0-100 liquidity score.
    """
    signal_ts, signal_values = _latest_signal_values(signals)
    feature_rows = _latest_feature_rows(features)
    feature_ts = pd.to_datetime(feature_rows["ts"]).max() if not feature_rows.empty else None
    latest_ts = max((ts for ts in (signal_ts, feature_ts) if ts is not None), default=None)

    components: dict[str, dict[str, float | str | None]] = {}
    missing_inputs: list[str] = []
    weighted_sum = 0.0
    available_weight = 0.0

    for series_name, config in LIQUIDITY_COMPONENTS.items():
        component_row = feature_rows.loc[feature_rows["series_name"] == series_name]
        if component_row.empty:
            missing_inputs.append(series_name)
            continue

        row = component_row.iloc[0]
        weight = float(config["weight"])
        orientation = str(config["orientation"])
        score = _component_score(row, orientation)
        components[series_name] = {
            "score": round(score, 4),
            "weight": weight,
            "trend_label": str(row.get("trend_label") or "SIDEWAYS"),
            "zscore_30d": (
                float(row.get("zscore_30d"))
                if row.get("zscore_30d") is not None and pd.notna(row.get("zscore_30d"))
                else 0.0
            ),
            "value": signal_values.get(series_name),
            "orientation": orientation,
        }
        weighted_sum += score * weight
        available_weight += weight

    normalized = (weighted_sum / available_weight) if available_weight > 0 else 0.0
    liquidity_score = round((normalized + 1.0) * 50.0, 2)
    liquidity_state = _state_from_score(liquidity_score)

    leading_components = [
        name.replace("_", " ")
        for name, payload in sorted(
            components.items(),
            key=lambda item: abs(float(item[1]["score"])),
            reverse=True,
        )[:3]
        if abs(float(payload["score"])) > 0.0
    ]
    summary = (
        f"Liquidity is {liquidity_state.lower()} led by {', '.join(leading_components)}."
        if leading_components
        else f"Liquidity is {liquidity_state.lower()} with limited supporting inputs."
    )

    return {
        "ts": latest_ts,
        "liquidity_score": liquidity_score,
        "liquidity_state": liquidity_state,
        "summary": summary,
        "components": components,
        "missing_inputs": missing_inputs,
    }


def materialize_liquidity_state(db_path: Path | None = None) -> int:
    """Recompute and replace the current liquidity environment snapshot."""
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

        snapshot = build_liquidity_state(signals=signals, features=features)
        connection.execute("DELETE FROM liquidity_snapshots")
        if snapshot["ts"] is None:
            return 0

        payload = pd.DataFrame(
            [
                {
                    "ts": snapshot["ts"],
                    "liquidity_score": float(snapshot["liquidity_score"]),
                    "liquidity_state": str(snapshot["liquidity_state"]),
                    "summary": str(snapshot["summary"]),
                    "components": json.dumps(snapshot["components"], sort_keys=True),
                    "missing_inputs": json.dumps(snapshot["missing_inputs"]),
                }
            ]
        )
        connection.register("liquidity_df", payload)
        connection.execute(
            """
            INSERT INTO liquidity_snapshots (
                ts,
                liquidity_score,
                liquidity_state,
                summary,
                components,
                missing_inputs
            )
            SELECT
                ts,
                liquidity_score,
                liquidity_state,
                summary,
                components,
                missing_inputs
            FROM liquidity_df
            """
        )
        connection.unregister("liquidity_df")
    return 1

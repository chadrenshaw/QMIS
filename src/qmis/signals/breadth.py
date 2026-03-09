"""Composite market breadth analysis for QMIS."""

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

BREADTH_COMPONENT_WEIGHTS = {
    "sp500_above_200dma": 0.40,
    "advance_decline_line": 0.30,
    "new_highs_vs_lows": 0.30,
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


def _clamp(value: float, *, minimum: float = -1.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, float(value)))


def _feature_row(feature_rows: pd.DataFrame, series_name: str) -> pd.Series | None:
    row = feature_rows.loc[feature_rows["series_name"] == series_name]
    if row.empty:
        return None
    return row.iloc[0]


def _zscore_value(row: pd.Series | None) -> float:
    if row is None:
        return 0.0
    value = row.get("zscore_30d")
    return float(value) if value is not None and pd.notna(value) else 0.0


def _trend_label(row: pd.Series | None) -> str:
    if row is None:
        return "SIDEWAYS"
    value = row.get("trend_label")
    return str(value) if value is not None and pd.notna(value) else "SIDEWAYS"


def _state_from_score(score: float) -> str:
    if score >= 60.0:
        return "STRONG"
    if score <= 40.0:
        return "FRAGILE"
    return "WEAKENING"


def build_breadth_health(*, signals: pd.DataFrame, features: pd.DataFrame) -> dict[str, Any]:
    """Build the latest breadth-health composite snapshot."""
    signal_ts, signal_values = _latest_signal_values(signals)
    feature_rows = _latest_feature_rows(features)
    feature_ts = pd.to_datetime(feature_rows["ts"]).max() if not feature_rows.empty else None
    latest_ts = max((ts for ts in (signal_ts, feature_ts) if ts is not None), default=None)

    components: dict[str, dict[str, float | str | None]] = {}
    missing_inputs: list[str] = []
    weighted_sum = 0.0
    available_weight = 0.0

    above_row = _feature_row(feature_rows, "sp500_above_200dma")
    above_value = signal_values.get("sp500_above_200dma")
    if above_row is None or above_value is None:
        missing_inputs.append("sp500_above_200dma")
    else:
        above_score = _clamp(((float(above_value) - 50.0) / 25.0) + TREND_BIAS.get(_trend_label(above_row), 0.0))
        components["sp500_above_200dma"] = {
            "score": round(above_score, 4),
            "weight": BREADTH_COMPONENT_WEIGHTS["sp500_above_200dma"],
            "value": above_value,
            "trend_label": _trend_label(above_row),
        }
        weighted_sum += above_score * BREADTH_COMPONENT_WEIGHTS["sp500_above_200dma"]
        available_weight += BREADTH_COMPONENT_WEIGHTS["sp500_above_200dma"]

    ad_row = _feature_row(feature_rows, "advance_decline_line")
    ad_value = signal_values.get("advance_decline_line")
    if ad_row is None or ad_value is None:
        missing_inputs.append("advance_decline_line")
    else:
        ad_score = _clamp((_zscore_value(ad_row) / 2.5) + TREND_BIAS.get(_trend_label(ad_row), 0.0))
        components["advance_decline_line"] = {
            "score": round(ad_score, 4),
            "weight": BREADTH_COMPONENT_WEIGHTS["advance_decline_line"],
            "value": ad_value,
            "trend_label": _trend_label(ad_row),
        }
        weighted_sum += ad_score * BREADTH_COMPONENT_WEIGHTS["advance_decline_line"]
        available_weight += BREADTH_COMPONENT_WEIGHTS["advance_decline_line"]

    highs_row = _feature_row(feature_rows, "new_highs")
    lows_row = _feature_row(feature_rows, "new_lows")
    highs_value = signal_values.get("new_highs")
    lows_value = signal_values.get("new_lows")
    if highs_row is None or lows_row is None or highs_value is None or lows_value is None:
        missing_inputs.append("new_highs_vs_lows")
    else:
        total = float(highs_value) + float(lows_value)
        ratio_score = ((float(highs_value) - float(lows_value)) / total) if total > 0 else 0.0
        trend_adjustment = (
            TREND_BIAS.get(_trend_label(highs_row), 0.0) - TREND_BIAS.get(_trend_label(lows_row), 0.0)
        ) / 2.0
        high_low_score = _clamp(ratio_score + trend_adjustment)
        components["new_highs_vs_lows"] = {
            "score": round(high_low_score, 4),
            "weight": BREADTH_COMPONENT_WEIGHTS["new_highs_vs_lows"],
            "new_highs": highs_value,
            "new_lows": lows_value,
            "highs_trend": _trend_label(highs_row),
            "lows_trend": _trend_label(lows_row),
        }
        weighted_sum += high_low_score * BREADTH_COMPONENT_WEIGHTS["new_highs_vs_lows"]
        available_weight += BREADTH_COMPONENT_WEIGHTS["new_highs_vs_lows"]

    normalized = (weighted_sum / available_weight) if available_weight > 0 else 0.0
    breadth_score = round((normalized + 1.0) * 50.0, 2)
    breadth_state = _state_from_score(breadth_score)
    lead_names = ", ".join(
        name.replace("_", " ")
        for name, payload in sorted(
            components.items(),
            key=lambda item: abs(float(item[1]["score"])),
            reverse=True,
        )[:3]
        if abs(float(payload["score"])) > 0.0
    )
    summary = (
        f"Breadth is {breadth_state.lower()} led by {lead_names}."
        if lead_names
        else f"Breadth is {breadth_state.lower()} with limited supporting inputs."
    )

    return {
        "ts": latest_ts,
        "breadth_score": breadth_score,
        "breadth_state": breadth_state,
        "summary": summary,
        "components": components,
        "missing_inputs": missing_inputs,
    }


def materialize_breadth_health(db_path: Path | None = None) -> int:
    """Recompute and replace the current breadth-health snapshot."""
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

        snapshot = build_breadth_health(signals=signals, features=features)
        connection.execute("DELETE FROM breadth_snapshots")
        if snapshot["ts"] is None:
            return 0

        payload = pd.DataFrame(
            [
                {
                    "ts": snapshot["ts"],
                    "breadth_score": float(snapshot["breadth_score"]),
                    "breadth_state": str(snapshot["breadth_state"]),
                    "summary": str(snapshot["summary"]),
                    "components": json.dumps(snapshot["components"], sort_keys=True),
                    "missing_inputs": json.dumps(snapshot["missing_inputs"]),
                }
            ]
        )
        connection.register("breadth_df", payload)
        connection.execute(
            """
            INSERT INTO breadth_snapshots (
                ts,
                breadth_score,
                breadth_state,
                summary,
                components,
                missing_inputs
            )
            SELECT
                ts,
                breadth_score,
                breadth_state,
                summary,
                components,
                missing_inputs
            FROM breadth_df
            """
        )
        connection.unregister("breadth_df")
    return 1

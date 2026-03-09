"""Composite Macro Pressure Index (MPI) analysis for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.liquidity import build_liquidity_state
from qmis.storage import connect_db, get_default_db_path


MPI_COMPONENT_WEIGHTS = {
    "credit_stress": 0.20,
    "volatility_stress": 0.20,
    "breadth_stress": 0.20,
    "liquidity_stress": 0.20,
    "yield_curve_stress": 0.20,
}

COMPONENT_LABELS = {
    "credit_stress": "credit spread widening",
    "volatility_stress": "volatility regime",
    "breadth_stress": "breadth deterioration",
    "liquidity_stress": "liquidity tightening",
    "yield_curve_stress": "yield curve stress",
}


def _latest_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=features.columns)
    latest = features.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    return latest.drop_duplicates(subset=["series_name"], keep="last")


def _feature_lookup(features: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest = _latest_feature_rows(features)
    return {
        str(row["series_name"]): row.to_dict()
        for _, row in latest.iterrows()
    }


def _signal_history(signals: pd.DataFrame, series_name: str) -> list[float]:
    frame = signals.loc[signals["series_name"] == series_name].copy()
    if frame.empty:
        return []
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values("ts")
    return [float(value) for value in frame["value"].tolist() if value is not None and pd.notna(value)]


def _latest_signal_value(signals: pd.DataFrame, series_name: str) -> float | None:
    history = _signal_history(signals, series_name)
    return history[-1] if history else None


def _previous_signal_value(signals: pd.DataFrame, series_name: str) -> float | None:
    history = _signal_history(signals, series_name)
    return history[-2] if len(history) >= 2 else None


def _feature_value(feature_map: dict[str, dict[str, Any]], series_name: str, column: str) -> float | None:
    row = feature_map.get(series_name)
    if not row:
        return None
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _trend_label(feature_map: dict[str, dict[str, Any]], series_name: str) -> str:
    row = feature_map.get(series_name) or {}
    return str(row.get("trend_label") or "SIDEWAYS").upper()


def _clamp(value: float, *, minimum: float = 0.0, maximum: float = 100.0) -> float:
    return max(minimum, min(maximum, float(value)))


def classify_macro_pressure(score: float) -> str:
    if score >= 85.0:
        return "CRISIS CONDITIONS"
    if score >= 70.0:
        return "SEVERE PRESSURE"
    if score >= 50.0:
        return "HIGH PRESSURE"
    if score >= 25.0:
        return "MODERATE PRESSURE"
    return "LOW PRESSURE"


def compute_credit_stress(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    high_yield = _latest_signal_value(signals, "high_yield_spread")
    baa = _latest_signal_value(signals, "baa_corporate_spread")
    if high_yield is None or baa is None:
        return {
            "score": None,
            "summary": "Credit spread inputs are incomplete.",
            "missing_inputs": ["high_yield_spread", "baa_corporate_spread"],
        }

    prior_high_yield = _previous_signal_value(signals, "high_yield_spread")
    prior_baa = _previous_signal_value(signals, "baa_corporate_spread")
    trend = 0.0
    if prior_high_yield is not None and prior_baa is not None:
        trend = ((high_yield - prior_high_yield) + (baa - prior_baa)) / 2.0
    feature_boost = max(_feature_value(feature_map, "high_yield_spread", "zscore_30d") or 0.0, 0.0) * 5.0
    score = _clamp(12.0 + high_yield * 5.5 + baa * 6.0 + max(trend, 0.0) * 20.0 + feature_boost)
    return {
        "score": round(score, 2),
        "summary": "Rising credit spreads point to increasing systemic risk.",
        "metrics": {
            "credit_spread_level": round((high_yield + baa) / 2.0, 4),
            "credit_spread_trend": round(trend, 4),
            "high_yield_spread": round(high_yield, 4),
            "baa_corporate_spread": round(baa, 4),
        },
        "missing_inputs": [],
    }


def compute_volatility_stress(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    vix = _latest_signal_value(signals, "vix")
    vix3m = _latest_signal_value(signals, "vix3m")
    vix6m = _latest_signal_value(signals, "vix6m")
    if vix is None:
        return {
            "score": None,
            "summary": "Volatility inputs are incomplete.",
            "missing_inputs": ["vix", "vix3m", "vix6m"],
        }

    previous_vix = _previous_signal_value(signals, "vix")
    term_structure = 0.0
    if vix3m is not None and vix6m is not None:
        term_structure = min(vix3m, vix6m) - vix
    shock = (vix - previous_vix) if previous_vix is not None else 0.0
    zscore = _feature_value(feature_map, "vix", "zscore_30d") or 0.0
    score = _clamp((max(vix - 15.0, 0.0) / 20.0) * 35.0 + max(-term_structure, 0.0) * 5.0 + max(shock, 0.0) * 1.0 + max(zscore, 0.0) * 7.0)
    return {
        "score": round(score, 2),
        "summary": "Backwardation and rising implied volatility signal a stressed volatility regime.",
        "metrics": {
            "volatility_level": round(vix, 4),
            "volatility_term_structure": round(term_structure, 4),
            "volatility_shock": round(shock, 4),
        },
        "missing_inputs": [],
    }


def compute_breadth_stress(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    above_200dma = _latest_signal_value(signals, "sp500_above_200dma")
    advance_decline_line = _latest_signal_value(signals, "advance_decline_line")
    new_highs = _latest_signal_value(signals, "new_highs")
    new_lows = _latest_signal_value(signals, "new_lows")
    if above_200dma is None or advance_decline_line is None or new_highs is None or new_lows is None:
        return {
            "score": None,
            "summary": "Breadth inputs are incomplete.",
            "missing_inputs": ["sp500_above_200dma", "advance_decline_line", "new_highs", "new_lows"],
        }

    prior_advance_decline = _previous_signal_value(signals, "advance_decline_line")
    ad_delta = advance_decline_line - prior_advance_decline if prior_advance_decline is not None else 0.0
    breadth_ratio = new_lows / max(new_highs + new_lows, 1.0)
    trend_penalty = 8.0 if _trend_label(feature_map, "sp500_above_200dma") == "DOWN" else 0.0
    score = _clamp(((100.0 - above_200dma) / 100.0) * 55.0 + breadth_ratio * 30.0 + max(-ad_delta, 0.0) / 60.0 + trend_penalty)
    return {
        "score": round(score, 2),
        "summary": "Breadth deterioration is reducing participation and raises correction risk.",
        "metrics": {
            "breadth_health_score": round(100.0 - score, 2),
            "sp500_above_200dma": round(above_200dma, 4),
            "advance_decline_delta": round(ad_delta, 4),
            "new_lows_ratio": round(breadth_ratio, 4),
        },
        "missing_inputs": [],
    }


def compute_liquidity_stress(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    liquidity = build_liquidity_state(signals=signals, features=pd.DataFrame(feature_map.values()))
    liquidity_score = float(liquidity.get("liquidity_score") or 50.0)
    stress_score = _clamp(100.0 - liquidity_score)
    return {
        "score": round(stress_score, 2),
        "summary": "Tighter liquidity conditions increase systemic fragility.",
        "metrics": {
            "liquidity_score": round(liquidity_score, 2),
            "liquidity_state": str(liquidity.get("liquidity_state") or "NEUTRAL"),
        },
        "missing_inputs": list(liquidity.get("missing_inputs") or []),
    }


def compute_yield_curve_stress(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    yield_10y = _latest_signal_value(signals, "yield_10y")
    yield_2y = _latest_signal_value(signals, "yield_2y")
    yield_3m = _latest_signal_value(signals, "yield_3m")
    if yield_10y is None or (yield_2y is None and yield_3m is None):
        return {
            "score": None,
            "summary": "Yield curve inputs are incomplete.",
            "missing_inputs": ["yield_10y", "yield_2y", "yield_3m"],
        }

    spread_10y_3m = (yield_10y - yield_3m) if yield_3m is not None else None
    spread_10y_2y = (yield_10y - yield_2y) if yield_2y is not None else None
    prior_10y = _previous_signal_value(signals, "yield_10y")
    prior_3m = _previous_signal_value(signals, "yield_3m")
    prior_2y = _previous_signal_value(signals, "yield_2y")
    prior_spread_10y_3m = (prior_10y - prior_3m) if prior_10y is not None and prior_3m is not None else None
    prior_spread_10y_2y = (prior_10y - prior_2y) if prior_10y is not None and prior_2y is not None else None

    inversion_depth = max(abs(min(spread_10y_3m or 0.0, 0.0)), abs(min(spread_10y_2y or 0.0, 0.0)))
    curve_change = 0.0
    if spread_10y_3m is not None and prior_spread_10y_3m is not None:
        curve_change = spread_10y_3m - prior_spread_10y_3m
    elif spread_10y_2y is not None and prior_spread_10y_2y is not None:
        curve_change = spread_10y_2y - prior_spread_10y_2y

    score = _clamp(inversion_depth * 70.0 + max(curve_change, 0.0) * 30.0)
    return {
        "score": round(score, 2),
        "summary": "Curve inversion and rapid steepening point to regime-transition risk.",
        "metrics": {
            "yield_curve_slope": round(spread_10y_3m if spread_10y_3m is not None else spread_10y_2y or 0.0, 4),
            "yield_curve_trend": round(curve_change, 4),
            "spread_10y_3m": round(spread_10y_3m, 4) if spread_10y_3m is not None else None,
            "spread_10y_2y": round(spread_10y_2y, 4) if spread_10y_2y is not None else None,
        },
        "missing_inputs": [],
    }


def compute_macro_pressure_index(components: dict[str, dict[str, Any]]) -> float:
    weighted_sum = 0.0
    available_weight = 0.0
    for name, weight in MPI_COMPONENT_WEIGHTS.items():
        payload = components.get(name) or {}
        score = payload.get("score")
        if score is None:
            continue
        weighted_sum += float(score) * weight
        available_weight += weight
    if available_weight == 0.0:
        return 0.0
    return round(weighted_sum / available_weight, 2)


def build_macro_pressure_snapshot(*, signals: pd.DataFrame, features: pd.DataFrame) -> dict[str, Any]:
    feature_map = _feature_lookup(features)
    components = {
        "credit_stress": compute_credit_stress(signals, feature_map),
        "volatility_stress": compute_volatility_stress(signals, feature_map),
        "breadth_stress": compute_breadth_stress(signals, feature_map),
        "liquidity_stress": compute_liquidity_stress(signals, feature_map),
        "yield_curve_stress": compute_yield_curve_stress(signals, feature_map),
    }
    latest_ts = None
    if not signals.empty:
        latest_ts = pd.to_datetime(signals["ts"]).max()
    elif not features.empty:
        latest_ts = pd.to_datetime(features["ts"]).max()
    mpi_score = compute_macro_pressure_index(components)
    pressure_level = classify_macro_pressure(mpi_score)
    primary_contributors = [
        COMPONENT_LABELS[name]
        for name, payload in sorted(
            components.items(),
            key=lambda item: float(item[1].get("score") or 0.0),
            reverse=True,
        )
        if payload.get("score") is not None and float(payload["score"]) > 0.0
    ][:3]
    missing_inputs = sorted(
        {
            missing_input
            for payload in components.values()
            for missing_input in payload.get("missing_inputs", [])
        }
    )
    summary = (
        f"Macro pressure is {pressure_level.lower()} led by {', '.join(primary_contributors)}."
        if primary_contributors
        else f"Macro pressure is {pressure_level.lower()} with limited supporting inputs."
    )
    return {
        "ts": latest_ts,
        "mpi_score": mpi_score,
        "pressure_level": pressure_level,
        "summary": summary,
        "components": components,
        "primary_contributors": primary_contributors,
        "missing_inputs": missing_inputs,
    }


def materialize_macro_pressure(db_path: Path | None = None) -> int:
    """Recompute and replace the current Macro Pressure Index snapshot."""
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

        snapshot = build_macro_pressure_snapshot(signals=signals, features=features)
        connection.execute("DELETE FROM macro_pressure_snapshots")
        if snapshot["ts"] is None:
            return 0

        payload = pd.DataFrame(
            [
                {
                    "ts": snapshot["ts"],
                    "mpi_score": float(snapshot["mpi_score"]),
                    "pressure_level": str(snapshot["pressure_level"]),
                    "summary": str(snapshot["summary"]),
                    "components": json.dumps(snapshot["components"], sort_keys=True),
                    "primary_contributors": json.dumps(snapshot["primary_contributors"]),
                    "missing_inputs": json.dumps(snapshot["missing_inputs"]),
                }
            ]
        )
        connection.register("macro_pressure_df", payload)
        connection.execute(
            """
            INSERT INTO macro_pressure_snapshots (
                ts,
                mpi_score,
                pressure_level,
                summary,
                components,
                primary_contributors,
                missing_inputs
            )
            SELECT
                ts,
                mpi_score,
                pressure_level,
                summary,
                components,
                primary_contributors,
                missing_inputs
            FROM macro_pressure_df
            """
        )
        connection.unregister("macro_pressure_df")
    return 1

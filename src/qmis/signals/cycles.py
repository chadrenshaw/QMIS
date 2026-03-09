"""Cycle detection and monitoring for QMIS."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


KNOWN_CYCLES = {
    "lunar_period": 29.53,
    "solar_cycle": 11.0 * 365.25,
}

CYCLE_COLUMNS = [
    "ts",
    "cycle_name",
    "phase",
    "strength",
    "is_turning_point",
    "transition_from",
    "alert_on_transition",
    "summary",
    "supporting_signals",
    "metadata",
]

CYCLE_ORDER = {
    "solar_cycle": 0,
    "lunar_cycle": 1,
    "macro_liquidity_cycle": 2,
}


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, float(value)))


def _latest_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=features.columns)
    latest = features.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    return latest.drop_duplicates(subset=["series_name"], keep="last")


def _feature_row(feature_rows: pd.DataFrame, series_name: str) -> pd.Series | None:
    if feature_rows.empty:
        return None
    row = feature_rows.loc[feature_rows["series_name"] == series_name]
    if row.empty:
        return None
    return row.iloc[0]


def _feature_trend(feature_rows: pd.DataFrame, series_name: str) -> str | None:
    row = _feature_row(feature_rows, series_name)
    if row is None:
        return None
    trend = row.get("trend_label")
    return str(trend) if trend is not None and pd.notna(trend) else None


def _phase_title(phase: str | None) -> str:
    if not phase:
        return "Unknown"
    return str(phase).replace("_", " ").title()


def _latest_signal_value(signals: pd.DataFrame, series_name: str) -> tuple[pd.Timestamp | None, float | None]:
    if signals.empty:
        return None, None
    rows = signals.loc[signals["series_name"] == series_name].copy()
    if rows.empty:
        return None, None
    rows["ts"] = pd.to_datetime(rows["ts"])
    rows = rows.sort_values("ts")
    value = rows.iloc[-1].get("value")
    return pd.to_datetime(rows.iloc[-1]["ts"]), (float(value) if value is not None and pd.notna(value) else None)


def _latest_previous_phases(previous_cycles: pd.DataFrame | None) -> dict[str, str]:
    if previous_cycles is None or previous_cycles.empty:
        return {}
    frame = previous_cycles.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["cycle_name", "ts"])
    latest = frame.drop_duplicates(subset=["cycle_name"], keep="last")
    return {
        str(row["cycle_name"]): str(row["phase"])
        for _, row in latest.iterrows()
        if row.get("phase") is not None and pd.notna(row.get("phase"))
    }


def _lunar_phase_name(lunar_cycle_day: float | None) -> str:
    if lunar_cycle_day is None:
        return "unknown"
    if lunar_cycle_day < 1.85:
        return "new_moon"
    if lunar_cycle_day < 5.54:
        return "waxing_crescent"
    if lunar_cycle_day < 9.23:
        return "first_quarter"
    if lunar_cycle_day < 12.92:
        return "waxing_gibbous"
    if lunar_cycle_day < 16.61:
        return "full_moon"
    if lunar_cycle_day < 20.30:
        return "waning_gibbous"
    if lunar_cycle_day < 23.99:
        return "last_quarter"
    if lunar_cycle_day < 27.68:
        return "waning_crescent"
    return "new_moon"


def _solar_cycle_snapshot(
    signals: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    previous_phase: str | None,
) -> dict[str, Any] | None:
    rows = signals.loc[signals["series_name"] == "sunspot_number", ["ts", "value"]].copy()
    if rows.empty:
        return None
    rows["ts"] = pd.to_datetime(rows["ts"])
    rows["value"] = pd.to_numeric(rows["value"], errors="coerce")
    rows = rows.dropna().sort_values("ts")
    if rows.empty:
        return None

    moving_average = rows["value"].rolling(90, min_periods=30).mean().dropna()
    if moving_average.empty:
        return None

    latest_ma = float(moving_average.iloc[-1])
    latest_ts = pd.to_datetime(rows["ts"]).max()
    medium_lookback = min(len(moving_average) - 1, 30)
    short_lookback = min(len(moving_average) - 1, 14)
    medium_slope = (latest_ma - float(moving_average.iloc[-1 - medium_lookback])) / max(1, medium_lookback)
    short_slope = (latest_ma - float(moving_average.iloc[-1 - short_lookback])) / max(1, short_lookback)

    yearly_cutoff = latest_ts - pd.Timedelta(days=365)
    cycle_cutoff = latest_ts - pd.Timedelta(days=int(11 * 365.25))
    yearly_rows = rows.loc[rows["ts"] >= yearly_cutoff, "value"]
    cycle_rows = rows.loc[rows["ts"] >= cycle_cutoff, "value"]
    if cycle_rows.empty:
        cycle_rows = rows["value"]

    cycle_min = float(cycle_rows.min())
    cycle_max = float(cycle_rows.max())
    cycle_range = max(cycle_max - cycle_min, 1.0)
    position = _clamp((latest_ma - cycle_min) / cycle_range)
    turning_threshold = max(cycle_range * 0.0015, 0.05)
    movement_threshold = max(cycle_range * 0.001, 0.03)

    if position >= 0.9 or (position >= 0.82 and (short_slope <= turning_threshold or short_slope <= max(medium_slope * 0.25, turning_threshold))):
        phase = "peak"
        is_turning_point = True
    elif position <= 0.18 and short_slope >= -turning_threshold:
        phase = "minimum"
        is_turning_point = True
    elif medium_slope > movement_threshold:
        phase = "rising"
        is_turning_point = False
    else:
        phase = "declining"
        is_turning_point = False

    transition_from = previous_phase if previous_phase and previous_phase != phase else None
    strength = round(_clamp(max(abs(medium_slope), abs(short_slope)) / (movement_threshold * 3.0), 0.35, 1.0), 2)
    window_365d_mean = float(yearly_rows.mean()) if not yearly_rows.empty else latest_ma
    summary = (
        f"Solar activity is at a {_phase_title(phase).lower()} phase using a 90d average of {latest_ma:.1f}, "
        f"with the 365d mean at {window_365d_mean:.1f} and position {position:.0%} through the current 11y band."
    )
    return {
        "ts": as_of,
        "cycle_name": "solar_cycle",
        "phase": phase,
        "strength": strength,
        "is_turning_point": is_turning_point,
        "transition_from": transition_from,
        "alert_on_transition": True,
        "summary": summary,
        "supporting_signals": ["sunspot_number"],
        "metadata": {
            "window_90d_mean": round(latest_ma, 2),
            "window_365d_mean": round(window_365d_mean, 2),
            "window_11y_range_pct": round(position, 4),
            "slope_14d": round(short_slope, 4),
            "slope_30d": round(medium_slope, 4),
        },
    }


def _lunar_cycle_snapshot(
    signals: pd.DataFrame,
    *,
    as_of: pd.Timestamp,
    previous_phase: str | None,
) -> dict[str, Any] | None:
    _, lunar_cycle_day = _latest_signal_value(signals, "lunar_cycle_day")
    if lunar_cycle_day is None:
        return None
    phase = _lunar_phase_name(lunar_cycle_day)
    boundary_points = [1.85, 5.54, 9.23, 12.92, 16.61, 20.30, 23.99, 27.68]
    distance_to_turn = min(abs(lunar_cycle_day - point) for point in boundary_points)
    return {
        "ts": as_of,
        "cycle_name": "lunar_cycle",
        "phase": phase,
        "strength": round(_clamp(1.0 - (distance_to_turn / 3.5), 0.35, 0.95), 2),
        "is_turning_point": distance_to_turn <= 0.75,
        "transition_from": previous_phase if previous_phase and previous_phase != phase else None,
        "alert_on_transition": False,
        "summary": f"The moon is in a {_phase_title(phase).lower()} phase at day {lunar_cycle_day:.1f}.",
        "supporting_signals": ["lunar_cycle_day"],
        "metadata": {"lunar_cycle_day": round(float(lunar_cycle_day), 2)},
    }


def _macro_liquidity_cycle_snapshot(
    features: pd.DataFrame,
    liquidity_environment: dict[str, Any] | None,
    *,
    as_of: pd.Timestamp,
    previous_phase: str | None,
) -> dict[str, Any] | None:
    if not liquidity_environment:
        return None

    feature_rows = _latest_feature_rows(features)
    state = str(liquidity_environment.get("liquidity_state", "")).upper()
    score = float(liquidity_environment.get("liquidity_score", 50.0))
    fed_trend = _feature_trend(feature_rows, "fed_balance_sheet")
    reverse_repo_trend = _feature_trend(feature_rows, "reverse_repo_usage")
    real_yield_trend = _feature_trend(feature_rows, "real_yields")
    dollar_trend = _feature_trend(feature_rows, "dollar_index")

    if state == "EXPANDING" and score >= 60.0:
        phase = "expanding"
    elif state == "TIGHTENING" and score <= 40.0:
        phase = "contracting"
    elif state == "TIGHTENING":
        phase = "peak"
    else:
        phase = "neutral"

    transition_from = previous_phase if previous_phase and previous_phase != phase else None
    is_turning_point = phase in {"peak", "neutral"} and transition_from is not None
    strength = round(_clamp(abs(score - 50.0) / 50.0, 0.35, 1.0), 2)
    supporting_signals = ["fed_balance_sheet", "reverse_repo_usage", "real_yields", "dollar_index"]
    summary = (
        f"Macro liquidity is in an {_phase_title(phase).lower()} phase with {state.lower()} composite conditions. "
        f"Fed trend={fed_trend or 'N/A'}, reverse repo trend={reverse_repo_trend or 'N/A'}, "
        f"real yields trend={real_yield_trend or 'N/A'}, dollar trend={dollar_trend or 'N/A'}."
    )
    return {
        "ts": as_of,
        "cycle_name": "macro_liquidity_cycle",
        "phase": phase,
        "strength": strength,
        "is_turning_point": is_turning_point,
        "transition_from": transition_from,
        "alert_on_transition": True,
        "summary": summary,
        "supporting_signals": supporting_signals,
        "metadata": {
            "liquidity_state": state,
            "liquidity_score": score,
            "fed_balance_sheet_trend": fed_trend,
            "reverse_repo_usage_trend": reverse_repo_trend,
            "real_yields_trend": real_yield_trend,
            "dollar_index_trend": dollar_trend,
        },
    }


def _median_step_days(ts: pd.Series) -> float:
    diffs = ts.sort_values().diff().dropna()
    if diffs.empty:
        return 1.0
    return float(diffs.dt.total_seconds().median() / 86400.0) or 1.0


def _classify_cycle_confidence(relative_power: float, matched_cycle: str | None) -> str:
    if matched_cycle and relative_power >= 0.45:
        return "validated"
    if relative_power >= 0.2:
        return "statistically_significant"
    if relative_power >= 0.1:
        return "tentative"
    return "likely_spurious"


def detect_dominant_cycles(signal_frame: pd.DataFrame, top_n: int = 3) -> pd.DataFrame:
    """Detect the dominant FFT periods for each series in a signal frame."""
    columns = [
        "series_name",
        "period_days",
        "frequency",
        "power",
        "relative_power",
        "matched_cycle",
        "confidence_label",
    ]
    if signal_frame.empty:
        return pd.DataFrame(columns=columns)

    frame = signal_frame.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    rows: list[dict[str, object]] = []

    for series_name, series in frame.groupby("series_name", sort=True):
        series = series.sort_values("ts").reset_index(drop=True)
        values = series["value"].to_numpy(dtype=float)
        if len(values) < 32:
            continue

        centered = values - np.mean(values)
        sample_spacing = _median_step_days(series["ts"])
        spectrum = np.fft.rfft(centered)
        frequencies = np.fft.rfftfreq(len(centered), d=sample_spacing)
        powers = np.abs(spectrum) ** 2

        positive = frequencies > 0
        frequencies = frequencies[positive]
        powers = powers[positive]
        if len(frequencies) == 0:
            continue

        total_power = float(np.sum(powers))
        top_indices = np.argsort(powers)[::-1][:top_n]
        for index in top_indices:
            frequency = float(frequencies[index])
            if frequency <= 0:
                continue
            period_days = 1.0 / frequency
            power = float(powers[index])
            relative_power = power / total_power if total_power > 0 else 0.0

            matched_cycle = None
            for cycle_name, cycle_period in KNOWN_CYCLES.items():
                tolerance = max(1.5, cycle_period * 0.05)
                if math.isclose(period_days, cycle_period, abs_tol=tolerance):
                    matched_cycle = cycle_name
                    break

            rows.append(
                {
                    "series_name": str(series_name),
                    "period_days": float(period_days),
                    "frequency": frequency,
                    "power": power,
                    "relative_power": relative_power,
                    "matched_cycle": matched_cycle,
                    "confidence_label": _classify_cycle_confidence(relative_power, matched_cycle),
                }
            )

    result = pd.DataFrame(rows, columns=columns)
    if result.empty:
        return result
    return result.sort_values(["series_name", "power"], ascending=[True, False]).reset_index(drop=True)


def build_cycle_snapshots(
    *,
    signals: pd.DataFrame,
    features: pd.DataFrame,
    liquidity_environment: dict[str, Any] | None,
    previous_cycles: pd.DataFrame | None = None,
    as_of: Any | None = None,
) -> pd.DataFrame:
    """Build the current environmental cycle snapshot rows."""
    candidate_timestamps = []
    if as_of is not None:
        candidate_timestamps.append(pd.to_datetime(as_of))
    if not signals.empty:
        candidate_timestamps.append(pd.to_datetime(signals["ts"]).max())
    if not features.empty:
        candidate_timestamps.append(pd.to_datetime(features["ts"]).max())
    if liquidity_environment and liquidity_environment.get("ts") is not None:
        candidate_timestamps.append(pd.to_datetime(liquidity_environment["ts"]))
    if not candidate_timestamps:
        return pd.DataFrame(columns=CYCLE_COLUMNS)

    snapshot_ts = max(candidate_timestamps)
    previous_phases = _latest_previous_phases(previous_cycles)
    rows = [
        _solar_cycle_snapshot(signals, as_of=snapshot_ts, previous_phase=previous_phases.get("solar_cycle")),
        _lunar_cycle_snapshot(signals, as_of=snapshot_ts, previous_phase=previous_phases.get("lunar_cycle")),
        _macro_liquidity_cycle_snapshot(
            features,
            liquidity_environment,
            as_of=snapshot_ts,
            previous_phase=previous_phases.get("macro_liquidity_cycle"),
        ),
    ]
    rows = [row for row in rows if row is not None]
    if not rows:
        return pd.DataFrame(columns=CYCLE_COLUMNS)
    result = pd.DataFrame(rows, columns=CYCLE_COLUMNS)
    result["_order"] = result["cycle_name"].map(CYCLE_ORDER).fillna(99)
    result = result.sort_values(["_order", "cycle_name"]).drop(columns="_order").reset_index(drop=True)
    return result


def materialize_cycle_snapshots(db_path: Path | None = None) -> int:
    """Recompute and replace the current environmental cycle snapshot."""
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
        previous_cycles = connection.execute(
            """
            SELECT ts, cycle_name, phase, strength, is_turning_point, transition_from,
                   alert_on_transition, summary, supporting_signals, metadata
            FROM cycle_snapshots
            ORDER BY ts, cycle_name
            """
        ).fetchdf()
        liquidity_rows = connection.execute(
            """
            SELECT ts, liquidity_score, liquidity_state, summary, components, missing_inputs
            FROM liquidity_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()

        liquidity_environment = None
        if not liquidity_rows.empty:
            row = liquidity_rows.iloc[0]
            liquidity_environment = {
                "ts": row["ts"],
                "liquidity_score": float(row["liquidity_score"]),
                "liquidity_state": str(row["liquidity_state"]),
                "summary": str(row["summary"]),
                "components": json.loads(row["components"] or "{}"),
                "missing_inputs": json.loads(row["missing_inputs"] or "[]"),
            }

        cycles = build_cycle_snapshots(
            signals=signals,
            features=features,
            liquidity_environment=liquidity_environment,
            previous_cycles=previous_cycles,
        )
        connection.execute("DELETE FROM cycle_snapshots")
        if cycles.empty:
            return 0

        payload = cycles.copy()
        payload["supporting_signals"] = payload["supporting_signals"].apply(lambda value: json.dumps(value or []))
        payload["metadata"] = payload["metadata"].apply(lambda value: json.dumps(value or {}, sort_keys=True))
        connection.register("cycle_snapshots_df", payload)
        connection.execute(
            """
            INSERT INTO cycle_snapshots (
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
            )
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
            FROM cycle_snapshots_df
            """
        )
        connection.unregister("cycle_snapshots_df")
    return int(len(cycles))

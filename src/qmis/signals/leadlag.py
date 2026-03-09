"""Lead-lag discovery for the QMIS signal engine."""

from __future__ import annotations

from itertools import combinations
from pathlib import Path

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.correlations import (
    CORRELATION_ALERT_THRESHOLD,
    DEFAULT_WINDOWS,
    MIN_OBSERVATIONS,
    _safe_pearsonr,
    _series_context,
)
from qmis.storage import connect_db, get_default_db_path


def _align_with_lag(series_x: pd.Series, series_y: pd.Series, lag_days: int) -> tuple[pd.Series, pd.Series]:
    if lag_days > 0:
        return series_x.iloc[:-lag_days], series_y.iloc[lag_days:]
    if lag_days < 0:
        return series_x.iloc[-lag_days:], series_y.iloc[:lag_days]
    return series_x, series_y


def _classify_lead_lag_row(exploratory_pair: bool, correlation: float, p_value: float) -> tuple[str, str]:
    magnitude = abs(correlation)
    if exploratory_pair:
        return "exploratory", "exploratory"
    if magnitude >= 0.8 and p_value < 0.01:
        return "stable", "validated"
    if magnitude >= CORRELATION_ALERT_THRESHOLD and p_value < 0.05:
        return "emerging", "statistically_significant"
    if magnitude >= 0.4 and p_value < 0.1:
        return "weakening", "tentative"
    return "broken", "likely_spurious"


def build_lead_lag_frame(
    signals: pd.DataFrame,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
    max_lag: int = 365,
) -> pd.DataFrame:
    """Find the strongest lagged relationship for each pair and window."""
    columns = [
        "ts",
        "series_x",
        "series_y",
        "window_days",
        "lag_days",
        "correlation",
        "p_value",
        "relationship_state",
        "confidence_label",
    ]
    if signals.empty:
        return pd.DataFrame(columns=columns)

    frame = signals.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["ts", "series_name"])
    context = _series_context(frame)
    pivot = (
        frame.pivot_table(index="ts", columns="series_name", values="value", aggfunc="last")
        .sort_index()
        .dropna(axis=1, how="all")
    )
    results: list[dict[str, object]] = []

    for raw_series_x, raw_series_y in combinations(sorted(pivot.columns.tolist()), 2):
        aligned = pivot[[raw_series_x, raw_series_y]].dropna()
        if len(aligned) < MIN_OBSERVATIONS:
            continue

        exploratory_pair = bool(context.get(raw_series_x, {}).get("exploratory")) or bool(
            context.get(raw_series_y, {}).get("exploratory")
        )
        for window_days in windows:
            if len(aligned) < window_days:
                continue
            window_frame = aligned.tail(window_days)
            if len(window_frame) < MIN_OBSERVATIONS:
                continue

            best: tuple[int, float, float] | None = None
            left = window_frame[raw_series_x].reset_index(drop=True)
            right = window_frame[raw_series_y].reset_index(drop=True)
            for lag_days in range(-max_lag, max_lag + 1):
                lagged_left, lagged_right = _align_with_lag(left, right, lag_days)
                if len(lagged_left) < MIN_OBSERVATIONS or len(lagged_right) < MIN_OBSERVATIONS:
                    continue
                correlation, p_value = _safe_pearsonr(lagged_left, lagged_right)
                score = abs(correlation)
                if best is None or score > abs(best[1]):
                    best = (lag_days, correlation, p_value)

            if best is None or best[0] == 0:
                continue

            best_lag, best_correlation, best_p_value = best
            if best_lag > 0:
                series_x = raw_series_x
                series_y = raw_series_y
                lag_days = best_lag
            else:
                series_x = raw_series_y
                series_y = raw_series_x
                lag_days = abs(best_lag)

            relationship_state, confidence_label = _classify_lead_lag_row(
                exploratory_pair=exploratory_pair,
                correlation=best_correlation,
                p_value=best_p_value,
            )
            results.append(
                {
                    "ts": pd.to_datetime(window_frame.index.max()),
                    "series_x": series_x,
                    "series_y": series_y,
                    "window_days": int(window_days),
                    "lag_days": int(lag_days),
                    "correlation": float(best_correlation),
                    "p_value": float(best_p_value),
                    "relationship_state": relationship_state,
                    "confidence_label": confidence_label,
                }
            )

    if not results:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(results, columns=columns).sort_values(
        ["window_days", "series_x", "series_y"]
    ).reset_index(drop=True)


def materialize_lead_lag_relationships(
    db_path: Path | None = None,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
    max_lag: int = 365,
) -> int:
    """Replace the persisted nonzero-lag relationship rows."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        signals = connection.execute(
            """
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM signals
            ORDER BY ts, series_name
            """
        ).fetchdf()

        lead_lag_frame = build_lead_lag_frame(signals, windows=windows, max_lag=max_lag)
        connection.execute("DELETE FROM relationships WHERE lag_days <> 0")
        if lead_lag_frame.empty:
            return 0

        connection.register("lead_lag_df", lead_lag_frame)
        connection.execute(
            """
            INSERT INTO relationships (
                ts,
                series_x,
                series_y,
                window_days,
                lag_days,
                correlation,
                p_value,
                relationship_state,
                confidence_label
            )
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
            FROM lead_lag_df
            """
        )
        connection.unregister("lead_lag_df")
    return int(len(lead_lag_frame))

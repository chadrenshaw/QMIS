"""Cross-domain correlation analysis for the QMIS signal engine."""

from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


DEFAULT_WINDOWS = (30, 90, 365, 1825)
CORRELATION_ALERT_THRESHOLD = 0.6
SIGNIFICANCE_LEVEL = 0.05
MIN_OBSERVATIONS = 30


def _parse_metadata(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return {}
    return {}


def _series_context(signals: pd.DataFrame) -> dict[str, dict[str, object]]:
    latest = signals.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")

    context: dict[str, dict[str, object]] = {}
    for _, row in latest.iterrows():
        metadata = _parse_metadata(row.get("metadata"))
        category = str(row.get("category", ""))
        context[str(row["series_name"])] = {
            "category": category,
            "metadata": metadata,
            "exploratory": bool(metadata.get("exploratory")) or category == "natural",
        }
    return context


def _safe_pearsonr(left: pd.Series, right: pd.Series) -> tuple[float, float]:
    if left.nunique(dropna=True) <= 1 or right.nunique(dropna=True) <= 1:
        return 0.0, 1.0
    correlation, p_value = pearsonr(left.to_numpy(dtype=float), right.to_numpy(dtype=float))
    if np.isnan(correlation) or np.isnan(p_value):
        return 0.0, 1.0
    return float(correlation), float(p_value)


def _classify_pair_rows(pair_frame: pd.DataFrame) -> pd.DataFrame:
    pair_frame = pair_frame.sort_values("window_days").copy()
    exploratory_pair = bool(pair_frame["exploratory_pair"].iloc[0])
    corrected_significant = pair_frame["p_value"] < pair_frame["corrected_alpha"]
    strong = corrected_significant & (pair_frame["correlation"].abs() >= CORRELATION_ALERT_THRESHOLD)
    significant_pair = pair_frame.loc[strong]
    persistence_count = len(significant_pair)
    longest_row = pair_frame.iloc[-1]
    shortest_row = pair_frame.iloc[0]
    long_significant = bool(strong.iloc[-1])
    short_significant = bool(strong.iloc[0])
    long_sign = float(np.sign(longest_row["correlation"])) if longest_row["correlation"] != 0 else 0.0
    short_sign = float(np.sign(shortest_row["correlation"])) if shortest_row["correlation"] != 0 else 0.0

    states: list[str] = []
    confidence_labels: list[str] = []
    for row in pair_frame.itertuples(index=False):
        magnitude = abs(float(row.correlation))
        corrected_is_significant = float(row.p_value) < float(row.corrected_alpha)

        if exploratory_pair:
            state = "exploratory"
            confidence = "exploratory"
        elif corrected_is_significant and magnitude >= CORRELATION_ALERT_THRESHOLD and persistence_count >= 2:
            state = "stable"
            confidence = "validated"
        elif corrected_is_significant and magnitude >= CORRELATION_ALERT_THRESHOLD:
            if row.window_days == shortest_row["window_days"] and (not long_significant or short_sign != long_sign):
                state = "emerging"
            else:
                state = "stable"
            confidence = "statistically_significant"
        elif (
            row.window_days == shortest_row["window_days"]
            and long_significant
            and (not short_significant or magnitude < CORRELATION_ALERT_THRESHOLD or short_sign != long_sign)
        ):
            state = "broken" if magnitude < 0.2 or not corrected_is_significant else "weakening"
            confidence = "likely_spurious" if not corrected_is_significant else "tentative"
        elif float(row.p_value) < SIGNIFICANCE_LEVEL and magnitude >= 0.4:
            state = "weakening" if long_significant else "emerging"
            confidence = "tentative"
        else:
            state = "weakening" if long_significant else "broken"
            confidence = "likely_spurious"

        states.append(state)
        confidence_labels.append(confidence)

    pair_frame["relationship_state"] = states
    pair_frame["confidence_label"] = confidence_labels
    return pair_frame


def build_relationship_frame(
    signals: pd.DataFrame,
    windows: tuple[int, ...] = DEFAULT_WINDOWS,
) -> pd.DataFrame:
    """Compute the latest cross-domain relationship snapshot from raw signals."""
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
    series_names = sorted(pivot.columns.tolist())
    relationship_rows: list[dict[str, object]] = []

    for series_x, series_y in combinations(series_names, 2):
        aligned = pivot[[series_x, series_y]].dropna()
        if len(aligned) < MIN_OBSERVATIONS:
            continue

        exploratory_pair = bool(context.get(series_x, {}).get("exploratory")) or bool(
            context.get(series_y, {}).get("exploratory")
        )
        for window_days in windows:
            if len(aligned) < window_days:
                continue
            window_frame = aligned.tail(window_days)
            if len(window_frame) < MIN_OBSERVATIONS:
                continue

            correlation, p_value = _safe_pearsonr(window_frame[series_x], window_frame[series_y])
            relationship_rows.append(
                {
                    "ts": pd.to_datetime(window_frame.index.max()),
                    "series_x": series_x,
                    "series_y": series_y,
                    "window_days": int(window_days),
                    "lag_days": 0,
                    "correlation": correlation,
                    "p_value": p_value,
                    "exploratory_pair": exploratory_pair,
                }
            )

    if not relationship_rows:
        return pd.DataFrame(columns=columns)

    relationship_frame = pd.DataFrame(relationship_rows)
    corrected_alpha = SIGNIFICANCE_LEVEL / max(len(relationship_frame), 1)
    relationship_frame["corrected_alpha"] = corrected_alpha

    classified_frames: list[pd.DataFrame] = []
    for _, pair_frame in relationship_frame.groupby(["series_x", "series_y"], sort=True):
        classified_frames.append(_classify_pair_rows(pair_frame))

    combined = pd.concat(classified_frames, ignore_index=True)
    return combined[columns].sort_values(["window_days", "series_x", "series_y"]).reset_index(drop=True)


def materialize_relationships(db_path: Path | None = None) -> int:
    """Recompute and replace the current `relationships` snapshot from raw signals."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        signals = connection.execute(
            """
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM signals
            ORDER BY ts, series_name
            """
        ).fetchdf()

        relationship_frame = build_relationship_frame(signals)
        connection.execute("DELETE FROM relationships")
        if relationship_frame.empty:
            return 0

        connection.register("relationships_df", relationship_frame)
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
            FROM relationships_df
            """
        )
        connection.unregister("relationships_df")
    return int(len(relationship_frame))

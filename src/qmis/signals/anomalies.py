"""Derived anomaly detection over persisted relationship states."""

from __future__ import annotations

import pandas as pd


def detect_relationship_anomalies(relationships: pd.DataFrame) -> pd.DataFrame:
    """Detect broken or weakening short-term relationships against strong historical baselines."""
    columns = [
        "ts",
        "series_x",
        "series_y",
        "anomaly_type",
        "historical_state",
        "current_state",
        "historical_window_days",
        "current_window_days",
        "historical_correlation",
        "current_correlation",
    ]
    if relationships.empty:
        return pd.DataFrame(columns=columns)

    frame = relationships.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["series_x", "series_y", "lag_days", "window_days"])

    rows: list[dict[str, object]] = []
    for _, pair_frame in frame.groupby(["series_x", "series_y", "lag_days"], sort=True):
        historical = pair_frame.iloc[-1]
        current = pair_frame.iloc[0]
        historical_state = str(historical["relationship_state"])
        current_state = str(current["relationship_state"])
        if historical_state not in {"stable", "emerging"}:
            continue
        if current_state not in {"broken", "weakening"}:
            continue

        rows.append(
            {
                "ts": pd.to_datetime(current["ts"]),
                "series_x": str(current["series_x"]),
                "series_y": str(current["series_y"]),
                "anomaly_type": "relationship_break" if current_state == "broken" else "relationship_weakening",
                "historical_state": historical_state,
                "current_state": current_state,
                "historical_window_days": int(historical["window_days"]),
                "current_window_days": int(current["window_days"]),
                "historical_correlation": float(historical["correlation"]),
                "current_correlation": float(current["correlation"]),
            }
        )

    return pd.DataFrame(rows, columns=columns)

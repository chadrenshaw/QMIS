"""Feature materialization for the QMIS feature engine."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from qmis.features.momentum import compute_moving_average
from qmis.features.trends import classify_trend, compute_percent_change_windows, compute_rolling_slope
from qmis.features.volatility import compute_drawdown, compute_rolling_volatility, compute_rolling_zscore
from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


def build_feature_frame(signals: pd.DataFrame) -> pd.DataFrame:
    """Compute the spec-defined feature rows from raw signal rows."""
    if signals.empty:
        return pd.DataFrame(
            columns=[
                "ts",
                "series_name",
                "pct_change_30d",
                "pct_change_90d",
                "pct_change_365d",
                "zscore_30d",
                "volatility_30d",
                "slope_30d",
                "drawdown_90d",
                "trend_label",
            ]
        )

    signals = signals.copy()
    signals["ts"] = pd.to_datetime(signals["ts"])
    feature_frames: list[pd.DataFrame] = []

    for series_name, series_frame in signals.groupby("series_name", sort=True):
        series_frame = series_frame.sort_values("ts").reset_index(drop=True)
        value_series = pd.Series(series_frame["value"].to_numpy(), index=series_frame["ts"])

        percent_changes = compute_percent_change_windows(value_series, windows=(30, 90, 365))
        moving_average_30d = compute_moving_average(value_series, window=30)
        zscore_30d = compute_rolling_zscore(value_series, window=30)
        volatility_30d = compute_rolling_volatility(value_series, window=30)
        slope_30d = compute_rolling_slope(value_series, window=30)
        drawdown_90d = compute_drawdown(value_series, window=90)

        frame = pd.DataFrame(
            {
                "ts": series_frame["ts"].to_numpy(),
                "series_name": series_name,
                "pct_change_30d": percent_changes["pct_change_30d"].to_numpy(),
                "pct_change_90d": percent_changes["pct_change_90d"].to_numpy(),
                "pct_change_365d": percent_changes["pct_change_365d"].to_numpy(),
                "zscore_30d": zscore_30d.to_numpy(),
                "volatility_30d": volatility_30d.to_numpy(),
                "slope_30d": slope_30d.to_numpy(),
                "drawdown_90d": drawdown_90d.to_numpy(),
                # The single stored label uses the most recent 30d change as the canonical trend window.
                "trend_label": percent_changes["pct_change_30d"].map(classify_trend).to_numpy(),
                "_moving_average_30d": moving_average_30d.to_numpy(),
            }
        )
        feature_frames.append(frame)

    combined = pd.concat(feature_frames, ignore_index=True)
    return combined.drop(columns=["_moving_average_30d"]).sort_values(["ts", "series_name"]).reset_index(drop=True)


def materialize_features(db_path: Path | None = None) -> int:
    """Recompute and replace the entire `features` table from raw `signals`."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        signals = connection.execute(
            """
            SELECT ts, series_name, value
            FROM signals
            ORDER BY ts, series_name
            """
        ).fetchdf()

        feature_frame = build_feature_frame(signals)
        connection.execute("DELETE FROM features")
        if feature_frame.empty:
            return 0

        connection.register("features_df", feature_frame)
        connection.execute(
            """
            INSERT INTO features (
                ts,
                series_name,
                pct_change_30d,
                pct_change_90d,
                pct_change_365d,
                zscore_30d,
                volatility_30d,
                slope_30d,
                drawdown_90d,
                trend_label
            )
            SELECT
                ts,
                series_name,
                pct_change_30d,
                pct_change_90d,
                pct_change_365d,
                zscore_30d,
                volatility_30d,
                slope_30d,
                drawdown_90d,
                trend_label
            FROM features_df
            """
        )
        connection.unregister("features_df")
    return int(len(feature_frame))

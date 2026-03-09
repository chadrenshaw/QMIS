"""Trend helpers for the QMIS feature engine."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import linregress


def compute_percent_change_windows(series: pd.Series, windows: tuple[int, ...] = (30, 90, 365)) -> pd.DataFrame:
    """Compute rolling percent-change columns for the requested windows."""
    numeric = pd.to_numeric(series, errors="coerce")
    payload = {
        f"pct_change_{window}d": ((numeric / numeric.shift(window)) - 1.0) * 100.0
        for window in windows
    }
    return pd.DataFrame(payload, index=series.index)


def compute_rolling_slope(series: pd.Series, window: int = 30) -> pd.Series:
    """Compute a rolling linear-regression slope on raw values."""
    numeric = pd.to_numeric(series, errors="coerce")

    def _window_slope(values: np.ndarray) -> float:
        x_axis = np.arange(len(values), dtype=float)
        return float(linregress(x_axis, values).slope)

    return numeric.rolling(window=window, min_periods=window).apply(_window_slope, raw=True)


def classify_trend(percent_change: float | None) -> str | None:
    """Classify a percent change with the spec trend thresholds."""
    if percent_change is None or pd.isna(percent_change):
        return None
    if percent_change > 5.0:
        return "UP"
    if percent_change < -5.0:
        return "DOWN"
    return "SIDEWAYS"

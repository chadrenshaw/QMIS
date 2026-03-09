"""Momentum helpers for the QMIS feature engine."""

from __future__ import annotations

import pandas as pd


def compute_moving_average(series: pd.Series, window: int) -> pd.Series:
    """Compute a simple rolling moving average over the given window."""
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.rolling(window=window, min_periods=window).mean()

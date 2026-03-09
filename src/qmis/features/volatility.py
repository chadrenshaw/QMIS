"""Volatility and normalization helpers for the QMIS feature engine."""

from __future__ import annotations

import pandas as pd


def compute_rolling_zscore(series: pd.Series, window: int = 30) -> pd.Series:
    """Compute a rolling z-score over raw values."""
    numeric = pd.to_numeric(series, errors="coerce")
    rolling_mean = numeric.rolling(window=window, min_periods=window).mean()
    rolling_std = numeric.rolling(window=window, min_periods=window).std(ddof=1)
    return (numeric - rolling_mean) / rolling_std


def compute_rolling_volatility(series: pd.Series, window: int = 30) -> pd.Series:
    """Compute rolling volatility as the sample stddev of daily returns."""
    numeric = pd.to_numeric(series, errors="coerce")
    returns = numeric.pct_change()
    return returns.rolling(window=window, min_periods=window).std(ddof=1)


def compute_drawdown(series: pd.Series, window: int = 90) -> pd.Series:
    """Compute rolling drawdown from the local window high, expressed as percent."""
    numeric = pd.to_numeric(series, errors="coerce")
    rolling_peak = numeric.rolling(window=window, min_periods=window).max()
    return ((numeric / rolling_peak) - 1.0) * 100.0

"""Macro score computation for the QMIS regime engine."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd


UP = "UP"
DOWN = "DOWN"
SIDEWAYS = "SIDEWAYS"


def _latest_trends(feature_frame: pd.DataFrame) -> dict[str, str]:
    """Return the latest trend label for each series in the feature frame."""
    if feature_frame.empty:
        return {}

    latest = feature_frame.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")
    return {
        str(row["series_name"]): str(row["trend_label"])
        for _, row in latest.iterrows()
        if pd.notna(row["trend_label"])
    }


def _is_up(trends: Mapping[str, str], series_name: str) -> bool:
    return trends.get(series_name) == UP


def _is_down(trends: Mapping[str, str], series_name: str) -> bool:
    return trends.get(series_name) == DOWN


def compute_macro_scores(
    feature_frame: pd.DataFrame,
    signal_snapshot: Mapping[str, float | int | None],
) -> dict[str, float | int | str | None]:
    """Compute macro scores from the latest feature trends and yield inputs."""
    trends = _latest_trends(feature_frame)

    yield_10y = signal_snapshot.get("yield_10y")
    yield_3m = signal_snapshot.get("yield_3m")
    yield_curve = None
    yield_curve_state = "UNKNOWN"
    if yield_10y is not None and yield_3m is not None:
        yield_curve = float(yield_10y) - float(yield_3m)
        yield_curve_state = "NORMAL" if yield_curve > 0 else "INVERTED"

    inflation_score = sum(
        (
            _is_up(trends, "gold"),
            _is_up(trends, "oil"),
            _is_up(trends, "yield_10y"),
        )
    )

    growth_score = sum(
        (
            _is_up(trends, "copper"),
            _is_up(trends, "sp500"),
            _is_up(trends, "pmi"),
        )
    )

    liquidity_score = sum(
        (
            _is_up(trends, "fed_balance_sheet"),
            _is_up(trends, "m2_money_supply"),
            _is_down(trends, "reverse_repo_usage"),
            _is_down(trends, "dollar_index"),
        )
    )

    risk_score = sum(
        (
            _is_up(trends, "vix"),
            _is_down(trends, "sp500"),
            yield_curve_state == "INVERTED",
        )
    )

    return {
        "inflation_score": int(inflation_score),
        "growth_score": int(growth_score),
        "liquidity_score": int(liquidity_score),
        "risk_score": int(risk_score),
        "yield_curve": yield_curve,
        "yield_curve_state": yield_curve_state,
    }

"""Cross-market divergence detection on top of relationship and feature snapshots."""

from __future__ import annotations

from typing import Any

import pandas as pd

from qmis.signals.persistence import build_persistence_metadata


EXPECTED_MAGNITUDE = 0.6
CURRENT_DIVERGENCE_THRESHOLD = 0.35

TEMPLATES = (
    {
        "divergence_key": "gold_vs_yields",
        "title": "Gold Rising With Yields",
        "pairs": (("gold", "yield_10y"), ("gold", "yield_3m")),
        "expected_direction": "negative",
        "description": "Gold and yields are moving together despite a historically inverse relationship.",
    },
    {
        "divergence_key": "crypto_vs_liquidity",
        "title": "Crypto Decoupling From Liquidity",
        "pairs": (
            ("BTCUSD", "fed_balance_sheet"),
            ("ETHUSD", "fed_balance_sheet"),
            ("BTCUSD", "m2_money_supply"),
            ("ETHUSD", "m2_money_supply"),
        ),
        "expected_direction": "positive",
        "description": "Crypto is moving opposite to core liquidity proxies.",
    },
    {
        "divergence_key": "equities_vs_copper",
        "title": "Equities Outrunning Copper",
        "pairs": (("sp500", "copper"),),
        "expected_direction": "positive",
        "description": "Equities are advancing while cyclical commodities lag.",
    },
)


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "ts",
            "divergence_key",
            "title",
            "series_x",
            "series_y",
            "expected_direction",
            "observed_direction",
            "historical_window_days",
            "current_window_days",
            "historical_correlation",
            "current_correlation",
            "current_state",
            "persistence_windows",
            "required_windows",
            "observed_windows",
            "persistence_label",
            "passes_filter",
            "strength",
            "severity",
            "summary",
        ]
    )


def _canonical_pair(series_x: str, series_y: str) -> tuple[str, str]:
    return tuple(sorted((series_x, series_y)))


def _latest_feature_map(features: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if features.empty:
        return {}
    frame = features.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["series_name", "ts"]).drop_duplicates(subset=["series_name"], keep="last")
    return {
        str(row["series_name"]): {
            "trend_label": str(row.get("trend_label") or "N/A"),
            "pct_change_30d": float(row["pct_change_30d"]) if row.get("pct_change_30d") is not None and pd.notna(row.get("pct_change_30d")) else None,
        }
        for _, row in frame.iterrows()
    }


def _sign_matches_expected(value: float, expected_direction: str) -> bool:
    if expected_direction == "positive":
        return value > 0
    return value < 0


def _observed_direction(left_trend: str, right_trend: str, expected_direction: str, current_correlation: float) -> str | None:
    if expected_direction == "positive" and {left_trend, right_trend} == {"UP", "DOWN"}:
        return "opposite_moves"
    if expected_direction == "negative" and left_trend == right_trend and left_trend in {"UP", "DOWN"}:
        return "same_direction_moves"
    if not _sign_matches_expected(current_correlation, expected_direction):
        return "correlation_flip"
    return None


def _is_degraded(row: dict[str, Any], expected_direction: str) -> bool:
    current_state = str(row.get("relationship_state", ""))
    correlation = float(row.get("correlation", 0.0))
    if current_state in {"broken", "weakening"}:
        return True
    if abs(correlation) < CURRENT_DIVERGENCE_THRESHOLD:
        return True
    return not _sign_matches_expected(correlation, expected_direction)


def _persistence_label(persistence_windows: int) -> str:
    if persistence_windows >= 3:
        return "entrenched"
    if persistence_windows >= 2:
        return "persistent"
    return "developing"


def _severity(strength: float) -> str:
    if strength >= 0.75:
        return "strong"
    if strength >= 0.5:
        return "moderate"
    return "developing"


def _build_summary(
    *,
    title: str,
    description: str,
    historical_window_days: int,
    current_window_days: int,
    historical_correlation: float,
    current_correlation: float,
    persistence_windows: int,
) -> str:
    return (
        f"{description} Historical {historical_window_days}d correlation {historical_correlation:.2f} has "
        f"deteriorated to {current_correlation:.2f} in the {current_window_days}d window and persisted across "
        f"{persistence_windows} window(s)."
    )


def _candidate_divergence(
    *,
    pair_rows: pd.DataFrame,
    feature_map: dict[str, dict[str, Any]],
    template: dict[str, Any],
) -> dict[str, Any] | None:
    pair_rows = pair_rows.sort_values("window_days").copy()
    expected_direction = str(template["expected_direction"])

    historical_candidates = pair_rows.loc[
        pair_rows["relationship_state"].isin(["stable", "emerging"])
        & (pair_rows["correlation"].abs() >= EXPECTED_MAGNITUDE)
    ]
    if historical_candidates.empty:
        return None

    historical_candidates = historical_candidates.loc[
        historical_candidates["correlation"].apply(lambda value: _sign_matches_expected(float(value), expected_direction))
    ]
    if historical_candidates.empty:
        return None

    degraded_rows = pair_rows.loc[pair_rows.apply(lambda row: _is_degraded(row.to_dict(), expected_direction), axis=1)]
    if degraded_rows.empty:
        return None

    current = degraded_rows.sort_values("window_days").iloc[0]
    historical = historical_candidates.sort_values("window_days").iloc[-1]
    if int(current["window_days"]) >= int(historical["window_days"]):
        return None

    left = str(current["series_x"])
    right = str(current["series_y"])
    left_trend = str(feature_map.get(left, {}).get("trend_label", "N/A"))
    right_trend = str(feature_map.get(right, {}).get("trend_label", "N/A"))
    observed_direction = _observed_direction(left_trend, right_trend, expected_direction, float(current["correlation"]))
    if observed_direction is None:
        return None

    persistence_windows = int(
        degraded_rows.loc[degraded_rows["window_days"] < int(historical["window_days"]), "window_days"].nunique()
    )
    correlation_gap = min(abs(float(historical["correlation"]) - float(current["correlation"])) / 1.5, 1.0)
    strength = min(
        1.0,
        round(
            abs(float(historical["correlation"])) * 0.45
            + correlation_gap * 0.35
            + min(persistence_windows / 3.0, 1.0) * 0.2,
            4,
        ),
    )

    latest_ts = pd.to_datetime(pair_rows["ts"]).max()
    persistence = build_persistence_metadata(
        degraded_rows.loc[degraded_rows["window_days"] < int(historical["window_days"]), "window_days"].tolist(),
        family="divergences",
    )
    return {
        "ts": latest_ts,
        "divergence_key": str(template["divergence_key"]),
        "title": str(template["title"]),
        "series_x": left,
        "series_y": right,
        "expected_direction": expected_direction,
        "observed_direction": observed_direction,
        "historical_window_days": int(historical["window_days"]),
        "current_window_days": int(current["window_days"]),
        "historical_correlation": float(historical["correlation"]),
        "current_correlation": float(current["correlation"]),
        "current_state": str(current["relationship_state"]),
        **persistence,
        "strength": strength,
        "severity": _severity(strength),
        "summary": _build_summary(
            title=str(template["title"]),
            description=str(template["description"]),
            historical_window_days=int(historical["window_days"]),
            current_window_days=int(current["window_days"]),
            historical_correlation=float(historical["correlation"]),
            current_correlation=float(current["correlation"]),
            persistence_windows=int(persistence["persistence_windows"]),
        ),
    }


def detect_cross_market_divergences(
    *,
    relationships: pd.DataFrame,
    features: pd.DataFrame,
) -> pd.DataFrame:
    """Detect a ranked set of canonical cross-market divergences."""
    if relationships.empty or features.empty:
        return _empty_frame()

    frame = relationships.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.loc[frame["lag_days"] == 0].copy()
    if frame.empty:
        return _empty_frame()

    feature_map = _latest_feature_map(features)
    pair_groups = {
        key: pair_frame.copy()
        for key, pair_frame in frame.groupby(frame.apply(lambda row: _canonical_pair(str(row["series_x"]), str(row["series_y"])), axis=1))
    }

    rows: list[dict[str, Any]] = []
    for template in TEMPLATES:
        best: dict[str, Any] | None = None
        for pair in template["pairs"]:
            pair_key = _canonical_pair(*pair)
            pair_rows = pair_groups.get(pair_key)
            if pair_rows is None or pair_rows.empty:
                continue
            candidate = _candidate_divergence(pair_rows=pair_rows, feature_map=feature_map, template=template)
            if candidate is None:
                continue
            if best is None or float(candidate["strength"]) > float(best["strength"]):
                best = candidate
        if best is not None:
            rows.append(best)

    if not rows:
        return _empty_frame()

    result = pd.DataFrame(rows)
    return result.sort_values(["strength", "persistence_windows", "title"], ascending=[False, False, True]).reset_index(drop=True)

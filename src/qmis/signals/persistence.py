"""Shared persistence rules for QMIS signal promotion."""

from __future__ import annotations

import json
from typing import Any, Iterable

import pandas as pd


PERSISTENCE_THRESHOLDS = {
    "factors": 2,
    "divergences": 2,
    "relationship_alerts": 2,
}

WINDOW_COLUMNS = {
    30: "pct_change_30d",
    90: "pct_change_90d",
    365: "pct_change_365d",
}


def build_persistence_metadata(observed_windows: Iterable[int], *, family: str) -> dict[str, Any]:
    unique_windows = sorted({int(window) for window in observed_windows if int(window) > 0})
    required_windows = int(PERSISTENCE_THRESHOLDS.get(family, 1))
    count = len(unique_windows)
    if count >= max(required_windows + 1, 3):
        label = "entrenched"
    elif count >= required_windows:
        label = "persistent"
    elif count == 1:
        label = "transient"
    elif count > 1:
        label = "emerging"
    else:
        label = "absent"
    return {
        "persistence_windows": count,
        "required_windows": required_windows,
        "observed_windows": unique_windows,
        "persistence_label": label,
        "passes_filter": count >= required_windows,
    }


def _parse_supporting_assets(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return []


def _factor_expected_signs(factor_name: str, direction: str) -> dict[str, int]:
    direction = str(direction).lower()
    factor_name = str(factor_name).lower()
    if factor_name == "liquidity":
        return {
            "tightening": {
                "fed_balance_sheet": -1,
                "m2_money_supply": -1,
                "reverse_repo_usage": 1,
                "yield_3m": 1,
                "yield_10y": 1,
                "dollar_index": 1,
                "real_yields": 1,
            },
            "expanding": {
                "fed_balance_sheet": 1,
                "m2_money_supply": 1,
                "reverse_repo_usage": -1,
                "yield_3m": -1,
                "yield_10y": -1,
                "dollar_index": -1,
                "real_yields": -1,
            },
        }.get(direction, {})
    if factor_name == "crypto":
        polarity = 1 if direction == "bullish" else -1 if direction == "bearish" else 0
        return {asset: polarity for asset in ("BTCUSD", "ETHUSD", "crypto_market_cap", "BTC_dominance") if polarity}
    if factor_name == "volatility":
        if direction == "stressed":
            return {"vix": 1, "new_lows": 1, "sp500_above_200dma": -1, "new_highs": -1}
        if direction == "contained":
            return {"vix": -1, "new_lows": -1, "sp500_above_200dma": 1, "new_highs": 1}
    if factor_name == "growth":
        polarity = 1 if direction == "accelerating" else -1 if direction == "slowing" else 0
        return {asset: polarity for asset in ("copper", "sp500", "pmi") if polarity}
    if factor_name == "inflation":
        polarity = 1 if direction == "rising" else -1 if direction == "cooling" else 0
        return {asset: polarity for asset in ("gold", "oil", "yield_10y") if polarity}
    if factor_name == "commodities":
        polarity = 1 if direction == "rising" else -1 if direction == "weakening" else 0
        return {asset: polarity for asset in ("gold", "oil", "copper") if polarity}
    if factor_name == "dollar":
        polarity = 1 if direction == "strengthening" else -1 if direction == "weakening" else 0
        return {"dollar_index": polarity} if polarity else {}
    return {}


def annotate_factor_persistence(factors: list[dict[str, Any]], features: pd.DataFrame) -> list[dict[str, Any]]:
    if not factors:
        return []
    if features.empty:
        return [{**factor, **build_persistence_metadata([], family="factors")} for factor in factors]

    frame = features.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["series_name", "ts"]).drop_duplicates(subset=["series_name"], keep="last")
    feature_map = {
        str(row["series_name"]): {
            column: float(row[column]) if column in row and row[column] is not None and pd.notna(row[column]) else None
            for column in WINDOW_COLUMNS.values()
        }
        for _, row in frame.iterrows()
    }

    annotated: list[dict[str, Any]] = []
    for factor in factors:
        expected_signs = _factor_expected_signs(str(factor.get("factor_name", "")), str(factor.get("direction", "")))
        supporting_assets = _parse_supporting_assets(factor.get("supporting_assets"))
        assets = [asset for asset in supporting_assets if asset in expected_signs] or list(expected_signs.keys())
        observed_windows: list[int] = []
        for window_days, column in WINDOW_COLUMNS.items():
            aligned = 0
            total = 0
            for asset in assets:
                value = feature_map.get(asset, {}).get(column)
                expected = expected_signs.get(asset)
                if value is None or not expected:
                    continue
                total += 1
                if value == 0:
                    continue
                if (value > 0 and expected > 0) or (value < 0 and expected < 0):
                    aligned += 1
            if total and (aligned / total) >= 0.6:
                observed_windows.append(window_days)
        annotated.append({**factor, **build_persistence_metadata(observed_windows, family="factors")})
    return annotated

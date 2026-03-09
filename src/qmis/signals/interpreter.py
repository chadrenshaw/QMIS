"""Operator-facing interpretation layer for QMIS dashboard snapshots."""

from __future__ import annotations

from collections import defaultdict
from typing import Any


SIGNIFICANT_CORRELATION_THRESHOLD = 0.7
SIGNIFICANCE_P_VALUE = 0.05
EXPERIMENTAL_CATEGORIES = {"astronomy", "natural"}
SOLAR_ACTIVITY_SERIES = ("sunspot_number", "solar_flare_count", "solar_flux_f107")
NATURAL_SIGNAL_SERIES = (
    "earthquake_count",
    "geomagnetic_activity",
    "geomagnetic_kp",
    "global_temperature_anomaly",
    "solar_wind_speed",
)
LIQUIDITY_SERIES = {"fed_balance_sheet", "m2_money_supply", "reverse_repo_usage"}
CRYPTO_SERIES = {"BTCUSD", "ETHUSD", "BTC_dominance", "crypto_market_cap"}
VOLATILITY_SERIES = {"vix", "sp500_above_200dma", "new_highs", "new_lows"}


def _signal(snapshot: dict[str, Any], series_name: str) -> dict[str, Any]:
    return dict(snapshot.get("signal_summary", {}).get(series_name, {}))


def _signal_value(snapshot: dict[str, Any], series_name: str) -> float | None:
    value = snapshot.get("signal_summary", {}).get(series_name, {}).get("value")
    return float(value) if value is not None else None


def _trend(snapshot: dict[str, Any], series_name: str) -> str:
    trend_label = snapshot.get("trend_summary", {}).get(series_name, {}).get("trend_label")
    return str(trend_label) if trend_label is not None else "N/A"


def _series_category(snapshot: dict[str, Any], series_name: str) -> str:
    signal = snapshot.get("signal_summary", {}).get(series_name, {})
    category = signal.get("category")
    if category:
        return str(category)

    if series_name in CRYPTO_SERIES:
        return "crypto"
    if series_name in LIQUIDITY_SERIES or series_name in {"yield_10y", "yield_3m", "pmi"}:
        return "macro"
    if series_name in {"vix", "gold", "oil", "copper", "sp500"}:
        return "market"
    if series_name in {"sp500_above_200dma", "advance_decline_line", "new_highs", "new_lows"}:
        return "breadth"
    if series_name in {"sunspot_number", "solar_flare_count", "solar_flux_f107", "solar_longitude", "zodiac_index"}:
        return "astronomy"
    if series_name in {"earthquake_count", "geomagnetic_kp", "solar_wind_speed", "global_temperature_anomaly"}:
        return "natural"
    return "unknown"


def _relationship_is_significant(row: dict[str, Any]) -> bool:
    return (
        int(row.get("lag_days", 0)) == 0
        and str(row.get("relationship_state")) == "stable"
        and abs(float(row.get("correlation", 0.0))) > SIGNIFICANT_CORRELATION_THRESHOLD
        and float(row.get("p_value", 1.0)) < SIGNIFICANCE_P_VALUE
    )


def _lunar_phase_name(lunar_cycle_day: float | None) -> str:
    if lunar_cycle_day is None:
        return "Unknown"
    if lunar_cycle_day < 1.85:
        return "New Moon"
    if lunar_cycle_day < 5.54:
        return "Waxing Crescent"
    if lunar_cycle_day < 9.23:
        return "First Quarter"
    if lunar_cycle_day < 12.92:
        return "Waxing Gibbous"
    if lunar_cycle_day < 16.61:
        return "Full Moon"
    if lunar_cycle_day < 20.30:
        return "Waning Gibbous"
    if lunar_cycle_day < 23.99:
        return "Last Quarter"
    if lunar_cycle_day < 27.68:
        return "Waning Crescent"
    return "New Moon"


def interpret_world_state(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Summarize astronomy and natural signals into a concise world-state view."""
    zodiac_signal = _signal(snapshot, "zodiac_index")
    solar_longitude = _signal_value(snapshot, "solar_longitude")
    lunar_cycle_day = _signal_value(snapshot, "lunar_cycle_day")
    lunar_illumination = _signal_value(snapshot, "lunar_illumination")

    solar_activity = []
    for series_name in SOLAR_ACTIVITY_SERIES:
        signal = _signal(snapshot, series_name)
        if not signal:
            continue
        solar_activity.append(
            {
                "series_name": series_name,
                "value": float(signal["value"]),
                "unit": str(signal["unit"]),
                "source": str(signal["source"]),
                "trend_label": _trend(snapshot, series_name),
            }
        )

    natural_signals = []
    for series_name in NATURAL_SIGNAL_SERIES:
        signal = _signal(snapshot, series_name)
        if not signal:
            continue
        natural_signals.append(
            {
                "series_name": series_name,
                "value": float(signal["value"]),
                "unit": str(signal["unit"]),
                "source": str(signal["source"]),
                "trend_label": _trend(snapshot, series_name),
            }
        )

    return {
        "sun_sign": str(zodiac_signal.get("metadata", {}).get("zodiac_sign", "Unknown")),
        "solar_longitude": solar_longitude,
        "zodiac_index": int(zodiac_signal["value"]) if zodiac_signal.get("value") is not None else None,
        "lunar_phase": _lunar_phase_name(lunar_cycle_day),
        "lunar_cycle_day": lunar_cycle_day,
        "lunar_illumination": lunar_illumination,
        "solar_activity": solar_activity,
        "natural_signals": natural_signals,
    }


def _factor_theme(snapshot: dict[str, Any], row: dict[str, Any]) -> str | None:
    left = str(row["series_x"])
    right = str(row["series_y"])
    left_category = _series_category(snapshot, left)
    right_category = _series_category(snapshot, right)
    categories = {left_category, right_category}
    names = {left, right}

    if names & LIQUIDITY_SERIES or "liquidity" in categories:
        return "liquidity_factor"
    if categories & EXPERIMENTAL_CATEGORIES:
        return None
    if "crypto" in categories:
        return "crypto_factor"
    if names & VOLATILITY_SERIES:
        return "volatility_factor"
    return None


def _factor_title(theme: str) -> str:
    return {
        "crypto_factor": "Crypto Factor",
        "liquidity_factor": "Liquidity Factor",
        "volatility_factor": "Volatility Factor",
    }.get(theme, theme.replace("_", " ").title())


def interpret_market_forces(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Aggregate the strongest non-experimental stable correlations into operator themes."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in snapshot.get("relationships", []):
        if not _relationship_is_significant(row):
            continue
        theme = _factor_theme(snapshot, row)
        if theme is None:
            continue
        grouped[theme].append(dict(row))

    results: list[dict[str, Any]] = []
    for theme, rows in grouped.items():
        rows = sorted(rows, key=lambda row: abs(float(row["correlation"])), reverse=True)
        lead = rows[0]
        direction = "positive" if float(lead["correlation"]) >= 0 else "negative"
        results.append(
            {
                "theme": theme,
                "title": _factor_title(theme),
                "direction": direction,
                "strength": abs(float(lead["correlation"])),
                "summary": f"{lead['series_x']} vs {lead['series_y']} leading at {float(lead['correlation']):.2f}.",
                "pairs": [
                    {
                        "label": f"{row['series_x']} vs {row['series_y']}",
                        "correlation": float(row["correlation"]),
                        "window_days": int(row["window_days"]),
                    }
                    for row in rows[:3]
                ],
            }
        )

    return sorted(results, key=lambda item: item["strength"], reverse=True)


def _relationship_change_title(snapshot: dict[str, Any], row: dict[str, Any]) -> str:
    left = str(row["series_x"])
    right = str(row["series_y"])
    categories = {_series_category(snapshot, left), _series_category(snapshot, right)}
    names = {left, right}
    if "crypto" in categories and "macro" in categories:
        return "Crypto vs Macro Decoupling"
    if "crypto" in categories and "liquidity" in categories:
        return "Liquidity Transmission Shift"
    if "crypto" in categories and ("market" in categories or "breadth" in categories):
        return "Crypto Risk Appetite Break"
    if names & VOLATILITY_SERIES or "breadth" in categories:
        return "Breadth and Volatility Regime Shift"
    if "macro" in categories and "market" in categories:
        return "Macro Pricing Shift"
    return ""


def summarize_relationship_breaks(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Group low-level relationship breaks into higher-level narratives."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in snapshot.get("anomalies", []):
        if str(row.get("anomaly_type")) != "relationship_break":
            continue
        title = _relationship_change_title(snapshot, row)
        if not title:
            continue
        grouped[title].append(dict(row))

    results = []
    for title, rows in grouped.items():
        pairs = [f"{row['series_x']} vs {row['series_y']}" for row in rows]
        results.append(
            {
                "title": title,
                "count": len(rows),
                "summary": f"{len(rows)} relationship break(s): {', '.join(pairs[:3])}.",
                "pairs": pairs,
            }
        )

    priorities = {
        "Crypto vs Macro Decoupling": 0,
        "Liquidity Transmission Shift": 1,
        "Breadth and Volatility Regime Shift": 2,
        "Crypto Risk Appetite Break": 3,
        "Macro Pricing Shift": 4,
    }
    return sorted(results, key=lambda item: (priorities.get(item["title"], 99), -item["count"], item["title"]))


def generate_risk_indicators(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Translate raw macro inputs into operator-friendly risk descriptors."""
    vix = _signal_value(snapshot, "vix")
    inflation_score = int(snapshot.get("scores", {}).get("inflation_score", 0))
    growth_score = int(snapshot.get("scores", {}).get("growth_score", 0))
    liquidity_score = int(snapshot.get("scores", {}).get("liquidity_score", 0))
    pmi = _signal_value(snapshot, "pmi")
    breadth = _signal_value(snapshot, "sp500_above_200dma")
    regime_label = str(snapshot.get("regime", {}).get("regime_label", ""))

    if vix is None:
        volatility_state = "unknown"
    elif vix >= 25.0:
        volatility_state = "stressed"
    elif vix >= 18.0:
        volatility_state = "elevated"
    else:
        volatility_state = "contained"

    if regime_label == "LIQUIDITY WITHDRAWAL" or liquidity_score <= 0:
        liquidity_state = "tightening"
    elif liquidity_score >= 3:
        liquidity_state = "expanding"
    else:
        liquidity_state = "neutral"

    if inflation_score >= 3:
        inflation_state = "hot"
    elif inflation_score >= 2:
        inflation_state = "elevated"
    else:
        inflation_state = "contained"

    if (pmi is not None and pmi < 50.0) or growth_score <= 1 or (breadth is not None and breadth < 55.0):
        growth_state = "softening"
    elif growth_score >= 3 and (pmi is None or pmi >= 52.0):
        growth_state = "accelerating"
    else:
        growth_state = "steady"

    return {
        "volatility": {
            "state": volatility_state,
            "value": vix,
            "summary": "VIX is driving the risk backdrop.",
        },
        "liquidity": {
            "state": liquidity_state,
            "value": liquidity_score,
            "summary": f"Regime bias is {regime_label or 'unknown'}.",
        },
        "inflation_pressure": {
            "state": inflation_state,
            "value": inflation_score,
            "summary": "Inflation pressure reflects the current score stack.",
        },
        "growth_momentum": {
            "state": growth_state,
            "value": pmi,
            "summary": "Growth momentum blends PMI and breadth participation.",
        },
    }


def _select_significant_correlations(snapshot: dict[str, Any], *, experimental_only: bool) -> list[dict[str, Any]]:
    rows_by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    for row in snapshot.get("relationships", []):
        if not _relationship_is_significant(row):
            continue
        categories = {
            _series_category(snapshot, str(row["series_x"])),
            _series_category(snapshot, str(row["series_y"])),
        }
        is_experimental = bool(categories & EXPERIMENTAL_CATEGORIES)
        if experimental_only != is_experimental:
            continue
        pair_key = tuple(sorted((str(row["series_x"]), str(row["series_y"]))))
        candidate = {
            "pair": f"{row['series_x']} vs {row['series_y']}",
            "correlation": float(row["correlation"]),
            "window_days": int(row["window_days"]),
            "p_value": float(row["p_value"]),
            "state": str(row["relationship_state"]),
        }
        existing = rows_by_pair.get(pair_key)
        if existing is None or abs(candidate["correlation"]) > abs(existing["correlation"]) or (
            abs(candidate["correlation"]) == abs(existing["correlation"]) and candidate["window_days"] > existing["window_days"]
        ):
            rows_by_pair[pair_key] = candidate
    return sorted(rows_by_pair.values(), key=lambda item: abs(item["correlation"]), reverse=True)


def _build_experimental_snapshot(snapshot: dict[str, Any], world_state: dict[str, Any]) -> dict[str, Any]:
    signals = world_state["solar_activity"] + world_state["natural_signals"]
    correlations = _select_significant_correlations(snapshot, experimental_only=True)
    return {"signals": signals, "correlations": correlations}


def generate_operator_watchlist(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a short operator watchlist from the interpreted state."""
    risk = generate_risk_indicators(snapshot)
    relationship_changes = summarize_relationship_breaks(snapshot)

    watch_items: list[dict[str, Any]] = []
    for change in relationship_changes[:2]:
        watch_items.append({"title": change["title"], "detail": change["summary"]})

    breadth = _signal_value(snapshot, "sp500_above_200dma")
    if breadth is not None and (breadth < 55.0 or _trend(snapshot, "sp500_above_200dma") == "DOWN"):
        watch_items.append(
            {
                "title": "Breadth deterioration",
                "detail": f"S&P 500 above 200DMA is down to {breadth:.2f}%.",
            }
        )

    if risk["volatility"]["state"] in {"elevated", "stressed"}:
        vix = _signal_value(snapshot, "vix")
        watch_items.append(
            {
                "title": "Rising volatility",
                "detail": f"VIX is elevated at {vix:.2f}." if vix is not None else "Volatility is elevated.",
            }
        )

    if risk["liquidity"]["state"] == "tightening":
        watch_items.append(
            {
                "title": "Liquidity remains restrictive",
                "detail": risk["liquidity"]["summary"],
            }
        )

    deduped: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    for item in watch_items:
        title = str(item["title"])
        if title in seen_titles:
            continue
        seen_titles.add(title)
        deduped.append(item)
        if len(deduped) == 5:
            break
    return deduped


def build_operator_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Compose the full operator intelligence summary from the raw dashboard snapshot."""
    world_state = interpret_world_state(snapshot)
    return {
        "world_state": world_state,
        "market_forces": interpret_market_forces(snapshot),
        "relationship_changes": summarize_relationship_breaks(snapshot),
        "risk_indicators": generate_risk_indicators(snapshot),
        "significant_correlations": _select_significant_correlations(snapshot, experimental_only=False),
        "experimental_signals": _build_experimental_snapshot(snapshot, world_state),
        "watchlist": generate_operator_watchlist(snapshot),
    }

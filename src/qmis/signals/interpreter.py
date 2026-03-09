"""Operator-facing interpretation layer for QMIS dashboard snapshots."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from qmis.signals.narrative import build_market_narrative


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
LIQUIDITY_SERIES = {"fed_balance_sheet", "m2_money_supply", "reverse_repo_usage", "real_yields", "dollar_index"}
CRYPTO_SERIES = {"BTCUSD", "ETHUSD", "BTC_dominance", "crypto_market_cap"}
VOLATILITY_SERIES = {"vix", "sp500_above_200dma", "new_highs", "new_lows"}


def _signal(snapshot: dict[str, Any], series_name: str) -> dict[str, Any]:
    return dict(snapshot.get("signal_summary", {}).get(series_name, {}))


def _signal_value(snapshot: dict[str, Any], series_name: str) -> float | None:
    value = snapshot.get("signal_summary", {}).get(series_name, {}).get("value")
    return float(value) if value is not None else None


def _liquidity_environment(snapshot: dict[str, Any]) -> dict[str, Any]:
    liquidity = snapshot.get("liquidity_environment")
    return dict(liquidity) if isinstance(liquidity, dict) else {}


def _breadth_health(snapshot: dict[str, Any]) -> dict[str, Any]:
    breadth = snapshot.get("breadth_health")
    return dict(breadth) if isinstance(breadth, dict) else {}


def build_regime_probability_summary(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    regime = snapshot.get("regime") or {}
    probabilities = regime.get("regime_probabilities") or {}
    drivers = regime.get("regime_drivers") or {}
    if not isinstance(probabilities, dict):
        return []
    ordered = sorted(
        ((str(label), float(probability)) for label, probability in probabilities.items()),
        key=lambda item: item[1],
        reverse=True,
    )
    return [
        {
            "label": label,
            "probability": probability,
            "drivers": list(drivers.get(label, [])) if isinstance(drivers, dict) else [],
        }
        for label, probability in ordered[:5]
    ]


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
        if not bool(row.get("passes_filter", True)):
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
    liquidity_snapshot = _liquidity_environment(snapshot)
    liquidity_score = liquidity_snapshot.get("liquidity_score")
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

    liquidity_state = str(liquidity_snapshot.get("liquidity_state", "")).lower()
    if liquidity_state not in {"tightening", "neutral", "expanding"}:
        fallback_score = int(snapshot.get("scores", {}).get("liquidity_score", 0))
        if regime_label == "LIQUIDITY WITHDRAWAL" or fallback_score <= 0:
            liquidity_state = "tightening"
        elif fallback_score >= 3:
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
            "summary": str(liquidity_snapshot.get("summary") or f"Regime bias is {regime_label or 'unknown'}."),
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


def _risk_level_from_vix(vix: float | None) -> str:
    if vix is None:
        return "MODERATE"
    if vix >= 35.0:
        return "CRITICAL"
    if vix >= 25.0:
        return "HIGH"
    if vix >= 20.0:
        return "ELEVATED"
    if vix >= 15.0:
        return "MODERATE"
    return "LOW"


def _liquidity_descriptor(snapshot: dict[str, Any]) -> str:
    risk = generate_risk_indicators(snapshot)
    state = str(risk["liquidity"]["state"])
    return {"tightening": "TIGHTENING", "neutral": "NEUTRAL", "expanding": "EXPANDING"}.get(state, "NEUTRAL")


def _growth_descriptor(snapshot: dict[str, Any]) -> str:
    breadth_snapshot = _breadth_health(snapshot)
    breadth_state = str(breadth_snapshot.get("breadth_state", "")).upper()
    pmi = _signal_value(snapshot, "pmi")
    breadth = _signal_value(snapshot, "sp500_above_200dma")
    if breadth_state == "FRAGILE":
        return "WEAK"
    if pmi is not None and pmi < 50.0:
        return "WEAK"
    if breadth is not None and breadth < 50.0:
        return "WEAK"
    if pmi is not None and pmi >= 54.0 and (breadth is None or breadth >= 65.0):
        return "STRONG"
    return "STABLE"


def _inflation_descriptor(snapshot: dict[str, Any]) -> str:
    inflation_score = int(snapshot.get("scores", {}).get("inflation_score", 0))
    if inflation_score >= 3:
        return "HOT"
    if inflation_score >= 2:
        return "ELEVATED"
    return "NEUTRAL"


def _solar_activity_level(snapshot: dict[str, Any]) -> str:
    sunspots = _signal_value(snapshot, "sunspot_number") or 0.0
    flares = _signal_value(snapshot, "solar_flare_count") or 0.0
    flux = _signal_value(snapshot, "solar_flux_f107") or 0.0
    if sunspots >= 200.0 or flares >= 10.0 or flux >= 180.0:
        return "HIGH"
    if sunspots >= 120.0 or flares >= 4.0 or flux >= 120.0:
        return "ELEVATED"
    if sunspots >= 60.0 or flares >= 2.0 or flux >= 100.0:
        return "MODERATE"
    return "LOW"


def build_global_state_line(snapshot: dict[str, Any]) -> str:
    regime = str(snapshot.get("regime", {}).get("regime_label", "UNKNOWN"))
    volatility = _risk_level_from_vix(_signal_value(snapshot, "vix"))
    liquidity = _liquidity_descriptor(snapshot)
    growth = _growth_descriptor(snapshot)
    inflation = _inflation_descriptor(snapshot)
    return (
        f"Regime: {regime} | Volatility: {volatility} | Liquidity: {liquidity} | "
        f"Growth: {growth} | Inflation: {inflation}"
    )


def _pulse_state(snapshot: dict[str, Any], series_name: str) -> str:
    trend = _trend(snapshot, series_name)
    if trend in {"UP", "DOWN", "SIDEWAYS"}:
        return trend
    return "N/A"


def build_market_pulse(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    return [
        {"label": "Equities", "state": _pulse_state(snapshot, "sp500")},
        {"label": "Crypto", "state": _pulse_state(snapshot, "BTCUSD")},
        {"label": "Energy", "state": _pulse_state(snapshot, "oil")},
        {"label": "Volatility", "state": _pulse_state(snapshot, "vix")},
        {"label": "Dollar", "state": _pulse_state(snapshot, "dollar_index")},
        {"label": "Rates", "state": "STABLE" if snapshot.get("yield_curve_state") == "NORMAL" else "INVERTED"},
    ]


def build_cycle_monitor(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    cycle_rows = snapshot.get("cycles") or []
    if not isinstance(cycle_rows, list):
        return []

    cycle_map = {str(row.get("cycle_name")): dict(row) for row in cycle_rows if isinstance(row, dict)}
    results = []
    for cycle_name, label in (
        ("solar_cycle", "Solar Cycle Phase"),
        ("lunar_cycle", "Lunar Cycle Phase"),
        ("macro_liquidity_cycle", "Macro Liquidity Cycle Phase"),
    ):
        row = cycle_map.get(cycle_name, {})
        phase = str(row.get("phase") or "unknown").replace("_", " ").title()
        summary = str(row.get("summary") or "No cycle snapshot available.")
        if row.get("is_turning_point"):
            summary = f"Turning point | {summary}"
        results.append({"label": label, "phase": phase, "summary": summary})
    return results


def build_cosmic_state_line(snapshot: dict[str, Any], world_state: dict[str, Any] | None = None) -> str:
    state = world_state or interpret_world_state(snapshot)
    lunar_cycle_day = state.get("lunar_cycle_day")
    lunar_illumination = state.get("lunar_illumination")
    day_display = f"{float(lunar_cycle_day):.1f}" if lunar_cycle_day is not None else "N/A"
    illumination_display = f"{float(lunar_illumination):.0f}%" if lunar_illumination is not None else "N/A"
    return (
        f"Sun: {state['sun_sign']} | Moon: {state['lunar_phase']} | "
        f"Day: {day_display} | Illumination: {illumination_display} | "
        f"Solar: {_solar_activity_level(snapshot)}"
    )


def build_market_drivers(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    factors = list(snapshot.get("factors") or [])
    if factors:
        drivers: list[dict[str, str]] = []
        for factor in sorted(
            factors,
            key=lambda item: (int(item.get("component_rank", 99)), -float(item.get("strength", 0.0))),
        ):
            if not bool(factor.get("passes_filter", True)):
                continue
            factor_name = str(factor.get("factor_name", "")).lower()
            direction = str(factor.get("direction", "")).lower()
            if factor_name == "liquidity":
                title = f"Liquidity {direction.title()}" if direction else "Liquidity Regime"
            elif factor_name == "crypto":
                title = "Crypto Cycle"
            elif factor_name == "volatility":
                title = "Volatility Regime"
            else:
                title = factor_name.replace("_", " ").title()

            strength = float(factor.get("strength", 0.0))
            strength_label = "Strong" if strength >= 0.6 else "Moderate" if strength >= 0.35 else "Developing"
            persistence_label = str(factor.get("persistence_label", "persistent")).replace("_", " ").title()
            supporting_assets = factor.get("supporting_assets", [])
            if not isinstance(supporting_assets, list):
                supporting_assets = []
            summary = str(factor.get("summary") or "").strip()
            if summary:
                detail = f"{persistence_label} | {strength_label} | {summary}"
            else:
                assets_label = ", ".join(str(asset) for asset in supporting_assets[:3]) or "supporting assets unavailable"
                detail = f"{persistence_label} | {strength_label} | {direction or 'mixed'} | {assets_label}"
            drivers.append({"title": title, "summary": detail})
            if len(drivers) == 3:
                break
        return drivers

    drivers = []
    for force in interpret_market_forces(snapshot)[:3]:
        label = force["title"].replace("Factor", "factor")
        direction = "supporting" if force["direction"] == "positive" else "pressuring"
        lead_pair = force["pairs"][0]["label"] if force["pairs"] else "no lead pair"
        drivers.append(
            {
                "title": label,
                "summary": f"{label} is {direction} markets via {lead_pair} ({force['strength']:.2f}).",
            }
        )
    return drivers


def build_divergence_summary(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("divergences") or []
    if not isinstance(rows, list):
        return []
    ordered = sorted(
        (
            {
                "title": str(row.get("title", "")),
                "summary": str(row.get("summary", "")),
                "strength": float(row.get("strength", 0.0)),
                "severity": str(row.get("severity", "developing")),
                "persistence_label": str(row.get("persistence_label", "developing")),
            }
            for row in rows
            if row.get("title") and bool(row.get("passes_filter", True))
        ),
        key=lambda item: item["strength"],
        reverse=True,
    )
    return ordered[:3]


def build_risk_monitor(snapshot: dict[str, Any]) -> dict[str, dict[str, str]]:
    vix = _signal_value(snapshot, "vix")
    pmi = _signal_value(snapshot, "pmi")
    breadth = _signal_value(snapshot, "sp500_above_200dma")
    critical_alerts = sum(1 for alert in snapshot.get("alerts", []) if str(alert.get("severity")) == "critical")
    anomaly_count = len(snapshot.get("anomalies", []))
    base_risk = int(snapshot.get("scores", {}).get("risk_score", 0))

    breadth_snapshot = _breadth_health(snapshot)
    breadth_state = str(breadth_snapshot.get("breadth_state", "")).upper()
    liquidity_snapshot = _liquidity_environment(snapshot)
    liquidity_state = str(liquidity_snapshot.get("liquidity_state") or _liquidity_descriptor(snapshot)).upper()
    divergence_rows = build_divergence_summary(snapshot)

    if liquidity_state == "TIGHTENING":
        liquidity_level = "HIGH"
    elif liquidity_state == "EXPANDING":
        liquidity_level = "LOW"
    else:
        liquidity_level = "MODERATE"

    if breadth_state == "FRAGILE":
        breadth_level = "HIGH"
    elif breadth_state == "WEAKENING":
        breadth_level = "MODERATE"
    elif breadth_state == "STRONG":
        breadth_level = "LOW"
    elif breadth is not None and breadth < 50.0:
        breadth_level = "HIGH"
    elif breadth is not None and breadth < 60.0:
        breadth_level = "MODERATE"
    else:
        breadth_level = "LOW"

    if pmi is not None and pmi < 48.0 or (breadth is not None and breadth < 50.0):
        growth_level = "HIGH"
    elif pmi is not None and pmi < 50.0:
        growth_level = "ELEVATED"
    elif pmi is not None and pmi < 52.0:
        growth_level = "MODERATE"
    else:
        growth_level = "LOW"

    systemic_score = base_risk
    if vix is not None and vix >= 25.0:
        systemic_score += 1
    if anomaly_count >= 3:
        systemic_score += 1
    if critical_alerts >= 2:
        systemic_score += 1
    if breadth_state == "FRAGILE":
        systemic_score += 1

    if systemic_score >= 5:
        systemic_level = "CRITICAL"
    elif systemic_score >= 4:
        systemic_level = "HIGH"
    elif systemic_score >= 3:
        systemic_level = "ELEVATED"
    elif systemic_score >= 2:
        systemic_level = "MODERATE"
    else:
        systemic_level = "LOW"

    top_divergence = divergence_rows[0] if divergence_rows else None
    divergence_strength = float(top_divergence["strength"]) if top_divergence else 0.0
    if divergence_strength >= 0.75:
        divergence_level = "HIGH"
    elif divergence_strength >= 0.5:
        divergence_level = "MODERATE"
    elif top_divergence:
        divergence_level = "ELEVATED"
    else:
        divergence_level = "LOW"

    return {
        "volatility_risk": {"level": _risk_level_from_vix(vix), "summary": "Derived from the current VIX regime."},
        "breadth_risk": {
            "level": breadth_level,
            "summary": str(breadth_snapshot.get("summary") or "Tracks participation and high-low expansion."),
        },
        "liquidity_risk": {
            "level": liquidity_level,
            "summary": str(liquidity_snapshot.get("summary") or "Tracks tightening or easing liquidity conditions."),
        },
        "growth_risk": {"level": growth_level, "summary": "Blends PMI and breadth participation."},
        "divergence_risk": {
            "level": divergence_level,
            "summary": str(top_divergence["summary"]) if top_divergence else "No major cross-market divergences detected.",
        },
        "systemic_risk": {"level": systemic_level, "summary": "Combines base risk score, anomalies, and critical alerts."},
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
    correlations = _select_significant_correlations(snapshot, experimental_only=True)
    if not correlations:
        return {
            "visible": False,
            "signals": [],
            "correlations": [],
            "summary": "No significant experimental correlations detected.",
        }

    involved_series = {
        part
        for correlation in correlations
        for part in correlation["pair"].split(" vs ")
    }
    signals = [
        row
        for row in world_state["solar_activity"] + world_state["natural_signals"]
        if str(row["series_name"]) in involved_series
    ]
    return {
        "visible": True,
        "signals": signals,
        "correlations": correlations,
        "summary": f"{len(correlations)} significant experimental correlation(s) detected.",
    }


def generate_operator_watchlist(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a short operator watchlist from the interpreted state."""
    risk = generate_risk_indicators(snapshot)
    relationship_changes = summarize_relationship_breaks(snapshot)
    breadth_snapshot = _breadth_health(snapshot)
    divergence_rows = build_divergence_summary(snapshot)

    watch_items: list[dict[str, Any]] = []
    for divergence in divergence_rows[:2]:
        watch_items.append({"title": divergence["title"], "detail": divergence["summary"]})
    for change in relationship_changes[:2]:
        watch_items.append({"title": change["title"], "detail": change["summary"]})

    breadth = _signal_value(snapshot, "sp500_above_200dma")
    breadth_state = str(breadth_snapshot.get("breadth_state", "")).upper()
    if breadth_state in {"WEAKENING", "FRAGILE"}:
        watch_items.append(
            {
                "title": "Breadth deterioration",
                "detail": str(breadth_snapshot.get("summary") or "Participation is narrowing."),
            }
        )
    elif breadth is not None and (breadth < 55.0 or _trend(snapshot, "sp500_above_200dma") == "DOWN"):
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


def build_warning_signals(snapshot: dict[str, Any]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    divergences = build_divergence_summary(snapshot)
    shifts = summarize_relationship_breaks(snapshot)
    breadth_snapshot = _breadth_health(snapshot)
    if divergences:
        warnings.append({"title": divergences[0]["title"], "detail": divergences[0]["summary"]})
    if shifts:
        warnings.append({"title": shifts[0]["title"], "detail": shifts[0]["summary"]})

    breadth = _signal_value(snapshot, "sp500_above_200dma")
    breadth_state = str(breadth_snapshot.get("breadth_state", "")).upper()
    if breadth_state in {"WEAKENING", "FRAGILE"}:
        warnings.append({"title": "Breadth deterioration", "detail": str(breadth_snapshot.get("summary") or "Participation is narrowing.")})
    elif breadth is not None and (breadth < 55.0 or _trend(snapshot, "sp500_above_200dma") == "DOWN"):
        warnings.append({"title": "Breadth deterioration", "detail": f"Participation has slipped to {breadth:.2f}% above 200DMA."})

    vix = _signal_value(snapshot, "vix")
    if vix is not None and vix >= 18.0:
        warnings.append({"title": "Rising volatility", "detail": f"VIX is elevated at {vix:.2f}."})

    if _liquidity_descriptor(snapshot) == "TIGHTENING":
        warnings.append({"title": "Tight liquidity", "detail": "Liquidity conditions remain restrictive."})

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for warning in warnings:
        if warning["title"] in seen:
            continue
        seen.add(warning["title"])
        deduped.append(warning)
        if len(deduped) == 3:
            break
    return deduped


def build_operator_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Compose the full operator intelligence summary from the raw dashboard snapshot."""
    world_state = interpret_world_state(snapshot)
    market_forces = interpret_market_forces(snapshot)
    relationship_changes = summarize_relationship_breaks(snapshot)
    risk_indicators = generate_risk_indicators(snapshot)
    significant_correlations = _select_significant_correlations(snapshot, experimental_only=False)
    risk_monitor = build_risk_monitor(snapshot)
    experimental_signals = _build_experimental_snapshot(snapshot, world_state)
    return {
        "global_state_line": build_global_state_line(snapshot),
        "market_pulse": build_market_pulse(snapshot),
        "cycle_monitor": build_cycle_monitor(snapshot),
        "cosmic_state_line": build_cosmic_state_line(snapshot, world_state),
        "market_narrative": build_market_narrative(snapshot),
        "market_stress": snapshot.get("market_stress"),
        "breadth_health": snapshot.get("breadth_health"),
        "liquidity_environment": snapshot.get("liquidity_environment"),
        "regime_probabilities": build_regime_probability_summary(snapshot),
        "divergences": build_divergence_summary(snapshot),
        "market_drivers": build_market_drivers(snapshot),
        "relationship_shifts": relationship_changes,
        "risk_monitor": risk_monitor,
        "warning_signals": build_warning_signals(snapshot),
        "world_state": world_state,
        "market_forces": market_forces,
        "relationship_changes": relationship_changes,
        "risk_indicators": risk_indicators,
        "significant_correlations": significant_correlations,
        "experimental_signals": experimental_signals,
        "watchlist": generate_operator_watchlist(snapshot),
    }

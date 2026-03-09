"""Bayesian regime posterior and transition forecast model for QMIS."""

from __future__ import annotations

import json
from typing import Any


BAYESIAN_REGIMES = (
    "LIQUIDITY EXPANSION",
    "LIQUIDITY WITHDRAWAL",
    "RECESSION RISK",
    "STAGFLATION RISK",
    "DISINFLATION",
    "NEUTRAL",
)


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    sanitized = {label: max(0.001, float(weights.get(label, 0.001))) for label in BAYESIAN_REGIMES}
    total = sum(sanitized.values())
    normalized = {label: round(value / total * 100.0, 2) for label, value in sanitized.items()}
    delta = round(100.0 - sum(normalized.values()), 2)
    if abs(delta) > 0:
        leader = max(normalized, key=normalized.get)
        normalized[leader] = round(normalized[leader] + delta, 2)
    return normalized


def _apply_evidence(
    weights: dict[str, float],
    evidence: dict[str, list[str]],
    impacts: dict[str, float],
    reason: str,
) -> None:
    for regime, multiplier in impacts.items():
        weights[regime] = weights.get(regime, 1.0) * float(multiplier)
        evidence.setdefault(regime, []).append(reason)


def _regime_prior(scores: dict[str, int]) -> dict[str, float]:
    inflation_score = int(scores.get("inflation_score", 0))
    growth_score = int(scores.get("growth_score", 0))
    liquidity_score = int(scores.get("liquidity_score", 0))
    risk_score = int(scores.get("risk_score", 0))

    weights = {label: 1.0 for label in BAYESIAN_REGIMES}
    weights["LIQUIDITY EXPANSION"] += max(liquidity_score - 1, 0) * 0.6 + max(growth_score - 1, 0) * 0.2
    weights["LIQUIDITY WITHDRAWAL"] += max(2 - liquidity_score, 0) * 0.9 + risk_score * 0.2
    weights["RECESSION RISK"] += max(2 - growth_score, 0) * 1.0 + risk_score * 0.7
    weights["STAGFLATION RISK"] += inflation_score * 0.8 + max(2 - growth_score, 0) * 0.5
    weights["DISINFLATION"] += max(2 - inflation_score, 0) * 0.8 + max(growth_score - 1, 0) * 0.4 + max(liquidity_score - 1, 0) * 0.3
    weights["NEUTRAL"] += 0.3
    return weights


def update_regime_probabilities(
    signals: dict[str, Any],
    prior: dict[str, float] | None = None,
) -> tuple[dict[str, float], dict[str, list[str]]]:
    scores = dict(signals.get("scores") or {})
    breadth_health = dict(signals.get("breadth_health") or {})
    liquidity_environment = dict(signals.get("liquidity_environment") or {})
    market_stress = dict(signals.get("market_stress") or {})
    macro_pressure = dict(signals.get("macro_pressure") or {})
    predictive_snapshot = dict(signals.get("predictive_snapshot") or {})
    predictive_signals = predictive_snapshot.get("forward_macro_signals") or {}
    if isinstance(predictive_signals, str):
        try:
            predictive_signals = json.loads(predictive_signals)
        except json.JSONDecodeError:
            predictive_signals = {}
    predictive_signals = dict(predictive_signals or {})

    weights = {label: float((prior or _regime_prior(scores)).get(label, 1.0)) for label in BAYESIAN_REGIMES}
    evidence = {label: [] for label in BAYESIAN_REGIMES}

    if str(breadth_health.get("breadth_state", "")).upper() in {"WEAKENING", "FRAGILE"}:
        _apply_evidence(
            weights,
            evidence,
            {"RECESSION RISK": 1.25, "LIQUIDITY WITHDRAWAL": 1.15, "NEUTRAL": 0.95},
            "breadth deterioration",
        )
    if str(liquidity_environment.get("liquidity_state", "")).upper() == "TIGHTENING":
        _apply_evidence(
            weights,
            evidence,
            {"LIQUIDITY WITHDRAWAL": 1.35, "RECESSION RISK": 1.12, "LIQUIDITY EXPANSION": 0.86},
            "liquidity tightening",
        )
    elif str(liquidity_environment.get("liquidity_state", "")).upper() == "EXPANDING":
        _apply_evidence(
            weights,
            evidence,
            {"LIQUIDITY EXPANSION": 1.35, "DISINFLATION": 1.12, "LIQUIDITY WITHDRAWAL": 0.85},
            "liquidity expansion",
        )

    if str(market_stress.get("stress_level", "")).upper() in {"HIGH", "CRITICAL"}:
        _apply_evidence(
            weights,
            evidence,
            {"RECESSION RISK": 1.25, "LIQUIDITY WITHDRAWAL": 1.1, "STAGFLATION RISK": 1.06},
            "market stress elevated",
        )
    elif str(market_stress.get("stress_level", "")).upper() == "LOW":
        _apply_evidence(
            weights,
            evidence,
            {"LIQUIDITY EXPANSION": 1.1, "DISINFLATION": 1.08, "NEUTRAL": 1.04},
            "market stress subdued",
        )

    macro_pressure_level = str(macro_pressure.get("pressure_level", "")).upper()
    if macro_pressure_level in {"SEVERE PRESSURE", "CRISIS CONDITIONS"}:
        _apply_evidence(
            weights,
            evidence,
            {"RECESSION RISK": 1.24, "LIQUIDITY WITHDRAWAL": 1.12, "STAGFLATION RISK": 1.08},
            "macro pressure elevated",
        )
    elif macro_pressure_level == "LOW PRESSURE":
        _apply_evidence(
            weights,
            evidence,
            {"LIQUIDITY EXPANSION": 1.08, "DISINFLATION": 1.06, "NEUTRAL": 1.04},
            "macro pressure subdued",
        )

    predictive_rules = {
        "yield_curve": {
            "Inverted": ({"RECESSION RISK": 1.45, "LIQUIDITY WITHDRAWAL": 1.18, "LIQUIDITY EXPANSION": 0.82}, "yield curve inversion"),
            "Normal": ({"NEUTRAL": 1.04, "LIQUIDITY EXPANSION": 1.06}, "yield curve normal"),
            "Steepening": ({"LIQUIDITY EXPANSION": 1.18, "DISINFLATION": 1.08}, "yield curve steepening"),
        },
        "credit_spreads": {
            "Widening": ({"RECESSION RISK": 1.32, "LIQUIDITY WITHDRAWAL": 1.18, "STAGFLATION RISK": 1.08}, "credit spreads widening"),
            "Narrowing": ({"LIQUIDITY EXPANSION": 1.16, "DISINFLATION": 1.08}, "credit spreads narrowing"),
        },
        "financial_conditions": {
            "Tightening": ({"LIQUIDITY WITHDRAWAL": 1.3, "RECESSION RISK": 1.14}, "financial conditions tightening"),
            "Loosening": ({"LIQUIDITY EXPANSION": 1.2, "DISINFLATION": 1.1}, "financial conditions loosening"),
        },
        "real_rates": {
            "Rising": ({"LIQUIDITY WITHDRAWAL": 1.2, "STAGFLATION RISK": 1.12}, "real rates rising"),
            "Falling": ({"LIQUIDITY EXPANSION": 1.12, "DISINFLATION": 1.1}, "real rates falling"),
        },
        "global_liquidity": {
            "Contracting": ({"LIQUIDITY WITHDRAWAL": 1.42, "RECESSION RISK": 1.18}, "global liquidity contracting"),
            "Expanding": ({"LIQUIDITY EXPANSION": 1.34, "DISINFLATION": 1.12}, "global liquidity expanding"),
        },
        "volatility_term_structure": {
            "Backwardation": ({"RECESSION RISK": 1.18, "LIQUIDITY WITHDRAWAL": 1.1}, "volatility backwardation"),
            "Contango": ({"LIQUIDITY EXPANSION": 1.1, "NEUTRAL": 1.08}, "volatility contango"),
        },
        "manufacturing_momentum": {
            "Weakening": ({"RECESSION RISK": 1.28, "STAGFLATION RISK": 1.08}, "manufacturing weakening"),
            "Improving": ({"LIQUIDITY EXPANSION": 1.16, "DISINFLATION": 1.12}, "manufacturing improving"),
        },
        "leadership_rotation": {
            "Defensive": ({"RECESSION RISK": 1.18, "LIQUIDITY WITHDRAWAL": 1.14}, "defensive leadership"),
            "Cyclical": ({"LIQUIDITY EXPANSION": 1.16, "DISINFLATION": 1.08}, "cyclical leadership"),
        },
        "commodity_pressure": {
            "Inflationary": ({"STAGFLATION RISK": 1.28, "LIQUIDITY WITHDRAWAL": 1.05}, "commodity inflation pressure"),
            "Disinflationary": ({"DISINFLATION": 1.2, "NEUTRAL": 1.06}, "commodity disinflation pressure"),
        },
    }

    for signal_key, state_map in predictive_rules.items():
        state = str((predictive_signals.get(signal_key) or {}).get("state", ""))
        rule = state_map.get(state)
        if rule is None:
            continue
        impacts, reason = rule
        _apply_evidence(weights, evidence, impacts, reason)

    posterior = _normalize(weights)
    evidence = {label: reasons for label, reasons in evidence.items() if reasons}
    return posterior, evidence


def compute_regime_transition_probabilities() -> dict[str, dict[str, float]]:
    return {
        "LIQUIDITY EXPANSION": {
            "LIQUIDITY EXPANSION": 0.45,
            "LIQUIDITY WITHDRAWAL": 0.18,
            "RECESSION RISK": 0.05,
            "STAGFLATION RISK": 0.08,
            "DISINFLATION": 0.14,
            "NEUTRAL": 0.10,
        },
        "LIQUIDITY WITHDRAWAL": {
            "LIQUIDITY EXPANSION": 0.08,
            "LIQUIDITY WITHDRAWAL": 0.24,
            "RECESSION RISK": 0.46,
            "STAGFLATION RISK": 0.11,
            "DISINFLATION": 0.03,
            "NEUTRAL": 0.08,
        },
        "RECESSION RISK": {
            "LIQUIDITY EXPANSION": 0.18,
            "LIQUIDITY WITHDRAWAL": 0.12,
            "RECESSION RISK": 0.44,
            "STAGFLATION RISK": 0.10,
            "DISINFLATION": 0.10,
            "NEUTRAL": 0.06,
        },
        "STAGFLATION RISK": {
            "LIQUIDITY EXPANSION": 0.10,
            "LIQUIDITY WITHDRAWAL": 0.20,
            "RECESSION RISK": 0.18,
            "STAGFLATION RISK": 0.34,
            "DISINFLATION": 0.06,
            "NEUTRAL": 0.12,
        },
        "DISINFLATION": {
            "LIQUIDITY EXPANSION": 0.28,
            "LIQUIDITY WITHDRAWAL": 0.08,
            "RECESSION RISK": 0.08,
            "STAGFLATION RISK": 0.06,
            "DISINFLATION": 0.36,
            "NEUTRAL": 0.14,
        },
        "NEUTRAL": {
            "LIQUIDITY EXPANSION": 0.24,
            "LIQUIDITY WITHDRAWAL": 0.22,
            "RECESSION RISK": 0.12,
            "STAGFLATION RISK": 0.08,
            "DISINFLATION": 0.10,
            "NEUTRAL": 0.24,
        },
    }


def forecast_regime(
    probabilities: dict[str, float],
    horizon_days: int,
    *,
    transition_matrix: dict[str, dict[str, float]] | None = None,
) -> dict[str, Any]:
    matrix = transition_matrix or compute_regime_transition_probabilities()
    steps = max(1, int(round(horizon_days / 30.0)))
    vector = {label: float(probabilities.get(label, 0.0)) / 100.0 for label in BAYESIAN_REGIMES}

    for _ in range(steps):
        next_vector = {label: 0.0 for label in BAYESIAN_REGIMES}
        for from_regime, from_probability in vector.items():
            for to_regime, transition_probability in matrix[from_regime].items():
                next_vector[to_regime] += from_probability * transition_probability
        vector = next_vector

    distribution = _normalize({label: value for label, value in vector.items()})
    top_regime = max(distribution, key=distribution.get)
    return {
        "horizon_days": int(horizon_days),
        "top_regime": top_regime,
        "probability": float(distribution[top_regime]),
        "distribution": distribution,
    }


def build_forward_regime_forecast(probabilities: dict[str, float]) -> dict[str, dict[str, Any]]:
    matrix = compute_regime_transition_probabilities()
    return {
        "30d": forecast_regime(probabilities, 30, transition_matrix=matrix),
        "90d": forecast_regime(probabilities, 90, transition_matrix=matrix),
        "180d": forecast_regime(probabilities, 180, transition_matrix=matrix),
    }

"""Deterministic market narrative generation from structured QMIS outputs."""

from __future__ import annotations

from typing import Any


def _headline_factor(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    factors = [
        dict(row)
        for row in (snapshot.get("factors") or [])
        if bool(row.get("passes_filter", True))
    ]
    if not factors:
        return None
    ordered = sorted(factors, key=lambda row: (-float(row.get("strength", 0.0)), int(row.get("component_rank", 99))))
    return ordered[0]


def _top_divergence(snapshot: dict[str, Any]) -> dict[str, Any] | None:
    divergences = [
        dict(row)
        for row in (snapshot.get("divergences") or [])
        if bool(row.get("passes_filter", True))
    ]
    if not divergences:
        return None
    ordered = sorted(divergences, key=lambda row: -float(row.get("strength", 0.0)))
    return ordered[0]


def _factor_sentence(snapshot: dict[str, Any], factor: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    factor_name = str(factor.get("factor_name", "")).lower()
    direction = str(factor.get("direction", "")).lower()
    liquidity = snapshot.get("liquidity_environment") or {}
    regime = snapshot.get("regime") or {}

    if factor_name == "liquidity":
        state = str(liquidity.get("liquidity_state") or direction).lower()
        sentence = (
            f"Markets are trading in a liquidity {state} regime, consistent with "
            f"{str(regime.get('regime_label', 'the current regime')).lower()} conditions."
        )
    elif factor_name == "crypto":
        sentence = "Crypto markets remain in a crypto-specific cycle led by tightly linked digital assets."
    elif factor_name == "volatility":
        sentence = "Volatility remains a leading driver as defensive positioning and risk aversion stay elevated."
    else:
        sentence = str(factor.get("summary") or "").strip() or f"{factor_name.title()} remains a leading market driver."

    return sentence, {
        "kind": "factor",
        "factor_name": factor_name,
        "direction": direction,
        "summary": str(factor.get("summary") or ""),
    }


def _divergence_sentence(divergence: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    title = str(divergence.get("title", "")).strip()
    summary = str(divergence.get("summary", "")).strip()
    if title == "Crypto Decoupling From Liquidity":
        sentence = "Crypto appears to be trading independently of macro liquidity, suggesting a crypto-specific cycle."
    elif title == "Gold Rising With Yields":
        sentence = "Gold rising with yields points to a macro pricing break that usually accompanies a more defensive regime shift."
    elif title:
        sentence = summary or f"{title} is a live cross-market divergence."
    else:
        sentence = summary
    return sentence, {
        "kind": "divergence",
        "title": title,
        "summary": summary,
    }


def _risk_sentence(snapshot: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    stress = snapshot.get("market_stress") or {}
    breadth = snapshot.get("breadth_health") or {}
    stress_level = str(stress.get("stress_level", "")).upper()
    breadth_state = str(breadth.get("breadth_state", "")).upper()

    if stress_level and breadth_state:
        sentence = f"Risk conditions remain {stress_level.lower()} while breadth is {breadth_state.lower()}, so participation should be monitored closely."
    elif stress_level:
        sentence = f"Risk conditions remain {stress_level.lower()} based on the current market stress snapshot."
    elif breadth_state:
        sentence = f"Breadth is {breadth_state.lower()}, keeping market participation in focus."
    else:
        sentence = "Market conditions remain mixed across the current operator snapshot."

    return sentence, {
        "kind": "risk",
        "stress_level": stress_level,
        "breadth_state": breadth_state,
        "summary": str(stress.get("summary") or breadth.get("summary") or ""),
    }


def build_market_narrative(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Build a short, evidence-traceable market narrative from structured outputs."""
    sentences: list[str] = []
    evidence: list[dict[str, Any]] = []

    factor = _headline_factor(snapshot)
    if factor:
        sentence, factor_evidence = _factor_sentence(snapshot, factor)
        if sentence:
            sentences.append(sentence)
            evidence.append(factor_evidence)

    divergence = _top_divergence(snapshot)
    if divergence:
        sentence, divergence_evidence = _divergence_sentence(divergence)
        if sentence:
            sentences.append(sentence)
            evidence.append(divergence_evidence)

    if len(sentences) < 3:
        sentence, risk_evidence = _risk_sentence(snapshot)
        if sentence:
            sentences.append(sentence)
            evidence.append(risk_evidence)

    sentences = [sentence.strip() for sentence in sentences if sentence and sentence.strip()]
    return {
        "text": " ".join(sentences[:3]),
        "sentences": sentences[:3],
        "evidence": evidence[:3],
    }

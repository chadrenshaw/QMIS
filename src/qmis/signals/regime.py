"""Regime selection and persistence for the QMIS macro engine."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.scoring import compute_macro_scores
from qmis.storage import connect_db, get_default_db_path


SUPPORTED_REGIMES = (
    "CRISIS / RISK-OFF",
    "INFLATIONARY EXPANSION",
    "DISINFLATION",
    "RECESSION RISK",
    "LIQUIDITY EXPANSION",
    "LIQUIDITY WITHDRAWAL",
    "SPECULATIVE BUBBLE",
    "NEUTRAL",
    "STAGFLATION RISK",
)


def determine_regime(scores: dict[str, int]) -> tuple[str, float]:
    """Map macro scores to a spec-defined regime label and bounded confidence."""
    inflation_score = int(scores["inflation_score"])
    growth_score = int(scores["growth_score"])
    liquidity_score = int(scores["liquidity_score"])
    risk_score = int(scores["risk_score"])

    if risk_score >= 3:
        regime_label = "CRISIS / RISK-OFF"
        confidence = min(1.0, 0.7 + 0.1 * (risk_score - 3))
    elif growth_score >= 3 and liquidity_score >= 3 and risk_score == 0:
        regime_label = "SPECULATIVE BUBBLE"
        confidence = min(1.0, 0.55 + 0.08 * (growth_score + liquidity_score))
    elif inflation_score >= 2 and growth_score <= 1 and risk_score >= 1:
        regime_label = "STAGFLATION RISK"
        confidence = min(1.0, 0.48 + 0.08 * (inflation_score + risk_score))
    elif inflation_score >= 2 and growth_score >= 2 and liquidity_score >= 2 and risk_score <= 1:
        regime_label = "INFLATIONARY EXPANSION"
        confidence = min(1.0, 0.5 + 0.08 * (inflation_score + growth_score + liquidity_score))
    elif inflation_score == 0 and growth_score >= 2 and liquidity_score >= 2 and risk_score <= 1:
        regime_label = "DISINFLATION"
        confidence = min(1.0, 0.45 + 0.1 * (growth_score + liquidity_score))
    elif growth_score <= 1 and risk_score >= 2:
        regime_label = "RECESSION RISK"
        confidence = min(1.0, 0.5 + 0.12 * risk_score)
    elif liquidity_score >= 3 and risk_score <= 1:
        regime_label = "LIQUIDITY EXPANSION"
        confidence = min(1.0, 0.45 + 0.1 * liquidity_score)
    elif liquidity_score <= 1:
        regime_label = "LIQUIDITY WITHDRAWAL"
        confidence = min(1.0, 0.35 + 0.08 * (2 - min(liquidity_score, 2)) + 0.06 * risk_score)
    else:
        regime_label = "NEUTRAL"
        confidence = 0.4

    return regime_label, max(0.0, min(1.0, confidence))


def _build_latest_feature_snapshot(connection) -> pd.DataFrame:
    return connection.execute(
        """
        WITH latest_features AS (
            SELECT MAX(ts) AS latest_ts
            FROM features
        )
        SELECT ts, series_name, trend_label
        FROM features
        WHERE ts = (SELECT latest_ts FROM latest_features)
        ORDER BY series_name
        """
    ).fetchdf()


def _build_signal_snapshot(connection, series_names: tuple[str, ...]) -> dict[str, float]:
    if not series_names:
        return {}

    placeholders = ", ".join(["?"] * len(series_names))
    frame = connection.execute(
        f"""
        SELECT series_name, value
        FROM (
            SELECT
                series_name,
                value,
                ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
            FROM signals
            WHERE series_name IN ({placeholders})
        )
        WHERE row_number = 1
        ORDER BY series_name
        """,
        list(series_names),
    ).fetchdf()
    return {str(row["series_name"]): float(row["value"]) for _, row in frame.iterrows()}


def _load_latest_snapshot(connection, table_name: str, columns: str) -> dict[str, Any] | None:
    frame = connection.execute(
        f"""
        SELECT {columns}
        FROM {table_name}
        ORDER BY ts DESC
        LIMIT 1
        """
    ).fetchdf()
    return frame.iloc[0].to_dict() if not frame.empty else None


def _load_factors(connection) -> list[dict[str, Any]]:
    frame = connection.execute(
        """
        SELECT ts, factor_name, component_rank, strength, direction, summary, supporting_assets, loadings
        FROM factors
        ORDER BY component_rank ASC, strength DESC
        """
    ).fetchdf()
    return frame.to_dict("records")


def _normalize_probabilities(raw_scores: dict[str, float]) -> dict[str, float]:
    positive_scores = {label: max(0.01, float(score)) for label, score in raw_scores.items()}
    total = sum(positive_scores.values())
    normalized = {label: round(score / total * 100.0, 2) for label, score in positive_scores.items()}
    rounding_delta = round(100.0 - sum(normalized.values()), 2)
    if abs(rounding_delta) > 0:
        leader = max(normalized, key=normalized.get)
        normalized[leader] = round(normalized[leader] + rounding_delta, 2)
    return normalized


def build_regime_probabilities(
    *,
    scores: dict[str, int],
    breadth_health: dict[str, Any] | None = None,
    liquidity_environment: dict[str, Any] | None = None,
    market_stress: dict[str, Any] | None = None,
    factors: list[dict[str, Any]] | None = None,
    headline_regime: str | None = None,
) -> tuple[dict[str, float], dict[str, list[str]]]:
    """Build a normalized probability distribution across supported regimes."""
    inflation_score = int(scores["inflation_score"])
    growth_score = int(scores["growth_score"])
    liquidity_score = int(scores["liquidity_score"])
    risk_score = int(scores["risk_score"])

    breadth_state = str((breadth_health or {}).get("breadth_state", "")).upper()
    liquidity_state = str((liquidity_environment or {}).get("liquidity_state", "")).upper()
    stress_level = str((market_stress or {}).get("stress_level", "")).upper()

    factors = list(factors or [])
    liquidity_factor = next((factor for factor in factors if str(factor.get("factor_name")) == "liquidity"), None)
    volatility_factor = next((factor for factor in factors if str(factor.get("factor_name")) == "volatility"), None)
    crypto_factor = next((factor for factor in factors if str(factor.get("factor_name")) == "crypto"), None)

    raw_scores = {label: 0.2 for label in SUPPORTED_REGIMES}
    drivers = {label: [] for label in SUPPORTED_REGIMES}

    raw_scores["CRISIS / RISK-OFF"] += risk_score * 1.2
    if stress_level in {"HIGH", "CRITICAL"}:
        raw_scores["CRISIS / RISK-OFF"] += 1.1
        drivers["CRISIS / RISK-OFF"].append(f"market stress {stress_level.lower()}")
    if breadth_state == "FRAGILE":
        raw_scores["CRISIS / RISK-OFF"] += 0.9
        drivers["CRISIS / RISK-OFF"].append("fragile breadth")
    if volatility_factor and str(volatility_factor.get("direction", "")).lower() == "stressed":
        raw_scores["CRISIS / RISK-OFF"] += 0.8
        drivers["CRISIS / RISK-OFF"].append("stressed volatility factor")

    raw_scores["INFLATIONARY EXPANSION"] += inflation_score * 0.8 + growth_score * 0.8 + liquidity_score * 0.7
    if breadth_state == "STRONG":
        raw_scores["INFLATIONARY EXPANSION"] += 0.7
        drivers["INFLATIONARY EXPANSION"].append("strong breadth")
    if liquidity_state == "EXPANDING":
        raw_scores["INFLATIONARY EXPANSION"] += 0.7
        drivers["INFLATIONARY EXPANSION"].append("expanding liquidity")
    raw_scores["INFLATIONARY EXPANSION"] -= risk_score * 0.4

    raw_scores["DISINFLATION"] += max(0, 2 - inflation_score) * 1.1 + growth_score * 0.5 + liquidity_score * 0.4
    if inflation_score <= 1:
        drivers["DISINFLATION"].append("contained inflation")
    raw_scores["DISINFLATION"] -= risk_score * 0.3

    raw_scores["RECESSION RISK"] += max(0, 2 - growth_score) * 1.3 + risk_score * 1.0
    if breadth_state in {"WEAKENING", "FRAGILE"}:
        raw_scores["RECESSION RISK"] += 0.8
        drivers["RECESSION RISK"].append(f"{breadth_state.lower()} breadth")
    if liquidity_state == "TIGHTENING":
        raw_scores["RECESSION RISK"] += 0.8
        drivers["RECESSION RISK"].append("tightening liquidity")
    if stress_level in {"HIGH", "CRITICAL"}:
        raw_scores["RECESSION RISK"] += 0.7
        drivers["RECESSION RISK"].append("elevated stress")

    raw_scores["LIQUIDITY EXPANSION"] += liquidity_score * 0.9
    if liquidity_state == "EXPANDING":
        raw_scores["LIQUIDITY EXPANSION"] += 1.2
        drivers["LIQUIDITY EXPANSION"].append("liquidity composite expanding")
    if liquidity_factor and str(liquidity_factor.get("direction", "")).lower() == "expanding":
        raw_scores["LIQUIDITY EXPANSION"] += float(liquidity_factor.get("strength", 0.0))
        drivers["LIQUIDITY EXPANSION"].append("expanding liquidity factor")
    raw_scores["LIQUIDITY EXPANSION"] -= risk_score * 0.3

    raw_scores["LIQUIDITY WITHDRAWAL"] += max(0, 2 - liquidity_score) * 1.1 + risk_score * 0.4
    if liquidity_state == "TIGHTENING":
        raw_scores["LIQUIDITY WITHDRAWAL"] += 1.3
        drivers["LIQUIDITY WITHDRAWAL"].append("liquidity composite tightening")
    if liquidity_factor and str(liquidity_factor.get("direction", "")).lower() == "tightening":
        raw_scores["LIQUIDITY WITHDRAWAL"] += float(liquidity_factor.get("strength", 0.0))
        drivers["LIQUIDITY WITHDRAWAL"].append("tightening liquidity factor")

    raw_scores["SPECULATIVE BUBBLE"] += growth_score * 1.0 + liquidity_score * 0.8 + max(0, 1 - risk_score) * 0.9
    if breadth_state == "STRONG":
        raw_scores["SPECULATIVE BUBBLE"] += 0.8
        drivers["SPECULATIVE BUBBLE"].append("strong breadth participation")
    if liquidity_state == "EXPANDING":
        raw_scores["SPECULATIVE BUBBLE"] += 0.9
        drivers["SPECULATIVE BUBBLE"].append("easy liquidity conditions")
    if crypto_factor and str(crypto_factor.get("direction", "")).lower() == "bullish":
        raw_scores["SPECULATIVE BUBBLE"] += 0.5
        drivers["SPECULATIVE BUBBLE"].append("bullish crypto factor")

    raw_scores["NEUTRAL"] += 1.0
    if breadth_state == "WEAKENING":
        raw_scores["NEUTRAL"] += 0.4
        drivers["NEUTRAL"].append("mixed breadth")
    if liquidity_state == "NEUTRAL":
        raw_scores["NEUTRAL"] += 0.4
        drivers["NEUTRAL"].append("neutral liquidity")
    if stress_level in {"LOW", "MODERATE"}:
        raw_scores["NEUTRAL"] += 0.2

    raw_scores["STAGFLATION RISK"] += inflation_score * 1.0 + max(0, 2 - growth_score) * 1.0 + risk_score * 0.7
    if liquidity_state == "TIGHTENING":
        raw_scores["STAGFLATION RISK"] += 0.6
        drivers["STAGFLATION RISK"].append("tight liquidity backdrop")
    if inflation_score >= 2:
        drivers["STAGFLATION RISK"].append("inflation remains elevated")

    if headline_regime:
        raw_scores[headline_regime] = raw_scores.get(headline_regime, 0.2) + 0.75
        drivers.setdefault(headline_regime, []).append("headline rule match")

    probabilities = _normalize_probabilities(raw_scores)
    drivers = {label: reasons for label, reasons in drivers.items() if reasons}
    return probabilities, drivers


def materialize_regime(db_path: Path | None = None) -> int:
    """Recompute and replace the current macro regime snapshot."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        feature_snapshot = _build_latest_feature_snapshot(connection)
        connection.execute("DELETE FROM regimes")
        if feature_snapshot.empty:
            return 0

        latest_ts = pd.to_datetime(feature_snapshot["ts"]).max()
        signal_snapshot = _build_signal_snapshot(connection, ("yield_10y", "yield_3m"))
        scores = compute_macro_scores(feature_snapshot, signal_snapshot)
        score_payload = {
            "inflation_score": int(scores["inflation_score"]),
            "growth_score": int(scores["growth_score"]),
            "liquidity_score": int(scores["liquidity_score"]),
            "risk_score": int(scores["risk_score"]),
        }
        regime_label, _ = determine_regime(score_payload)
        regime_probabilities, regime_drivers = build_regime_probabilities(
            scores=score_payload,
            breadth_health=_load_latest_snapshot(
                connection,
                "breadth_snapshots",
                "ts, breadth_score, breadth_state, summary, components, missing_inputs",
            ),
            liquidity_environment=_load_latest_snapshot(
                connection,
                "liquidity_snapshots",
                "ts, liquidity_score, liquidity_state, summary, components, missing_inputs",
            ),
            market_stress=_load_latest_snapshot(
                connection,
                "stress_snapshots",
                "ts, stress_score, stress_level, summary, components, missing_inputs",
            ),
            factors=_load_factors(connection),
            headline_regime=regime_label,
        )
        confidence = max(regime_probabilities.values()) / 100.0 if regime_probabilities else 0.0

        payload = pd.DataFrame(
            [
                {
                    "ts": latest_ts,
                    "inflation_score": score_payload["inflation_score"],
                    "growth_score": score_payload["growth_score"],
                    "liquidity_score": score_payload["liquidity_score"],
                    "risk_score": score_payload["risk_score"],
                    "regime_label": regime_label,
                    "confidence": float(confidence),
                    "regime_probabilities": json.dumps(regime_probabilities, sort_keys=True),
                    "regime_drivers": json.dumps(regime_drivers, sort_keys=True),
                }
            ]
        )

        connection.register("regimes_df", payload)
        connection.execute(
            """
            INSERT INTO regimes (
                ts,
                inflation_score,
                growth_score,
                liquidity_score,
                risk_score,
                regime_label,
                confidence,
                regime_probabilities,
                regime_drivers
            )
            SELECT
                ts,
                inflation_score,
                growth_score,
                liquidity_score,
                risk_score,
                regime_label,
                confidence,
                regime_probabilities,
                regime_drivers
            FROM regimes_df
            """
        )
        connection.unregister("regimes_df")
    return 1

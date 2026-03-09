"""Regime selection and persistence for the QMIS macro engine."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.scoring import compute_macro_scores
from qmis.storage import connect_db, get_default_db_path


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
        regime_label, confidence = determine_regime(
            {
                "inflation_score": int(scores["inflation_score"]),
                "growth_score": int(scores["growth_score"]),
                "liquidity_score": int(scores["liquidity_score"]),
                "risk_score": int(scores["risk_score"]),
            }
        )

        payload = pd.DataFrame(
            [
                {
                    "ts": latest_ts,
                    "inflation_score": int(scores["inflation_score"]),
                    "growth_score": int(scores["growth_score"]),
                    "liquidity_score": int(scores["liquidity_score"]),
                    "risk_score": int(scores["risk_score"]),
                    "regime_label": regime_label,
                    "confidence": float(confidence),
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
                confidence
            )
            SELECT
                ts,
                inflation_score,
                growth_score,
                liquidity_score,
                risk_score,
                regime_label,
                confidence
            FROM regimes_df
            """
        )
        connection.unregister("regimes_df")
    return 1

"""Regime selection and persistence for the QMIS macro engine."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.models.bayesian_regime import (
    build_forward_regime_forecast,
    update_regime_probabilities,
)
from qmis.schema import bootstrap_database
from qmis.signals.macro_pressure import materialize_macro_pressure
from qmis.signals.predictive import materialize_predictive_signals
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

SCORING_FEATURE_SERIES = (
    "gold",
    "oil",
    "yield_10y",
    "copper",
    "sp500",
    "pmi",
    "fed_balance_sheet",
    "m2_money_supply",
    "reverse_repo_usage",
    "dollar_index",
    "vix",
)

SCORING_SIGNAL_SERIES = (
    "yield_10y",
    "yield_3m",
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
        SELECT ts, series_name, trend_label
        FROM (
            SELECT
                ts,
                series_name,
                trend_label,
                ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
            FROM features
        )
        WHERE row_number = 1
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


def _build_historical_regime_payload(connection, *, latest_ts: pd.Timestamp) -> pd.DataFrame:
    latest_day = latest_ts.normalize()
    feature_placeholders = ", ".join(["?"] * len(SCORING_FEATURE_SERIES))
    signal_placeholders = ", ".join(["?"] * len(SCORING_SIGNAL_SERIES))

    feature_history = connection.execute(
        f"""
        SELECT snapshot_day, series_name, trend_label
        FROM (
            SELECT
                CAST(ts AS DATE) AS snapshot_day,
                series_name,
                trend_label,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(ts AS DATE), series_name
                    ORDER BY ts DESC
                ) AS row_number
            FROM features
            WHERE series_name IN ({feature_placeholders})
        )
        WHERE row_number = 1
        ORDER BY snapshot_day ASC, series_name ASC
        """,
        list(SCORING_FEATURE_SERIES),
    ).fetchdf()
    if feature_history.empty:
        return pd.DataFrame()

    signal_history = connection.execute(
        f"""
        SELECT snapshot_day, series_name, value
        FROM (
            SELECT
                CAST(ts AS DATE) AS snapshot_day,
                series_name,
                value,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST(ts AS DATE), series_name
                    ORDER BY ts DESC
                ) AS row_number
            FROM signals
            WHERE series_name IN ({signal_placeholders})
        )
        WHERE row_number = 1
        ORDER BY snapshot_day ASC, series_name ASC
        """,
        list(SCORING_SIGNAL_SERIES),
    ).fetchdf()
    existing_regime_days = connection.execute(
        """
        SELECT DISTINCT CAST(ts AS DATE) AS snapshot_day
        FROM regimes
        """
    ).fetchdf()

    feature_history["snapshot_day"] = pd.to_datetime(feature_history["snapshot_day"])
    signal_history["snapshot_day"] = pd.to_datetime(signal_history["snapshot_day"])
    existing_days = {
        pd.Timestamp(day).normalize()
        for day in existing_regime_days.get("snapshot_day", pd.Series(dtype="datetime64[ns]")).tolist()
        if day is not None
    }
    timeline = sorted(
        {
            *feature_history["snapshot_day"].tolist(),
            *signal_history["snapshot_day"].tolist(),
        }
    )
    timeline = [day for day in timeline if day < latest_day and day.normalize() not in existing_days]
    if not timeline:
        return pd.DataFrame()

    trend_panel = (
        feature_history.pivot(index="snapshot_day", columns="series_name", values="trend_label")
        .sort_index()
        .reindex(timeline)
        .ffill()
    )
    signal_panel = (
        signal_history.pivot(index="snapshot_day", columns="series_name", values="value")
        .sort_index()
        .reindex(timeline)
        .ffill()
    )

    def trend_flag(series_name: str, trend_label: str) -> pd.Series:
        if series_name not in trend_panel:
            return pd.Series(0, index=trend_panel.index, dtype="int64")
        return trend_panel[series_name].eq(trend_label).astype("int64")

    score_frame = pd.DataFrame(index=trend_panel.index)
    score_frame["inflation_score"] = (
        trend_flag("gold", "UP")
        + trend_flag("oil", "UP")
        + trend_flag("yield_10y", "UP")
    )
    score_frame["growth_score"] = (
        trend_flag("copper", "UP")
        + trend_flag("sp500", "UP")
        + trend_flag("pmi", "UP")
    )
    score_frame["liquidity_score"] = (
        trend_flag("fed_balance_sheet", "UP")
        + trend_flag("m2_money_supply", "UP")
        + trend_flag("reverse_repo_usage", "DOWN")
        + trend_flag("dollar_index", "DOWN")
    )

    if {"yield_10y", "yield_3m"}.issubset(signal_panel.columns):
        inverted_curve = signal_panel["yield_10y"].sub(signal_panel["yield_3m"]).le(0).fillna(False)
    else:
        inverted_curve = pd.Series(False, index=trend_panel.index)
    score_frame["risk_score"] = (
        trend_flag("vix", "UP")
        + trend_flag("sp500", "DOWN")
        + inverted_curve.astype("int64")
    )

    empty_json = json.dumps({}, sort_keys=True)
    records: list[dict[str, Any]] = []
    for snapshot_day, row in score_frame.iterrows():
        score_payload = {
            "inflation_score": int(row["inflation_score"]),
            "growth_score": int(row["growth_score"]),
            "liquidity_score": int(row["liquidity_score"]),
            "risk_score": int(row["risk_score"]),
        }
        regime_label, confidence = determine_regime(score_payload)
        records.append(
            {
                "ts": snapshot_day,
                **score_payload,
                "regime_label": regime_label,
                "confidence": float(confidence),
                "regime_probabilities": empty_json,
                "regime_drivers": empty_json,
                "bayesian_evidence": empty_json,
                "forward_regime_forecast": empty_json,
            }
        )
    return pd.DataFrame.from_records(records)


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
    macro_pressure: dict[str, Any] | None = None,
    factors: list[dict[str, Any]] | None = None,
    predictive_snapshot: dict[str, Any] | None = None,
    headline_regime: str | None = None,
) -> tuple[dict[str, float], dict[str, list[str]]]:
    """Build Bayesian posterior regime probabilities from the latest signal stack."""
    del factors
    del headline_regime
    posterior, evidence = update_regime_probabilities(
        {
            "scores": scores,
            "breadth_health": breadth_health,
            "liquidity_environment": liquidity_environment,
            "market_stress": market_stress,
            "macro_pressure": macro_pressure,
            "predictive_snapshot": predictive_snapshot,
        }
    )
    return posterior, evidence


def materialize_regime(db_path: Path | None = None) -> int:
    """Recompute and replace the current macro regime snapshot."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    materialize_predictive_signals(db_path=target_path)
    materialize_macro_pressure(db_path=target_path)
    with connect_db(target_path) as connection:
        feature_snapshot = _build_latest_feature_snapshot(connection)
        if feature_snapshot.empty:
            return 0

        latest_ts = pd.to_datetime(feature_snapshot["ts"]).max()
        historical_payload = _build_historical_regime_payload(connection, latest_ts=latest_ts)
        if not historical_payload.empty:
            connection.register("historical_regimes_df", historical_payload)
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
                    regime_drivers,
                    bayesian_evidence,
                    forward_regime_forecast
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
                    regime_drivers,
                    bayesian_evidence,
                    forward_regime_forecast
                FROM historical_regimes_df
                """
            )
            connection.unregister("historical_regimes_df")
        connection.execute("DELETE FROM regimes WHERE ts = ?", [latest_ts])
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
            macro_pressure=_load_latest_snapshot(
                connection,
                "macro_pressure_snapshots",
                "ts, mpi_score, pressure_level, summary, components, primary_contributors, missing_inputs",
            ),
            factors=_load_factors(connection),
            predictive_snapshot=_load_latest_snapshot(
                connection,
                "predictive_snapshots",
                "ts, summary, forward_macro_signals, missing_inputs",
            ),
            headline_regime=regime_label,
        )
        forward_regime_forecast = build_forward_regime_forecast(regime_probabilities)
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
                    "bayesian_evidence": json.dumps(regime_drivers, sort_keys=True),
                    "forward_regime_forecast": json.dumps(forward_regime_forecast, sort_keys=True),
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
                regime_drivers,
                bayesian_evidence,
                forward_regime_forecast
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
                regime_drivers,
                bayesian_evidence,
                forward_regime_forecast
            FROM regimes_df
            """
        )
        connection.unregister("regimes_df")
    return 1

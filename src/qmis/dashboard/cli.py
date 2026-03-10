"""Rich CLI dashboard for QMIS derived outputs."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qmis.alerts.engine import load_alert_snapshot
from qmis.schema import bootstrap_database
from qmis.signals.anomalies import detect_relationship_anomalies
from qmis.signals.divergence import detect_cross_market_divergences
from qmis.signals.interpreter import build_operator_snapshot
from qmis.signals.persistence import annotate_factor_persistence
from qmis.signals.scoring import compute_macro_scores
from qmis.storage import connect_db, get_default_db_path


DASHBOARD_TRENDS = (
    ("gold", "Gold Trend"),
    ("oil", "Oil Trend"),
    ("copper", "Copper Trend"),
    ("BTCUSD", "BTC Trend"),
)

DASHBOARD_HISTORY_SERIES = (
    "gold",
    "oil",
    "BTCUSD",
    "yield_10y",
    "yield_3m",
    "fed_balance_sheet",
    "dollar_index",
)

SIGNAL_GROUP_ORDER = ("market", "breadth", "macro", "liquidity", "crypto", "astronomy", "natural")
SCORE_HISTORY_LIMIT = 365
SCORE_FEATURE_SERIES = (
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
SCORE_SIGNAL_SERIES = ("yield_10y", "yield_3m")

CATEGORY_TITLES = {
    "market": "Market Signals",
    "breadth": "Breadth Signals",
    "macro": "Macro Signals",
    "liquidity": "Liquidity Signals",
    "crypto": "Crypto Signals",
    "astronomy": "Astronomy Signals",
    "natural": "Natural Signals",
}


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return parsed
    return []


def _titleize_signal_key(key: str) -> str:
    return key.replace("_", " ").title()


def _latest_rows(connection, table_name: str, value_columns: str = "*"):
    return connection.execute(
        f"""
        SELECT {value_columns}
        FROM (
            SELECT
                {value_columns},
                ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
            FROM {table_name}
        )
        WHERE row_number = 1
        """
    ).fetchdf()


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return None


def _build_freshness(
    latest_signal_ts: Any,
    latest_regime_ts: Any,
    latest_breadth_ts: Any,
    latest_liquidity_ts: Any,
    latest_relationship_ts: Any,
    latest_stress_ts: Any,
    latest_macro_pressure_ts: Any,
    latest_cycle_ts: Any,
) -> dict[str, Any]:
    timestamps = [
        ts for ts in (
            _coerce_timestamp(latest_signal_ts),
            _coerce_timestamp(latest_regime_ts),
            _coerce_timestamp(latest_breadth_ts),
            _coerce_timestamp(latest_liquidity_ts),
            _coerce_timestamp(latest_relationship_ts),
            _coerce_timestamp(latest_stress_ts),
            _coerce_timestamp(latest_macro_pressure_ts),
            _coerce_timestamp(latest_cycle_ts),
        )
        if ts is not None
    ]
    if not timestamps:
        return {
            "status": "empty",
            "latest_signal_ts": latest_signal_ts,
            "latest_regime_ts": latest_regime_ts,
            "latest_breadth_ts": latest_breadth_ts,
            "latest_liquidity_ts": latest_liquidity_ts,
            "latest_relationship_ts": latest_relationship_ts,
            "latest_stress_ts": latest_stress_ts,
            "latest_macro_pressure_ts": latest_macro_pressure_ts,
            "latest_cycle_ts": latest_cycle_ts,
        }

    newest = max(timestamps)
    age_days = (datetime.now(UTC) - newest).days
    status = "fresh" if age_days <= 5 else "stale"
    return {
        "status": status,
        "latest_signal_ts": latest_signal_ts,
        "latest_regime_ts": latest_regime_ts,
        "latest_breadth_ts": latest_breadth_ts,
        "latest_liquidity_ts": latest_liquidity_ts,
        "latest_relationship_ts": latest_relationship_ts,
        "latest_stress_ts": latest_stress_ts,
        "latest_macro_pressure_ts": latest_macro_pressure_ts,
        "latest_cycle_ts": latest_cycle_ts,
        "age_days": age_days,
    }


def _compute_current_score_stack(
    feature_rows: pd.DataFrame,
    signal_summary: dict[str, dict[str, Any]],
) -> dict[str, int]:
    signal_snapshot = {
        series_name: signal_summary.get(series_name, {}).get("value")
        for series_name in SCORE_SIGNAL_SERIES
    }
    scores = compute_macro_scores(feature_rows, signal_snapshot)
    return {
        "inflation_score": int(scores["inflation_score"]),
        "growth_score": int(scores["growth_score"]),
        "liquidity_score": int(scores["liquidity_score"]),
        "risk_score": int(scores["risk_score"]),
    }


def _build_score_history_from_features(
    feature_history_rows: pd.DataFrame,
    signal_history_rows: pd.DataFrame,
) -> list[dict[str, Any]]:
    if feature_history_rows.empty:
        return []

    feature_history = feature_history_rows.loc[
        feature_history_rows["series_name"].isin(SCORE_FEATURE_SERIES)
    ].copy()
    if feature_history.empty:
        return []
    feature_history["snapshot_day"] = pd.to_datetime(feature_history["ts"]).dt.normalize()
    feature_history = feature_history.sort_values(["snapshot_day", "series_name", "ts"])
    feature_history = feature_history.drop_duplicates(["snapshot_day", "series_name"], keep="last")

    signal_history = signal_history_rows.loc[
        signal_history_rows["series_name"].isin(SCORE_SIGNAL_SERIES)
    ].copy()
    if not signal_history.empty:
        signal_history["snapshot_day"] = pd.to_datetime(signal_history["ts"]).dt.normalize()
        signal_history = signal_history.sort_values(["snapshot_day", "series_name", "ts"])
        signal_history = signal_history.drop_duplicates(["snapshot_day", "series_name"], keep="last")

    timeline = sorted(
        {
            *feature_history["snapshot_day"].tolist(),
            *signal_history.get("snapshot_day", pd.Series(dtype="datetime64[ns]")).tolist(),
        }
    )
    if not timeline:
        return []

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
        if not signal_history.empty
        else pd.DataFrame(index=timeline)
    )

    def trend_flag(series_name: str, expected_label: str) -> pd.Series:
        if series_name not in trend_panel:
            return pd.Series(0, index=trend_panel.index, dtype="int64")
        return trend_panel[series_name].eq(expected_label).astype("int64")

    if {"yield_10y", "yield_3m"}.issubset(signal_panel.columns):
        inverted_curve = signal_panel["yield_10y"].sub(signal_panel["yield_3m"]).le(0).fillna(False)
    else:
        inverted_curve = pd.Series(False, index=trend_panel.index)

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
    score_frame["risk_score"] = (
        trend_flag("vix", "UP")
        + trend_flag("sp500", "DOWN")
        + inverted_curve.astype("int64")
    )

    return [
        {
            "ts": snapshot_day,
            "inflation_score": int(row["inflation_score"]),
            "growth_score": int(row["growth_score"]),
            "liquidity_score": int(row["liquidity_score"]),
            "risk_score": int(row["risk_score"]),
            "regime_label": "",
            "confidence": 0.0,
            "regime_probabilities": {},
            "regime_drivers": {},
            "bayesian_evidence": {},
            "forward_regime_forecast": {},
        }
        for snapshot_day, row in score_frame.iterrows()
    ][-SCORE_HISTORY_LIMIT:]


def load_dashboard_snapshot(db_path: Path | None = None) -> dict[str, Any]:
    """Load the latest derived dashboard state from DuckDB."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        feature_rows = connection.execute(
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
            """
        ).fetchdf()
        signal_rows = connection.execute(
            """
            SELECT ts, series_name, value, unit, category, source, metadata
            FROM (
                SELECT
                    ts,
                    series_name,
                    value,
                    unit,
                    category,
                    source,
                    metadata,
                    ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
                FROM signals
            )
            WHERE row_number = 1
            """
        ).fetchdf()
        regime_rows = connection.execute(
            """
            SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence,
                   regime_probabilities, regime_drivers, bayesian_evidence, forward_regime_forecast
            FROM regimes
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        factor_rows = connection.execute(
            """
            SELECT ts, factor_name, component_rank, strength, direction, summary, supporting_assets, loadings
            FROM factors
            ORDER BY component_rank ASC, strength DESC
            """
        ).fetchdf()
        stress_rows = connection.execute(
            """
            SELECT ts, stress_score, stress_level, summary, components, missing_inputs
            FROM stress_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        macro_pressure_rows = connection.execute(
            """
            SELECT ts, mpi_score, pressure_level, summary, components, primary_contributors, missing_inputs
            FROM macro_pressure_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        breadth_rows = connection.execute(
            """
            SELECT ts, breadth_score, breadth_state, summary, components, missing_inputs
            FROM breadth_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        liquidity_rows = connection.execute(
            """
            SELECT ts, liquidity_score, liquidity_state, summary, components, missing_inputs
            FROM liquidity_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        predictive_rows = connection.execute(
            """
            SELECT ts, summary, forward_macro_signals, missing_inputs
            FROM predictive_snapshots
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()
        cycle_rows = connection.execute(
            """
            SELECT ts, cycle_name, phase, strength, is_turning_point, transition_from,
                   alert_on_transition, summary, supporting_signals, metadata
            FROM cycle_snapshots
            ORDER BY
                CASE cycle_name
                    WHEN 'solar_cycle' THEN 0
                    WHEN 'lunar_cycle' THEN 1
                    WHEN 'macro_liquidity_cycle' THEN 2
                    ELSE 99
                END,
                cycle_name ASC
            """
        ).fetchdf()
        relationship_rows = connection.execute(
            """
            SELECT
                ts,
                series_x,
                series_y,
                window_days,
                lag_days,
                correlation,
                p_value,
                relationship_state,
                confidence_label
            FROM relationships
            ORDER BY ABS(correlation) DESC, window_days DESC
            """
        ).fetchdf()
        history_series = connection.execute(
            f"""
            SELECT ts, series_name, value, unit
            FROM signals
            WHERE series_name IN ({", ".join(["?"] * len(DASHBOARD_HISTORY_SERIES))})
            ORDER BY series_name, ts ASC
            """,
            list(DASHBOARD_HISTORY_SERIES),
        ).fetchdf()
        historical_feature_rows = connection.execute(
            f"""
            SELECT ts, series_name, trend_label
            FROM features
            WHERE series_name IN ({", ".join(["?"] * len(SCORE_FEATURE_SERIES))})
            ORDER BY ts ASC, series_name ASC
            """,
            list(SCORE_FEATURE_SERIES),
        ).fetchdf()
        historical_score_signals = connection.execute(
            f"""
            SELECT ts, series_name, value
            FROM signals
            WHERE series_name IN ({", ".join(["?"] * len(SCORE_SIGNAL_SERIES))})
            ORDER BY ts ASC, series_name ASC
            """,
            list(SCORE_SIGNAL_SERIES),
        ).fetchdf()
        score_history_rows = connection.execute(
            """
            SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence,
                   regime_probabilities, regime_drivers, bayesian_evidence, forward_regime_forecast
            FROM (
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
                FROM regimes
                ORDER BY ts DESC
                LIMIT ?
            )
            ORDER BY ts ASC
            """,
            [SCORE_HISTORY_LIMIT],
        ).fetchdf()

    trend_summary = {
        str(row["series_name"]): {
            "ts": row["ts"],
            "trend_label": str(row["trend_label"]),
        }
        for _, row in feature_rows.iterrows()
    }
    signal_summary = {
        str(row["series_name"]): {
            "ts": row["ts"],
            "value": float(row["value"]),
            "unit": str(row["unit"]),
            "category": str(row["category"]),
            "source": str(row["source"]),
            "metadata": _parse_metadata(row.get("metadata")),
        }
        for _, row in signal_rows.iterrows()
    }
    factors = [
        {
            "ts": row["ts"],
            "factor_name": str(row["factor_name"]),
            "component_rank": int(row["component_rank"]),
            "strength": float(row["strength"]),
            "direction": str(row["direction"]),
            "summary": str(row["summary"]),
            "supporting_assets": _parse_metadata(row["supporting_assets"]) if isinstance(row["supporting_assets"], dict) else json.loads(row["supporting_assets"] or "[]"),
            "loadings": _parse_metadata(row["loadings"]),
        }
        for _, row in factor_rows.iterrows()
    ]
    factors = annotate_factor_persistence(factors, feature_rows)
    market_stress = (
        {
            "ts": stress_rows.iloc[0]["ts"],
            "stress_score": float(stress_rows.iloc[0]["stress_score"]),
            "stress_level": str(stress_rows.iloc[0]["stress_level"]),
            "summary": str(stress_rows.iloc[0]["summary"]),
            "components": _parse_metadata(stress_rows.iloc[0]["components"]),
            "missing_inputs": _parse_json_list(stress_rows.iloc[0]["missing_inputs"]),
        }
        if not stress_rows.empty
        else None
    )
    macro_pressure = (
        {
            "ts": macro_pressure_rows.iloc[0]["ts"],
            "mpi_score": float(macro_pressure_rows.iloc[0]["mpi_score"]),
            "pressure_level": str(macro_pressure_rows.iloc[0]["pressure_level"]),
            "summary": str(macro_pressure_rows.iloc[0]["summary"]),
            "components": _parse_metadata(macro_pressure_rows.iloc[0]["components"]),
            "primary_contributors": _parse_json_list(macro_pressure_rows.iloc[0]["primary_contributors"]),
            "missing_inputs": _parse_json_list(macro_pressure_rows.iloc[0]["missing_inputs"]),
        }
        if not macro_pressure_rows.empty
        else None
    )
    breadth_health = (
        {
            "ts": breadth_rows.iloc[0]["ts"],
            "breadth_score": float(breadth_rows.iloc[0]["breadth_score"]),
            "breadth_state": str(breadth_rows.iloc[0]["breadth_state"]),
            "summary": str(breadth_rows.iloc[0]["summary"]),
            "components": _parse_metadata(breadth_rows.iloc[0]["components"]),
            "missing_inputs": _parse_json_list(breadth_rows.iloc[0]["missing_inputs"]),
        }
        if not breadth_rows.empty
        else None
    )
    liquidity_environment = (
        {
            "ts": liquidity_rows.iloc[0]["ts"],
            "liquidity_score": float(liquidity_rows.iloc[0]["liquidity_score"]),
            "liquidity_state": str(liquidity_rows.iloc[0]["liquidity_state"]),
            "summary": str(liquidity_rows.iloc[0]["summary"]),
            "components": _parse_metadata(liquidity_rows.iloc[0]["components"]),
            "missing_inputs": _parse_json_list(liquidity_rows.iloc[0]["missing_inputs"]),
        }
        if not liquidity_rows.empty
        else None
    )
    predictive_snapshot = (
        {
            "ts": predictive_rows.iloc[0]["ts"],
            "summary": str(predictive_rows.iloc[0]["summary"]),
            "forward_macro_signals": _parse_metadata(predictive_rows.iloc[0]["forward_macro_signals"]),
            "missing_inputs": _parse_json_list(predictive_rows.iloc[0]["missing_inputs"]),
        }
        if not predictive_rows.empty
        else None
    )
    cycles = [
        {
            "ts": row["ts"],
            "cycle_name": str(row["cycle_name"]),
            "phase": str(row["phase"]),
            "strength": float(row["strength"]),
            "is_turning_point": bool(row["is_turning_point"]),
            "transition_from": str(row["transition_from"]) if row["transition_from"] is not None else None,
            "alert_on_transition": bool(row["alert_on_transition"]),
            "summary": str(row["summary"]),
            "supporting_signals": _parse_json_list(row["supporting_signals"]),
            "metadata": _parse_metadata(row["metadata"]),
        }
        for _, row in cycle_rows.iterrows()
    ]

    latest_regime = regime_rows.iloc[0].to_dict() if not regime_rows.empty else None
    if latest_regime:
        latest_regime["regime_probabilities"] = _parse_metadata(latest_regime.get("regime_probabilities"))
        latest_regime["regime_drivers"] = _parse_metadata(latest_regime.get("regime_drivers"))
        latest_regime["bayesian_evidence"] = _parse_metadata(latest_regime.get("bayesian_evidence"))
        latest_regime["forward_regime_forecast"] = _parse_metadata(latest_regime.get("forward_regime_forecast"))
    persisted_scores = (
        {
            "inflation_score": int(latest_regime["inflation_score"]),
            "growth_score": int(latest_regime["growth_score"]),
            "liquidity_score": int(latest_regime["liquidity_score"]),
            "risk_score": int(latest_regime["risk_score"]),
        }
        if latest_regime
        else {}
    )
    computed_scores = _compute_current_score_stack(feature_rows, signal_summary)

    yield_10y = signal_summary.get("yield_10y", {}).get("value")
    yield_3m = signal_summary.get("yield_3m", {}).get("value")
    yield_curve = None
    yield_curve_state = "UNKNOWN"
    if yield_10y is not None and yield_3m is not None:
        yield_curve = float(yield_10y) - float(yield_3m)
        yield_curve_state = "NORMAL" if yield_curve > 0 else "INVERTED"

    zero_lag = relationship_rows.loc[relationship_rows["lag_days"] == 0].copy()
    lead_lag = relationship_rows.loc[relationship_rows["lag_days"] != 0].copy()
    top_relationships = zero_lag.loc[zero_lag["relationship_state"] != "broken"].head(3).to_dict("records")
    lead_lag_relationships = lead_lag.head(3).to_dict("records")
    anomalies = detect_relationship_anomalies(zero_lag).to_dict("records")
    divergences = detect_cross_market_divergences(relationships=zero_lag, features=feature_rows).to_dict("records")
    signal_history: dict[str, list[dict[str, Any]]] = {}
    for series_name, frame in history_series.groupby("series_name"):
        signal_history[str(series_name)] = [
            {
                "ts": row["ts"],
                "value": float(row["value"]),
                "unit": str(row["unit"]),
            }
            for _, row in frame.iterrows()
        ]

    score_history = [
        {
            "ts": row["ts"],
            "inflation_score": int(row["inflation_score"]),
            "growth_score": int(row["growth_score"]),
            "liquidity_score": int(row["liquidity_score"]),
            "risk_score": int(row["risk_score"]),
            "regime_label": str(row["regime_label"]),
            "confidence": float(row["confidence"]),
            "regime_probabilities": _parse_metadata(row.get("regime_probabilities")),
            "regime_drivers": _parse_metadata(row.get("regime_drivers")),
            "bayesian_evidence": _parse_metadata(row.get("bayesian_evidence")),
            "forward_regime_forecast": _parse_metadata(row.get("forward_regime_forecast")),
        }
        for _, row in score_history_rows.iterrows()
    ]
    derived_score_history = _build_score_history_from_features(
        historical_feature_rows,
        historical_score_signals,
    )
    signal_groups = {
        category: [
            series_name
            for series_name, signal in signal_summary.items()
            if signal.get("category") == category
        ]
        for category in SIGNAL_GROUP_ORDER
        if any(signal.get("category") == category for signal in signal_summary.values())
    }
    grouped_signals = {
        category: [
            {
                "series_name": series_name,
                "trend_label": trend_summary.get(series_name, {}).get("trend_label", "N/A"),
                "value": signal_summary[series_name]["value"],
                "unit": signal_summary[series_name]["unit"],
                "source": signal_summary[series_name]["source"],
                "metadata": signal_summary[series_name]["metadata"],
                "ts": signal_summary[series_name]["ts"],
            }
            for series_name in sorted(signal_groups[category])
        ]
        for category in signal_groups
    }
    freshness = _build_freshness(
        latest_signal_ts=signal_rows["ts"].max() if not signal_rows.empty else None,
        latest_regime_ts=regime_rows["ts"].max() if not regime_rows.empty else None,
        latest_breadth_ts=breadth_rows["ts"].max() if not breadth_rows.empty else None,
        latest_liquidity_ts=liquidity_rows["ts"].max() if not liquidity_rows.empty else None,
        latest_relationship_ts=relationship_rows["ts"].max() if not relationship_rows.empty else None,
        latest_stress_ts=stress_rows["ts"].max() if not stress_rows.empty else None,
        latest_macro_pressure_ts=macro_pressure_rows["ts"].max() if not macro_pressure_rows.empty else None,
        latest_cycle_ts=cycle_rows["ts"].max() if not cycle_rows.empty else None,
    )
    latest_snapshot_ts = max(
        (
            ts
            for ts in (
                signal_rows["ts"].max() if not signal_rows.empty else None,
                regime_rows["ts"].max() if not regime_rows.empty else None,
                breadth_rows["ts"].max() if not breadth_rows.empty else None,
                liquidity_rows["ts"].max() if not liquidity_rows.empty else None,
                relationship_rows["ts"].max() if not relationship_rows.empty else None,
                stress_rows["ts"].max() if not stress_rows.empty else None,
                macro_pressure_rows["ts"].max() if not macro_pressure_rows.empty else None,
                cycle_rows["ts"].max() if not cycle_rows.empty else None,
            )
            if ts is not None
        ),
        default=None,
    )
    alert_rows, alert_summary = load_alert_snapshot(db_path=target_path)

    latest_feature_ts = feature_rows["ts"].max() if not feature_rows.empty else None
    latest_regime_ts = regime_rows["ts"].max() if not regime_rows.empty else None
    scores_are_stale = (
        latest_feature_ts is not None
        and latest_regime_ts is not None
        and pd.to_datetime(latest_regime_ts) < pd.to_datetime(latest_feature_ts)
    )
    scores = computed_scores if (not persisted_scores or scores_are_stale) else persisted_scores
    if len(score_history) < 2 or scores_are_stale:
        score_history = derived_score_history

    snapshot = {
        "trend_summary": trend_summary,
        "signal_summary": signal_summary,
        "signal_groups": signal_groups,
        "grouped_signals": grouped_signals,
        "signal_history": signal_history,
        "scores": scores,
        "score_history": score_history,
        "regime": latest_regime,
        "factors": factors,
        "market_stress": market_stress,
        "macro_pressure": macro_pressure,
        "cycles": cycles,
        "breadth_health": breadth_health,
        "liquidity_environment": liquidity_environment,
        "predictive_snapshot": predictive_snapshot,
        "forward_macro_signals": (predictive_snapshot or {}).get("forward_macro_signals", {}),
        "yield_curve": yield_curve,
        "yield_curve_state": yield_curve_state,
        "freshness": freshness,
        "latest_snapshot_ts": latest_snapshot_ts,
        "relationships": relationship_rows.to_dict("records"),
        "top_relationships": top_relationships,
        "lead_lag_relationships": lead_lag_relationships,
        "anomalies": anomalies,
        "divergences": divergences,
        "alert_summary": alert_summary,
        "alerts": alert_rows.to_dict("records"),
    }
    snapshot["intelligence"] = build_operator_snapshot(snapshot)
    return snapshot


def _build_signal_summary_table(snapshot: dict[str, Any]) -> Table:
    table = Table(title="Signal Summary")
    table.add_column("Signal")
    table.add_column("Trend", justify="center")
    table.add_column("Value", justify="right")

    trend_summary = snapshot["trend_summary"]
    signal_summary = snapshot["signal_summary"]
    for series_name, label in DASHBOARD_TRENDS:
        trend = trend_summary.get(series_name, {}).get("trend_label", "N/A")
        value = signal_summary.get(series_name, {}).get("value")
        display_value = f"{value:,.2f}" if value is not None else "N/A"
        table.add_row(label, trend, display_value)

    vix_value = signal_summary.get("vix", {}).get("value")
    table.add_row("Yield Curve", snapshot["yield_curve_state"], f"{snapshot['yield_curve']:.2f}" if snapshot["yield_curve"] is not None else "N/A")
    table.add_row("VIX", "LEVEL", f"{vix_value:,.2f}" if vix_value is not None else "N/A")
    return table


def _format_signal_value(value: float | None) -> str:
    if value is None:
        return "N/A"
    return f"{float(value):,.2f}"


def _build_category_signal_table(category: str, rows: list[dict[str, Any]]) -> Table:
    table = Table(title=CATEGORY_TITLES.get(category, f"{category.title()} Signals"))
    table.add_column("Signal")
    table.add_column("Trend", justify="center")
    table.add_column("Value", justify="right")
    table.add_column("Unit")
    table.add_column("Source")

    if not rows:
        table.add_row("No data", "-", "-", "-", "-")
        return table

    for row in rows:
        table.add_row(
            str(row["series_name"]),
            str(row["trend_label"]),
            _format_signal_value(row.get("value")),
            str(row["unit"]),
            str(row["source"]),
        )
    return table


def _build_scores_table(snapshot: dict[str, Any]) -> Table:
    table = Table(title="Macro Scores")
    table.add_column("Score")
    table.add_column("Value", justify="right")

    scores = snapshot["scores"]
    for label, key in (
        ("Inflation", "inflation_score"),
        ("Growth", "growth_score"),
        ("Liquidity", "liquidity_score"),
        ("Risk", "risk_score"),
    ):
        value = scores.get(key)
        table.add_row(label, str(value) if value is not None else "N/A")
    return table


def _build_relationships_table(rows: list[dict[str, Any]], title: str) -> Table:
    table = Table(title=title)
    table.add_column("Pair")
    table.add_column("Window", justify="right")
    table.add_column("Lag", justify="right")
    table.add_column("Corr", justify="right")
    table.add_column("State")
    table.add_column("Confidence")

    if not rows:
        table.add_row("No data", "-", "-", "-", "-", "-")
        return table

    for row in rows:
        pair = f"{row['series_x']} vs {row['series_y']}"
        table.add_row(
            pair,
            str(int(row["window_days"])),
            str(int(row["lag_days"])),
            f"{float(row['correlation']):.2f}",
            str(row["relationship_state"]),
            str(row["confidence_label"]),
        )
    return table


def _build_anomalies_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(title="Anomalies")
    table.add_column("Pair")
    table.add_column("Type")
    table.add_column("Historical")
    table.add_column("Current")

    if not rows:
        table.add_row("No anomalies", "-", "-", "-")
        return table

    for row in rows:
        pair = f"{row['series_x']} vs {row['series_y']}"
        table.add_row(pair, str(row["anomaly_type"]), str(row["historical_state"]), str(row["current_state"]))
    return table


def _build_alerts_table(rows: list[dict[str, Any]]) -> Table:
    table = Table(title="Alerts")
    table.add_column("Severity")
    table.add_column("Type")
    table.add_column("Title")
    table.add_column("Message")

    if not rows:
        table.add_row("clear", "-", "No active alerts", "-")
        return table

    for row in rows[:8]:
        table.add_row(
            str(row["severity"]),
            str(row["alert_type"]),
            str(row["title"]),
            str(row["message"]),
        )
    return table


def render_world_snapshot(snapshot: dict[str, Any], console: Console) -> None:
    intelligence = snapshot["intelligence"]
    console.print(Panel(intelligence["global_state_line"], title="GLOBAL STATE", expand=False))


def _render_market_pulse(snapshot: dict[str, Any], console: Console) -> None:
    pulse = snapshot["intelligence"]["market_pulse"]
    line = " | ".join(f"{item['label']}: {item['state']}" for item in pulse)
    console.print(Panel(line, title="MARKET PULSE", expand=False))


def _render_cosmic_state(snapshot: dict[str, Any], console: Console) -> None:
    console.print(Panel(snapshot["intelligence"]["cosmic_state_line"], title="COSMIC STATE", expand=False))


def render_market_narrative(snapshot: dict[str, Any], console: Console) -> None:
    narrative = snapshot["intelligence"].get("market_narrative") or {}
    text = str(narrative.get("text") or "").strip()
    console.print(Panel(text or "No market narrative available.", title="MARKET NARRATIVE", expand=False))


def render_market_stress(snapshot: dict[str, Any], console: Console) -> None:
    stress = snapshot["intelligence"].get("market_stress")
    if not stress:
        console.print(Panel("No market stress snapshot available.", title="MARKET STRESS", expand=False))
        return
    missing_inputs = stress.get("missing_inputs", [])
    missing_text = f"\nMissing inputs: {', '.join(missing_inputs)}" if missing_inputs else ""
    body = f"{stress['stress_level']} ({float(stress['stress_score']):.1f})\n{stress['summary']}{missing_text}"
    console.print(Panel(body, title="MARKET STRESS", expand=False))


def render_macro_pressure(snapshot: dict[str, Any], console: Console) -> None:
    macro_pressure = snapshot["intelligence"].get("macro_pressure")
    if not macro_pressure:
        console.print(Panel("No macro pressure snapshot available.", title="MACRO PRESSURE INDEX", expand=False))
        return
    contributors = macro_pressure.get("primary_contributors", [])
    contributor_text = (
        f"\nPrimary contributors: {', '.join(str(item) for item in contributors)}"
        if contributors
        else ""
    )
    missing_inputs = macro_pressure.get("missing_inputs", [])
    missing_text = f"\nMissing inputs: {', '.join(missing_inputs)}" if missing_inputs else ""
    body = (
        f"{macro_pressure['pressure_level']} ({float(macro_pressure['mpi_score']):.1f})"
        f"{contributor_text}\n{macro_pressure['summary']}{missing_text}"
    )
    console.print(Panel(body, title="MACRO PRESSURE INDEX", expand=False))


def render_cycle_monitor(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"].get("cycle_monitor", [])
    if not rows:
        console.print(Panel("No cycle snapshots available.", title="CYCLE MONITOR", expand=False))
        return
    body = "\n".join(f"- {row['label']}: {row['phase']} | {row['summary']}" for row in rows)
    console.print(Panel(body, title="CYCLE MONITOR", expand=False))


def render_regime_probabilities(snapshot: dict[str, Any], console: Console) -> None:
    regime = snapshot.get("regime") or {}
    probabilities = regime.get("regime_probabilities") or {}
    drivers = regime.get("regime_drivers") or {}
    if not probabilities:
        console.print(Panel("No probabilistic regime snapshot available.", title="REGIME PROBABILITIES", expand=False))
        return
    ordered = sorted(probabilities.items(), key=lambda item: float(item[1]), reverse=True)[:5]
    lines = []
    for label, probability in ordered:
        driver_text = ", ".join(drivers.get(label, [])[:2])
        suffix = f" | {driver_text}" if driver_text else ""
        lines.append(f"- {label}: {float(probability):.2f}%{suffix}")
    console.print(Panel("\n".join(lines), title="REGIME PROBABILITIES", expand=False))


def render_forward_regime_forecast(snapshot: dict[str, Any], console: Console) -> None:
    regime = snapshot.get("regime") or {}
    forecast = regime.get("forward_regime_forecast") or {}
    if not forecast:
        console.print(Panel("No forward regime forecast available.", title="FORWARD REGIME FORECAST", expand=False))
        return

    lines = []
    for horizon in ("30d", "90d", "180d"):
        payload = forecast.get(horizon)
        if not isinstance(payload, dict):
            continue
        top_regime = str(payload.get("top_regime") or "Unknown")
        probability = payload.get("probability")
        display_regime = top_regime.title()
        lines.append(f"{horizon} outlook:")
        lines.append(f"{display_regime} ({float(probability):.0f}%)" if probability is not None else display_regime)
        lines.append("")

    console.print(Panel("\n".join(lines).strip(), title="FORWARD REGIME FORECAST", expand=False))


def render_forward_macro_signals(snapshot: dict[str, Any], console: Console) -> None:
    predictive_snapshot = snapshot.get("predictive_snapshot") or {}
    forward_signals = predictive_snapshot.get("forward_macro_signals") or {}
    if not forward_signals:
        console.print(Panel("No forward macro snapshot available.", title="FORWARD MACRO SIGNALS", expand=False))
        return

    signal_order = (
        "yield_curve",
        "credit_spreads",
        "financial_conditions",
        "real_rates",
        "global_liquidity",
        "manufacturing_momentum",
        "volatility_term_structure",
        "leadership_rotation",
        "commodity_pressure",
    )
    lines = []
    for key in signal_order:
        payload = forward_signals.get(key)
        if not isinstance(payload, dict):
            continue
        state = str(payload.get("state") or "Unavailable")
        summary = str(payload.get("summary") or "")
        brief_summary = summary.split(".")[0].strip()
        suffix = f" ({brief_summary.lower()})" if brief_summary else ""
        lines.append(f"{_titleize_signal_key(key)}: {state}{suffix}")

    missing_inputs = predictive_snapshot.get("missing_inputs") or []
    if missing_inputs:
        lines.append(f"Missing inputs: {', '.join(missing_inputs)}")
    console.print(Panel("\n".join(lines), title="FORWARD MACRO SIGNALS", expand=False))


def render_breadth_health(snapshot: dict[str, Any], console: Console) -> None:
    breadth = snapshot["intelligence"].get("breadth_health")
    if not breadth:
        console.print(Panel("No breadth health snapshot available.", title="BREADTH HEALTH", expand=False))
        return
    missing_inputs = breadth.get("missing_inputs", [])
    missing_text = f"\nMissing inputs: {', '.join(missing_inputs)}" if missing_inputs else ""
    body = f"{breadth['breadth_state']} ({float(breadth['breadth_score']):.1f})\n{breadth['summary']}{missing_text}"
    console.print(Panel(body, title="BREADTH HEALTH", expand=False))


def render_liquidity_environment(snapshot: dict[str, Any], console: Console) -> None:
    liquidity = snapshot["intelligence"].get("liquidity_environment")
    if not liquidity:
        console.print(Panel("No liquidity environment snapshot available.", title="LIQUIDITY ENVIRONMENT", expand=False))
        return
    missing_inputs = liquidity.get("missing_inputs", [])
    missing_text = f"\nMissing inputs: {', '.join(missing_inputs)}" if missing_inputs else ""
    body = f"{liquidity['liquidity_state']} ({float(liquidity['liquidity_score']):.1f})\n{liquidity['summary']}{missing_text}"
    console.print(Panel(body, title="LIQUIDITY ENVIRONMENT", expand=False))


def render_market_forces(snapshot: dict[str, Any], console: Console) -> None:
    drivers = snapshot["intelligence"]["market_drivers"]
    body = "\n".join(f"- {driver['title']}: {driver['summary']}" for driver in drivers) if drivers else "- No dominant drivers detected."
    console.print(Panel(body, title="PRIMARY MARKET DRIVERS", expand=False))


def render_divergences(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"].get("divergences", [])
    if not rows:
        console.print(Panel("- No regime-relevant divergences detected.", title="CROSS-MARKET DIVERGENCES", expand=False))
        return
    body = "\n".join(f"- {row['title']}: {row['summary']}" for row in rows[:3])
    console.print(Panel(body, title="CROSS-MARKET DIVERGENCES", expand=False))


def render_relationship_changes(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["relationship_shifts"]
    body = "\n".join(f"- {row['title']}: {row['summary']}" for row in rows[:4]) if rows else "- No material relationship shifts."
    console.print(Panel(body, title="RELATIONSHIP SHIFTS", expand=False))


def render_risk_indicators(snapshot: dict[str, Any], console: Console) -> None:
    rows = snapshot["intelligence"]["risk_monitor"]
    body = "\n".join(f"- {key}: {row['level']} | {row['summary']}" for key, row in rows.items())
    console.print(Panel(body, title="RISK MONITOR", expand=False))


def _render_experimental_signals(snapshot: dict[str, Any], console: Console) -> None:
    experimental = snapshot["intelligence"]["experimental_signals"]
    if not experimental["visible"]:
        console.print(Panel(str(experimental["summary"]), title="EXPERIMENTAL SIGNALS", expand=False))
        return

    signal_lines = [
        f"{row['series_name']}: {_format_signal_value(row.get('value'))} {row['unit']}"
        for row in experimental["signals"]
    ]
    correlation_lines = [
        f"{row['pair']} ({row['correlation']:.2f})"
        for row in experimental["correlations"][:4]
    ]
    body = "\n".join(
        ["Signals:"] + [f"- {line}" for line in signal_lines] + ["Correlations:"] + [f"- {line}" for line in correlation_lines]
    )
    console.print(Panel(body, title="EXPERIMENTAL SIGNALS", expand=False))


def render_watchlist(snapshot: dict[str, Any], console: Console) -> None:
    warnings = snapshot["intelligence"]["warning_signals"]
    body = "\n".join(f"- {item['title']}: {item['detail']}" for item in warnings) if warnings else "- No immediate warning signals."
    console.print(Panel(body, title="WARNING SIGNALS", expand=False))


def render_dashboard(snapshot: dict[str, Any], console: Console | None = None) -> None:
    """Render the Rich CLI dashboard for the latest QMIS snapshot."""
    console = console or Console()
    title = Text("OPERATOR INTELLIGENCE SNAPSHOT", style="bold")
    console.print(Panel(title, expand=False))
    render_world_snapshot(snapshot, console)
    _render_market_pulse(snapshot, console)
    _render_cosmic_state(snapshot, console)
    render_market_narrative(snapshot, console)
    render_forward_macro_signals(snapshot, console)
    render_regime_probabilities(snapshot, console)
    render_forward_regime_forecast(snapshot, console)
    render_market_stress(snapshot, console)
    render_macro_pressure(snapshot, console)
    render_cycle_monitor(snapshot, console)
    render_breadth_health(snapshot, console)
    render_liquidity_environment(snapshot, console)
    render_market_forces(snapshot, console)
    render_divergences(snapshot, console)
    render_relationship_changes(snapshot, console)
    render_risk_indicators(snapshot, console)
    render_watchlist(snapshot, console)
    _render_experimental_signals(snapshot, console)

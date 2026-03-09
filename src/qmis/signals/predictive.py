"""Forward-looking macro signal computation and persistence for QMIS."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from qmis.schema import bootstrap_database
from qmis.signals.liquidity import build_liquidity_state
from qmis.storage import connect_db, get_default_db_path


FORWARD_SIGNAL_ORDER = (
    "yield_curve",
    "credit_spreads",
    "financial_conditions",
    "real_rates",
    "global_liquidity",
    "volatility_term_structure",
    "manufacturing_momentum",
    "leadership_rotation",
    "commodity_pressure",
)


def _latest_feature_rows(features: pd.DataFrame) -> pd.DataFrame:
    if features.empty:
        return pd.DataFrame(columns=features.columns)
    latest = features.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    return latest.drop_duplicates(subset=["series_name"], keep="last")


def _feature_lookup(features: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest = _latest_feature_rows(features)
    return {
        str(row["series_name"]): row.to_dict()
        for _, row in latest.iterrows()
    }


def _signal_history(signals: pd.DataFrame, series_name: str) -> list[float]:
    frame = signals.loc[signals["series_name"] == series_name].copy()
    if frame.empty:
        return []
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values("ts")
    return [float(value) for value in frame["value"].tolist() if value is not None and pd.notna(value)]


def _latest_signal_value(signals: pd.DataFrame, series_name: str) -> float | None:
    history = _signal_history(signals, series_name)
    return history[-1] if history else None


def _previous_signal_value(signals: pd.DataFrame, series_name: str) -> float | None:
    history = _signal_history(signals, series_name)
    return history[-2] if len(history) >= 2 else None


def _trend_label(feature_map: dict[str, dict[str, Any]], series_name: str) -> str:
    row = feature_map.get(series_name, {})
    return str(row.get("trend_label") or "SIDEWAYS").upper()


def _pct_change(feature_map: dict[str, dict[str, Any]], series_name: str, column: str = "pct_change_90d") -> float | None:
    row = feature_map.get(series_name)
    if not row:
        return None
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return float(value)


def _indicator(
    *,
    state: str,
    summary: str,
    values: dict[str, float | None],
    missing_inputs: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "state": state,
        "summary": summary,
        "values": values,
        "missing_inputs": list(missing_inputs or []),
    }


def compute_yield_curve_signals(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    yield_10y = _latest_signal_value(signals, "yield_10y")
    yield_2y = _latest_signal_value(signals, "yield_2y")
    yield_3m = _latest_signal_value(signals, "yield_3m")
    if yield_10y is None or (yield_2y is None and yield_3m is None):
        return _indicator(
            state="Unavailable",
            summary="Yield curve inputs are incomplete.",
            values={},
            missing_inputs=["yield_10y", "yield_2y", "yield_3m"],
        )

    prior_10y = _previous_signal_value(signals, "yield_10y")
    prior_3m = _previous_signal_value(signals, "yield_3m")
    spread_10y_3m = (yield_10y - yield_3m) if yield_3m is not None else None
    spread_10y_2y = (yield_10y - yield_2y) if yield_2y is not None else None
    prior_spread_10y_3m = (prior_10y - prior_3m) if prior_10y is not None and prior_3m is not None else None
    active_spread = spread_10y_3m if spread_10y_3m is not None else spread_10y_2y
    spread_change = (active_spread - prior_spread_10y_3m) if active_spread is not None and prior_spread_10y_3m is not None else 0.0

    if (spread_10y_3m is not None and spread_10y_3m < 0) or (spread_10y_2y is not None and spread_10y_2y < 0):
        state = "Inverted"
        summary = "Yield curve remains inverted and recession risk is rising."
    elif spread_change > 0.2:
        state = "Steepening"
        summary = "Yield curve is steepening and points to an early-cycle recovery setup."
    else:
        state = "Normal"
        summary = "Yield curve is positively sloped with no immediate recession inversion signal."

    return _indicator(
        state=state,
        summary=summary,
        values={
            "10y_3m_spread": round(spread_10y_3m, 4) if spread_10y_3m is not None else None,
            "10y_2y_spread": round(spread_10y_2y, 4) if spread_10y_2y is not None else None,
            "spread_change": round(spread_change, 4),
        },
    )


def compute_credit_spreads(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    high_yield = _latest_signal_value(signals, "high_yield_spread")
    baa = _latest_signal_value(signals, "baa_corporate_spread")
    if high_yield is None or baa is None:
        return _indicator(
            state="Unavailable",
            summary="Credit spread inputs are incomplete.",
            values={},
            missing_inputs=["high_yield_spread", "baa_corporate_spread"],
        )

    prior_high_yield = _previous_signal_value(signals, "high_yield_spread")
    prior_baa = _previous_signal_value(signals, "baa_corporate_spread")
    trend = 0.0
    if prior_high_yield is not None and prior_baa is not None:
        trend = ((high_yield - prior_high_yield) + (baa - prior_baa)) / 2.0

    state = "Widening" if trend >= 0.1 else "Narrowing" if trend <= -0.1 else "Stable"
    stress_score = max(0.0, min(100.0, round(35.0 + high_yield * 8.0 + baa * 10.0 + max(trend, 0.0) * 25.0, 2)))
    summary = {
        "Widening": "Credit spreads are widening and systemic risk expectations are rising.",
        "Narrowing": "Credit spreads are narrowing and risk appetite is improving.",
        "Stable": "Credit spreads are stable with no decisive stress impulse.",
    }[state]
    return _indicator(
        state=state,
        summary=summary,
        values={
            "high_yield_spread": round(high_yield, 4),
            "baa_spread": round(baa, 4),
            "credit_stress_score": stress_score,
            "credit_spread_trend": round(trend, 4),
        },
    )


def compute_financial_conditions(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    value = _latest_signal_value(signals, "financial_conditions_index")
    if value is None:
        return _indicator(
            state="Unavailable",
            summary="Financial conditions input is incomplete.",
            values={},
            missing_inputs=["financial_conditions_index"],
        )

    previous = _previous_signal_value(signals, "financial_conditions_index")
    delta = value - previous if previous is not None else 0.0
    if value >= 0.5 or delta >= 0.1:
        state = "Tightening"
        summary = "Financial conditions are tightening and growth slowdown risk is rising."
    elif value <= -0.25 or delta <= -0.1:
        state = "Loosening"
        summary = "Financial conditions are loosening and the backdrop is improving for risk assets."
    else:
        state = "Stable"
        summary = "Financial conditions are broadly stable."

    return _indicator(
        state=state,
        summary=summary,
        values={
            "financial_conditions_index": round(value, 4),
            "financial_conditions_trend": round(delta, 4),
        },
    )


def compute_real_rate_signals(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    nominal_yield = _latest_signal_value(signals, "yield_10y")
    breakeven = _latest_signal_value(signals, "breakeven_inflation_10y")
    latest_real = _latest_signal_value(signals, "real_yields")
    previous_real = _previous_signal_value(signals, "real_yields")

    if latest_real is None and nominal_yield is not None and breakeven is not None:
        latest_real = nominal_yield - breakeven
    if previous_real is None:
        prior_nominal = _previous_signal_value(signals, "yield_10y")
        prior_breakeven = _previous_signal_value(signals, "breakeven_inflation_10y")
        if prior_nominal is not None and prior_breakeven is not None:
            previous_real = prior_nominal - prior_breakeven

    if latest_real is None:
        return _indicator(
            state="Unavailable",
            summary="Real-rate inputs are incomplete.",
            values={},
            missing_inputs=["real_yields", "yield_10y", "breakeven_inflation_10y"],
        )

    delta = latest_real - previous_real if previous_real is not None else 0.0
    shock = abs(delta) >= 0.2
    if delta >= 0.1:
        state = "Rising"
        summary = "Real rates are rising and tightening financial conditions."
    elif delta <= -0.1:
        state = "Falling"
        summary = "Real rates are falling and liquidity conditions are becoming more supportive."
    else:
        state = "Stable"
        summary = "Real rates are stable."

    return _indicator(
        state=state,
        summary=summary,
        values={
            "real_rate": round(latest_real, 4),
            "real_rate_trend": round(delta, 4),
            "real_rate_shock": 1.0 if shock else 0.0,
        },
    )


def compute_global_liquidity(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    liquidity = build_liquidity_state(signals=signals, features=pd.DataFrame(feature_map.values()))
    state_map = {
        "EXPANDING": "Expanding",
        "TIGHTENING": "Contracting",
        "NEUTRAL": "Neutral",
    }
    state = state_map.get(str(liquidity.get("liquidity_state", "NEUTRAL")).upper(), "Neutral")
    summary_map = {
        "Expanding": "Global liquidity is expanding and supporting risk assets.",
        "Contracting": "Global liquidity is contracting and increases recession and volatility risk.",
        "Neutral": "Global liquidity is mixed.",
    }
    return _indicator(
        state=state,
        summary=summary_map[state],
        values={
            "global_liquidity_score": round(float(liquidity.get("liquidity_score", 50.0)), 2),
        },
        missing_inputs=list(liquidity.get("missing_inputs", [])),
    )


def compute_volatility_term_structure(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del feature_map
    vix = _latest_signal_value(signals, "vix")
    vix3m = _latest_signal_value(signals, "vix3m")
    vix6m = _latest_signal_value(signals, "vix6m")
    if vix is None or vix3m is None:
        return _indicator(
            state="Unavailable",
            summary="Volatility term-structure inputs are incomplete.",
            values={},
            missing_inputs=["vix", "vix3m"],
        )

    term_structure = vix3m - vix
    if term_structure < 0:
        state = "Backwardation"
        summary = "Volatility curve is in backwardation and points to a stress regime."
    elif term_structure > 0.5:
        state = "Contango"
        summary = "Volatility curve remains in contango and indicates stable market conditions."
    else:
        state = "Flat"
        summary = "Volatility curve is flat and macro stress is mixed."

    values = {
        "term_structure": round(term_structure, 4),
        "vix": round(vix, 4),
        "vix3m": round(vix3m, 4),
    }
    if vix6m is not None:
        values["vix6m"] = round(vix6m, 4)
    return _indicator(state=state, summary=summary, values=values)


def compute_manufacturing_momentum(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    pmi = _latest_signal_value(signals, "pmi")
    if pmi is None:
        return _indicator(
            state="Unavailable",
            summary="Manufacturing input is incomplete.",
            values={},
            missing_inputs=["pmi"],
        )

    trend = _trend_label(feature_map, "pmi")
    if pmi < 50 or trend == "DOWN":
        state = "Weakening"
        summary = "Manufacturing momentum is weakening and raises contraction risk."
    elif pmi >= 52 and trend == "UP":
        state = "Improving"
        summary = "Manufacturing momentum is improving and supports an early-cycle expansion view."
    else:
        state = "Stable"
        summary = "Manufacturing momentum is stable."
    return _indicator(
        state=state,
        summary=summary,
        values={
            "pmi": round(pmi, 4),
            "pmi_trend": 1.0 if trend == "UP" else -1.0 if trend == "DOWN" else 0.0,
        },
    )


def compute_leadership_rotation(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del signals
    sp500_change = _pct_change(feature_map, "sp500")
    leadership_series = (
        "semiconductor_index",
        "small_caps",
        "bank_stocks",
        "transportation_index",
    )
    if sp500_change is None or any(_pct_change(feature_map, series_name) is None for series_name in leadership_series):
        return _indicator(
            state="Unavailable",
            summary="Leadership inputs are incomplete.",
            values={},
            missing_inputs=["sp500", *leadership_series],
        )

    relative_strengths = {
        series_name: round(float(_pct_change(feature_map, series_name) - sp500_change), 4)
        for series_name in leadership_series
    }
    average_relative_strength = sum(relative_strengths.values()) / len(relative_strengths)
    if average_relative_strength <= -1.0:
        state = "Defensive"
        summary = "Cyclical leadership is deteriorating versus the S&P 500."
    elif average_relative_strength >= 1.0:
        state = "Cyclical"
        summary = "Cyclical leadership is strengthening versus the S&P 500."
    else:
        state = "Mixed"
        summary = "Leadership rotation is mixed."

    values: dict[str, float] = {"cyclical_vs_defensive_strength": round(average_relative_strength, 4)}
    values.update(relative_strengths)
    return _indicator(state=state, summary=summary, values=values)


def compute_commodity_pressure(signals: pd.DataFrame, feature_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    del signals
    commodity_series = ("copper", "oil", "agriculture_index", "commodity_index")
    changes = [_pct_change(feature_map, series_name) for series_name in commodity_series]
    if any(change is None for change in changes):
        return _indicator(
            state="Unavailable",
            summary="Commodity pressure inputs are incomplete.",
            values={},
            missing_inputs=list(commodity_series),
        )

    average_change = sum(float(change) for change in changes if change is not None) / len(commodity_series)
    positive_trends = sum(_trend_label(feature_map, series_name) == "UP" for series_name in commodity_series)
    if average_change >= 2.0 and positive_trends >= 3:
        state = "Inflationary"
        summary = "Broad commodity strength is adding inflation pressure."
    elif average_change <= -2.0:
        state = "Disinflationary"
        summary = "Commodity weakness points to cooling growth and inflation pressure."
    else:
        state = "Mixed"
        summary = "Commodity pressure is mixed."

    return _indicator(
        state=state,
        summary=summary,
        values={
            "commodity_inflation_pressure": round(average_change, 4),
        },
    )


def build_predictive_snapshot(*, signals: pd.DataFrame, features: pd.DataFrame) -> dict[str, Any]:
    feature_map = _feature_lookup(features)
    latest_signal_ts = pd.to_datetime(signals["ts"]).max() if not signals.empty else None
    latest_feature_ts = pd.to_datetime(features["ts"]).max() if not features.empty else None
    latest_ts = max((ts for ts in (latest_signal_ts, latest_feature_ts) if ts is not None), default=None)

    forward_macro_signals = {
        "yield_curve": compute_yield_curve_signals(signals, feature_map),
        "credit_spreads": compute_credit_spreads(signals, feature_map),
        "financial_conditions": compute_financial_conditions(signals, feature_map),
        "real_rates": compute_real_rate_signals(signals, feature_map),
        "global_liquidity": compute_global_liquidity(signals, feature_map),
        "volatility_term_structure": compute_volatility_term_structure(signals, feature_map),
        "manufacturing_momentum": compute_manufacturing_momentum(signals, feature_map),
        "leadership_rotation": compute_leadership_rotation(signals, feature_map),
        "commodity_pressure": compute_commodity_pressure(signals, feature_map),
    }

    missing_inputs = sorted(
        {
            missing_input
            for payload in forward_macro_signals.values()
            for missing_input in payload.get("missing_inputs", [])
        }
    )

    recession_flags = sum(
        (
            forward_macro_signals["yield_curve"]["state"] == "Inverted",
            forward_macro_signals["credit_spreads"]["state"] == "Widening",
            forward_macro_signals["financial_conditions"]["state"] == "Tightening",
            forward_macro_signals["global_liquidity"]["state"] == "Contracting",
            forward_macro_signals["manufacturing_momentum"]["state"] == "Weakening",
            forward_macro_signals["volatility_term_structure"]["state"] == "Backwardation",
        )
    )
    inflation_flags = sum(
        (
            forward_macro_signals["commodity_pressure"]["state"] == "Inflationary",
            forward_macro_signals["real_rates"]["state"] == "Rising",
        )
    )

    summary_parts = []
    if recession_flags >= 3:
        summary_parts.append("Forward macro signals point to rising recession risk.")
    elif recession_flags == 0:
        summary_parts.append("Forward macro signals are broadly balanced.")
    else:
        summary_parts.append("Forward macro signals are mixed but lean defensive.")

    if inflation_flags >= 2:
        summary_parts.append("Inflation pressure remains active across rates and commodities.")
    elif inflation_flags == 1:
        summary_parts.append("Inflation pressure remains present.")

    if missing_inputs:
        summary_parts.append(f"Missing inputs: {', '.join(missing_inputs)}.")

    return {
        "ts": latest_ts,
        "summary": " ".join(summary_parts).strip(),
        "forward_macro_signals": forward_macro_signals,
        "missing_inputs": missing_inputs,
    }


def materialize_predictive_signals(db_path: Path | None = None) -> int:
    """Recompute and replace the current forward macro snapshot."""
    target_path = bootstrap_database(db_path or get_default_db_path())
    with connect_db(target_path) as connection:
        signals = connection.execute(
            """
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM signals
            ORDER BY ts, series_name
            """
        ).fetchdf()
        features = connection.execute(
            """
            SELECT ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                   zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
            FROM features
            ORDER BY ts, series_name
            """
        ).fetchdf()

        snapshot = build_predictive_snapshot(signals=signals, features=features)
        connection.execute("DELETE FROM predictive_snapshots")
        if snapshot["ts"] is None:
            return 0

        payload = pd.DataFrame(
            [
                {
                    "ts": snapshot["ts"],
                    "summary": str(snapshot["summary"]),
                    "forward_macro_signals": json.dumps(snapshot["forward_macro_signals"], sort_keys=True),
                    "missing_inputs": json.dumps(snapshot["missing_inputs"]),
                }
            ]
        )
        connection.register("predictive_df", payload)
        connection.execute(
            """
            INSERT INTO predictive_snapshots (
                ts,
                summary,
                forward_macro_signals,
                missing_inputs
            )
            SELECT
                ts,
                summary,
                forward_macro_signals,
                missing_inputs
            FROM predictive_df
            """
        )
        connection.unregister("predictive_df")
    return 1

"""Dominant factor detection for the QMIS macro engine."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from qmis.schema import bootstrap_database
from qmis.storage import connect_db, get_default_db_path


MIN_SERIES = 3
MIN_OBSERVATIONS = 60
MAX_FACTORS = 3
SUPPORTING_ASSET_COUNT = 4

THEME_SERIES = {
    "liquidity": {
        "fed_balance_sheet",
        "m2_money_supply",
        "reverse_repo_usage",
        "yield_3m",
        "yield_10y",
        "dollar_index",
        "real_yields",
    },
    "inflation": {"gold", "oil", "yield_10y"},
    "growth": {"copper", "sp500", "pmi"},
    "volatility": {"vix", "sp500_above_200dma", "new_highs", "new_lows"},
    "crypto": {"BTCUSD", "ETHUSD", "BTC_dominance", "crypto_market_cap"},
    "commodities": {"gold", "oil", "copper"},
    "dollar": {"dollar_index"},
}


def _latest_trends(feature_frame: pd.DataFrame) -> dict[str, str]:
    if feature_frame.empty:
        return {}

    latest = feature_frame.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")
    return {
        str(row["series_name"]): str(row["trend_label"])
        for _, row in latest.iterrows()
        if pd.notna(row["trend_label"])
    }


def _load_signal_context(signals: pd.DataFrame) -> dict[str, dict[str, Any]]:
    latest = signals.copy()
    latest["ts"] = pd.to_datetime(latest["ts"])
    latest = latest.sort_values(["series_name", "ts"])
    latest = latest.drop_duplicates(subset=["series_name"], keep="last")
    return {
        str(row["series_name"]): {
            "category": str(row.get("category", "")),
            "source": str(row.get("source", "")),
        }
        for _, row in latest.iterrows()
    }


def _prepare_return_matrix(signals: pd.DataFrame) -> pd.DataFrame:
    if signals.empty:
        return pd.DataFrame()

    frame = signals.copy()
    frame["ts"] = pd.to_datetime(frame["ts"])
    frame = frame.sort_values(["ts", "series_name"])
    pivot = frame.pivot_table(index="ts", columns="series_name", values="value", aggfunc="last").sort_index()
    pivot = pivot.ffill(limit=7)
    returns = pivot.pct_change(fill_method=None).replace([np.inf, -np.inf], np.nan)
    returns = returns.dropna(axis=1, thresh=MIN_OBSERVATIONS)
    if returns.shape[1] < MIN_SERIES:
        return pd.DataFrame()

    standardized = returns.copy()
    standardized = standardized - standardized.mean(axis=0)
    std = standardized.std(axis=0, ddof=0).replace(0.0, np.nan)
    standardized = standardized.divide(std, axis=1)
    standardized = standardized.dropna(axis=1, how="all")
    standardized = standardized.fillna(0.0)
    if standardized.shape[0] < MIN_OBSERVATIONS or standardized.shape[1] < MIN_SERIES:
        return pd.DataFrame()
    return standardized


def _rank_themes(loadings: dict[str, float]) -> list[str]:
    scores: list[tuple[str, float]] = []
    for theme, members in THEME_SERIES.items():
        score = sum(abs(float(loadings.get(series_name, 0.0))) for series_name in members)
        scores.append((theme, score))
    return [theme for theme, score in sorted(scores, key=lambda item: (item[1], item[0]), reverse=True) if score > 0]


def _supporting_assets(loadings: dict[str, float]) -> list[str]:
    ordered = sorted(loadings.items(), key=lambda item: abs(float(item[1])), reverse=True)
    return [series_name for series_name, _ in ordered[:SUPPORTING_ASSET_COUNT]]


def _liquidity_direction(trends: dict[str, str]) -> str:
    tightening_score = 0
    tightening_score += 1 if trends.get("fed_balance_sheet") == "DOWN" else -1 if trends.get("fed_balance_sheet") == "UP" else 0
    tightening_score += 1 if trends.get("reverse_repo_usage") == "UP" else -1 if trends.get("reverse_repo_usage") == "DOWN" else 0
    tightening_score += 1 if trends.get("yield_3m") == "UP" else -1 if trends.get("yield_3m") == "DOWN" else 0
    tightening_score += 1 if trends.get("yield_10y") == "UP" else -1 if trends.get("yield_10y") == "DOWN" else 0
    tightening_score += 1 if trends.get("dollar_index") == "UP" else -1 if trends.get("dollar_index") == "DOWN" else 0
    return "tightening" if tightening_score >= 1 else "expanding"


def _trend_direction(trends: dict[str, str], series_names: tuple[str, ...], *, positive: str, negative: str, fallback: str) -> str:
    score = 0
    for series_name in series_names:
        trend = trends.get(series_name)
        if trend == "UP":
            score += 1
        elif trend == "DOWN":
            score -= 1
    if score > 0:
        return positive
    if score < 0:
        return negative
    return fallback


def _determine_direction(theme: str, trends: dict[str, str]) -> str:
    if theme == "liquidity":
        return _liquidity_direction(trends)
    if theme == "crypto":
        return _trend_direction(
            trends,
            ("BTCUSD", "ETHUSD", "crypto_market_cap"),
            positive="bullish",
            negative="bearish",
            fallback="mixed",
        )
    if theme == "volatility":
        if trends.get("vix") == "UP" or trends.get("new_lows") == "UP" or trends.get("sp500_above_200dma") == "DOWN":
            return "stressed"
        return "contained"
    if theme == "growth":
        return _trend_direction(trends, ("copper", "sp500", "pmi"), positive="accelerating", negative="slowing", fallback="steady")
    if theme == "inflation":
        return _trend_direction(trends, ("gold", "oil", "yield_10y"), positive="rising", negative="cooling", fallback="stable")
    if theme == "commodities":
        return _trend_direction(trends, ("gold", "oil", "copper"), positive="rising", negative="weakening", fallback="mixed")
    if theme == "dollar":
        return _trend_direction(trends, ("dollar_index",), positive="strengthening", negative="weakening", fallback="stable")
    return "mixed"


def _driver_title(theme: str, direction: str) -> str:
    if theme == "liquidity":
        return f"Liquidity {direction.title()}"
    if theme == "crypto":
        return "Crypto Cycle"
    if theme == "volatility":
        return "Volatility Regime"
    return theme.replace("_", " ").title()


def _factor_summary(theme: str, direction: str, strength: float, supporting_assets: list[str]) -> str:
    strength_label = "Strong" if strength >= 0.6 else "Moderate" if strength >= 0.35 else "Developing"
    assets = ", ".join(supporting_assets[:3]) if supporting_assets else "supporting assets unavailable"
    return f"{strength_label} {theme} driver ({direction}) led by {assets}."


def build_factor_frame(signals: pd.DataFrame, feature_frame: pd.DataFrame | None = None) -> pd.DataFrame:
    """Compute the dominant factor snapshot from raw signal history."""
    matrix = _prepare_return_matrix(signals)
    columns = [
        "ts",
        "factor_name",
        "component_rank",
        "strength",
        "direction",
        "summary",
        "supporting_assets",
        "loadings",
    ]
    if matrix.empty:
        return pd.DataFrame(columns=columns)

    covariance = np.cov(matrix.to_numpy(dtype=float), rowvar=False)
    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    total_variance = float(np.sum(np.clip(eigenvalues, a_min=0.0, a_max=None)))
    if total_variance <= 0.0:
        return pd.DataFrame(columns=columns)

    latest_ts = pd.to_datetime(signals["ts"]).max()
    trends = _latest_trends(feature_frame if feature_frame is not None else pd.DataFrame())
    assigned_themes: set[str] = set()
    factor_rows: list[dict[str, Any]] = []

    for component_index, eigenvalue in enumerate(eigenvalues[:MAX_FACTORS], start=1):
        if float(eigenvalue) <= 0.0:
            continue

        loadings = {
            str(series_name): float(eigenvectors[series_index, component_index - 1])
            for series_index, series_name in enumerate(matrix.columns)
        }
        ranked_themes = _rank_themes(loadings)
        factor_name = next((theme for theme in ranked_themes if theme not in assigned_themes), ranked_themes[0] if ranked_themes else "macro")
        assigned_themes.add(factor_name)
        supporting_assets = _supporting_assets(loadings)
        strength = float(eigenvalue) / total_variance
        direction = _determine_direction(factor_name, trends)

        factor_rows.append(
            {
                "ts": latest_ts,
                "factor_name": factor_name,
                "component_rank": component_index,
                "strength": float(strength),
                "direction": direction,
                "summary": _factor_summary(factor_name, direction, float(strength), supporting_assets),
                "supporting_assets": json.dumps(supporting_assets),
                "loadings": json.dumps(loadings, sort_keys=True),
            }
        )

    result = pd.DataFrame(factor_rows, columns=columns)
    if result.empty:
        return result
    return result.sort_values(["strength", "component_rank"], ascending=[False, True]).reset_index(drop=True)


def materialize_factors(db_path: Path | None = None) -> int:
    """Recompute and replace the dominant factor snapshot."""
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

        factor_frame = build_factor_frame(signals, features)
        connection.execute("DELETE FROM factors")
        if factor_frame.empty:
            return 0

        # Re-rank after ordering so persisted rows stay stable for CLI consumption.
        factor_frame = factor_frame.copy()
        factor_frame["component_rank"] = np.arange(1, len(factor_frame) + 1)
        connection.register("factors_df", factor_frame)
        connection.execute(
            """
            INSERT INTO factors (
                ts,
                factor_name,
                component_rank,
                strength,
                direction,
                summary,
                supporting_assets,
                loadings
            )
            SELECT
                ts,
                factor_name,
                component_rank,
                strength,
                direction,
                summary,
                supporting_assets,
                loadings
            FROM factors_df
            """
        )
        connection.unregister("factors_df")
    return int(len(factor_frame))

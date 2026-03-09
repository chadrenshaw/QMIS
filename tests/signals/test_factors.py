import json
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


class QMISFactorTests(unittest.TestCase):
    def _build_signal_frame(self) -> pd.DataFrame:
        rng = np.random.default_rng(42)
        dates = pd.date_range("2025-01-01", periods=180, freq="D")

        liquidity_factor = rng.normal(0.0, 0.015, len(dates))
        crypto_factor = rng.normal(0.0, 0.03, len(dates))
        volatility_factor = rng.normal(0.0, 0.02, len(dates))

        returns = {
            "fed_balance_sheet": 0.9 * liquidity_factor + rng.normal(0.0, 0.003, len(dates)),
            "yield_3m": -0.8 * liquidity_factor + rng.normal(0.0, 0.003, len(dates)),
            "reverse_repo_usage": -0.85 * liquidity_factor + rng.normal(0.0, 0.003, len(dates)),
            "dollar_index": -0.7 * liquidity_factor + rng.normal(0.0, 0.003, len(dates)),
            "BTCUSD": 0.9 * crypto_factor + 0.2 * liquidity_factor + rng.normal(0.0, 0.005, len(dates)),
            "ETHUSD": 0.95 * crypto_factor + 0.2 * liquidity_factor + rng.normal(0.0, 0.005, len(dates)),
            "crypto_market_cap": 0.92 * crypto_factor + 0.15 * liquidity_factor + rng.normal(0.0, 0.005, len(dates)),
            "vix": 0.95 * volatility_factor - 0.15 * liquidity_factor + rng.normal(0.0, 0.004, len(dates)),
            "sp500_above_200dma": -0.9 * volatility_factor + 0.1 * liquidity_factor + rng.normal(0.0, 0.004, len(dates)),
            "new_lows": 0.85 * volatility_factor + rng.normal(0.0, 0.004, len(dates)),
        }

        categories = {
            "fed_balance_sheet": "liquidity",
            "yield_3m": "macro",
            "reverse_repo_usage": "liquidity",
            "dollar_index": "liquidity",
            "BTCUSD": "crypto",
            "ETHUSD": "crypto",
            "crypto_market_cap": "crypto",
            "vix": "market",
            "sp500_above_200dma": "breadth",
            "new_lows": "breadth",
        }
        units = {
            "fed_balance_sheet": "usd_billions",
            "yield_3m": "percent",
            "reverse_repo_usage": "usd_billions",
            "dollar_index": "index_points",
            "BTCUSD": "usd",
            "ETHUSD": "usd",
            "crypto_market_cap": "usd_billions",
            "vix": "index_points",
            "sp500_above_200dma": "percent",
            "new_lows": "count",
        }
        starting_values = {
            "fed_balance_sheet": 8_500.0,
            "yield_3m": 5.2,
            "reverse_repo_usage": 650.0,
            "dollar_index": 104.0,
            "BTCUSD": 82_000.0,
            "ETHUSD": 4_200.0,
            "crypto_market_cap": 2_500.0,
            "vix": 18.0,
            "sp500_above_200dma": 67.0,
            "new_lows": 45.0,
        }

        rows: list[dict[str, object]] = []
        for series_name, series_returns in returns.items():
            values = starting_values[series_name] * np.cumprod(1.0 + series_returns)
            for ts, value in zip(dates, values, strict=True):
                rows.append(
                    {
                        "ts": ts,
                        "source": "test",
                        "category": categories[series_name],
                        "series_name": series_name,
                        "value": float(value),
                        "unit": units[series_name],
                        "metadata": "{}",
                    }
                )
        return pd.DataFrame(rows)

    def _build_feature_frame(self, ts: pd.Timestamp) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"ts": ts, "series_name": "fed_balance_sheet", "pct_change_30d": -2.0, "pct_change_90d": -3.5, "pct_change_365d": -4.0, "zscore_30d": -1.0, "volatility_30d": 0.3, "slope_30d": -0.2, "drawdown_90d": -4.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "yield_3m", "pct_change_30d": 3.0, "pct_change_90d": 5.0, "pct_change_365d": 7.0, "zscore_30d": 1.1, "volatility_30d": 0.2, "slope_30d": 0.3, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "reverse_repo_usage", "pct_change_30d": 6.0, "pct_change_90d": 9.0, "pct_change_365d": 12.0, "zscore_30d": 1.2, "volatility_30d": 0.3, "slope_30d": 0.4, "drawdown_90d": -1.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "dollar_index", "pct_change_30d": 2.0, "pct_change_90d": 3.0, "pct_change_365d": 5.0, "zscore_30d": 0.6, "volatility_30d": 0.1, "slope_30d": 0.2, "drawdown_90d": -0.5, "trend_label": "UP"},
                {"ts": ts, "series_name": "BTCUSD", "pct_change_30d": 12.0, "pct_change_90d": 18.0, "pct_change_365d": 55.0, "zscore_30d": 1.8, "volatility_30d": 0.5, "slope_30d": 0.8, "drawdown_90d": -9.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "ETHUSD", "pct_change_30d": 10.0, "pct_change_90d": 16.0, "pct_change_365d": 60.0, "zscore_30d": 1.7, "volatility_30d": 0.6, "slope_30d": 0.9, "drawdown_90d": -11.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "crypto_market_cap", "pct_change_30d": 9.0, "pct_change_90d": 15.0, "pct_change_365d": 45.0, "zscore_30d": 1.5, "volatility_30d": 0.4, "slope_30d": 0.7, "drawdown_90d": -8.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "vix", "pct_change_30d": 14.0, "pct_change_90d": 11.0, "pct_change_365d": 4.0, "zscore_30d": 1.4, "volatility_30d": 0.5, "slope_30d": 0.4, "drawdown_90d": -2.0, "trend_label": "UP"},
                {"ts": ts, "series_name": "sp500_above_200dma", "pct_change_30d": -6.0, "pct_change_90d": -10.0, "pct_change_365d": -3.0, "zscore_30d": -1.3, "volatility_30d": 0.3, "slope_30d": -0.5, "drawdown_90d": -9.0, "trend_label": "DOWN"},
                {"ts": ts, "series_name": "new_lows", "pct_change_30d": 18.0, "pct_change_90d": 22.0, "pct_change_365d": 30.0, "zscore_30d": 1.6, "volatility_30d": 0.6, "slope_30d": 0.6, "drawdown_90d": -1.0, "trend_label": "UP"},
            ]
        )

    def test_build_factor_frame_identifies_dominant_drivers(self) -> None:
        from qmis.signals.factors import build_factor_frame

        signals = self._build_signal_frame()
        features = self._build_feature_frame(pd.Timestamp("2025-06-29"))

        factors = build_factor_frame(signals, features)

        self.assertEqual(list(factors["factor_name"]), ["liquidity", "crypto", "volatility"])
        self.assertGreater(float(factors.iloc[0]["strength"]), 0.2)
        self.assertEqual(factors.loc[factors["factor_name"] == "liquidity", "direction"].iloc[0], "tightening")
        liquidity_assets = json.loads(factors.loc[factors["factor_name"] == "liquidity", "supporting_assets"].iloc[0])
        self.assertIn("fed_balance_sheet", liquidity_assets)
        self.assertIn("yield_3m", liquidity_assets)

    def test_materialize_factors_persists_latest_snapshot(self) -> None:
        from qmis.schema import bootstrap_database
        from qmis.signals.factors import materialize_factors
        from qmis.storage import connect_db

        signals = self._build_signal_frame()
        features = self._build_feature_frame(pd.Timestamp("2025-06-29"))

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "qmis.duckdb"
            bootstrap_database(db_path)
            with connect_db(db_path) as connection:
                connection.register("signals_df", signals)
                connection.execute(
                    """
                    INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
                    SELECT ts, source, category, series_name, value, unit, metadata
                    FROM signals_df
                    """
                )
                connection.unregister("signals_df")
                connection.register("features_df", features)
                connection.execute(
                    """
                    INSERT INTO features (
                        ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                        zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
                    )
                    SELECT
                        ts, series_name, pct_change_30d, pct_change_90d, pct_change_365d,
                        zscore_30d, volatility_30d, slope_30d, drawdown_90d, trend_label
                    FROM features_df
                    """
                )
                connection.unregister("features_df")

            inserted_rows = materialize_factors(db_path=db_path)

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                persisted = connection.execute(
                    """
                    SELECT factor_name, component_rank, strength, direction, supporting_assets
                    FROM factors
                    ORDER BY component_rank
                    """
                ).fetchdf()
            finally:
                connection.close()

        self.assertEqual(inserted_rows, 3)
        self.assertEqual(list(persisted["factor_name"]), ["liquidity", "crypto", "volatility"])
        self.assertEqual(list(persisted["component_rank"]), [1, 2, 3])
        self.assertEqual(str(persisted.iloc[0]["direction"]), "tightening")


if __name__ == "__main__":
    unittest.main()

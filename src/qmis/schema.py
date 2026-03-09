"""DuckDB schema bootstrap for QMIS core tables."""

from __future__ import annotations

from pathlib import Path

from qmis.storage import connect_db, get_default_db_path


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS signals (
        ts TIMESTAMP,
        source TEXT,
        category TEXT,
        series_name TEXT,
        value DOUBLE,
        unit TEXT,
        metadata JSON
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS features (
        ts TIMESTAMP,
        series_name TEXT,
        pct_change_30d DOUBLE,
        pct_change_90d DOUBLE,
        pct_change_365d DOUBLE,
        zscore_30d DOUBLE,
        volatility_30d DOUBLE,
        slope_30d DOUBLE,
        drawdown_90d DOUBLE,
        trend_label TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS relationships (
        ts TIMESTAMP,
        series_x TEXT,
        series_y TEXT,
        window_days INTEGER,
        lag_days INTEGER,
        correlation DOUBLE,
        p_value DOUBLE,
        relationship_state TEXT,
        confidence_label TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS regimes (
        ts TIMESTAMP,
        inflation_score INTEGER,
        growth_score INTEGER,
        liquidity_score INTEGER,
        risk_score INTEGER,
        regime_label TEXT,
        confidence DOUBLE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS alerts (
        ts TIMESTAMP,
        alert_type TEXT,
        severity TEXT,
        rule_key TEXT,
        dedupe_key TEXT,
        title TEXT,
        message TEXT,
        source_table TEXT,
        series_name TEXT,
        series_x TEXT,
        series_y TEXT,
        metadata JSON
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS collector_runs (
        collector_name TEXT,
        source TEXT,
        collected_at TIMESTAMP,
        status TEXT,
        row_count INTEGER,
        message TEXT
    )
    """,
)

SCHEMA_MIGRATIONS = (
    "ALTER TABLE relationships ADD COLUMN IF NOT EXISTS confidence_label TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS rule_key TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS dedupe_key TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS title TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS message TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS source_table TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS series_name TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS series_x TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS series_y TEXT",
    "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS metadata JSON",
)


def bootstrap_schema(connection) -> None:
    """Create the core QMIS tables on an existing connection."""
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    for statement in SCHEMA_MIGRATIONS:
        connection.execute(statement)


def bootstrap_database(db_path: Path | None = None) -> Path:
    """Ensure the database exists and all core schema tables are present."""
    target_path = db_path if db_path is not None else get_default_db_path()
    with connect_db(target_path) as connection:
        bootstrap_schema(connection)
    return target_path

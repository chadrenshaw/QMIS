"""DuckDB storage helpers for QMIS."""

from __future__ import annotations

from pathlib import Path

import duckdb

from qmis.config import load_config


def get_default_db_path() -> Path:
    """Return the repo-local default DuckDB database path."""
    return load_config().db_path


def connect_db(db_path: Path | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating the parent directory when needed."""
    target_path = db_path or get_default_db_path()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(target_path), read_only=read_only)

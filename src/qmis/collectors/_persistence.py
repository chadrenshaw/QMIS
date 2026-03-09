"""Shared signal persistence helpers for QMIS collectors."""

from __future__ import annotations

import pandas as pd


def replace_signal_rows(connection, payload: pd.DataFrame, temp_table_name: str) -> int:
    """Replace existing signal history for the payload's source/category/series set."""
    if payload.empty:
        return 0

    deduped_payload = payload.drop_duplicates(
        subset=["ts", "source", "category", "series_name", "value", "unit", "metadata"],
    ).copy()
    connection.register(temp_table_name, deduped_payload)
    try:
        connection.execute(
            f"""
            DELETE FROM signals
            USING (
                SELECT DISTINCT source, category, series_name
                FROM {temp_table_name}
            ) AS replacement_scope
            WHERE signals.source = replacement_scope.source
              AND signals.category = replacement_scope.category
              AND signals.series_name = replacement_scope.series_name
            """
        )
        connection.execute(
            f"""
            INSERT INTO signals (ts, source, category, series_name, value, unit, metadata)
            SELECT ts, source, category, series_name, value, unit, metadata
            FROM {temp_table_name}
            """
        )
    finally:
        connection.unregister(temp_table_name)
    return int(len(deduped_payload))

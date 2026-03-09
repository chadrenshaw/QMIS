"""Optional read-only FastAPI surface for QMIS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from qmis.alerts.engine import load_alert_snapshot
from qmis.config import load_config
from qmis.dashboard.cli import load_dashboard_snapshot, _parse_metadata
from qmis.schema import bootstrap_database
from qmis.signals.anomalies import detect_relationship_anomalies
from qmis.signals.divergence import detect_cross_market_divergences
from qmis.storage import connect_db, get_default_db_path


def _serialize_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _serialize_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: _serialize_value(value) for key, value in record.items()}


def _fetch_latest_signals(db_path: Path) -> dict[str, dict[str, Any]]:
    with connect_db(db_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT ts, series_name, value, unit, category, source
            FROM (
                SELECT
                    ts,
                    series_name,
                    value,
                    unit,
                    category,
                    source,
                    ROW_NUMBER() OVER (PARTITION BY series_name ORDER BY ts DESC) AS row_number
                FROM signals
            )
            WHERE row_number = 1
            ORDER BY series_name
            """
        ).fetchdf()

    return {
        str(row["series_name"]): _serialize_record(
            {
                "ts": row["ts"],
                "value": float(row["value"]),
                "unit": str(row["unit"]),
                "category": str(row["category"]),
                "source": str(row["source"]),
            }
        )
        for _, row in rows.iterrows()
    }


def _fetch_latest_regime(db_path: Path) -> dict[str, Any] | None:
    with connect_db(db_path, read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT ts, inflation_score, growth_score, liquidity_score, risk_score, regime_label, confidence, regime_probabilities, regime_drivers
            FROM regimes
            ORDER BY ts DESC
            LIMIT 1
            """
        ).fetchdf()

    if rows.empty:
        return None
    row = rows.iloc[0].to_dict()
    row["regime_probabilities"] = _parse_metadata(row.get("regime_probabilities"))
    row["regime_drivers"] = _parse_metadata(row.get("regime_drivers"))
    return _serialize_record(row)


def _fetch_relationships(db_path: Path) -> list[dict[str, Any]]:
    with connect_db(db_path, read_only=True) as connection:
        rows = connection.execute(
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
            ORDER BY ts DESC, ABS(correlation) DESC, window_days DESC
            """
        ).fetchdf()
    return [_serialize_record(row) for row in rows.to_dict("records")]


def _fetch_anomalies(db_path: Path) -> list[dict[str, Any]]:
    with connect_db(db_path, read_only=True) as connection:
        rows = connection.execute(
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
            WHERE lag_days = 0
            ORDER BY ts DESC, window_days DESC
            """
        ).fetchdf()
    anomalies = detect_relationship_anomalies(rows)
    return [_serialize_record(row) for row in anomalies.to_dict("records")]


def _fetch_divergences(db_path: Path) -> list[dict[str, Any]]:
    with connect_db(db_path, read_only=True) as connection:
        relationships = connection.execute(
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
            WHERE lag_days = 0
            ORDER BY ts DESC, window_days DESC
            """
        ).fetchdf()
        features = connection.execute(
            """
            SELECT ts, series_name, pct_change_30d, trend_label
            FROM features
            ORDER BY ts DESC, series_name
            """
        ).fetchdf()
    divergences = detect_cross_market_divergences(relationships=relationships, features=features)
    return [_serialize_record(row) for row in divergences.to_dict("records")]


def create_app(db_path: Path | None = None, web_dist_dir: Path | None = None) -> FastAPI:
    """Create the optional read-only FastAPI application."""
    config = load_config()
    resolved_db_path = bootstrap_database(db_path or get_default_db_path())
    resolved_web_dist_dir = web_dist_dir or config.web_dist_dir
    app = FastAPI(title="QMIS Read API", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "db_path": str(resolved_db_path), "read_only": True}

    @app.get("/regime/latest")
    def regime_latest() -> dict[str, Any]:
        regime = _fetch_latest_regime(resolved_db_path)
        return regime or {}

    @app.get("/signals/latest")
    def signals_latest() -> dict[str, Any]:
        return {"signals": _fetch_latest_signals(resolved_db_path)}

    @app.get("/relationships")
    def relationships() -> dict[str, Any]:
        return {"relationships": _fetch_relationships(resolved_db_path)}

    @app.get("/anomalies")
    def anomalies() -> dict[str, Any]:
        return {"anomalies": _fetch_anomalies(resolved_db_path)}

    @app.get("/divergences")
    def divergences() -> dict[str, Any]:
        return {"divergences": _fetch_divergences(resolved_db_path)}

    @app.get("/alerts")
    def alerts() -> dict[str, Any]:
        rows, summary = load_alert_snapshot(db_path=resolved_db_path)
        return {
            "summary": _serialize_record(summary),
            "alerts": [_serialize_record(row) for row in rows.to_dict("records")],
        }

    @app.get("/dashboard")
    def dashboard() -> dict[str, Any]:
        snapshot = load_dashboard_snapshot(db_path=resolved_db_path)
        return {
            "trend_summary": {key: _serialize_record(value) for key, value in snapshot["trend_summary"].items()},
            "signal_summary": {key: _serialize_record(value) for key, value in snapshot["signal_summary"].items()},
            "signal_groups": snapshot["signal_groups"],
            "signal_history": {
                key: [_serialize_record(point) for point in values]
                for key, values in snapshot["signal_history"].items()
            },
            "scores": snapshot["scores"],
            "score_history": [_serialize_record(point) for point in snapshot["score_history"]],
            "regime": _serialize_record(snapshot["regime"]) if snapshot["regime"] else None,
            "market_stress": _serialize_record(snapshot["market_stress"]) if snapshot["market_stress"] else None,
            "breadth_health": _serialize_record(snapshot["breadth_health"]) if snapshot["breadth_health"] else None,
            "liquidity_environment": _serialize_record(snapshot["liquidity_environment"]) if snapshot["liquidity_environment"] else None,
            "yield_curve": snapshot["yield_curve"],
            "yield_curve_state": snapshot["yield_curve_state"],
            "freshness": _serialize_record(snapshot["freshness"]),
            "latest_snapshot_ts": _serialize_value(snapshot["latest_snapshot_ts"]),
            "narrative": _serialize_record(snapshot["intelligence"]["market_narrative"]),
            "top_relationships": [_serialize_record(row) for row in snapshot["top_relationships"]],
            "lead_lag_relationships": [_serialize_record(row) for row in snapshot["lead_lag_relationships"]],
            "anomalies": [_serialize_record(row) for row in snapshot["anomalies"]],
            "divergences": [_serialize_record(row) for row in snapshot["divergences"]],
            "alert_summary": _serialize_record(snapshot["alert_summary"]),
            "alerts": [_serialize_record(row) for row in snapshot["alerts"]],
        }

    index_file = resolved_web_dist_dir / "index.html"
    assets_dir = resolved_web_dist_dir / "assets"
    if index_file.exists():
        if assets_dir.exists():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="frontend-assets")

        @app.get("/", include_in_schema=False)
        def frontend_index() -> FileResponse:
            return FileResponse(index_file)

        @app.get("/{frontend_path:path}", include_in_schema=False)
        def frontend_fallback(frontend_path: str) -> FileResponse:
            return FileResponse(index_file)

    return app


app = create_app()

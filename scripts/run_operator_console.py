#!/usr/bin/env python3
"""Refresh the current QMIS pipeline and render the operator console."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qmis.alerts.engine import materialize_alerts
from qmis.collectors.astronomy import run_astronomy_collector
from qmis.collectors.breadth import run_breadth_collector
from qmis.collectors.crypto import run_crypto_collector
from qmis.collectors.liquidity import run_liquidity_collector
from qmis.collectors.macro import run_macro_collector
from qmis.collectors.market import run_market_collector
from qmis.collectors.natural import run_natural_collector
from qmis.collectors.solar import run_solar_collector
from qmis.config import load_config
from qmis.dashboard.cli import load_dashboard_snapshot, render_dashboard
from qmis.features.normalization import materialize_features
from qmis.logger import get_logger
from qmis.signals.correlations import materialize_relationships
from qmis.signals.leadlag import materialize_lead_lag_relationships
from qmis.signals.regime import materialize_regime


def _collector_runners() -> tuple[tuple[str, object], ...]:
    return (
        ("market", run_market_collector),
        ("crypto", run_crypto_collector),
        ("breadth", run_breadth_collector),
        ("macro", run_macro_collector),
        ("liquidity", run_liquidity_collector),
        ("solar", run_solar_collector),
        ("astronomy", run_astronomy_collector),
        ("natural", run_natural_collector),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh QMIS and render the operator console")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned operator-console steps and exit")
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Render the operator console from the existing DuckDB state without running collectors or analysis",
    )
    return parser.parse_args(argv)


def _run_collectors(db_path: Path) -> dict[str, int]:
    return {
        collector_name: int(runner(db_path=db_path))
        for collector_name, runner in _collector_runners()
    }


def _refresh_pipeline(db_path: Path) -> dict[str, int | dict[str, int]]:
    collector_counts = _run_collectors(db_path)
    return {
        "collectors": collector_counts,
        "features": int(materialize_features(db_path=db_path)),
        "regime": int(materialize_regime(db_path=db_path)),
        "relationships": int(materialize_relationships(db_path=db_path)),
        "lead_lag": int(materialize_lead_lag_relationships(db_path=db_path)),
        "alerts": int(materialize_alerts(db_path=db_path)),
    }


def _render_refresh_summary(summary: dict[str, int | dict[str, int]], console: Console) -> None:
    collector_counts = summary["collectors"]
    assert isinstance(collector_counts, dict)
    collector_text = ", ".join(f"{name}={count}" for name, count in collector_counts.items())
    message = (
        "Refresh complete\n"
        f"Collectors: {collector_text}\n"
        f"Features: {summary['features']}  Regime: {summary['regime']}  "
        f"Relationships: {summary['relationships']}  Lead-lag: {summary['lead_lag']}  Alerts: {summary['alerts']}"
    )
    console.print(Panel(message, title="QMIS Refresh Summary", expand=False))


def main(argv: list[str] | None = None, console: Console | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_operator_console")
    console = console or Console()

    if args.dry_run:
        message = (
            f"QMIS operator console dry-run: repo={config.repo_root} db={config.db_path} "
            "collectors=all analysis=features,regime,relationships,lead-lag alerts=materialize dashboard=render"
        )
        logger.info(message)
        print(message)
        return 0

    if not args.no_refresh:
        refresh_summary = _refresh_pipeline(config.db_path)
        logger.info("Refreshed QMIS pipeline into %s", config.db_path)
        _render_refresh_summary(refresh_summary, console)

    snapshot = load_dashboard_snapshot(db_path=config.db_path)
    render_dashboard(snapshot, console=console)
    logger.info("Rendered operator console from %s", config.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

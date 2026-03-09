#!/usr/bin/env python3
"""Refresh the current QMIS pipeline and render the operator console."""

from __future__ import annotations

import argparse
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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
from qmis.signals.breadth import materialize_breadth_health
from qmis.signals.correlations import materialize_relationships
from qmis.signals.cycles import materialize_cycle_snapshots
from qmis.signals.factors import materialize_factors
from qmis.signals.leadlag import materialize_lead_lag_relationships
from qmis.signals.liquidity import materialize_liquidity_state
from qmis.signals.macro_pressure import materialize_macro_pressure
from qmis.signals.regime import materialize_regime
from qmis.signals.stress import materialize_market_stress
from qmis.storage import connect_db


def _collector_specs() -> tuple[dict[str, Any], ...]:
    return (
        {
            "name": "market",
            "source": "yfinance",
            "runner": run_market_collector,
            "series_names": ("gold", "oil", "copper", "sp500", "vix", "dollar_index"),
            "max_age": timedelta(minutes=15),
            "timeout_seconds": 60,
        },
        {
            "name": "crypto",
            "source": "yfinance,coingecko",
            "runner": run_crypto_collector,
            "series_names": ("BTCUSD", "ETHUSD", "crypto_market_cap", "BTC_dominance"),
            "max_age": timedelta(minutes=15),
            "timeout_seconds": 60,
        },
        {
            "name": "breadth",
            "source": "github_raw,yfinance",
            "runner": run_breadth_collector,
            "series_names": ("sp500_above_200dma", "advance_decline_line", "new_highs", "new_lows"),
            "max_age": timedelta(hours=12),
            "timeout_seconds": 120,
        },
        {
            "name": "macro",
            "source": "fred",
            "runner": run_macro_collector,
            "series_names": ("yield_10y", "yield_3m", "m2_money_supply", "fed_balance_sheet", "reverse_repo_usage", "real_yields", "pmi"),
            "max_age": timedelta(hours=12),
            "timeout_seconds": 75,
        },
        {
            "name": "liquidity",
            "source": "fred,yfinance",
            "runner": run_liquidity_collector,
            "series_names": ("fed_balance_sheet", "reverse_repo_usage", "m2_money_supply", "dollar_index", "real_yields"),
            "max_age": timedelta(hours=12),
            "timeout_seconds": 75,
        },
        {
            "name": "solar",
            "source": "noaa_swpc",
            "runner": run_solar_collector,
            "series_names": ("sunspot_number", "solar_flux_f107", "geomagnetic_kp", "solar_flare_count"),
            "max_age": timedelta(hours=6),
            "timeout_seconds": 45,
        },
        {
            "name": "astronomy",
            "source": "derived_ephemeris",
            "runner": run_astronomy_collector,
            "series_names": ("lunar_cycle_day", "zodiac_index", "solar_longitude"),
            "max_age": timedelta(hours=24),
            "timeout_seconds": 30,
        },
        {
            "name": "natural",
            "source": "usgs,noaa_ncei,noaa_swpc,nasa_iswa_hapi",
            "runner": run_natural_collector,
            "series_names": (
                "earthquake_count",
                "global_temperature_anomaly",
                "geomagnetic_activity",
                "solar_wind_speed",
            ),
            "max_age": timedelta(hours=12),
            "timeout_seconds": 75,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh QMIS and render the operator console")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned operator-console steps and exit")
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Render the operator console from the existing DuckDB state without running collectors or analysis",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Refresh every collector even when recent data already exists",
    )
    return parser.parse_args(argv)


def _latest_signal_timestamp(db_path: Path, series_names: tuple[str, ...]) -> datetime | None:
    if not series_names:
        return None

    placeholders = ", ".join(["?"] * len(series_names))
    query = f"""
        SELECT MAX(ts)
        FROM signals
        WHERE series_name IN ({placeholders})
    """
    connection = connect_db(db_path, read_only=True)
    try:
        result = connection.execute(query, list(series_names)).fetchone()
    finally:
        connection.close()

    latest_ts = result[0] if result else None
    if latest_ts is None:
        return None
    if hasattr(latest_ts, "to_pydatetime"):
        latest_ts = latest_ts.to_pydatetime()
    if isinstance(latest_ts, datetime):
        return latest_ts.replace(tzinfo=UTC) if latest_ts.tzinfo is None else latest_ts.astimezone(UTC)
    return None


def _latest_collector_run_timestamp(db_path: Path, collector_name: str) -> datetime | None:
    connection = connect_db(db_path, read_only=True)
    try:
        result = connection.execute(
            """
            SELECT MAX(collected_at)
            FROM collector_runs
            WHERE collector_name = ? AND status = 'success'
            """,
            [collector_name],
        ).fetchone()
    except Exception:
        return None
    finally:
        connection.close()

    latest_ts = result[0] if result else None
    if latest_ts is None:
        return None
    if hasattr(latest_ts, "to_pydatetime"):
        latest_ts = latest_ts.to_pydatetime()
    if isinstance(latest_ts, datetime):
        return latest_ts.replace(tzinfo=UTC) if latest_ts.tzinfo is None else latest_ts.astimezone(UTC)
    return None


def _is_fresh(latest_ts: datetime | None, max_age: timedelta, now: datetime) -> bool:
    if latest_ts is None:
        return False
    return (now - latest_ts) <= max_age


def _record_collector_run(
    db_path: Path,
    *,
    collector_name: str,
    source: str,
    status: str,
    row_count: int,
    message: str,
    collected_at: datetime,
) -> None:
    stored_collected_at = collected_at.astimezone(UTC).replace(tzinfo=None)
    connection = connect_db(db_path)
    try:
        connection.execute(
            """
            INSERT INTO collector_runs (collector_name, source, collected_at, status, row_count, message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [collector_name, source, stored_collected_at, status, row_count, message],
        )
    finally:
        connection.close()


def _call_with_timeout(runner, db_path: Path, timeout_seconds: int) -> int:
    if timeout_seconds <= 0:
        return int(runner(db_path=db_path))

    if sys.platform == "win32":
        return int(runner(db_path=db_path))

    def _timeout_handler(_signum, _frame):
        raise TimeoutError(f"collector exceeded {timeout_seconds}s")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return int(runner(db_path=db_path))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _run_collectors(
    db_path: Path,
    *,
    console: Console,
    logger,
    force_refresh: bool,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    failures: list[str] = []
    now = datetime.now(UTC)

    for spec in _collector_specs():
        name = str(spec["name"])
        source = str(spec["source"])
        latest_ts = _latest_collector_run_timestamp(db_path, name) or _latest_signal_timestamp(
            db_path,
            tuple(spec["series_names"]),
        )
        if not force_refresh and _is_fresh(latest_ts, spec["max_age"], now):
            age_minutes = max(0, int((now - latest_ts).total_seconds() // 60)) if latest_ts else 0
            message = f"Skipping {name}: recent data available ({age_minutes}m old)"
            console.print(message)
            logger.info(message)
            counts[name] = 0
            continue

        start = time.monotonic()
        calling_message = f"Calling {name} ({source})"
        console.print(calling_message)
        logger.info(calling_message)

        try:
            inserted_rows = _call_with_timeout(spec["runner"], db_path=db_path, timeout_seconds=int(spec["timeout_seconds"]))
        except TimeoutError as exc:
            failure = f"Timed out {name}: {exc}"
            console.print(failure)
            logger.warning(failure)
            counts[name] = 0
            failures.append(failure)
            _record_collector_run(
                db_path,
                collector_name=name,
                source=source,
                status="timeout",
                row_count=0,
                message=str(exc),
                collected_at=datetime.now(UTC),
            )
            continue
        except Exception as exc:
            failure = f"Failed {name}: {exc}"
            console.print(failure)
            logger.warning(failure)
            counts[name] = 0
            failures.append(failure)
            _record_collector_run(
                db_path,
                collector_name=name,
                source=source,
                status="error",
                row_count=0,
                message=str(exc),
                collected_at=datetime.now(UTC),
            )
            continue

        duration_seconds = time.monotonic() - start
        completed_message = f"Completed {name}: rows={inserted_rows} duration={duration_seconds:.1f}s"
        console.print(completed_message)
        logger.info(completed_message)
        counts[name] = inserted_rows
        _record_collector_run(
            db_path,
            collector_name=name,
            source=source,
            status="success",
            row_count=inserted_rows,
            message=completed_message,
            collected_at=datetime.now(UTC),
        )

    return {
        "counts": counts,
        "failures": failures,
    }


def _refresh_pipeline(
    db_path: Path,
    *,
    console: Console,
    logger,
    force_refresh: bool,
) -> dict[str, Any]:
    collector_summary = _run_collectors(db_path, console=console, logger=logger, force_refresh=force_refresh)
    return {
        "collectors": collector_summary["counts"],
        "collector_failures": collector_summary["failures"],
        "features": int(materialize_features(db_path=db_path)),
        "regime": int(materialize_regime(db_path=db_path)),
        "breadth": int(materialize_breadth_health(db_path=db_path)),
        "liquidity": int(materialize_liquidity_state(db_path=db_path)),
        "factors": int(materialize_factors(db_path=db_path)),
        "relationships": int(materialize_relationships(db_path=db_path)),
        "stress": int(materialize_market_stress(db_path=db_path)),
        "macro_pressure": int(materialize_macro_pressure(db_path=db_path)),
        "cycles": int(materialize_cycle_snapshots(db_path=db_path)),
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
        f"Features: {summary['features']}  Regime: {summary['regime']}  Breadth: {summary['breadth']}  Liquidity: {summary['liquidity']}  Factors: {summary['factors']}  Stress: {summary['stress']}  "
        f"MPI: {summary['macro_pressure']}  Cycles: {summary['cycles']}  Relationships: {summary['relationships']}  Lead-lag: {summary['lead_lag']}  Alerts: {summary['alerts']}"
    )
    failures = summary.get("collector_failures", [])
    if failures:
        message = f"{message}\nFailures: {' | '.join(str(item) for item in failures)}"
    console.print(Panel(message, title="QMIS Refresh Summary", expand=False))


def main(argv: list[str] | None = None, console: Console | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_operator_console")
    console = console or Console()

    if args.dry_run:
        message = (
            f"QMIS operator console dry-run: repo={config.repo_root} db={config.db_path} "
            "collectors=all analysis=features,regime,breadth,liquidity,factors,relationships,stress,cycles,lead-lag alerts=materialize dashboard=render"
        )
        logger.info(message)
        print(message)
        return 0

    if not args.no_refresh:
        refresh_summary = _refresh_pipeline(
            config.db_path,
            console=console,
            logger=logger,
            force_refresh=args.force_refresh,
        )
        logger.info("Refreshed QMIS pipeline into %s", config.db_path)
        _render_refresh_summary(refresh_summary, console)

    snapshot = load_dashboard_snapshot(db_path=config.db_path)
    render_dashboard(snapshot, console=console)
    logger.info("Rendered operator console from %s", config.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

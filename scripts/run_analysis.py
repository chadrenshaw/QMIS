#!/usr/bin/env python3
"""Runtime entrypoint for QMIS analysis jobs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qmis.config import load_config
from qmis.features.normalization import materialize_features
from qmis.logger import get_logger
from qmis.scheduling import ANALYSIS_GROUPS, build_schedule_manifest, format_schedule_manifest
from qmis.signals.breadth import materialize_breadth_health
from qmis.signals.correlations import materialize_relationships
from qmis.signals.cycles import materialize_cycle_snapshots
from qmis.signals.factors import materialize_factors
from qmis.signals.leadlag import materialize_lead_lag_relationships
from qmis.signals.liquidity import materialize_liquidity_state
from qmis.signals.regime import materialize_regime
from qmis.signals.stress import materialize_market_stress


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QMIS analysis jobs")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned analysis job and exit")
    parser.add_argument(
        "--cadence",
        choices=tuple(ANALYSIS_GROUPS.keys()),
        default="daily",
        help="Run the named analysis cadence group",
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="Print the recommended cron/systemd job commands and exit",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_analysis")

    if args.list_jobs:
        manifest = build_schedule_manifest(config.repo_root)
        output = format_schedule_manifest(manifest, section="analysis")
        logger.info(output)
        print(output)
        return 0

    if args.dry_run:
        message = (
            f"QMIS analysis dry-run: cadence={args.cadence} "
            f"repo={config.repo_root} db={config.db_path}"
        )
        logger.info(message)
        print(message)
        return 0

    feature_rows = materialize_features(db_path=config.db_path)
    regime_rows = materialize_regime(db_path=config.db_path)
    breadth_rows = materialize_breadth_health(db_path=config.db_path)
    liquidity_rows = materialize_liquidity_state(db_path=config.db_path)
    factor_rows = materialize_factors(db_path=config.db_path)
    relationship_rows = materialize_relationships(db_path=config.db_path)
    stress_rows = materialize_market_stress(db_path=config.db_path)
    cycle_rows = materialize_cycle_snapshots(db_path=config.db_path)
    lead_lag_rows = materialize_lead_lag_relationships(db_path=config.db_path)
    logger.info(
        (
        "Materialized %s feature rows, %s regime rows, %s breadth rows, %s liquidity rows, %s factor rows, %s relationship rows, "
        "%s stress rows, %s cycle rows, and %s lead-lag rows into %s for cadence=%s"
        ),
        feature_rows,
        regime_rows,
        breadth_rows,
        liquidity_rows,
        factor_rows,
        relationship_rows,
        stress_rows,
        cycle_rows,
        lead_lag_rows,
        config.db_path,
        args.cadence,
    )
    print(
        f"QMIS analysis materialized {feature_rows} feature rows, "
        f"{regime_rows} regime rows, {breadth_rows} breadth rows, {liquidity_rows} liquidity rows, {factor_rows} factor rows, {relationship_rows} relationship rows, "
        f"{stress_rows} stress rows, {cycle_rows} cycle rows, and {lead_lag_rows} lead-lag rows into {config.db_path} for cadence={args.cadence}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

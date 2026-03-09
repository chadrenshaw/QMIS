#!/usr/bin/env python3
"""Runtime entrypoint for QMIS collector jobs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qmis.collectors.breadth import run_breadth_collector
from qmis.collectors.natural import run_natural_collector
from qmis.collectors.astronomy import run_astronomy_collector
from qmis.collectors.crypto import run_crypto_collector
from qmis.collectors.liquidity import run_liquidity_collector
from qmis.collectors.macro import run_macro_collector
from qmis.collectors.market import run_market_collector
from qmis.collectors.solar import run_solar_collector
from qmis.config import load_config
from qmis.logger import get_logger
from qmis.scheduling import COLLECTOR_GROUPS, build_schedule_manifest, format_schedule_manifest


def _collector_runners():
    return {
        "market": run_market_collector,
        "macro": run_macro_collector,
        "liquidity": run_liquidity_collector,
        "crypto": run_crypto_collector,
        "solar": run_solar_collector,
        "astronomy": run_astronomy_collector,
        "breadth": run_breadth_collector,
        "natural": run_natural_collector,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QMIS collectors")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned collector job and exit")
    parser.add_argument(
        "--cadence",
        choices=("all", *COLLECTOR_GROUPS.keys()),
        default="all",
        help="Run a specific collector cadence group or all groups",
    )
    parser.add_argument(
        "--list-jobs",
        action="store_true",
        help="Print the recommended cron/systemd job commands and exit",
    )
    return parser.parse_args(argv)


def _selected_collectors(cadence: str) -> tuple[str, ...]:
    if cadence == "all":
        names: list[str] = []
        for group in COLLECTOR_GROUPS.values():
            names.extend(group)
        return tuple(names)
    return COLLECTOR_GROUPS[cadence]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_collectors")

    if args.list_jobs:
        manifest = build_schedule_manifest(config.repo_root)
        output = format_schedule_manifest(manifest, section="collectors")
        logger.info(output)
        print(output)
        return 0

    if args.dry_run:
        selected = ",".join(_selected_collectors(args.cadence))
        message = (
            f"QMIS collectors dry-run: cadence={args.cadence} collectors={selected} "
            f"repo={config.repo_root} db={config.db_path}"
        )
        logger.info(message)
        print(message)
        return 0

    results: dict[str, int] = {}
    runners = _collector_runners()
    for collector_name in _selected_collectors(args.cadence):
        results[collector_name] = int(runners[collector_name](db_path=config.db_path))

    total_rows = sum(results.values())
    logger.info(
        "Persisted %s signal rows into %s for cadence=%s (%s)",
        total_rows,
        config.db_path,
        args.cadence,
        ", ".join(f"{name}={count}" for name, count in results.items()),
    )
    print(
        f"QMIS collectors inserted {total_rows} signal rows into {config.db_path} "
        f"for cadence={args.cadence} ({', '.join(f'{name}={count}' for name, count in results.items())})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

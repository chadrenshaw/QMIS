#!/usr/bin/env python3
"""Runtime entrypoint for QMIS alert jobs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qmis.config import load_config
from qmis.logger import get_logger
from qmis.alerts.engine import load_alert_snapshot, materialize_alerts
from qmis.scheduling import ALERT_GROUPS, build_schedule_manifest, format_schedule_manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QMIS alert jobs")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned alert job and exit")
    parser.add_argument(
        "--cadence",
        choices=tuple(ALERT_GROUPS.keys()),
        default="daily",
        help="Run the named alert cadence group",
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
    logger = get_logger("qmis.run_alerts")

    if args.list_jobs:
        manifest = build_schedule_manifest(config.repo_root)
        output = format_schedule_manifest(manifest, section="alerts")
        logger.info(output)
        print(output)
        return 0

    if args.dry_run:
        message = (
            f"QMIS alerts dry-run: cadence={args.cadence} "
            f"repo={config.repo_root} db={config.db_path}"
        )
        logger.info(message)
        print(message)
        return 0

    count = materialize_alerts(db_path=config.db_path)
    _, summary = load_alert_snapshot(db_path=config.db_path)
    message = (
        f"QMIS alerts run complete: cadence={args.cadence} count={count} "
        f"status={summary['status']} message={summary['message']}"
    )
    logger.info(message)
    print(message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

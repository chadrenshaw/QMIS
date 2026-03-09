#!/usr/bin/env python3
"""Runtime entrypoint for the QMIS Rich CLI dashboard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from qmis.config import load_config
from qmis.dashboard.cli import load_dashboard_snapshot, render_dashboard
from qmis.logger import get_logger


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render the QMIS CLI dashboard")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned dashboard job and exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_dashboard")

    if args.dry_run:
        message = f"QMIS dashboard dry-run: repo={config.repo_root} db={config.db_path}"
        logger.info(message)
        print(message)
        return 0

    snapshot = load_dashboard_snapshot(db_path=config.db_path)
    render_dashboard(snapshot)
    logger.info("Rendered QMIS dashboard from %s", config.db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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

from qmis.config import load_config
from qmis.logger import get_logger


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QMIS collectors")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned collector job and exit")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = load_config()
    logger = get_logger("qmis.run_collectors")

    if args.dry_run:
        message = f"QMIS collectors dry-run: repo={config.repo_root} db={config.db_path}"
        logger.info(message)
        print(message)
        return 0

    logger.info("Collector runtime scaffold is ready but collectors are not implemented yet.")
    print("QMIS collectors runtime scaffold is ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

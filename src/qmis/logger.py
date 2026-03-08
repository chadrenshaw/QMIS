"""Shared logging configuration for QMIS scripts."""

from __future__ import annotations

import logging


def configure_logging(level: int = logging.INFO) -> None:
    """Configure a simple process-wide logging format."""
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
    else:
        root_logger.setLevel(level)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger after ensuring logging is configured."""
    configure_logging()
    return logging.getLogger(name)

"""Quantitative Macro Intelligence System package."""

from pathlib import Path


__all__ = ["__version__", "package_root"]

__version__ = "0.1.0"


def package_root() -> Path:
    """Return the qmis package root directory."""
    return Path(__file__).resolve().parent

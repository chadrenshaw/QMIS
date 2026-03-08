"""Configuration helpers for QMIS runtime entrypoints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QMISConfig:
    """Repo-local configuration for the QMIS application."""

    repo_root: Path
    db_path: Path
    log_dir: Path
    data_dir: Path


def resolve_repo_root() -> Path:
    """Resolve the repository root from the src/qmis package layout."""
    return Path(__file__).resolve().parents[2]


def load_config() -> QMISConfig:
    """Load the default QMIS configuration rooted in the current repository."""
    repo_root = resolve_repo_root()
    return QMISConfig(
        repo_root=repo_root,
        db_path=repo_root / "db" / "qmis.duckdb",
        log_dir=repo_root / "logs",
        data_dir=repo_root / "data",
    )

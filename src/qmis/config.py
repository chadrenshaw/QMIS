"""Configuration helpers for QMIS runtime entrypoints."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class QMISConfig:
    """Repo-local configuration for the QMIS application."""

    repo_root: Path
    data_root: Path
    db_path: Path
    log_dir: Path
    data_dir: Path
    web_dist_dir: Path


def resolve_repo_root() -> Path:
    """Resolve the repository root from the src/qmis package layout."""
    return Path(__file__).resolve().parents[2]


def load_config() -> QMISConfig:
    """Load the default QMIS configuration rooted in the current repository."""
    repo_root = resolve_repo_root()
    data_root = Path(os.environ.get("QMIS_DATA_ROOT", repo_root))
    db_path = Path(os.environ.get("QMIS_DB_PATH", data_root / "db" / "qmis.duckdb"))
    log_dir = Path(os.environ.get("QMIS_LOG_DIR", data_root / "logs"))
    data_dir = Path(os.environ.get("QMIS_RUNTIME_DATA_DIR", data_root / "data"))
    web_dist_dir = Path(os.environ.get("QMIS_WEB_DIST_DIR", repo_root / "web" / "dist"))
    return QMISConfig(
        repo_root=repo_root,
        data_root=data_root,
        db_path=db_path,
        log_dir=log_dir,
        data_dir=data_dir,
        web_dist_dir=web_dist_dir,
    )

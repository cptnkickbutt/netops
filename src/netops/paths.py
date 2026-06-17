from __future__ import annotations

import os
from pathlib import Path


NETOPS_FILES_DIR_ENV = "NETOPS_FILES_DIR"
DEFAULT_FILES_DIR = Path("files")


def generated_files_dir() -> Path:
    configured = os.getenv(NETOPS_FILES_DIR_ENV, "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_FILES_DIR


def generated_file_path(filename: str | Path) -> Path:
    path = Path(filename)
    if path.is_absolute():
        return path
    return generated_files_dir() / path


def ensure_parent_dir(path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path

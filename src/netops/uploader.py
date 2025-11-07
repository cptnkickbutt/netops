# src/netops/uploader.py
from __future__ import annotations
from pathlib import Path
import posixpath
from .transports.sftp import sftp_upload_file
from .config import FileSvrCfg

def upload_to_file_server(
    local_path: Path,
    cfg: FileSvrCfg,
    *,
    remote_dir: str | None = None,
) -> str:
    """
    Upload a file to the SFTP server. The target directory is chosen in the CLI:
      - If remote_dir is provided, use it.
      - Else, use cfg.remote_dir (from .env).
    Returns the remote POSIX path to the uploaded file.
    """
    assert isinstance(cfg, FileSvrCfg)
    target_dir = (remote_dir or cfg.remote_dir).rstrip("/")
    remote_path = posixpath.join(target_dir, local_path.name)
    sftp_upload_file(
        host=cfg.host,
        port=getattr(cfg, "port", 22),
        username=cfg.username,
        password=cfg.resolve_password(),
        local_path=str(local_path),
        remote_path=remote_path,
        make_dirs=True,
    )
    return remote_path

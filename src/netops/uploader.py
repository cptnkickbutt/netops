# src/netops/uploader.py
from __future__ import annotations
from pathlib import Path
import posixpath
from .transports.ssh import make_ssh_client
from .transports.sftp import sftp_upload_file, ensure_dir_over_ssh
from .config import FileSvrCfg

__all__ = ["upload_to_file_server"]

def _join_remote(*parts: str) -> str:
    parts = [p.strip("/") for p in parts if p is not None]
    if not parts:
        return "/"
    return "/" + "/".join(parts).rstrip("/") + "/"

def upload_to_file_server(local_path: Path, cfg: FileSvrCfg, *, subdir: str) -> str:
    """
    Uploads a file to: {cfg.base_dir}/{subdir}/{filename}
    Ensures remote directories exist. Returns the full remote file path.
    """
    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    password = cfg.resolve_password()

    # Build remote dir and full path
    remote_dir = _join_remote(cfg.base(), subdir)
    remote_file = posixpath.join(remote_dir.rstrip("/"), local_path.name)

    ssh = make_ssh_client(cfg.host, cfg.port, cfg.user, password, timeout=10)
    try:
        ensure_dir_over_ssh(ssh, remote_dir)       # mkdir -p with quotes (RouterOS/posix)
        sftp = ssh.open_sftp()
        try:
            sftp.put(str(local_path), remote_file)
        finally:
            sftp.close()
    finally:
        ssh.close()

    return remote_file

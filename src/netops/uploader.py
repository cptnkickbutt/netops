# src/netops/uploader.py
from __future__ import annotations

from pathlib import Path
import posixpath

from .transports.ssh import make_ssh_client
from .transports.sftp import sftp_upload_file, ensure_dir_over_ssh
from .config import FileSvrCfg

__all__ = ["upload_to_file_server", "remove_from_file_server", "upload_then_optionally_delete"]


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
        ensure_dir_over_ssh(ssh, remote_dir)
        sftp = ssh.open_sftp()
        try:
            # Keeping your existing style (direct put)
            sftp.put(str(local_path), remote_file)
        finally:
            sftp.close()
    finally:
        ssh.close()

    return remote_file


def remove_from_file_server(cfg: FileSvrCfg, remote_file: str) -> bool:
    """
    Best-effort delete of a remote file on the file server.
    Returns True if delete succeeded, False otherwise.
    """
    password = cfg.resolve_password()

    ssh = make_ssh_client(cfg.host, cfg.port, cfg.user, password, timeout=10)
    try:
        sftp = ssh.open_sftp()
        try:
            sftp.remove(remote_file)
            return True
        except Exception:
            return False
        finally:
            try:
                sftp.close()
            except Exception:
                pass
    finally:
        try:
            ssh.close()
        except Exception:
            pass


def upload_then_optionally_delete(local_path: Path, cfg: FileSvrCfg, *, subdir: str, delete_after: bool) -> str:
    """
    Convenience wrapper for testing mode: upload to prove connectivity,
    then delete the remote file if requested.
    Returns remote_file path (even if deletion succeeds).
    """
    remote_file = upload_to_file_server(local_path, cfg, subdir=subdir)
    if delete_after:
        remove_from_file_server(cfg, remote_file)
    return remote_file

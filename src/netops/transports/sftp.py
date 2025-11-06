from __future__ import annotations
from typing import List
import paramiko

__all__ = ["ensure_dir_over_ssh", "sftp_listdir"]

def ensure_dir_over_ssh(ssh_client: paramiko.SSHClient, path: str) -> None:
    """
    Best-effort ensure a remote directory exists using the SSH shell.
    Works even if SFTP chdir/mkdir permissions are restricted.
    """
    try:
        ssh_client.exec_command(f"mkdir -p {path}")
    except Exception:
        # Swallow errors; callers should still attempt put and handle failures explicitly
        pass

def sftp_listdir(sftp: paramiko.SFTPClient, remote_dir: str = ".") -> List[str]:
    """List remote directory contents, returning [] on failure."""
    try:
        return sftp.listdir(remote_dir)
    except Exception:
        return []

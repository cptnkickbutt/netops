# src/netops/transports/sftp.py
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Tuple, Optional, List

import os
import posixpath


__all__ = [
    "ensure_dir_over_ssh",
    "sftp_listdir",
    "sftp_download_file",
    "sftp_download_dir",
    "sftp_upload_file",
]


# ---------------------------
# Internal helpers
# ---------------------------

def _norm_remote(p: str) -> str:
    """Ensure POSIX-style remote paths."""
    return (p or "").replace("\\", "/")

@contextmanager
def _sftp_open(host: str, port: int, username: str, password: str):
    """
    Yield a connected (transport, sftp) pair. Always closes cleanly.
    """
    import paramiko
    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((host, int(port)))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        yield transport, sftp
    finally:
        try:
            if sftp:
                sftp.close()
        finally:
            try:
                if transport:
                    transport.close()
            except Exception:
                pass

def _remote_mkdirs(sftp, remote_dir: str) -> None:
    """
    Recursively mkdir on remote. Ignores dirs that already exist.
    """
    parts = _norm_remote(remote_dir).strip("/").split("/")
    if not parts or parts == [""]:
        return
    cur = ""
    for part in parts:
        cur = f"{cur}/{part}" if cur else f"/{part}"
        try:
            sftp.stat(cur)
        except Exception:
            try:
                sftp.mkdir(cur)
            except Exception:
                # race or permission; next ops will raise if truly missing
                pass


# ---------------------------
# Public API
# ---------------------------

def ensure_dir_over_ssh(ssh_client, path: str) -> None:
    """
    Best-effort remote mkdir using an existing SSH client.
    For MikroTik RouterOS, 'file make-dir' is used; for other shells, 'mkdir -p'.
    """
    remote = _norm_remote(path)
    try:
        # Try MikroTik RouterOS command first
        ssh_client.exec_command(f'/file make-dir "{remote}"')
    except Exception:
        try:
            ssh_client.exec_command(f'mkdir -p "{remote}"')
        except Exception:
            # Non-fatal; upload may still succeed if parent exists
            pass


def sftp_listdir(sftp, remote_dir: str = ".") -> List[str]:
    """
    List names in a remote directory using an *existing* SFTP handle.
    """
    try:
        return sftp.listdir(_norm_remote(remote_dir))
    except Exception:
        return []


def sftp_download_file(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_path: str,
    local_path: str,
) -> None:
    """
    Download a single file from `remote_path` to `local_path`.
    """
    lp = Path(local_path)
    lp.parent.mkdir(parents=True, exist_ok=True)
    with _sftp_open(host, port, username, password) as (_, sftp):
        sftp.get(_norm_remote(remote_path), str(lp))


def sftp_download_dir(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    local_dir: str,
    *,
    skip_hidden: bool = True,
) -> int:
    """
    Recursively download a directory tree. Returns number of files copied.
    """
    import stat
    base = Path(local_dir)
    base.mkdir(parents=True, exist_ok=True)

    count = 0
    rroot = _norm_remote(remote_dir).rstrip("/") or "/"

    with _sftp_open(host, port, username, password) as (_, sftp):

        def walk(rdir: str, ldir: Path):
            nonlocal count
            ldir.mkdir(parents=True, exist_ok=True)
            try:
                for ent in sftp.listdir_attr(rdir):
                    name = ent.filename
                    if skip_hidden and name.startswith("."):
                        continue
                    rpath = f"{rdir.rstrip('/')}/{name}"
                    lpath = ldir / name
                    mode = getattr(ent, "st_mode", 0)
                    if stat.S_ISDIR(mode):
                        walk(rpath, lpath)
                    else:
                        sftp.get(rpath, str(lpath))
                        count += 1
            except Exception:
                # missing dir or permission; skip
                return

        walk(rroot, base)

    return count


def sftp_upload_file(
    host: str,
    port: int,
    username: str,
    password: str,
    local_path: str,
    remote_path: str,
    *,
    make_dirs: bool = True,
) -> None:
    """
    Upload a single file to the server. If `make_dirs` is True, create
    remote parents as needed.
    """
    lp = Path(local_path)
    if not lp.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    rpath = _norm_remote(remote_path)
    rdir = posixpath.dirname(rpath) or "/"

    with _sftp_open(host, port, username, password) as (_, sftp):
        if make_dirs:
            _remote_mkdirs(sftp, rdir)
        sftp.put(str(lp), rpath)

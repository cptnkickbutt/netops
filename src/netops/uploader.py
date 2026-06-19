# src/netops/uploader.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import posixpath
import re

from .transports.ssh import make_ssh_client
from .transports.sftp import sftp_upload_file, ensure_dir_over_ssh
from .config import FileSvrCfg

__all__ = [
    "DailyExportCleanupResult",
    "cleanup_old_daily_exports",
    "upload_to_file_server",
    "remove_from_file_server",
    "upload_then_optionally_delete",
]


_DAILY_EXPORT_ZIP_RE = re.compile(r"^(?P<day>\d{4}-\d{2}-\d{2})_Daily_Exports(?:_SAFE)?\.zip$", re.I)


@dataclass(frozen=True)
class DailyExportCleanupResult:
    remote_dir: str
    cutoff_date: date
    retention_days: int
    dry_run: bool
    scanned: int
    matched: int
    expired: list[str]
    deleted: list[str]
    failed: list[str]


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


def cleanup_old_daily_exports(
    cfg: FileSvrCfg,
    *,
    subdir: str,
    retention_days: int,
    dry_run: bool = False,
) -> DailyExportCleanupResult:
    """
    Delete old daily-export zip files from the file server.

    Only files named like YYYY-MM-DD_Daily_Exports.zip or
    YYYY-MM-DD_Daily_Exports_SAFE.zip are eligible.
    """
    if retention_days < 1:
        raise ValueError("retention_days must be at least 1")

    password = cfg.resolve_password()
    remote_dir = _join_remote(cfg.base(), subdir)
    cutoff_date = datetime.now().date() - timedelta(days=retention_days)

    scanned = 0
    matched = 0
    expired: list[str] = []
    deleted: list[str] = []
    failed: list[str] = []

    ssh = make_ssh_client(cfg.host, cfg.port, cfg.user, password, timeout=10)
    try:
        sftp = ssh.open_sftp()
        try:
            for ent in sftp.listdir_attr(remote_dir):
                scanned += 1
                filename = ent.filename
                file_date = _daily_export_zip_date(filename)
                if file_date is None:
                    continue

                matched += 1
                if file_date >= cutoff_date:
                    continue

                remote_file = posixpath.join(remote_dir.rstrip("/"), filename)
                expired.append(remote_file)
                if dry_run:
                    continue

                try:
                    sftp.remove(remote_file)
                    deleted.append(remote_file)
                except Exception:
                    failed.append(remote_file)
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

    return DailyExportCleanupResult(
        remote_dir=remote_dir,
        cutoff_date=cutoff_date,
        retention_days=retention_days,
        dry_run=dry_run,
        scanned=scanned,
        matched=matched,
        expired=expired,
        deleted=deleted,
        failed=failed,
    )


def _daily_export_zip_date(filename: str) -> date | None:
    match = _DAILY_EXPORT_ZIP_RE.match(filename)
    if not match:
        return None

    try:
        return datetime.strptime(match.group("day"), "%Y-%m-%d").date()
    except ValueError:
        return None


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

#!/usr/bin/env python3
"""
callrec_sorter.py  (full drop-in)

What it does
------------
1) SFTPs into your file server using your existing .env (FILESERV_*).
2) Ensures today's folder exists:
      <FILESERV_BASE_DIR>/WIOGEN-CX/YYYY/MM/DD
3) Moves ALL files from the CX dump folder:
      <FILESERV_BASE_DIR>/File_Server/Call_Recordings
   into today's folder (server-side rename when possible).
4) Optional retention cleanup (default: 2 years) for BOTH:
      WIOGEN-CX and WIOGEN-TS
   Deletes day folders older than retention and prunes empty month/year folders.

.env expected (you already have FILESERV_*):
-------------------------------------------
FILESERV_HOST=10.100.3.9
FILESERV_USER=automations
FILESERV_PASSWORD=...

FILESERV_BASE_DIR=/mnt/TelcomFS/

# Call recordings
CALLREC_CX_SOURCE=File_Server/Call_Recordings
CALLREC_CX_ROOT=WIOGEN-CX
CALLREC_VIP_ROOT=WIOGEN-TS
CALLREC_RETENTION_YEARS=2

Install:
--------
pip install paramiko python-dotenv

Run:
----
python3 callrec_sorter.py --dry-run --verbose
python3 callrec_sorter.py
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import socket
import stat
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import paramiko
from dotenv import load_dotenv


# -----------------------------
# Defaults / Regex
# -----------------------------

DEFAULT_TZ = timezone.utc
YEAR_RE = re.compile(r"^\d{4}$")
MONTH_RE = re.compile(r"^\d{2}$")
DAY_RE = re.compile(r"^\d{2}$")


@dataclass(frozen=True)
class SFTPConfig:
    host: str
    port: int
    username: str
    password: Optional[str]
    pkey_path: Optional[str]
    pkey_passphrase: Optional[str]
    known_hosts: Optional[str]
    strict_host_key: bool
    connect_timeout_s: int


@dataclass(frozen=True)
class PathsConfig:
    cx_source_dir: str
    cx_root: str
    vip_root: Optional[str]


class SingleInstanceLock:
    """Simple local lock to avoid overlapping cron runs."""
    def __init__(self, lock_path: str):
        self.lock_path = lock_path
        self.fd = None

    def acquire(self) -> None:
        os.makedirs(os.path.dirname(self.lock_path), exist_ok=True)
        self.fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(self.fd, str(os.getpid()).encode("utf-8"))

    def release(self) -> None:
        try:
            if self.fd is not None:
                os.close(self.fd)
        finally:
            self.fd = None
            try:
                os.unlink(self.lock_path)
            except FileNotFoundError:
                pass


# -----------------------------
# Helpers
# -----------------------------

def utc_now() -> datetime:
    return datetime.now(tz=DEFAULT_TZ)


def date_parts(dt: datetime) -> Tuple[str, str, str]:
    return (f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}")


def normalize_remote_path(p: str) -> str:
    p = p.replace("\\", "/")
    if len(p) > 1:
        p = p.rstrip("/")
    return p


def join_remote(*parts: str) -> str:
    """
    Join path parts into a POSIX absolute path.
    Accepts absolute-ish inputs ("/mnt/TelcomFS") and relative ("WIOGEN-CX").
    """
    cleaned = []
    for part in parts:
        if part is None:
            continue
        part = part.replace("\\", "/").strip("/")
        if part:
            cleaned.append(part)
    return "/" + "/".join(cleaned)


def safe_name(name: str) -> bool:
    return name not in (".", "..") and name != ""


def is_regular(attr: paramiko.SFTPAttributes) -> bool:
    return stat.S_ISREG(attr.st_mode)


def is_dir(attr: paramiko.SFTPAttributes) -> bool:
    return stat.S_ISDIR(attr.st_mode)


def remote_exists(sftp: paramiko.SFTPClient, p: str) -> bool:
    try:
        sftp.stat(p)
        return True
    except FileNotFoundError:
        return False


def build_logger(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("callrec_sorter")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger


# -----------------------------
# SSH/SFTP
# -----------------------------

def build_ssh_client(cfg: SFTPConfig, logger: logging.Logger) -> paramiko.SSHClient:
    client = paramiko.SSHClient()

    if cfg.strict_host_key:
        if not cfg.known_hosts:
            raise ValueError("strict_host_key=True requires known_hosts path.")
        client.load_host_keys(cfg.known_hosts)
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    pkey = None
    if cfg.pkey_path:
        key_excs = []
        for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
            try:
                pkey = key_cls.from_private_key_file(cfg.pkey_path, password=cfg.pkey_passphrase)
                break
            except Exception as e:
                key_excs.append(e)
        if pkey is None:
            raise RuntimeError(f"Failed to load private key {cfg.pkey_path}: {key_excs[-1]}")

    logger.info("Connecting to %s:%s as %s", cfg.host, cfg.port, cfg.username)
    client.connect(
        hostname=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password if not pkey else None,
        pkey=pkey,
        timeout=cfg.connect_timeout_s,
        banner_timeout=cfg.connect_timeout_s,
        auth_timeout=cfg.connect_timeout_s,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def ensure_remote_dir(sftp: paramiko.SFTPClient, path: str, logger: logging.Logger, dry_run: bool) -> None:
    path = normalize_remote_path(path)
    if path == "/":
        return

    parts = [p for p in path.split("/") if p]
    cur = "/"
    for part in parts:
        cur = join_remote(cur, part)
        try:
            sftp.stat(cur)
        except FileNotFoundError:
            logger.info("Creating remote dir: %s", cur)
            if not dry_run:
                sftp.mkdir(cur)


def listdir_attr_safe(sftp: paramiko.SFTPClient, path: str) -> List[paramiko.SFTPAttributes]:
    return [a for a in sftp.listdir_attr(path) if safe_name(a.filename)]


# -----------------------------
# CX Move
# -----------------------------

def move_cx_files(
    sftp: paramiko.SFTPClient,
    paths: PathsConfig,
    today: datetime,
    logger: logging.Logger,
    dry_run: bool,
) -> Tuple[int, int]:
    src = normalize_remote_path(paths.cx_source_dir)

    # IMPORTANT: do not create today's folder unless there are new files to move.
    try:
        items = listdir_attr_safe(sftp, src)
    except FileNotFoundError:
        logger.error("CX source directory not found: %s", src)
        return (0, 0)

    files = [a for a in items if is_regular(a)]
    if not files:
        logger.info("No CX files found in source: %s (no folder created)", src)
        return (0, 0)

    y, m, d = date_parts(today)
    dest_day_dir = join_remote(paths.cx_root, y, m, d)
    ensure_remote_dir(sftp, dest_day_dir, logger, dry_run)


    logger.info("Found %d CX files to process.", len(files))

    moved = 0
    skipped = 0

    for a in files:
        filename = a.filename
        src_path = join_remote(src, filename)
        dest_path = join_remote(dest_day_dir, filename)

        if remote_exists(sftp, dest_path):
            logger.warning("Destination already exists; skipping (left in source): %s", dest_path)
            skipped += 1
            continue

        logger.info("Moving: %s -> %s", src_path, dest_path)
        if dry_run:
            moved += 1
            continue

        # Prefer server-side move
        try:
            sftp.rename(src_path, dest_path)
            moved += 1
        except Exception as e:
            # Fall back to copy+remove (slower but reliable)
            logger.error("Rename failed (%s). Falling back to copy+remove: %s", e, filename)
            import tempfile
            tmp_local = None
            try:
                fd, tmp_local = tempfile.mkstemp(prefix="cx_move_", suffix=".bin")
                os.close(fd)
                sftp.get(src_path, tmp_local)
                sftp.put(tmp_local, dest_path)
                sftp.remove(src_path)
                moved += 1
            finally:
                if tmp_local and os.path.exists(tmp_local):
                    try:
                        os.remove(tmp_local)
                    except Exception:
                        pass

    return (moved, skipped)


# -----------------------------
# Retention Cleanup
# -----------------------------

def parse_ymd(y: str, m: str, d: str) -> Optional[datetime]:
    if not (YEAR_RE.match(y) and MONTH_RE.match(m) and DAY_RE.match(d)):
        return None
    try:
        return datetime(int(y), int(m), int(d), tzinfo=DEFAULT_TZ)
    except ValueError:
        return None


def remove_remote_tree(sftp: paramiko.SFTPClient, root: str, logger: logging.Logger, dry_run: bool) -> None:
    try:
        for a in listdir_attr_safe(sftp, root):
            p = join_remote(root, a.filename)
            if is_dir(a):
                remove_remote_tree(sftp, p, logger, dry_run)
            else:
                logger.info("Deleting file: %s", p)
                if not dry_run:
                    sftp.remove(p)
        logger.info("Deleting dir: %s", root)
        if not dry_run:
            sftp.rmdir(root)
    except FileNotFoundError:
        return


def cleanup_empty_parents(sftp: paramiko.SFTPClient, base_root: str, logger: logging.Logger, dry_run: bool) -> None:
    base_root = normalize_remote_path(base_root)

    try:
        years = [a for a in listdir_attr_safe(sftp, base_root) if is_dir(a) and YEAR_RE.match(a.filename)]
    except FileNotFoundError:
        return

    for y in years:
        ypath = join_remote(base_root, y.filename)

        try:
            months = [a for a in listdir_attr_safe(sftp, ypath) if is_dir(a) and MONTH_RE.match(a.filename)]
        except FileNotFoundError:
            continue

        for m in months:
            mpath = join_remote(ypath, m.filename)
            try:
                if not sftp.listdir(mpath):
                    logger.info("Removing empty month dir: %s", mpath)
                    if not dry_run:
                        sftp.rmdir(mpath)
            except FileNotFoundError:
                pass

        try:
            if not sftp.listdir(ypath):
                logger.info("Removing empty year dir: %s", ypath)
                if not dry_run:
                    sftp.rmdir(ypath)
        except FileNotFoundError:
            pass


def enforce_retention(
    sftp: paramiko.SFTPClient,
    roots: Iterable[str],
    keep_days: int,
    logger: logging.Logger,
    dry_run: bool,
) -> int:
    cutoff = utc_now() - timedelta(days=keep_days)
    cutoff_date = cutoff.date()
    deleted = 0

    for root in roots:
        if not root:
            continue
        root = normalize_remote_path(root)

        logger.info("Retention check under: %s (cutoff: %s)", root, cutoff_date)

        try:
            years = [a for a in listdir_attr_safe(sftp, root) if is_dir(a) and YEAR_RE.match(a.filename)]
        except FileNotFoundError:
            logger.warning("Root not found (skipping): %s", root)
            continue

        for y in years:
            ypath = join_remote(root, y.filename)
            try:
                months = [a for a in listdir_attr_safe(sftp, ypath) if is_dir(a) and MONTH_RE.match(a.filename)]
            except FileNotFoundError:
                continue

            for m in months:
                mpath = join_remote(ypath, m.filename)
                try:
                    days = [a for a in listdir_attr_safe(sftp, mpath) if is_dir(a) and DAY_RE.match(a.filename)]
                except FileNotFoundError:
                    continue

                for d in days:
                    dt = parse_ymd(y.filename, m.filename, d.filename)
                    if not dt:
                        continue

                    if dt.date() < cutoff_date:
                        dpath = join_remote(mpath, d.filename)
                        logger.info("Deleting old day folder: %s (date=%s)", dpath, dt.date())
                        if not dry_run:
                            remove_remote_tree(sftp, dpath, logger, dry_run)
                        deleted += 1

        cleanup_empty_parents(sftp, root, logger, dry_run)

    return deleted


# -----------------------------
# Env + Args
# -----------------------------

def load_env(dotenv_path: Optional[str], logger: logging.Logger) -> None:
    if dotenv_path:
        p = Path(dotenv_path).expanduser().resolve()
        load_dotenv(p)
        logger.debug("Loaded .env: %s", p)
    else:
        load_dotenv()  # default: .env in cwd (matches your current runtime style)
        logger.debug("Loaded default .env from cwd (if present).")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sort CX recordings into WIOGEN-CX/YYYY/MM/DD and enforce retention.")
    p.add_argument("--dotenv", default=None, help="Path to .env file (default: .env in current working directory).")
    p.add_argument("--dry-run", action="store_true", help="Show actions without making changes.")
    p.add_argument("--no-retention", action="store_true", help="Disable retention cleanup.")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--lock-file", default=os.getenv("CALLREC_LOCK_FILE", os.path.join(os.getcwd(), ".callrec_sorter.lock")))
    return p.parse_args()


def cfg_from_env(logger: logging.Logger) -> Tuple[SFTPConfig, PathsConfig, int]:
    # SFTP creds (re-using your FILESERV_* block)
    host = os.getenv("FILESERV_HOST", "").strip()
    user = os.getenv("FILESERV_USER", "").strip()
    password = os.getenv("FILESERV_PASSWORD")
    base_dir = os.getenv("FILESERV_BASE_DIR", "").strip()

    if not host:
        raise ValueError("Missing FILESERV_HOST in .env")
    if not user:
        raise ValueError("Missing FILESERV_USER in .env")
    if not password and not os.getenv("FILESERV_PKEY"):
        raise ValueError("Missing FILESERV_PASSWORD (or FILESERV_PKEY) in .env")
    if not base_dir:
        raise ValueError("Missing FILESERV_BASE_DIR in .env (e.g. /mnt/TelcomFS/)")

    base_dir = base_dir.rstrip("/")

    sftp_cfg = SFTPConfig(
        host=host,
        port=int(os.getenv("FILESERV_PORT", "22")),
        username=user,
        password=password,
        pkey_path=os.getenv("FILESERV_PKEY"),
        pkey_passphrase=os.getenv("FILESERV_PKEY_PASSPHRASE"),
        known_hosts=os.getenv("FILESERV_KNOWN_HOSTS"),
        strict_host_key=os.getenv("FILESERV_STRICT_HOST_KEY", "0") == "1",
        connect_timeout_s=int(os.getenv("FILESERV_TIMEOUT", "15")),
    )

    # Call recordings paths (relative to base_dir by default)
    cx_source_rel = os.getenv("CALLREC_CX_SOURCE", "File_Server/Call_Recordings").strip()
    cx_root_rel = os.getenv("CALLREC_CX_ROOT", "WIOGEN-CX").strip()  # <-- requested hyphen name
    vip_root_rel = os.getenv("CALLREC_VIP_ROOT", "WIOGEN-TS").strip()

    paths = PathsConfig(
        cx_source_dir=join_remote(base_dir, cx_source_rel),
        cx_root=join_remote(base_dir, cx_root_rel),
        vip_root=join_remote(base_dir, vip_root_rel) if vip_root_rel else None,
    )

    retention_years = int(os.getenv("CALLREC_RETENTION_YEARS", "2"))
    keep_days = retention_years * 365

    logger.info("Resolved CX source: %s", paths.cx_source_dir)
    logger.info("Resolved CX root:   %s", paths.cx_root)
    if paths.vip_root:
        logger.info("Resolved VIP root:  %s", paths.vip_root)
    logger.info("Retention: %d years (~%d days)", retention_years, keep_days)

    return sftp_cfg, paths, keep_days


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    args = parse_args()
    logger = build_logger(args.verbose)

    load_env(args.dotenv, logger)

    lock = SingleInstanceLock(args.lock_file)
    try:
        lock.acquire()
    except FileExistsError:
        logger.warning("Another instance appears to be running (lock exists): %s", args.lock_file)
        return 2

    try:
        sftp_cfg, paths, keep_days = cfg_from_env(logger)

        ssh = build_ssh_client(sftp_cfg, logger)
        try:
            sftp = ssh.open_sftp()

            moved, skipped = move_cx_files(sftp, paths, utc_now(), logger, dry_run=args.dry_run)
            logger.info("CX move summary: moved=%d, skipped=%d", moved, skipped)

            if not args.no_retention:
                roots = [paths.cx_root]
                if paths.vip_root:
                    roots.append(paths.vip_root)
                deleted = enforce_retention(sftp, roots, keep_days, logger, dry_run=args.dry_run)
                logger.info("Retention summary: day-folders deleted=%d", deleted)
            else:
                logger.info("Retention disabled (--no-retention).")

            return 0
        finally:
            try:
                sftp.close()
            except Exception:
                pass
            ssh.close()

    except (paramiko.SSHException, socket.error) as e:
        logger.error("SFTP/SSH connection error: %s", e)
        return 1
    except Exception as e:
        logger.exception("Unhandled error: %s", e)
        return 1
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())

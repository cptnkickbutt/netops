# src/netops/cli/daily_export.py
from __future__ import annotations

import os
import zipfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import click
import asyncio

from ..inventory import load_inventory_csv, select, Device
from ..config import load_env, resolve_env, FileSvrCfg
from ..logging import setup_logging, get_logger
from ..uploader import upload_to_file_server, remove_from_file_server
from ..emailer import send_email_with_attachment
from ..transports.ssh import make_ssh_client, ssh_exec


# --------- blocking helpers (run in threads) ---------

def _sftp_read_bytes(sftp, remote_path: str) -> bytes:
    with sftp.open(remote_path, "rb") as f:
        return f.read()


def _try_remove_ros_file(ssh, sftp, name: str) -> None:
    """
    Best-effort remove of a RouterOS file by name.
    Prefer SFTP remove; fallback to RouterOS /file remove.
    """
    try:
        sftp.remove(name)
        return
    except Exception:
        pass
    try:
        ssh_exec(ssh, f'/file remove [ find name="{name}" ]')
    except Exception:
        pass


def _find_hotspot_base(sftp) -> Optional[str]:
    try:
        sftp.listdir("hotspot")
        return "hotspot"
    except Exception:
        pass
    try:
        sftp.listdir("/flash/hotspot")
        return "/flash/hotspot"
    except Exception:
        return None


def _sftp_walk_read_all_files(sftp, base: str) -> Dict[str, bytes]:
    """
    Recursively read all files under `base` into memory.
    Returns paths relative to `base`, prefixed with hotspot/.
    """
    collected: Dict[str, bytes] = {}

    def _join(a: str, b: str) -> str:
        if a.endswith("/"):
            return a + b
        return a + "/" + b

    def _walk(dir_path: str) -> None:
        try:
            entries = sftp.listdir_attr(dir_path)
        except Exception:
            return

        for ent in entries:
            name = ent.filename
            full = _join(dir_path, name)

            is_dir = False
            try:
                is_dir = bool(ent.st_mode & 0o040000)
            except Exception:
                try:
                    sftp.listdir(full)
                    is_dir = True
                except Exception:
                    is_dir = False

            if is_dir:
                _walk(full)
            else:
                try:
                    collected[full] = _sftp_read_bytes(sftp, full)
                except Exception:
                    pass

    _walk(base)

    normalized: Dict[str, bytes] = {}
    base_norm = base.rstrip("/")
    for full, data in collected.items():
        rel = full
        if rel.startswith(base_norm + "/"):
            rel = rel[len(base_norm) + 1 :]
        normalized[f"hotspot/{rel}"] = data
    return normalized


def _collect_one_blocking(dev: Device, *, delete_remote_logs: bool) -> Tuple[str, Dict[str, bytes], Dict[str, bytes], Dict[str, bytes], str, Optional[str]]:
    """
    Single-connection collector:
    Returns (site_name, hash_logs_dict, changelog_logs_dict, hotspot_dict, export_text, error_str|None)

    - Always exports
    - Logs + hotspot only if 'backup' role present
    """
    log = get_logger()
    prop, ip = dev.site, dev.mgmt_ip

    # creds
    try:
        user, pw = resolve_env(dev.user_env, dev.pw_env)
    except Exception as e:
        msg = f"Env resolve failed: {e}"
        log.error(f"{prop}: {msg}")
        return prop, {}, {}, {}, "", msg

    ssh = make_ssh_client(ip, 22, user, pw)
    sftp = None
    try:
        try:
            ssh_exec(ssh, "setline 0")
        except Exception:
            pass

        # --- EXPORT ---
        try:
            ssh_exec(ssh, r'/export file="__netops_export__"')
        except Exception as e:
            msg = f"Export failed: {e}"
            log.error(f"{prop}: {msg}")
            return prop, {}, {}, {}, "", msg

        try:
            sftp = ssh.open_sftp()
            export_bytes = _sftp_read_bytes(sftp, "__netops_export__.rsc")
            export_text = export_bytes.decode("utf-8", "ignore")
        except Exception as e:
            msg = f"Export download failed: {e}"
            log.error(f"{prop}: {msg}")
            return prop, {}, {}, {}, "", msg
        finally:
            if sftp is not None:
                _try_remove_ros_file(ssh, sftp, "__netops_export__.rsc")

        hash_logs: Dict[str, bytes] = {}
        changelog_logs: Dict[str, bytes] = {}
        hotspot: Dict[str, bytes] = {}

        # --- LOGS / HOTSPOT (backup role only) ---
        if dev.has_role("backup"):
            # Ensure SFTP is open
            try:
                if sftp is None:
                    sftp = ssh.open_sftp()
            except Exception as e:
                log.warning(f"{prop}: SFTP open failed for backup pulls: {e}")
                return prop, {}, {}, {}, export_text, None

            try:
                names = set(sftp.listdir("."))
            except Exception:
                names = set()

            present_for_delete: List[str] = []

            # Hash logs: log.N.txt + Hashlog.N.txt
            for i in range(0, 100):
                for prefix in ("log", "Hashlog"):
                    fname = f"{prefix}.{i}.txt"
                    if fname not in names:
                        continue
                    try:
                        hash_logs[fname] = _sftp_read_bytes(sftp, fname)
                        present_for_delete.append(fname)
                    except Exception:
                        pass

            # Changelog logs: Changelog.N.txt
            for i in range(0, 100):
                fname = f"Changelog.{i}.txt"
                if fname not in names:
                    continue
                try:
                    changelog_logs[fname] = _sftp_read_bytes(sftp, fname)
                    present_for_delete.append(fname)
                except Exception:
                    pass

            if delete_remote_logs:
                for fname in present_for_delete:
                    _try_remove_ros_file(ssh, sftp, fname)

            # Hotspot: never delete remote; read recursively if present
            try:
                base = _find_hotspot_base(sftp)
                if base:
                    hotspot = _sftp_walk_read_all_files(sftp, base)
            except Exception as e:
                log.warning(f"{prop}: hotspot pull failed: {e}")

        log.debug(
            f"{prop}: export + {len(hash_logs)} hash log(s) + {len(changelog_logs)} changelog file(s) + {len(hotspot)} hotspot file(s)"
        )
        return prop, hash_logs, changelog_logs, hotspot, export_text, None

    finally:
        try:
            if sftp is not None:
                sftp.close()
        except Exception:
            pass
        try:
            ssh.close()
        except Exception:
            pass


# --------- worker (async wrapper; uses threads for paramiko) ---------

async def _collect_one(dev: Device, *, delete_remote_logs: bool):
    log = get_logger()
    try:
        return await asyncio.to_thread(_collect_one_blocking, dev, delete_remote_logs=delete_remote_logs)
    except Exception as e:
        msg = f"Unhandled error: {e}"
        log.error(f"{dev.site}: {msg}")
        return dev.site, {}, {}, {}, "", msg


# --------- CLI ---------

@click.command("daily-export")
@click.option("-I", "--inventory", "inventory_path", default="inventory.csv",
              help="Unified inventory CSV (Site,Device,MgmtIP,System,Roles,Access,Port,UserEnv,PwEnv,Enabled,Notes).")
@click.option("-s", "--single", is_flag=True, help="Interactively select properties.")
@click.option("--roles", default="firewall,export,backup",
              help="Comma list of roles to include (default: firewall,export,backup).")
@click.option("--no-email", is_flag=True, help="Email only Eric instead of full distro.")
@click.option("--keep", is_flag=True, help="Keep local zip(s) (skip cleanup).")
@click.option("--keep-remote-logs", is_flag=True,
              help="Do NOT delete remote log.N.txt / Changelog.N.txt after download (debug/test).")
@click.option("--testing", is_flag=True,
              help="Testing mode: implies --no-email + --keep-remote-logs + --no-progress + --log-level DEBUG; "
                   "uploads and then deletes remote zip to validate connectivity.")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Show overall progress bar.")
@click.option("--concurrency", default=6, show_default=True, type=click.IntRange(1, 64),
              help="Number of properties to collect in parallel.")
@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]))
def daily_export_cli(inventory_path, single, roles, no_email, keep, keep_remote_logs, testing, progress, concurrency, log_file, log_level):
    """
    Daily exports with unified inventory + tags:
      - Exports run for devices matching --roles (default: firewall,export,backup)
      - Logs + hotspot only when device has the 'backup' role
    """
    # testing overrides
    if testing:
        no_email = True
        keep_remote_logs = True
        progress = False
        log_level = "DEBUG"

    setup_logging(level=log_level, log_file=log_file)
    log = get_logger()
    load_env()

    # load inventory and select targets
    devs = load_inventory_csv(inventory_path)
    role_list = [r.strip().lower() for r in roles.split(",") if r.strip()]
    targets = select(devs, roles_any=role_list, enabled_only=True)

    # optional interactive filter by Site
    if single:
        sites = sorted({d.site for d in targets}, key=str.lower)
        click.echo("\nSelect sites (comma and ranges allowed):\n")
        for i, n in enumerate(sites, 1):
            click.echo(f"  {i:2d}. {n}")
        sel = (click.prompt("\nEnter numbers (blank cancels)", default="", show_default=False) or "").strip()
        if not sel:
            click.echo("No selection; exiting.")
            return

        def expand(tok, n):
            tok = tok.strip()
            if "-" in tok:
                a, b = tok.split("-", 1)
                if a.isdigit() and b.isdigit():
                    a, b = sorted([int(a), int(b)])
                    return [i for i in range(max(1, a), min(n, b) + 1)]
                return []
            return [int(tok)] if tok.isdigit() and 1 <= int(tok) <= n else []

        idxs = sorted({i for t in sel.replace(",", " ").split() for i in expand(t, len(sites))})
        chosen = {sites[i - 1].lower() for i in idxs}
        targets = [d for d in targets if d.site.lower() in chosen]

    day = datetime.now().strftime("%Y-%m-%d")
    zip_name = f"{day}_Daily_Exports.zip"
    day_root = f"{day}_Daily_Exports"

    async def _run() -> List[Tuple[str, Dict[str, bytes], Dict[str, bytes], Dict[str, bytes], str, Optional[str]]]:
        sem = asyncio.Semaphore(concurrency)

        async def _run_one(dev: Device):
            async with sem:
                return await _collect_one(dev, delete_remote_logs=not keep_remote_logs)

        tasks = [asyncio.create_task(_run_one(d)) for d in targets]
        results: List[Tuple[str, Dict[str, bytes], Dict[str, bytes], Dict[str, bytes], str, Optional[str]]] = []

        if progress:
            with click.progressbar(length=len(tasks), label="Collecting", show_pos=True) as bar:
                for fut in asyncio.as_completed(tasks):
                    results.append(await fut)
                    bar.update(1)
        else:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)

        with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for prop, hash_logs, changelog_logs, hotspot, export_text, err in sorted(results, key=lambda x: x[0].lower()):
                root = f"{day_root}/{prop}"
                zf.writestr(f"{root}/export.rsc", export_text or "")
                for name, data in hash_logs.items():
                    zf.writestr(f"{root}/hash-log/{name}", data)
                for name, data in changelog_logs.items():
                    zf.writestr(f"{root}/change-log/{name}", data)
                for rel, data in hotspot.items():
                    zf.writestr(f"{root}/{rel}", data)

        return results

    results = asyncio.run(_run())
    log.info(f"created {zip_name}")

    # ----- summary -----
    ok = [r for r in results if not r[5]]
    bad = [r for r in results if r[5]]

    log.info("----- Daily Export Summary -----")
    
    for prop, hash_logs, changelog_logs, hotspot, export_text, err in sorted(results, key=lambda x: x[0].lower()):
        if err:
            log.error(f"{prop}: FAILED - {err}")
        else:
            log.info(
                f"{prop}: OK  "
                f"hash_logs={len(hash_logs)}  "
                f"changelog={len(changelog_logs)}  "
                f"hotspot_files={len(hotspot)}"
            )
    log.info(f"Sites total: {len(results)}  OK: {len(ok)}  Failed: {len(bad)}")
    
    # upload
    cfg = FileSvrCfg.from_env()
    daily_subdir = os.getenv("FILESERV_DAILY_EXPORTS_SUBDIR", "Daily_Exports_and_Hash_Logs")

    remote_path = upload_to_file_server(Path(zip_name), cfg, subdir=daily_subdir)
    log.info(f"uploaded to {remote_path}")

    # testing: remove remote zip immediately
    if testing:
        if remove_from_file_server(cfg, remote_path):
            log.info("testing: removed remote zip after upload")
        else:
            log.warning("testing: remote zip removal failed (left remote copy in place)")

    # Gmail-safe email
    DISALLOWED = {".exe", ".dll", ".js", ".cmd", ".bat", ".sh", ".reg", ".msi", ".vbs", ".jar", ".scr", ".ps1"}
    contains_disallowed = False
    try:
        with zipfile.ZipFile(zip_name, "r") as src:
            for zi in src.infolist():
                zpath = zi.filename.replace("\\", "/").lower()
                ext = Path(zpath).suffix.lower()
                if "/hotspot/" in zpath or ext in DISALLOWED:
                    contains_disallowed = True
                    break
    except Exception:
        contains_disallowed = True

    sanitized_zip: Path | None = None
    safe_name = Path(f"{Path(zip_name).stem}_SAFE.zip")

    if contains_disallowed:
        with zipfile.ZipFile(zip_name, "r") as src, zipfile.ZipFile(
            safe_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as dst:
            for zi in src.infolist():
                zpath = zi.filename.replace("\\", "/")
                zpath_l = zpath.lower()
                ext = Path(zpath_l).suffix
                if "/hotspot/" in zpath_l or ext in DISALLOWED:
                    continue
                dst.writestr(zpath, src.read(zi))
        sanitized_zip = safe_name

    sender = os.getenv("GMAIL_USER", "")
    app_pw = os.getenv("GMAIL_APP_PASSWORD", "")
    subj = f"{day} Daily Exports"

    body_lines = [f"Daily exports for {day}", f"Uploaded to: {remote_path}"]
    if testing:
        body_lines.append("TESTING MODE: remote zip was uploaded to test connectivity and then removed.")
    if contains_disallowed:
        body_lines.append("Note: Attachment omitted due to Gmail security policy (hotspot assets or disallowed file types).")
    body = "\n".join(body_lines)

    recipients = ["eshortt@telcomsys.net"] if no_email else [
        "eshortt@telcomsys.net",
        "jedwards@ripheat.com",
        "rkammerman@ripheat.com",
    ]

    try:
        if not contains_disallowed:
            send_email_with_attachment(
                sender, app_pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
                int(os.getenv("SMTP_PORT", "587")),
                recipients, subj, body, Path(zip_name)
            )
        else:
            if sanitized_zip and sanitized_zip.exists():
                send_email_with_attachment(
                    sender, app_pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
                    int(os.getenv("SMTP_PORT", "587")),
                    recipients, subj, body, sanitized_zip
                )
            else:
                link_note = Path("EXPORT_LINK.txt")
                link_note.write_text(body, encoding="utf-8")
                send_email_with_attachment(
                    sender, app_pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
                    int(os.getenv("SMTP_PORT", "587")),
                    recipients, subj, body, link_note
                )
                try:
                    link_note.unlink(missing_ok=True)
                except Exception:
                    pass
        log.info(f"email sent to {', '.join(recipients)}")
    except Exception as e:
        log.error(f"email failed: {e}")

    # local cleanup (delete BOTH normal + _SAFE zips unless --keep)
    if not keep:
        for p in (Path(zip_name), safe_name):
            try:
                p.unlink(missing_ok=True)
            except Exception as e:
                log.warning(f"failed to delete local file {p.name}: {e}")
        log.info("cleaned local zip(s)")
    else:
        log.info("kept local zip(s) per --keep")

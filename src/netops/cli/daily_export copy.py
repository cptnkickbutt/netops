# src/netops/cli/daily_export.py
from __future__ import annotations

import os, zipfile, tempfile
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

import click
import asyncio

from ..inventory import load_inventory_csv, select, Device
from ..config import load_env, resolve_env, FileSvrCfg
from ..logging import setup_logging, get_logger
from ..uploader import upload_to_file_server
from ..emailer import send_email_with_attachment
from ..transports.ssh import make_ssh_client, ssh_exec
from ..transports import sftp as sftp_utils


# --------- blocking helpers (run in threads) ---------

def _pull_export_via_ssh_blocking(ip: str, user: str, pw: str) -> str:
    ssh = make_ssh_client(ip, 22, user, pw)
    try:
        try: ssh_exec(ssh, "setline 0")
        except Exception: pass
        ssh_exec(ssh, r'/export file="__netops_export__"')
        sftp = ssh.open_sftp()
        try:
            with sftp.open("__netops_export__.rsc", "rb") as f:
                return f.read().decode("utf-8", "ignore")
        finally:
            try: sftp.remove("__netops_export__.rsc")
            except Exception: pass
            try: sftp.close()
            except Exception: pass
    finally:
        try: ssh.close()
        except Exception: pass

def _pull_hash_logs_blocking(ip: str, user: str, pw: str, *, delete_remote: bool) -> Dict[str, bytes]:
    logs: Dict[str, bytes] = {}
    changelog_logs: Dict[str, bytes] = {}
    ssh = make_ssh_client(ip, 22, user, pw)
    try:
        sftp = ssh.open_sftp()
        try:
            names = set(sftp_utils.sftp_listdir(sftp, "."))
        finally:
            try: sftp.close()
            except Exception: pass

        present: List[str] = []
        for i in range(0, 100):
            for prefix in ("log", "Hashlog"):
                fname = f"{prefix}.{i}.txt"
                if fname not in names:
                    continue
                with tempfile.TemporaryDirectory(prefix="netops_logs_") as tdir:
                    tmp = Path(tdir) / fname
                    sftp_utils.sftp_download_file(ip, 22, user, pw, fname, str(tmp))
                    logs[fname] = tmp.read_bytes()
                    present.append(fname)

        if delete_remote and present:
            for name in present:
                try: ssh_exec(ssh, f'/file remove [ find name="{name}" ]')
                except Exception: pass

        return logs
    finally:
        try: ssh.close()
        except Exception: pass


def _pull_changelog_logs_blocking(ip: str, user: str, pw: str, *, delete_remote: bool) -> Dict[str, bytes]:
    """Download Changelog.N.txt files (RouterOS disk log action using filename 'Changelog')."""
    logs: Dict[str, bytes] = {}
    ssh = make_ssh_client(ip, 22, user, pw)
    try:
        sftp = ssh.open_sftp()
        try:
            names = set(sftp_utils.sftp_listdir(sftp, "."))
        finally:
            try: sftp.close()
            except Exception: pass

        present: List[str] = []
        for i in range(0, 100):
            fname = f"Changelog.{i}.txt"
            if fname not in names:
                continue
            with tempfile.TemporaryDirectory(prefix="netops_changelog_") as tdir:
                tmp = Path(tdir) / fname
                sftp_utils.sftp_download_file(ip, 22, user, pw, fname, str(tmp))
                logs[fname] = tmp.read_bytes()
                present.append(fname)

        if delete_remote and present:
            for name in present:
                try:
                    ssh_exec(ssh, f'/file remove [ find name="{name}" ]')
                except Exception:
                    pass

        return logs
    finally:
        try: ssh.close()
        except Exception: pass


def _pull_hotspot_blocking(ip: str, user: str, pw: str) -> Dict[str, bytes]:
    """Never delete remote hotspot; only read into memory via a tempdir."""
    ssh = make_ssh_client(ip, 22, user, pw)
    try:
        sftp = ssh.open_sftp()
        base: Optional[str] = None
        try:
            try:
                sftp.listdir("hotspot"); base = "hotspot"
            except Exception:
                pass
            if base is None:
                try:
                    sftp.listdir("/flash/hotspot"); base = "/flash/hotspot"
                except Exception:
                    pass
        finally:
            try: sftp.close()
            except Exception: pass

        if base is None: return {}

        with tempfile.TemporaryDirectory(prefix="netops_hotspot_") as tdir:
            count = sftp_utils.sftp_download_dir(ip, 22, user, pw, base, tdir)
            if count == 0: return {}
            collected: Dict[str, bytes] = {}
            root = Path(tdir)
            for p in root.rglob("*"):
                if p.is_file():
                    rel = p.relative_to(root).as_posix()
                    collected[f"hotspot/{rel}"] = p.read_bytes()
            return collected
    finally:
        try: ssh.close()
        except Exception: pass


# --------- worker (async wrapper; uses threads for paramiko) ---------

async def _collect_one(dev: Device, *, delete_remote_logs: bool, progress=None):
    """
    Returns (site_name, logs_dict, hotspot_dict, export_text, error_str|None)
    - always export
    - logs+hotspot only if 'backup' role present
    """
    log = get_logger()
    prop, ip = dev.site, dev.mgmt_ip

    # creds
    try:
        user, pw = resolve_env(dev.user_env, dev.pw_env)
    except Exception as e:
        msg = f"Env resolve failed: {e}"
        log.error(f"{prop}: {msg}")
        if progress: progress.done("Error")
        return prop, {}, {}, {}, msg, msg

    # export (always)
    try:
        export_text = await asyncio.to_thread(_pull_export_via_ssh_blocking, ip, user, pw)
    except Exception as e:
        msg = f"Export failed: {e}"
        log.error(f"{prop}: {msg}")
        if progress: progress.done("Error")
        return prop, {}, {}, {}, msg, msg

    logs: Dict[str, bytes] = {}
    hotspot: Dict[str, bytes] = {}

    # only if backup role
    if dev.has_role("backup"):
        try:
            logs = await asyncio.to_thread(_pull_hash_logs_blocking, ip, user, pw, delete_remote=delete_remote_logs)
            try:
                changelog_logs = await asyncio.to_thread(_pull_changelog_logs_blocking, ip, user, pw, delete_remote=delete_remote_logs)
            except Exception as e:
                log.warning(f"{prop}: changelog pull failed: {e}")

        except Exception as e:
            log.warning(f"{prop}: hash log pull failed: {e}")
        try:
            hotspot = await asyncio.to_thread(_pull_hotspot_blocking, ip, user, pw)
        except Exception as e:
            log.warning(f"{prop}: hotspot pull failed: {e}")

    if progress: progress.done("Done")
    log.debug(f"{prop}: export + {len(logs)} hash log(s) + {len(changelog_logs)} changelog file(s) + {len(hotspot)} hotspot file(s)")
    return prop, logs, changelog_logs, hotspot, export_text, None


# --------- CLI ---------

@click.command("daily-export")
@click.option("-I", "--inventory", "inventory_path", default="inventory.csv",
              help="Unified inventory CSV (Site,Device,MgmtIP,System,Roles,Access,Port,UserEnv,PwEnv,Enabled,Notes).")
@click.option("-s", "--single", is_flag=True, help="Interactively select properties.")
@click.option("--roles", default="firewall,export,backup",
              help="Comma list of roles to include (default: firewall,export,backup).")
@click.option("--no-email", is_flag=True, help="Email only Eric instead of full distro.")
@click.option("--keep", is_flag=True, help="Keep local zip (skip cleanup).")
@click.option("--keep-remote-logs", is_flag=True,
              help="Do NOT delete remote log.N.txt / Changelog.N.txt after download (debug/test).")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Show overall progress bar (disables per-device bars).")
@click.option("--concurrency", default=6, show_default=True, type=click.IntRange(1, 64),
              help="Number of properties to collect in parallel.")
@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG","INFO","WARNING","ERROR","CRITICAL"]))
def daily_export_cli(inventory_path, single, roles, no_email, keep, keep_remote_logs, progress, concurrency, log_file, log_level):
    """
    Daily exports with unified inventory + tags:
      - Exports run for devices matching --roles (default: firewall,export,backup)
      - Logs + hotspot only when device has the 'backup' role
    """
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
                    return [i for i in range(max(1,a), min(n,b)+1)]
                return []
            return [int(tok)] if tok.isdigit() and 1 <= int(tok) <= n else []
        idxs = sorted({i for t in sel.replace(",", " ").split() for i in expand(t, len(sites))})
        chosen = {sites[i-1].lower() for i in idxs}
        targets = [d for d in targets if d.site.lower() in chosen]

    day = datetime.now().strftime("%Y-%m-%d")
    zip_name = f"{day}_Daily_Exports.zip"
    day_root = f"{day}_Daily_Exports"

    async def _run():
        sem = asyncio.Semaphore(concurrency)

        async def _run_one(dev: Device):
            async with sem:
                try:
                    return await _collect_one(dev, delete_remote_logs=not keep_remote_logs, progress=None)
                except Exception as e:
                    msg = f"Unhandled error: {e}"
                    log.error(f"{dev.site}: {msg}")
                    return dev.site, {}, {}, {}, "", msg

        tasks = [asyncio.create_task(_run_one(d)) for d in targets]
        results = []
        if progress:
            with click.progressbar(length=len(tasks), label="Collecting", show_pos=True) as bar:
                for fut in asyncio.as_completed(tasks):
                    results.append(await fut)
                    bar.update(1)
        else:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
        with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for prop, logs, changelog_logs, hotspot, export_text, err in sorted(results, key=lambda x: x[0].lower()):
                root = f"{day_root}/{prop}"
                zf.writestr(f"{root}/export.rsc", export_text or "")
                for name, data in logs.items():
                    zf.writestr(f"{root}/hash-log/{name}", data)
                for name, data in changelog_logs.items():
                    zf.writestr(f"{root}/change-log/{name}", data)
                for rel, data in hotspot.items():
                    zf.writestr(f"{root}/{rel}", data)

    asyncio.run(_run())
    log.info(f"created {zip_name}")

    # upload
   # after creating zip_name
    cfg = FileSvrCfg.from_env()

    # this CLI’s subdir (overrideable via env if you want)
    daily_subdir = os.getenv("FILESERV_DAILY_EXPORTS_SUBDIR", "Daily_Exports_and_Hash_Logs")

    remote_path = upload_to_file_server(Path(zip_name), cfg, subdir=daily_subdir)
    log.info(f"uploaded to {remote_path}")


    # Gmail-safe email (unchanged)
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
    if contains_disallowed:
        safe_name = Path(f"{Path(zip_name).stem}_SAFE.zip")
        with zipfile.ZipFile(zip_name, "r") as src, zipfile.ZipFile(safe_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as dst:
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
                try: link_note.unlink(missing_ok=True)
                except Exception: pass
        log.info(f"email sent to {', '.join(recipients)}")
    except Exception as e:
        log.error(f"email failed: {e}")

    if not keep:
        try: Path(zip_name).unlink(missing_ok=True)
        except Exception: pass
        log.info("cleaned local zip; remote copy retained")
    else:
        log.info("kept local zip per --keep")

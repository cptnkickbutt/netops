# src/netops/cli/daily_export.py
from __future__ import annotations

import os, io, zipfile
from pathlib import Path
from datetime import datetime

import click

from ..config import load_env, load_inventory, resolve_env, FileSvrCfg
from ..logging import setup_logging, get_logger
from ..uploader import upload_to_file_server
from ..emailer import send_email_with_attachment
from ..transports.ssh import make_ssh_client, ssh_exec
from ..transports import sftp as sftp_utils


# ---------- focused helpers (logic only) ----------

def _ssh(ip: str, user: str, pw: str):
    """Open SSH using our shared transport."""
    return make_ssh_client(ip, 22, user, pw)  # returns connected paramiko.SSHClient

def _pull_export_via_ssh(ssh) -> str:
    """MikroTik full export → string (same pattern as ETTP)."""
    ssh_exec(ssh, r'/export file="__netops_export__"')
    sftp = ssh.open_sftp()
    try:
        with sftp.open("__netops_export__.rsc", "rb") as f:
            return f.read().decode("utf-8", "ignore")
    finally:
        try:
            sftp.remove("__netops_export__.rsc")
        except Exception:
            pass
        try:
            sftp.close()
        except Exception:
            pass

def _pull_hash_logs_via_sftp(ssh, ip: str, user: str, pw: str) -> tuple[dict[str, bytes], list[str]]:
    """
    Fetch log.N.txt via SFTP helpers (using handle-based listing).
    Returns (logs_dict, present_names_list). Deletion is handled by caller.
    """
    out: dict[str, bytes] = {}

    sftp = ssh.open_sftp()
    try:
        names = set(sftp_utils.sftp_listdir(sftp, "."))
    finally:
        try:
            sftp.close()
        except Exception:
            pass

    present = []
    for i in range(0, 100):
        fname = f"log.{i}.txt"
        if fname not in names:
            continue
        tmp = Path(".netops_tmp") / fname
        sftp_utils.sftp_download_file(ip, 22, user, pw, fname, str(tmp))
        out[fname] = tmp.read_bytes()
        present.append(fname)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    return out, present

def _delete_hash_logs_via_ssh(ssh, present_names: list[str]) -> None:
    """Remove log files on the device after successful download."""
    for name in present_names:
        try:
            ssh_exec(ssh, f'/file remove [ find name="{name}" ]')
        except Exception:
            # non-fatal
            pass

def _pull_hotspot_via_sftp(ssh, ip: str, user: str, pw: str) -> dict[str, bytes]:
    """
    Use handle-based listing to detect hotspot folder, then
    download recursively via sftp_download_dir(...).
    NOTE: We NEVER delete the hotspot folder on the device.
    """
    base = None
    sftp = ssh.open_sftp()
    try:
        try:
            sftp.listdir("hotspot")
            base = "hotspot"
        except Exception:
            pass
        if base is None:
            try:
                sftp.listdir("/flash/hotspot")
                base = "/flash/hotspot"
            except Exception:
                pass
    finally:
        try:
            sftp.close()
        except Exception:
            pass

    if base is None:
        return {}

    # Download to temp dir using connection-based helper, then read into memory.
    temp_root = Path(".netops_tmp_hotspot")
    temp_root.mkdir(exist_ok=True, parents=True)
    try:
        copied = sftp_utils.sftp_download_dir(ip, 22, user, pw, base, str(temp_root))
        if copied == 0:
            return {}
        collected: dict[str, bytes] = {}
        for p in temp_root.rglob("*"):
            if p.is_file():
                rel = p.relative_to(temp_root).as_posix()
                collected[f"hotspot/{rel}"] = p.read_bytes()
        return collected
    finally:
        # cleanup ONLY local temp files/dirs
        for p in sorted(temp_root.rglob("*"), reverse=True):
            try:
                p.unlink()
            except IsADirectoryError:
                try: p.rmdir()
                except Exception: pass
            except Exception:
                pass
        try: temp_root.rmdir()
        except Exception:
            pass

def _zip_property(zf: zipfile.ZipFile, day_root: str, prop: str, export_text: str,
                  logs: dict[str, bytes], hotspot: dict[str, bytes]) -> None:
    root = f"{day_root}/{prop}"
    zf.writestr(f"{root}/export.rsc", export_text or "")
    if logs:
        for name, data in logs.items():
            zf.writestr(f"{root}/hash_logs/{name}", data)
    if hotspot:
        for rel, data in hotspot.items():
            zf.writestr(f"{root}/{rel}", data)


# ---------- CLI ----------

@click.command("daily-export")
@click.option("-i", "--inventory", default="propertyinformation.csv",
              help="CSV: Property,IP,UserEnv,PwEnv")
@click.option("-s", "--single", is_flag=True, help="Interactively select properties.")
@click.option("--no-email", is_flag=True, help="Email only Eric instead of full distro.")
@click.option("--keep", is_flag=True, help="Keep local zip (skip cleanup).")
@click.option("--keep-remote-logs", is_flag=True,
              help="Do NOT delete remote log.N.txt after download (debug/test).")
@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG","INFO","WARNING","ERROR","CRITICAL"]))
def daily_export_cli(inventory, single, no_email, keep, keep_remote_logs, log_file, log_level):
    """
    Collect per-property RouterOS artifacts (ETTP firewall sites):
      - full /export (export.rsc)
      - log.N.txt (removed after copy, unless --keep-remote-logs)
      - hotspot/ (if present, recursively; never deleted on device)

    ZIP layout:
      YYYY-MM-DD_Daily_Exports/<Property>/(export.rsc, hash_logs/, hotspot/)
    """
    setup_logging(level=log_level, log_file=log_file)
    log = get_logger()
    load_env()

    rows = load_inventory(inventory)
    props = [r[0] for r in rows]

    if single:
        props_sorted = sorted(props, key=str.lower)
        click.echo("\nSelect properties (comma and ranges allowed):\n")
        for i, n in enumerate(props_sorted, 1):
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

        idxs = sorted({i for t in sel.replace(",", " ").split() for i in expand(t, len(props_sorted))})
        chosen = {props_sorted[i-1].lower() for i in idxs}
        rows = [r for r in rows if r[0].lower() in chosen]

    day = datetime.now().strftime("%Y-%m-%d")
    zip_name = f"{day}_Daily_Exports.zip"
    day_root = f"{day}_Daily_Exports"

    with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for prop, ip, user_key, pw_key, *rest in rows:
            log.info(f"→ {prop} ({ip})")
            try:
                user, pw = resolve_env(user_key, pw_key)
            except Exception as e:
                log.error(f"{prop}: env resolve failed: {e}")
                _zip_property(zf, day_root, prop, f"Env resolve failed: {e}", {}, {})
                continue

            # SSH connect (for /export and optional deletions)
            try:
                ssh = _ssh(ip, user, pw)
            except Exception as e:
                log.error(f"{prop}: SSH connect failed: {e}")
                _zip_property(zf, day_root, prop, f"SSH failed: {e}", {}, {})
                continue

            try:
                # Best-effort wide output (some boxes)
                try:
                    ssh_exec(ssh, "setline 0")
                except Exception:
                    pass

                export_text = _pull_export_via_ssh(ssh)

                # logs via sftp helpers; optionally delete via SSH
                logs, present = _pull_hash_logs_via_sftp(ssh, ip, user, pw)
                if present and not keep_remote_logs:
                    _delete_hash_logs_via_ssh(ssh, present)

                # hotspot folder via sftp helpers (NEVER deleted remotely)
                hotspot = _pull_hotspot_via_sftp(ssh, ip, user, pw)

                _zip_property(zf, day_root, prop, export_text, logs, hotspot)
                log.debug(f"{prop}: export + {len(logs)} log(s) + {len(hotspot)} hotspot file(s)")

            except Exception as e:
                log.error(f"{prop}: collection failed: {e}")
                _zip_property(zf, day_root, prop, f"Collection failed: {e}", {}, {})
            finally:
                try:
                    ssh.close()
                except Exception:
                    pass

    log.info(f"created {zip_name}")

    # upload (per-CLI dest; falls back to FILESERV_PATH)
    cfg = FileSvrCfg.from_env()
    daily_dir = os.getenv("FILESERV_DAILY_EXPORTS_PATH") or os.getenv("FILESERV_PATH", "/mnt/TelcomFS/Daily_Exports")
    cfg.remote_dir = daily_dir
    remote_path = upload_to_file_server(Path(zip_name), cfg, remote_dir=daily_dir)
    log.info(f"uploaded to {remote_path}")

    # Decide if attachment is safe (Gmail blocks .js/.exe/... even inside zips)
    DISALLOWED = {".exe", ".dll", ".js", ".cmd", ".bat", ".sh", ".reg", ".msi", ".vbs", ".jar", ".scr", ".ps1"}
    contains_disallowed = False

    try:
        with zipfile.ZipFile(zip_name, "r") as src:
            for zi in src.infolist():
                zpath = zi.filename.replace("\\", "/").lower()
                ext = Path(zpath).suffix.lower()
                # block if any disallowed extension OR any hotspot file
                if "/hotspot/" in zpath or ext in DISALLOWED:
                    contains_disallowed = True
                    break
    except Exception:
        # If we can't inspect, play it safe and send link-only.
        contains_disallowed = True

    # If unsafe, build a trimmed zip with only export + logs (no hotspot, no disallowed)
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

    # Email: --no-email still emails Eric only
    sender = os.getenv("GMAIL_USER", "")
    app_pw = os.getenv("GMAIL_APP_PASSWORD", "")
    subj = f"{day} Daily Exports"
    body_lines = [
        f"Daily exports for {day}",
        f"Uploaded to: {remote_path}",
    ]
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
            # full zip is safe to attach
            send_email_with_attachment(
                sender, app_pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
                int(os.getenv("SMTP_PORT", "587")),
                recipients, subj, body, Path(zip_name)
            )
        else:
            # attach the sanitized zip if we created one; otherwise fall back to link-only via tiny txt
            if sanitized_zip and sanitized_zip.exists():
                send_email_with_attachment(
                    sender, app_pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
                    int(os.getenv("SMTP_PORT", "587")),
                    recipients, subj, body, sanitized_zip
                )
            else:
                # Fallback: send a tiny .txt attachment with the link
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

    if not keep:
        try:
            Path(zip_name).unlink(missing_ok=True)
        except Exception:
            pass
        log.info("cleaned local zip; remote copy retained")
    else:
        log.info("kept local zip per --keep")

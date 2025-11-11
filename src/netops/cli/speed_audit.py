# src/netops/cli/speed_audit.py
from __future__ import annotations

import os
import asyncio
import time
from datetime import date
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import click

from ..logging import setup_logging, get_logger
from ..config import load_env, resolve_env, FileSvrCfg
from ..excel import write_workbook
from ..orchestrator import run_many
from ..uploader import upload_to_file_server
from ..emailer import send_email_with_attachment

# unified inventory
from ..inventory import load_inventory_csv, select, Device

# systems (adjust imports if your paths differ)
from ..systems.ettp import ETTPSystem
from ..systems.cmts import CMTSystem
from ..systems.gpon import GPONSystem
from ..systems.dsl import DSLSystem

Transport = Literal["ssh", "telnet"]


@dataclass
class SiteRow:
    property: str
    ip: str
    user_env: str
    pw_env: str
    system: str
    access: Transport


def _build_sites_from_inventory(inventory_path: str, roles: list[str]) -> list[SiteRow]:
    devs = load_inventory_csv(inventory_path)
    selected = select(devs, roles_any=roles, enabled_only=True)
    sites: list[SiteRow] = []
    for d in selected:
        sites.append(
            SiteRow(
                property=d.site,
                ip=d.mgmt_ip,
                user_env=d.user_env,
                pw_env=d.pw_env,
                system=d.system.upper(),
                access=d.access.lower(),  # currently unused, but kept for parity
            )
        )
    return sites


def _system_factory(site: SiteRow):
    mapping = {
        "ETTP": ETTPSystem,
        "CMTS": CMTSystem,
        "GPON": GPONSystem,
        "DSL":  DSLSystem,
    }
    cls = mapping.get(site.system)
    if not cls:
        raise RuntimeError(f"Unsupported system {site.system}")
    # NOTE: your System constructors previously took (site, ip, user, pw) or similar via SiteRow
    # If they expect the dataclass, pass 'site' directly (as earlier versions did).
    return cls(site, None)


def _pick_properties_interactive(props: list[str]) -> list[str] | None:
    props_sorted = sorted(props, key=str.lower)
    if not props_sorted:
        click.echo("No properties found in inventory.")
        return None

    click.echo("\nSelect one or more properties to run (comma and ranges allowed):\n")
    for i, name in enumerate(props_sorted, start=1):
        click.echo(f"  {i:2d}. {name}")
    choice = (click.prompt("\nEnter numbers (e.g., 1,4,7-9)", default="", show_default=False) or "").strip()
    if not choice:
        click.echo("No selection made. Exiting.")
        return None

    def expand(expr: str, n: int):
        expr = expr.strip()
        if "-" in expr:
            a, b = expr.split("-", 1)
            if a.isdigit() and b.isdigit():
                a, b = int(a), int(b)
                if a > b:
                    a, b = b, a
                return [i for i in range(max(1, a), min(n, b) + 1)]
            return []
        return [int(expr)] if expr.isdigit() and 1 <= int(expr) <= n else []

    idxs = sorted({i for tok in choice.replace(",", " ").split() for i in expand(tok, len(props_sorted))})
    if not idxs:
        click.echo("No valid selections parsed. Exiting.")
        return None
    return [props_sorted[i - 1] for i in idxs]


@click.command("speed-audit")
@click.option("-I", "--inventory", "inventory_path", default="inventory.csv",
              help="Unified inventory CSV (with Roles).")
@click.option("--roles", default="web-system",
              help="Comma list of roles to include (default: web-system).")
@click.option("-s", "--single", is_flag=True, help="Interactively select sites before running.")
@click.option("-c", "--concurrency", type=int, default=6, show_default=True,
              help="Max concurrent sites.")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Show progress bars.")
@click.option("--quiet", is_flag=True, help="Silence console logs (progress only).")
@click.option("--no-email", is_flag=True, help="Email only Eric (disable full distro).")
@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]))
def speed_audit_cli(inventory_path, roles, single, concurrency, progress, quiet, no_email, log_file, log_level):
    """
    Speed Audit using unified inventory + role filtering (default role: web-system).
    """
    setup_logging(
        level=log_level,
        quiet=quiet,
        log_file=log_file,
        use_tqdm_handler=bool(progress) and not quiet,
    )
    log = get_logger()
    load_env()

    role_list = [r.strip().lower() for r in roles.split(",") if r.strip()]
    sites = _build_sites_from_inventory(inventory_path, role_list)
    if not sites:
        log.warning(f"No devices found with roles {role_list}")
        return

    if single:
        chosen = _pick_properties_interactive([s.property for s in sites])
        if not chosen:
            return
        wanted = {c.lower() for c in chosen}
        sites = [s for s in sites if s.property.lower() in wanted]

    start = time.time()

    # Orchestrate
    results = asyncio.run(run_many(
        sites,
        _system_factory,
        concurrency=concurrency,
        show_progress=bool(progress),
    ))

    # Write workbook
    fname = f"{date.today():%Y_%m_%d}_Speed_Audit.xlsx"
    write_workbook(fname, results)
    log.info(f"Wrote {fname}")

    # Upload to file server subdir
    cfg = FileSvrCfg.from_env()
    audit_subdir = os.getenv("FILESERV_SPEED_AUDIT_SUBDIR", "Speed_Audit")
    remote_path = upload_to_file_server(Path(fname), cfg, subdir=audit_subdir)
    log.info(f"Uploaded to {remote_path}")

    # Email
    sender = os.getenv("GMAIL_USER", "")
    pw = os.getenv("GMAIL_APP_PASSWORD", "")
    subj = f"{date.today():%Y-%m-%d} Speed Audit"
    body = f"Speed Audit report for {date.today():%Y-%m-%d}\nUploaded to: {remote_path}"

    recipients = (
        ["eshortt@telcomsys.net"] if no_email else
        ["eshortt@telcomsys.net", "jedwards@ripheat.com", "rkammerman@ripheat.com"]
    )

    try:
        # run in thread to keep event loop simple
        asyncio.run(asyncio.to_thread(
            send_email_with_attachment,
            sender, pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
            int(os.getenv("SMTP_PORT", "587")),
            recipients, subj, body, Path(fname)
        ))
        log.info(f"Email sent to {', '.join(recipients)}")
    except Exception as e:
        log.error(f"Email failed: {e}")

    m, s = divmod(time.time() - start, 60)
    log.info(f"Done in {int(m)}:{s:.2f} min — wrote {fname}")

# src/netops/cli/speed_audit.py

import sys
import os
import argparse
import asyncio
import time
import logging
from datetime import date
from dataclasses import dataclass
from typing import Optional, Literal
from pathlib import Path

from ..logging import setup_logging, get_logger
from ..config import load_env, load_inventory, resolve_env, FileSvrCfg
from ..excel import write_workbook
from ..orchestrator import run_many
from ..uploader import upload_to_file_server
from ..emailer import send_email_with_attachment

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


def parse_args():
    p = argparse.ArgumentParser(description="Speed Audit (netops)")
    p.add_argument("-i", "--inventory", default="web_systems.csv",
                   help="CSV file with columns: Property,IP,UserEnv,PwEnv,System,Access")
    p.add_argument("-s", "--single", action="store_true",
                   help="Interactively pick one or more properties (otherwise runs ALL).")
    p.add_argument("--no-email", action="store_true",
                   help="Disable emailing of the report (email is ON by default).")
    p.add_argument("-c", "--concurrency", type=int, default=6,
                   help="Max concurrent sites (default: 6).")
    p.add_argument("--progress", action="store_true",
                   help="Force-enable the progress bar (even if not a TTY).")
    p.add_argument("--no-progress", action="store_true",
                   help="Disable the progress bar.")
    p.add_argument("--quiet", action="store_true",
                   help="Silence console logs (progress bar only).")
    p.add_argument("--log-file", default=None)
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return p.parse_args()


def pick_properties_interactive(props: list[str]) -> Optional[list[str]]:
    props_sorted = sorted(props, key=str.lower)
    if not props_sorted:
        print("No properties found in CSV.")
        return None

    print("\nSelect one or more properties to run (comma and ranges allowed):\n")
    for i, name in enumerate(props_sorted, start=1):
        print(f"  {i:2d}. {name}")
    choice = input("\nEnter numbers (e.g., 1,4,7-9), or press Enter to cancel: ").strip()
    if not choice:
        print("No selection made. Exiting.")
        return None

    def expand(expr, n):
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
        print("No valid selections parsed. Exiting.")
        return None
    return [props_sorted[i - 1] for i in idxs]


def build_sites(rows: list[list[str]]) -> list[SiteRow]:
    sites = []
    for r in rows:
        sites.append(SiteRow(
            property=str(r[0]),
            ip=str(r[1]),
            user_env=str(r[2]),
            pw_env=str(r[3]),
            system=str(r[4]).upper() if len(r) > 4 else "ETTP",
            access=str(r[5]).lower() if len(r) > 5 else "ssh",
        ))
    return sites


def system_factory(site: SiteRow):
    """
    Create and return the System object.
    NOTE: Transports are now opened/closed **inside** each System:
      - ETTP uses Paramiko SSH on demand (sync).
      - DSL/CMTS/GPON use AsyncTelnetClient on demand (async).
    We pass None for runner to keep the constructor signature stable.
    """
    mapping = {
        "ETTP": ETTPSystem,
        "CMTS": CMTSystem,
        "GPON": GPONSystem,
        "DSL":  DSLSystem,
    }
    cls = mapping.get(site.system)
    if not cls:
        raise RuntimeError(f"Unsupported system {site.system}")
    return cls(site, None)


def main_entry():
    asyncio.run(main())


async def main():
    args = parse_args()

    is_tty = sys.stderr.isatty() or sys.stdout.isatty()
    show_progress = args.progress or (is_tty and not args.no_progress)

    setup_logging(
        level=args.log_level,
        quiet=args.quiet,
        log_file=args.log_file,
        use_tqdm_handler=show_progress and not args.quiet,
    )

    # ---------- Fallback handler if setup_logging added none ----------
    log = get_logger()
    if not log.handlers:
        # Console
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, args.log_level))
        ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        log.addHandler(ch)
        # File (if requested)
        if args.log_file:
            fh = logging.FileHandler(args.log_file, encoding="utf-8")
            fh.setLevel(getattr(logging, args.log_level))
            fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            log.addHandler(fh)
        log.setLevel(getattr(logging, args.log_level))

    log.debug("Logger initialized (post-setup fallback check).")
    
    # Load env and inventory
    load_env()
    rows = load_inventory(args.inventory)
    sites = build_sites(rows)

    # Interactive subset
    if args.single:
        chosen = pick_properties_interactive([s.property for s in sites])
        if not chosen:
            return
        wanted = {c.lower() for c in chosen}
        sites = [s for s in sites if s.property.lower() in wanted]

    # Run
    start = time.time()
    results = await run_many(
        sites,
        system_factory,
        concurrency=args.concurrency,
        show_progress=show_progress,
    )

    # Write workbook
    fname = f"{date.today():%Y_%m_%d}_Speed_Audit.xlsx"
    write_workbook(fname, results)

    log = get_logger()

    # --------- Upload destination selection (per CLI) ---------
    # Prefer a tool-specific env var, fallback to FILESERV_PATH
    audit_dir = os.getenv("FILESERV_SPEED_AUDITS_PATH") or os.getenv("FILESERV_PATH", "/mnt/TelcomFS/Monthly_Speed_Audit")

    
    # Upload to file server via SFTP using FileSvrCfg.from_env()
    log.info(f"Uploading {fname} to file server...")
    cfg = FileSvrCfg.from_env()
    cfg.remote_dir = audit_dir  # override the default for this CLI
    await asyncio.to_thread(upload_to_file_server, Path(fname), cfg)
    remote_path = await asyncio.to_thread(upload_to_file_server, Path(fname), cfg, remote_dir=audit_dir)
    log.debug(f"Uploaded to {remote_path}")
    
    # ---------- Email handling ----------
    sender = os.getenv("GMAIL_USER", "")
    pw = os.getenv("GMAIL_APP_PASSWORD", "")
    subj = f"{date.today():%Y-%m-%d} Speed Audit"
    body = f"Attached Speed Audit report for {date.today():%Y-%m-%d}"

    # If --no-email, still email *only* Eric
    if args.no_email:
        recipients = ["eshortt@telcomsys.net"]
    else:
        # full distribution
        recipients = [
            "eshortt@telcomsys.net",
            "jedwards@ripheat.com",
            "rkammerman@ripheat.com",
        ]

    try:
        await asyncio.to_thread(
            send_email_with_attachment,
            sender, pw, os.getenv("SMTP_HOST", "smtp.gmail.com"),
            int(os.getenv("SMTP_PORT", "587")),
            recipients, subj, body, Path(fname)
        )
        log.info(f"✓ Email sent to {', '.join(recipients)}")
    except Exception as e:
        log.error(f"Email failed: {e}")


    m, s = divmod(time.time() - start, 60)
    log.info(f"Done in {int(m)}:{s:.2f} min — wrote {fname}")

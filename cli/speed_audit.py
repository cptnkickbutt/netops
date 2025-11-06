
import sys, argparse, asyncio, time
from datetime import date
from dataclasses import dataclass
from typing import Optional, Literal

from netops.logging import setup_logging
from netops.config import load_env, load_inventory, resolve_env
from netops.excel import write_workbook
from netops.orchestrator import run_many
from netops.uploader import store_on_server
from netops.emailer import send_email
from netops.transports.ssh import SSHRunner
from netops.transports.telnet import TelnetRunner
from netops.systems.ettp import ETTPSystem
from netops.systems.cmts import CMTSystem
from netops.systems.gpon import GPONSystem
from netops.systems.dsl import DSLSystem

Transport = Literal["ssh","telnet"]

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
    p.add_argument("-i","--inventory", default="web_systems.csv")
    p.add_argument("-s","--single", action="store_true",
                   help="Interactively pick one or more properties (otherwise runs ALL).")
    p.add_argument("--no-email", action="store_true", help="Disable emailing of the report (email is ON by default).")
    p.add_argument("-c","--concurrency", type=int, default=6, help="Max concurrent sites (default: 6).")
    p.add_argument("--progress", action="store_true", help="Force-enable the progress bar (even if not a TTY).")
    p.add_argument("--no-progress", action="store_true", help="Disable the progress bar.")
    p.add_argument("--quiet", action="store_true", help="Silence console logs (progress bar only).")
    p.add_argument("--log-file", default=None)
    p.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"])
    return p.parse_args()

def pick_properties_interactive(props: list[str]) -> Optional[list[str]]:
    props_sorted = sorted(props, key=str.lower)
    if not props_sorted:
        print("No properties found in CSV."); return None
    print("\nSelect one or more properties to run (comma and ranges allowed):\n")
    for i, name in enumerate(props_sorted, start=1):
        print(f"  {i:2d}. {name}")
    choice = input("\nEnter numbers (e.g., 1,4,7-9), or press Enter to cancel: ").strip()
    if not choice: print("No selection made. Exiting."); return None
    def expand(expr, n):
        expr=expr.strip()
        if "-" in expr:
            a,b = expr.split("-",1)
            if a.isdigit() and b.isdigit():
                a,b=int(a),int(b)
                if a>b: a,b=b,a
                return [i for i in range(max(1,a), min(n,b)+1)]
            return []
        return [int(expr)] if expr.isdigit() and 1<=int(expr)<=n else []
    idxs = sorted({i for tok in choice.replace(","," ").split() for i in expand(tok, len(props_sorted))})
    if not idxs: print("No valid selections parsed. Exiting."); return None
    return [props_sorted[i-1] for i in idxs]

def build_sites(df) -> list[SiteRow]:
    sites = []
    for _, r in df.iterrows():
        sites.append(SiteRow(
            property=str(r["Property"]),
            ip=str(r["IP"]),
            user_env=str(r["User"]),
            pw_env=str(r["PW"]),
            system=str(r["System"]).upper(),
            access=str(r["Access"]).lower() if r.get("Access") else "ssh",
        ))
    return sites

def system_factory(site: SiteRow):
    # Bind runner per site and instantiate the proper system
    if site.access == "ssh":
        runner = SSHRunner(site.ip, resolve_env(site.user_env), resolve_env(site.pw_env))
    else:
        runner = TelnetRunner(site.ip, resolve_env(site.user_env), resolve_env(site.pw_env))
    mapping = {
        "ETTP": ETTPSystem,
        "CMTS": CMTSystem,
        "GPON": GPONSystem,
        "DSL":  DSLSystem,
    }
    cls = mapping.get(site.system)
    if not cls:
        raise RuntimeError(f"Unsupported system {site.system}")
    sys = cls(site, runner)
    return sys

def main_entry():
    asyncio.run(main())

async def main():
    args = parse_args()
    is_tty = sys.stderr.isatty() or sys.stdout.isatty()
    show_progress = args.progress or (is_tty and not args.no_progress)
    setup_logging(level=args.log_level, quiet=args.quiet, log_file=args.log_file, use_tqdm_handler=show_progress and not args.quiet)

    load_env()
    df = load_inventory(args.inventory)
    sites = build_sites(df)

    # interactive filter
    if args.single:
        chosen = pick_properties_interactive([s.property for s in sites])
        if not chosen: return
        wanted = {c.lower() for c in chosen}
        sites = [s for s in sites if s.property.lower() in wanted]

    start = time.time()
    results = await run_many(sites, system_factory, concurrency=args.concurrency, show_progress=show_progress)
    fname = f"{date.today().strftime('%Y_%m_%d')}_Speed_Audit.xlsx"
    write_workbook(fname, results)

    # upload + optional email
    from netops.logging import get_logger
    log = get_logger()
    log.info(f"Saving {fname} to server")
    await asyncio.to_thread(store_on_server, fname)
    if not args.no_email:
        log.info(f"{fname} uploaded; emailing...")
        await asyncio.to_thread(send_email, date.today().strftime('%Y_%m_%d'), fname)
    else:
        log.info(f"{fname} uploaded (email disabled by argument)." )

    m,s = divmod(time.time()-start, 60)
    log.info(f"Done in {int(m)}:{s:.2f} min — wrote {fname}")

"""
mass_config.py (rebuilt)

This file replaces a corrupted version where key async mode functions were truncated
into "..." placeholders, causing collect/run to no-op and finish instantly.

Supports:
- inventory.csv (Site,Device,MgmtIP,System,Roles,Access,Port,UserEnv,PwEnv,Enabled,Notes)
- role include/exclude filters
- modes: collect / build / run
- run-level: dry-run (connect only), upload (upload only), apply (upload + /import)
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import click
import pandas as pd

from netops.config import load_env, resolve_env
from netops.logging import setup_logging, get_logger
from netops.transports.ssh import make_ssh_client, ssh_exec


# ----------------------------
# Models / helpers
# ----------------------------

@dataclass(frozen=True)
class SiteRow:
    site: str
    ip: str
    system: str
    roles: str
    access: str
    port: int
    user_env: str
    pw_env: str
    enabled: str


def _truthy(s: str) -> bool:
    return str(s).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_csv_list(s: Optional[str]) -> list[str]:
    if not s:
        return []
    return [x.strip().lower() for x in s.split(",") if x.strip()]


def _row_has_any_role(cell: str, wanted: Sequence[str]) -> bool:
    have = {r.strip().lower() for r in str(cell).split(",") if r.strip()}
    return any(r in have for r in wanted)


def _decide_progress_flag(progress_flag: Optional[bool], quiet: bool) -> bool:
    if progress_flag is True:
        return True
    if progress_flag is False or quiet:
        return False
    return sys.stderr.isatty() or sys.stdout.isatty()


def _read_inventory(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.is_file():
        raise click.ClickException(f"Inventory CSV not found: {p}")
    df = pd.read_csv(p, dtype=str).fillna("")
    if df.empty:
        raise click.ClickException(f"Inventory loaded from {p} is empty.")
    return df


def _build_sites_from_inventory(
    inv_df: pd.DataFrame,
    wanted_systems: set[str],
    include_roles: list[str],
    exclude_roles: list[str],
) -> list[SiteRow]:
    cols = {str(c).strip().lower(): c for c in inv_df.columns}
    required = ["site", "mgmtip", "system", "roles", "access", "port", "userenv", "pwenv", "enabled"]
    missing = [r for r in required if r not in cols]
    if missing:
        raise click.ClickException(
            f"Inventory missing required columns: {missing}. Columns seen: {list(inv_df.columns)}"
        )

    df = inv_df.copy()
    # enabled filter
    df = df[df[cols["enabled"]].apply(_truthy)]
    # system filter (only if any systems were specified)
    if wanted_systems:
        df = df[df[cols["system"]].astype(str).str.upper().isin(wanted_systems)]

    # roles include/exclude
    if include_roles:
        df = df[df[cols["roles"]].apply(lambda x: _row_has_any_role(x, include_roles))]
    if exclude_roles:
        df = df[~df[cols["roles"]].apply(lambda x: _row_has_any_role(x, exclude_roles))]

    sites: list[SiteRow] = []
    for _, r in df.iterrows():
        sites.append(
            SiteRow(
                site=str(r[cols["site"]]).strip(),
                ip=str(r[cols["mgmtip"]]).strip(),
                system=str(r[cols["system"]]).strip(),
                roles=str(r[cols["roles"]]).strip(),
                access=str(r[cols["access"]]).strip() or "ssh",
                port=int(str(r[cols["port"]]).strip() or "22"),
                user_env=str(r[cols["userenv"]]).strip(),
                pw_env=str(r[cols["pwenv"]]).strip(),
                enabled=str(r[cols["enabled"]]).strip(),
            )
        )
    return sites


def _render_template(template_text: str, row: dict[str, Any]) -> str:
    """
    Supports both:
      - Python format: {name}
      - Jinja-ish: {{name}}
    Missing keys are left untouched.
    """
    out = template_text

    # Replace {{key}} tokens first
    for k, v in row.items():
        out = out.replace("{{" + str(k) + "}}", str(v))

    # Then try .format_map with a safe dict
    class SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    try:
        out = out.format_map(SafeDict({str(k): v for k, v in row.items()}))
    except Exception:
        # keep as-is if formatting fails
        pass
    return out


def _ssh_upload_and_maybe_import(
    ip: str,
    port: int,
    user: str,
    pw: str,
    *,
    remote_name: str,
    content: bytes,
    run_level: str,  # dry-run / upload / apply
    keep_remote_file: bool,
) -> tuple[bool, str]:
    """
    dry-run: connect + /system identity print, no upload
    upload : connect + upload only
    apply  : connect + upload + /import + (optional cleanup)
    """
    ssh = None
    try:
        ssh = make_ssh_client(ip, port, user, pw)
        # harmless connectivity check
        ssh_exec(ssh, "/system identity print")

        if run_level == "dry-run":
            return True, "connected"

        # upload content
        sftp = ssh.open_sftp()
        try:
            with sftp.file(remote_name, "wb") as f:
                f.write(content)
        finally:
            try:
                sftp.close()
            except Exception:
                pass

        if run_level == "upload":
            return True, "uploaded"

        # apply
        ssh_exec(ssh, f'/import file="{remote_name}"')

        if not keep_remote_file:
            try:
                ssh_exec(ssh, f'/file remove [ find name="{remote_name}" ]')
            except Exception:
                pass

        return True, "uploaded+imported"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
    finally:
        if ssh is not None:
            try:
                ssh.close()
            except Exception:
                pass


# ----------------------------
# Modes
# ----------------------------

async def _mode_collect(
    *,
    sites: list[SiteRow],
    output_csv: str,
) -> None:
    """
    For now, collect mode simply writes a devices CSV from inventory-filtered sites.
    (This matches your current use case: inventory is the source of truth.)
    """
    log = get_logger()
    out = Path(output_csv)
    rows = []
    for s in sites:
        rows.append(
            {
                "site": s.site,
                "ip": s.ip,
                "system": s.system,
                "access": s.access,
                "port": s.port,
                "user_env": s.user_env,
                "pw_env": s.pw_env,
                "roles": s.roles,
            }
        )
    df = pd.DataFrame(rows)
    out.write_text(df.to_csv(index=False), encoding="utf-8")
    log.info("Wrote %d device row(s) to %s", len(df), out)


def _mode_build(
    *,
    devices_csv: str,
    template_path: str,
    plan_dir: Optional[str],
    single_output: Optional[str],
) -> None:
    log = get_logger()
    csv_path = Path(devices_csv)
    tpl_path = Path(template_path)
    if not csv_path.is_file():
        raise click.ClickException(f"devices CSV not found: {csv_path}")
    if not tpl_path.is_file():
        raise click.ClickException(f"template file not found: {tpl_path}")

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    template_text = tpl_path.read_text(encoding="utf-8", errors="replace")

    if df.empty:
        log.warning("Devices CSV is empty; nothing to build.")
        return

    if single_output:
        out = Path(single_output)
        parts: list[str] = []
        for _, r in df.iterrows():
            parts.append(_render_template(template_text, r.to_dict()))
        out.write_text("\n".join(parts).strip() + "\n", encoding="utf-8")
        log.info("Built single output %s (%d row(s))", out, len(df))
        return

    if not plan_dir:
        raise click.ClickException("build mode requires --plan-dir or --single-output")

    out_dir = Path(plan_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Choose a filename column if present
    name_col = None
    for cand in ["site", "device", "identity", "name"]:
        if cand in {c.lower() for c in df.columns}:
            # map to actual
            for c in df.columns:
                if c.lower() == cand:
                    name_col = c
                    break
            break

    for _, r in df.iterrows():
        base = (str(r.get(name_col, "")).strip() if name_col else "") or "device"
        safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in base)
        out_path = out_dir / f"{safe}.rsc"
        out_path.write_text(_render_template(template_text, r.to_dict()), encoding="utf-8")
    log.info("Built %d file(s) in %s", len(df), out_dir)


async def _mode_run(
    *,
    sites: list[SiteRow],
    template_path: str,
    devices_csv: Optional[str],
    plan_dir: Optional[str],
    run_level: str,
    keep_remote_file: bool,
    concurrency: int,
    show_progress: bool,
) -> None:
    log = get_logger()

    # Determine what content to apply per site:
    # - If plan_dir: look for <site>.rsc (fallback to Device/Identity if present in devices_csv)
    # - Else: apply template_path content verbatim to every site
    tpl = Path(template_path)
    if not tpl.is_file():
        raise click.ClickException(f"template file not found: {tpl}")
    template_text = tpl.read_text(encoding="utf-8", errors="replace")

    # Optional per-site/per-device rendering from devices_csv (if provided)
    per_site_content: dict[str, bytes] = {}
    if devices_csv:
        df = pd.read_csv(Path(devices_csv), dtype=str).fillna("")
        if not df.empty:
            for _, r in df.iterrows():
                site = str(r.get("site", "") or r.get("Site", "")).strip()
                if not site:
                    continue
                per_site_content[site.lower()] = _render_template(template_text, r.to_dict()).encode("utf-8")

    sem = asyncio.Semaphore(concurrency)

    async def _run_one(s: SiteRow) -> tuple[str, bool, str]:
        async with sem:
            if s.access.lower() != "ssh":
                return s.site, False, f"skipped (access={s.access})"

            user, pw = resolve_env(s.user_env, s.pw_env)
            content = per_site_content.get(s.site.lower(), template_text.encode("utf-8"))
            try:
                ok, msg = await asyncio.to_thread(
                    _ssh_upload_and_maybe_import,
                    s.ip,
                    s.port,
                    user,
                    pw,
                    remote_name="__netops_mass_config.rsc",
                    content=content,
                    run_level=run_level,
                    keep_remote_file=keep_remote_file,
                )
            except Exception as e:
                ok, msg = False, f"{type(e).__name__}: {e}"
            if not ok:
                log.error("%s (%s) failed: %s", s.site, s.system, msg)
            else:
                log.debug("%s (%s) ok: %s", s.site, s.system, msg)
            return s.site, ok, msg

    tasks = [asyncio.create_task(_run_one(s)) for s in sites]
    results: list[tuple[str, bool, str]] = []
    if show_progress:
        with click.progressbar(length=len(tasks), label="Applying", show_pos=True) as bar:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)
                bar.update(1)
    else:
        for fut in asyncio.as_completed(tasks):
            results.append(await fut)

    ok = sum(1 for _, success, _ in results if success)
    bad = len(results) - ok
    log.info("Run complete: %d ok, %d failed/skipped", ok, bad)
    if bad:
        raise SystemExit(2)


# ----------------------------
# CLI
# ----------------------------
import sys  # placed here to keep helpers above clean


@click.command()
@click.option(
    "--mode",
    type=click.Choice(["collect", "build", "run"], case_sensitive=False),
    required=True,
    help="Overall mode: collect, build, or run.",
)
@click.option(
    "-i",
    "--inventory",
    default="inventory.csv",
    show_default=True,
    help="Unified inventory CSV: Site,Device,MgmtIP,System,Roles,Access,Port,UserEnv,PwEnv,Enabled,Notes",
)
@click.option(
    "--dotenv",
    type=click.Path(dir_okay=False, exists=True, readable=True),
    default=None,
    help="Optional explicit .env path.",
)
@click.option(
    "-s",
    "--single",
    is_flag=True,
    help="Interactively choose one or more sites instead of running all.",
)
@click.option(
    "--systems",
    multiple=True,
    default=None,
    show_default=False,
    help="Which systems to include (repeatable). If omitted, include all systems.",
)
@click.option(
    "--roles",
    default=None,
    help="Comma-separated inventory role(s) to include (requires Roles column).",
)
@click.option(
    "--role",
    "role_list",
    multiple=True,
    help="Inventory role to include (repeatable).",
)
@click.option(
    "--exclude-roles",
    default=None,
    help="Comma-separated inventory role(s) to exclude.",
)
@click.option(
    "--exclude-role",
    "exclude_role_list",
    multiple=True,
    help="Inventory role to exclude (repeatable).",
)
@click.option(
    "--devices-csv",
    type=click.Path(dir_okay=False, readable=True),
    default=None,
    help="Devices CSV (input for build/run; optional for collect).",
)
@click.option(
    "--output-csv",
    type=click.Path(dir_okay=False),
    default="mass_config_devices.csv",
    show_default=True,
    help="Where to write devices CSV in collect mode.",
)
@click.option(
    "--template",
    "template_path",
    type=click.Path(dir_okay=False, readable=True),
    required=False,
    help="RouterOS .rsc template file (used in build/run).",
)
@click.option(
    "--plan-dir",
    type=click.Path(file_okay=False),
    default=None,
    help="Directory for rendered .rsc files in build mode or pre-built plans in run mode.",
)
@click.option(
    "--single-output",
    type=click.Path(dir_okay=False),
    default=None,
    help="In build mode: write a single merged .rsc file instead of many files.",
)
@click.option(
    "--run-level",
    type=click.Choice(["dry-run", "upload", "apply"], case_sensitive=False),
    default="dry-run",
    show_default=True,
    help="dry-run=connectivity check only; upload=upload only; apply=upload + import",
)
@click.option(
    "--keep-remote-file",
    is_flag=True,
    help="Keep uploaded .rsc on router after upload/import (upload/apply only).",
)
@click.option(
    "-c",
    "--concurrency",
    type=int,
    default=6,
    show_default=True,
    help="Max concurrent sites.",
)
@click.option(
    "--progress/--no-progress",
    "progress_flag",
    default=None,
    help="Force enable/disable progress bar; default is auto (TTY only).",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Reduce console output.",
)
@click.option("--log-file", default=None, help="Optional path to a log file.")
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    show_default=True,
    help="Log verbosity.",
)
def mass_config_cli(
    mode: str,
    inventory: str,
    dotenv: Optional[str],
    single: bool,
    systems: tuple[str, ...],
    roles: Optional[str],
    role_list: tuple[str, ...],
    exclude_roles: Optional[str],
    exclude_role_list: tuple[str, ...],
    devices_csv: Optional[str],
    output_csv: str,
    template_path: Optional[str],
    plan_dir: Optional[str],
    single_output: Optional[str],
    run_level: str,
    keep_remote_file: bool,
    concurrency: int,
    progress_flag: Optional[bool],
    quiet: bool,
    log_file: Optional[str],
    log_level: str,
) -> None:
    show_progress = _decide_progress_flag(progress_flag, quiet)

    if dotenv:
        os.environ["NETOPS_DOTENV"] = str(dotenv)

    setup_logging(
        level=log_level.upper(),
        quiet=quiet,
        log_file=log_file,
        use_tqdm_handler=show_progress and not quiet,
    )
    log = get_logger()
    load_env()

    inv_df = _read_inventory(inventory)
    wanted_systems = {s.strip().upper() for s in systems if s and s.strip()} if systems else set()  # empty=set => include all

    include_roles: list[str] = []
    include_roles.extend(_parse_csv_list(roles))
    include_roles.extend([r.strip().lower() for r in role_list if r and r.strip()])

    exclude: list[str] = []
    exclude.extend(_parse_csv_list(exclude_roles))
    exclude.extend([r.strip().lower() for r in exclude_role_list if r and r.strip()])

    sites = _build_sites_from_inventory(inv_df, wanted_systems, include_roles, exclude)

    if not sites:
        raise click.ClickException("No sites matched the requested filters.")

    # Interactive selection (simple)
    if single:
        names = [s.site for s in sites]
        chosen = click.prompt(
            f"Enter site name (substring match). Options: {', '.join(names[:10])}...",
            default="",
            show_default=False,
        ).strip().lower()
        if chosen:
            sites = [s for s in sites if chosen in s.site.lower()]
        if not sites:
            raise click.ClickException("No sites left after selection.")

    mode_l = mode.lower()
    start = time.time()

    if mode_l == "collect":
        asyncio.run(_mode_collect(sites=sites, output_csv=output_csv))
    elif mode_l == "build":
        if not devices_csv:
            raise click.ClickException("build mode requires --devices-csv")
        if not template_path:
            raise click.ClickException("build mode requires --template")
        _mode_build(devices_csv=devices_csv, template_path=template_path, plan_dir=plan_dir, single_output=single_output)
    elif mode_l == "run":
        if not template_path:
            raise click.ClickException("run mode requires --template")
        asyncio.run(
            _mode_run(
                sites=sites,
                template_path=template_path,
                devices_csv=devices_csv,
                plan_dir=plan_dir,
                run_level=run_level.lower(),
                keep_remote_file=keep_remote_file,
                concurrency=concurrency,
                show_progress=show_progress,
            )
        )
    else:
        raise click.ClickException(f"Unsupported mode {mode!r}")

    m, s = divmod(time.time() - start, 60)
    log.info("mass-config %s completed in %d:%05.2f", mode_l, int(m), s)


def main():
    mass_config_cli(standalone_mode=False)


def main_entry():
    mass_config_cli()

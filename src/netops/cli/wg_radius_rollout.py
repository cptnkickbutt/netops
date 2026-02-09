# src/netops/cli/wg_radius_rollout.py
from __future__ import annotations

import asyncio
import csv
import ipaddress
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import click

from ..inventory import load_inventory_csv, select, Device
from ..config import load_env, resolve_env
from ..logging import setup_logging, get_logger
from ..transports.ssh import make_ssh_client, ssh_exec


# -----------------------------
# Data model
# -----------------------------

@dataclass
class RolloutResult:
    site: str
    mgmt_ip: str
    wg_ip_cidr: str
    router_public_key: str
    ok: bool
    error: str = ""


# -----------------------------
# SSH output normalization (Linux side)
# -----------------------------

def _ssh_out(ssh, cmd: str) -> Tuple[str, str, int]:
    """
    Normalize ssh_exec output into (stdout, stderr, rc).
    Your ssh_exec currently returns either:
      - string stdout
      - (stdout, stderr, rc)
    """
    res: Any = ssh_exec(ssh, cmd)
    if isinstance(res, tuple) and len(res) >= 3:
        stdout, stderr, rc = res[0], res[1], res[2]
        return str(stdout or ""), str(stderr or ""), int(rc if rc is not None else 0)
    return str(res or ""), "", 0


def _ssh_text(ssh, cmd: str) -> str:
    out, err, _ = _ssh_out(ssh, cmd)
    # Keep both so probes see YES even if it went to stderr
    return (out or "") + (err or "")


def _has_yes_token(text: str) -> bool:
    return bool(re.search(r"\bYES\b", text or "", flags=re.IGNORECASE))


# -----------------------------
# RouterOS helpers
# -----------------------------

def _ros_escape(s: str) -> str:
    return s.replace('"', r"\"")


def _parse_ros_kv(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def _pick_wg_ips(start_ip: str, count: int, reserved: Optional[set[str]] = None) -> List[str]:
    reserved = reserved or set()
    base = ipaddress.ip_address(start_ip)
    out: List[str] = []
    i = 0
    while len(out) < count:
        cand = ipaddress.ip_address(int(base) + i)
        cand_s = str(cand)
        if cand_s not in reserved:
            out.append(f"{cand_s}/32")
            reserved.add(cand_s)
        i += 1
    return out


# -----------------------------
# Linux (radius-core) helpers
# -----------------------------

PEER_BEGIN = "# BEGIN NETOPS WG RADIUS PEERS"
PEER_END = "# END NETOPS WG RADIUS PEERS"


def _linux_read_file(ssh, path: str) -> Optional[str]:
    log = get_logger()
    # direct
    try:
        out, err, rc = _ssh_out(ssh, f'cat "{path}"')
        if rc == 0:
            return out.rstrip("\n")
        log.debug(f"cat {path} rc={rc} err={err!r}")
    except Exception as e:
        log.debug(f"cat {path} failed: {e}")

    # sudo -n
    try:
        out, err, rc = _ssh_out(ssh, f'sudo -n cat "{path}"')
        if rc == 0:
            return out.rstrip("\n")
        log.debug(f"sudo cat {path} rc={rc} err={err!r}")
    except Exception as e:
        log.debug(f"sudo cat {path} failed: {e}")

    return None


def _linux_write_file(ssh, path: str, content: str) -> None:
    marker = "NETOPS_EOF_93b1b2"
    # direct attempt
    out, err, rc = _ssh_out(ssh, f'cat > "{path}" <<\'{marker}\'\n{content}\n{marker}\n')
    if rc == 0:
        return
    # sudo -n
    out, err, rc = _ssh_out(ssh, f'sudo -n sh -c \'cat > "{path}" <<\\"{marker}\\"\n{content}\n{marker}\n\'')
    if rc != 0:
        raise RuntimeError(f"Failed writing {path}: rc={rc} err={err!r}")


def _linux_mkdir_p(ssh, path: str) -> None:
    out, err, rc = _ssh_out(ssh, f'mkdir -p "{path}"')
    if rc == 0:
        return
    out, err, rc = _ssh_out(ssh, f'sudo -n mkdir -p "{path}"')
    if rc != 0:
        raise RuntimeError(f"Failed mkdir -p {path}: rc={rc} err={err!r}")


def _linux_chmod_600(ssh, path: str) -> None:
    out, err, rc = _ssh_out(ssh, f'chmod 600 "{path}"')
    if rc == 0:
        return
    out, err, rc = _ssh_out(ssh, f'sudo -n chmod 600 "{path}"')
    if rc != 0:
        raise RuntimeError(f"Failed chmod 600 {path}: rc={rc} err={err!r}")


def _linux_has_wg_tools(ssh) -> bool:
    log = get_logger()
    cmd = 'test -x /usr/bin/wg -a -x /usr/bin/wg-quick && echo YES || echo NO'
    try:
        txt = _ssh_text(ssh, cmd)
        log.debug(f"radius-core wg tools probe raw output:\n{txt!r}")
        return _has_yes_token(txt)
    except Exception as e:
        log.debug(f"radius-core wg tools probe failed: {e}")
        return False


def _linux_requires_passwordless_sudo(ssh) -> bool:
    log = get_logger()
    cmd = "sudo -n true && echo YES || echo NO"
    try:
        txt = _ssh_text(ssh, cmd)
        log.debug(f"radius-core sudo -n probe raw output:\n{txt!r}")
        return not _has_yes_token(txt)
    except Exception as e:
        log.debug(f"radius-core sudo -n probe failed: {e}")
        return True


def _linux_gen_core_keys_if_missing(ssh, *, key_path: str, pub_path: str) -> str:
    log = get_logger()

    pub = _linux_read_file(ssh, pub_path)
    if pub and pub.strip():
        return pub.strip()

    if not _linux_has_wg_tools(ssh):
        raise RuntimeError("radius-core: /usr/bin/wg or /usr/bin/wg-quick missing (wireguard-tools).")

    if _linux_requires_passwordless_sudo(ssh):
        raise RuntimeError(
            "radius-core: passwordless sudo is required for core automation. "
            "Either SSH as root, or add a sudoers NOPASSWD rule for wg/wg-quick/systemctl and /etc/wireguard writes."
        )

    _linux_mkdir_p(ssh, str(Path(key_path).parent))

    gen_cmd = (
        "umask 077; "
        f"/usr/bin/wg genkey | /usr/bin/tee '{key_path}' | /usr/bin/wg pubkey | /usr/bin/tee '{pub_path}'"
    )
    out, err, rc = _ssh_out(ssh, f"sudo -n sh -c \"{gen_cmd}\"")
    if rc != 0:
        raise RuntimeError(f"Failed generating core WG keys: rc={rc} err={err!r}")

    _linux_chmod_600(ssh, key_path)

    pub = _linux_read_file(ssh, pub_path)
    if not pub or not pub.strip():
        raise RuntimeError("Failed to generate/read radius-core public key.")
    log.info(f"radius-core: ensured core WG keys at {key_path} / {pub_path}")
    return pub.strip()


def _ensure_wg_quick_config(
    ssh,
    *,
    wg_conf_path: str,
    interface_address: str,
    listen_port: int,
    private_key_path: str,
) -> str:
    conf = _linux_read_file(ssh, wg_conf_path)
    if conf and conf.strip():
        return conf

    if _linux_requires_passwordless_sudo(ssh):
        raise RuntimeError("radius-core: cannot create wg-quick config without passwordless sudo (or root SSH).")

    priv = _linux_read_file(ssh, private_key_path)
    if not priv or not priv.strip():
        raise RuntimeError(f"Missing private key at {private_key_path}; cannot create {wg_conf_path}")

    content = (
        "[Interface]\n"
        f"Address = {interface_address}\n"
        f"ListenPort = {listen_port}\n"
        f"PrivateKey = {priv.strip()}\n\n"
        f"{PEER_BEGIN}\n"
        f"{PEER_END}\n"
    )

    _linux_mkdir_p(ssh, str(Path(wg_conf_path).parent))
    _linux_write_file(ssh, wg_conf_path, content)
    _linux_chmod_600(ssh, wg_conf_path)
    return content


def _inject_peers_block(wg_conf_text: str, peers_block: str) -> str:
    block = f"{PEER_BEGIN}\n{peers_block.rstrip()}\n{PEER_END}"
    if PEER_BEGIN in wg_conf_text and PEER_END in wg_conf_text:
        pattern = re.compile(rf"{re.escape(PEER_BEGIN)}.*?{re.escape(PEER_END)}", re.DOTALL)
        return pattern.sub(block, wg_conf_text)

    sep = "" if wg_conf_text.endswith("\n") else "\n"
    return f"{wg_conf_text}{sep}\n{block}\n"


def _linux_restart_wg_quick(ssh, iface: str) -> None:
    if _linux_requires_passwordless_sudo(ssh):
        raise RuntimeError("radius-core: cannot restart wg-quick without passwordless sudo (or root SSH).")

    out, err, rc = _ssh_out(ssh, f"sudo -n /bin/systemctl restart wg-quick@{iface}")
    if rc == 0:
        return

    _ssh_out(ssh, f"sudo -n /usr/bin/wg-quick down {iface} || true")
    out, err, rc = _ssh_out(ssh, f"sudo -n /usr/bin/wg-quick up {iface}")
    if rc != 0:
        raise RuntimeError(f"Failed wg-quick up {iface}: rc={rc} err={err!r}")


def _build_core_peers_block(results: List[RolloutResult]) -> str:
    lines: List[str] = []
    lines.append("# Managed by netops wg-radius-rollout")
    lines.append("# Do not edit between BEGIN/END markers; re-run rollout instead.")
    lines.append("")
    for r in results:
        if not r.ok or not r.router_public_key:
            continue
        wg_ip_host = r.wg_ip_cidr.split("/")[0]
        lines.append(f"# {r.site} ({r.mgmt_ip})")
        lines.append("[Peer]")
        lines.append(f"PublicKey = {r.router_public_key}")
        lines.append(f"AllowedIPs = {wg_ip_host}/32")
        lines.append("PersistentKeepalive = 25")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


# -----------------------------
# MikroTik worker (unchanged)
# -----------------------------

def _apply_one_mikrotik_blocking(
    dev: Device,
    *,
    wg_if: str,
    wg_listen_port: int,
    wg_ip_cidr: str,
    core_wg_ip: str,
    core_pubkey: str,
    core_endpoint: str,
    core_endpoint_port: int,
    radius_secret: str,
    radius_services: str,
    rule_first: bool,
    apply: bool,
) -> RolloutResult:
    log = get_logger()
    site = dev.site
    ip = dev.mgmt_ip

    try:
        user, pw = resolve_env(dev.user_env, dev.pw_env)
    except Exception as e:
        msg = f"Env resolve failed: {e}"
        log.error(f"{site}: {msg}")
        return RolloutResult(site=site, mgmt_ip=ip, wg_ip_cidr=wg_ip_cidr, router_public_key="", ok=False, error=msg)

    ssh = make_ssh_client(ip, 22, user, pw)

    def run(cmd: str) -> str:
        log.debug(f"{site}: ROS cmd: {cmd}")
        if not apply:
            return ""
        res = ssh_exec(ssh, cmd)
        # RouterOS path expects stdout-like string
        if isinstance(res, tuple):
            return str(res[0] or "").strip()
        return str(res or "").strip()

    try:
        try:
            ssh_exec(ssh, "setline 0")
        except Exception:
            pass

        wg_ip_host = wg_ip_cidr.split("/")[0]

        priv = ""
        pub = ""
        if apply:
            gen_out = run("/interface/wireguard key generate")
            kv = _parse_ros_kv(gen_out)
            priv = kv.get("private-key", "")
            pub = kv.get("public-key", "")
            if not priv or not pub:
                raise RuntimeError(f"WireGuard key generate returned unexpected output: {gen_out!r}")

        existing_if = run(f':put [/interface/wireguard find where name="{_ros_escape(wg_if)}"]')
        if apply and existing_if == "":
            run(
                f'/interface/wireguard add name="{_ros_escape(wg_if)}" '
                f'listen-port={wg_listen_port} private-key="{_ros_escape(priv)}" '
                f'mtu=1420 comment="WG to radius-core"'
            )

        existing_addr = run(
            f':put [/ip/address find where interface="{_ros_escape(wg_if)}" and address="{_ros_escape(wg_ip_cidr)}"]'
        )
        if apply and existing_addr == "":
            run(
                f'/ip/address add address="{_ros_escape(wg_ip_cidr)}" '
                f'interface="{_ros_escape(wg_if)}" comment="WG RADIUS IP"'
            )

        peer_find = run(
            f':put [/interface/wireguard/peers find where interface="{_ros_escape(wg_if)}" '
            f'and public-key="{_ros_escape(core_pubkey)}"]'
        )
        if apply and peer_find == "":
            run(
                f'/interface/wireguard/peers add interface="{_ros_escape(wg_if)}" '
                f'public-key="{_ros_escape(core_pubkey)}" '
                f'endpoint-address="{_ros_escape(core_endpoint)}" endpoint-port={core_endpoint_port} '
                f'allowed-address="{_ros_escape(core_wg_ip)}/32" persistent-keepalive=25 '
                f'comment="radius-core"'
            )

        route_find = run(
            f':put [/ip/route find where dst-address="{_ros_escape(core_wg_ip)}/32" and gateway="{_ros_escape(wg_if)}"]'
        )
        if apply and route_find == "":
            run(
                f'/ip/route add dst-address="{_ros_escape(core_wg_ip)}/32" '
                f'gateway="{_ros_escape(wg_if)}" comment="RADIUS via WG"'
            )

        rad_find = run(
            f':put [/radius find where address="{_ros_escape(core_wg_ip)}" and src-address="{_ros_escape(wg_ip_host)}"]'
        )
        if apply and rad_find == "":
            run(
                f'/radius add address="{_ros_escape(core_wg_ip)}" '
                f'secret="{_ros_escape(radius_secret)}" '
                f'service={_ros_escape(radius_services)} '
                f'src-address="{_ros_escape(wg_ip_host)}" timeout=500ms '
                f'comment="RADIUS over WG"'
            )

        fw_find = run(
            f':put [/ip/firewall/filter find where chain="input" and in-interface="{_ros_escape(wg_if)}" '
            f'and protocol="udp" and dst-port="1812,1813" and src-address="{_ros_escape(core_wg_ip)}"]'
        )
        if apply and fw_find == "":
            cmd = (
                f'/ip/firewall/filter add chain=input action=accept in-interface="{_ros_escape(wg_if)}" '
                f'protocol=udp dst-port=1812,1813 src-address="{_ros_escape(core_wg_ip)}" '
                f'comment="Allow RADIUS from radius-core via WG"'
            )
            if rule_first:
                cmd += " place-before=0"
            run(cmd)

        return RolloutResult(site=site, mgmt_ip=ip, wg_ip_cidr=wg_ip_cidr, router_public_key=pub, ok=True)

    except Exception as e:
        return RolloutResult(site=site, mgmt_ip=ip, wg_ip_cidr=wg_ip_cidr, router_public_key="", ok=False, error=str(e))

    finally:
        try:
            ssh.close()
        except Exception:
            pass


async def _apply_one_mikrotik(dev: Device, **kwargs) -> RolloutResult:
    log = get_logger()
    try:
        return await asyncio.to_thread(_apply_one_mikrotik_blocking, dev, **kwargs)
    except Exception as e:
        msg = f"Unhandled error: {e}"
        log.error(f"{dev.site}: {msg}")
        return RolloutResult(site=dev.site, mgmt_ip=dev.mgmt_ip, wg_ip_cidr=kwargs.get("wg_ip_cidr", ""), router_public_key="", ok=False, error=msg)

# -----------------------------
# CLI
# -----------------------------

@click.command("wg-radius-rollout")
@click.option("-I", "--inventory", "inventory_path", default="inventory.csv",
              help="Unified inventory CSV (Site,Device,MgmtIP,System,Roles,Access,Port,UserEnv,PwEnv,Enabled,Notes).")
@click.option("--roles", default="firewall", show_default=True,
              help="Comma list of roles to include (default: firewall).")

@click.option("--wg-if", default="wg-radius", show_default=True)
@click.option("--wg-listen-port", default=51820, show_default=True, type=int)
@click.option("--wg-start-ip", default="10.255.255.10", show_default=True,
              help="Starting WG IP for assigning /32s to sites (sequential).")

@click.option("--core-wg-ip", default="10.255.255.2", show_default=True,
              help="radius-core WG IP (the RADIUS server address on WG).")
@click.option("--core-endpoint", default="68.131.74.29",
              help="radius-core endpoint (public IP/DNS). If omitted, reads env RADIUS_WG_ENDPOINT.")
@click.option("--core-endpoint-port", default=51820, show_default=True, type=int)

@click.option("--radius-secret", default=None,
              help="RADIUS shared secret. If omitted, reads env RADIUS_SECRET.")
@click.option("--radius-services", default="login,wireless", show_default=True,
              help="RouterOS /radius service= list (e.g. login,wireless,ppp,dhcp).")

@click.option("--rule-first/--rule-append", default=False, show_default=True,
              help="Insert allow rule at top of chain=input (otherwise append).")

@click.option("--concurrency", default=6, show_default=True, type=click.IntRange(1, 64))
@click.option("--progress/--no-progress", default=True, show_default=True)

@click.option("--apply/--dry-run", default=False, show_default=True,
              help="Dry-run prints plan + writes outputs but makes no changes.")

# ---- Core automation options ----
@click.option("--core-host", default="10.100.3.6", show_default=True,
              help="SSH host for radius-core.")
@click.option("--core-user-env", default="USER3", show_default=True,
              help="Env var name holding SSH username for radius-core.")
@click.option("--core-pw-env", default="PW1", show_default=True,
              help="Env var name holding SSH password for radius-core.")
@click.option("--core-wg-iface", default="wg0", show_default=True,
              help="wg-quick interface name on radius-core (wg-quick@<iface>).")
@click.option("--core-wg-conf", default="/etc/wireguard/wg0.conf", show_default=True,
              help="wg-quick config path on radius-core.")
@click.option("--core-key", default="/etc/wireguard/server.key", show_default=True,
              help="Path to store/lookup core private key on radius-core.")
@click.option("--core-pub", default="/etc/wireguard/server.pub", show_default=True,
              help="Path to store/lookup core public key on radius-core.")
@click.option("--core-address", default="10.255.255.2/32", show_default=True,
              help="Address to set in core wg-quick Interface stanza (only used if config is created).")
@click.option("--auto-core/--no-auto-core", default=True, show_default=True,
              help="If enabled, generate core keys, ensure wg config, inject peers, and optionally restart wg.")
@click.option("--restart-core-wg/--no-restart-core-wg", default=True, show_default=True,
              help="Restart wg-quick@<core-wg-iface> after updating core peers (apply mode only).")

# Outputs
@click.option("--export-core-peers/--no-export-core-peers", default=True, show_default=True,
              help="Write local wg-quick peer snippet file using collected router public keys.")
@click.option("--core-peers-file", default=None,
              help="Optional explicit output filename for local peers snippet file.")

@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]))
def wg_radius_rollout_cli(
    inventory_path: str,
    roles: str,
    wg_if: str,
    wg_listen_port: int,
    wg_start_ip: str,
    core_wg_ip: str,
    core_endpoint: Optional[str],
    core_endpoint_port: int,
    radius_secret: Optional[str],
    radius_services: str,
    rule_first: bool,
    concurrency: int,
    progress: bool,
    apply: bool,
    core_host: str,
    core_user_env: str,
    core_pw_env: str,
    core_wg_iface: str,
    core_wg_conf: str,
    core_key: str,
    core_pub: str,
    core_address: str,
    auto_core: bool,
    restart_core_wg: bool,
    export_core_peers: bool,
    core_peers_file: Optional[str],
    log_file: Optional[str],
    log_level: str,
):
    setup_logging(level=log_level, log_file=log_file)
    log = get_logger()
    load_env()

    if not core_endpoint:
        core_endpoint = os.getenv("RADIUS_WG_ENDPOINT", "").strip()
    if not radius_secret:
        radius_secret = os.getenv("RADIUS_SECRET", "").strip()

    if not core_endpoint:
        raise click.ClickException("Missing core endpoint (set env RADIUS_WG_ENDPOINT or pass --core-endpoint).")
    if not radius_secret:
        raise click.ClickException("Missing radius secret (set env RADIUS_SECRET or pass --radius-secret).")

    core_pubkey = ""
    core_ssh = None

    if auto_core:
        core_user = os.getenv(core_user_env, "").strip()
        core_pw = os.getenv(core_pw_env, "").strip()
        if not core_user or not core_pw:
            raise click.ClickException(f"Missing core SSH creds (env {core_user_env}/{core_pw_env}).")

        core_ssh = make_ssh_client(core_host, 22, core_user, core_pw)

        if not _linux_has_wg_tools(core_ssh):
            raise click.ClickException("radius-core: /usr/bin/wg or /usr/bin/wg-quick missing (wireguard-tools).")

        if core_user != "root" and _linux_requires_passwordless_sudo(core_ssh):
            raise click.ClickException(
                "radius-core: passwordless sudo required for core automation. "
                "Either SSH as root or add NOPASSWD sudoers."
            )

        core_pubkey = _linux_gen_core_keys_if_missing(core_ssh, key_path=core_key, pub_path=core_pub)
        log.info(f"radius-core: core public key = {core_pubkey}")

        _ensure_wg_quick_config(
            core_ssh,
            wg_conf_path=core_wg_conf,
            interface_address=core_address,
            listen_port=wg_listen_port,
            private_key_path=core_key,
        )
    else:
        core_pubkey = os.getenv("RADIUS_WG_CORE_PUBKEY", "").strip()
        if not core_pubkey:
            raise click.ClickException("Missing core pubkey (set env RADIUS_WG_CORE_PUBKEY or enable --auto-core).")

    devs = load_inventory_csv(inventory_path)
    role_list = [r.strip().lower() for r in roles.split(",") if r.strip()]
    targets = select(devs, roles_any=role_list, enabled_only=True)
    if not targets:
        click.echo("No targets matched.")
        return

    targets_sorted = sorted(targets, key=lambda d: (d.site or "").lower())
    wg_ips = _pick_wg_ips(wg_start_ip, len(targets_sorted), reserved={core_wg_ip})
    plan: List[Tuple[Device, str]] = list(zip(targets_sorted, wg_ips))

    click.echo("\nPlanned WG assignments:")
    for dev, wg_ip_cidr in plan:
        click.echo(f"  {dev.site:<28} {dev.mgmt_ip:<15} -> {wg_ip_cidr}")
    click.echo("")

    async def _run_all() -> List[RolloutResult]:
        sem = asyncio.Semaphore(concurrency)

        async def _run_one(dev: Device, wg_ip_cidr: str) -> RolloutResult:
            async with sem:
                return await _apply_one_mikrotik(
                    dev,
                    wg_if=wg_if,
                    wg_listen_port=wg_listen_port,
                    wg_ip_cidr=wg_ip_cidr,
                    core_wg_ip=core_wg_ip,
                    core_pubkey=core_pubkey,
                    core_endpoint=core_endpoint,  # type: ignore[arg-type]
                    core_endpoint_port=core_endpoint_port,
                    radius_secret=radius_secret,  # type: ignore[arg-type]
                    radius_services=radius_services,
                    rule_first=rule_first,
                    apply=apply,
                )

        tasks = [asyncio.create_task(_run_one(dev, wg_ip)) for dev, wg_ip in plan]
        results: List[RolloutResult] = []

        if progress:
            with click.progressbar(length=len(tasks), label="WG+RADIUS rollout", show_pos=True) as bar:
                for fut in asyncio.as_completed(tasks):
                    results.append(await fut)
                    bar.update(1)
        else:
            for fut in asyncio.as_completed(tasks):
                results.append(await fut)

        return sorted(results, key=lambda r: (r.site or "").lower())

    results = asyncio.run(_run_all())

    day = datetime.now().strftime("%Y-%m-%d")
    out_csv = Path(f"{day}_wg_radius_rollout_summary.csv")
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["site", "mgmt_ip", "wg_ip", "router_public_key", "ok", "error"])
        for r in results:
            w.writerow([r.site, r.mgmt_ip, r.wg_ip_cidr, r.router_public_key, "TRUE" if r.ok else "FALSE", r.error])

    log.info(f"Wrote CSV: {out_csv}")

    if export_core_peers:
        peers_path = Path(core_peers_file) if core_peers_file else Path(f"{day}_radius-core_wg_peers.conf")
        peers_path.write_text(_build_core_peers_block(results), encoding="utf-8")
        log.info(f"Wrote local peers snippet file: {peers_path}")

    if auto_core and core_ssh is not None:
        try:
            if apply:
                conf_text = _linux_read_file(core_ssh, core_wg_conf) or ""
                peers_block = _build_core_peers_block(results)
                new_conf = _inject_peers_block(conf_text, peers_block)
                if new_conf != conf_text:
                    _linux_write_file(core_ssh, core_wg_conf, new_conf)
                    _linux_chmod_600(core_ssh, core_wg_conf)
                    log.info(f"radius-core: updated {core_wg_conf} with managed peers block")
                if restart_core_wg:
                    _linux_restart_wg_quick(core_ssh, core_wg_iface)
                    log.info(f"radius-core: restarted wg-quick@{core_wg_iface}")
            else:
                log.info("Dry-run: skipping radius-core peer injection/restart (no changes made).")
        finally:
            try:
                core_ssh.close()
            except Exception:
                pass

    if not apply:
        click.echo("\nNOTE: Dry-run mode; no router or core changes were made. Re-run with --apply to push config.\n")


if __name__ == "__main__":
    wg_radius_rollout_cli()
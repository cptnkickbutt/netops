#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import ipaddress
import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from netops.config import load_env, resolve_env  # noqa: E402
from netops.paths import generated_file_path  # noqa: E402
from netops.transports.ssh import make_ssh_client, ssh_exec  # noqa: E402


RADIUS_PUBLIC_IP = "199.115.144.5"
RADIUS_PRIVATE_IP = "10.100.3.6"
PROXMOX_PRIVATE_IP = "10.100.3.10"
PUBLIC_INTERFACE = "vmbr96"
PRIVATE_INTERFACE = "vmbr0"


@dataclass
class RadiusRow:
    site: str
    mgmt_ip: str
    radius_secret: str
    public_ip: str = ""
    method: str = ""
    note: str = ""


@dataclass
class InventoryAuth:
    mgmt_ip: str
    access: str = "ssh"
    port: int = 22
    user_env: str = ""
    pw_env: str = ""


def clean_name(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unnamed_client"


def valid_ip(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
        return ip.version == 4
    except ValueError:
        return False


def is_public_ipv4(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
        return (
            ip.version == 4
            and not ip.is_private
            and not ip.is_loopback
            and not ip.is_link_local
            and not ip.is_multicast
            and not ip.is_reserved
            and not ip.is_unspecified
        )
    except ValueError:
        return False


def first_ipv4(text: str, *, public_only: bool = True) -> str:
    for match in re.finditer(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text or ""):
        ip = match.group(0)
        if public_only:
            if is_public_ipv4(ip):
                return ip
        elif valid_ip(ip):
            return ip
    return ""


def parse_ip_cloud_public(text: str) -> str:
    patterns = [
        r"public-address:\s*((?:\d{1,3}\.){3}\d{1,3})",
        r"public-address=([0-9.]+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text or "", re.IGNORECASE)
        if not match:
            continue
        ip = match.group(1).strip()
        if is_public_ipv4(ip):
            return ip

    return first_ipv4(text, public_only=True)


def parse_dhcp_network_public_gateway(text: str) -> tuple[str, str]:
    for line in (text or "").splitlines():
        if "gateway=" not in line:
            continue

        gateway_match = re.search(r"gateway=([0-9.]+)", line)
        address_match = re.search(r"address=([0-9./]+)", line)

        if not gateway_match:
            continue

        gateway = gateway_match.group(1).strip()
        network = address_match.group(1).strip() if address_match else ""

        if is_public_ipv4(gateway):
            return gateway, f"dhcp-server-network gateway for {network or 'unknown network'}"

    return "", "no public DHCP gateway found"


def parse_ip_address_public(text: str) -> tuple[str, str]:
    candidates: list[tuple[str, str]] = []

    for line in (text or "").splitlines():
        if "address=" not in line:
            continue

        address_match = re.search(r'address=("[^"]+"|\S+)', line)
        iface_match = re.search(r'interface=("[^"]+"|\S+)', line)

        if not address_match:
            continue

        address = address_match.group(1).strip('"')
        iface = iface_match.group(1).strip('"') if iface_match else ""
        ip = address.split("/", 1)[0].strip()

        if is_public_ipv4(ip):
            candidates.append((ip, iface))

    preferred_names = ("bridge_public", "public", "wan", "internet")

    for ip, iface in candidates:
        if iface.lower() in preferred_names:
            return ip, f"public address on interface={iface}"

    for ip, iface in candidates:
        if any(name in iface.lower() for name in preferred_names):
            return ip, f"public address on interface={iface}"

    if candidates:
        ip, iface = candidates[0]
        return ip, f"public address on interface={iface or 'unknown'}"

    return "", "no public address found"


def read_radius_csv(path: Path) -> list[RadiusRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows: list[RadiusRow] = []

        for row in reader:
            rows.append(
                RadiusRow(
                    site=(row.get("Site") or "").strip(),
                    mgmt_ip=(row.get("MgmtIP") or "").strip(),
                    public_ip=(row.get("PublicIP") or "").strip(),
                    radius_secret=(row.get("RadiusSecret") or "").strip(),
                )
            )

        return rows


def read_inventory_auth(path: Path) -> dict[str, InventoryAuth]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        auth_by_mgmt: dict[str, InventoryAuth] = {}

        for row in reader:
            mgmt_ip = (row.get("MgmtIP") or "").strip()
            if not mgmt_ip:
                continue

            try:
                port = int((row.get("Port") or "22").strip() or "22")
            except ValueError:
                port = 22

            auth_by_mgmt[mgmt_ip] = InventoryAuth(
                mgmt_ip=mgmt_ip,
                access=(row.get("Access") or "ssh").strip().lower(),
                port=port,
                user_env=(row.get("UserEnv") or "").strip(),
                pw_env=(row.get("PwEnv") or "").strip(),
            )

        return auth_by_mgmt


def run_router_cmd(client, cmd: str, timeout: int) -> str:
    out, err, _rc = ssh_exec(client, cmd, timeout=timeout)
    return f"{out}\n{err}"


def pull_public_ip(auth: InventoryAuth, timeout: int) -> tuple[str, str, str]:
    if auth.access != "ssh":
        return "", "skipped", f"Access={auth.access!r}"
    if not auth.user_env or not auth.pw_env:
        return "", "skipped", "missing UserEnv/PwEnv"

    username, password = resolve_env(auth.user_env, auth.pw_env)
    client = None
    notes: list[str] = []

    try:
        client = make_ssh_client(
            host=auth.mgmt_ip,
            port=auth.port,
            username=username,
            password=password,
            timeout=timeout,
            strict_host_key=False,
        )

        checks = [
            (
                "ip-cloud-get",
                ':put [/ip cloud get public-address]',
                lambda text: (parse_ip_cloud_public(text), "ip cloud get public-address"),
            ),
            (
                "ip-cloud-print",
                "/ip cloud print",
                lambda text: (parse_ip_cloud_public(text), "ip cloud print"),
            ),
            (
                "dhcp-network",
                "/ip dhcp-server network print detail without-paging",
                parse_dhcp_network_public_gateway,
            ),
            (
                "ipify",
                '/tool fetch url="http://api.ipify.org" output=user',
                lambda text: (first_ipv4(text), "api.ipify.org"),
            ),
            (
                "ifconfig",
                '/tool fetch url="http://ifconfig.me/ip" output=user',
                lambda text: (first_ipv4(text), "ifconfig.me"),
            ),
            (
                "amazon-checkip",
                '/tool fetch url="http://checkip.amazonaws.com" output=user',
                lambda text: (first_ipv4(text), "checkip.amazonaws.com"),
            ),
            (
                "ip-address",
                "/ip address print detail without-paging",
                parse_ip_address_public,
            ),
        ]

        for method, cmd, parser in checks:
            text = run_router_cmd(client, cmd, timeout)
            ip, note = parser(text)

            if ip:
                return ip, method, note

            notes.append(f"{method}: no match")

        return "", "manual", "; ".join(notes)

    except Exception as exc:
        return "", "ssh-failed", str(exc)

    finally:
        if client is not None:
            client.close()


def write_radius_csv(path: Path, rows: list[RadiusRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Site", "MgmtIP", "PublicIP", "RadiusSecret", "Method", "Note"],
        )
        writer.writeheader()

        for row in rows:
            writer.writerow(
                {
                    "Site": row.site,
                    "MgmtIP": row.mgmt_ip,
                    "PublicIP": row.public_ip,
                    "RadiusSecret": row.radius_secret,
                    "Method": row.method,
                    "Note": row.note,
                }
            )


def write_clients_conf(path: Path, rows: list[RadiusRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Generated by scripts/reconcile_radius_publics.py",
        "",
    ]

    for row in rows:
        ipaddr = row.public_ip or row.mgmt_ip
        name = clean_name(row.site)

        lines.extend(
            [
                f"client {name} {{",
                f"    ipaddr = {ipaddr}",
                f"    secret = {row.radius_secret}",
                f"    shortname = {name}",
                "}",
                "",
            ]
        )

    path.write_text("\n".join(lines), encoding="utf-8")


def write_proxmox_rules(path: Path, rows: list[RadiusRow], include_proxmox_routes: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by scripts/reconcile_radius_publics.py",
        "# Review before applying.",
        "",
        "set -euo pipefail",
        "",
    ]

    for row in rows:
        if not row.public_ip:
            continue

        site = row.site or row.mgmt_ip
        public_ip = row.public_ip

        lines.extend(
            [
                f"# {site}",
                f"iptables -t nat -A PREROUTING -i {PUBLIC_INTERFACE} -s {public_ip} -d {RADIUS_PUBLIC_IP} -p udp --dport 1812 -j DNAT --to-destination {RADIUS_PRIVATE_IP}:1812",
                f"iptables -t nat -A PREROUTING -i {PUBLIC_INTERFACE} -s {public_ip} -d {RADIUS_PUBLIC_IP} -p udp --dport 1813 -j DNAT --to-destination {RADIUS_PRIVATE_IP}:1813",
                "",
                f"iptables -A FORWARD -i {PUBLIC_INTERFACE} -o {PRIVATE_INTERFACE} -s {public_ip} -d {RADIUS_PRIVATE_IP} -p udp --dport 1812 -j ACCEPT",
                f"iptables -A FORWARD -i {PUBLIC_INTERFACE} -o {PRIVATE_INTERFACE} -s {public_ip} -d {RADIUS_PRIVATE_IP} -p udp --dport 1813 -j ACCEPT",
                f"iptables -A FORWARD -i {PRIVATE_INTERFACE} -o {PUBLIC_INTERFACE} -s {RADIUS_PRIVATE_IP} -d {public_ip} -p udp --sport 1812 -j ACCEPT",
                f"iptables -A FORWARD -i {PRIVATE_INTERFACE} -o {PUBLIC_INTERFACE} -s {RADIUS_PRIVATE_IP} -d {public_ip} -p udp --sport 1813 -j ACCEPT",
                "",
            ]
        )

        if include_proxmox_routes:
            lines.extend(
                [
                    f"ip route replace {public_ip}/32 via {PROXMOX_PRIVATE_IP}",
                    "",
                ]
            )

    path.write_text("\n".join(lines), encoding="utf-8")


def write_radius_routes(path: Path, rows: list[RadiusRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by scripts/reconcile_radius_publics.py",
        "# Run on radius-core.",
        "",
        "set -euo pipefail",
        "",
    ]

    for row in rows:
        if not row.public_ip:
            continue

        lines.extend(
            [
                f"# {row.site or row.mgmt_ip}",
                f"ip route replace {row.public_ip}/32 via {PROXMOX_PRIVATE_IP}",
                "",
            ]
        )

    path.write_text("\n".join(lines), encoding="utf-8")


def write_review_report(path: Path, rows: list[RadiusRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    seen: dict[str, list[RadiusRow]] = {}
    for row in rows:
        if row.public_ip:
            seen.setdefault(row.public_ip, []).append(row)

    lines = [
        "# Radius Public IP Review",
        "",
        "## Duplicate Public IPs",
        "",
    ]

    duplicates = {ip: items for ip, items in seen.items() if len(items) > 1}

    if duplicates:
        for ip, items in duplicates.items():
            lines.append(f"### {ip}")
            for item in items:
                lines.append(f"- {item.site} ({item.mgmt_ip}) method={item.method} note={item.note}")
            lines.append("")
    else:
        lines.append("No duplicate public IPs found.")
        lines.append("")

    missing = [r for r in rows if not r.public_ip]
    lines.extend(["## Missing Public IPs", ""])

    if missing:
        for row in missing:
            lines.append(f"- {row.site} ({row.mgmt_ip}) method={row.method} note={row.note}")
    else:
        lines.append("No missing public IPs.")

    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh public IPs in radius_secrets.csv and regenerate RADIUS/iptables helper files."
    )
    parser.add_argument("--secrets", default=None, help="Input secrets CSV. Defaults to files/radius_secrets.csv.")
    parser.add_argument("--inventory", default="inventory.csv")
    parser.add_argument("--out", default=None, help="Output CSV path. Defaults to files/radius_secrets_updated.csv.")
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--ssh-timeout", type=int, default=15)

    parser.add_argument("--refresh-public", action="store_true")

    parser.add_argument("--clients-out", default=None, help="Defaults to files/radius_clients.conf.")
    parser.add_argument("--proxmox-out", default=None, help="Defaults to files/radius_proxmox_rules.sh.")
    parser.add_argument("--radius-routes-out", default=None, help="Defaults to files/radius_routes.sh.")
    parser.add_argument("--review-out", default=None, help="Defaults to files/radius_public_review.md.")

    parser.add_argument(
        "--include-proxmox-routes",
        action="store_true",
        help="Also add ip route replace PUBLICIP/32 via 10.100.3.10 to the Proxmox rules file.",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env(args.env_file)

    secrets_path = Path(args.secrets) if args.secrets else generated_file_path("radius_secrets.csv")
    out_path = Path(args.out) if args.out else generated_file_path("radius_secrets_updated.csv")
    clients_path = Path(args.clients_out) if args.clients_out else generated_file_path("radius_clients.conf")
    proxmox_path = Path(args.proxmox_out) if args.proxmox_out else generated_file_path("radius_proxmox_rules.sh")
    routes_path = Path(args.radius_routes_out) if args.radius_routes_out else generated_file_path("radius_routes.sh")
    review_path = Path(args.review_out) if args.review_out else generated_file_path("radius_public_review.md")

    if not args.secrets and not secrets_path.exists():
        legacy_secrets_path = Path("radius_secrets.csv")
        if legacy_secrets_path.exists():
            secrets_path = legacy_secrets_path

    rows = read_radius_csv(secrets_path)
    auth_by_mgmt = read_inventory_auth(Path(args.inventory))

    if args.refresh_public:
        for row in rows:
            auth = auth_by_mgmt.get(row.mgmt_ip)
            if not auth:
                row.method = "missing-inventory"
                row.note = f"no inventory auth found for {row.mgmt_ip}"
                print(f"{row.site}: {row.note}")
                continue

            old_public = row.public_ip
            new_public, method, note = pull_public_ip(auth, timeout=args.ssh_timeout)

            row.method = method
            row.note = note

            if new_public:
                row.public_ip = new_public

            if old_public and new_public and old_public != new_public:
                print(f"{row.site}: public changed {old_public} -> {new_public} ({method}: {note})")
            elif new_public:
                print(f"{row.site}: public {new_public} ({method}: {note})")
            else:
                print(f"{row.site}: no public found ({method}: {note})")

    write_radius_csv(out_path, rows)
    write_clients_conf(clients_path, rows)
    write_proxmox_rules(proxmox_path, rows, args.include_proxmox_routes)
    write_radius_routes(routes_path, rows)
    write_review_report(review_path, rows)

    missing = [r for r in rows if not r.public_ip]
    duplicates = {}
    for row in rows:
        if row.public_ip:
            duplicates.setdefault(row.public_ip, []).append(row)
    duplicates = {ip: items for ip, items in duplicates.items() if len(items) > 1}

    print(f"Wrote updated secrets CSV: {out_path}")
    print(f"Wrote FreeRADIUS clients: {clients_path}")
    print(f"Wrote Proxmox rules: {proxmox_path}")
    print(f"Wrote radius routes: {routes_path}")
    print(f"Wrote review report: {review_path}")

    if missing:
        print(f"Missing PublicIP: {len(missing)} rows")

    if duplicates:
        print(f"Duplicate PublicIP warning: {len(duplicates)} public IPs appear more than once")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

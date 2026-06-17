#!/usr/bin/env python3
"""
Generate RADIUS shared secrets for firewall-tagged inventory rows.

Default CSV:
    Site,MgmtIP,RadiusSecret

Optional public IP pull:
    - Prefer interface=Bridge_Public
    - Fallback to addresses starting with 199. or 68.
"""

from __future__ import annotations

import argparse
import csv
import ipaddress
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from netops.config import load_env, resolve_env  # noqa: E402
from netops.paths import generated_file_path  # noqa: E402
from netops.security.passwords import MemorablePasswordPolicy, generate_memorable_password  # noqa: E402

try:
    from zxcvbn import zxcvbn  # type: ignore
except Exception:
    zxcvbn = None


PUBLIC_PREFIXES = ("199.", "68.")
DEFAULT_OUTPUT_COLUMNS = ["Site", "MgmtIP", "RadiusSecret"]
PUBLIC_OUTPUT_COLUMNS = ["Site", "MgmtIP", "PublicIP", "RadiusSecret"]


@dataclass(frozen=True)
class InventoryRow:
    site: str
    device: str
    mgmt_ip: str
    roles: str
    access: str
    port: int
    user_env: str
    pw_env: str
    enabled: str


@dataclass(frozen=True)
class SecretResult:
    site: str
    mgmt_ip: str
    radius_secret: str
    public_ip: str = ""
    note: str = ""


def truthy(value: str) -> bool:
    return str(value or "").strip().lower() in {"yes", "y", "true", "1", "enabled"}


def has_role(roles: str, wanted: str) -> bool:
    parts = [p.strip().lower() for p in str(roles or "").split(",")]
    return wanted.lower() in parts


def clean_name(value: str) -> str:
    value = str(value or "").strip()
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "unnamed_client"


def read_firewall_rows(path: Path) -> list[InventoryRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise RuntimeError(f"Inventory has no header row: {path}")

        normalized = {name.strip().lower(): name for name in reader.fieldnames}
        required = ["site", "mgmtip", "roles", "enabled"]
        missing = [col for col in required if col not in normalized]
        if missing:
            raise RuntimeError(
                f"Inventory missing required columns: {', '.join(missing)}. "
                f"Seen: {reader.fieldnames}"
            )

        def get(row: dict[str, str], key: str, default: str = "") -> str:
            source_key = normalized.get(key.lower())
            if source_key is None:
                return default
            return str(row.get(source_key, default) or "").strip()

        rows: list[InventoryRow] = []
        for row in reader:
            roles = get(row, "roles")
            if not truthy(get(row, "enabled")):
                continue
            if not has_role(roles, "firewall"):
                continue

            try:
                port = int(get(row, "port", "22") or "22")
            except ValueError:
                port = 22

            rows.append(
                InventoryRow(
                    site=get(row, "site"),
                    device=get(row, "device"),
                    mgmt_ip=get(row, "mgmtip"),
                    roles=roles,
                    access=get(row, "access", "ssh").lower() or "ssh",
                    port=port,
                    user_env=get(row, "userenv"),
                    pw_env=get(row, "pwenv"),
                    enabled=get(row, "enabled"),
                )
            )
        return rows


def fallback_strength_ok(secret: str) -> bool:
    has_upper = bool(re.search(r"[A-Z]", secret))
    has_lower = bool(re.search(r"[a-z]", secret))
    has_digit = bool(re.search(r"\d", secret))
    has_symbol = bool(re.search(r"[^A-Za-z0-9]", secret))
    return len(secret) >= 14 and has_upper and has_lower and has_digit and has_symbol


def strength_score(secret: str) -> tuple[int, str]:
    if zxcvbn is not None:
        result = zxcvbn(secret)
        return int(result.get("score", 0)), "zxcvbn"
    return (4 if fallback_strength_ok(secret) else 0), "fallback"


def generate_checked_secret(
    *,
    min_score: int,
    max_attempts: int,
    min_numbers: int,
    max_numbers: int,
) -> tuple[str, int, str, int]:
    policy = MemorablePasswordPolicy(
        min_numbers=min_numbers,
        max_numbers=max_numbers,
        substitution_chance=0.70,
        uppercase_chance=0.45,
    )

    best_secret = ""
    best_score = -1
    checker = "unknown"

    for attempt in range(1, max_attempts + 1):
        secret = generate_memorable_password(policy)
        score, checker = strength_score(secret)
        if score > best_score:
            best_secret = secret
            best_score = score
        if score >= min_score:
            return secret, score, checker, attempt

    return best_secret, best_score, checker, max_attempts


def parse_ip_address_print(output: str) -> tuple[str, str]:
    candidates: list[tuple[str, str, str]] = []

    for raw_line in output.splitlines():
        line = raw_line.strip()
        if "address=" not in line:
            continue

        address_match = re.search(r'address=("[^"]+"|\S+)', line)
        iface_match = re.search(r'interface=("[^"]+"|\S+)', line)
        if not address_match:
            continue

        address = address_match.group(1).strip('"')
        iface = iface_match.group(1).strip('"') if iface_match else ""
        ip_only = address.split("/", 1)[0].strip()

        try:
            ipaddress.ip_address(ip_only)
        except ValueError:
            continue

        candidates.append((ip_only, iface, line))

    for ip_only, iface, _line in candidates:
        if iface.lower() == "bridge_public":
            return ip_only, "interface=Bridge_Public"

    for ip_only, iface, _line in candidates:
        if ip_only.startswith(PUBLIC_PREFIXES):
            return ip_only, f"public prefix on interface={iface or 'unknown'}"

    return "", "no Bridge_Public or 199./68. address found"


def pull_public_ip(row: InventoryRow, *, timeout: int) -> tuple[str, str]:
    if row.access.lower() != "ssh":
        return "", f"skipped: Access={row.access!r}"
    if not row.user_env or not row.pw_env:
        return "", "skipped: missing UserEnv/PwEnv"

    from netops.transports.ssh import make_ssh_client, ssh_exec

    username, password = resolve_env(row.user_env, row.pw_env)
    client = None
    try:
        client = make_ssh_client(
            host=row.mgmt_ip,
            port=row.port,
            username=username,
            password=password,
            timeout=timeout,
            strict_host_key=False,
        )
        out, err, rc = ssh_exec(client, "/ip address print detail without-paging", timeout=timeout)
        if rc != 0 and not out:
            return "", f"ssh command failed rc={rc}: {err.strip()}"
        return parse_ip_address_print(out)
    except Exception as exc:
        return "", f"ssh failed: {exc}"
    finally:
        if client is not None:
            client.close()


def write_csv(path: Path, results: Iterable[SecretResult], *, include_public: bool) -> None:
    columns = PUBLIC_OUTPUT_COLUMNS if include_public else DEFAULT_OUTPUT_COLUMNS
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for item in results:
            row = {
                "Site": item.site,
                "MgmtIP": item.mgmt_ip,
                "PublicIP": item.public_ip,
                "RadiusSecret": item.radius_secret,
            }
            writer.writerow({key: row[key] for key in columns})


def write_clients_conf(path: Path, results: Iterable[SecretResult], *, prefer_public: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["# Generated by scripts/generate_radius_secrets.py", ""]
    for item in results:
        ipaddr = item.public_ip if prefer_public and item.public_ip else item.mgmt_ip
        block_name = clean_name(item.site)
        lines.extend(
            [
                f"client {block_name} {{",
                f"    ipaddr = {ipaddr}",
                f"    secret = {item.radius_secret}",
                f"    shortname = {block_name}",
                "}",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")

def write_mikrotik_rsc(path: Path, results: Iterable[SecretResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    RADIUS_IP = "199.115.144.5"

    lines: list[str] = [
        "# Generated by scripts/generate_radius_secrets.py",
        "# Review before applying.",
        "",
        "/radius",
    ]

    for item in results:
        comment = clean_name(item.site)

        lines.append(
            f'add service=login address={RADIUS_IP} secret="{item.radius_secret}" '
            f'timeout=3s comment="{comment}"'
        )

    lines.extend(
        [
            "",
            "# Optional (only if not already configured):",
            "# /user aaa set use-radius=yes",
            "",
        ]
    )

    path.write_text("\n".join(lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate RADIUS shared secrets for firewall inventory rows.")
    parser.add_argument("--inventory", default="inventory.csv")
    parser.add_argument("--out", default=None, help="Output CSV path. Defaults to files/radius_secrets.csv.")
    parser.add_argument("--env-file", default=None)
    parser.add_argument("--pull-public", action="store_true")
    parser.add_argument("--ssh-timeout", type=int, default=12)
    parser.add_argument("--clients-out", default=None)
    parser.add_argument("--mikrotik-out", default=None)
    parser.add_argument(
        "--clients-prefer-public",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--min-numbers", type=int, default=6)
    parser.add_argument("--max-numbers", type=int, default=8)
    parser.add_argument("--min-zxcvbn-score", type=int, default=4, choices=range(0, 5))
    parser.add_argument("--max-attempts", type=int, default=200)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env(args.env_file)
    out_path = Path(args.out) if args.out else generated_file_path("radius_secrets.csv")

    rows = read_firewall_rows(Path(args.inventory))
    if not rows:
        print("No enabled firewall rows found.")
        return 1

    results: list[SecretResult] = []
    checker_used = "unknown"

    for row in rows:
        secret, score, checker_used, attempts = generate_checked_secret(
            min_score=args.min_zxcvbn_score,
            max_attempts=args.max_attempts,
            min_numbers=args.min_numbers,
            max_numbers=args.max_numbers,
        )

        public_ip = ""
        note = f"strength={score}/4 via {checker_used}; attempts={attempts}"

        if args.pull_public:
            public_ip, public_note = pull_public_ip(row, timeout=args.ssh_timeout)
            note = f"{note}; public={public_note}"

        results.append(
            SecretResult(
                site=row.site,
                mgmt_ip=row.mgmt_ip,
                public_ip=public_ip,
                radius_secret=secret,
                note=note,
            )
        )

        print(f"{row.site}: generated ({note})")

    write_csv(out_path, results, include_public=args.pull_public)
    print(f"Wrote {len(results)} rows to {out_path}")

    if args.clients_out:
        write_clients_conf(
            Path(args.clients_out),
            results,
            prefer_public=args.clients_prefer_public,
        )
        print(f"Wrote FreeRADIUS clients file to {args.clients_out}")

    if checker_used == "fallback":
        print("Note: zxcvbn is not installed; used fallback strength checks. For better scoring: pip install zxcvbn")

    if args.mikrotik_out:
        write_mikrotik_rsc(Path(args.mikrotik_out), results)
        print(f"Wrote MikroTik RSC file to {args.mikrotik_out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

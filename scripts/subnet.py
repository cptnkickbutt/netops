#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import ipaddress
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Optional XLSX support (installed in your environment)
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None


# ---- Import your helper (works when run from repo root) ----
def _import_subnetting_helpers():
    """
    Tries to import from the installed package first.
    Falls back to adding repo root to sys.path when run from /opt/netops/repo.
    """
    try:
        from netops.ipam.subnetting import nth_subnet, describe_subnet  # type: ignore
        return nth_subnet, describe_subnet
    except Exception:
        # If running from repo root, ensure src/ is on path
        repo_root = Path(__file__).resolve().parents[1]
        src_path = repo_root / "src"
        if src_path.exists():
            sys.path.insert(0, str(src_path))
        from netops.ipam.subnetting import nth_subnet, describe_subnet  # type: ignore
        return nth_subnet, describe_subnet


nth_subnet, describe_subnet = _import_subnetting_helpers()


# ---- Core subnet row builder ----
def _ip_add(ip: ipaddress.IPv4Address, n: int) -> ipaddress.IPv4Address:
    return ipaddress.IPv4Address(int(ip) + n)


def subnet_fields(
    *,
    base_net: str,
    prefix: int,
    index: int,
    gateway_offset: int = 1,
) -> Dict[str, str]:
    """
    Returns a dict of subnet columns for the Nth subnet of base_net/prefix.
    """
    net = nth_subnet(base_net, prefix, index)
    info = describe_subnet(net, gateway_offset=gateway_offset)

    gateway = info.gateway
    last_usable = info.last_usable

    # Pool: remaining usables after gateway
    pool_start = ""
    pool_end = ""
    pool = ""

    try:
        candidate_pool_start = _ip_add(gateway, 1)
        if int(candidate_pool_start) <= int(last_usable):
            pool_start = str(candidate_pool_start)
            pool_end = str(last_usable)
            pool = f"{pool_start}-{pool_end}"
    except Exception:
        pass

    return {
        "cidr": info.cidr,
        "network": str(info.network.network_address),
        "prefix": str(info.network.prefixlen),
        "gateway": str(gateway),
        "pool": pool,
        "pool_start": pool_start,
        "pool_end": pool_end,
        "first_usable": str(info.first_usable),
        "last_usable": str(info.last_usable),
        "broadcast": str(info.broadcast),
    }


# ---- IO helpers ----
def read_table(path: Path, sheet: Optional[str] = None) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Returns (rows, columns) where rows is list of dict[str,str].
    Supports CSV and XLSX.
    """
    suffix = path.suffix.lower()

    if suffix == ".csv":
        # utf-8-sig strips Excel's UTF-8 BOM if present
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = [dict(r) for r in reader]
            cols = reader.fieldnames or []
        if not cols:
            raise SystemExit(f"CSV has no header row: {path}")
        return rows, cols

    if suffix in (".xlsx", ".xlsm", ".xls"):
        if pd is None:
            raise SystemExit("XLSX support requires pandas+openpyxl. (pandas import failed)")
        df = pd.read_excel(path, sheet_name=sheet or 0, dtype=str)
        df = df.fillna("")
        cols = list(df.columns)
        rows = df.to_dict(orient="records")
        # Ensure plain strings
        rows = [{k: ("" if v is None else str(v)) for k, v in r.items()} for r in rows]
        return rows, cols

    raise SystemExit(f"Unsupported input type: {path.suffix} (use .csv or .xlsx)")


def write_table(path: Path, rows: List[Dict[str, str]], columns: List[str], sheet: str = "Sheet1") -> None:
    suffix = path.suffix.lower()

    if suffix == ".csv":
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=columns)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in columns})
        return

    if suffix in (".xlsx", ".xlsm", ".xls"):
        if pd is None:
            raise SystemExit("XLSX output requires pandas+openpyxl. (pandas import failed)")
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(rows, columns=columns)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet)
        return

    raise SystemExit(f"Unsupported output type: {path.suffix} (use .csv or .xlsx)")


# ---- Modes ----
SUBNET_COLS = [
    "cidr",
    "network",
    "prefix",
    "gateway",
    "pool",
    "pool_start",
    "pool_end",
    "first_usable",
    "last_usable",
    "broadcast",
]


def mode_generate(*, start_vlan: int, base_net: str, prefix: int, count: int, gateway_offset: int) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for i in range(count):
        vlan = start_vlan + i
        fields = subnet_fields(base_net=base_net, prefix=prefix, index=i, gateway_offset=gateway_offset)
        out.append({"vlan": str(vlan), **fields})
    return out


def mode_augment(
    *,
    rows: List[Dict[str, str]],
    start_vlan: Optional[int],
    base_net: str,
    prefix: int,
    gateway_offset: int,
) -> List[Dict[str, str]]:
    """
    Adds subnet columns to each row.

    If a 'vlan' column exists:
      index = int(vlan) - start_vlan
      (start_vlan defaults to min(vlan) if not provided)

    Else:
      index = row_number (0..)
    """
    has_vlan = any("vlan" in r for r in rows)

    if has_vlan:
        # Determine start_vlan if not provided
        if start_vlan is None:
            vlans = []
            for r in rows:
                v = str(r.get("vlan", "")).strip()
                if v.isdigit():
                    vlans.append(int(v))
            if not vlans:
                raise SystemExit("Input has a 'vlan' column but no numeric VLAN values were found.")
            start_vlan = min(vlans)

        for r in rows:
            v = str(r.get("vlan", "")).strip()
            if not v.isdigit():
                raise SystemExit(f"Non-numeric VLAN value encountered: vlan={v!r}")
            idx = int(v) - int(start_vlan)
            if idx < 0:
                raise SystemExit(f"VLAN {v} is lower than start-vlan {start_vlan} (index would be negative).")
            r.update(subnet_fields(base_net=base_net, prefix=prefix, index=idx, gateway_offset=gateway_offset))
    else:
        for idx, r in enumerate(rows):
            r.update(subnet_fields(base_net=base_net, prefix=prefix, index=idx, gateway_offset=gateway_offset))

    return rows


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Generate subnet CSVs, or augment an existing CSV/XLSX by appending subnet columns."
    )

    p.add_argument("--base-net", required=True, help='Base network for subnetting (e.g. "192.168.96.0" or "192.168.96.0/24")')
    p.add_argument("--prefix", type=int, required=True, help="Prefix length for generated subnets (e.g. 24, 28)")
    p.add_argument("--gateway-offset", type=int, default=1, help="Gateway offset from network (default 1 => first usable)")

    # Generate mode args
    p.add_argument("--start-vlan", type=int, help="Starting VLAN (required for generate; optional for augment if vlan column exists)")
    p.add_argument("--count", type=int, help="How many subnets to generate (generate mode)")

    # Augment mode args
    p.add_argument("--in", dest="in_path", help="Input CSV/XLSX to augment (adds subnet columns)")
    p.add_argument("--sheet", help="Excel sheet name (XLSX input/output)")

    # Output
    p.add_argument("--out", default="-", help='Output path. "-" prints CSV to stdout (generate mode only).')

    args = p.parse_args(argv)

    base_net = args.base_net
    prefix = int(args.prefix)
    gateway_offset = int(args.gateway_offset)

    if args.in_path:
        # Augment mode
        in_path = Path(args.in_path)
        rows, cols = read_table(in_path, sheet=args.sheet)

        rows = mode_augment(
            rows=rows,
            start_vlan=args.start_vlan,
            base_net=base_net,
            prefix=prefix,
            gateway_offset=gateway_offset,
        )

        # Extend columns (preserve existing order, append new subnet columns)
        for c in SUBNET_COLS:
            if c not in cols:
                cols.append(c)

        out_path = Path(args.out)
        if args.out == "-":
            raise SystemExit("Augment mode requires --out to be a file path (.csv or .xlsx), not '-'.")

        write_table(out_path, rows, cols, sheet=args.sheet or "Sheet1")
        print(f"Wrote {len(rows)} rows → {out_path}")
        return 0

    # Generate mode
    if args.start_vlan is None or args.count is None:
        raise SystemExit("Generate mode requires --start-vlan and --count (or use --in to augment a file).")

    rows = mode_generate(
        start_vlan=int(args.start_vlan),
        base_net=base_net,
        prefix=prefix,
        count=int(args.count),
        gateway_offset=gateway_offset,
    )

    out_cols = ["vlan", *SUBNET_COLS]

    if args.out == "-":
        w = csv.DictWriter(sys.stdout, fieldnames=out_cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    else:
        out_path = Path(args.out)
        write_table(out_path, rows, out_cols, sheet=args.sheet or "Sheet1")
        print(f"Wrote {len(rows)} rows → {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

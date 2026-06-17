from __future__ import annotations

import asyncio
import ipaddress
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import click
import pandas as pd
from tqdm import tqdm

from ..config import load_env, resolve_env
from ..inventory import Device, load_inventory_csv
from ..logging import get_logger, setup_logging
from ..paths import generated_file_path
from ..transports.ssh import make_ssh_client, ssh_exec


DEFAULT_NEIGHBOR_SCRIPT = r''':global matches [/ip neighbor print as-value];
:foreach match in=$matches do=[:put (($match->"identity") . "," . ($match->"interface") . "," . ($match->"address") . "," . ($match->"mac-address") . ";");]
'''

RAW_COLUMNS = [
    "Site",
    "Source Router",
    "Identity",
    "Device_Role",
    "Match_Key",
    "IP",
    "MAC",
    "Interface",
    "WIFI",
    "SSID_2.4",
    "SSID_5",
    "PW",
    "Queue",
    "Speed(Mbps)",
    "Status",
    "Error",
]

MERGED_COLUMNS = [
    "Site",
    "Source Router",
    "Customer_Key",
    "Queue_Source_Identity",
    "Queue_Source_IP",
    "Queue_Source_MAC",
    "Queue_Source_Interface",
    "Queue_Disabled",
    "Speed(Mbps)",
    "Queue_Status",
    "Wireless_Source_Identity",
    "Wireless_Source_IP",
    "Wireless_Source_MAC",
    "Wireless_Source_Interface",
    "Wireless_Disabled",
    "SSID_2.4",
    "SSID_5",
    "PW",
    "Wireless_Status",
    "Merge_Status",
    "Error",
]

NO_DATA_VALUES = {"", "no data", "none", "nan"}


@dataclass(frozen=True)
class Neighbor:
    site: str
    source_router: str
    identity: str
    ip: str
    mac: str
    interface: str
    vlan_id: int | None = None


def _safe_filename_part(value: str) -> str:
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "site"


def _looks_like_ip(value: str) -> bool:
    try:
        ipaddress.ip_address((value or "").strip())
        return True
    except Exception:
        return False


def _load_neighbor_script(script_path: str | Path | None) -> str:
    if script_path:
        return Path(script_path).read_text(encoding="utf-8")

    cwd_script = Path("getNeighbors2.rsc")
    if cwd_script.exists():
        return cwd_script.read_text(encoding="utf-8")

    return DEFAULT_NEIGHBOR_SCRIPT


def _clean_control_chars(raw: str) -> str:
    escapes = "".join(chr(char) for char in range(1, 32))
    return raw.translate(str.maketrans("", "", escapes))


def _parse_neighbor_output(raw: str, *, site: str, source_router: str) -> list[Neighbor]:
    """
    Parse either known neighbor-script column order:
      - current netops getNeighbors2.rsc: Identity,Interface,IP,MAC
      - older filtered getNeighbors.rsc: Identity,IP,MAC,Interface
    """
    cleaned = _clean_control_chars(raw)
    neighbors: list[Neighbor] = []
    seen: set[tuple[str, str]] = set()

    for record in cleaned.split(";"):
        parts = [p.strip() for p in record.split(",")]
        if len(parts) != 4 or not any(parts):
            continue

        if _looks_like_ip(parts[1]):
            identity, ip, mac, interface = parts
        elif _looks_like_ip(parts[2]):
            identity, interface, ip, mac = parts
        else:
            # Not a neighbor row we understand.
            continue

        key = (ip, identity)
        if key in seen:
            continue
        seen.add(key)

        neighbors.append(
            Neighbor(
                site=site,
                source_router=source_router,
                identity=identity,
                ip=ip,
                mac=mac,
                interface=interface,
            )
        )

    neighbors.sort(key=lambda n: (n.identity.lower(), n.interface.lower(), n.ip))
    return neighbors


def _filter_neighbors(
    neighbors: list[Neighbor],
    *,
    identity_filter: str | None,
    interface_filter: str | None,
    all_neighbors: bool,
) -> list[Neighbor]:
    selected = neighbors

    if identity_filter:
        rx = re.compile(identity_filter, flags=re.I)
        selected = [n for n in selected if rx.search(n.identity or "")]

    if not all_neighbors and interface_filter:
        rx = re.compile(interface_filter, flags=re.I)
        selected = [n for n in selected if rx.search(n.interface or "")]

    return selected


def _assign_vlan_ids(neighbors: list[Neighbor], *, vlan_start: int) -> list[Neighbor]:
    return [
        Neighbor(
            site=n.site,
            source_router=n.source_router,
            identity=n.identity,
            ip=n.ip,
            mac=n.mac,
            interface=n.interface,
            vlan_id=vlan_start + idx,
        )
        for idx, n in enumerate(neighbors)
    ]


def _select_ettp_devices(inventory_path: str | Path, *, include_disabled: bool) -> list[Device]:
    devices = load_inventory_csv(inventory_path)
    return [
        d for d in devices
        if d.system.strip().upper() == "ETTP" and (include_disabled or d.enabled)
    ]


def _device_abbr(dev: Device) -> str:
    return str(getattr(dev, "abbr", "") or "").strip()


def _device_display(dev: Device) -> str:
    abbr = _device_abbr(dev)
    abbr_part = f" [{abbr}]" if abbr else ""
    return f"{dev.site}{abbr_part} / {dev.device} / {dev.mgmt_ip}"


def _matches_site_or_abbr(dev: Device, value: str, *, exact: bool) -> bool:
    wanted = value.strip().lower()
    site = dev.site.strip().lower()
    abbr = _device_abbr(dev).lower()
    if exact:
        return wanted == site or (abbr and wanted == abbr)
    return wanted in site or (abbr and wanted in abbr)


def _pick_device_interactive(devices: list[Device]) -> Device | None:
    if not devices:
        click.echo("No ETTP devices found in inventory.")
        return None

    ordered = sorted(devices, key=lambda d: (d.site.lower(), d.device.lower(), d.mgmt_ip))
    click.echo("\nSelect one ETTP site/device:\n")
    for idx, dev in enumerate(ordered, start=1):
        click.echo(f"  {idx:3d}. {_device_display(dev)}")

    choice = click.prompt("\nEnter number", type=int)
    if choice < 1 or choice > len(ordered):
        click.echo("Selection out of range. Exiting.")
        return None
    return ordered[choice - 1]


def _pick_device_by_site(devices: list[Device], site: str) -> Device:
    exact = [d for d in devices if _matches_site_or_abbr(d, site, exact=True)]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        matches = ", ".join(_device_display(d) for d in exact[:15])
        raise click.ClickException(
            f"Multiple ETTP inventory rows match site/abbr '{site}'. Use --device as well. Matches: {matches}"
        )

    partial = [d for d in devices if _matches_site_or_abbr(d, site, exact=False)]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        matches = ", ".join(_device_display(d) for d in partial[:15])
        raise click.ClickException(f"Site/abbr '{site}' is ambiguous. Matches: {matches}")

    raise click.ClickException(f"No enabled ETTP site or Abbr matched '{site}'.")


def _pick_device_by_site_and_device(devices: list[Device], site: str, device: str) -> Device:
    wanted_device = device.strip().lower()
    matches = [
        d for d in devices
        if _matches_site_or_abbr(d, site, exact=True) and d.device.strip().lower() == wanted_device
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise click.ClickException(f"No enabled ETTP row matched site/abbr '{site}' and device '{device}'.")
    raise click.ClickException(f"Multiple ETTP rows matched site '{site}' and device '{device}'.")


def _find_value(text: str, key: str) -> str | None:
    # Handles key=value and key="value with spaces" from RouterOS print detail/export text.
    m = re.search(rf'(?<![\w-]){re.escape(key)}=("[^"]*"|\S+)', text, flags=re.I)
    if not m:
        return None
    value = m.group(1).strip()
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        value = value[1:-1]
    return value


def _is_disabled_from_print(text: str) -> str:
    val = _find_value(text, "disabled")
    if val:
        return "yes" if val.lower() in {"yes", "true", "1", "on"} else "no"

    # RouterOS compact print often marks disabled rows with an X flag after the row number.
    if re.search(r"(?m)^\s*\d+\s+X\b", text):
        return "yes"
    return "no"


def _first_record_from_print(text: str, *, prefer_patterns: tuple[str, ...] = ()) -> str:
    records: list[str] = []
    cur: list[str] = []

    for line in text.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("flags:"):
            continue
        if re.match(r"^\d+\s", line):
            if cur:
                records.append(" ".join(cur))
            cur = [line]
        elif cur:
            cur.append(line)

    if cur:
        records.append(" ".join(cur))

    if not records:
        return text.strip()

    for pattern in prefer_patterns:
        rx = re.compile(pattern, flags=re.I)
        for rec in records:
            if rx.search(rec):
                return rec

    return records[0]


def _parse_wifi(text: str) -> tuple[str, str]:
    if not text.strip():
        return "No Data", "No Data"
    disabled = _is_disabled_from_print(text)
    ssid = _find_value(text, "ssid") or "No Data"
    return disabled, ssid


def _parse_password(text: str) -> str:
    for key in (
        "wpa2-pre-shared-key",
        "wpa-pre-shared-key",
        "passphrase",
        "pre-shared-key",
    ):
        val = _find_value(text, key)
        if val:
            return val
    return "No Data"


def _rate_to_mbps(rate: str) -> str:
    rate = (rate or "").strip()
    if not rate:
        return "No Data"

    # Common RouterOS forms: 50M, 1G, 512K, 10000000.
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([KMG]?)", rate, flags=re.I)
    if not m:
        return rate

    value = float(m.group(1))
    unit = (m.group(2) or "").upper()
    if unit == "G":
        value *= 1000
    elif unit == "K":
        value /= 1000
    elif unit in {"", "M"}:
        pass

    if value.is_integer():
        return str(int(value))
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _parse_queue(text: str) -> tuple[str, str]:
    if not text.strip():
        return "No Data", "No Data"

    rec = _first_record_from_print(
        text,
        prefer_patterns=(r'\bname="?Internet"?\b', r"\btarget=Bridge_Internet\b"),
    )
    disabled = _is_disabled_from_print(rec)
    max_limit = _find_value(rec, "max-limit")
    if not max_limit:
        max_limit = _find_value(rec, "queue")

    if not max_limit:
        return disabled, "No Data"

    download_side = max_limit.split("/", 1)[0]
    return disabled, _rate_to_mbps(download_side)


def _ssh_exec_one(host: str, port: int, user: str, pw: str, cmd: str, timeout: int) -> tuple[str, str, int]:
    client = make_ssh_client(host, port, user, pw, timeout=timeout)
    try:
        return ssh_exec(client, cmd, timeout=max(timeout, 30))
    finally:
        try:
            client.close()
        except Exception:
            pass


def _ssh_exec_many(host: str, port: int, user: str, pw: str, cmds: list[str], timeout: int) -> list[tuple[str, str, int]]:
    client = make_ssh_client(host, port, user, pw, timeout=timeout)
    results: list[tuple[str, str, int]] = []
    try:
        for cmd in cmds:
            results.append(ssh_exec(client, cmd, timeout=max(timeout, 30)))
        return results
    finally:
        try:
            client.close()
        except Exception:
            pass


def _role_for_row(row: dict[str, Any]) -> str:
    if _is_ap_row(row):
        return "wireless"
    if _is_queue_row(row):
        return "queue"
    if _has_queue_data(row) and _has_wireless_data(row):
        return "single"
    return "unclassified"


def _base_row(neighbor: Neighbor, *, include_vlan_id: bool) -> dict[str, Any]:
    row = {
        "Site": neighbor.site,
        "Source Router": neighbor.source_router,
        "Identity": neighbor.identity,
        "Device_Role": "",
        "Match_Key": _match_key_from_identity(neighbor.identity),
        "IP": neighbor.ip,
        "MAC": neighbor.mac,
        "Interface": neighbor.interface,
        "WIFI": "No Data",
        "SSID_2.4": "No Data",
        "SSID_5": "No Data",
        "PW": "No Data",
        "Queue": "No Data",
        "Speed(Mbps)": "No Data",
        "Status": "No Data",
        "Error": "",
    }
    if include_vlan_id:
        row["vlan-id"] = neighbor.vlan_id if neighbor.vlan_id is not None else ""
    return row


def _pull_hap_blocking(neighbor: Neighbor, *, user: str, pw: str, port: int, timeout: int, include_vlan_id: bool) -> dict[str, Any]:
    row = _base_row(neighbor, include_vlan_id=include_vlan_id)
    cmds = [
        '/interface wireless print detail where name="WIFI_5.0"',
        '/interface wireless print detail where name="WIFI_2.4"',
        '/interface wireless security-profiles print detail where name=default',
        '/queue simple print detail',
    ]

    try:
        (wifi5, _, _), (wifi24, _, _), (security, _, _), (queue, _, _) = _ssh_exec_many(
            neighbor.ip,
            port,
            user,
            pw,
            cmds,
            timeout,
        )

        wifi5_disabled, ssid5 = _parse_wifi(wifi5)
        _, ssid24 = _parse_wifi(wifi24)
        queue_disabled, speed = _parse_queue(queue)

        row.update(
            {
                "WIFI": wifi5_disabled,
                "SSID_2.4": ssid24,
                "SSID_5": ssid5,
                "PW": _parse_password(security),
                "Queue": queue_disabled,
                "Speed(Mbps)": speed,
                "Status": "OK",
                "Error": "",
            }
        )
        row["Device_Role"] = _role_for_row(row)
        return row
    except Exception as exc:
        row.update({"Device_Role": _role_for_row(row), "Status": "Could Not Connect", "Error": str(exc)})
        return row


async def _pull_all_haps(
    neighbors: list[Neighbor],
    *,
    user: str,
    pw: str,
    port: int,
    timeout: int,
    concurrency: int,
    show_progress: bool,
    include_vlan_id: bool,
) -> list[dict[str, Any]]:
    sem = asyncio.Semaphore(max(1, concurrency))

    async def worker(neighbor: Neighbor) -> dict[str, Any]:
        async with sem:
            return await asyncio.to_thread(
                _pull_hap_blocking,
                neighbor,
                user=user,
                pw=pw,
                port=port,
                timeout=timeout,
                include_vlan_id=include_vlan_id,
            )

    tasks = [asyncio.create_task(worker(neighbor)) for neighbor in neighbors]
    clean_rows: list[dict[str, Any]] = []
    with tqdm(
        total=len(tasks),
        desc="Processing devices",
        disable=not show_progress,
        dynamic_ncols=True,
        leave=True,
    ) as overall:
        for fut in asyncio.as_completed(tasks):
            try:
                item = await fut
            except Exception as exc:
                item = {col: "" for col in RAW_COLUMNS}
                if include_vlan_id:
                    item["vlan-id"] = ""
                item.update({"Status": "Error", "Error": str(exc)})
            clean_rows.append(item)
            overall.update(1)

    clean_rows.sort(key=lambda r: (str(r.get("Identity", "")).lower(), str(r.get("IP", ""))))
    return clean_rows


def _split_tokens(value: Any) -> list[str]:
    return [tok for tok in re.split(r"[^A-Za-z0-9]+", str(value or "")) if tok]


def _has_token(value: Any, token: str) -> bool:
    wanted = token.upper()
    return any(tok.upper() == wanted for tok in _split_tokens(value))


def _strip_ap_token(value: Any) -> str:
    tokens = [tok for tok in _split_tokens(value) if tok.upper() != "AP"]
    return "_".join(tokens)


def _match_key_from_identity(identity: Any) -> str:
    return _strip_ap_token(identity).upper()


def _match_key_from_interface(interface: Any) -> str:
    return _strip_ap_token(interface).upper()


def _is_ap_row(row: dict[str, Any]) -> bool:
    interface = row.get("Interface", "")
    return _has_token(interface, "AP") and _has_token(interface, "Modem")


def _is_queue_row(row: dict[str, Any]) -> bool:
    interface = row.get("Interface", "")
    return _has_token(interface, "Modem") and not _is_ap_row(row)


def _has_data(row: dict[str, Any], *fields: str) -> bool:
    for field in fields:
        value = str(row.get(field, "") or "").strip().lower()
        if value not in NO_DATA_VALUES:
            return True
    return False


def _has_queue_data(row: dict[str, Any]) -> bool:
    # Queue yes/no alone only tells us the disabled state. Treat the row as having
    # usable queue data only when a speed/max-limit was actually parsed.
    return _has_data(row, "Speed(Mbps)")


def _has_wireless_data(row: dict[str, Any]) -> bool:
    # WIFI yes/no alone only tells us the disabled state. SSID/password fields are
    # the reliable indicators that wireless data was actually returned.
    return _has_data(row, "SSID_2.4", "SSID_5", "PW")


def _status_for_data(row: dict[str, Any], data_kind: str) -> str:
    status = str(row.get("Status", "") or "")
    if status and status != "OK":
        return status
    if data_kind == "queue":
        return "OK" if _has_queue_data(row) else "No Data"
    if data_kind == "wireless":
        return "OK" if _has_wireless_data(row) else "No Data"
    return status or "No Data"


def _source_fields(row: dict[str, Any] | None, prefix: str) -> dict[str, Any]:
    if not row:
        return {
            f"{prefix}_Source_Identity": "",
            f"{prefix}_Source_IP": "",
            f"{prefix}_Source_MAC": "",
            f"{prefix}_Source_Interface": "",
        }
    return {
        f"{prefix}_Source_Identity": row.get("Identity", ""),
        f"{prefix}_Source_IP": row.get("IP", ""),
        f"{prefix}_Source_MAC": row.get("MAC", ""),
        f"{prefix}_Source_Interface": row.get("Interface", ""),
    }


def _combine_errors(*rows: dict[str, Any] | None) -> str:
    errors: list[str] = []
    for row in rows:
        if not row:
            continue
        err = str(row.get("Error", "") or "").strip()
        if err and err not in errors:
            errors.append(err)
    return " | ".join(errors)


def _customer_key_from_row(row: dict[str, Any]) -> str:
    key = _strip_ap_token(row.get("Identity", ""))
    return key or str(row.get("Identity", "") or row.get("IP", ""))


def _build_merged_row(
    *,
    customer_key: str,
    queue_row: dict[str, Any] | None,
    wireless_row: dict[str, Any] | None,
    merge_status: str,
) -> dict[str, Any]:
    base = queue_row or wireless_row or {}
    wireless_source = wireless_row if wireless_row and _has_wireless_data(wireless_row) else None
    if wireless_row and merge_status.startswith("paired"):
        # Even failed/no-data APs are useful to show as the intended wireless source.
        wireless_source = wireless_row

    row: dict[str, Any] = {
        "Site": base.get("Site", ""),
        "Source Router": base.get("Source Router", ""),
        "Customer_Key": customer_key,
        **_source_fields(queue_row, "Queue"),
        "Queue_Disabled": queue_row.get("Queue", "") if queue_row else "",
        "Speed(Mbps)": queue_row.get("Speed(Mbps)", "") if queue_row else "",
        "Queue_Status": _status_for_data(queue_row, "queue") if queue_row else "No Data",
        **_source_fields(wireless_source, "Wireless"),
        "Wireless_Disabled": wireless_row.get("WIFI", "") if wireless_row else "",
        "SSID_2.4": wireless_row.get("SSID_2.4", "") if wireless_row else "",
        "SSID_5": wireless_row.get("SSID_5", "") if wireless_row else "",
        "PW": wireless_row.get("PW", "") if wireless_row else "",
        "Wireless_Status": _status_for_data(wireless_row, "wireless") if wireless_row else "No Data",
        "Merge_Status": merge_status,
        "Error": _combine_errors(queue_row, wireless_row),
    }
    return row


def _last_numeric_token(value: Any) -> str:
    nums = [tok for tok in _split_tokens(value) if tok.isdigit()]
    return nums[-1] if nums else ""


def _pair_unique_remaining(
    *,
    queue_rows: list[dict[str, Any]],
    ap_rows: list[dict[str, Any]],
    used_queue_ids: set[int],
    used_ap_ids: set[int],
    key_func,
    merge_status: str,
) -> list[tuple[dict[str, Any], dict[str, Any], str]]:
    """Pair remaining queue/AP rows only when the key is unique on both sides.

    This prevents broad fallback keys, like 02_10Gig_Modem, from stealing APs
    that should later pair by identity with another queue row.
    """
    queue_by_key: dict[str, list[dict[str, Any]]] = {}
    ap_by_key: dict[str, list[dict[str, Any]]] = {}

    for queue_row in queue_rows:
        if id(queue_row) in used_queue_ids:
            continue
        key = key_func(queue_row)
        if not key:
            continue
        queue_by_key.setdefault(key, []).append(queue_row)

    for ap_row in ap_rows:
        if id(ap_row) in used_ap_ids:
            continue
        key = key_func(ap_row)
        if not key:
            continue
        ap_by_key.setdefault(key, []).append(ap_row)

    pairs: list[tuple[dict[str, Any], dict[str, Any], str]] = []

    for key in sorted(queue_by_key):
        queues = queue_by_key.get(key, [])
        aps = ap_by_key.get(key, [])

        if len(queues) != 1 or len(aps) != 1:
            continue

        queue_row = queues[0]
        ap_row = aps[0]

        used_queue_ids.add(id(queue_row))
        used_ap_ids.add(id(ap_row))
        pairs.append((queue_row, ap_row, merge_status))

    return pairs


def _merge_device_rows(raw_rows: list[dict[str, Any]], *, include_vlan_id: bool, vlan_start: int) -> list[dict[str, Any]]:
    ap_rows = [row for row in raw_rows if _is_ap_row(row)]
    queue_rows = [row for row in raw_rows if _is_queue_row(row)]
    classified_ids = {id(row) for row in ap_rows} | {id(row) for row in queue_rows}
    other_rows = [row for row in raw_rows if id(row) not in classified_ids]

    used_ap_ids: set[int] = set()
    used_queue_ids: set[int] = set()
    paired_rows: list[tuple[dict[str, Any], dict[str, Any], str]] = []

    # Phase 1: exact normalized identity pairing.
    # This must happen for all queue rows before any fallback pairing is attempted.
    ap_by_identity: dict[str, list[dict[str, Any]]] = {}
    for ap_row in sorted(ap_rows, key=lambda r: (_match_key_from_identity(r.get("Identity", "")), str(r.get("IP", "")))):
        key = _match_key_from_identity(ap_row.get("Identity", ""))
        if key:
            ap_by_identity.setdefault(key, []).append(ap_row)

    for queue_row in sorted(queue_rows, key=lambda r: (_match_key_from_identity(r.get("Identity", "")), str(r.get("IP", "")))):
        identity_key = _match_key_from_identity(queue_row.get("Identity", ""))
        candidates = [
            ap_row
            for ap_row in ap_by_identity.get(identity_key, [])
            if id(ap_row) not in used_ap_ids
        ]

        if not candidates:
            continue

        ap_row = candidates[0]
        used_queue_ids.add(id(queue_row))
        used_ap_ids.add(id(ap_row))
        paired_rows.append((queue_row, ap_row, "paired_identity"))

    # Phase 2: unique unit-number fallback.
    # Handles odd cases like TC_6A_646 <-> TC_646, but only when that suffix is
    # unique on both the remaining queue side and AP side.
    paired_rows.extend(
        _pair_unique_remaining(
            queue_rows=queue_rows,
            ap_rows=ap_rows,
            used_queue_ids=used_queue_ids,
            used_ap_ids=used_ap_ids,
            key_func=lambda row: _last_numeric_token(row.get("Identity", "")),
            merge_status="paired_unit_suffix",
        )
    )

    # Phase 3: unique interface fallback.
    # This is intentionally very conservative. It only pairs when exactly one
    # queue and exactly one AP remain for the normalized interface key.
    paired_rows.extend(
        _pair_unique_remaining(
            queue_rows=queue_rows,
            ap_rows=ap_rows,
            used_queue_ids=used_queue_ids,
            used_ap_ids=used_ap_ids,
            key_func=lambda row: _match_key_from_interface(row.get("Interface", "")),
            merge_status="paired_interface_unique",
        )
    )

    merged: list[dict[str, Any]] = []

    for queue_row, ap_row, merge_status in paired_rows:
        merged.append(
            _build_merged_row(
                customer_key=_customer_key_from_row(queue_row),
                queue_row=queue_row,
                wireless_row=ap_row,
                merge_status=merge_status,
            )
        )

    # Remaining queue rows.
    for queue_row in sorted(queue_rows, key=lambda r: (_match_key_from_identity(r.get("Identity", "")), str(r.get("IP", "")))):
        if id(queue_row) in used_queue_ids:
            continue

        if _has_wireless_data(queue_row):
            merge_status = "single_device"
            wireless_row = queue_row
        else:
            merge_status = "queue_only"
            wireless_row = None

        merged.append(
            _build_merged_row(
                customer_key=_customer_key_from_row(queue_row),
                queue_row=queue_row,
                wireless_row=wireless_row,
                merge_status=merge_status,
            )
        )

    # Remaining AP rows.
    for ap_row in sorted(ap_rows, key=lambda r: (_match_key_from_identity(r.get("Identity", "")), str(r.get("IP", "")))):
        if id(ap_row) in used_ap_ids:
            continue

        merged.append(
            _build_merged_row(
                customer_key=_customer_key_from_row(ap_row),
                queue_row=None,
                wireless_row=ap_row,
                merge_status="wireless_only",
            )
        )

    # Remaining unclassified rows.
    for row in sorted(other_rows, key=lambda r: (str(r.get("Identity", "")).lower(), str(r.get("IP", "")))):
        if _has_queue_data(row) and _has_wireless_data(row):
            merged.append(
                _build_merged_row(
                    customer_key=_customer_key_from_row(row),
                    queue_row=row,
                    wireless_row=row,
                    merge_status="single_device",
                )
            )
        elif _has_queue_data(row):
            merged.append(
                _build_merged_row(
                    customer_key=_customer_key_from_row(row),
                    queue_row=row,
                    wireless_row=None,
                    merge_status="queue_only_unclassified",
                )
            )
        elif _has_wireless_data(row):
            merged.append(
                _build_merged_row(
                    customer_key=_customer_key_from_row(row),
                    queue_row=None,
                    wireless_row=row,
                    merge_status="wireless_only_unclassified",
                )
            )
        else:
            merged.append(
                _build_merged_row(
                    customer_key=_customer_key_from_row(row),
                    queue_row=row,
                    wireless_row=None,
                    merge_status="unclassified_no_data",
                )
            )

    merged.sort(
        key=lambda r: (
            str(r.get("Customer_Key", "")).lower(),
            str(r.get("Queue_Source_IP", "")),
            str(r.get("Wireless_Source_IP", "")),
        )
    )

    if include_vlan_id:
        for idx, row in enumerate(merged):
            row["vlan-id"] = vlan_start + idx

    return merged


def _ordered_dataframe(rows: list[dict[str, Any]], columns: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in columns:
        if col not in df.columns:
            df[col] = ""
    return df[columns]


def _format_worksheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    wb = writer.book
    ws = writer.sheets[sheet_name]
    header_fmt = wb.add_format({"bold": True})

    for col_idx, col_name in enumerate(df.columns):
        ws.write(0, col_idx, col_name, header_fmt)
        try:
            width = max(len(str(col_name)), int(df[col_name].astype(str).map(len).max())) + 2
        except Exception:
            width = len(str(col_name)) + 2
        ws.set_column(col_idx, col_idx, min(max(width, 10), 42))

    if len(df.index) > 0:
        ws.autofilter(0, 0, len(df.index), len(df.columns) - 1)
    ws.freeze_panes(1, 0)


def _raw_csv_path(output_path: Path) -> Path:
    return output_path.with_name(f"{output_path.stem}_raw.csv")


def _write_output(
    merged_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    output_path: Path,
    output_format: str,
    *,
    raw_output: bool,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "csv":
        merged_df.to_csv(output_path, index=False)
        if raw_output:
            raw_df.to_csv(_raw_csv_path(output_path), index=False)
        return

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        merged_df.to_excel(writer, sheet_name="Wireless_Info", index=False)
        _format_worksheet(writer, "Wireless_Info", merged_df)
        if raw_output:
            raw_df.to_excel(writer, sheet_name="Raw_Devices", index=False)
            _format_worksheet(writer, "Raw_Devices", raw_df)


def _default_output_path(dev: Device, output_format: str) -> Path:
    name_part = _device_abbr(dev) or dev.site
    return generated_file_path(f"{date.today():%Y_%m_%d}_Wireless_Info_{_safe_filename_part(name_part)}.{output_format}")


@click.command("wireless-info")
@click.option("-I", "--inventory", "inventory_path", default="inventory.csv", show_default=True,
              help="Unified inventory CSV. Only System=ETTP rows are selectable.")
@click.option("-s", "--site", default=None,
              help="ETTP site name or optional inventory Abbr value to run. If omitted, an interactive selector is shown.")
@click.option("--device", default=None,
              help="Inventory Device value, only needed when a site has multiple ETTP rows.")
@click.option("--list-sites", is_flag=True,
              help="List enabled ETTP inventory rows and exit.")
@click.option("--include-disabled", is_flag=True,
              help="Allow disabled ETTP inventory rows to be listed/selected.")
@click.option("--format", "output_format", type=click.Choice(["csv", "xlsx"]), default="xlsx", show_default=True,
              help="Output file format.")
@click.option("-o", "--output", "output_path", default=None,
              help="Output file path. Extension is inferred from --format if omitted.")
@click.option("--raw-output/--no-raw-output", default=None,
              help="Also write raw per-device data. Default: enabled for xlsx, disabled for csv.")
@click.option("--neighbor-script", "neighbor_script_path", default=None,
              help="Optional RouterOS neighbor script. Defaults to getNeighbors2.rsc if present, then an embedded equivalent.")
@click.option("--identity-filter", default=None,
              help="Optional case-insensitive regex applied to neighbor identity after collection.")
@click.option("--interface-filter", default="Modem", show_default=True,
              help="Case-insensitive regex applied to neighbor interface after collection.")
@click.option("--all-neighbors", is_flag=True,
              help="Do not apply --interface-filter. Useful for troubleshooting only.")
@click.option("--include-vlan-id", is_flag=True,
              help="Add a legacy vlan-id column to the merged output. Disabled by default.")
@click.option("--vlan-start", type=int, default=3001, show_default=True,
              help="Starting vlan-id value when --include-vlan-id is used.")
@click.option("--device-user-env", default="USER1", show_default=True,
              help="Environment variable containing the per-neighbor MikroTik username.")
@click.option("--device-pw-env", default="PW3", show_default=True,
              help="Environment variable containing the per-neighbor MikroTik password.")
@click.option("--device-port", type=int, default=22, show_default=True,
              help="SSH port for per-neighbor MikroTik devices.")
@click.option("-c", "--concurrency", type=int, default=8, show_default=True,
              help="Max concurrent per-neighbor SSH pulls.")
@click.option("--timeout", type=int, default=10, show_default=True,
              help="SSH connect timeout in seconds.")
@click.option("--progress/--no-progress", default=True, show_default=True,
              help="Show one overall device-processing progress bar.")
@click.option("--quiet", is_flag=True,
              help="Silence console logs except Click output/progress.")
@click.option("--log-file", default=None)
@click.option("--log-level", default="INFO",
              type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]))
def wireless_info_cli(
    inventory_path: str,
    site: str | None,
    device: str | None,
    list_sites: bool,
    include_disabled: bool,
    output_format: str,
    output_path: str | None,
    raw_output: bool | None,
    neighbor_script_path: str | None,
    identity_filter: str | None,
    interface_filter: str | None,
    all_neighbors: bool,
    include_vlan_id: bool,
    vlan_start: int,
    device_user_env: str,
    device_pw_env: str,
    device_port: int,
    concurrency: int,
    timeout: int,
    progress: bool,
    quiet: bool,
    log_file: str | None,
    log_level: str,
) -> None:
    """
    Pull wireless/router data from MikroTik neighbor devices for one ETTP site.

    The command collects raw neighbor device data first, then builds merged
    customer rows. AP devices discovered on AP_Modem-style interfaces provide
    wireless fields, while modem/router devices provide queue/speed fields.
    hAP-style single devices can provide both sides of the row.
    """
    setup_logging(
        level=log_level,
        quiet=quiet,
        log_file=log_file,
        use_tqdm_handler=bool(progress) and not quiet,
    )
    log = get_logger()
    load_env()

    devices = _select_ettp_devices(inventory_path, include_disabled=include_disabled)
    if list_sites:
        for dev in sorted(devices, key=lambda d: (d.site.lower(), d.device.lower(), d.mgmt_ip)):
            status = "enabled" if dev.enabled else "disabled"
            click.echo(f"{_device_display(dev)} / {status}")
        return

    if site and device:
        selected = _pick_device_by_site_and_device(devices, site, device)
    elif site:
        selected = _pick_device_by_site(devices, site)
    else:
        selected = _pick_device_interactive(devices)
        if selected is None:
            return

    if selected.system.strip().upper() != "ETTP":
        raise click.ClickException("wireless-info only supports inventory rows with System=ETTP.")

    started = time.time()
    output = Path(output_path) if output_path else _default_output_path(selected, output_format)
    if output.suffix.lower() != f".{output_format}":
        output = output.with_suffix(f".{output_format}")
    if raw_output is None:
        raw_output = output_format == "xlsx"

    try:
        router_user, router_pw = resolve_env(selected.user_env, selected.pw_env)
        hap_user, hap_pw = resolve_env(device_user_env, device_pw_env)
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    script = _load_neighbor_script(neighbor_script_path)
    log.info(f"Collecting neighbors from {selected.site} ({selected.mgmt_ip})")
    try:
        raw_neighbors, err, rc = asyncio.run(asyncio.to_thread(
            _ssh_exec_one,
            selected.mgmt_ip,
            selected.port or 22,
            router_user,
            router_pw,
            script,
            timeout,
        ))
    except Exception as exc:
        raise click.ClickException(f"Failed to collect neighbors from {selected.site}: {exc}") from exc

    if err:
        log.debug(f"Neighbor script stderr from {selected.site}: {err}")
    if rc not in (0, None):
        log.debug(f"Neighbor script return code from {selected.site}: {rc}")

    discovered_neighbors = _parse_neighbor_output(
        raw_neighbors,
        site=selected.site,
        source_router=selected.mgmt_ip,
    )
    neighbors = _filter_neighbors(
        discovered_neighbors,
        identity_filter=identity_filter,
        interface_filter=interface_filter,
        all_neighbors=all_neighbors,
    )
    if include_vlan_id:
        neighbors = _assign_vlan_ids(neighbors, vlan_start=vlan_start)

    if not neighbors:
        raise click.ClickException(
            "No neighbors matched. Try --all-neighbors or adjust --interface-filter/--identity-filter."
        )

    click.echo(f"Selected site: {selected.site} ({selected.mgmt_ip})")
    click.echo(f"Discovered {len(discovered_neighbors)} neighbor device(s); processing {len(neighbors)} matched device(s).")
    log.info(f"Matched {len(neighbors)} neighbor(s); pulling device data")

    raw_rows = asyncio.run(_pull_all_haps(
        neighbors,
        user=hap_user,
        pw=hap_pw,
        port=device_port,
        timeout=timeout,
        concurrency=concurrency,
        show_progress=bool(progress) and not quiet,
        include_vlan_id=include_vlan_id,
    ))

    merged_rows = _merge_device_rows(raw_rows, include_vlan_id=include_vlan_id, vlan_start=vlan_start)

    raw_columns = list(RAW_COLUMNS)
    merged_columns = list(MERGED_COLUMNS)
    if include_vlan_id:
        raw_columns.insert(raw_columns.index("WIFI"), "vlan-id")
        merged_columns.insert(merged_columns.index("Queue_Source_Identity"), "vlan-id")

    raw_df = _ordered_dataframe(raw_rows, raw_columns)
    merged_df = _ordered_dataframe(merged_rows, merged_columns)

    _write_output(merged_df, raw_df, output, output_format, raw_output=bool(raw_output))
    ok = int((raw_df["Status"] == "OK").sum()) if "Status" in raw_df else 0
    failed = len(raw_df.index) - ok
    elapsed = time.time() - started

    paired = int(merged_df["Merge_Status"].astype(str).str.startswith("paired").sum()) if "Merge_Status" in merged_df else 0
    single = int((merged_df["Merge_Status"] == "single_device").sum()) if "Merge_Status" in merged_df else 0
    unpaired = len(merged_df.index) - paired - single

    log.info(
        f"Wrote {output} ({len(merged_df.index)} merged rows, {ok} devices OK, "
        f"{failed} failed, {elapsed:.1f}s)"
    )
    click.echo(
        f"Built {len(merged_df.index)} merged row(s): {paired} paired, {single} single-device, {unpaired} unpaired."
    )
    if output_format == "csv" and raw_output:
        click.echo(f"Wrote {output} and {_raw_csv_path(output)} ({ok} devices OK, {failed} failed)")
    elif output_format == "xlsx" and raw_output:
        click.echo(f"Wrote {output} with Raw_Devices sheet ({ok} devices OK, {failed} failed)")
    else:
        click.echo(f"Wrote {output} ({ok} devices OK, {failed} failed)")

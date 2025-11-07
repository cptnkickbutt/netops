# src/netops/systems/gpon.py

import os
import re
from typing import Optional, List, Tuple

from ..progress import SiteProgress
from ..transports.telnet_simple import TelnetRunner
from ..config import resolve_env

ID_RE = re.compile(r"^\d+(?:-\d+){3}$")
FSAN_RE = re.compile(r'([A-Z]{4}\s?[A-Za-z0-9]{6,12})')
HEADER_TOKENS = {"onu", "gpononu", "fixed", "traf"}

def _debug(msg: str) -> None:
    if os.getenv("NETOPS_DEBUG") == "1":
        try:
            from tqdm import tqdm
            tqdm.write(msg)
        except Exception:
            print(msg)

async def discover_gpon_ports(sess: TelnetRunner) -> List[Tuple[str, str]]:
    ports: List[Tuple[str, str]] = []
    slots_raw = await sess.exec("slots")
    slots = re.findall(r'^\s*(\d+):', slots_raw, re.MULTILINE)

    # ✅ loop ALL cards (1..16), not just /1
    for slot in slots:
        for card in range(1, 17):
            data = await sess.exec(f'port description list 1/{slot}/{card}')
            for line in data.strip().splitlines():
                m = re.match(r'^(\d+(?:-\d+){3})/\S+\s+(\S+)', line)
                if not m:
                    continue
                interface, desc = m.groups()
                if desc == "-":
                    continue
                # keep last 3 components and format x/y/z
                parts = interface.split('-')[-3:]
                formatted = '/'.join(parts)
                ports.append((formatted, desc))
    return ports

def parse_traf_prof_legacy_values(text: str) -> List[int]:
    vals: List[int] = []
    for raw in text.splitlines():
        parts = raw.strip().split()
        if not parts:
            continue
        head = parts[0].lower()
        if head.startswith('=') or head in HEADER_TOKENS or head.endswith('>'):
            continue
        if not any(ID_RE.match(p) for p in parts):
            continue
        prof_index = 3 if len(parts) == 13 else 2
        if len(parts) > prof_index and parts[prof_index].isdigit():
            vals.append(int(parts[prof_index]))
    return vals

def pick_speed_and_note(values: List[int]):
    valid = sorted({v for v in values if v > 1 and v != 512})
    if not valid:
        return None, ""
    speed_val = min(valid)
    note = ""
    if len(valid) > 1:
        if 1000 in valid and speed_val != 1000:
            note = "Camera profile present"
        else:
            note = "Multiple profiles: " + ", ".join(str(v) for v in valid)
    return f"{speed_val} Mbps", note

def extract_fsan(onu_show_text: str) -> str:
    m = FSAN_RE.search(onu_show_text or "")
    return m.group(1).strip() if m else ""

class GPONSystem:
    name = "GPON"

    def __init__(self, site, runner):
        self.site = site
        self.r = runner

    async def get_info(self, progress: Optional[SiteProgress] = None) -> List[List[str]]:
        final: List[List[str]] = [['Identity', 'Serial/Mac', 'Speed', 'Status', 'Notes']]

        user, pw = resolve_env(self.site.user_env, self.site.pw_env)

        async with TelnetRunner(self.site.ip, user, pw, (b"login: ", b"password: ")) as sess:
            # ✅ remove paging / widen output if supported
            try:
                await sess.exec("setline 0")
            except Exception:
                pass

            ports = await discover_gpon_ports(sess)
            if progress:
                progress.start(total=len(ports), desc=f"{self.site.property} (GPON)")

            for formatted, unit in ports:
                try:
                    onu_text = await sess.exec(f"onu show {formatted}")
                except Exception:
                    onu_text = ""
                fsan = extract_fsan(onu_text)

                try:
                    gem_text = await sess.exec(f"gpononu gemports 1/{formatted}/gpononu")
                except Exception:
                    gem_text = ""

                if os.getenv("NETOPS_DEBUG") == "1":
                    _debug(f"\n[GPON dump] {self.site.property} {formatted}\n{gem_text}\n")

                prof_vals = parse_traf_prof_legacy_values(gem_text)
                speed, note = pick_speed_and_note(prof_vals)
                status = "Active" if speed else "Inactive"

                final.append([unit, fsan, speed or 'INT Disabled', status, note])

                if progress:
                    progress.update(1)

        return final

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> List[List[str]]:
        return [['Result'], ['Not Implemented']]

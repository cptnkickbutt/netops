# netops/systems/gpon.py

import os
import re
from typing import Optional, List, Tuple

from netops.progress import SiteProgress
from netops.transports.telnet import TelnetRunner
from netops.config import resolve_env


# -------------------------
# Constants / Regex
# -------------------------

ID_RE = re.compile(r"^\d+(?:-\d+){3}$")          # e.g., 1-1-1-289, 1-2-1-278
FSAN_RE = re.compile(r'([A-Z]{4}\s?[A-Za-z0-9]{6,12})')
HEADER_TOKENS = {"onu", "gpononu", "fixed", "traf"}
SENTINEL_PROFILES = {"0", "1", "512"}


# -------------------------
# Debug helper
# -------------------------

def _debug(msg: str) -> None:
    """TQDM-safe debug printing when NETOPS_DEBUG=1."""
    if os.getenv("NETOPS_DEBUG") == "1":
        try:
            from tqdm import tqdm
            tqdm.write(msg)
        except Exception:
            print(msg)


# -------------------------
# Discovery helpers
# -------------------------

async def discover_gpon_ports(sess: TelnetRunner) -> List[Tuple[str, str]]:
    """
    Discover GPON ONU interfaces that have non '-' descriptions.

    Returns list of (formatted_iface, description), where formatted_iface is 'x/y/z'.
    """
    ports: List[Tuple[str, str]] = []

    # Discover slots
    slots_raw = await sess.run("slots")
    slots = re.findall(r'^\s*(\d+):', slots_raw, re.MULTILINE)

    # For each slot, scan ports 1..16 and collect ONUs with descriptions
    for slot in slots:
        for i in range(1, 17):
            data = await sess.run(f'port description list 1/{slot}/{i}')
            for line in data.strip().splitlines():
                # Example row: "1-9-4-XXX/<rest>    <desc>"
                m = re.match(r'^(\d+(?:-\d+){3})/\S+\s+(\S+)', line)
                if not m:
                    continue
                interface, desc = m.groups()
                if desc == "-":
                    continue
                # keep last 3 segments (x/y/z)
                parts = interface.split('-')[-3:]
                formatted = '/'.join(parts)
                ports.append((formatted, desc))

    return ports


# -------------------------
# Parser helpers
# -------------------------

def parse_traf_prof_legacy_values(text: str) -> List[int]:
    """
    Your original logic, returning *all* parsed 'traf prof' integers.

    - Split by whitespace
    - Skip headers/rulers/prompts
    - Require some ONU/GEM token to avoid junk rows
    - If both ONU and GEM on the same line (long rows), prof_index = 3
    - Else (continuation rows), prof_index = 2
    """
    vals: List[int] = []

    for raw in text.splitlines():
        parts = raw.strip().split()
        if not parts:
            continue

        head = parts[0].lower()

        # Skip rulers, headers, prompts
        if head.startswith('=') or head in HEADER_TOKENS or head.endswith('>'):
            continue

        # Require an ONU/GEM-like token (looks like 1-2-3-456) somewhere
        if not any(ID_RE.match(p) for p in parts):
            continue

        # Column heuristic from your working code:
        # many DZS rows are 13 tokens when ONU+GEM appear; continuation rows are shorter.
        prof_index = 3 if len(parts) == 13 else 2

        if len(parts) > prof_index and parts[prof_index].isdigit():
            vals.append(int(parts[prof_index]))

    _debug(f"[GPON legacy] collected profs: {vals}")
    return vals


def pick_speed_and_note(values: List[int]) -> Tuple[Optional[str], str]:
    """
    - Filter sentinels (0/1/512)
    - Choose the *lowest* remaining as speed
    - If multiple remain and 1000 present, note 'Camera profile present'
    - Else if multiple remain, note them (e.g., "Multiple profiles: 50, 1000")
    """
    valid = sorted({v for v in values if v > 1 and v != 512})
    if not valid:
        return None, ""
    speed_val = min(valid)  # lowest wins
    note = ""
    if len(valid) > 1:
        if 1000 in valid and speed_val != 1000:
            note = "Camera profile present"
        else:
            note = "Multiple profiles: " + ", ".join(str(v) for v in valid)
    return f"{speed_val} Mbps", note


def extract_fsan(onu_show_text: str) -> str:
    """Extract FSAN/serial token like 'ZNTS 03E3B53F' from 'onu show' output."""
    m = FSAN_RE.search(onu_show_text or "")
    return m.group(1).strip() if m else ""


# -------------------------
# GPON system
# -------------------------

class GPONSystem:
    name = "GPON"

    def __init__(self, site, runner):
        self.site = site
        self.r = runner

    async def get_info(self, progress: Optional[SiteProgress] = None) -> List[List[str]]:
        """
        For each populated GPON port (from 'port description list'):
          - Grab FSAN from 'onu show <iface>'
          - Parse *all* traf prof values from 'gpononu gemports 1/<iface>/gpononu'
          - Pick the LOWEST valid profile as Speed
          - Add a Notes column when multiple profiles exist (e.g., camera at 1000)
        """
        final: List[List[str]] = [['Identity', 'Serial/Mac', 'Speed', 'Status', 'Notes']]

        user = resolve_env(self.site.user_env)
        pw = resolve_env(self.site.pw_env)

        async with TelnetRunner(self.site.ip, user, pw, (b"login: ", b"password: ")) as sess:
            # Normalize session output (if supported on your platform)
            try:
                await sess.run("setline 0")
            except Exception:
                pass

            ports = await discover_gpon_ports(sess)
            if progress:
                progress.start(total=len(ports), desc=f"{self.site.property} (GPON)")

            for formatted, unit in ports:
                # FSAN
                try:
                    onu_text = await sess.run(f"onu show {formatted}")
                except Exception:
                    onu_text = ""
                fsan = extract_fsan(onu_text)

                # GEM profiles
                try:
                    gem_text = await sess.run(f"gpononu gemports 1/{formatted}/gpononu")
                except Exception:
                    gem_text = ""

                if os.getenv("NETOPS_DEBUG") == "1":
                    _debug(f"\n[GPON dump] {self.site.property} {formatted}\n{gem_text}\n")

                prof_vals = parse_traf_prof_legacy_values(gem_text)
                speed, note = pick_speed_and_note(prof_vals)
                status = "Active" if speed else "Inactive"

                if os.getenv("NETOPS_DEBUG") == "1":
                    _debug(f"[GPON parsed] {self.site.property} {formatted} -> speed={speed!r} note={note}")

                final.append([unit, fsan, speed or 'INT Disabled', status, note])

                if progress:
                    progress.update(1)

        return final

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> List[List[str]]:
        # Not implemented for GPON in this workflow
        return [['Result'], ['Not Implemented']]

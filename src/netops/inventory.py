# src/netops/inventory.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Iterable, Optional
import csv, unicodedata

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKC", s).replace("\uFEFF", "").strip()
    return s

def _norm_key(s: str) -> str:
    # ENV keys like USER1/PW1: strip whitespace/newlines and upper-case
    s = _norm(s)
    return "".join(ch for ch in s if ch not in " \t\r\n").upper()

@dataclass
class Device:
    site: str
    device: str
    mgmt_ip: str
    system: str
    roles: List[str]
    access: str
    port: int
    user_env: str
    pw_env: str
    enabled: bool
    notes: str = ""

    def has_role(self, role: str) -> bool:
        r = _norm(role).lower()
        return any(tok == r for tok in self.roles)

def load_inventory_csv(path: str | Path) -> List[Device]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Inventory not found: {p}")

    out: List[Device] = []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            if not row: continue
            site   = _norm(row.get("Site",""))
            device = _norm(row.get("Device","default")) or "default"
            ip     = _norm(row.get("MgmtIP",""))
            system = _norm(row.get("System","Other"))
            roles  = [tok.strip().lower() for tok in _norm(row.get("Roles","")).split(",") if tok.strip()]
            access = (_norm(row.get("Access","ssh")) or "ssh").lower()
            # default port by access
            dport  = "22" if access == "ssh" else "23"
            port   = int(_norm(row.get("Port", dport)) or dport)
            uenv   = _norm_key(row.get("UserEnv",""))
            penv   = _norm_key(row.get("PwEnv",""))
            enabled= (_norm(row.get("Enabled","yes")).lower() in {"yes","true","1"})
            notes  = _norm(row.get("Notes",""))
            out.append(Device(site, device, ip, system, roles, access, port, uenv, penv, enabled, notes))
    return out

def select(devs: Iterable[Device], *, systems: Optional[List[str]] = None,
           roles_any: Optional[List[str]] = None, enabled_only: bool = True) -> List[Device]:
    out: List[Device] = []
    systems_l = [s.lower() for s in systems] if systems else None
    roles_l   = [r.lower() for r in roles_any] if roles_any else None
    for d in devs:
        if enabled_only and not d.enabled: continue
        if systems_l and d.system.lower() not in systems_l: continue
        if roles_l   and not any(tok in d.roles for tok in roles_l): continue
        out.append(d)
    return out

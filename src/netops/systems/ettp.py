# src/netops/systems/ettp.py  (full drop-in)

import re, os, asyncio
import pandas as pd, numpy as np
from pathlib import Path
from typing import Optional

from ..parsers import parse_queue_export_verbose, rate_from_rule
from ..progress import SiteProgress
from ..transports.ssh import make_ssh_client, ssh_exec
from ..config import resolve_env

class ETTPSystem:
    name = "ETTP"

    def __init__(self, site, runner):
        self.site = site
        self.r = runner
        # Default modem creds pulled once; individual modem creds may be overridden if you add logic later
        self.cuser, self.cpass = resolve_env("USER1", "PW3")

    # ---------- sync helpers that we run in a worker thread ----------
    @staticmethod
    def _ssh_exec_one(host: str, user: str, pw: str, cmd: str):
        client = make_ssh_client(host, 22, user, pw)
        try:
            out, err, rc = ssh_exec(client, cmd)
            return out, err, rc
        finally:
            try:
                client.close()
            except Exception:
                pass

    @staticmethod
    def _ssh_exec_many(host: str, user: str, pw: str, cmds: list[str]) -> list[tuple[str,str,int]]:
        client = make_ssh_client(host, 22, user, pw)
        results = []
        try:
            for c in cmds:
                out, err, rc = ssh_exec(client, c)
                results.append((out, err, rc))
            return results
        finally:
            try:
                client.close()
            except Exception:
                pass

    # ---------- helpers ----------
    @staticmethod
    def _normalize_simple_export(text: str) -> str:
        """Join backslash-continued lines from '/queue simple export'."""
        return re.sub(r'\\\r?\n\s*', ' ', text.strip())

    @staticmethod
    def _internet_queue_disabled_from_simple(qtext_simple: str) -> bool:
        """
        Returns True if the Internet queue is disabled based on '/queue simple export'.
        We look for 'add ... disabled=yes ... name=Internet' OR matching target=Bridge_Internet.
        """
        t = ETTPSystem._normalize_simple_export(qtext_simple)
        add_lines = [m.group(0) for m in re.finditer(r'(?m)^\s*add\b[^\n]*', t)]
        # Prefer explicit name=Internet if multiple match
        candidates = [ln for ln in add_lines
                      if re.search(r'\bname="?Internet"?\b', ln) or re.search(r'\btarget=Bridge_Internet\b', ln)]
        if not candidates:
            return False
        candidates.sort(key=lambda s: 0 if re.search(r'\bname="?Internet"?\b', s) else 1)
        rule = candidates[0]
        m = re.search(r'\bdisabled=(yes|no)\b', rule)
        return bool(m and m.group(1) == 'yes')

    async def _get_neighbors_df(self) -> pd.DataFrame:
        # Prefer script if present
        script_path = Path("getNeighbors2.rsc")
        script = script_path.read_text(encoding="utf-8") if script_path.exists() else "/system script print"

        # Site/router creds
        ruser, rpass = resolve_env(self.site.user_env, self.site.pw_env)

        # Run over SSH in a worker thread
        out, _, _ = await asyncio.to_thread(self._ssh_exec_one, self.site.ip, ruser, rpass, script)

        rows = out.split(';')
        table = [row.split(',') for row in rows]
        final_list = [[item.strip() for item in row] for row in table]
        modem_list = [sub for sub in final_list if sub != ['']]
        raw_df = pd.DataFrame(modem_list, columns=["Identity", "Interface", "IP", "Mac"])

        df = raw_df[~raw_df['Interface'].str.contains('AP_', na=False)]
        modem_df = df[df['Interface'].str.contains('_Modem', na=False)][['Identity', 'Mac', 'IP']].rename(columns={'IP':'Modem IP'})
        int_df   = df[df['Interface'].str.contains('_INT',   na=False)][['Identity', 'Mac', 'IP']].rename(columns={'IP':'Internet IP'})
        pub_df   = df[df['Interface'].str.contains('_Public',na=False)][['Identity', 'Mac', 'IP']].rename(columns={'IP':'Public IP'})

        merged = modem_df.merge(int_df, on=['Identity','Mac'], how='outer').merge(pub_df, on=['Identity','Mac'], how='outer')
        return merged.drop_duplicates(subset=['Identity','Mac']).replace('', np.nan)

    async def get_info(self, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        results = [['Identity', 'Mac/Serial', 'Speed', 'Status']]

        modems = await self._get_neighbors_df()
        total = len(modems.index)
        if progress:
            progress.start(total=total, desc=f"{self.site.property} (ETTP)")

        for _, row in modems.iterrows():
            identity = row.get('Identity'); mac = row.get('Mac')
            modem_ip = row.get('Modem IP'); int_ip = row.get('Internet IP'); public_ip = row.get('Public IP')

            if pd.isna(modem_ip):
                results.append([identity, mac, 'No Data', 'No Modem IP'])
                if progress: progress.update(1)
                continue

            if pd.isna(int_ip) and pd.isna(public_ip):
                results.append([identity, mac, 'No Data', 'Inactive'])
                if progress: progress.update(1)
                continue

            try:
                muser, mpass = self.cuser, self.cpass
                # NEW: grab simple export (for disabled check), verbose (for rate parsing), and ethernet
                [(qsimple, _, _), (qverb, _, _), (itext, _, _)] = await asyncio.to_thread(
                    self._ssh_exec_many,
                    str(modem_ip), muser, mpass,
                    ['/queue simple export', '/queue simple export verbose', '/interface ethernet export']
                )

                # If Internet queue disabled in simple export => force 1000 Mbps
                if self._internet_queue_disabled_from_simple(qsimple):
                    queue_rate = "1000 Mbps"
                else:
                    # Original behavior: parse verbose and pick first rate
                    rules = parse_queue_export_verbose(qverb)
                    queue_rate = next((rt for rt in (rate_from_rule(r) for r in rules) if rt), "1000 Mbps")

                # Prefer ethernet-reported link speed, mark with '*'
                hw = re.findall(r'\bspeed=(\d+Mbps)\b', itext)
                if hw:
                    queue_rate = f"{hw[0]}*"

                results.append([identity, mac, queue_rate, 'Active'])

            except Exception:
                results.append([identity, mac, 'No Data', 'Could Not Connect'])
            finally:
                if progress:
                    progress.update(1)

        return results

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        return [['Result'], ['Not Implemented']]

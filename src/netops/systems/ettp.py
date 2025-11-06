
import re
from pathlib import Path
import pandas as pd
import numpy as np
import os
from typing import Optional
from netops.parsers import parse_queue_export_verbose, rate_from_rule
from netops.progress import SiteProgress
from netops.transports.ssh import SSHRunner
from netops.config import resolve_env

class ETTPSystem:
    name = "ETTP"
    def __init__(self, site, runner):
        self.site = site
        self.r = runner
        self.cuser = resolve_env("USER1") if "USER1" in dict(os.environ) else None  # optional
        self.cpass = resolve_env("PW3") if "PW3" in dict(os.environ) else None

    async def _get_neighbors_df(self) -> pd.DataFrame:
        script_path = Path("getNeighbors2.rsc")
        script = script_path.read_text(encoding="utf-8") if script_path.exists() else "/system script print"
        out = await self.r.run(script)
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
        merged = merged.drop_duplicates(subset=['Identity','Mac']).replace('', np.nan)
        return merged

    async def get_info(self, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        results = [['Identity', 'Mac/Serial', 'Speed', 'Status']]
        modems = await self._get_neighbors_df()
        total = len(modems.index)
        if progress: progress.start(total=total, desc=f"{self.site.property} (ETTP)")
        for _, row in modems.iterrows():
            identity = row.get('Identity'); mac = row.get('Mac')
            modem_ip = row.get('Modem IP'); int_ip = row.get('Internet IP'); public_ip = row.get('Public IP')
            if pd.isna(modem_ip):
                results.append([identity, mac, 'No Data', 'No Modem IP'])
                if progress: progress.update(1); continue
            if pd.isna(int_ip) and pd.isna(public_ip):
                results.append([identity, mac, 'No Data', 'Inactive'])
                if progress: progress.update(1); continue
            try:
                # Per-modem runner (SSH)
                muser = self.cuser or resolve_env("USER1")
                mpass = self.cpass or resolve_env("PW3")
                modem_runner = SSHRunner(str(modem_ip), muser, mpass)
                qtext = await modem_runner.run('/queue simple export verbose')
                rules = parse_queue_export_verbose(qtext)
                queue_rate = "1000 Mbps"
                for r in rules:
                    rate = rate_from_rule(r)
                    if rate:
                        queue_rate = rate; break
                itext = await modem_runner.run('/interface ethernet export')
                hw = re.findall(r'\bspeed=(\d+Mbps)\b', itext)
                if hw:
                    queue_rate = f"{hw[0]}*"
                await modem_runner.close()
                results.append([identity, mac, queue_rate, 'Active'])
            except Exception:
                results.append([identity, mac, 'No Data', 'Could Not Connect'])
            finally:
                if progress: progress.update(1)
        return results

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        # Example stub for future bulk-set jobs
        targets = payload.get("targets", [])
        if progress: progress.start(total=len(targets), desc=f"{self.site.property} (Set)")
        out = [['Target','Result']]
        for t in targets:
            try:
                await self.r.run(f"/queue simple set [find name={t!r}] max-limit={payload.get('rate','50M/50M')}")
                out.append([t, 'ok'])
            except Exception as e:
                out.append([t, f'error: {e}'])
            finally:
                if progress: progress.update(1)
        return out

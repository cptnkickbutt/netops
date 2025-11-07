import re, asyncio
from typing import Optional

from ..progress import SiteProgress
from ..transports.telnet_simple import TelnetRunner
from ..config import resolve_env


class CMTSystem:
    name = "CMTS"

    def __init__(self, site, runner):
        self.site = site
        self.r = runner

    async def _get_list(self, sess: TelnetRunner):
        # ensure no paging
        try:
            await sess.exec("terminal length 0")
        except Exception:
            pass

        data = await sess.exec('show running-config verbose | include description')

        rows = data.splitlines()
        clean = []
        for row in rows:
            m = re.search(r'([A-Fa-f0-9]{4}\.[A-Fa-f0-9]{4}\.[A-Fa-f0-9]{4})', row)
            if not m:
                continue
            mac = m.group(1)
            parts = row.split('"')
            desc = parts[1].strip() if len(parts) > 1 else ""
            if mac:
                clean.append([mac, desc])
        return sorted(clean, key=lambda x: x[1])

    async def get_info(self, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        final_list = [['Identity', 'Mac/Serial', 'Speed', 'Status']]

        user, pw = resolve_env(self.site.user_env, self.site.pw_env)

        # CMTS typically needs enable to land on '# ' prompt
        async with TelnetRunner(
            self.site.ip, user, pw,
            (b"Username: ", b"Password: "),
            enable=("en\n", b"# ")
        ) as sess:

            modems = await self._get_list(sess)
            if progress:
                progress.start(total=len(modems), desc=f"{self.site.property} (CMTS)")

            for mac, identity in modems:
                try:
                    out = await sess.exec(f"show cable modem {mac} verbose | include DHCPv4")
                    m = re.search(r'(\d+Mbps)', out)
                    speed = m.group(1) if m else ''
                    status = 'Active' if speed else 'Inactive'
                    final_list.append([identity, mac, speed, status])
                except Exception:
                    final_list.append([identity, 'No Data', 'No Data', 'No Data'])
                finally:
                    if progress:
                        progress.update(1)

        return final_list

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        return [['Result'], ['Not Implemented']]

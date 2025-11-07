import re, math, asyncio
from typing import Optional

from ..progress import SiteProgress
from ..transports.telnet_simple import TelnetRunner
from ..config import resolve_env


class DSLSystem:
    name = "DSL"

    def __init__(self, site, runner):
        self.site = site
        self.r = runner

    async def get_info(self, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        final_list = [['Identity', 'Serial/Mac', 'Speed', 'Status']]

        user, pw = resolve_env(self.site.user_env, self.site.pw_env)

        # Login prompts on these boxes are typically lowercase “login:” / “password:”
        async with TelnetRunner(self.site.ip, user, pw, (b"login: ", b"password: ")) as sess:
            # Some firmware supports this to remove paging; tolerate failure silently.
            try:
                await sess.exec("setline 0")
            except Exception:
                pass

            slots_raw = await sess.exec("slots")
            slots = re.findall(r'^\s*(\d+):', slots_raw, re.MULTILINE)

            total_ports = 24 * len(slots)
            if progress:
                progress.start(total=total_ports, desc=f"{self.site.property} (DSL)")

            for slot in slots:
                for port in range(1, 25):
                    try:
                        stats_out = await sess.exec(f'dslstat 1-{slot}-{port}-0/vdsl -v')
                        admin = 'Inactive'
                        rate = ''
                        sn = ''

                        for ln in stats_out.splitlines():
                            s = ln.strip()
                            if s.startswith('AdminStatus'):
                                admin = 'Active' if s.split('.')[-1].strip().upper() == 'UP' else 'Inactive'
                            if s.startswith('DslDownLineRate'):
                                try:
                                    rate = f"{math.ceil(int(s.split('.')[-1].strip()) / 1_000_000)} Mbps"
                                except Exception:
                                    pass
                            if s.startswith('serialNumber'):
                                sn = s.split('.')[-1].strip()

                        desc_raw = await sess.exec(f'port show 1/{slot}/{port}/0/vdsl')
                        m = re.search(r'Description:\s+(.*)', desc_raw)
                        desc = m.group(1).strip() if m else None

                        final_list.append([desc, sn, rate, admin])

                    except Exception:
                        final_list.append([f"1/{slot}/{port}", "", "", "Error"])

                    finally:
                        if progress:
                            progress.update(1)

        return final_list

    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> list[list[str]]:
        return [['Result'], ['Not Implemented']]

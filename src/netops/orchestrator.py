
import asyncio, pandas as pd
from typing import Iterable, Optional, Callable
from netops.progress import SiteProgressManager
from netops.logging import get_logger

log = get_logger()

async def run_many(sites: Iterable, factory: Callable, *, concurrency=6, show_progress=True,
                   mode="get_info", payload: Optional[dict]=None):
    sem = asyncio.Semaphore(max(1, concurrency))
    mgr = SiteProgressManager(pool_size=concurrency, enabled=show_progress)
    results = []

    async def one(site):
        async with sem:
            sp = await mgr.acquire()
            try:
                system = factory(site)
                if mode == "get_info":
                    table = await system.get_info(progress=sp)
                else:
                    table = await system.set_config(payload or {}, progress=sp)
                df = pd.DataFrame(table[1:], columns=table[0])
                sp.done("Done")
            except Exception as e:
                log.error(f"{site.property} ({site.system}) failed: {e}")
                df = pd.DataFrame([["Property","System","Status"],
                                   [site.property, site.system, f"Error: {e}"]], columns=["Property","System","Status"])
                sp.done("Error")
            finally:
                await mgr.release(sp)
                try: await system.r.close()
                except Exception: pass
            return (site.property, site.system, df)

    coros = [one(s) for s in sites]
    with mgr.overall(total=len(coros)) as overall:
        for fut in asyncio.as_completed(coros):
            res = await fut
            results.append(res)
            overall.update(1)
    return results

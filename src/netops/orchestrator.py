
import asyncio, pandas as pd
from typing import Iterable, Optional, Callable, List, Any
from netops.progress import SiteProgressManager
from netops.logging import get_logger

log = get_logger()
_log = get_logger()

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

async def run_many_simple(
    items: Iterable[Any],
    worker: Callable[[Any, Optional[Any]], "asyncio.Future[Any]"] | Callable[[Any], "asyncio.Future[Any]"],
    *,
    concurrency: int = 6,
    show_progress: bool = True,
) -> List[Any]:
    """
    Run an async worker(item, progress?) over a list of items with a concurrency cap,
    showing the same SiteProgressManager UI used by speed-audit.
    - The worker may accept a second 'progress' parameter; if not, it's called with only 'item'.
    - Returns a list of results in completion order (you can re-sort by a key later).
    """
    sem = asyncio.Semaphore(max(1, concurrency))
    mgr = SiteProgressManager(pool_size=concurrency, enabled=show_progress)
    results: List[Any] = []

    async def one(item):
        async with sem:
            sp = await mgr.acquire()
            try:
                # Try calling worker(item, progress=sp); fall back to worker(item)
                try:
                    res = await worker(item, sp)  # type: ignore[misc]
                except TypeError:
                    res = await worker(item)      # type: ignore[misc]
                sp.done("Done")
                return res
            except Exception as e:
                _log.error(f"Worker failed: {e}")
                sp.done("Error")
                return e
            finally:
                await mgr.release(sp)

    coros = [one(x) for x in items]
    with mgr.overall(total=len(coros)) as overall:
        for fut in asyncio.as_completed(coros):
            res = await fut
            results.append(res)
            overall.update(1)
    return results
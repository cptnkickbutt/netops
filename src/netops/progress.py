
from dataclasses import dataclass
from typing import Optional
from tqdm import tqdm
import asyncio

@dataclass
class SiteProgress:
    bar: Optional[tqdm]
    index: int
    busy: bool = False

    def start(self, total: int, desc: str):
        if self.bar is None: return
        self.busy = True
        self.bar.reset(total=total)
        self.bar.set_description(desc, refresh=True)
        self.bar.n = 0
        self.bar.refresh()

    def update(self, n: int = 1):
        if self.bar is None: return
        self.bar.update(n)

    def done(self, tail_text: str = "Done"):
        if self.bar is None: return
        if self.bar.total and self.bar.n < self.bar.total:
            self.bar.n = self.bar.total
            self.bar.refresh()
        self.bar.set_postfix_str(tail_text, refresh=True)
        self.busy = False

class SiteProgressManager:
    def __init__(self, pool_size: int, enabled: bool):
        self.enabled = enabled
        self.pool_size = max(1, pool_size)
        self._lock = asyncio.Lock()
        self._pool: list[SiteProgress] = []
        if self.enabled:
            for pos in range(self.pool_size):
                bar = tqdm(total=1, position=pos, leave=True, disable=False, dynamic_ncols=True)
                bar.set_description("Idle", refresh=False)
                bar.n = 1; bar.refresh()
                self._pool.append(SiteProgress(bar=bar, index=pos, busy=False))

    async def acquire(self) -> SiteProgress:
        if not self.enabled:
            return SiteProgress(bar=None, index=-1, busy=True)
        while True:
            async with self._lock:
                for sp in self._pool:
                    if not sp.busy:
                        sp.busy = True
                        sp.bar.reset(total=0)
                        sp.bar.n = 0
                        sp.bar.set_postfix_str("", refresh=False)
                        sp.bar.set_description("Starting...", refresh=True)
                        return sp
            await asyncio.sleep(0.02)

    async def release(self, sp: SiteProgress):
        if not self.enabled or sp.bar is None: return
        async with self._lock:
            sp.bar.reset(total=1); sp.bar.n = 1
            sp.bar.set_description("Idle", refresh=True)
            sp.bar.set_postfix_str("", refresh=False)
            sp.busy = False

    def overall(self, total: int):
        if not self.enabled:
            class _Dummy:
                def __enter__(self): return self
                def __exit__(self, *a): return False
                def update(self, *a, **k): pass
            return _Dummy()
        return tqdm(total=total, desc="Processing properties",
                    position=self.pool_size, leave=True, disable=False, dynamic_ncols=True)

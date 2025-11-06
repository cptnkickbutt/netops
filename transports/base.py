
from typing import Protocol
import asyncio, functools, random

class Runner(Protocol):
    async def run(self, command: str) -> str: ...
    async def close(self) -> None: ...

def retry_async(times=3, base=0.5, jitter=0.3, exceptions=(Exception,)):
    def deco(fn):
        @functools.wraps(fn)
        async def wrapper(*a, **k):
            delay = base
            for attempt in range(1, times + 1):
                try:
                    return await fn(*a, **k)
                except exceptions:
                    if attempt == times:
                        raise
                    await asyncio.sleep(delay + random.uniform(0, jitter))
                    delay *= 2
        return wrapper
    return deco

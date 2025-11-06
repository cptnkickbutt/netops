
from typing import Protocol, Optional
from netops.progress import SiteProgress

class System(Protocol):
    name: str
    async def get_info(self, progress: Optional[SiteProgress] = None) -> list[list[str]]: ...
    async def set_config(self, payload: dict, progress: Optional[SiteProgress] = None) -> list[list[str]]: ...

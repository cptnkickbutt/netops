from __future__ import annotations
from typing import Protocol, Tuple, runtime_checkable

__all__ = ["ExecTransport"]

@runtime_checkable
class ExecTransport(Protocol):
    """
    Minimal interface for anything that can run a command and close.
    Works for SSH and Telnet. No SFTP implied here.
    """
    def exec(self, cmd: str, timeout: int = 60) -> Tuple[str, str, int]: ...
    def close(self) -> None: ...

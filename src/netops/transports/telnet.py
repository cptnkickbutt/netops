from __future__ import annotations
from typing import Tuple, Union, Optional
import asyncio

__all__ = ["make_telnet_client", "telnet_exec"]

try:
    import telnetlib3
except ImportError as e:
    raise RuntimeError(
        "telnetlib3 is required for Telnet support on Python 3.13+. "
        "Install it with: pip install telnetlib3"
    ) from e


class _SyncTelnetClient:
    """
    Sync-looking telnet client backed by telnetlib3 (async).
    Creates its own event loop so callers can use it like a blocking client.
    """
    def __init__(
        self,
        host: str,
        port: int = 23,
        timeout: int = 10,
        *,
        encoding: str = "utf-8",
        prompt: Optional[Union[str, bytes]] = "\n",
        username: str = "",
        password: str = "",
        username_prompt: Union[str, bytes] = "login:",
        password_prompt: Union[str, bytes] = "Password:",
        auto_login: bool = False,
        strip_echo: bool = True,
    ):
        self._host = host
        self._port = port
        self._connect_timeout = float(timeout)
        self._encoding = encoding
        self._prompt = prompt.decode(encoding, "ignore") if isinstance(prompt, bytes) else prompt
        self._user = username
        self._pw = password
        self._user_prompt = username_prompt.decode(encoding, "ignore") if isinstance(username_prompt, bytes) else username_prompt
        self._pw_prompt = password_prompt.decode(encoding, "ignore") if isinstance(password_prompt, bytes) else password_prompt
        self._auto_login = auto_login
        self._strip_echo = strip_echo

        self._loop = asyncio.new_event_loop()
        self._reader = None
        self._writer = None
        self._loop.run_until_complete(self._open())

    async def _open(self):
        self._reader, self._writer = await asyncio.wait_for(
            telnetlib3.open_connection(
                host=self._host,
                port=self._port,
                encoding=self._encoding,
            ),
            timeout=self._connect_timeout,
        )
        if self._auto_login and (self._user or self._pw):
            # Username
            if self._user:
                await asyncio.wait_for(self._reader.readuntil(self._user_prompt), timeout=self._connect_timeout)
                self._writer.write(self._user + "\n")
                await self._writer.drain()
            # Password
            if self._pw:
                await asyncio.wait_for(self._reader.readuntil(self._pw_prompt), timeout=self._connect_timeout)
                self._writer.write(self._pw + "\n")
                await self._writer.drain()
        # Try to settle on a prompt if provided
        if self._prompt:
            try:
                await asyncio.wait_for(self._reader.readuntil(self._prompt), timeout=self._connect_timeout)
            except asyncio.TimeoutError:
                pass  # Not fatal if device doesn't echo a prompt immediately

    async def _exec_async(self, cmd: str, timeout: int) -> Tuple[str, str, int]:
        self._writer.write(cmd + "\n")
        await self._writer.drain()
        if self._prompt:
            raw = await asyncio.wait_for(self._reader.readuntil(self._prompt), timeout=timeout)
        else:
            raw = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
        out = raw
        if self._strip_echo:
            lines = out.splitlines(True)
            if lines and lines[0].strip() == cmd.strip():
                out = "".join(lines[1:])
        return out, "", 0  # telnet has no real stderr/exit code

    def exec(self, cmd: str, timeout: int = 60) -> Tuple[str, str, int]:
        return self._loop.run_until_complete(self._exec_async(cmd, timeout))

    async def _close_async(self):
        try:
            if self._writer is not None:
                self._writer.close()
                await self._writer.wait_closed()
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._loop.run_until_complete(self._close_async())
        finally:
            self._loop.close()


def make_telnet_client(host: str, port: int = 23, timeout: int = 10, **kwargs) -> _SyncTelnetClient:
    """
    Sync factory, kwargs forwarded to _SyncTelnetClient:
      encoding, prompt, username, password, username_prompt, password_prompt,
      auto_login, strip_echo
    """
    return _SyncTelnetClient(host, port, timeout, **kwargs)


def telnet_exec(client: _SyncTelnetClient, cmd: str, timeout: int = 60):
    return client.exec(cmd, timeout=timeout)

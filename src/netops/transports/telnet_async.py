from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Optional, Tuple, Union, Iterable

__all__ = [
    "AsyncTelnetClient",
    "make_telnet_client_async",
    "telnet_exec_async",
]

try:
    import telnetlib3
except ImportError as e:
    raise RuntimeError("telnetlib3 is required for async telnet. pip install telnetlib3") from e


@dataclass
class TelnetLogin:
    """Optional login flow; extend prompts per device family as needed."""
    username: str = ""
    password: str = ""
    username_prompt: Union[str, bytes] = "login:"
    password_prompt: Union[str, bytes] = "Password:"


class AsyncTelnetClient:
    """
    Async telnet client with simple login, prompt handling, and timeouts.

    Typical use:
        async with AsyncTelnetClient(host, prompt="> ") as cli:
            out, err, rc = await cli.exec("/interface print")

    Notes:
    - Telnet has no real "stderr" or exit codes -> returns ("", 0) for those.
    - Prompt matching uses telnetlib3's .readuntil(text_or_bytes).
    - If the device echoes commands, we trim one leading line if it equals the cmd.
    """
    def __init__(
        self,
        host: str,
        port: int = 23,
        *,
        connect_timeout: float = 10.0,
        encoding: str = "utf-8",
        prompt: Optional[Union[str, bytes]] = "\n",
        login: Optional[TelnetLogin] = None,
        read_chunk_timeout: float = 30.0,
        strip_echo: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.connect_timeout = float(connect_timeout)
        self.encoding = encoding
        # normalize prompt to str for telnetlib3
        if isinstance(prompt, bytes):
            prompt = prompt.decode(encoding, "ignore")
        self.prompt = prompt
        self.login = login
        self.read_chunk_timeout = float(read_chunk_timeout)
        self.strip_echo = strip_echo

        self._reader = None
        self._writer = None
        self._opened = False

    # ---------- context manager ----------
    async def __aenter__(self) -> "AsyncTelnetClient":
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ---------- lifecycle ----------
    async def open(self) -> None:
        if self._opened:
            return
        self._reader, self._writer = await asyncio.wait_for(
            telnetlib3.open_connection(
                host=self.host,
                port=self.port,
                encoding=self.encoding,
            ),
            timeout=self.connect_timeout,
        )
        self._opened = True

        if self.login and (self.login.username or self.login.password):
            await self._do_login(self.login)

        # settle to prompt if provided
        if self.prompt:
            try:
                await asyncio.wait_for(self._reader.readuntil(self.prompt), timeout=self.read_chunk_timeout)
            except asyncio.TimeoutError:
                # not fatal for devices that don't print a prompt immediately
                pass

    async def close(self) -> None:
        if not self._opened:
            return
        try:
            if self._writer is not None:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass
        finally:
            self._reader = None
            self._writer = None
            self._opened = False

    # ---------- commands ----------
    async def exec(self, cmd: str, timeout: float = 60.0) -> Tuple[str, str, int]:
        """Send a command and read until 'prompt' (if set) or a single line fallback."""
        if not self._opened:
            await self.open()

        # write command
        self._writer.write(cmd + "\n")
        await self._writer.drain()

        # read reply
        if self.prompt:
            raw = await asyncio.wait_for(self._reader.readuntil(self.prompt), timeout=timeout)
        else:
            # read one line at minimum
            raw = await asyncio.wait_for(self._reader.readline(), timeout=timeout)

        out = raw

        # trim echoed command (common on telnet shells)
        if self.strip_echo:
            lines = out.splitlines(True)  # keepends
            if lines and lines[0].strip() == cmd.strip():
                out = "".join(lines[1:])

        # telnet doesn't expose stderr/rc
        return out, "", 0

    # ---------- helpers ----------
    async def _do_login(self, creds: TelnetLogin) -> None:
        # username prompt
        if creds.username_prompt:
            await asyncio.wait_for(self._reader.readuntil(_to_str(creds.username_prompt, self.encoding)),
                                   timeout=self.read_chunk_timeout)
        if creds.username:
            self._writer.write(creds.username + "\n")
            await self._writer.drain()

        # password prompt
        if creds.password_prompt:
            await asyncio.wait_for(self._reader.readuntil(_to_str(creds.password_prompt, self.encoding)),
                                   timeout=self.read_chunk_timeout)
        if creds.password:
            self._writer.write(creds.password + "\n")
            await self._writer.drain()


def _to_str(s: Union[str, bytes], enc: str) -> str:
    return s if isinstance(s, str) else s.decode(enc, "ignore")


# --------- convenience API (mirrors your sync naming, but async) ---------
async def make_telnet_client_async(
    host: str,
    port: int = 23,
    *,
    connect_timeout: float = 10.0,
    encoding: str = "utf-8",
    prompt: Optional[Union[str, bytes]] = "\n",
    login: Optional[TelnetLogin] = None,
    read_chunk_timeout: float = 30.0,
    strip_echo: bool = True,
) -> AsyncTelnetClient:
    cli = AsyncTelnetClient(
        host, port,
        connect_timeout=connect_timeout,
        encoding=encoding,
        prompt=prompt,
        login=login,
        read_chunk_timeout=read_chunk_timeout,
        strip_echo=strip_echo,
    )
    await cli.open()
    return cli


async def telnet_exec_async(client: AsyncTelnetClient, cmd: str, timeout: float = 60.0):
    return await client.exec(cmd, timeout=timeout)

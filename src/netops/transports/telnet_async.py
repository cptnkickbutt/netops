# src/netops/transports/telnet_async.py
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from typing import Optional, Tuple, Union

__all__ = ["AsyncTelnetClient", "TelnetLogin"]

try:
    import telnetlib3
except ImportError as e:
    raise RuntimeError("telnetlib3 is required (pip install telnetlib3).") from e


@dataclass
class TelnetLogin:
    username: str = ""
    password: str = ""
    username_prompt: Union[str, bytes] = b"login:"
    password_prompt: Union[str, bytes] = b"Password:"


def _to_bytes(x: Union[str, bytes], encoding: str = "utf-8") -> bytes:
    return x if isinstance(x, (bytes, bytearray)) else bytes(str(x), encoding)


def _to_str(x: Union[str, bytes], encoding: str = "utf-8") -> str:
    return x.decode(encoding, "ignore") if isinstance(x, (bytes, bytearray)) else str(x)


class AsyncTelnetClient:
    """
    Async Telnet client that:
      - Uses raw bytes I/O (encoding=None)
      - Supports login with byte/str prompts
      - Auto-detects real device prompt ("AUTO") and reads until it reappears
    """

    def __init__(
        self,
        host: str,
        port: int = 23,
        *,
        connect_timeout: float = 10.0,
        encoding: str = "utf-8",
        prompt: Union[str, bytes] = b"AUTO",   # <--- AUTO means detect after login
        login: Optional[TelnetLogin] = None,
        read_timeout: float = 30.0,
        strip_echo: bool = True,
    ):
        self.host = str(host)
        self.port = int(port)
        self.connect_timeout = float(connect_timeout)
        self.encoding = encoding
        self.login = login
        self.read_timeout = float(read_timeout)
        self.strip_echo = strip_echo

        # Prompt state
        if (isinstance(prompt, (bytes, bytearray)) and prompt.upper() == b"AUTO") or (isinstance(prompt, str) and prompt.upper() == "AUTO"):
            self._auto_prompt = True
            self.prompt_bytes = None  # discover later
        else:
            self._auto_prompt = False
            self.prompt_bytes = _to_bytes(prompt, encoding)

        self._reader = None
        self._writer = None
        self._opened = False
        self._text_mode = False  # telnetlib3 text mode detection (rare)

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self):
        if self._opened:
            return
        # Open raw byte streams
        self._reader, self._writer = await asyncio.wait_for(
            telnetlib3.open_connection(host=self.host, port=self.port, encoding=None),
            timeout=self.connect_timeout,
        )
        self._opened = True
        self._text_mode = bool(getattr(self._reader, "_encoding", None))  # defensive

        if self.login:
            await self._login(self.login)

        # drain banner
        await self._drain_initial()

        # Auto-detect prompt if requested
        if self._auto_prompt:
            await self._detect_prompt()

    async def close(self):
        if not self._opened:
            return
        try:
            if self._writer:
                self._writer.close()
                try:
                    await self._writer.wait_closed()
                except Exception:
                    pass
        finally:
            self._opened = False
            self._reader = None
            self._writer = None

    # ------------ public ------------
    async def exec(self, cmd: str, timeout: float = 60.0) -> Tuple[str, str, int]:
        """Send a command and read until the (detected or provided) prompt reappears."""
        if not self._opened:
            await self.open()

        self._writer.write(_to_bytes(cmd, self.encoding) + b"\n")
        await self._writer.drain()

        # If we have a real prompt, read until that prompt appears again.
        if self.prompt_bytes:
            outb = await asyncio.wait_for(self._reader.readuntil(self.prompt_bytes), timeout=timeout)
            text = outb.decode(self.encoding, "ignore")
        else:
            # Fallback: read one "block" (device that doesn't give stable prompts)
            outb = await asyncio.wait_for(self._reader.readline(), timeout=timeout)
            text = outb.decode(self.encoding, "ignore")

        if self.strip_echo and text.strip().startswith(cmd.strip()):
            # drop the echoed command line
            text = text.split("\n", 1)[-1]
        return text, "", 0

    # ------------ internals ------------
    async def _login(self, login: TelnetLogin):
        up = _to_bytes(login.username_prompt, self.encoding)
        pp = _to_bytes(login.password_prompt, self.encoding)

        # Some firmwares send banners with no prompts; tolerate timeouts.
        try:
            if up:
                await asyncio.wait_for(self._reader.readuntil(up), timeout=self.read_timeout)
        except asyncio.TimeoutError:
            pass

        if login.username:
            self._writer.write(_to_bytes(login.username, self.encoding) + b"\n")
            await self._writer.drain()

        try:
            if pp:
                await asyncio.wait_for(self._reader.readuntil(pp), timeout=self.read_timeout)
        except asyncio.TimeoutError:
            pass

        if login.password:
            self._writer.write(_to_bytes(login.password, self.encoding) + b"\n")
            await self._writer.drain()

    async def _drain_initial(self):
        # Try to read one more line/buffer; some boxes print MOTD, etc.
        try:
            await asyncio.wait_for(self._reader.readline(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    async def _detect_prompt(self):
        """
        Send a newline, capture the last non-empty line, and treat it as a prompt line.
        Typical prompts end with '#', '>' or '$', often with trailing space, e.g. 'Router# '.
        """
        # Nudge device to print prompt
        self._writer.write(b"\n")
        await self._writer.drain()

        buf = b""
        # Read a short burst; most devices print the prompt immediately after newline
        try:
            # Read several small chunks quickly
            for _ in range(5):
                chunk = await asyncio.wait_for(self._reader.readline(), timeout=0.6)
                if not chunk:
                    break
                buf += chunk
                # Heuristic: if the line ends with typical prompt chars, we can stop
                tail = buf.splitlines()[-1] if buf.splitlines() else b""
                if tail.rstrip().endswith((b"#", b">", b"$")) or tail.endswith(b"# ") or tail.endswith(b"> ") or tail.endswith(b"$ "):
                    break
        except asyncio.TimeoutError:
            pass

        # Decide prompt
        lines = buf.splitlines()
        last = lines[-1] if lines else b""
        # If last line looks like a prompt, keep it with trailing space if present; else default to newline fallback
        if last:
            # Normalize: ensure a trailing space if usual char present
            if last.rstrip().endswith((b"#", b">", b"$")) and not last.endswith(b" "):
                last += b" "
            # Require prompt not to be just empty/newline
            if any(ch in last for ch in (b"#", b">", b"$")):
                self.prompt_bytes = last
                return

        # Fallback: we didn’t find a stable prompt — we’ll read line-by-line
        self.prompt_bytes = None

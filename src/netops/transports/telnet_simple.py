# src/netops/transports/telnet_simple.py
from __future__ import annotations

import telnetlib3
from dataclasses import dataclass
from typing import Optional, Tuple, Sequence, List

@dataclass
class TelnetPrompts:
    username: bytes = b"login: "
    password: bytes = b"password: "
    prompt:   bytes = b"> "          # default shell prompt; override with enable=... for '# '

class TelnetRunner:
    """
    Async Telnet runner (bytes-mode) using telnetlib3:
      - Forces encoding=None so streams are bytes.
      - All readuntil(...) and writes use bytes.
      - Converts output to str (UTF-8) right before returning.
      - Optional 'enable' to land on privileged prompt (e.g., '# ').

    Usage in systems:
        async with TelnetRunner(ip, user, pw, (b"Username: ", b"Password: "), enable=("en\n", b"# ")) as sess:
            txt = await sess.exec("show ...")
    """
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        login_prompts: Tuple[bytes, bytes] = (b"login: ", b"password: "),
        enable: Optional[Tuple[str, bytes]] = None,   # e.g., ("en\n", b"# ")
        *,
        encoding: str = "utf-8",
        port: int = 23,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.login_prompts = login_prompts
        self.enable = enable
        self.encoding = encoding

        self._reader = None
        self._writer = None
        self._prompt: bytes = b"> "

    # ---------------- context manager ----------------
    async def __aenter__(self):
        # Force bytes-mode streams
        self._reader, self._writer = await telnetlib3.open_connection(self.host, self.port, encoding=None)

        # username prompt -> username
        await self._reader.readuntil(self.login_prompts[0])
        self._writer.write(self.user.encode(self.encoding) + b"\n")

        # password prompt -> password
        await self._reader.readuntil(self.login_prompts[1])
        self._writer.write(self.password.encode(self.encoding) + b"\n")

        # land on initial prompt (> by default)
        await self._reader.readuntil(b"> ")
        self._prompt = b"> "

        # optional privilege / enable mode
        if self.enable:
            enable_cmd, enable_prompt = self.enable
            cmd_bytes = enable_cmd.encode(self.encoding)
            self._writer.write(cmd_bytes if cmd_bytes.endswith(b"\n") else (cmd_bytes + b"\n"))
            await self._reader.readuntil(enable_prompt)
            self._prompt = enable_prompt

        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._writer:
            try:
                self._writer.close()
                if hasattr(self._writer, "wait_closed"):
                    await self._writer.wait_closed()
            except Exception:
                pass
        self._reader = None
        self._writer = None

    # ---------------- core ops ----------------
    async def run(self, command: str) -> str:
        """Send one command and wait until the current prompt reappears."""
        if not (self._writer and self._reader):
            async with TelnetRunner(
                self.host, self.user, self.password, self.login_prompts, self.enable,
                encoding=self.encoding, port=self.port
            ) as sess:
                return await sess.run(command)

        cmd_b = command.encode(self.encoding)
        self._writer.write(cmd_b if cmd_b.endswith(b"\n") else (cmd_b + b"\n"))
        data_b = await self._reader.readuntil(self._prompt)

        # Drop echoed command if present
        try:
            text = data_b.decode(self.encoding, errors="ignore")
        except Exception:
            text = str(data_b)
        if text.strip().startswith(command.strip()):
            text = text.split("\n", 1)[-1]
        return text

    # ---------------- compatibility shims ----------------
    async def exec(self, command: str) -> str:
        """Alias retained for older system code."""
        return await self.run(command)

    async def exec_many(self, commands: Sequence[str]) -> List[str]:
        out: List[str] = []
        for c in commands:
            out.append(await self.run(c))
        return out

    async def close(self) -> None:
        # Managed by context manager
        return

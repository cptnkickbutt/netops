
import telnetlib3, asyncio
from typing import Optional, Tuple
from netops.transports.base import Runner

class TelnetRunner(Runner):
    def __init__(self, host: str, user: str, password: str,
                 login_prompts: Tuple[bytes, bytes] = (b"login: ", b"password: "),
                 enable: Optional[tuple[str, bytes]] = None):
        self.host, self.user, self.password = host, user, password
        self.login_prompts = login_prompts
        self.enable = enable
        self._reader = None; self._writer = None
        self._prompt: bytes = b"> "

    async def __aenter__(self):
        self._reader, self._writer = await telnetlib3.open_connection(self.host, 23)
        await self._reader.readuntil(self.login_prompts[0])
        self._writer.write(f"{self.user}\n")
        await self._reader.readuntil(self.login_prompts[1])
        self._writer.write(f"{self.password}\n")
        await self._reader.readuntil(b"> ")
        if self.enable:
            self._writer.write(self.enable[0])
            await self._reader.readuntil(self.enable[1])
            self._prompt = self.enable[1]
        else:
            self._prompt = b"> "
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._writer:
            self._writer.close()

    async def run(self, command: str) -> str:
        if self._writer and self._reader:
            self._writer.write(command if command.endswith("\n") else (command+"\n"))
            data = await self._reader.readuntil(self._prompt)
            return data if isinstance(data, str) else data.decode(errors="ignore")
        async with TelnetRunner(self.host, self.user, self.password, self.login_prompts, self.enable) as sess:
            return await sess.run(command)

    async def close(self) -> None:
        pass

# netops/transports/ssh.py
import asyncio
from typing import Optional
import paramiko

from netops.transports.base import Runner, retry_async


class SSHRunner(Runner):
    """
    Async-friendly SSH runner backed by Paramiko.
    - Maintains a single persistent SSHClient per instance
    - .run(command) opens a fresh channel per command (exec_command)
    - Uses asyncio.to_thread to avoid blocking the event loop
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        *,
        port: int = 22,
        timeout: int = 20,
        command_timeout: int = 60,
        look_for_keys: bool = False,
        allow_agent: bool = False,
        compress: bool = True,
        keepalive_secs: int = 30,
        hostkey_policy: str = "auto",  # "auto" | "reject" | "warning"
        known_hosts_file: Optional[str] = None,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.timeout = timeout
        self.command_timeout = command_timeout
        self.look_for_keys = look_for_keys
        self.allow_agent = allow_agent
        self.compress = compress
        self.keepalive_secs = keepalive_secs
        self.hostkey_policy = hostkey_policy
        self.known_hosts_file = known_hosts_file

        self._client: Optional[paramiko.SSHClient] = None

    async def _connect(self) -> None:
        if self._client is not None:
            return

        def _open():
            client = paramiko.SSHClient()
            # Host key policy
            if self.hostkey_policy == "reject":
                client.set_missing_host_key_policy(paramiko.RejectPolicy())
            elif self.hostkey_policy == "warning":
                client.set_missing_host_key_policy(paramiko.WarningPolicy())
            else:
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self.known_hosts_file:
                # Optional: load known_hosts if you want stricter checking
                try:
                    client.load_host_keys(self.known_hosts_file)
                except Exception:
                    pass

            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                timeout=self.timeout,
                auth_timeout=self.timeout,
                banner_timeout=self.timeout,
                look_for_keys=self.look_for_keys,
                allow_agent=self.allow_agent,
                compress=self.compress,
            )

            # Keepalive to help with NAT/idle devices
            try:
                transport = client.get_transport()
                if transport and self.keepalive_secs > 0:
                    transport.set_keepalive(self.keepalive_secs)
            except Exception:
                pass

            return client

        self._client = await asyncio.to_thread(_open)

    @retry_async(times=3, base=0.5, jitter=0.3, exceptions=(Exception,))
    async def run(self, command: str) -> str:
        """
        Execute a command; return combined stdout+stderr as text.
        Retries transient failures with exponential backoff.
        """
        await self._connect()
        assert self._client is not None

        def _exec() -> str:
            stdin, stdout, stderr = self._client.exec_command(
                command,
                timeout=self.command_timeout,
                get_pty=False,  # set True if a device insists on PTY
            )
            # We don't need stdin
            try:
                out = stdout.read()  # bytes
                err = stderr.read()
            finally:
                try:
                    stdout.channel.close()
                except Exception:
                    pass

            data = (out or b"") + (err or b"")
            return data.decode("utf-8", errors="ignore")

        return await asyncio.to_thread(_exec)

    async def close(self) -> None:
        if self._client is None:
            return

        def _close():
            try:
                self._client.close()
            except Exception:
                pass

        await asyncio.to_thread(_close)
        self._client = None

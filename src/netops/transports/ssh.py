from __future__ import annotations
from typing import Tuple
import paramiko

__all__ = ["make_ssh_client", "ssh_exec"]

DEFAULT_TIMEOUT = 10

def make_ssh_client(
    host: str,
    port: int,
    username: str,
    password: str,
    timeout: int = DEFAULT_TIMEOUT,
    *,
    strict_host_key: bool = False,
) -> paramiko.SSHClient:
    """
    Create and return a connected Paramiko SSHClient.

    - strict_host_key=False (default): accept unknown keys (AutoAddPolicy) ✅
    - strict_host_key=True: require known keys (RejectPolicy)

    We also disable key/agent auth to avoid surprises and set explicit timeouts.
    """
    client = paramiko.SSHClient()
    if strict_host_key:
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
    else:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(
        hostname=host,
        port=port,
        username=username,
        password=password,
        look_for_keys=False,
        allow_agent=False,
        timeout=timeout,
        banner_timeout=timeout,
        auth_timeout=timeout,
    )
    return client


def ssh_exec(client: paramiko.SSHClient, cmd: str, timeout: int = 60) -> Tuple[str, str, int]:
    """
    Execute a command over SSH and return (stdout, stderr, exit_code).
    """
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    rc = stdout.channel.recv_exit_status()
    return out, err, rc

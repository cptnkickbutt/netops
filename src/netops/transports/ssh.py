from __future__ import annotations
from typing import Tuple
import paramiko

__all__ = ["make_ssh_client", "ssh_exec"]

def make_ssh_client(host: str, port: int, username: str, password: str, timeout: int = 10) -> paramiko.SSHClient:
    """Create and return a connected Paramiko SSHClient."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(hostname=host, port=port, username=username, password=password, timeout=timeout)
    return client

def ssh_exec(client: paramiko.SSHClient, cmd: str, timeout: int = 60) -> Tuple[str, str, int]:
    """Execute a command over SSH and return (stdout, stderr, exit_code)."""
    stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="ignore")
    err = stderr.read().decode("utf-8", errors="ignore")
    rc = stdout.channel.recv_exit_status()
    return out, err, rc

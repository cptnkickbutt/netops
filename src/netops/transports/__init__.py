# Back-compat shim: re-export transports.* symbols under transport.*
from netops.transports.base import ExecTransport
from .ssh import make_ssh_client, ssh_exec
from .sftp import ensure_dir_over_ssh, sftp_listdir

# sync telnet
from .telnet import make_telnet_client, telnet_exec

# async telnet (telnetlib3)
from .telnet_async import (
    AsyncTelnetClient,
    TelnetLogin,
)

__all__ = [
    "make_ssh_client", "ssh_exec",
    "ensure_dir_over_ssh", "sftp_listdir",
    "make_telnet_client", "telnet_exec",
    "AsyncTelnetClient", "TelnetLogin",
    "make_telnet_client_async", "telnet_exec_async",
]

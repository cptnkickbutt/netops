from __future__ import annotations
from pathlib import Path
from netops.config import FileSvrCfg
from netops.transports import make_ssh_client, ensure_dir_over_ssh
from netops.transports.sftp import ensure_dir_over_ssh

__all__ = ["upload_to_file_server"]

DEFAULT_REMOTE_DIR = "/mnt/TelcomFS/Daily_Export_and_Hash_Logs/"

def upload_to_file_server(local_path: Path, cfg: FileSvrCfg, remote_dir: str = DEFAULT_REMOTE_DIR) -> str:
    """
    Upload a local file to the file server via SFTP.
    Returns the remote path used.

    Raises on failure with a clear message.
    """
    local_path = Path(local_path)
    if not local_path.is_file():
        raise FileNotFoundError(f"Upload source not found: {local_path}")

    cfg.validate()
    password = cfg.resolve_password()

    client = make_ssh_client(cfg.host, cfg.port, cfg.user, password, timeout=10)
    try:
        ensure_dir_over_ssh(client, remote_dir)
        sftp = client.open_sftp()
        try:
            remote_path = f"{remote_dir}{local_path.name}"
            sftp.put(str(local_path), remote_path)
        finally:
            sftp.close()
    finally:
        client.close()

    return remote_path

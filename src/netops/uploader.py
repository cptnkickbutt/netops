
import os, paramiko
from netops.logging import get_logger
log = get_logger()

def store_on_server(file: str) -> None:
    host = '10.100.3.9'; port = 22
    user = os.getenv('USER3'); pw = os.getenv('PW1')
    if not (user and pw):
        log.warning("SFTP credentials missing; skip upload")
        return
    remote_path = f'/mnt/TelcomFS/Monthly_Speed_Audit/{file}'
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(host, port, user, pw)
        sftp = ssh_client.open_sftp()
        sftp.put(file, remote_path)
        sftp.close(); ssh_client.close()
        log.info(f"{file} successfully added to server")
    except Exception as e:
        log.error(f"SFTP upload failed: {e}")

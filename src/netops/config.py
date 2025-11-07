# src/netops/config.py
from __future__ import annotations

import os
from dataclasses import dataclass
import csv
from pathlib import Path
from typing import Optional, List, Tuple
from dotenv import load_dotenv

# Load .env once at import; harmless if no .env present.
load_dotenv()

__all__ = [
    "MailCfg",
    "FileSvrCfg",
    "load_env",
    "load_inventory",
    "resolve_env",
    "resolve_env_or_literal",
    "require_env",
]

# ---------------------------
# Mail / SMTP configuration
# ---------------------------

@dataclass(frozen=True)
class MailCfg:
    """
    SMTP config. Prefers GMAIL_* variables; falls back to legacy SENDER_* if present.
    """
    sender_email: str = os.getenv("GMAIL_USER", "") or os.getenv("SENDER_EMAIL", "")
    sender_password: str = os.getenv("GMAIL_APP_PASSWORD", "") or os.getenv("SENDER_PASSWORD", "")
    host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port: int = int(os.getenv("SMTP_PORT", "587"))

    def validate(self) -> None:
        missing = []
        if not self.sender_email:
            missing.append("GMAIL_USER (or SENDER_EMAIL)")
        if not self.sender_password:
            missing.append("GMAIL_APP_PASSWORD (or SENDER_PASSWORD)")
        if missing:
            raise RuntimeError(f"Missing mail env: {', '.join(missing)}")


# ---------------------------
# File server (SFTP) config
# ---------------------------

@dataclass
class FileSvrCfg:
    host: str
    username: str
    password: str
    remote_dir: str
    port: int = 22

    @classmethod
    def from_env(cls) -> "FileSvrCfg":
        """
        Build from current environment:
          FILESERV_HOST, FILESERV_USER, FILESERV_PASSWORD, FILESERV_PATH, FILESERV_PORT
        """
        return cls(
            host=os.getenv("FILESERV_HOST", "").strip(),
            username=os.getenv("FILESERV_USER", "").strip(),
            password=os.getenv("FILESERV_PASSWORD", "").strip(),
            remote_dir=os.getenv("FILESERV_PATH", "/mnt/TelcomFS/Monthly_Speed_Audit").strip(),
            port=int(os.getenv("FILESERV_PORT", "22")),
        )

    def resolve_password(self) -> str:
        """
        Backward-compatible shim: simply return the configured password.
        (Older versions tried to resolve by indirection; we don't need that now.)
        """
        if not self.password:
            raise RuntimeError("File server password not set (FILESERV_PASSWORD).")
        return self.password

    def validate(self) -> None:
        missing = []
        if not self.host:
            missing.append("FILESERV_HOST")
        if not self.username:
            missing.append("FILESERV_USER")
        if not self.password:
            missing.append("FILESERV_PASSWORD")
        if not self.remote_dir:
            missing.append("FILESERV_PATH")
        if missing:
            raise RuntimeError(f"Missing file server env: {', '.join(missing)}")


# ---------------------------
# Env helpers
# ---------------------------

def load_env(env_file: Optional[str | Path] = None) -> None:
    """
    Load environment variables from a .env file.
    - If env_file is provided, load it directly.
    - Otherwise, attempt to load from current working directory, then repo root.
    """
    if env_file:
        env_path = Path(env_file)
    else:
        env_path = Path.cwd() / ".env"

    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        # fallback: try repo root (.env next to pyproject.toml)
        repo_env = Path(__file__).resolve().parents[2] / ".env"
        if repo_env.exists():
            load_dotenv(dotenv_path=repo_env)


def load_inventory(csv_path: str | Path = "propertyinformation.csv") -> List[List[str]]:
    """
    Read property information from CSV into list of lists.

    Expected CSV columns:
      0 - property name
      1 - IP address
      2 - username env key
      3 - password env key
      (extra columns are ignored here)
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Inventory file not found: {csv_path}")

    with path.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header if present
        return [row for row in reader if len(row) >= 4]


def resolve_env(user_key: str, pw_key: str) -> Tuple[str, str]:
    """
    Look up credentials by environment variable keys.
    Example:
        resolve_env("USER1", "PW3")
    """
    user = os.getenv(user_key)
    pw = os.getenv(pw_key)
    if not user or not pw:
        raise EnvironmentError(f"Missing environment variables: {user_key}/{pw_key}")
    return user, pw


def resolve_env_or_literal(key_or_pw: str) -> str:
    """Return os.getenv(key_or_pw) if present; otherwise treat the input as a literal."""
    return os.getenv(key_or_pw) or key_or_pw


def require_env(name: str, *, friendly: Optional[str] = None) -> str:
    """Raise a clear error if a required env is missing."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing environment variable: {friendly or name} ({name})")
    return val

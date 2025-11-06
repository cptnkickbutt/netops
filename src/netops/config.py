from __future__ import annotations
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load .env once at import; harmless if no .env present.
load_dotenv()

__all__ = [
    "MailCfg",
    "FileSvrCfg",
    "resolve_env_or_literal",
    "require_env",
]

@dataclass(frozen=True)
class MailCfg:
    """SMTP configuration for sending messages."""
    sender_email: str = os.getenv("SENDER_EMAIL", "")
    sender_password: str = os.getenv("SENDER_PASSWORD", "")
    host: str = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port: int = int(os.getenv("SMTP_PORT", "587"))

    def validate(self) -> None:
        missing = []
        if not self.sender_email:    missing.append("SENDER_EMAIL")
        if not self.sender_password: missing.append("SENDER_PASSWORD")
        if missing:
            raise RuntimeError(f"Missing mail env: {', '.join(missing)}")

@dataclass(frozen=True)
class FileSvrCfg:
    """
    File server (SFTP) configuration.

    You can either provide FILE_SVR_PW_VALUE directly,
    OR set FILE_SVR_PW to the name of another env var that holds the password.
    """
    host: str = os.getenv("FILE_SVR_HOST", "")
    port: int = int(os.getenv("FILE_SVR_PORT", "22"))
    user: str = os.getenv("FILE_SVR_USER", "")
    pw_key: str = os.getenv("FILE_SVR_PW", "")          # name of an env var
    pw_value: str = os.getenv("FILE_SVR_PW_VALUE", "")  # direct secret value

    def resolve_password(self) -> str:
        if self.pw_value:
            return self.pw_value
        if self.pw_key:
            val = os.getenv(self.pw_key, "")
            if val:
                return val
        raise RuntimeError(
            "File server password not found. "
            "Set FILE_SVR_PW_VALUE or set FILE_SVR_PW to the name of an env var that holds the password."
        )

    def validate(self) -> None:
        missing = []
        if not self.host: missing.append("FILE_SVR_HOST")
        if not self.user: missing.append("FILE_SVR_USER")
        if missing:
            raise RuntimeError(f"Missing file server env: {', '.join(missing)}")

def resolve_env_or_literal(key_or_pw: str) -> str:
    """Return os.getenv(key_or_pw) if present; otherwise treat the input as a literal."""
    return os.getenv(key_or_pw) or key_or_pw

def require_env(name: str, *, friendly: Optional[str] = None) -> str:
    """Raise a clear error if a required env is missing."""
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing environment variable: {friendly or name} ({name})")
    return val

from __future__ import annotations
import smtplib
from pathlib import Path
from typing import Iterable
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

__all__ = ["send_email_with_attachment", "send_plain"]

def send_plain(sender_email: str, sender_password: str, host: str, port: int,
               recipients: Iterable[str], subject: str, body: str) -> None:
    """Send a plain text email to a list of recipients."""
    recipients = list(recipients)
    if not recipients:
        return
    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        for rcpt in recipients:
            msg = MIMEText(body, "plain")
            msg["From"] = sender_email
            msg["To"] = rcpt
            msg["Subject"] = subject
            server.sendmail(sender_email, rcpt, msg.as_string())

def send_email_with_attachment(sender_email: str, sender_password: str, host: str, port: int,
                               recipients: Iterable[str], subject: str, body: str,
                               file_path: Path) -> None:
    """
    Send a plaintext message with a single attachment.

    NOTE: 'From' must be the sender email (not the password). This fixes a common bug.
    """
    recipients = list(recipients)
    if not recipients:
        return

    file_path = Path(file_path)
    if not file_path.is_file():
        raise FileNotFoundError(f"Attachment missing: {file_path}")

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        for rcpt in recipients:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = rcpt
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            with file_path.open("rb") as f:
                part = MIMEApplication(f.read(), Name=file_path.name)
            part.add_header("Content-Disposition", "attachment", filename=file_path.name)
            msg.attach(part)

            server.sendmail(sender_email, rcpt, msg.as_string())

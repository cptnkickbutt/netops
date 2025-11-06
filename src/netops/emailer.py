
import os, smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from netops.logging import get_logger
log = get_logger()

def send_email(report_date: str, file: str) -> None:
    recipients = [e.strip() for e in (os.getenv("EMAIL_TO") or "").split(",") if e.strip()]
    bcc = [e.strip() for e in (os.getenv("EMAIL_BCC") or "").split(",") if e.strip()]
    sender_email = os.getenv('EMAIL_USER')
    sender_password = os.getenv('EMAIL_APP_PASSWORD')
    if not sender_email or not sender_password or not recipients:
        log.warning("Email config missing; skipping email")
        return
    subject = f'{report_date} Speed Audit Results'
    body = (f'Attached speed data gathered on {report_date}.\n\n'
            "*This is an automated message; replies won't be seen.*\n"
            "Send any questions to eshortt@telcomsys.net")
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ','.join(recipients)
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))
    with open(file, 'rb') as f:
        message.attach(MIMEApplication(f.read(), Name=file))
    all_recipients = recipients + bcc
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        text = message.as_string()
        server.sendmail(sender_email, all_recipients, text)
        log.info(f"Email sent to {recipients}")
    except Exception as e:
        log.error(f"Email failed: {e}")
    finally:
        try: server.quit()
        except Exception: pass

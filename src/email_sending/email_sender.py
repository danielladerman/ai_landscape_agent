from config.config import settings
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(recipient_email: str, subject: str, body: str) -> bool:
    """
    Sends an email using the SMTP settings from the centralized config.
    """
    # All settings are now pulled directly from the validated settings object.
    if not all([settings.SMTP_SERVER, settings.SMTP_PORT, settings.SMTP_USERNAME, settings.SMTP_PASSWORD, settings.SENDER_EMAIL]):
        print("🔴 SMTP settings are not fully configured. Cannot send email.")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = settings.SENDER_EMAIL
        msg['To'] = recipient_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP(settings.SMTP_SERVER, settings.SMTP_PORT)
        server.starttls()
        server.login(settings.SMTP_USERNAME, settings.SMTP_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Email sent successfully to {recipient_email}")
        return True
    except Exception as e:
        print(f"🔴 Failed to send email to {recipient_email}. Error: {e}")
        return False

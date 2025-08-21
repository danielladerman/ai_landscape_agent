import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from src.gmail_helpers import get_gmail_service

def send_email(recipient_email: str, subject: str, body: str) -> bool:
    """
    Sends an email using the Gmail API.
    """
    service = get_gmail_service()
    if not service:
        logging.error("🔴 Could not get Gmail service. Cannot send email.")
        return False

    try:
        message = MIMEMultipart()
        message['to'] = recipient_email
        message['subject'] = subject
        
        # --- HTML Formatting ---
        # Replace newline characters with HTML line breaks for proper rendering.
        html_body = body.replace('\n', '<br>')
        
        # The body of the email is now attached as HTML
        message.attach(MIMEText(html_body, 'html'))

        # The API requires the message to be base64url encoded
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
        
        create_message = {
            'raw': raw_message
        }

        # Send the message
        sent_message = service.users().messages().send(
            userId="me",
            body=create_message
        ).execute()

        logging.info(f"✅ Email sent successfully to {recipient_email}. Message ID: {sent_message['id']}")
        return True

    except Exception as e:
        logging.error(f"🔴 Failed to send email to {recipient_email} via Gmail API. Error: {e}")
        return False

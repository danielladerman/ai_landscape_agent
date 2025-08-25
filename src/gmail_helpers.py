import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from config.config import settings
from datetime import datetime, timedelta

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
# This new scope allows reading, composing, and sending emails.
# If you change this, you MUST delete the token.json file to re-authenticate.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

def _execute_gmail_query(service, query):
    """A helper to execute a query and return the message count."""
    try:
        result = service.users().messages().list(userId='me', q=query).execute()
        return result.get('resultSizeEstimate', 0)
    except Exception as e:
        logging.error(f"🔴 Error executing Gmail query '{query}': {e}")
        return 0

def get_email_stats():
    """
    Fetches key email statistics for the last 24 hours.
    """
    service = get_gmail_service()
    if not service:
        return {
            "emails_sent": "Error",
            "replies_received": "Error",
            "bounces": "Error"
        }

    # Define the time window for the last 24 hours
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y/%m/%d')
    query_base = f'after:{yesterday}'

    # --- Build Queries ---
    # Emails sent from the user's account
    sent_query = f'{query_base} from:me'
    # Replies are harder to track, but we can look for common indicators
    # Note: This is an approximation.
    reply_query = f'{query_base} to:me (subject:"Re:" OR in:inbox)'
    # Bounce query from the bounce processing script
    bounce_query = f'{query_base} subject:("Delivery Status Notification (Failure)" OR "Undelivered Mail Returned to Sender" OR "Undeliverable") OR from:mailer-daemon@google.com'

    # --- Fetch Stats ---
    emails_sent = _execute_gmail_query(service, sent_query)
    replies_received = _execute_gmail_query(service, reply_query)
    bounces = _execute_gmail_query(service, bounce_query)

    return {
        "emails_sent_24h": emails_sent,
        "replies_received_24h": replies_received,
        "bounces_24h": bounces
    }

def get_gmail_service():
    """
    Authenticates with the Gmail API and returns a service object.
    Handles the OAuth 2.0 flow with the correct scopes for sending and reading.
    """
    creds = None
    if os.path.exists(settings.GMAIL_API_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(settings.GMAIL_API_TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                
                # If token is refreshed, save it to a writable temporary path for this session
                try:
                    temp_token_path = os.path.join("/tmp", "gmail_token.json")
                    with open(temp_token_path, 'w') as token:
                        token.write(creds.to_json())
                    logging.info("Refreshed Gmail token and saved to temporary session file.")
                except Exception as e:
                    logging.warning(f"Could not save refreshed token to temporary file: {e}")

            except Exception as e:
                logging.warning(f"Could not refresh token, re-authentication might be needed locally: {e}")
                # Don't try to re-auth on a server; fail gracefully.
                return None
        
        if not creds: # This block runs if no valid token exists
            if not os.path.exists(settings.GMAIL_API_CREDENTIALS_PATH):
                logging.error(f"🔴 CRITICAL: '{os.path.basename(settings.GMAIL_API_CREDENTIALS_PATH)}' not found.")
                logging.error("Please enable the Gmail API in Google Cloud Console and download credentials.json.")
                return None
            
            logging.error("Gmail token is invalid or missing. Please re-authenticate locally to generate a valid token.json and upload it as a secret file.")
            return None
            
    try:
        service = build('gmail', 'v1', credentials=creds)
        logging.info("✅ Gmail service authenticated successfully.")
        return service
    except Exception as e:
        logging.error(f"🔴 Error building Gmail service: {e}")
        return None

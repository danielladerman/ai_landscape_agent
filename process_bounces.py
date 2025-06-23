import os
import base64
import re
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from src import google_sheets_helpers
from config.config import settings # Import the centralized settings

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bounce_processing.log"),
        logging.StreamHandler()
    ]
)

# --- Constants ---
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_gmail_service():
    """
    Authenticates with the Gmail API and returns a service object.
    Handles the OAuth 2.0 flow.
    """
    creds = None
    if os.path.exists(settings.GMAIL_API_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(settings.GMAIL_API_TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(settings.GMAIL_API_CREDENTIALS_PATH):
                logging.error(f"🔴 CRITICAL: '{os.path.basename(settings.GMAIL_API_CREDENTIALS_PATH)}' not found in '{os.path.dirname(settings.GMAIL_API_CREDENTIALS_PATH)}'.")
                logging.error("Please enable the Gmail API in the Google Cloud Console and download the credentials file.")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(settings.GMAIL_API_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(settings.GMAIL_API_TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
            
    return build('gmail', 'v1', credentials=creds)

def find_bounced_emails(service):
    """
    Searches for bounced emails and returns a list of message IDs.
    """
    try:
        # Search for standard bounce-back subjects from Mailer-Daemon and other common patterns
        query = 'subject:("Delivery Status Notification (Failure)" OR "Undelivered Mail Returned to Sender" OR "Undeliverable") OR from:mailer-daemon@google.com'
        result = service.users().messages().list(userId='me', q=query).execute()
        messages = result.get('messages', [])
        return messages
    except Exception as e:
        logging.error(f"Error searching for bounced emails: {e}")
        return []

def get_bounced_recipient(service, msg_id):
    """
    Parses a single email to find the original recipient who bounced and a simple reason.
    """
    try:
        msg = service.users().messages().get(userId='me', id=msg_id, format='full').execute()
        payload = msg.get('payload', {})
        snippet = msg.get('snippet', '').lower()
        
        # --- Find Recipient ---
        email_regex = r'<([\w\.-]+@[\w\.-]+)>|[\w\.-]+@[\w\.-]+'
        recipient = None

        # Search headers first for reliability
        headers = payload.get('headers', [])
        for header in headers:
            if header['name'].lower() == 'final-recipient':
                match = re.search(email_regex, header['value'])
                if match:
                    recipient = match.group(1) if match.group(1) else match.group(0)
                    break
        
        # If not in headers, try the snippet
        if not recipient:
            match = re.search(email_regex, snippet)
            if match:
                recipient = match.group(1) if match.group(1) else match.group(0)

        if not recipient:
            return None, None

        # --- Find Reason from Snippet ---
        if "does not exist" in snippet or "address couldn't be found" in snippet or "no such user" in snippet:
            reason = "Address not found"
        elif "mailbox full" in snippet or "quota exceeded" in snippet:
            reason = "Mailbox full"
        elif "blocked by" in snippet or "rejected" in snippet:
            reason = "Blocked by server"
        elif "unable to receive" in snippet:
            reason = "Recipient unable to receive"
        else:
            reason = "Delivery failed"
            
        return recipient, reason

    except Exception as e:
        logging.error(f"Error parsing email ID {msg_id}: {e}")
        return None, None

def process_bounces():
    """
    Main function to orchestrate finding, parsing, and processing bounced emails.
    """
    logging.info("--- STARTING BOUNCE PROCESSING ---")
    
    service = get_gmail_service()
    if not service:
        logging.error("Could not authenticate with Gmail. Aborting.")
        return

    bounced_messages = find_bounced_emails(service)
    if not bounced_messages:
        logging.info("No new bounced emails found.")
        logging.info("--- BOUNCE PROCESSING COMPLETE ---")
        return
        
    logging.info(f"Found {len(bounced_messages)} potential bounce notifications.")
    
    bounced_recipients_info = {} # Use a dict to store {email: reason}
    clean_email_regex = r'[\w\.\-]{1,64}@[\w\.\-]+\.[a-zA-Z]{2,}'

    for message in bounced_messages:
        raw_recipient, reason = get_bounced_recipient(service, message['id'])
        if raw_recipient:
            # Clean the extracted string to get only the valid email part
            match = re.search(clean_email_regex, raw_recipient)
            if match:
                clean_recipient = match.group(0)
                # Store the email and reason. Overwrites duplicates, which is fine.
                bounced_recipients_info[clean_recipient] = reason
                logging.info(f"Identified bounced recipient: {clean_recipient}, Reason: {reason}")
            else:
                logging.warning(f"Could not clean extracted recipient: {raw_recipient}")
        else:
            logging.warning(f"Could not extract recipient from message ID: {message['id']}")
            
    if bounced_recipients_info:
        logging.info(f"Updating Google Sheet for {len(bounced_recipients_info)} unique bounced emails...")
        
        g_service = google_sheets_helpers.get_google_sheets_service()
        if g_service:
            # First, get the entire sheet as a DataFrame
            prospects_df = google_sheets_helpers.get_sheet_as_df(g_service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)
            if prospects_df is not None:
                google_sheets_helpers.update_bounced_status_bulk(
                    g_service,
                    settings.SPREADSHEET_ID,
                    settings.GOOGLE_SHEET_NAME,
                    bounced_recipients_info,  # Pass the dictionary
                    prospects_df
                )
            else:
                logging.error("Could not retrieve prospects from Google Sheet to check for bounces.")
        else:
            logging.error("Could not get Google Sheets service to update bounce statuses.")

    else:
        logging.info("No valid bounced recipient email addresses were identified after parsing.")

    logging.info("--- BOUNCE PROCESSING COMPLETE ---")


if __name__ == "__main__":
    process_bounces() 
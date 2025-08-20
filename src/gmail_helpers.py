import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from config.config import settings

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
# This new scope allows reading, composing, and sending emails.
# If you change this, you MUST delete the token.json file to re-authenticate.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

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
            except Exception as e:
                logging.warning(f"Could not refresh token, forcing re-authentication: {e}")
                creds = None # Force re-auth if refresh fails
        
        if not creds: # This block runs if no valid token exists
            if not os.path.exists(settings.GMAIL_API_CREDENTIALS_PATH):
                logging.error(f"🔴 CRITICAL: '{os.path.basename(settings.GMAIL_API_CREDENTIALS_PATH)}' not found.")
                logging.error("Please enable the Gmail API in Google Cloud Console and download credentials.json.")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(settings.GMAIL_API_CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        with open(settings.GMAIL_API_TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
            
    try:
        service = build('gmail', 'v1', credentials=creds)
        logging.info("✅ Gmail service authenticated successfully.")
        return service
    except Exception as e:
        logging.error(f"🔴 Error building Gmail service: {e}")
        return None

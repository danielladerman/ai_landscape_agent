import pandas as pd
import logging
import argparse
import re # Import the regex module
from datetime import datetime
from config.config import settings
from src.google_sheets_helpers import get_google_sheets_service, get_sheet_as_df, update_sent_status_bulk, update_bounced_status_bulk, add_tracking_columns
from src.email_sending import email_sender

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/daily_sending.log"),
        logging.StreamHandler()
    ]
)

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

def _clean_email(email_string: str) -> str:
    """Extracts a clean email address from a raw string using regex."""
    if not isinstance(email_string, str):
        return None
    match = re.search(EMAIL_REGEX, email_string)
    if match:
        return match.group(0).lower()
    return None

def run_daily_sending(max_emails: int):
    """
    Sends a daily batch of initial emails to prospects who have not yet been contacted.
    """
    logging.info(f"--- STARTING DAILY SENDING JOB: Max Emails = {max_emails} ---")

    service = get_google_sheets_service()
    if not service:
        logging.error("🔴 Could not connect to Google Sheets. Aborting.")
        return

    add_tracking_columns(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)

    df = get_sheet_as_df(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)
    if df is None or df.empty:
        logging.info("Prospect sheet is empty. Nothing to do.")
        return

    # Filter for prospects where 'sent_date' is empty
    prospects_to_email = df[df['sent_date'].fillna('') == ''].head(max_emails)

    if prospects_to_email.empty:
        logging.info("No new prospects to email today. All initial emails have been sent.")
        return

    logging.info(f"Found {len(prospects_to_email)} prospects to email.")
    
    successful_sends = []
    failed_sends = {} # Using a dict to store name and reason

    for _, prospect in prospects_to_email.iterrows():
        subject = prospect.get('generated_subject')
        body = prospect.get('generated_body')
        
        # Safely extract the first email address from the list-like string
        try:
            # The data is stored as a string '["email@a.com"]', so we parse it
            recipient_list = eval(prospect.get('verified_emails', '[]'))
            if isinstance(recipient_list, list) and recipient_list:
                raw_recipient = recipient_list[0]
            else:
                raw_recipient = None
        except:
            raw_recipient = prospect.get('verified_emails') # Fallback for plain strings

        recipient = _clean_email(raw_recipient)
        name = prospect.get('name')

        if not all([subject, body, recipient, name]):
            logging.warning(f"Skipping prospect {name} due to missing data. Marking as bounced.")
            failed_sends[name] = 'Missing Data'
            continue

        logging.info(f"Attempting to send email to {name} at {recipient}...")
        success = email_sender.send_email(
            recipient_email=recipient,
            subject=subject,
            body=body
        )

        if success:
            successful_sends.append(name)
        else:
            logging.warning(f"Failed to send email to {name}. Marking as bounced.")
            failed_sends[name] = 'Sending Failed'
        
    # --- Bulk Update Google Sheet ---
    if successful_sends:
        update_sent_status_bulk(
            service,
            settings.SPREADSHEET_ID,
            settings.GOOGLE_SHEET_NAME,
            prospect_names=successful_sends,
            column_name='sent_date'
        )

    if failed_sends:
        update_bounced_status_bulk(
            service,
            settings.SPREADSHEET_ID,
            settings.GOOGLE_SHEET_NAME,
            bounced_prospects=failed_sends # Pass the dict here
        )

    logging.info(f"--- DAILY SENDING COMPLETE: Successfully sent {len(successful_sends)} emails. ---")


if __name__ == "__main__":
    if not all([settings.SMTP_SERVER, settings.SMTP_PORT, settings.SMTP_USERNAME, settings.SMTP_PASSWORD, settings.SENDER_EMAIL]):
        logging.error("🔴 ABORTING: SMTP settings are not fully configured in your .env file.")
    else:
        parser = argparse.ArgumentParser(description="Run the daily email sending job for initial outreach.")
        parser.add_argument("--max_emails", type=int, default=10, help="The maximum number of emails to send in this batch.")
        args = parser.parse_args()
        run_daily_sending(max_emails=args.max_emails) 
import pandas as pd
import logging
import argparse
from datetime import datetime, timedelta
import time
import json

from config.config import settings
from src.email_sending import email_sender
from src.email_generation import email_generator
from src import google_sheets_helpers

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/follow_up.log"),
        logging.StreamHandler()
    ]
)

def run_follow_up_campaign(daily_limit: int):
    """
    Scans the master prospect list and sends scheduled follow-up emails.
    """
    logging.info(f"--- STARTING FOLLOW-UP CAMPAIGN: Limit={daily_limit} ---")

    # --- Load Master Prospect List ---
    service = google_sheets_helpers.get_google_sheets_service()
    if not service:
        logging.error("🔴 Could not connect to Google Sheets. Aborting.")
        return
        
    df = google_sheets_helpers.get_sheet_as_df(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)
    if df is None or df.empty:
        logging.info("Prospect sheet is empty or could not be loaded. Nothing to do.")
        return

    # --- Data Cleaning and Preparation ---
    # Ensure all required columns exist
    required_cols = ['sent_date', 'follow_up_1_sent_date', 'follow_up_2_sent_date', 'follow_up_3_sent_date', 'email_status']
    for col in required_cols:
        if col not in df.columns:
            df[col] = '' # Add missing columns to prevent KeyErrors
            logging.warning(f"Added missing column '{col}' to DataFrame.")

    # Convert date columns to datetime objects for comparison, keeping only the date part
    for col in ['sent_date', 'follow_up_1_sent_date', 'follow_up_2_sent_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

    # Filter out prospects who should not be contacted
    active_prospects = df[df['email_status'].isin(['', 'Sent', 'Delivered'])].copy()
    
    # --- Determine Which Follow-up to Send ---
    today = datetime.now().date()
    prospects_to_email = []

    for index, row in active_prospects.iterrows():
        # Stop if we hit the daily limit
        if len(prospects_to_email) >= daily_limit:
            break

        # Sequence 1: 3 days after initial email
        if pd.notna(row['sent_date']) and pd.isna(row['follow_up_1_sent_date']):
            if today >= row['sent_date'] + timedelta(days=3):
                row['follow_up_stage'] = 1
                prospects_to_email.append(row)
                continue

        # Sequence 2: 5 days after first follow-up
        if pd.notna(row['follow_up_1_sent_date']) and pd.isna(row['follow_up_2_sent_date']):
            if today >= row['follow_up_1_sent_date'] + timedelta(days=5):
                row['follow_up_stage'] = 2
                prospects_to_email.append(row)
                continue

        # Sequence 3: 7 days after second follow-up
        if pd.notna(row['follow_up_2_sent_date']) and pd.isna(row['follow_up_3_sent_date']):
            if today >= row['follow_up_2_sent_date'] + timedelta(days=7):
                row['follow_up_stage'] = 3
                prospects_to_email.append(row)
                continue
    
    if not prospects_to_email:
        logging.info("No prospects are due for a follow-up email today.")
        logging.info("--- FOLLOW-UP CAMPAIGN COMPLETE ---")
        return

    logging.info(f"Found {len(prospects_to_email)} prospects due for a follow-up.")

    # --- Generate and Send Emails ---
    successful_sends = [] # List of (email, stage) tuples

    for prospect in prospects_to_email:
        prospect_dict = prospect.to_dict()
        stage = prospect_dict['follow_up_stage']
        
        logging.info(f"Processing Stage {stage} follow-up for {prospect_dict['name']}...")

        # --- Strategy Alignment Check ---
        # Ensure the prospect's original proposed solution aligns with our current strategy.
        try:
            # The 'proposed_solutions' field is a plain string, not JSON.
            # We can use the raw string directly.
            primary_solution = prospect_dict.get('proposed_solutions', '')
            
            # This is the crucial check. Only proceed if the original pitch was about our current strategy.
            allowed_keywords = [
                "Content",
                "Social Media",
                "Brand",
                "Targeted Lead Generation",
                "Curated Instagram Content Management"
            ]
            if not any(keyword in primary_solution for keyword in allowed_keywords):
                logging.warning(f"Skipping follow-up for {prospect_dict['name']} due to outdated strategy ('{primary_solution}'). Marking as bounced.")
                # Mark as bounced in the sheet
                google_sheets_helpers.update_prospect_status(
                    service,
                    settings.SPREADSHEET_ID,
                    settings.GOOGLE_SHEET_NAME,
                    prospect_name=prospect_dict['name'],
                    column_name='email_status',
                    status_value='Bounced'
                )
                google_sheets_helpers.update_prospect_status(
                    service,
                    settings.SPREADSHEET_ID,
                    settings.GOOGLE_SHEET_NAME,
                    prospect_name=prospect_dict['name'],
                    column_name='termination_reason',
                    status_value='Outdated Strategy'
                )
                continue
        except Exception as e:
            logging.warning(f"Skipping follow-up for {prospect_dict['name']} due to a processing error: {e}")
            continue
            
        # 1. Generate the follow-up email
        email_content = email_generator.generate_follow_up_email(prospect_dict, stage)
        
        if not email_content:
            logging.warning(f"Could not generate email for {prospect_dict['name']}. Skipping.")
            continue
            
        # 2. Send the email
        try:
            recipient_list = eval(prospect.get('verified_emails', '[]'))
            if isinstance(recipient_list, list) and recipient_list:
                recipient = recipient_list[0]
            else:
                recipient = None
        except:
            recipient = prospect.get('verified_emails') # Fallback for plain strings
            
        if not recipient:
            logging.warning(f"Skipping {prospect_dict['name']} due to invalid email format.")
            continue

        success = email_sender.send_email(
            recipient_email=recipient,
            subject=email_content['subject'],
            body=email_content['body']
        )

        # 3. Update the Google Sheet if sending was successful
        if success:
            # Add to our list for bulk update
            successful_sends.append((recipient, stage))
            time.sleep(5) # Respectful delay between sends

    # --- Bulk Update Google Sheet ---
    if successful_sends:
        logging.info(f"Updating status for {len(successful_sends)} successfully sent follow-ups...")
        google_sheets_helpers.update_follow_up_status(
            service,
            settings.SPREADSHEET_ID,
            settings.GOOGLE_SHEET_NAME,
            prospect_updates=successful_sends
        )

    logging.info(f"Successfully sent {len(successful_sends)} follow-up emails.")
    logging.info("--- FOLLOW-UP CAMPAIGN COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Follow-up Email Campaign Job.")
    parser.add_argument("--limit", type=int, default=25, help="The maximum number of follow-up emails to send in this batch.")
    args = parser.parse_args()

    run_follow_up_campaign(daily_limit=args.limit) 
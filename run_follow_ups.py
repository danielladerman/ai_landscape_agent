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

    # Convert date columns to datetime objects for comparison
    for col in ['sent_date', 'follow_up_1_sent_date', 'follow_up_2_sent_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Filter out prospects who should not be contacted
    active_prospects = df[df['email_status'].isin(['', 'Sent', 'Delivered'])].copy()
    
    # --- Determine Which Follow-up to Send ---
    today = datetime.now()
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
    send_count = 0
    for prospect in prospects_to_email:
        prospect_dict = prospect.to_dict()
        stage = prospect_dict['follow_up_stage']
        
        logging.info(f"Processing Stage {stage} follow-up for {prospect_dict['name']}...")

        # --- Strategy Alignment Check ---
        # Ensure the prospect's original proposed solution aligns with our current strategy.
        try:
            solutions = json.loads(prospect_dict.get('proposed_solutions', '[]'))
            primary_solution = solutions[0] if solutions else ""
            
            # This is the crucial check. Only proceed if the original pitch was about content/social media/brand.
            if "Content" not in primary_solution and "Social Media" not in primary_solution and "Brand" not in primary_solution and "Targeted Lead Generation" not in primary_solution:
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
        except (json.JSONDecodeError, IndexError):
            logging.warning(f"Skipping follow-up for {prospect_dict['name']} due to invalid 'proposed_solutions' format.")
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
            google_sheets_helpers.update_follow_up_status(
                service, 
                settings.SPREADSHEET_ID, 
                settings.GOOGLE_SHEET_NAME,
                prospect_name=prospect_dict['name'],
                stage=stage
            )
            send_count += 1
            time.sleep(5) # Respectful delay between sends

    logging.info(f"Successfully sent {send_count} follow-up emails.")
    logging.info("--- FOLLOW-UP CAMPAIGN COMPLETE ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Follow-up Email Campaign Job.")
    parser.add_argument("--limit", type=int, default=25, help="The maximum number of follow-up emails to send in this batch.")
    args = parser.parse_args()

    run_follow_up_campaign(daily_limit=args.limit) 
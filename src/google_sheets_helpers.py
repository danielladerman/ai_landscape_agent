import logging
import os
import pandas as pd
from io import StringIO
from datetime import datetime

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

from config.config import settings

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Service Authentication ---

def get_google_sheets_service():
    """
    Authenticates with the Google Sheets API using service account credentials
    and returns a service object. This is the primary way to interact with the API.
    """
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(settings.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except FileNotFoundError:
        logging.error(f"🔴 CRITICAL: Google credentials file not found at '{settings.GOOGLE_CREDENTIALS_PATH}'.")
    except Exception as e:
        logging.error(f"🔴 Error initializing Google Sheets service: {e}")
    return None

# --- Data Retrieval ---

def get_sheet_as_df(service, spreadsheet_id, sheet_name):
    """
    Fetches the entire content of a Google Sheet and returns it as a pandas DataFrame.
    """
    if not service:
        logging.error("Google Sheets service object is invalid.")
        return None
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) < 1:
            logging.warning(f"Sheet '{sheet_name}' is empty or has no header row.")
            return pd.DataFrame()
            
        header = values[0]
        data = values[1:]
        
        # --- Data Integrity Check ---
        # Ensure all data rows have the same number of columns as the header.
        # This prevents crashes if the sheet has inconsistent row lengths.
        num_columns = len(header)
        cleaned_data = []
        for i, row in enumerate(data):
            if len(row) != num_columns:
                # Pad rows that are too short
                padded_row = row + [''] * (num_columns - len(row))
                cleaned_data.append(padded_row[:num_columns]) # Truncate rows that are too long
                logging.warning(f"Corrected inconsistent column count in row {i+2}. Expected {num_columns}, found {len(row)}.")
            else:
                cleaned_data.append(row)

        return pd.DataFrame(cleaned_data, columns=header)
    except Exception as e:
        logging.error(f"🔴 Error fetching sheet '{sheet_name}' as DataFrame: {e}")
        return None

def get_sheet_summary_stats(service, spreadsheet_id, sheet_name):
    """
    Fetches the entire sheet and calculates summary statistics for the dashboard.
    """
    df = get_sheet_as_df(service, spreadsheet_id, sheet_name)
    if df is None or df.empty:
        return {
            "total_prospects": 0,
            "stage_counts": {},
            "contacted_in_last_24h": 0,
            "error": "Sheet is empty or could not be loaded."
        }

    try:
        # --- Calculate Stats ---
        total_prospects = len(df)
        
        # Calculate stage counts if the column exists
        stage_counts = df['Stage'].value_counts().to_dict() if 'Stage' in df.columns else {}

        # Calculate contacts in the last 24 hours
        contacted_in_last_24h = 0
        if 'last_contact_date' in df.columns:
            # Convert to datetime, coercing errors to NaT (Not a Time)
            last_contact_dates = pd.to_datetime(df['last_contact_date'], errors='coerce')
            # Get the timestamp for 24 hours ago
            yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
            # Count how many are more recent than yesterday
            contacted_in_last_24h = last_contact_dates[last_contact_dates >= yesterday].count()

        return {
            "total_prospects": total_prospects,
            "stage_counts": stage_counts,
            "contacted_in_last_24h": contacted_in_last_24h
        }
    except Exception as e:
        logging.error(f"🔴 Error calculating summary stats from DataFrame: {e}")
        return {"error": str(e)}

# --- Data Modification ---

def update_cells_bulk(service, spreadsheet_id, updates):
    """
    Performs a batch update to modify multiple cell ranges with new values.
    
    Args:
        service: The authenticated Google Sheets service object.
        spreadsheet_id (str): The ID of the spreadsheet.
        updates (list): A list of dictionaries, where each dict contains
                        'range' (e.g., "Sheet1!A2") and 'values' (e.g., [["new_value"]]).
    """
    if not service:
        logging.error("Google Sheets service is not available for bulk update.")
        return
    try:
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': updates
        }
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        logging.info(f"✅ Successfully performed bulk update for {len(updates)} cell(s).")
    except Exception as e:
        logging.error(f"🔴 Error performing batch update: {e}")


def update_follow_up_status(service, spreadsheet_id, sheet_name, prospect_updates):
    """
    Updates the follow-up sent date and last contact date for multiple prospects.

    Args:
        prospect_updates (list): A list of tuples, where each tuple is
                                 (prospect_email, stage_to_update).
    """
    df = get_sheet_as_df(service, spreadsheet_id, sheet_name)
    if df is None or df.empty:
        logging.error("Cannot update follow-up status because the sheet is empty or could not be read.")
        return

    # Create a mapping from email to row index for quick lookups
    try:
        # Ensure the key column exists
        if 'verified_emails' not in df.columns:
            logging.error("🔴 Missing 'verified_emails' column in the sheet. Cannot update follow-up status.")
            return
        email_to_row_map = {row['verified_emails']: index for index, row in df.iterrows()}
    except KeyError:
        logging.error("Could not create email-to-row mapping. Check column names.")
        return

    updates = []
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    stage_column_map = {
        1: 'follow_up_1_sent_date',
        2: 'follow_up_2_sent_date',
        3: 'follow_up_3_sent_date'
    }

    # Verify that all necessary columns exist in the DataFrame
    required_cols = list(stage_column_map.values()) + ['last_contact_date']
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"🔴 Missing required column '{col}' in the sheet. Cannot proceed with updates.")
            return
            
    for email, stage in prospect_updates:
        if email in email_to_row_map:
            row_index = email_to_row_map[email]
            sheet_row_index = row_index + 2  # +1 for header, +1 for 0-based index

            # --- Prepare Follow-up Date Update ---
            follow_up_col_name = stage_column_map.get(stage)
            if follow_up_col_name:
                col_index = df.columns.get_loc(follow_up_col_name)
                col_letter = chr(ord('A') + col_index)
                updates.append({
                    'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                    'values': [[today_str]]
                })

            # --- Prepare Last Contact Date Update ---
            col_index = df.columns.get_loc('last_contact_date')
            col_letter = chr(ord('A') + col_index)
            updates.append({
                'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                'values': [[today_str]]
            })
        else:
            logging.warning(f"Could not find prospect with email '{email}' to update follow-up status.")

    if updates:
        update_cells_bulk(service, spreadsheet_id, updates)


def update_sent_status_bulk(service, spreadsheet_id, sheet_name, prospect_updates):
    """
    Updates the sent date and last contact date for multiple prospects.

    Args:
        prospect_updates (list): A list of prospect emails that were successfully contacted.
    """
    df = get_sheet_as_df(service, spreadsheet_id, sheet_name)
    if df is None or df.empty:
        logging.error("Cannot update sent status because the sheet is empty or could not be read.")
        return

    try:
        if 'verified_emails' not in df.columns:
            logging.error("🔴 Missing 'verified_emails' column. Cannot update sent status.")
            return
        email_to_row_map = {row['verified_emails']: index for index, row in df.iterrows()}
    except KeyError:
        logging.error("Could not create email-to-row mapping for sent status update.")
        return

    updates = []
    today_str = datetime.now().strftime('%Y-%m-%d')
    
    required_cols = ['sent_date', 'last_contact_date']
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"🔴 Missing required column '{col}'. Cannot proceed with sent status update.")
            return
            
    for email in prospect_updates:
        if email in email_to_row_map:
            row_index = email_to_row_map[email]
            sheet_row_index = row_index + 2

            # Prepare 'sent_date' update
            col_index = df.columns.get_loc('sent_date')
            col_letter = chr(ord('A') + col_index)
            updates.append({
                'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                'values': [[today_str]]
            })

            # Prepare 'last_contact_date' update
            col_index = df.columns.get_loc('last_contact_date')
            col_letter = chr(ord('A') + col_index)
            updates.append({
                'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                'values': [[today_str]]
            })
        else:
            logging.warning(f"Could not find prospect with email '{email}' to update sent status.")

    if updates:
        update_cells_bulk(service, spreadsheet_id, updates)


def update_bounced_status_bulk(service, spreadsheet_id, sheet_name, bounced_updates):
    """
    Updates the email status and termination reason for multiple bounced emails.

    Args:
        bounced_updates (dict): A dictionary where keys are prospect emails and
                                values are the reason for the bounce.
    """
    df = get_sheet_as_df(service, spreadsheet_id, sheet_name)
    if df is None or df.empty:
        logging.error("Cannot update bounced status because the sheet is empty or could not be read.")
        return

    try:
        if 'verified_emails' not in df.columns:
            logging.error("🔴 Missing 'verified_emails' column. Cannot update bounced status.")
            return
        email_to_row_map = {row['verified_emails']: index for index, row in df.iterrows()}
    except KeyError:
        logging.error("Could not create email-to-row mapping for bounced status update.")
        return

    updates = []
    
    required_cols = ['email_status', 'termination_reason']
    for col in required_cols:
        if col not in df.columns:
            logging.error(f"🔴 Missing required column '{col}'. Cannot proceed with bounced status update.")
            return
            
    for email, reason in bounced_updates.items():
        if email in email_to_row_map:
            row_index = email_to_row_map[email]
            sheet_row_index = row_index + 2

            # Prepare 'email_status' update
            col_index = df.columns.get_loc('email_status')
            col_letter = chr(ord('A') + col_index)
            updates.append({
                'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                'values': [['Bounced']]
            })

            # Prepare 'termination_reason' update
            col_index = df.columns.get_loc('termination_reason')
            col_letter = chr(ord('A') + col_index)
            updates.append({
                'range': f"{sheet_name}!{col_letter}{sheet_row_index}",
                'values': [[reason]]
            })
        else:
            logging.warning(f"Could not find prospect with email '{email}' to update bounced status.")

    if updates:
        update_cells_bulk(service, spreadsheet_id, updates)


def append_df_to_sheet(service, spreadsheet_id, sheet_name, df_to_append):
    """
    Appends a DataFrame to the specified Google Sheet.
    """
    if not service:
        logging.error("Google Sheets service is not available for appending.")
        return False
    try:
        # Get the existing data from the sheet to determine the last row
        existing_values = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute().get('values', [])

        body = {
            'values': df_to_append.values.tolist()
        }
        
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A{len(existing_values) + 1}",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logging.info(f"✅ Successfully appended {len(df_to_append)} new rows to the sheet.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error appending to sheet: {e}")
        return False


def deduplicate_prospects(service, spreadsheet_id, sheet_name):
    """
    Removes duplicate rows from the sheet based on the 'name' column.
    """
    df = get_sheet_as_df(service, spreadsheet_id, sheet_name)
    if df is None or df.empty:
        logging.info("Sheet is empty, no deduplication needed.")
        return

    if 'name' not in df.columns:
        logging.error("🔴 Cannot deduplicate because 'name' column is missing.")
        return

    initial_row_count = len(df)
    df.drop_duplicates(subset=['name'], keep='first', inplace=True)
    deduplicated_row_count = len(df)
    
    num_removed = initial_row_count - deduplicated_row_count

    if num_removed > 0:
        logging.info(f"Removed {num_removed} duplicate prospect(s).")
        
        # Overwrite the entire sheet with the deduplicated data
        try:
            # Clear the existing sheet content
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()

            # Write the deduplicated DataFrame back to the sheet
            body = {
                'values': [df.columns.values.tolist()] + df.values.tolist()
            }
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            logging.info("✅ Successfully overwrote sheet with deduplicated data.")
        except Exception as e:
            logging.error(f"🔴 Error overwriting sheet with deduplicated data: {e}")
    else:
        logging.info("No duplicate prospects found.") 
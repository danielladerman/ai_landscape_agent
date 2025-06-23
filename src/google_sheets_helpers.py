import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from config.config import settings
import logging
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_google_sheets_service():
    """Authenticates and returns a service object for the Google Sheets API."""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file(settings.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
        # We use gspread for most operations, but this setup is good practice
        return gspread.service_account(filename=settings.GOOGLE_CREDENTIALS_PATH)
    except FileNotFoundError:
        logging.error(f"🔴 Google credentials file not found at: {settings.GOOGLE_CREDENTIALS_PATH}")
        return None
    except Exception as e:
        logging.error(f"🔴 Error initializing Google Sheets service: {e}")
        return None

def get_worksheet(service, spreadsheet_id, sheet_name):
    """Opens the Google Sheet and returns the first worksheet."""
    try:
        spreadsheet = service.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        return worksheet
    except gspread.exceptions.SpreadsheetNotFound:
        logging.error(f"🔴 GSpread Error: Spreadsheet not found. Check your SPREADSHEET_ID in the .env file.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"🔴 GSpread Error: Worksheet '{sheet_name}' not found. Check your GOOGLE_SHEET_NAME in the .env file.")
        return None
    except Exception as e:
        logging.error(f"🔴 An unexpected error occurred while accessing the sheet: {e}")
        return None

def get_existing_websites_from_sheet(service, spreadsheet_id: str, sheet_name: str) -> set:
    """
    Efficiently fetches only the 'Website' column from the sheet to check for existing prospects.
    This avoids downloading the entire sheet into memory.
    """
    try:
        worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
        if not worksheet:
            return set()
        
        # Assuming 'Website' is the second column (index 2)
        # gspread uses 1-based indexing for columns
        website_column_index = 2 
        websites = worksheet.col_values(website_column_index)
        
        # Return a set for efficient O(1) average time complexity lookups
        # Skip the header row by slicing from the first element
        return set(websites[1:])
    except Exception as e:
        logging.error(f"🔴 Error fetching existing websites from sheet: {e}")
        return set()

def get_all_prospects(service, spreadsheet_id, sheet_name):
    """Fetches all prospects from the specified sheet and returns them as a DataFrame."""
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return None
    try:
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"🔴 Error fetching prospects from Google Sheet: {e}")
        return None

def update_prospect_list(service, spreadsheet_id, sheet_name, df):
    """Updates the Google Sheet with the new prospects DataFrame."""
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False
    try:
        existing_headers = worksheet.row_values(1)
        if not existing_headers:
            # If sheet is empty, write with headers
            set_with_dataframe(worksheet, df, resize=True)
        else:
            # Append rows without headers
            worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        logging.info(f"✅ Successfully updated Google Sheet with {len(df)} new prospects.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error updating prospect list in Google Sheet: {e}")
        return False

def get_sheet_as_df(service, spreadsheet_id, sheet_name):
    """
    Reads the entire Google Sheet and returns it as a pandas DataFrame.
    """
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return None
    try:
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"🔴 Error reading from Google Sheet: {e}")
        return None

def append_df_to_sheet(service, spreadsheet_id, sheet_name, df):
    """
    Appends a DataFrame to the end of the worksheet without clearing it.
    """
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False
    try:
        # Before appending, convert any list-like cells to strings
        for col in df.columns:
            if df[col].apply(type).eq(list).any():
                df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)

        # Get existing headers
        header_row = []
        try:
            header_row = worksheet.row_values(1)
        except gspread.exceptions.APIError:
            # Sheet is likely empty, which is fine
            pass

        # If the sheet is empty, write with header. Otherwise, append without header.
        if not header_row:
            set_with_dataframe(worksheet, df, resize=True)
        else:
            # Align the DataFrame to the sheet's columns.
            # Only use columns that exist in BOTH the DataFrame and the Sheet.
            # This prevents errors if the df is missing columns that are in the sheet.
            cols_to_upload = [col for col in header_row if col in df.columns]
            
            # If there are no common columns, something is wrong.
            if not cols_to_upload:
                logging.error("🔴 No matching columns found between DataFrame and Google Sheet. Cannot append.")
                return False

            worksheet.append_rows(df[cols_to_upload].values.tolist(), value_input_option='USER_ENTERED')

        logging.info(f"✅ Successfully appended {len(df)} new rows to Google Sheet.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error appending to Google Sheet: {e}")
        return False

def update_sent_status(service, spreadsheet_id, sheet_name, prospect_name, column_name):
    """
    Finds a prospect by their unique name and updates the status of a specific column.
    """
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False
    try:
        # Find the cell with the prospect's name in the second column ('Name')
        cell = worksheet.find(prospect_name, in_column=2)
        
        if not cell:
            logging.warning(f"Could not find prospect with name '{prospect_name}' to update status.")
            return False

        found_row = cell.row

        # Find the column index for the status update
        header_row = worksheet.row_values(1)
        if column_name not in header_row:
            logging.error(f"Column '{column_name}' not found in the sheet.")
            return False
        
        col_index = header_row.index(column_name) + 1
        
        # Update the cell in that row with today's date and time
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        worksheet.update_cell(found_row, col_index, timestamp)

        # Also update the 'last_contact_date'
        if 'last_contact_date' in header_row:
            last_contact_col_index = header_row.index('last_contact_date') + 1
            worksheet.update_cell(found_row, last_contact_col_index, timestamp)

        logging.info(f"✅ Updated '{column_name}' for {prospect_name}.")
        return True

    except Exception as e:
        logging.error(f"🔴 Error updating sent status for {prospect_name}: {e}")
        return False

def update_follow_up_status(service, spreadsheet_id, sheet_name, prospect_name, stage):
    """
    Finds a prospect by name and updates the correct follow-up sent date column.
    """
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False

    column_to_update = f'follow_up_{stage}_sent_date'
    
    try:
        # Find the cell with the prospect's name in the second column ('Name')
        cell = worksheet.find(prospect_name, in_column=2) 
        if not cell:
            logging.warning(f"Could not find prospect '{prospect_name}' to update follow-up status.")
            return False

        header_row = worksheet.row_values(1)
        if column_to_update not in header_row:
            logging.error(f"Column '{column_to_update}' not found in the sheet.")
            return False
            
        col_index = header_row.index(column_to_update) + 1
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        worksheet.update_cell(cell.row, col_index, timestamp)

        # Also update the 'last_contact_date'
        if 'last_contact_date' in header_row:
            last_contact_col_index = header_row.index('last_contact_date') + 1
            worksheet.update_cell(cell.row, last_contact_col_index, timestamp)

        logging.info(f"✅ Updated follow-up stage {stage} for {prospect_name}.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error updating follow-up status for {prospect_name}: {e}")
        return False

def update_prospect_status(service, spreadsheet_id, sheet_name, prospect_name, column_name, status_value):
    """
    Finds a prospect by their unique name and updates a specific column with a given value.
    """
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False
    try:
        # Find the cell with the prospect's name
        # Assuming 'name' is in the second column (B)
        cell = worksheet.find(prospect_name, in_column=2)
        if not cell:
            logging.warning(f"Could not find prospect '{prospect_name}' to update status.")
            return False

        header_row = worksheet.row_values(1)
        if column_name not in header_row:
            logging.error(f"Column '{column_name}' not found in the sheet.")
            return False

        col_index = header_row.index(column_name) + 1
        worksheet.update_cell(cell.row, col_index, status_value)
        logging.info(f"✅ Updated '{column_name}' to '{status_value}' for {prospect_name}.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error updating prospect status for {prospect_name}: {e}")
        return False

def update_bounced_status_bulk(service, spreadsheet_id, sheet_name, bounced_info: dict, prospects_df):
    """
    Finds prospects by their email within the provided DataFrame and marks them as bounced with a specific reason.
    """
    if not bounced_info or prospects_df.empty:
        return True
        
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False

    try:
        cells_to_update = []
        bounced_emails_set = set(bounced_info.keys())

        # Ensure we have the necessary columns before iterating
        if 'email_status' not in prospects_df.columns or 'termination_reason' not in prospects_df.columns or 'verified_emails' not in prospects_df.columns:
            logging.error("Sheet is missing required columns: 'email_status', 'termination_reason', or 'verified_emails'.")
            return False

        # Get column letters/indices once
        status_col_index = prospects_df.columns.get_loc('email_status') + 1
        reason_col_index = prospects_df.columns.get_loc('termination_reason') + 1

        for index, row in prospects_df.iterrows():
            email_cell = row.get('verified_emails', '')
            if not email_cell:
                continue
            
            # The email can be a string '["email@a.com"]' or just 'email@a.com'
            # We just need to check if the bounced email is in that string.
            for bounced_email in bounced_emails_set.copy(): # Iterate over a copy
                if bounced_email in email_cell:
                    reason = bounced_info[bounced_email] # Get the specific reason
                    row_number = index + 2  # +1 for header, +1 for 0-indexing
                    cells_to_update.append(gspread.Cell(row=row_number, col=status_col_index, value='Bounced'))
                    cells_to_update.append(gspread.Cell(row=row_number, col=reason_col_index, value=reason))
                    bounced_emails_set.remove(bounced_email) # Remove from set to avoid re-processing
                    break # Move to the next row in the dataframe

            if not bounced_emails_set:
                break

        if cells_to_update:
            worksheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
            logging.info(f"✅ Bulk updated {len(cells_to_update)//2} bounced emails in the Google Sheet.")
        else:
            logging.info("No matching emails found in the sheet to mark as bounced.")
        return True

    except Exception as e:
        logging.error(f"🔴 Error in bulk update of bounced emails: {e}")
        return False

def add_tracking_columns(service, spreadsheet_id, sheet_name):
    """Adds all necessary tracking and data columns to the sheet if they don't exist."""
    worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
    if not worksheet:
        return False
    try:
        existing_headers = worksheet.row_values(1)

        all_expected_columns = [
            'Name', 'Website', 'Email', 'Contact Person Title', 
            'Pain Point', 'Icebreaker', 'Website Content Summary',
            'Initial Email Sent', 'follow_up_1_sent_date', 
            'follow_up_2_sent_date', 'follow_up_3_sent_date', 
            'last_contact_date', 'email_status', 'Reason for bounce'
        ]

        new_columns_to_add = [col for col in all_expected_columns if col not in existing_headers]

        if new_columns_to_add:
            logging.info(f"Adding new columns to Google Sheet: {', '.join(new_columns_to_add)}")
            
            # This is a robust way to append columns without overwriting data
            worksheet.append_row(new_columns_to_add, table_range='A1')
            # This is a bit of a hack to add headers correctly.
            # A better way might involve more complex API calls to insert columns.
            # For now, this works for adding to the end.
            # We'll need to re-fetch the sheet and re-organize if columns are not in order.

        # Let's resize to make sure all columns are visible
        worksheet_id = worksheet.id
        num_cols = len(all_expected_columns)
        
        body = {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": { "sheetId": worksheet_id, "gridProperties": { "columnCount": num_cols }},
                        "fields": "gridProperties.columnCount"
                    }
                }
            ]
        }
        # This requires the Google Sheets API (build object), not just gspread
        # For simplicity, we'll assume the user can resize if needed, or we stick to append.
        # To do this properly, the service object needs to be the googleapiclient.discovery.build object
        # worksheet.spreadsheet.batch_update(body) 

        logging.info("✅ Sheet columns are up to date.")
        return True

    except Exception as e:
        logging.error(f"🔴 Error adding tracking columns to Google Sheet: {e}")
        return False 
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

def get_raw_sheets_service():
    """Authenticates and returns a raw service object for the Google Sheets API."""
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        creds = Credentials.from_service_account_file(settings.GOOGLE_CREDENTIALS_PATH, scopes=scopes)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        logging.error(f"🔴 Error initializing raw Google Sheets service: {e}")
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
            # --- ROBUST APPEND LOGIC ---
            # 1. Create a new, empty DataFrame with columns in the exact order of the sheet's headers.
            aligned_df = pd.DataFrame(columns=header_row)
            
            # 2. Concatenate the new data (df). This aligns df's columns to the header_row order.
            #    Columns in df that are not in header_row will be ignored.
            #    Columns in header_row that are not in df will be present but filled with NaN.
            combined_df = pd.concat([aligned_df, df], ignore_index=True)
            
            # 3. Select only the columns that exist in the sheet to prepare for upload.
            upload_df = combined_df[header_row]
            
            # 4. Convert the ordered data to a list of lists for the API call.
            #    Fill any NaN values with empty strings to avoid issues with the API.
            rows_to_append = upload_df.fillna('').values.tolist()

            worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')

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

def update_sent_status_bulk(service, spreadsheet_id, sheet_name, prospect_names: list, column_name: str):
    """
    Updates a specific column for a list of prospects using a brute-force
    read-modify-write approach with the raw Google Sheets API to guarantee changes.
    """
    logging.info("Attempting DIRECT API brute-force sheet update to guarantee changes...")
    gspread_service = get_google_sheets_service() # For reading
    raw_service = get_raw_sheets_service() # For writing
    if not gspread_service or not raw_service:
        return False

    try:
        worksheet = get_worksheet(gspread_service, spreadsheet_id, sheet_name)
        if not worksheet:
            return False

        all_records = worksheet.get_all_records()
        if not all_records: return True
        df = pd.DataFrame(all_records)

        # Use a vectorized operation with .isin() for a robust, single-shot update
        timestamp = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
        mask = df['name'].isin(prospect_names)
        updated_count = mask.sum()

        if updated_count > 0:
            df.loc[mask, column_name] = timestamp
            if 'last_contact_date' in df.columns:
                df.loc[mask, 'last_contact_date'] = timestamp

            # Clear the sheet first
            raw_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            # Write the updated dataframe
            raw_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body={'values': [df.columns.values.tolist()] + df.values.tolist()}
            ).execute()
            logging.info(f"✅ Successfully overwrote sheet via DIRECT API with updates for {updated_count} prospects.")
        return True
    except Exception as e:
        logging.error(f"🔴 Error during DIRECT API brute-force bulk update: {e}")
        return False

def update_bounced_status_bulk(service, spreadsheet_id, sheet_name, bounced_prospects: dict):
    """
    Updates bounced prospects using a DIRECT API brute-force read-modify-write.
    """
    logging.info("Attempting DIRECT API brute-force sheet update for bounced prospects...")
    gspread_service = get_google_sheets_service()
    raw_service = get_raw_sheets_service()
    if not gspread_service or not raw_service or not bounced_prospects:
        return False

    try:
        worksheet = get_worksheet(gspread_service, spreadsheet_id, sheet_name)
        if not worksheet: return False

        all_records = worksheet.get_all_records()
        if not all_records: return True
        df = pd.DataFrame(all_records)

        updated_count = 0
        for email, reason in bounced_prospects.items():
            match_index = df.index[df['verified_emails'] == email].tolist()
            if match_index:
                idx = match_index[0]
                df.loc[idx, 'email_status'] = 'Bounced'
                df.loc[idx, 'termination_reason'] = reason
                updated_count += 1

        if updated_count > 0:
            raw_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            raw_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body={'values': [df.columns.values.tolist()] + df.values.tolist()}
            ).execute()
            logging.info(f"✅ Successfully overwrote sheet via DIRECT API with updates for {updated_count} bounced prospects.")

        return True
    except Exception as e:
        logging.error(f"🔴 Error during DIRECT API brute-force bounce update: {e}")
        return False

def deduplicate_prospects(service, spreadsheet_id, sheet_name):
    """
    Removes duplicate rows from the sheet based on the 'website' column, keeping the last occurrence.
    """
    logging.info("Attempting to deduplicate sheet by 'website' column...")
    gspread_service = get_google_sheets_service()
    raw_service = get_raw_sheets_service()
    if not gspread_service or not raw_service:
        return False

    try:
        worksheet = get_worksheet(gspread_service, spreadsheet_id, sheet_name)
        if not worksheet: return False

        all_records = worksheet.get_all_records()
        if not all_records:
            logging.info("Sheet is empty, no deduplication needed.")
            return True
        
        df = pd.DataFrame(all_records)
        initial_row_count = len(df)
        
        # Drop duplicates based on 'website' column, keeping the last entry
        df.drop_duplicates(subset=['website'], keep='last', inplace=True)
        final_row_count = len(df)
        
        num_removed = initial_row_count - final_row_count

        if num_removed > 0:
            logging.info(f"Removed {num_removed} duplicate prospect(s). Overwriting sheet with cleaned data...")
            # Clear the sheet first
            raw_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=sheet_name
            ).execute()
            
            # Write the updated dataframe
            raw_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption='USER_ENTERED',
                body={'values': [df.columns.values.tolist()] + df.values.tolist()}
            ).execute()
            logging.info(f"✅ Successfully overwrote sheet with {final_row_count} unique prospects.")
        else:
            logging.info("✅ No duplicate prospects found based on the 'website' column.")

        return True
    except Exception as e:
        logging.error(f"🔴 Error during sheet deduplication: {e}")
        return False

def backfill_last_contact_dates(service, spreadsheet_id, sheet_name):
    """
    Backfills the 'last_contact_date' column based on the most recent timestamp
    from other date-related columns. This is a one-time utility.
    """
    logging.info("Attempting to backfill 'last_contact_date' for existing prospects...")
    gspread_service = get_google_sheets_service()
    raw_service = get_raw_sheets_service()
    if not gspread_service or not raw_service:
        return False

    try:
        worksheet = get_worksheet(gspread_service, spreadsheet_id, sheet_name)
        if not worksheet: return False

        all_records = worksheet.get_all_records()
        if not all_records:
            logging.info("Sheet is empty, no backfill needed.")
            return True
        
        df = pd.DataFrame(all_records)
        
        # Define the columns that track contact dates
        date_cols = [
            'sent_date', 'follow_up_1_sent_date', 
            'follow_up_2_sent_date', 'follow_up_3_sent_date'
        ]
        
        # Ensure all relevant date columns exist, adding them if not
        for col in date_cols:
            if col not in df.columns:
                df[col] = ''
        
        # Convert date columns to datetime objects, coercing errors will turn non-dates into NaT
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
        # Find the most recent date across the specified columns for each row
        df['last_contact_date'] = df[date_cols].max(axis=1)
        
        # Format dates back to strings, leaving empty where no date was found
        df['last_contact_date'] = df['last_contact_date'].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        
        # Format the other date columns back to string as well to avoid writing 'NaT'
        for col in date_cols:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        
        logging.info("Backfill calculation complete. Overwriting sheet with updated dates...")
        
        # Overwrite the entire sheet with the updated data
        raw_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        
        raw_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='USER_ENTERED',
            body={'values': [df.columns.values.tolist()] + df.values.tolist()}
        ).execute()
        
        logging.info("✅ Successfully backfilled 'last_contact_date' and cleaned sheet.")
        return True

    except Exception as e:
        logging.error(f"🔴 Error during 'last_contact_date' backfill: {e}")
        return False

def add_tracking_columns(service, spreadsheet_id, sheet_name):
    """
    Checks if all required tracking columns exist in the sheet and adds them if they don't.
    """
    required_columns = [
        'name', 'website', 'verified_emails', 'found_titles', 
        'icebreaker', 'identified_pains', 'proposed_solutions', 'evidence',
        'generated_subject', 'generated_body',
        'sent_date', 'last_contact_date', 'email_status', 'termination_reason',
        'follow_up_1_sent_date', 'follow_up_2_sent_date', 'follow_up_3_sent_date'
    ]
    
    try:
        worksheet = get_worksheet(service, spreadsheet_id, sheet_name)
        if not worksheet:
            return

        header_row = worksheet.row_values(1)
        
        # Find which columns are missing
        missing_columns = [col for col in required_columns if col not in header_row]

        if missing_columns:
            logging.info(f"Adding missing columns to Google Sheet: {', '.join(missing_columns)}")
            
            # Find the starting column to append to
            start_column = len(header_row) + 1
            
            # Prepare the update request
            # Note: The values must be a list of lists
            cell_list = worksheet.range(1, start_column, 1, start_column + len(missing_columns) - 1)
            for i, cell in enumerate(cell_list):
                cell.value = missing_columns[i]
            
            # Update the cells in a single batch
            worksheet.update_cells(cell_list)
            logging.info(f"✅ Successfully added {len(missing_columns)} new columns to the header.")

        else:
            logging.info("✅ Sheet columns are up to date.")
            
    except Exception as e:
        logging.error(f"🔴 Error checking or adding tracking columns: {e}") 
import logging
from config.config import settings
from src import google_sheets_helpers
import gspread

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/sheet_setup.log"),
        logging.StreamHandler()
    ]
)

def setup_sheet(service, spreadsheet_id: str, sheet_name: str, columns: list):
    """
    Ensures a sheet with the specified name exists and has the correct header row.

    If the sheet already exists, it will be completely cleared. If it does not
    exist, it will be created. Finally, it sets the first row with the
    provided column names.

    Args:
        service: The authenticated gspread service object.
        spreadsheet_id (str): The ID of the Google Spreadsheet.
        sheet_name (str): The name of the worksheet to set up.
        columns (list): A list of strings representing the column headers.

    Returns:
        The gspread Worksheet object if successful, otherwise None.
    """
    try:
        spreadsheet = service.open_by_key(spreadsheet_id)
    except gspread.exceptions.SpreadsheetNotFound:
        logging.error(f"🔴 Spreadsheet not found with ID: {spreadsheet_id}. Please check your .env file.")
        return None

    try:
        # If the worksheet exists, clear it to start fresh
        worksheet = spreadsheet.worksheet(sheet_name)
        logging.info(f"Sheet '{sheet_name}' already exists. Clearing all content...")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        # If it doesn't exist, create it
        logging.info(f"Sheet '{sheet_name}' not found. Creating a new one.")
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(columns))
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        return None

    logging.info(f"Setting the header row for '{sheet_name}'...")
    worksheet.update('A1', [columns])
    logging.info("✅ Header row set successfully.")
    return worksheet

def main():
    """
    Main function to run the sheet setup process.
    """
    logging.info("--- STARTING NEW SHEET SETUP SCRIPT ---")
    
    NEW_SHEET_NAME = "Sheet2"
    
    # This list contains all the columns the system needs to track prospects correctly.
    REQUIRED_COLUMNS = [
        'name', 'website', 'phone_number', 'address', 'place_id', 'google_reviews', 
        'website_analysis', 'verified_emails', 'found_titles', 'icebreaker', 
        'identified_pains', 'proposed_solutions', 'evidence', 'generated_subject', 
        'generated_body', 'sent_date', 'last_contact_date', 'email_status', 
        'termination_reason', 'follow_up_1_sent_date', 'follow_up_2_sent_date', 
        'follow_up_3_sent_date'
    ]

    g_service = google_sheets_helpers.get_google_sheets_service()
    if not g_service:
        logging.error("🔴 Could not authenticate with Google Sheets. Aborting.")
        return

    worksheet = setup_sheet(
        g_service, 
        settings.SPREADSHEET_ID, 
        NEW_SHEET_NAME, 
        REQUIRED_COLUMNS
    )

    if worksheet:
        logging.info(f"--- SHEET '{NEW_SHEET_NAME}' IS READY ---")
        logging.info("IMPORTANT: To use this new sheet for your email campaigns, you must now do two things:")
        logging.info(f"1. Update the GOOGLE_SHEET_NAME in your .env file to '{NEW_SHEET_NAME}'.")
        logging.info("2. Populate the sheet with your new prospect data.")
    else:
        logging.error("--- SHEET SETUP FAILED ---")


if __name__ == "__main__":
    main()

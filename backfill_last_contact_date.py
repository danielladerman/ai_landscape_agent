import logging
from src import google_sheets_helpers
from config.config import settings

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/backfill_dates.log"),
        logging.StreamHandler()
    ]
)

def main():
    """
    Main function to orchestrate the backfilling of the 'last_contact_date' column.
    """
    logging.info("--- STARTING 'last_contact_date' BACKFILL SCRIPT ---")
    
    g_service = google_sheets_helpers.get_google_sheets_service()
    if not g_service:
        logging.error("Could not authenticate with Google Sheets. Aborting.")
        return

    success = google_sheets_helpers.backfill_last_contact_dates(
        g_service,
        settings.SPREADSHEET_ID,
        settings.GOOGLE_SHEET_NAME
    )

    if success:
        logging.info("--- BACKFILL PROCESS COMPLETED SUCCESSFULLY ---")
    else:
        logging.error("--- BACKFILL PROCESS FAILED ---")

if __name__ == "__main__":
    main() 
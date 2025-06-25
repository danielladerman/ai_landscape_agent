import logging
from src import google_sheets_helpers
from config.config import settings

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/deduplication.log"),
        logging.StreamHandler()
    ]
)

def main():
    """
    Main function to orchestrate the deduplication of the Google Sheet.
    """
    logging.info("--- STARTING GOOGLE SHEET DEDUPLICATION ---")
    
    g_service = google_sheets_helpers.get_google_sheets_service()
    if not g_service:
        logging.error("Could not authenticate with Google Sheets. Aborting.")
        return

    success = google_sheets_helpers.deduplicate_prospects(
        g_service,
        settings.SPREADSHEET_ID,
        settings.GOOGLE_SHEET_NAME
    )

    if success:
        logging.info("--- DEDUPLICATION PROCESS COMPLETE ---")
    else:
        logging.error("--- DEDUPLICATION PROCESS FAILED ---")

if __name__ == "__main__":
    main() 
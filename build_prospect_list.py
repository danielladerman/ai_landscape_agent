import pandas as pd
import concurrent.futures
import logging
import argparse
import json
import time

from config.config import settings
from src.lead_generation import google_maps_finder
from src.website_analysis import contact_finder, content_analyzer
from src.verification import email_verifier
from src.review_analysis import review_analyzer
from src.pain_analysis import pain_point_detector
from src.email_generation import email_generator
from src.google_sheets_helpers import get_google_sheets_service, get_sheet_as_df, append_df_to_sheet, add_tracking_columns, get_existing_websites_from_sheet

# --- Setup ---
# Configure logging to write to a file in the 'logs' directory
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/build_prospects.log"),
        logging.StreamHandler()
    ]
)

def analyze_prospect(business: dict) -> dict:
    """
    Takes a business dictionary and performs all analysis steps.
    This is a helper function for concurrent processing.
    """
    website = business.get('website')
    if not website:
        return None

    # --- Analysis Phase ---
    # Each of these steps enriches the original business dictionary.
    try:
        # 1. Get Reviews and store
        reviews = review_analyzer.get_google_reviews(business.get('place_id'))
        business['google_reviews'] = json.dumps(reviews)

        # 2. Analyze Website Content and store
        content_analysis = content_analyzer.analyze_website_content(website)
        business['website_analysis'] = json.dumps(content_analysis)
        
        # 3. Identify Pain Points based on all data
        pain_results = pain_point_detector.analyze_pain_points(
            business['google_reviews'],
            business['website_analysis'],
        )
        business.update(pain_results) # Adds 'icebreaker', 'identified_pains', etc.

        # 4. Generate Email
        email_content = email_generator.generate_personalized_email(
            business_name=business.get('name'),
            titles=business.get('found_titles'), # Use the correct key
            icebreaker=business.get('icebreaker'),
            pains=json.dumps(business.get('identified_pains', [])),
            solutions=json.dumps(business.get('proposed_solutions', [])),
            evidence=json.dumps(business.get('evidence', []))
        )
        if email_content:
            business['generated_subject'] = email_content.get('subject')
            business['generated_body'] = email_content.get('body')
            return business

    except Exception as e:
        logging.error(f"Error analyzing business '{business.get('name')}': {e}")
    
    return None


def build_prospect_list(query: str, max_leads: int = 100, max_workers: int = 10):
    """
    Main orchestrator function to build a list of prospects.
    """
    logging.info(f"--- STARTING NEW PROSPECT BUILD: Query='{query}', Max Leads={max_leads}, Workers={max_workers} ---")

    # --- PHASE 1: Setup & Initial Data Loading ---
    service = get_google_sheets_service()
    if not service:
        logging.error("🔴 Failed to initialize Google Sheets service. Aborting.")
        return

    add_tracking_columns(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)
    
    # Efficiently get existing websites to avoid duplicates
    existing_websites = get_existing_websites_from_sheet(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME)
    logging.info(f"Found {len(existing_websites)} existing prospects in the tracker.")

    # --- PHASE 2: Lead Generation & Contact Finding ---
    logging.info("--- Finding new businesses via Google Maps... ---")
    gmaps = google_maps_finder.GoogleMapsFinder(api_key=settings.GOOGLE_MAPS_API_KEY)
    businesses = gmaps.find_businesses(query, max_results=max_leads)
    
    new_businesses = [b for b in businesses if b.get('website') and b.get('website') not in existing_websites]
    logging.info(f"Found {len(new_businesses)} new businesses to process.")

    if not new_businesses:
        logging.info("--- No new businesses found. Process complete. ---")
        return

    logging.info(f"--- Finding and verifying contacts for {len(new_businesses)} businesses... ---")
    prospects_to_analyze = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_business = {executor.submit(contact_finder.find_contacts, b['website']): b for b in new_businesses}
        for future in concurrent.futures.as_completed(future_to_business):
            business = future_to_business[future]
            try:
                contact_info = future.result()
                if contact_info and contact_info.get('emails'):
                    # Directly call the verification function
                    verified_emails = email_verifier.verify_emails_bulk(contact_info['emails'])
                    if verified_emails:
                        prospect = business.copy()
                        prospect['verified_emails'] = verified_emails[0]
                        prospect['found_titles'] = ', '.join(contact_info.get('titles', []))
                        prospects_to_analyze.append(prospect)
            except Exception as e:
                logging.error(f"Error processing contacts for {business.get('name')}: {e}")

    logging.info(f"--- Found {len(prospects_to_analyze)} prospects with valid emails. Starting analysis. ---")
    
    # --- PHASE 3: Concurrent Analysis & Email Generation ---
    final_prospects_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prospect = {executor.submit(analyze_prospect, p): p for p in prospects_to_analyze}
        for future in concurrent.futures.as_completed(future_to_prospect):
            try:
                result = future.result()
                if result:
                    # The 'sent_date' will be added by the sending script, not here.
                    final_prospects_data.append(result)
                time.sleep(1) # Add a 1-second delay to avoid rate limiting
            except Exception as e:
                logging.error(f"Error in main analysis pipeline: {e}")

    if not final_prospects_data:
        logging.warning("--- No prospects remained after full analysis and email generation. ---")
        return

    # --- FINAL STEP: Saving to Google Sheet ---
    logging.info(f"--- Appending {len(final_prospects_data)} new prospects to Google Sheet ---")
    
    upload_df = pd.DataFrame(final_prospects_data)

    success = append_df_to_sheet(service, settings.SPREADSHEET_ID, settings.GOOGLE_SHEET_NAME, upload_df)

    if success:
        logging.info("✅ --- Prospect build process completed successfully! ---")
    else:
        logging.error("🔴 --- Failed to save prospects to Google Sheet. ---")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Build a list of prospects with personalized outreach emails.")
    parser.add_argument("query", type=str, help="The search query for Google Maps (e.g., 'landscaping in San Diego')")
    parser.add_argument("--max_leads", type=int, default=100, help="Maximum number of leads to process for the list.")
    parser.add_argument("--max_workers", type=int, default=10, help="Maximum number of parallel threads to use for processing.")
    args = parser.parse_args()

    build_prospect_list(
        query=args.query,
        max_leads=args.max_leads,
        max_workers=args.max_workers
    ) 
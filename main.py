import pandas as pd
import os
from config import config
from src.lead_generation import google_maps_finder
from src.website_analysis import contact_finder
from src.verification import email_verifier

def main():
    """
    Main function to run the AI-Powered Hyper-Personalized Outreach System.
    """
    print(f"Starting {config.PROJECT_NAME}")

    # --- Phase 2.1: Lead Identification from Google Maps ---
    print("\n--- Running Phase 2.1: Lead Identification ---")
    
    # Check for API Key
    if not config.GOOGLE_MAPS_API_KEY or config.GOOGLE_MAPS_API_KEY == "your_google_maps_api_key_here":
        print("🔴 Error: Google Maps API key is not configured.")
        print("Please set GOOGLE_MAPS_API_KEY in your .env file.")
        return

    # Define search query and run the finder
    search_query = "landscaping services in San Diego, CA"
    businesses = google_maps_finder.find_landscaping_businesses(query=search_query, max_results=5) # Reduced for faster testing

    if not businesses:
        print("🔴 No businesses found or an error occurred during Google Maps search.")
        return
        
    print(f"\n✅ Successfully found {len(businesses)} businesses from Google Maps.")
    
    # Convert to DataFrame
    df = pd.DataFrame(businesses)
    
    # --- Phase 2.2: Website Contact Scraping & Verification ---
    print("\n--- Running Phase 2.2: Contact Scraping & Verification ---")

    all_contacts = []
    for index, row in df.iterrows():
        print(f"\nAnalyzing: {row['name']} ({row['website']})")
        contacts = contact_finder.find_contacts_on_website(row['website'])
        
        verified_emails = []
        if contacts['emails']:
            print(f"  > Found {len(contacts['emails'])} potential emails. Verifying...")
            for email in contacts['emails']:
                # In a real scenario, you might add more filtering here
                # (e.g., ignore emails from common blocklists)
                verification_result = email_verifier.verify_email(email)
                if verification_result and verification_result['status'] == 'valid':
                    verified_emails.append(email)
                    print(f"    - ✅ Verified: {email}")
                else:
                    print(f"    - ❌ Invalid/Risky: {email}")
        
        all_contacts.append({
            "verified_emails": verified_emails,
            "found_titles": contacts['titles']
        })

    # Add verified contacts to the DataFrame
    df['verified_emails'] = [d['verified_emails'] for d in all_contacts]
    df['found_titles'] = [d['found_titles'] for d in all_contacts]

    # Filter out businesses where we found no verified emails
    df_final = df[df['verified_emails'].apply(lambda x: len(x) > 0)].reset_index(drop=True)

    if df_final.empty:
        print("\n🔴 No valid contact emails were found after scraping and verification.")
    else:
        output_path = "data/verified_leads.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_final.to_csv(output_path, index=False)
        print(f"\n✅ Complete! Found {len(df_final)} leads with verified emails.")
        print(f"Final lead list saved to {output_path}")


if __name__ == "__main__":
    main()

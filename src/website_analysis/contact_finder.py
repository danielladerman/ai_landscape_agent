import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse
from .utils import get_page_content # Use the shared utility function

# --- Helper Functions & Constants ---

# Regex to find email addresses
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

# Common keywords for senior-level roles
SENIOR_LEVEL_TITLES = [
    'owner', 'ceo', 'founder', 'president', 'managing director', 
    'marketing director', 'sales director', 'marketing manager'
]

# Common URL paths for contact/about pages
CONTACT_PAGE_PATHS = ['/contact', '/contact-us', '/about', '/about-us', '/team']

def _clean_email(email_string: str) -> str:
    """Extracts a clean email address from a raw string using regex."""
    match = re.search(EMAIL_REGEX, email_string)
    if match:
        return match.group(0).lower()
    return None

def _parse_for_contacts(soup: BeautifulSoup):
    """Parses a BeautifulSoup object to find emails and job titles."""
    found_emails = set()
    found_titles = set()

    # Find all email addresses in the body text
    emails_in_body = re.findall(EMAIL_REGEX, soup.get_text())
    for email in emails_in_body:
        cleaned = _clean_email(email)
        if cleaned:
            found_emails.add(cleaned)
    
    # Also check mailto links
    for a in soup.find_all('a', href=True):
        if a['href'].startswith('mailto:'):
            # The raw string could be 'mailto:info@example.com?subject=...'
            raw_email = a['href'][7:]
            cleaned = _clean_email(raw_email)
            if cleaned:
                found_emails.add(cleaned)

    # Search for job titles in the page text
    page_text_lower = soup.get_text().lower()
    for title in SENIOR_LEVEL_TITLES:
        if title in page_text_lower:
            found_titles.add(title.title()) # Capitalize for display

    return list(found_emails), list(found_titles)

# --- Main Function ---

def find_contacts(base_url: str):
    """
    Searches a business's website for public contact information.

    It checks the homepage and common contact/about pages for email
    addresses and senior-level job titles.

    Args:
        base_url (str): The base URL of the business's website.

    Returns:
        dict: A dictionary containing 'emails' (a list of found email addresses)
              and 'titles' (a list of found job titles).
    """
    if not base_url:
        return {"emails": [], "titles": []}

    # Ensure base_url has a scheme
    if not urlparse(base_url).scheme:
        base_url = "http://" + base_url

    all_found_emails = set()
    all_found_titles = set()

    # List of URLs to check, starting with the homepage
    urls_to_check = {base_url}
    for path in CONTACT_PAGE_PATHS:
        urls_to_check.add(urljoin(base_url, path))
    
    print(f"\nAnalyzing website: {base_url}")
    for url in urls_to_check:
        soup = get_page_content(url) # Use the refactored function
        if soup:
            print(f"  - Parsing {url} for contacts...")
            emails, titles = _parse_for_contacts(soup)
            all_found_emails.update(emails)
            all_found_titles.update(titles)

    return {
        "emails": list(all_found_emails),
        "titles": list(all_found_titles)
    }

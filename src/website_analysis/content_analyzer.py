import re
from urllib.parse import urlparse, urljoin
import requests
from .utils import get_page_content
from config.config import settings
from bs4 import BeautifulSoup
import openai

# --- Constants for Analysis ---

CTA_PHRASES = [
    "get a quote", "free quote", "request a quote", "get an estimate",
    "free estimate", "request an estimate", "contact us", "schedule a consultation",
    "book now", "request service", "learn more"
]

SOCIAL_MEDIA_DOMAINS = [
    "facebook.com", "instagram.com", "twitter.com", "linkedin.com", "youtube.com"
]

def get_website_quality_scores(url: str, strategy: str = "desktop"):
    """Fetches PageSpeed Insights scores for a given URL."""
    # Note: This requires a Google API key with PageSpeed Insights API enabled.
    # The key is often the same as the Maps API key but needs the specific API enabled in the cloud console.
    api_key = settings.GOOGLE_MAPS_API_KEY 
    pagespeed_url = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={url}&key={api_key}&strategy={strategy}&category=PERFORMANCE&category=ACCESSIBILITY&category=SEO"
    
    try:
        response = requests.get(pagespeed_url, timeout=60)
        response.raise_for_status()
        
        results = response.json()
        lighthouse = results.get('lighthouseResult', {})
        
        scores = {
            "performance": int(lighthouse.get('categories', {}).get('performance', {}).get('score', 0) * 100),
            "accessibility": int(lighthouse.get('categories', {}).get('accessibility', {}).get('score', 0) * 100),
            "seo": int(lighthouse.get('categories', {}).get('seo', {}).get('score', 0) * 100),
        }
        print(f"  > Quality Scores: Performance={scores['performance']}, Accessibility={scores['accessibility']}, SEO={scores['seo']}")
        return scores

    except requests.exceptions.RequestException as e:
        print(f"  > Error fetching PageSpeed scores for {url}: {e}")
        return {}

def analyze_website_content(base_url: str):
    """
    Analyzes a website's homepage for content and conversion signals.

    Args:
        base_url (str): The base URL of the business's website.

    Returns:
        dict: A dictionary containing analysis results:
              - has_blog (bool)
              - cta_phrases (list of found CTA phrases)
              - social_links (list of found social media links)
    """
    analysis = {
        "has_blog": False,
        "cta_phrases": [],
        "social_links": []
    }

    if not base_url:
        return analysis

    # Ensure base_url has a scheme
    if not urlparse(base_url).scheme:
        base_url = "http://" + base_url

    print(f"Analyzing content for: {base_url}")
    soup = get_page_content(base_url)

    if not soup:
        print("  > Could not retrieve website content.")
        return analysis

    page_text_lower = soup.get_text().lower()
    
    # 1. Check for a blog
    for a in soup.find_all('a', href=True):
        href = a.get('href', '').lower()
        text = a.get_text().lower()
        if 'blog' in href or 'blog' in text:
            analysis["has_blog"] = True
            break
    
    # 2. Check for CTAs
    found_ctas = set()
    for phrase in CTA_PHRASES:
        if phrase in page_text_lower:
            found_ctas.add(phrase)
    analysis["cta_phrases"] = list(found_ctas)

    # 3. Check for social media links
    found_socials = set()
    for a in soup.find_all('a', href=True):
        href = a.get('href', '')
        for domain in SOCIAL_MEDIA_DOMAINS:
            if domain in href:
                found_socials.add(href)
    analysis["social_links"] = list(found_socials)

    print(f"  > Blog found: {analysis['has_blog']}")
    print(f"  > CTAs found: {len(analysis['cta_phrases'])}")
    print(f"  > Social links found: {len(analysis['social_links'])}")

    return analysis

def summarize_text_with_llm(text: str, business_name: str):
    """Summarizes website text using an LLM to find the company's mission or specialty."""
    
    if not text:
        return "No website content available to summarize."

    # Use the configured OpenAI key
    openai.api_key = settings.OPENAI_API_KEY
    if not openai.api_key:
        print("Warning: OPENAI_API_KEY not set. Cannot summarize text.")
        return "Summary not available."

    prompt = f"""
    Analyze the following 'About Us' or homepage text from a landscaping company named '{business_name}'. 
    """
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=100
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"  > Error summarizing text: {e}")
        return "Summary not available."

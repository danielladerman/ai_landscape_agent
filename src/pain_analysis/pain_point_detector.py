import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import json
import pandas as pd
from urllib.parse import urlparse
import openai
from openai import OpenAI
from config.config import settings
import logging
import os

# --- OpenAI Client Initialization ---
# It's best practice to initialize the client once and reuse it.
client = OpenAI(api_key=settings.OPENAI_API_KEY)

# --- NLTK Setup for Serverless Environment ---
# On platforms like Vercel, only the /tmp directory is writable.
NLTK_DATA_PATH = os.path.join(os.path.sep, 'tmp', 'nltk_data')
nltk.data.path.append(NLTK_DATA_PATH)

def download_nltk_data():
    """
    Downloads the VADER lexicon to a writable directory (/tmp) if not present.
    """
    try:
        nltk.data.find('sentiment/vader_lexicon.zip')
    except nltk.downloader.DownloadError:
        logging.info("VADER lexicon not found. Downloading to /tmp/nltk_data...")
        nltk.download('vader_lexicon', download_dir=NLTK_DATA_PATH)
        logging.info("Download complete.")

def generate_icebreaker(reviews_json, analysis_json):
    """Uses an LLM to generate a genuine, non-technical compliment about their work."""
    if not settings.OPENAI_API_KEY:
        logging.warning("OPENAI_API_KEY not found. Returning a fallback icebreaker.")
        return "I was looking at your online presence"

    prompt = f"""
    You are a marketing strategist who excels at writing genuine, one-sentence compliments for business outreach.
    Your task is to find the most authentic and positive compliment based on the provided website analysis and Google Reviews.
    Your goal is to set an aspirational tone. **NEVER mention negative reviews or technical website issues.**

    Here is the data:
    - Google Reviews: {reviews_json}
    - Website Content Analysis: {analysis_json}

    Instructions:
    1. Scan the website content for the company's mission, "about us" page, or project gallery descriptions. This is your primary source.
    2. If the website content is thin, scan the reviews for **highly positive quotes (4 stars or higher)**. A customer quote about their beautiful work is very powerful.
    3. Based on the BEST piece of data, write a single, compelling sentence for an email icebreaker.
    4. Your output MUST be a single JSON object with one key: "icebreaker".

    Example of good output:
    {{
        "icebreaker": "Your gallery of patio installations is stunning; the craftsmanship is immediately obvious."
    }}
    {{
        "icebreaker": "I was impressed by your company's mission to create 'unique outdoor living spaces' for your clients."
    }}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4-turbo-preview",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=100,
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content).get("icebreaker", "I was admiring your portfolio of work")
    except Exception as e:
        logging.error(f"Error generating icebreaker: {e}")
        return "I was admiring your portfolio of work" # Fallback

def analyze_pain_points(reviews_json, analysis_json):
    """
    Analyzes a prospect's online presence to identify the opportunity
    to build a powerful social media presence.
    """
    # Ensure NLTK data is available before running analysis that might need it.
    download_nltk_data()
    
    analysis = json.loads(analysis_json) if analysis_json and analysis_json != 'null' else {}

    # --- Icebreaker Generation ---
    icebreaker = generate_icebreaker(reviews_json, analysis_json)

    # --- Redefined Pain Point & Solution ---
    # The primary "pain point" is the missed opportunity of not having a strong social brand.
    
    pain_point = "Opportunity to Attract High-Value Leads via Social Media"
    solution = "Curated Instagram Content Management (Strategy, Editing & Posting)"

    # --- Evidence Discovery ---
    # The evidence should justify why Instagram is a good strategy for them.
    social_links = analysis.get('social_links', [])
    has_instagram = any('instagram.com' in link for link in social_links)

    if has_instagram:
        evidence = "I saw you have an Instagram presence, and I believe with a dedicated content strategy, it could become a powerful asset for attracting premium clients who appreciate high-quality work."
    elif social_links:
        evidence = "I noticed you're on some social platforms, and I see a tremendous opportunity to build on that by creating a professionally curated Instagram presence to showcase your beautiful work and attract more high-value customers."
    else:
        evidence = "Your portfolio of work is impressive, and I believe a professionally curated Instagram presence would be a powerful way to showcase it, build your brand, and attract the kind of high-value clients you're looking for."

    return {
        "icebreaker": icebreaker,
        "identified_pains": [pain_point],
        "proposed_solutions": [solution],
        "evidence": [evidence]
    }

def finalize_prospects(df_prospects: pd.DataFrame):
    """Selects and orders the most relevant columns for the final output."""
    final_columns = [
        'name', 'website', 'verified_emails', 'found_titles', 
        'icebreaker', 'identified_pains', 'proposed_solutions', 'evidence',
        'generated_subject', 'generated_body', 'sent_date'
    ]
    # Ensure all columns exist, adding any that are missing
    for col in final_columns:
        if col not in df_prospects.columns:
            df_prospects[col] = ''
            
    return df_prospects[final_columns]

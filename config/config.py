"""
Centralized Configuration Management using Pydantic.

This module defines a `Settings` class that loads configuration from environment
variables and .env files. It provides a single, typed, and validated source of
truth for all configurations used throughout the application.

Using Pydantic for settings management offers several advantages:
- Type Hinting: Settings are fully type-hinted, improving IDE support and static analysis.
- Validation: Automatically validates that required settings are present and of the correct type.
- Centralization: All configuration is defined and loaded in one place.
- Immutability: The settings object is frozen, preventing accidental modifications at runtime.
"""
import os
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

# --- Project Directory Setup ---
# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Settings(BaseSettings):
    """
    Defines the application's configuration settings, loaded from .env files and environment variables.
    """
    # --- Model Configuration ---
    # Defines settings for a BaseSettings class.
    # .env_file: Specifies the path to the .env file.
    # extra='ignore': Ignores extra fields that are not defined in the model.
    model_config = SettingsConfigDict(
        env_file=os.path.join(BASE_DIR, '.env'),
        env_file_encoding='utf-8',
        extra='ignore'
    )

    # --- Project Metadata ---
    PROJECT_NAME: str = "AI-Powered Hyper-Personalized Outreach System"

    # --- API Keys (Required) ---
    # These settings are critical and MUST be present in the .env file.
    # The application will fail to start if they are missing.
    OPENAI_API_KEY: str = Field(..., description="API key for OpenAI services.")
    GOOGLE_MAPS_API_KEY: str = Field(..., description="API key for Google Maps Places API.")

    # --- Google Sheets Configuration (Required) ---
    SPREADSHEET_ID: str = Field(..., description="The ID of the Google Sheet for tracking prospects.")
    GOOGLE_SHEET_NAME: str = "Sheet1"

    # --- File Paths ---
    # These paths are derived from the project's base directory.
    BASE_DIR: str = BASE_DIR
    PROSPECTS_DATA_PATH: str = os.path.join(BASE_DIR, 'data', 'master_prospect_list.csv')
    GOOGLE_CREDENTIALS_PATH: str = os.path.join(BASE_DIR, 'config', 'google_credentials.json')
    GMAIL_API_CREDENTIALS_PATH: str = os.path.join(BASE_DIR, 'config', 'credentials.json')
    GMAIL_API_TOKEN_PATH: str = os.path.join(BASE_DIR, 'config', 'token.json')

    # --- Optional & Defaulted Settings ---
    # These settings have sensible defaults but can be overridden via the .env file.
    HUNTER_API_KEY: Optional[str] = None
    ANTHROPIC_API_KEY: Optional[str] = None

    # --- SMTP Configuration ---
    SMTP_SERVER: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USERNAME: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    SENDER_EMAIL: Optional[str] = None

    # --- Educational Content Links ---
    # These can be overridden in the .env file if needed.
    CONTENT_STRATEGY_DOC_URL: str = "https://docs.google.com/document/d/your_content_strategy_doc_id/edit?usp=sharing"
    PAID_ADS_DOC_URL: str = "https://docs.google.com/document/d/your_paid_ads_doc_id/edit?usp=sharing"
    WEB_OPTIMIZATION_DOC_URL: str = "https://docs.google.com/document/d/your_web_optimization_doc_id/edit?usp=sharing"


@lru_cache()
def get_settings() -> Settings:
    """
    Returns a cached instance of the Settings object.
    The `@lru_cache` decorator ensures the settings are loaded from the environment only once,
    improving performance by avoiding repeated file I/O and validation.
    """
    return Settings()

# --- Export a single settings instance for easy access ---
# This instance can be imported directly by other modules.
settings = get_settings()

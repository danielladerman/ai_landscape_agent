import requests
from config.config import settings

def verify_email(email: str):
    """
    Verifies an email address using the Hunter.io API.
    If no API key is provided, it will return a simulated 'valid' response
    to allow the pipeline to continue during development.
    """
    if not settings.HUNTER_API_KEY:
        # print("Warning: HUNTER_API_KEY not set. Returning simulated 'valid' response.")
        return {"status": "valid", "source": "simulation"}

    url = f"https://api.hunter.io/v2/email-verifier?email={email}&api_key={settings.HUNTER_API_KEY}"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json().get('data', {})
        return {
            "status": data.get('status'),
            "score": data.get('score'),
            "source": "hunter.io"
        }
    except requests.exceptions.RequestException as e:
        print(f"Error verifying email {email}: {e}")
        return {"status": "error", "source": "error"}

def verify_emails_bulk(emails, delay=0.1):
    """
    Verifies a list of emails, returning only the valid ones.
    This is a placeholder and should be implemented with bulk verification for production.
    """
    verified_emails = []
    for email in emails:
        result = verify_email(email)
        if result and result.get('status') == 'valid':
            verified_emails.append(email)
    return verified_emails

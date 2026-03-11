"""
StoneX API Configuration
Loads credentials from environment or uses saved defaults
"""
import os

# Try to load from .env file if python-dotenv is installed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# StoneX API Configuration
STONEX_CONFIG = {
    "host": os.environ.get("STONEX_API_HOST", "api.stonex.com"),
    "subscription_key": os.environ.get("STONEX_SUBSCRIPTION_KEY", "a96678625aee41509876329fbd09fcb7"),
    "username": os.environ.get("STONEX_USERNAME", "joshua.kress@metcon.co.za"),
    "password": os.environ.get("STONEX_PASSWORD", "P@ran01d99!!12345"),
}

def get_stonex_credentials():
    """Return StoneX API credentials as a dictionary"""
    return STONEX_CONFIG.copy()

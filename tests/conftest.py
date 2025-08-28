"""
Pytest configuration and fixtures for econometrics tests.
"""

import os
import pytest
from dotenv import load_dotenv

# Load environment variables for all tests
load_dotenv()

@pytest.fixture(scope="session")
def api_keys():
    """Fixture providing API keys for testing."""
    return {
        "fred": os.getenv("FRED_API_KEY"),
        "bea": os.getenv("BEA_API_KEY"),
        "bls": os.getenv("BLS_API_KEY"),
    }

@pytest.fixture(scope="session")
def fred_api_key(api_keys):
    """FRED API key fixture."""
    if not api_keys["fred"]:
        pytest.skip("FRED_API_KEY not set")
    return api_keys["fred"]

@pytest.fixture(scope="session") 
def bea_api_key(api_keys):
    """BEA API key fixture."""
    if not api_keys["bea"]:
        pytest.skip("BEA_API_KEY not set")
    return api_keys["bea"]

@pytest.fixture(scope="session")
def bls_api_key(api_keys):
    """BLS API key fixture (optional)."""
    return api_keys["bls"]  # Don't skip if missing - it's optional

@pytest.fixture(scope="session")
def db_config():
    """Database configuration fixture."""
    return {
        "database_url": os.getenv("DATABASE_URL"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "database": os.getenv("POSTGRES_DB", "econometrics"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "port": os.getenv("POSTGRES_PORT", "5432")
    }
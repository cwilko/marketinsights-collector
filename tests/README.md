# Econometrics Data Pipeline - Test Suite

This directory contains comprehensive pytest-based tests for the econometrics data pipeline.

## Test Structure

```
tests/
├── __init__.py                 # Tests package
├── conftest.py                # pytest fixtures and configuration
├── test_api_keys.py          # API key validation and connectivity
├── test_database.py          # Database schema and connectivity tests
├── test_economic_indicators.py # Economic data source tests
└── test_market_data.py        # Market data source tests
```

## Test Categories

### API Key Tests (`test_api_keys.py`)
- **Setup validation**: Ensures required API keys are configured
- **Format validation**: Basic format checks for API keys
- **Connectivity tests**: Smoke tests for API endpoints

### Economic Indicators Tests (`test_economic_indicators.py`)
- **FRED API tests**: Fed Funds Rate data collection and processing
- **BLS API tests**: Consumer Price Index (CPI) data validation
- **BEA API tests**: GDP data collection and parsing logic

### Database Tests (`test_database.py`)
- **Connectivity tests**: Database connection and permissions
- **Schema validation**: Table structure and constraints
- **Data operations**: Insert, update, and query operations  
- **Integration tests**: Full pipeline simulation

### Market Data Tests (`test_market_data.py`)
- **S&P 500 tests**: Stock index data validation
- **VIX tests**: Volatility index data processing
- **Treasury tests**: 10-Year Treasury yield data
- **Integration tests**: Cross-source data validation
- **Freshness tests**: Ensures data is current

## Running Tests

### Basic Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_market_data.py

# Run specific test class
pytest tests/test_market_data.py::TestFREDMarketData

# Run specific test method  
pytest tests/test_api_keys.py::TestAPIKeySetup::test_required_api_keys_present
```

### Using Makefile

```bash
# Run all tests
make test

# Run market data tests only
make test-market

# Run economic indicator tests only
make test-economic

# Run API key tests only
make test-keys

# Run database tests only  
make test-db

# Run with coverage report
make test-coverage
```

### Test Markers

Tests are organized with markers for flexible execution:

```bash
# Run smoke tests (quick connectivity checks)
pytest -m smoke

# Run integration tests
pytest -m integration

# Run tests excluding slow ones
pytest -m "not slow"

# Run database tests only (will skip if no DB credentials)
pytest -m database
```

## Test Data

All tests use **live API data** from official sources:
- **FRED**: Federal Reserve Economic Data
- **BLS**: Bureau of Labor Statistics
- **BEA**: Bureau of Economic Analysis

Tests validate both API connectivity and data processing logic with real economic and market data.

## Requirements

Tests require valid API keys in your `.env` file:

```bash
# API Keys
FRED_API_KEY=your_fred_api_key
BEA_API_KEY=your_bea_api_key
BLS_API_KEY=your_bls_api_key  # Optional

# Database (for database tests) - choose one:

# Option 1: DATABASE_URL (preferred)
DATABASE_URL=postgresql://user:password@host:port/econometrics

# Option 2: Individual parameters (fallback)
POSTGRES_HOST=localhost
POSTGRES_DB=econometrics
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_PORT=5432
```

## Coverage

Run tests with coverage reporting:

```bash
make test-coverage
# Opens htmlcov/index.html with detailed coverage report
```

## CI/CD Integration

Tests are designed to run in CI/CD environments:
- Fast execution (most tests complete in <10 seconds)
- Proper error handling and timeouts
- Clear assertions and failure messages
- Environment variable based configuration
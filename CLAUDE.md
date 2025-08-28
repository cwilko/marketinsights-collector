# Econometrics Data Pipeline

An Airflow-based data collection pipeline for economic indicators and financial market data with **safe-by-default** collector classes.

## Project Overview

This project collects economic and financial data from official government and financial sources:

**Economic Indicators:**
- Consumer Price Index (CPI) - Bureau of Labor Statistics
- Federal Funds Rate - Federal Reserve (FRED)
- Unemployment Rate - Bureau of Labor Statistics
- Gross Domestic Product (GDP) - Bureau of Economic Analysis

**Market Data:**
- S&P 500 Index - Federal Reserve (FRED)
- VIX Volatility Index - Federal Reserve (FRED)
- Treasury Yield Curve - U.S. Department of Treasury
- P/E Ratios - Multiple sources

## Quick Setup

### 1. Database Setup

Create PostgreSQL database and user:

```bash
# Default setup (development)
psql -U postgres \
     -v db_name="econometrics" \
     -v db_user="econometrics_user" \
     -v db_password="econometrics_password" \
     -f scripts/setup_database.sql

# Using environment variables (production)
export ECON_DB_NAME=econometrics_prod
export ECON_DB_USER=econ_user
export ECON_DB_PASSWORD=secure_password
psql -U postgres \
     -v db_name="$ECON_DB_NAME" \
     -v db_user="$ECON_DB_USER" \
     -v db_password="$ECON_DB_PASSWORD" \
     -f scripts/setup_database.sql

```

### 2. API Keys

Get free API keys from:
- [FRED API](https://fred.stlouisfed.org/docs/api/api_key.html) (required)
- [BEA API](https://apps.bea.gov/API/signup/) (required)  
- [BLS API](https://data.bls.gov/registrationEngine/) (optional)

### 3. Environment Configuration

Create `.env` file:
```bash
# API Keys
FRED_API_KEY=your_fred_api_key
BEA_API_KEY=your_bea_api_key
BLS_API_KEY=your_bls_api_key

# Database
DATABASE_URL=postgresql://econometrics_user:econometrics_password@localhost:5432/econometrics
```

### 4. Package Installation & Testing

```bash
# Development setup (includes all dependencies)
pip install -r requirements-dev.txt

# Production setup (minimal dependencies)
pip install -r requirements.txt

# Airflow deployment setup  
pip install -r requirements-airflow.txt

# Run all tests (safe by default - no database writes)
pytest tests/ -v

# Test specific components
pytest tests/test_api_keys.py -v        # API key validation
pytest tests/test_database.py -v        # Database connectivity (requires DB setup)
pytest tests/test_market_data.py -v     # Market data APIs
pytest tests/test_economic_indicators.py -v # Economic indicator APIs
```

## Development Commands

### Testing
```bash
# Safe testing (no database writes)
pytest tests/ -v                    # All tests with safe defaults
pytest -m integration               # Integration tests (safe mode)

# Test specific components
pytest tests/test_api_keys.py -v    # API connectivity
pytest tests/test_database.py -v    # Database schema
pytest tests/test_economic_indicators.py -v # Economic collectors
pytest tests/test_market_data.py -v # Market collectors

# Enable database write tests (use with test database only)
ENABLE_DATABASE_WRITE_TESTS=true pytest tests/test_full_pipeline.py::test_collector_functions_with_database -v
```

### Database Management
```bash
# Create database and user with psql
psql -U postgres \
     -v db_name="$ECON_DB_NAME" \
     -v db_user="$ECON_DB_USER" \
     -v db_password="$ECON_DB_PASSWORD" \
     -f scripts/setup_database.sql

# Cleanup database (if needed)  
psql -U postgres \
     -v db_name="$ECON_DB_NAME" \
     -v db_user="$ECON_DB_USER" \
     -f scripts/cleanup_database.sql

# Create tables manually for local testing
psql -h 192.168.1.206 -U econometrics_user -d econometrics -f sql/create_tables.sql

# Note: Tables are created automatically by Airflow DAG when deployed
```

### Data Collection

The project uses modular collector classes that are **safe by default**:

```bash
# Development setup - install package in editable mode
pip install -r requirements-dev.txt

# Test collectors in safe mode (no database writes)
python -c "
from data_collectors.economic_indicators import collect_fed_funds_rate
from data_collectors.market_data import collect_sp500

# Safe mode - fetches data but doesn't store it
result = collect_fed_funds_rate(database_url=None)
print(f'Fed Funds: {result} records processed (not stored)')

result = collect_sp500(database_url=None)
print(f'S&P 500: {result} records processed (not stored)')
"

# Production mode - stores data to database
python -c "
import os
from data_collectors.economic_indicators import collect_fed_funds_rate

database_url = os.getenv('DATABASE_URL')
result = collect_fed_funds_rate(database_url=database_url)
print(f'Fed Funds: {result} records stored to database')
"
```

## Airflow Deployment (Kubernetes)

The pipeline is designed for Kubernetes Airflow deployment:

1. **Add API keys to sealed secrets**
2. **Configure DATABASE_URL in environment**
3. **Deploy DAG via git-sync** (automatic pickup)
4. **Monitor execution** in Airflow UI

DAG runs daily on weekdays (6 PM ET) and automatically:
- Creates database tables
- Collects data from all sources using safe collector classes
- Handles errors and retries
- Logs collection statistics

## Project Structure

```
econometrics/
├── data_collectors/               # Modular collector classes
│   ├── __init__.py
│   ├── base.py                   # BaseCollector with safe-by-default behavior
│   ├── economic_indicators.py    # FRED, BLS, BEA collectors
│   └── market_data.py            # S&P 500, VIX, Treasury collectors
├── tests/                        # Pytest test suite (safe by default)
│   ├── test_api_keys.py         # API validation tests
│   ├── test_database.py         # Database schema tests
│   ├── test_economic_indicators.py # Economic data tests
│   ├── test_market_data.py      # Market data tests
│   └── test_full_pipeline.py    # End-to-end pipeline tests
├── scripts/                      # Database setup
│   └── setup_database.sql       # Creates DB and user
├── sql/                         # Database schema
│   └── create_tables.sql        # Table definitions
├── setup.py                     # Package installation
└── requirements.txt             # Python dependencies
```

## Data Schema

All data is stored in PostgreSQL with proper indexing and constraints:

- **Timestamps** - All records have created_at/updated_at
- **Unique constraints** - Prevent duplicate data
- **Decimal precision** - Accurate financial calculations
- **Upsert operations** - Safe data updates

## Monitoring

- **Airflow UI** - Task execution and logs
- **Database queries** - Data validation and statistics  
- **Test suite** - Continuous validation of APIs and data
- **Error handling** - Automatic retries and alerting

## Security

- **Safe by default** - Collectors require explicit `database_url` to write data
- **API keys** - Stored in Kubernetes sealed secrets
- **Database** - Dedicated user with minimal privileges
- **No secrets in code** - All credentials externalized
- **Environment-based config** - Different settings per environment
- **Test isolation** - Tests run without database writes by default

## Performance

- **Daily schedule** - Matches data source update frequency (weekdays only)
- **Parallel execution** - Independent tasks run concurrently
- **Rate limiting** - Respects API quotas and limits
- **Efficient queries** - Indexed database operations
- **Modular collectors** - Reusable classes with minimal dependencies

## Troubleshooting

### Common Issues

1. **API Key Errors** - Verify keys in environment variables
2. **Database Connection** - Check DATABASE_URL format
3. **Missing Data** - Review Airflow task logs
4. **Test Failures** - Ensure API keys are configured

### Useful Commands

```bash
# Check API connectivity
pytest tests/test_api_keys.py -v

# Validate database schema
pytest tests/test_database.py -v

# Test data processing
python test_single_collector.py all

# Check database contents
psql $DATABASE_URL -c "SELECT COUNT(*) FROM federal_funds_rate;"
```

## Collector Classes (Safe by Default)

The pipeline uses modular collector classes that are **safe by default**:

### Usage Examples

```python
from data_collectors.economic_indicators import FREDCollector, collect_fed_funds_rate
from data_collectors.market_data import collect_sp500

# Safe mode - fetch data without database writes
collector = FREDCollector(database_url=None)
data = collector.get_series_data("FEDFUNDS", limit=5)
result = collect_fed_funds_rate(database_url=None)  # Returns count but doesn't store

# Production mode - explicitly enable database storage  
import os
database_url = os.getenv('DATABASE_URL')
result = collect_fed_funds_rate(database_url=database_url)  # Stores to database
```

### Available Collectors

**Economic Indicators:**
- `FREDCollector` - Federal Reserve Economic Data
- `BLSCollector` - Bureau of Labor Statistics  
- `BEACollector` - Bureau of Economic Analysis

**Market Data:**
- `collect_sp500(database_url=None)` - S&P 500 index data
- `collect_vix(database_url=None)` - VIX volatility index
- `collect_treasury_yields(database_url=None)` - Treasury yield curves
- `collect_pe_ratios(database_url=None)` - P/E ratio data

### Safety Features

- ✅ **No accidental database writes** - `database_url=None` by default
- ✅ **Explicit opt-in** - Must provide `database_url` to store data  
- ✅ **Test isolation** - Tests run safely without database setup
- ✅ **Production ready** - DAG explicitly passes database credentials
- ✅ **Backwards compatible** - Existing functionality preserved

This pipeline provides reliable, automated collection of economic and financial data with **secure-by-default** collector classes for analysis and dashboard creation.
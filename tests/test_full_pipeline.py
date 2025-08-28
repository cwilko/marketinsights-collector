"""
Full pipeline integration tests.

Tests the complete data flow: table creation, API data retrieval, 
data insertion, validation, and cleanup using actual collector classes.
"""

import pytest
import psycopg2
import os
import logging
from datetime import datetime, date
from decimal import Decimal
from data_collectors.economic_indicators import FREDCollector, collect_fed_funds_rate
from data_collectors.market_data import collect_sp500

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def db_connection():
    """Create database connection for testing."""
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        conn = psycopg2.connect(database_url)
    else:
        # Fallback to individual parameters
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            database=os.getenv('POSTGRES_DB', 'econometrics'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', '5432')
        )
    
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def test_table_name():
    """Generate unique test table name."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"test_fed_funds_{timestamp}"


@pytest.mark.integration
@pytest.mark.database
def test_full_pipeline_fed_funds_rate(db_connection, test_table_name):
    """
    Test complete pipeline using actual FREDCollector: create table, fetch data, insert, verify, cleanup.
    """
    cur = db_connection.cursor()
    
    try:
        # Step 1: Create test table
        logger.info(f"Creating test table: {test_table_name}")
        create_table_sql = f"""
        CREATE TABLE {test_table_name} (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            effective_rate DECIMAL(10,4) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        
        # Verify table was created
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (test_table_name,))
        assert cur.fetchone()[0], f"Test table {test_table_name} was not created"
        
        # Step 2: Use FRED collector to fetch data
        logger.info("Using FREDCollector to fetch Federal Funds Rate data")
        collector = FREDCollector(database_url=None)  # Don't write to database
        observations = collector.get_series_data("FEDFUNDS", limit=5)
        
        assert len(observations) > 0, "No data returned from FRED collector"
        
        # Step 3: Process and insert data using collector's upsert method
        logger.info(f"Processing {len(observations)} records")
        inserted_count = 0
        test_data = []
        
        for item in observations:
            if item["value"] == ".":
                continue
                
            try:
                record_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
                rate = Decimal(item["value"])
                
                # Manual insert for test table (since collector expects specific table names)
                insert_sql = f"""
                INSERT INTO {test_table_name} (date, effective_rate)
                VALUES (%s, %s)
                ON CONFLICT (date) DO UPDATE SET
                effective_rate = EXCLUDED.effective_rate,
                updated_at = CURRENT_TIMESTAMP
                """
                cur.execute(insert_sql, (record_date, rate))
                inserted_count += 1
                test_data.append((record_date, rate))
                
            except Exception as e:
                logger.warning(f"Error processing record: {e}")
                continue
        
        assert inserted_count > 0, "No records were inserted"
        logger.info(f"Successfully inserted {inserted_count} records")
        
        # Step 4: Verify data was inserted correctly
        cur.execute(f"SELECT COUNT(*) FROM {test_table_name}")
        row_count = cur.fetchone()[0]
        assert row_count == inserted_count, f"Expected {inserted_count} rows, found {row_count}"
        
        # Verify specific data
        cur.execute(f"""
            SELECT date, effective_rate 
            FROM {test_table_name} 
            ORDER BY date DESC 
            LIMIT 1
        """)
        result = cur.fetchone()
        assert result is not None, "No data found in test table"
        
        db_date, db_rate = result
        assert db_date in [d[0] for d in test_data], "Date not found in expected data"
        assert db_rate in [d[1] for d in test_data], "Rate not found in expected data"
        
        # Step 5: Test data types and constraints
        cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, (test_table_name,))
        columns = cur.fetchall()
        
        column_types = {col[0]: col[1] for col in columns}
        assert 'date' in column_types
        assert 'effective_rate' in column_types
        assert column_types['effective_rate'] == 'numeric'
        
        # Test precision
        cur.execute(f"SELECT effective_rate FROM {test_table_name} LIMIT 1")
        rate_value = cur.fetchone()[0]
        assert isinstance(rate_value, Decimal), "Rate should be Decimal type"
        
        logger.info("✅ Full pipeline test completed successfully")
        
    finally:
        # Step 6: Cleanup - Drop test table
        logger.info(f"Cleaning up test table: {test_table_name}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {test_table_name}")
            logger.info("✅ Test table cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            # Don't fail the test due to cleanup issues
            
        # Verify table was dropped
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            );
        """, (test_table_name,))
        table_exists = cur.fetchone()[0]
        if table_exists:
            logger.warning(f"Test table {test_table_name} still exists after cleanup")
        else:
            logger.info("✅ Verified test table was removed")


@pytest.mark.integration
@pytest.mark.database 
def test_full_pipeline_treasury_yields(db_connection, test_table_name):
    """
    Test complete pipeline with Treasury yield data using FRED collector.
    """
    treasury_table_name = f"test_treasury_{test_table_name.split('_')[-1]}"
    cur = db_connection.cursor()
    
    try:
        # Step 1: Create test table for Treasury yields
        logger.info(f"Creating Treasury test table: {treasury_table_name}")
        create_table_sql = f"""
        CREATE TABLE {treasury_table_name} (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            maturity VARCHAR(10) NOT NULL,
            yield_rate DECIMAL(10,4) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, maturity)
        );
        """
        cur.execute(create_table_sql)
        
        # Step 2: Use FRED collector to fetch Treasury yields
        logger.info("Using FREDCollector to fetch Treasury yield data")
        collector = FREDCollector(database_url=None)  # Don't write to database
        
        test_yield_series = {
            "DGS10": "10Y",   # 10-Year Treasury
            "DGS2": "2Y"      # 2-Year Treasury  
        }
        
        inserted_count = 0
        
        for series_id, maturity in test_yield_series.items():
            logger.info(f"Fetching {maturity} Treasury yields ({series_id})")
            observations = collector.get_series_data(series_id, limit=5)
            
            assert len(observations) > 0, f"No data returned for {series_id}"
            
            # Process and insert data for this maturity
            for item in observations:
                if item["value"] == ".":  # FRED uses "." for missing values
                    continue
                    
                try:
                    record_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
                    yield_rate = Decimal(item["value"])
                    
                    insert_sql = f"""
                    INSERT INTO {treasury_table_name} (date, maturity, yield_rate)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (date, maturity) DO UPDATE SET
                    yield_rate = EXCLUDED.yield_rate,
                    updated_at = CURRENT_TIMESTAMP
                    """
                    cur.execute(insert_sql, (record_date, maturity, yield_rate))
                    inserted_count += 1
                    
                except Exception as e:
                    logger.warning(f"Error processing {series_id} item: {e}")
                    continue
        
        assert inserted_count > 0, "No Treasury records were inserted"
        logger.info(f"Successfully inserted {inserted_count} Treasury yield records")
        
        # Step 4: Verify Treasury data
        cur.execute(f"SELECT COUNT(*) FROM {treasury_table_name}")
        row_count = cur.fetchone()[0]
        assert row_count == inserted_count, f"Expected {inserted_count} rows, found {row_count}"
        
        # Verify we have both maturities
        cur.execute(f"SELECT DISTINCT maturity FROM {treasury_table_name} ORDER BY maturity")
        maturities = [row[0] for row in cur.fetchall()]
        expected_maturities = list(test_yield_series.values())
        for expected in expected_maturities:
            assert expected in maturities, f"Missing maturity: {expected}"
        
        # Verify unique constraint works
        cur.execute(f"""
            SELECT date, maturity, COUNT(*) 
            FROM {treasury_table_name} 
            GROUP BY date, maturity 
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()
        assert len(duplicates) == 0, "Found duplicate date/maturity combinations"
        
        # Verify data types and precision
        cur.execute(f"SELECT yield_rate FROM {treasury_table_name} LIMIT 1")
        rate_value = cur.fetchone()[0]
        assert isinstance(rate_value, Decimal), "Yield rate should be Decimal type"
        
        logger.info("✅ Treasury FRED pipeline test completed successfully")
        
    finally:
        # Cleanup Treasury table
        logger.info(f"Cleaning up Treasury test table: {treasury_table_name}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {treasury_table_name}")
        except Exception as e:
            logger.error(f"Error during Treasury cleanup: {e}")


@pytest.mark.integration  
def test_collector_functions_safe_mode():
    """
    Test the actual collector functions used by the DAG in safe mode (no database writes).
    """
    
    # Test Fed Funds Rate collector function
    logger.info("Testing collect_fed_funds_rate function (safe mode)")
    try:
        result = collect_fed_funds_rate(database_url=None)
        assert isinstance(result, int)
        assert result >= 0
        logger.info(f"✅ Fed Funds Rate collector processed {result} records (no DB write)")
    except Exception as e:
        logger.error(f"Fed Funds Rate collector failed: {e}")
        raise
    
    # Test S&P 500 collector function  
    logger.info("Testing collect_sp500 function (safe mode)")
    try:
        result = collect_sp500(database_url=None)
        assert isinstance(result, int)
        assert result >= 0
        logger.info(f"✅ S&P 500 collector processed {result} records (no DB write)")
    except Exception as e:
        logger.error(f"S&P 500 collector failed: {e}")
        raise


@pytest.mark.integration
@pytest.mark.database
def test_collector_functions_with_database(db_connection):
    """
    Test collector functions that actually write to database.
    WARNING: This writes to real database tables - use only with test database.
    """
    import os
    
    # Only run this test if explicitly enabled
    if not os.getenv('ENABLE_DATABASE_WRITE_TESTS'):
        pytest.skip("Database write tests disabled - set ENABLE_DATABASE_WRITE_TESTS=true to enable")
    
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        pytest.skip("DATABASE_URL not available")
    
    # Test Fed Funds Rate collector function with database
    logger.info("Testing collect_fed_funds_rate function (with database)")
    try:
        result = collect_fed_funds_rate(database_url=database_url)
        assert isinstance(result, int)
        assert result >= 0
        logger.info(f"✅ Fed Funds Rate collector processed {result} records (written to DB)")
    except Exception as e:
        logger.error(f"Fed Funds Rate collector failed: {e}")
        raise
    
    # Test S&P 500 collector function with database 
    logger.info("Testing collect_sp500 function (with database)")
    try:
        result = collect_sp500(database_url=database_url)
        assert isinstance(result, int)
        assert result >= 0
        logger.info(f"✅ S&P 500 collector processed {result} records (written to DB)")
    except Exception as e:
        logger.error(f"S&P 500 collector failed: {e}")
        raise


@pytest.mark.integration
@pytest.mark.database
def test_pipeline_error_handling(db_connection, test_table_name):
    """
    Test pipeline error handling and recovery.
    """
    error_table_name = f"test_error_{test_table_name.split('_')[-1]}"
    cur = db_connection.cursor()
    
    try:
        # Create table with constraints to test error handling
        logger.info(f"Creating error handling test table: {error_table_name}")
        create_table_sql = f"""
        CREATE TABLE {error_table_name} (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            value DECIMAL(5,2) NOT NULL CHECK (value >= 0 AND value <= 100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create_table_sql)
        
        # Test successful insertion
        cur.execute(f"""
            INSERT INTO {error_table_name} (date, value)
            VALUES (%s, %s)
        """, (date.today(), Decimal('5.25')))
        
        # Test constraint violation handling
        with pytest.raises(psycopg2.IntegrityError):
            cur.execute(f"""
                INSERT INTO {error_table_name} (date, value)
                VALUES (%s, %s)
            """, (date.today(), Decimal('5.25')))  # Duplicate date
        
        # Rollback the failed transaction
        db_connection.rollback()
        
        # Test data type error handling
        with pytest.raises((psycopg2.DataError, psycopg2.IntegrityError)):
            cur.execute(f"""
                INSERT INTO {error_table_name} (date, value)
                VALUES (%s, %s)
            """, (date.today(), Decimal('150.00')))  # Exceeds CHECK constraint
        
        db_connection.rollback()
        
        # Verify original data is still there
        cur.execute(f"SELECT COUNT(*) FROM {error_table_name}")
        assert cur.fetchone()[0] == 1
        
        logger.info("✅ Error handling test completed successfully")
        
    finally:
        # Cleanup
        logger.info(f"Cleaning up error test table: {error_table_name}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {error_table_name}")
        except Exception as e:
            logger.error(f"Error during error table cleanup: {e}")
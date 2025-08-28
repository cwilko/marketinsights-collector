"""
Tests for database connectivity and schema validation.
"""

import pytest
import psycopg2
import os
from datetime import datetime, date


@pytest.fixture(scope="session")
def db_connection():
    """Database connection fixture."""
    # Try DATABASE_URL first, then fall back to individual parameters
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        try:
            conn = psycopg2.connect(database_url)
            yield conn
            conn.close()
            return
        except psycopg2.Error as e:
            pytest.skip(f"Cannot connect to database via DATABASE_URL: {e}")
    
    # Fallback to individual parameters
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_name = os.getenv('POSTGRES_DB', 'econometrics')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    
    # Skip if database credentials not provided
    if not all([db_user, db_password]):
        pytest.skip("Database credentials not provided (neither DATABASE_URL nor individual POSTGRES_* vars)")
    
    try:
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        yield conn
        conn.close()
    except psycopg2.Error as e:
        pytest.skip(f"Cannot connect to database: {e}")


@pytest.fixture
def db_cursor(db_connection):
    """Database cursor fixture with transaction rollback."""
    cursor = db_connection.cursor()
    yield cursor
    db_connection.rollback()  # Rollback any changes made during tests
    cursor.close()


@pytest.mark.database
class TestDatabaseConnectivity:
    """Tests for basic database connectivity."""
    
    def test_database_connection(self, db_connection):
        """Test that we can connect to the database."""
        assert db_connection is not None
        assert not db_connection.closed
        
    def test_database_version(self, db_cursor):
        """Test PostgreSQL version information."""
        db_cursor.execute("SELECT version();")
        version = db_cursor.fetchone()[0]
        assert "PostgreSQL" in version
        
    def test_database_permissions(self, db_cursor):
        """Test that we have necessary database permissions."""
        # Test we can create/drop tables (should work if we have proper permissions)
        try:
            db_cursor.execute("CREATE TABLE test_permissions (id SERIAL PRIMARY KEY);")
            db_cursor.execute("DROP TABLE test_permissions;")
        except psycopg2.Error as e:
            pytest.fail(f"Insufficient database permissions: {e}")


@pytest.mark.database
class TestDatabaseSchema:
    """Tests for database schema and table structure."""
    
    def test_economic_tables_exist(self, db_cursor):
        """Test that economic indicator tables exist."""
        expected_tables = [
            'consumer_price_index',
            'federal_funds_rate', 
            'unemployment_rate',
            'gross_domestic_product'
        ]
        
        for table_name in expected_tables:
            db_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            
            exists = db_cursor.fetchone()[0]
            assert exists, f"Table {table_name} does not exist"
            
    def test_market_data_tables_exist(self, db_cursor):
        """Test that market data tables exist."""
        expected_tables = [
            'sp500_index',
            'vix_index',
            'treasury_yields',
            'pe_ratios'
        ]
        
        for table_name in expected_tables:
            db_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                );
            """, (table_name,))
            
            exists = db_cursor.fetchone()[0]
            assert exists, f"Table {table_name} does not exist"
            
    def test_table_columns(self, db_cursor):
        """Test that tables have expected column structure."""
        # Test consumer_price_index table structure
        db_cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'consumer_price_index'
            ORDER BY ordinal_position;
        """)
        
        columns = db_cursor.fetchall()
        column_names = [col[0] for col in columns]
        
        expected_columns = [
            'id', 'date', 'value', 'month_over_month_change',
            'year_over_year_change', 'created_at', 'updated_at'
        ]
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} missing from consumer_price_index"
            
    def test_indexes_exist(self, db_cursor):
        """Test that expected indexes exist for performance."""
        expected_indexes = [
            ('consumer_price_index', 'idx_cpi_date'),
            ('federal_funds_rate', 'idx_fed_funds_date'),
            ('sp500_index', 'idx_sp500_date'),
            ('treasury_yields', 'idx_treasury_date_maturity')
        ]
        
        for table_name, index_name in expected_indexes:
            db_cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM pg_indexes 
                    WHERE tablename = %s AND indexname = %s
                );
            """, (table_name, index_name))
            
            exists = db_cursor.fetchone()[0]
            assert exists, f"Index {index_name} does not exist on table {table_name}"
            
    def test_unique_constraints(self, db_cursor):
        """Test that unique constraints are properly set."""
        # Test that consumer_price_index has unique date constraint
        db_cursor.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'consumer_price_index' 
            AND constraint_type = 'UNIQUE';
        """)
        
        constraints = db_cursor.fetchall()
        assert len(constraints) > 0, "consumer_price_index should have unique constraints"
        
        # Test that treasury_yields has compound unique constraint (date, maturity)
        db_cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'treasury_yields' 
            AND constraint_type = 'UNIQUE';
        """)
        
        constraints = db_cursor.fetchall()
        assert len(constraints) > 0, "treasury_yields should have unique constraints"


@pytest.mark.database
class TestDataOperations:
    """Tests for data insertion and querying operations."""
    
    def test_insert_consumer_price_index(self, db_cursor):
        """Test inserting data into consumer_price_index table."""
        test_data = {
            'date': date.today(),
            'value': 323.048,
            'year_over_year_change': 2.5
        }
        
        db_cursor.execute("""
            INSERT INTO consumer_price_index (date, value, year_over_year_change)
            VALUES (%(date)s, %(value)s, %(year_over_year_change)s)
            RETURNING id;
        """, test_data)
        
        result = db_cursor.fetchone()
        assert result is not None
        assert isinstance(result[0], int)  # Should return an ID
        
    def test_insert_federal_funds_rate(self, db_cursor):
        """Test inserting data into federal_funds_rate table."""
        test_data = {
            'date': date.today(),
            'effective_rate': 4.33
        }
        
        db_cursor.execute("""
            INSERT INTO federal_funds_rate (date, effective_rate)
            VALUES (%(date)s, %(effective_rate)s)
            RETURNING id;
        """, test_data)
        
        result = db_cursor.fetchone()
        assert result is not None
        assert isinstance(result[0], int)
        
    def test_insert_sp500_data(self, db_cursor):
        """Test inserting data into sp500_index table."""
        test_data = {
            'date': date.today(),
            'open_price': 6400.00,
            'high_price': 6450.00, 
            'low_price': 6350.00,
            'close_price': 6439.32,
            'volume': 1000000
        }
        
        db_cursor.execute("""
            INSERT INTO sp500_index (date, open_price, high_price, low_price, close_price, volume)
            VALUES (%(date)s, %(open_price)s, %(high_price)s, %(low_price)s, %(close_price)s, %(volume)s)
            RETURNING id;
        """, test_data)
        
        result = db_cursor.fetchone()
        assert result is not None
        assert isinstance(result[0], int)
        
    def test_insert_treasury_yields(self, db_cursor):
        """Test inserting data into treasury_yields table."""
        test_data = {
            'date': date.today(),
            'maturity': '10Y',
            'yield_rate': 4.39
        }
        
        db_cursor.execute("""
            INSERT INTO treasury_yields (date, maturity, yield_rate)
            VALUES (%(date)s, %(maturity)s, %(yield_rate)s)
            RETURNING id;
        """, test_data)
        
        result = db_cursor.fetchone()
        assert result is not None
        assert isinstance(result[0], int)
        
    def test_upsert_functionality(self, db_cursor):
        """Test upsert (INSERT ... ON CONFLICT) functionality."""
        test_date = date.today()
        
        # First insert
        db_cursor.execute("""
            INSERT INTO federal_funds_rate (date, effective_rate, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (date) DO UPDATE SET
            effective_rate = EXCLUDED.effective_rate,
            updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """, (test_date, 4.33))
        
        first_result = db_cursor.fetchone()[0]
        
        # Second insert (should update, not create new row)
        db_cursor.execute("""
            INSERT INTO federal_funds_rate (date, effective_rate, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (date) DO UPDATE SET
            effective_rate = EXCLUDED.effective_rate,
            updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """, (test_date, 4.50))
        
        second_result = db_cursor.fetchone()[0]
        
        # Should be same ID (updated, not inserted)
        assert first_result == second_result
        
        # Verify the value was updated
        db_cursor.execute("""
            SELECT effective_rate FROM federal_funds_rate WHERE id = %s;
        """, (first_result,))
        
        updated_rate = db_cursor.fetchone()[0]
        assert float(updated_rate) == 4.50
        
    def test_data_types_and_precision(self, db_cursor):
        """Test that decimal precision is maintained correctly."""
        # Test high precision decimal values
        db_cursor.execute("""
            INSERT INTO consumer_price_index (date, value, year_over_year_change)
            VALUES (%s, %s, %s)
            RETURNING value, year_over_year_change;
        """, (date.today(), 323.0486, 2.5678))
        
        result = db_cursor.fetchone()
        value, yoy_change = result
        
        # Check that precision is maintained (within reasonable floating point tolerance)
        assert abs(float(value) - 323.0486) < 0.0001
        assert abs(float(yoy_change) - 2.5678) < 0.0001


@pytest.mark.database
@pytest.mark.integration
class TestDatabaseIntegration:
    """Integration tests for complete database workflows."""
    
    def test_full_data_pipeline_simulation(self, db_cursor):
        """Test complete data collection simulation."""
        # Simulate collecting data from multiple sources
        test_date = date.today()
        
        # Economic indicators
        db_cursor.execute("""
            INSERT INTO consumer_price_index (date, value, year_over_year_change)
            VALUES (%s, %s, %s);
        """, (test_date, 323.048, 2.5))
        
        db_cursor.execute("""
            INSERT INTO federal_funds_rate (date, effective_rate)
            VALUES (%s, %s);
        """, (test_date, 4.33))
        
        # Market data  
        db_cursor.execute("""
            INSERT INTO sp500_index (date, open_price, high_price, low_price, close_price)
            VALUES (%s, %s, %s, %s, %s);
        """, (test_date, 6400.00, 6450.00, 6350.00, 6439.32))
        
        db_cursor.execute("""
            INSERT INTO treasury_yields (date, maturity, yield_rate)
            VALUES (%s, %s, %s);
        """, (test_date, '10Y', 4.39))
        
        # Verify all data was inserted
        tables_to_check = [
            'consumer_price_index',
            'federal_funds_rate', 
            'sp500_index',
            'treasury_yields'
        ]
        
        for table in tables_to_check:
            db_cursor.execute(f"""
                SELECT COUNT(*) FROM {table} WHERE date = %s;
            """, (test_date,))
            
            count = db_cursor.fetchone()[0]
            assert count > 0, f"No data found in {table} for test date"
            
    def test_cross_table_queries(self, db_cursor):
        """Test queries across multiple tables."""
        test_date = date.today()
        
        # Insert test data in multiple tables
        db_cursor.execute("""
            INSERT INTO federal_funds_rate (date, effective_rate)
            VALUES (%s, %s);
        """, (test_date, 4.33))
        
        db_cursor.execute("""
            INSERT INTO treasury_yields (date, maturity, yield_rate) 
            VALUES (%s, %s, %s);
        """, (test_date, '10Y', 4.39))
        
        # Test join query
        db_cursor.execute("""
            SELECT f.effective_rate, t.yield_rate
            FROM federal_funds_rate f
            JOIN treasury_yields t ON f.date = t.date
            WHERE f.date = %s AND t.maturity = '10Y';
        """, (test_date,))
        
        result = db_cursor.fetchone()
        assert result is not None
        
        fed_rate, treasury_rate = result
        assert float(fed_rate) == 4.33
        assert float(treasury_rate) == 4.39
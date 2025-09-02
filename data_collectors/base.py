import os
import logging
import requests
import time
import psycopg2
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, date, timedelta

class BaseCollector:
    def __init__(self, database_url=None):
        self.database_url = database_url
        self.session = requests.Session()
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def get_db_connection(self):
        """Get database connection ONLY if database_url was explicitly provided."""
        if self.database_url is None:
            return None  # No database operations if URL not provided
        
        try:
            return psycopg2.connect(self.database_url)
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {str(e)}")
            raise
        
    def make_request(self, url: str, params: Dict[str, Any] = None, 
                    retries: int = 3, backoff_factor: float = 1.0, headers: Dict[str, str] = None) -> Optional[Dict]:
        """Make HTTP request with retry logic and rate limiting."""
        for attempt in range(retries):
            try:
                # Use provided headers or default session headers
                if headers:
                    response = self.session.get(url, params=params, headers=headers, timeout=30)
                else:
                    response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt < retries - 1:
                    time.sleep(backoff_factor * (2 ** attempt))
                else:
                    self.logger.error(f"All {retries} attempts failed for URL: {url}")
                    raise
        return None
        
    def upsert_data(self, table: str, data: Dict[str, Any], 
                   conflict_columns: list = None) -> bool:
        """Insert or update data in PostgreSQL table if database_url provided."""
        if self.database_url is None:
            self.logger.info(f"No database URL provided - skipping storage of data to {table}")
            return True  # Return success but skip storage
        
        conn = None
        try:
            conn = self.get_db_connection()
            
            if conflict_columns is None:
                conflict_columns = ['date']
                
            columns = list(data.keys())
            values = list(data.values())
            
            placeholders = ', '.join(['%s'] * len(values))
            columns_str = ', '.join(columns)
            
            # Create UPDATE clause for ON CONFLICT
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
            conflict_str = ', '.join(conflict_columns)
            
            sql = f"""
            INSERT INTO {table} ({columns_str}, updated_at)
            VALUES ({placeholders}, CURRENT_TIMESTAMP)
            ON CONFLICT ({conflict_str}) DO UPDATE SET
            {update_clause}, updated_at = CURRENT_TIMESTAMP
            """
            
            with conn.cursor() as cur:
                cur.execute(sql, values)
                conn.commit()
                
            self.logger.info(f"Successfully upserted data to {table}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to upsert data to {table}: {str(e)}")
            return False
        finally:
            if conn:  # Always close connection if we created one
                conn.close()

    def bulk_upsert_data(self, table: str, data_list: List[Dict[str, Any]], 
                        conflict_columns: list = None, batch_size: int = 1000) -> int:
        """Bulk insert or update data in PostgreSQL table if database_url provided.
        
        Args:
            table: Table name
            data_list: List of dictionaries with data to upsert
            conflict_columns: Columns to use for conflict resolution (defaults to ['date'])
            batch_size: Number of records to process in each batch
            
        Returns:
            Number of successfully processed records
        """
        if self.database_url is None:
            self.logger.info(f"No database URL provided - skipping storage of {len(data_list)} records to {table}")
            return len(data_list)  # Return count but skip storage
        
        if not data_list:
            self.logger.info(f"No data provided for bulk upsert to {table}")
            return 0
            
        conn = None
        total_processed = 0
        
        try:
            conn = self.get_db_connection()
            
            if conflict_columns is None:
                conflict_columns = ['date']
                
            # Get column structure from first record
            first_record = data_list[0]
            columns = list(first_record.keys())
            columns_str = ', '.join(columns)
            
            # Create UPDATE clause for ON CONFLICT
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_columns])
            conflict_str = ', '.join(conflict_columns)
            
            # Create placeholders for VALUES clause
            placeholders = ', '.join(['%s'] * len(columns))
            
            # Process data in batches
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i + batch_size]
                
                # Prepare values for batch
                values_list = []
                for record in batch:
                    values_list.append(tuple(record[col] for col in columns))
                
                # Create VALUES clause for multiple records
                values_placeholders = ', '.join([f"({placeholders})" for _ in range(len(batch))])
                
                sql = f"""
                INSERT INTO {table} ({columns_str}, updated_at)
                VALUES {values_placeholders.replace(placeholders, placeholders + ', CURRENT_TIMESTAMP')}
                ON CONFLICT ({conflict_str}) DO UPDATE SET
                {update_clause}, updated_at = CURRENT_TIMESTAMP
                """
                
                # Flatten values for execute
                flat_values = []
                for record in batch:
                    flat_values.extend(record[col] for col in columns)
                
                with conn.cursor() as cur:
                    cur.execute(sql, flat_values)
                    conn.commit()
                    
                total_processed += len(batch)
                self.logger.debug(f"Processed batch of {len(batch)} records for {table}")
                
            self.logger.info(f"Successfully bulk upserted {total_processed} records to {table}")
            return total_processed
            
        except Exception as e:
            self.logger.error(f"Failed to bulk upsert data to {table}: {str(e)}")
            if conn:
                conn.rollback()
            return total_processed  # Return what we managed to process
        finally:
            if conn:
                conn.close()
            
    def get_env_var(self, var_name: str, required: bool = True) -> Optional[str]:
        """Get environment variable with optional requirement check."""
        value = os.getenv(var_name)
        if required and not value:
            raise ValueError(f"Required environment variable {var_name} not set")
        return value
    
    def get_last_record_date(self, table: str, date_column: str = 'date') -> Optional[date]:
        """Get the date of the most recent record in the specified table."""
        if self.database_url is None:
            return None
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT MAX({date_column}) FROM {table}
                """)
                result = cur.fetchone()
                return result[0] if result and result[0] else None
        except Exception as e:
            self.logger.warning(f"Could not get last record date from {table}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in the database."""
        if self.database_url is None:
            return False
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table,))
                result = cur.fetchone()
                return result[0] if result else False
        except Exception as e:
            self.logger.warning(f"Could not check if table {table} exists: {str(e)}")
            return False
        finally:
            if conn:
                conn.close()
    
    def get_cpi_value_for_date(self, target_date: date, table: str = "consumer_price_index") -> Optional[float]:
        """Get CPI value for a specific date from the database."""
        if self.database_url is None:
            return None
        
        conn = None
        try:
            conn = self.get_db_connection()
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT value FROM {table} 
                    WHERE date = %s
                """, (target_date,))
                result = cur.fetchone()
                return float(result[0]) if result else None
        except Exception as e:
            self.logger.debug(f"Could not get CPI value for date {target_date} from {table}: {str(e)}")
            return None
        finally:
            if conn:
                conn.close()
    
    def get_date_range_for_collection(self, table: str, date_column: str = 'date', 
                                    default_lookback_days: int = 365) -> Tuple[Optional[date], Optional[date]]:
        """
        Determine the date range to collect data for based on existing records.
        Returns (start_date, end_date) tuple.
        
        For empty tables: returns (start_date for historical data, today)
        For existing tables: returns (last_record_date + 1 day, today)
        """
        end_date = datetime.now().date()
        
        # Check if we have existing data
        last_date = self.get_last_record_date(table, date_column)
        
        if last_date is None:
            # No existing data - fetch ALL available historical data (no time limit)
            start_date = None  # None means fetch all available historical data
            self.logger.info(f"No existing data in {table}, fetching all available historical data")
        else:
            # Existing data - fetch only new data since last record
            start_date = last_date + timedelta(days=1)
            if start_date > end_date:
                # No new data needed
                self.logger.info(f"Data in {table} is up to date (last: {last_date})")
                return None, None
            else:
                self.logger.info(f"Fetching incremental data for {table} from {start_date} to {end_date}")
        
        return start_date, end_date
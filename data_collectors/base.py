import os
import logging
import requests
import time
import psycopg2
from typing import Dict, Any, Optional
from datetime import datetime

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
                    retries: int = 3, backoff_factor: float = 1.0) -> Optional[Dict]:
        """Make HTTP request with retry logic and rate limiting."""
        for attempt in range(retries):
            try:
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
            
    def get_env_var(self, var_name: str, required: bool = True) -> Optional[str]:
        """Get environment variable with optional requirement check."""
        value = os.getenv(var_name)
        if required and not value:
            raise ValueError(f"Required environment variable {var_name} not set")
        return value
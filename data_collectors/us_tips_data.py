"""
US TIPS (Treasury Inflation-Protected Securities) data collector.
Collects daily TIPS yields from FRED API for calculation of breakeven inflation.
"""
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .base import BaseCollector
from .economic_indicators import FREDCollector

# FRED TIPS Series Mapping
FRED_TIPS_SERIES = {
    "DFII5": "5Y",
    "DFII7": "7Y", 
    "DFII10": "10Y",
    "DFII20": "20Y",
    "DFII30": "30Y"
}

def collect_us_tips(database_url=None):
    """Collect US TIPS yields from FRED with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="us_tips_yields",
        default_lookback_days=25*365  # 25 years of historical data (covers full TIPS history)
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("US TIPS data is already up to date")
        return 0
    
    total_inserted = 0
    
    for fred_series, maturity in FRED_TIPS_SERIES.items():
        collector.logger.info(f"Collecting TIPS {maturity} data (series: {fred_series})")
        
        # Handle unlimited historical data fetch
        if start_date is None:
            # Fetch all available historical data - don't specify observation_start
            series_data = collector.get_series_data(
                fred_series,
                observation_end=end_date.strftime("%Y-%m-%d") if end_date else None
            )
        else:
            series_data = collector.get_series_data(
                fred_series,
                observation_start=start_date.strftime("%Y-%m-%d"),
                observation_end=end_date.strftime("%Y-%m-%d") if end_date else None
            )
        
        bulk_data = []
        for item in series_data:
            # Skip null/missing data points
            if item.get('value') == '.' or item.get('value') is None:
                continue
                
            try:
                yield_rate = float(item['value'])
                date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                
                bulk_data.append({
                    'date': date,
                    'maturity': maturity,
                    'maturity_years': float(maturity.replace('Y', '')),
                    'yield_rate': yield_rate
                })
            except (ValueError, TypeError) as e:
                collector.logger.warning(f"Skipping invalid data point for {fred_series}: {item} - {e}")
                continue
        
        if bulk_data:
            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS us_tips_yields (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                maturity VARCHAR(10) NOT NULL,
                maturity_years DECIMAL(4,1) NOT NULL,
                yield_rate DECIMAL(8,4) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, maturity)
            );
            
            CREATE INDEX IF NOT EXISTS idx_us_tips_yields_date ON us_tips_yields(date);
            CREATE INDEX IF NOT EXISTS idx_us_tips_yields_maturity ON us_tips_yields(maturity);
            CREATE INDEX IF NOT EXISTS idx_us_tips_yields_date_maturity ON us_tips_yields(date, maturity);
            """
            
            collector.execute_sql(create_table_sql)
            
            # Bulk insert with ON CONFLICT handling
            insert_sql = """
            INSERT INTO us_tips_yields (date, maturity, maturity_years, yield_rate)
            VALUES (%(date)s, %(maturity)s, %(maturity_years)s, %(yield_rate)s)
            ON CONFLICT (date, maturity) DO UPDATE SET
                yield_rate = EXCLUDED.yield_rate,
                created_at = CURRENT_TIMESTAMP
            """
            
            inserted = collector.bulk_insert(insert_sql, bulk_data)
            total_inserted += inserted
            
            collector.logger.info(f"Inserted {inserted} records for TIPS {maturity}")
        else:
            collector.logger.warning(f"No valid data found for TIPS {maturity}")
    
    collector.logger.info(f"US TIPS collection completed. Total records inserted: {total_inserted}")
    return total_inserted

def collect_us_tips_single_series(series_id: str, database_url=None):
    """Collect a single TIPS series for testing or specific needs."""
    if series_id not in FRED_TIPS_SERIES:
        raise ValueError(f"Unknown TIPS series: {series_id}. Available: {list(FRED_TIPS_SERIES.keys())}")
    
    collector = FREDCollector(database_url)
    maturity = FRED_TIPS_SERIES[series_id]
    
    collector.logger.info(f"Collecting single TIPS series: {series_id} ({maturity})")
    
    # Get all available data for this series
    series_data = collector.get_series_data(series_id, limit=50000)
    
    bulk_data = []
    for item in series_data:
        if item.get('value') == '.' or item.get('value') is None:
            continue
            
        try:
            yield_rate = float(item['value'])
            date = datetime.strptime(item['date'], '%Y-%m-%d').date()
            
            bulk_data.append({
                'date': date,
                'maturity': maturity,
                'maturity_years': float(maturity.replace('Y', '')),
                'yield_rate': yield_rate
            })
        except (ValueError, TypeError) as e:
            collector.logger.warning(f"Skipping invalid data point: {item} - {e}")
            continue
    
    collector.logger.info(f"Collected {len(bulk_data)} valid observations for {series_id}")
    return bulk_data
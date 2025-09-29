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

# FRED Forward Inflation Expectation Series
FRED_FORWARD_INFLATION_SERIES = {
    "T5YIFR": "5Y5Y"  # 5-Year, 5-Year Forward Inflation Expectation Rate
}

def collect_us_tips(database_url=None):
    """Collect US TIPS yields and forward inflation expectations from FRED with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection (TIPS table)
    start_date, end_date = collector.get_date_range_for_collection(
        table="us_tips_yields",
        default_lookback_days=25*365  # 25 years of historical data (covers full TIPS history)
    )
    
    # Get date range for forward inflation expectations
    forward_start_date, forward_end_date = collector.get_date_range_for_collection(
        table="us_forward_inflation_expectations",
        default_lookback_days=20*365  # 20 years of historical data
    )
    
    if (start_date is None and end_date is None and 
        forward_start_date is None and forward_end_date is None):
        collector.logger.info("US TIPS and forward inflation data is already up to date")
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
            # Bulk upsert with conflict resolution
            inserted = collector.bulk_upsert_data(
                table="us_tips_yields",
                data_list=bulk_data,
                conflict_columns=["date", "maturity"]
            )
            total_inserted += inserted
            
            collector.logger.info(f"Inserted {inserted} records for TIPS {maturity}")
        else:
            collector.logger.warning(f"No valid data found for TIPS {maturity}")
    
    # Collect forward inflation expectations
    for fred_series, maturity_label in FRED_FORWARD_INFLATION_SERIES.items():
        collector.logger.info(f"Collecting forward inflation {maturity_label} data (series: {fred_series})")
        
        # Handle unlimited historical data fetch for forward inflation
        if forward_start_date is None:
            series_data = collector.get_series_data(
                fred_series,
                observation_end=forward_end_date.strftime("%Y-%m-%d") if forward_end_date else None
            )
        else:
            series_data = collector.get_series_data(
                fred_series,
                observation_start=forward_start_date.strftime("%Y-%m-%d"),
                observation_end=forward_end_date.strftime("%Y-%m-%d") if forward_end_date else None
            )
        
        bulk_data = []
        for item in series_data:
            # Skip null/missing data points
            if item.get('value') == '.' or item.get('value') is None:
                continue
                
            try:
                expectation_rate = float(item['value'])
                date = datetime.strptime(item['date'], '%Y-%m-%d').date()
                
                bulk_data.append({
                    'date': date,
                    'maturity_label': maturity_label,
                    'expectation_rate': expectation_rate,
                    'series_id': fred_series
                })
            except (ValueError, TypeError) as e:
                collector.logger.warning(f"Skipping invalid data point for {fred_series}: {item} - {e}")
                continue
        
        if bulk_data:
            # Bulk upsert with conflict resolution
            inserted = collector.bulk_upsert_data(
                table="us_forward_inflation_expectations", 
                data_list=bulk_data,
                conflict_columns=["date", "series_id"]
            )
            total_inserted += inserted
            
            collector.logger.info(f"Inserted {inserted} records for forward inflation {maturity_label}")
        else:
            collector.logger.warning(f"No valid data found for forward inflation {maturity_label}")
    
    collector.logger.info(f"US TIPS and forward inflation collection completed. Total records inserted: {total_inserted}")
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
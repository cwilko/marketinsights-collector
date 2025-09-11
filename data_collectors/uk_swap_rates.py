"""
UK Interest Rate Swap data collector using investiny.

This module requires additional dependencies:
- investiny>=0.7.2 (for swap rate data from investing.com)

Install with: pip install -e .[uk_swaps]

Collects GBP Interest Rate Swap curves for major maturities.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import logging
from .base import BaseCollector

try:
    from investiny import search_assets, historical_data
except ImportError:
    search_assets = None
    historical_data = None


class UKSwapRatesCollector(BaseCollector):
    """
    Collector for UK GBP Interest Rate Swap curves from investing.com.
    Scrapes swap rates for major maturities and stores historical data.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        
        # UK GBP Interest Rate Swap symbols and their investing.com ticker IDs
        self.swap_symbols = {
            '2Y': {
                'symbol': 'GBPSB6L2Y=',
                'ticker_id': '1156493',
                'description': 'GBP 2 Years IRS Interest Rate Swap'
            },
            '5Y': {
                'symbol': 'GBPSB6L5Y=', 
                'ticker_id': '1156495',
                'description': 'GBP 5 Years IRS Interest Rate Swap'
            },
            '10Y': {
                'symbol': 'GBPSB6L10Y=',
                'ticker_id': '1156497', 
                'description': 'GBP 10 Years IRS Interest Rate Swap'
            },
            '30Y': {
                'symbol': 'GBPSB6L30Y=',
                'ticker_id': '1156505',
                'description': 'GBP 30 Years IRS Interest Rate Swap'
            }
        }
        
    def _check_dependencies(self):
        """Check if required dependencies are available."""
        if search_assets is None or historical_data is None:
            raise ImportError(
                "investiny package is required for UK swap rates collection. "
                "Install with: pip install investiny>=0.7.2"
            )
    
    def get_swap_data(self, maturity: str, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get historical swap rate data for specified maturity.
        
        Args:
            maturity: Swap maturity ('2Y', '5Y', '10Y', '30Y')
            start_date: Start date for data collection. If None, gets full history.
            
        Returns:
            List of swap rate records
        """
        self._check_dependencies()
        
        if maturity not in self.swap_symbols:
            raise ValueError(f"Unsupported maturity: {maturity}. Supported: {list(self.swap_symbols.keys())}")
        
        swap_info = self.swap_symbols[maturity]
        ticker_id = swap_info['ticker_id']
        
        try:
            if start_date is None:
                # Get full history from May 2018 (when swap data starts)
                self.logger.info(f"Fetching full history for {maturity} GBP swap rates from May 2018")
                historical = historical_data(
                    investing_id=ticker_id,
                    from_date='01/05/2018',  # Full history from May 2018
                    to_date=datetime.now().strftime('%d/%m/%Y')
                )
            else:
                # Get incremental data from start_date
                end_date = datetime.now()
                self.logger.info(f"Fetching {maturity} GBP swap rates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                historical = historical_data(
                    investing_id=ticker_id,
                    from_date=start_date.strftime('%d/%m/%Y'),
                    to_date=end_date.strftime('%d/%m/%Y')
                )
            
            if not isinstance(historical, dict) or 'date' not in historical:
                self.logger.warning(f"No data returned for {maturity} swap rates")
                return []
            
            # Convert to list of records
            records = []
            dates = historical['date']
            opens = historical['open']
            highs = historical['high'] 
            lows = historical['low']
            closes = historical['close']
            
            for i in range(len(dates)):
                try:
                    # Parse date (MM/DD/YYYY format from investiny)
                    obs_date = datetime.strptime(dates[i], '%m/%d/%Y').date()
                    
                    record = {
                        'date': obs_date,
                        'maturity': maturity,
                        'maturity_years': self._maturity_to_years(maturity),
                        'open_rate': float(opens[i]),
                        'high_rate': float(highs[i]),
                        'low_rate': float(lows[i]), 
                        'close_rate': float(closes[i]),
                        'source': 'investiny',
                        'symbol': swap_info['symbol']
                    }
                    records.append(record)
                    
                except Exception as e:
                    self.logger.debug(f"Error processing {maturity} swap data point {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully processed {len(records)} {maturity} swap rate records")
            return records
            
        except Exception as e:
            self.logger.error(f"Error fetching {maturity} swap rates: {str(e)}")
            return []
    
    def _maturity_to_years(self, maturity: str) -> float:
        """Convert maturity string to years."""
        maturity_map = {
            '2Y': 2.0,
            '5Y': 5.0, 
            '10Y': 10.0,
            '30Y': 30.0
        }
        return maturity_map.get(maturity, 0.0)
    
    def collect_all_swap_rates(self, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Collect swap rates for all maturities.
        
        Args:
            start_date: Start date for data collection. If None, gets full history.
            
        Returns:
            List of all swap rate records across all maturities
        """
        all_records = []
        
        for maturity in self.swap_symbols.keys():
            try:
                records = self.get_swap_data(maturity, start_date)
                all_records.extend(records)
            except Exception as e:
                self.logger.error(f"Failed to collect {maturity} swap rates: {str(e)}")
                continue
        
        return all_records


def collect_uk_swap_rates(database_url=None):
    """
    Collect UK GBP Interest Rate Swap curves.
    
    Args:
        database_url: Database connection URL. If None, returns count without storing.
        
    Returns:
        Number of records processed
    """
    collector = UKSwapRatesCollector(database_url)
    
    try:
        # Determine if this is initial load or incremental update
        start_date = None
        if database_url:
            try:
                # Check for existing data to determine collection strategy
                import psycopg2
                conn = psycopg2.connect(database_url)
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(date) FROM uk_swap_rates LIMIT 1")
                result = cursor.fetchone()
                latest_date = result[0] if result else None
                cursor.close()
                conn.close()
                
                if latest_date is not None:
                    # Incremental update - get data from day after latest date
                    from datetime import timedelta
                    start_date = latest_date + timedelta(days=1)
                    collector.logger.info(f"Found existing data - collecting incrementally from {start_date}")
                else:
                    collector.logger.info("No existing data - collecting full history")
                    
            except Exception as e:
                collector.logger.info(f"Could not check existing data ({e}) - collecting full history")
                start_date = None
        
        # Collect swap rate data
        swap_data = collector.collect_all_swap_rates(start_date)
        
        if not swap_data:
            collector.logger.error("No UK swap rate data found - this indicates API failure or connection issues")
            raise RuntimeError("Failed to collect any UK swap rate data - check API connectivity and permissions")
        
        # Process and store data
        if database_url:
            success_count = collector.bulk_upsert_data(
                "uk_swap_rates",
                swap_data,
                conflict_columns=['date', 'maturity']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(swap_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} UK swap rate records")
            return success_count
        else:
            collector.logger.info(f"Successfully processed {len(swap_data)} UK swap rate records (not stored)")
            return len(swap_data)
            
    except Exception as e:
        collector.logger.error(f"Failed to collect UK swap rates: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task
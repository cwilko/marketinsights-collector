"""
ETF Price data collector using investiny for investing.com.

This module requires additional dependencies:
- investiny>=0.7.2 (for ETF price data from investing.com)

Install with: pip install -e .[etf_prices]

Collects ETF price data for arbitrage analysis.
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


class ETFPricesCollector(BaseCollector):
    """
    Collector for ETF price data from investing.com.
    Scrapes ETF prices for arbitrage analysis and premium/discount calculations.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        
        # ETF symbols and their investing.com ticker IDs
        self.etf_symbols = {
            'IGLT': {
                'symbol': 'iShares Core UK Gilts UCITS ETF',
                'ticker_id': '38403',  # From investing.com (corrected)
                'description': 'iShares Core UK Gilts UCITS ETF',
                'currency': 'GBP',
                'provider': 'iShares'
            },
            'INXG': {
                'symbol': 'iShares UK Index-Linked Gilts UCITS ETF',
                'ticker_id': '38411',  # From investing.com (corrected)
                'description': 'iShares UK Index-Linked Gilts UCITS ETF',
                'currency': 'GBP',
                'provider': 'iShares'
            },
            'VGOV': {
                'symbol': 'Vanguard UK Government Bond UCITS ETF',
                'ticker_id': '45747',  # From investing.com (corrected)
                'description': 'Vanguard UK Government Bond UCITS ETF',
                'currency': 'GBP',
                'provider': 'Vanguard'
            },
            'GLTY': {
                'symbol': 'SPDR Bloomberg UK Gilt UCITS ETF',
                'ticker_id': '45552',  # From investing.com (corrected)
                'description': 'SPDR Bloomberg UK Gilt UCITS ETF',
                'currency': 'GBP',
                'provider': 'SSGA'
            }
        }
        
    def _check_dependencies(self):
        """Check if required dependencies are available."""
        if search_assets is None or historical_data is None:
            raise ImportError(
                "investiny package is required for ETF prices collection. "
                "Install with: pip install investiny>=0.7.2"
            )
    
    def get_etf_price_data(self, etf_ticker: str, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Get historical ETF price data for specified ticker.
        
        Args:
            etf_ticker: ETF ticker ('IGLT', 'INXG', 'VGOV', 'GLTY')
            start_date: Start date for data collection. If None, gets full history.
            
        Returns:
            List of ETF price records
        """
        self._check_dependencies()
        
        if etf_ticker not in self.etf_symbols:
            raise ValueError(f"Unsupported ETF ticker: {etf_ticker}. Supported: {list(self.etf_symbols.keys())}")
        
        etf_info = self.etf_symbols[etf_ticker]
        ticker_id = etf_info['ticker_id']
        
        try:
            if start_date is None:
                # Get full history from 2009 (when most ETFs started)
                self.logger.info(f"Fetching full history for {etf_ticker} ETF prices from 2009")
                historical = historical_data(
                    investing_id=ticker_id,
                    from_date='01/01/2009',  # MM/DD/YYYY format - January 1, 2009
                    to_date=datetime.now().strftime('%m/%d/%Y'),  # MM/DD/YYYY format
                    interval='D'
                )
            else:
                # Get incremental data from start_date
                end_date = datetime.now()
                self.logger.info(f"Fetching {etf_ticker} ETF prices from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                historical = historical_data(
                    investing_id=ticker_id,
                    from_date=start_date.strftime('%m/%d/%Y'),  # MM/DD/YYYY format
                    to_date=end_date.strftime('%m/%d/%Y'),      # MM/DD/YYYY format
                    interval='D'
                )
            
            if not isinstance(historical, dict) or 'date' not in historical:
                self.logger.warning(f"No data returned for {etf_ticker} ETF prices")
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
                        'etf_ticker': etf_ticker,
                        'open_price': float(opens[i]),
                        'high_price': float(highs[i]),
                        'low_price': float(lows[i]), 
                        'close_price': float(closes[i]),
                        'currency': etf_info['currency'],
                        'provider': etf_info['provider'],
                        'data_source': 'investing.com',
                        'symbol': etf_info['symbol']
                    }
                    records.append(record)
                    
                except Exception as e:
                    self.logger.debug(f"Error processing {etf_ticker} ETF price data point {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully processed {len(records)} {etf_ticker} ETF price records")
            return records
            
        except Exception as e:
            self.logger.error(f"Error fetching {etf_ticker} ETF prices: {str(e)}")
            return []
    
    def collect_all_etf_prices(self, start_date: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Collect ETF prices for all configured tickers.
        
        Args:
            start_date: Start date for data collection. If None, gets full history.
            
        Returns:
            List of all ETF price records across all tickers
        """
        import time
        all_records = []
        
        for i, etf_ticker in enumerate(self.etf_symbols.keys()):
            try:
                # Add 5-second delay between requests to avoid rate limiting (except for first request)
                if i > 0:
                    self.logger.info(f"Waiting 5 seconds before fetching {etf_ticker} data to avoid rate limiting...")
                    time.sleep(5)
                
                records = self.get_etf_price_data(etf_ticker, start_date)
                all_records.extend(records)
            except Exception as e:
                self.logger.error(f"Failed to collect {etf_ticker} ETF prices: {str(e)}")
                continue
        
        return all_records
    
    def store_etf_price_data(self, etf_price_data: List[Dict[str, Any]]) -> int:
        """Store ETF price data in database."""
        if not etf_price_data:
            return 0
        
        # Bulk upsert to etf_price_history table
        success_count = self.bulk_upsert_data(
            "etf_price_history",
            etf_price_data,
            conflict_columns=['date', 'etf_ticker']
        )
        return success_count


def collect_etf_prices(database_url=None, etf_tickers=None):
    """
    Collect ETF price data from investing.com.
    
    Args:
        database_url: Database connection URL. If None, returns count without storing.
        etf_tickers: List of ETF tickers to collect. If None, collects all configured.
        
    Returns:
        Number of records processed
    """
    collector = ETFPricesCollector(database_url)
    
    try:
        # Determine if this is initial load or incremental update
        start_date = None
        if database_url:
            try:
                # Check for existing data to determine collection strategy
                import psycopg2
                conn = psycopg2.connect(database_url)
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(date) FROM etf_price_history LIMIT 1")
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
        
        # Collect ETF price data
        if etf_tickers:
            # Collect specific tickers
            all_records = []
            for i, ticker in enumerate(etf_tickers):
                # Add 5-second delay between requests to avoid rate limiting (except for first request)
                if i > 0:
                    collector.logger.info(f"Waiting 5 seconds before fetching {ticker} data to avoid rate limiting...")
                    import time
                    time.sleep(5)
                
                records = collector.get_etf_price_data(ticker, start_date)
                all_records.extend(records)
            etf_price_data = all_records
        else:
            # Collect all tickers
            etf_price_data = collector.collect_all_etf_prices(start_date)
        
        if not etf_price_data:
            collector.logger.error("No ETF price data found - this indicates API failure or connection issues")
            raise RuntimeError("Failed to collect any ETF price data - check API connectivity and permissions")
        
        # Process and store data
        if database_url:
            success_count = collector.store_etf_price_data(etf_price_data)
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(etf_price_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} ETF price records")
            return success_count
        else:
            collector.logger.info(f"Successfully processed {len(etf_price_data)} ETF price records (not stored)")
            return len(etf_price_data)
            
    except Exception as e:
        collector.logger.error(f"Failed to collect ETF prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task
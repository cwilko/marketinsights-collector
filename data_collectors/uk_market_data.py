"""
UK Market Data Collectors

Collectors for UK financial market data including FTSE indices,
gilt yields, and GBP/USD exchange rates using MarketWatch for indices 
and Alpha Vantage for forex data.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import pandas as pd
import io
from .base import BaseCollector

class UKMarketCollector(BaseCollector):
    """Base collector for UK market data using multiple sources."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)

class MarketWatchFTSECollector(UKMarketCollector):
    """FTSE 100 collector using MarketWatch for incremental updates and CSV file for historical data."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.marketwatch.com/investing/index/ukx/downloaddatapartial"
        self.historical_csv_path = "/Users/cwilkin/Documents/Development/repos/econometrics/FTSE 100 Historical Results Price Data.csv"
        
    def get_ftse_100_data(self, days_back: int = 365) -> List[Dict]:
        """
        Get FTSE 100 index data from MarketWatch for incremental updates.
        
        Args:
            days_back: Number of days back to fetch data (default 365 for 1 year)
        """
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # MarketWatch expects MM/DD/YYYY HH:mm:ss format
        start_str = start_date.strftime("%m/%d/%Y %H:%M:%S")
        end_str = end_date.strftime("%m/%d/%Y %H:%M:%S")
        
        params = {
            "startdate": start_str,
            "enddate": end_str,
            "daterange": f"d{days_back}",
            "frequency": "p1d",  # Daily data
            "csvdownload": "true",
            "downloadpartial": "false",
            "newdates": "false",
            "countrycode": "uk"
        }
        
        try:
            self.logger.info(f"Fetching FTSE 100 data from MarketWatch (period: {days_back} days)")
            
            # Make request with CSV headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'Accept': 'text/csv,text/plain,*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Referer': 'https://www.marketwatch.com/investing/index/ukx'
            }
            
            # Use session.get directly to get CSV content
            response = self.session.get(self.base_url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data
            csv_content = response.text
            if not csv_content or "Date" not in csv_content:
                self.logger.warning("No valid CSV data returned from MarketWatch")
                return []
            
            # Read CSV using pandas, handling thousands separator
            df = pd.read_csv(io.StringIO(csv_content), thousands=',')
            
            if df.empty:
                self.logger.warning("Empty DataFrame from MarketWatch CSV")
                return []
            
            # Convert to list of dictionaries
            ftse_data = []
            for _, row in df.iterrows():
                try:
                    # MarketWatch date format is typically MM/DD/YYYY
                    date = pd.to_datetime(row['Date']).date()
                    
                    # Skip if any essential data is missing
                    if pd.isna(row['Open']) or pd.isna(row['High']) or pd.isna(row['Low']) or pd.isna(row['Close']):
                        continue
                    
                    # Convert string values with commas to float
                    open_val = float(str(row['Open']).replace(',', '')) if not pd.isna(row['Open']) else None
                    high_val = float(str(row['High']).replace(',', '')) if not pd.isna(row['High']) else None
                    low_val = float(str(row['Low']).replace(',', '')) if not pd.isna(row['Low']) else None
                    close_val = float(str(row['Close']).replace(',', '')) if not pd.isna(row['Close']) else None
                    
                    if any(val is None for val in [open_val, high_val, low_val, close_val]):
                        continue
                    
                    # Parse volume with improved handling for B/M/K suffixes
                    volume = 0
                    if 'Volume' in row and not pd.isna(row['Volume']):
                        volume_str = str(row['Volume']).replace(',', '').strip()
                        try:
                            if 'B' in volume_str:
                                volume = int(float(volume_str.replace('B', '')) * 1000000000)
                            elif 'M' in volume_str:
                                volume = int(float(volume_str.replace('M', '')) * 1000000)
                            elif 'K' in volume_str:
                                volume = int(float(volume_str.replace('K', '')) * 1000)
                            else:
                                volume = int(float(volume_str))
                        except (ValueError, TypeError):
                            volume = 0
                    
                    ftse_data.append({
                        "date": date,
                        "open": open_val,
                        "high": high_val,
                        "low": low_val,
                        "close": close_val,
                        "volume": volume
                    })
                    
                except (ValueError, TypeError, KeyError) as e:
                    self.logger.debug(f"Skipping invalid FTSE data point: {str(e)}")
                    continue
            
            # Sort by date
            ftse_data.sort(key=lambda x: x["date"])
            
            self.logger.info(f"Retrieved {len(ftse_data)} FTSE 100 data points from MarketWatch")
            return ftse_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch FTSE 100 data from MarketWatch: {str(e)}")
            return []
    
    def get_gbp_usd_rate(self, outputsize="compact") -> List[Dict]:
        """Get GBP/USD exchange rate data."""
        if not self.api_key:
            self.logger.warning("Alpha Vantage API key not set")
            return []
            
        params = {
            "function": "FX_DAILY",
            "from_symbol": "GBP",
            "to_symbol": "USD",
            "apikey": self.api_key,
            "outputsize": outputsize
        }
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "Time Series (FX Daily)" in data:
                time_series = data["Time Series (FX Daily)"]
                self.logger.info(f"Retrieved {len(time_series)} GBP/USD data points")
                return time_series
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch GBP/USD data: {str(e)}")
            return []

def collect_ftse_100(database_url=None):
    """Collect FTSE 100 index data with incremental updates using MarketWatch."""
    collector = MarketWatchFTSECollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="ftse_100_index",
        default_lookback_days=5*365  # 5 years of historical data
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("FTSE 100 data is already up to date")
        return 0
    
    # Determine days_back based on date range
    if start_date is None:
        # Get full historical data for initial collection (but data is already loaded)
        # MarketWatch is limited to ~1 year, so just get recent data for validation
        days_back = 90  # Last 90 days for validation
    else:
        # Calculate days needed for incremental updates
        days_back = (end_date - start_date).days + 30  # Add buffer
        
        # CRITICAL: MarketWatch only returns max 1 year of data
        if days_back > 365:
            collector.logger.warning(f"Data gap is {days_back} days, but MarketWatch only provides 1 year max. Manual data loading required for gaps > 365 days.")
            collector.logger.warning(f"Last database date: {start_date - timedelta(days=1)}, Current date: {end_date}")
            collector.logger.warning("Consider using CSV data or other sources to fill large gaps")
            days_back = 365  # Get what we can (last 365 days)
    
    ftse_data = collector.get_ftse_100_data(days_back=days_back)
    
    if not ftse_data:
        collector.logger.info("No FTSE 100 data retrieved from MarketWatch")
        return 0
    
    bulk_data = []
    for item in ftse_data:
        try:
            date = item["date"]
            
            # Only process data within our target date range
            if (start_date and date < start_date) or date > end_date:
                continue
                
            data = {
                "date": date,
                "open_price": item["open"],
                "high_price": item["high"], 
                "low_price": item["low"],
                "close_price": item["close"],
                "volume": item["volume"]
            }
            bulk_data.append(data)
            
        except Exception as e:
            collector.logger.error(f"Error processing FTSE 100 data for {item.get('date', 'unknown')}: {str(e)}")
    
    # Sort by date for consistency
    bulk_data.sort(key=lambda x: x["date"])
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("ftse_100_index", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} FTSE 100 records")
        return success_count
    else:
        collector.logger.info("No valid FTSE 100 data to process")
        return 0

def collect_gbp_usd_rate(database_url=None):
    """Collect GBP/USD exchange rate data with incremental updates."""
    collector = AlphaVantageUKCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="gbp_usd_exchange_rate",
        default_lookback_days=2*365  # 2 years of historical data
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("GBP/USD exchange rate data is already up to date")
        return 0
    
    # Determine if we need full or compact data
    outputsize = "full" if start_date is None else "compact"
    
    time_series_data = collector.get_gbp_usd_rate(outputsize)
    
    if not time_series_data:
        collector.logger.info("No GBP/USD data retrieved")
        return 0
    
    bulk_data = []
    for date_str, daily_data in time_series_data.items():
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # Only process data within our target date range
            if (start_date and date < start_date) or date > end_date:
                continue
                
            data = {
                "date": date,
                "exchange_rate": float(daily_data["4. close"])  # Using close rate
            }
            bulk_data.append(data)
            
        except Exception as e:
            collector.logger.error(f"Error processing GBP/USD data for {date_str}: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("gbp_usd_exchange_rate", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} GBP/USD records")
        return success_count
    else:
        collector.logger.info("No valid GBP/USD data to process")
        return 0
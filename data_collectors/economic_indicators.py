import json
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .base import BaseCollector

class BLSCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data"
        self.api_key = self.get_env_var("BLS_API_KEY", required=False)
        
    def get_series_data(self, series_id: str, start_year: int = None, end_year: int = None) -> List[Dict]:
        """
        Get BLS time series data with support for multi-year bulk fetching.
        BLS API supports up to 20 years of data in a single request with API key.
        """
        if start_year is None:
            start_year = datetime.now().year - 1
        if end_year is None:
            end_year = datetime.now().year
            
        # BLS API limits: 20 years with key, 10 years without
        max_years = 20 if self.api_key else 10
        
        all_data = []
        
        # Split large date ranges into chunks if necessary
        current_start = start_year
        while current_start <= end_year:
            current_end = min(current_start + max_years - 1, end_year)
            
            payload = {
                "seriesid": [series_id],
                "startyear": str(current_start),
                "endyear": str(current_end),
            }
            
            if self.api_key:
                payload["registrationkey"] = self.api_key
                
            headers = {'Content-Type': 'application/json'}
            
            try:
                response = self.session.post(
                    self.base_url, 
                    data=json.dumps(payload),
                    headers=headers,
                    timeout=30
                )
                response.raise_for_status()
                data = response.json()
                
                if data.get("status") == "REQUEST_SUCCEEDED":
                    batch_data = data["Results"]["series"][0]["data"]
                    all_data.extend(batch_data)
                    self.logger.info(f"Retrieved {len(batch_data)} observations for {series_id} ({current_start}-{current_end})")
                else:
                    self.logger.error(f"BLS API error: {data.get('message', 'Unknown error')}")
                    break
                    
            except Exception as e:
                self.logger.error(f"Failed to fetch BLS data for series {series_id} ({current_start}-{current_end}): {str(e)}")
                break
            
            current_start = current_end + 1
            
            # Add delay between requests to respect rate limits
            if current_start <= end_year:
                time.sleep(1)
        
        self.logger.info(f"Total retrieved {len(all_data)} observations for {series_id}")
        return all_data

class FREDCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        self.api_key = self.get_env_var("FRED_API_KEY")
        
    def get_series_data(self, series_id: str, limit: int = 100000, 
                       observation_start: str = None, observation_end: str = None) -> List[Dict]:
        """
        Get FRED time series data with bulk fetching support.
        Uses higher default limit for bulk operations and date range filtering.
        """
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "limit": limit,
            "sort_order": "asc"  # Changed to ascending for chronological processing
        }
        
        if observation_start:
            params["observation_start"] = observation_start
        if observation_end:
            params["observation_end"] = observation_end
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "observations" in data:
                observations = data["observations"]
                self.logger.info(f"Retrieved {len(observations)} observations for {series_id}")
                return observations
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch FRED data for series {series_id}: {str(e)}")
            return []

class BEACollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://apps.bea.gov/api/data"
        self.api_key = self.get_env_var("BEA_API_KEY")
        
    def get_gdp_data(self) -> List[Dict]:
        """Get GDP data from BEA."""
        params = {
            "UserID": self.api_key,
            "Method": "GetData",
            "datasetname": "NIPA",
            "TableName": "T10101",
            "Frequency": "Q",
            "Year": "ALL",
            "ResultFormat": "json"
        }
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "BEAAPI" in data and "Results" in data["BEAAPI"]:
                return data["BEAAPI"]["Results"]["Data"]
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch BEA GDP data: {str(e)}")
            return []

def collect_cpi(database_url=None):
    """Collect Consumer Price Index data with incremental updates."""
    collector = BLSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="consumer_price_index",
        default_lookback_days=10*365  # 10 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("CPI data is already up to date")
        return 0
    
    # BLS uses years, so convert dates to year range
    start_year = start_date.year
    end_year = end_date.year
    
    series_data = collector.get_series_data("CUUR0000SA0", start_year, end_year)  # All items CPI-U
    
    bulk_data = []
    for item in series_data:
        try:
            # Parse BLS date format (YYYY + MM)
            year = int(item["year"])
            period = item["period"]
            if period.startswith("M"):
                month = int(period[1:])
                date = datetime(year, month, 1).date()
                
                # Only process data within our target date range
                if date < start_date or date > end_date:
                    continue
            else:
                continue  # Skip non-monthly data
                
            data = {
                "date": date,
                "value": float(item["value"]),
            }
            
            # Calculate year-over-year change if we have previous year data
            prev_year_data = [d for d in series_data 
                            if d["year"] == str(year-1) and d["period"] == period]
            if prev_year_data:
                prev_value = float(prev_year_data[0]["value"])
                data["year_over_year_change"] = ((float(item["value"]) - prev_value) / prev_value) * 100
            
            bulk_data.append(data)
                
        except Exception as e:
            collector.logger.error(f"Error processing CPI data item: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("consumer_price_index", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} CPI records")
        return success_count
    else:
        collector.logger.info("No valid CPI data to process")
        return 0

def collect_monthly_fed_funds_rate(database_url=None):
    """Collect monthly Federal Funds Rate data with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection (incremental or historical)
    start_date, end_date = collector.get_date_range_for_collection(
        table="federal_funds_rate",
        default_lookback_days=10*365  # 10 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("Federal Funds Rate data is already up to date")
        return 0
    
    # Fetch data with date range
    series_data = collector.get_series_data(
        "FEDFUNDS", 
        observation_start=start_date.strftime("%Y-%m-%d"),
        observation_end=end_date.strftime("%Y-%m-%d")
    )
    
    bulk_data = []
    for item in series_data:
        try:
            if item["value"] == ".":
                continue  # Skip missing values
                
            data = {
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "effective_rate": float(item["value"]),
            }
            bulk_data.append(data)
                
        except Exception as e:
            collector.logger.error(f"Error processing Fed Funds data item: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("federal_funds_rate", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} Fed Funds records")
        return success_count
    else:
        collector.logger.info("No valid Fed Funds data to process")
        return 0

def collect_unemployment_rate(database_url=None):
    """Collect Unemployment Rate data with incremental updates."""
    collector = BLSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="unemployment_rate",
        default_lookback_days=10*365  # 10 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("Unemployment data is already up to date")
        return 0
    
    # BLS uses years, so convert dates to year range
    start_year = start_date.year
    end_year = end_date.year
    
    series_data = collector.get_series_data("LNS14000000", start_year, end_year)  # Unemployment Rate
    
    bulk_data = []
    for item in series_data:
        try:
            year = int(item["year"])
            period = item["period"]
            if period.startswith("M"):
                month = int(period[1:])
                date = datetime(year, month, 1).date()
                
                # Only process data within our target date range
                if date < start_date or date > end_date:
                    continue
            else:
                continue
                
            data = {
                "date": date,
                "rate": float(item["value"]),
            }
            bulk_data.append(data)
                
        except Exception as e:
            collector.logger.error(f"Error processing unemployment data item: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("unemployment_rate", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} unemployment records")
        return success_count
    else:
        collector.logger.info("No valid unemployment data to process")
        return 0

def collect_daily_fed_funds_rate(database_url=None):
    """Collect daily Federal Funds Rate data from FRED (DFF series) with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="daily_federal_funds_rate",
        default_lookback_days=2*365  # 2 years of daily data
    )
    
    if start_date is None:
        collector.logger.info("Daily Federal Funds Rate data is already up to date")
        return 0
    
    series_data = collector.get_series_data(
        "DFF", 
        observation_start=start_date.strftime('%Y-%m-%d'),
        observation_end=end_date.strftime('%Y-%m-%d')
    )
    
    bulk_data = []
    for item in series_data:
        try:
            if item["value"] == ".":
                continue  # Skip missing values
                
            data = {
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "effective_rate": float(item["value"]),
            }
            bulk_data.append(data)
                
        except Exception as e:
            collector.logger.error(f"Error processing daily Fed Funds data item: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("daily_federal_funds_rate", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} daily Fed Funds records")
        return success_count
    else:
        collector.logger.info("No valid daily Fed Funds data to process")
        return 0


def collect_gdp(database_url=None):
    """Collect GDP data."""
    collector = BEACollector(database_url)
    gdp_data = collector.get_gdp_data()
    
    bulk_data = []
    for item in gdp_data:
        try:
            if item.get("LineDescription") != "Gross domestic product":
                continue
                
            # Parse quarter format (YYYY-QN)
            time_period = item["TimePeriod"]
            year, quarter = time_period.split("-Q")
            quarter_month = int(quarter) * 3  # Q1=3, Q2=6, Q3=9, Q4=12
            quarter_date = datetime(int(year), quarter_month, 1).date()
            
            data = {
                "quarter": quarter_date,
                "gdp_billions": float(item["DataValue"]),
            }
            bulk_data.append(data)
                
        except Exception as e:
            collector.logger.error(f"Error processing GDP data item: {str(e)}")
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("gross_domestic_product", bulk_data, conflict_columns=["quarter"])
        collector.logger.info(f"Successfully bulk upserted {success_count} GDP records")
        return success_count
    else:
        collector.logger.info("No valid GDP data to process")
        return 0
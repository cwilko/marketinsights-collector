import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .base import BaseCollector

class BLSCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data"
        self.api_key = self.get_env_var("BLS_API_KEY", required=False)
        
    def get_series_data(self, series_id: str, start_year: int = None, end_year: int = None) -> List[Dict]:
        """Get BLS time series data."""
        if start_year is None:
            start_year = datetime.now().year - 1
        if end_year is None:
            end_year = datetime.now().year
            
        payload = {
            "seriesid": [series_id],
            "startyear": str(start_year),
            "endyear": str(end_year),
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
                return data["Results"]["series"][0]["data"]
            else:
                self.logger.error(f"BLS API error: {data.get('message', 'Unknown error')}")
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to fetch BLS data for series {series_id}: {str(e)}")
            return []

class FREDCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.stlouisfed.org/fred/series/observations"
        self.api_key = self.get_env_var("FRED_API_KEY")
        
    def get_series_data(self, series_id: str, limit: int = 100) -> List[Dict]:
        """Get FRED time series data."""
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "limit": limit,
            "sort_order": "desc"
        }
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "observations" in data:
                return data["observations"]
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
    """Collect Consumer Price Index data."""
    collector = BLSCollector(database_url)
    series_data = collector.get_series_data("CUUR0000SA0")  # All items CPI-U
    
    success_count = 0
    for item in series_data:
        try:
            # Parse BLS date format (YYYY + MM)
            year = int(item["year"])
            period = item["period"]
            if period.startswith("M"):
                month = int(period[1:])
                date = datetime(year, month, 1).date()
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
                
            if collector.upsert_data("consumer_price_index", data):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing CPI data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} CPI records")
    return success_count

def collect_fed_funds_rate(database_url=None):
    """Collect Federal Funds Rate data."""
    collector = FREDCollector(database_url)
    series_data = collector.get_series_data("FEDFUNDS")
    
    success_count = 0
    for item in series_data:
        try:
            if item["value"] == ".":
                continue  # Skip missing values
                
            data = {
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "effective_rate": float(item["value"]),
            }
            
            if collector.upsert_data("federal_funds_rate", data):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing Fed Funds data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} Fed Funds records")
    return success_count

def collect_unemployment(database_url=None):
    """Collect Unemployment Rate data."""
    collector = BLSCollector(database_url)
    series_data = collector.get_series_data("LNS14000000")  # Unemployment Rate
    
    success_count = 0
    for item in series_data:
        try:
            year = int(item["year"])
            period = item["period"]
            if period.startswith("M"):
                month = int(period[1:])
                date = datetime(year, month, 1).date()
            else:
                continue
                
            data = {
                "date": date,
                "rate": float(item["value"]),
            }
            
            if collector.upsert_data("unemployment_rate", data):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing unemployment data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} unemployment records")
    return success_count

def collect_gdp(database_url=None):
    """Collect GDP data."""
    collector = BEACollector(database_url)
    gdp_data = collector.get_gdp_data()
    
    success_count = 0
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
            
            if collector.upsert_data("gross_domestic_product", data, conflict_columns=["quarter"]):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing GDP data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} GDP records")
    return success_count
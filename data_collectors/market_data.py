import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .base import BaseCollector
from .economic_indicators import FREDCollector

# FRED Treasury Series Mapping
FRED_TREASURY_SERIES = {
    "DGS1MO": "1M",
    "DGS3MO": "3M", 
    "DGS6MO": "6M",
    "DGS1": "1Y",
    "DGS2": "2Y",
    "DGS5": "5Y",
    "DGS7": "7Y",
    "DGS10": "10Y",
    "DGS20": "20Y",
    "DGS30": "30Y"
}

def collect_sp500(database_url=None):
    """Collect S&P 500 Index data from FRED with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="sp500_index",
        default_lookback_days=10*365  # 10 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("S&P 500 data is already up to date")
        return 0
    
    series_data = collector.get_series_data(
        "SP500",
        observation_start=start_date.strftime("%Y-%m-%d"),
        observation_end=end_date.strftime("%Y-%m-%d")
    )
    
    success_count = 0
    for item in series_data:
        try:
            if item["value"] == ".":
                continue
                
            data = {
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "close_price": float(item["value"]),
                "open_price": float(item["value"]),  # FRED only provides closing prices
                "high_price": float(item["value"]),
                "low_price": float(item["value"]),
            }
            
            if collector.upsert_data("sp500_index", data):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing S&P 500 data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} S&P 500 records")
    return success_count

def collect_vix(database_url=None):
    """Collect VIX data from FRED with incremental updates."""
    collector = FREDCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="vix_index",
        default_lookback_days=10*365  # 10 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("VIX data is already up to date")
        return 0
    
    series_data = collector.get_series_data(
        "VIXCLS",
        observation_start=start_date.strftime("%Y-%m-%d"),
        observation_end=end_date.strftime("%Y-%m-%d")
    )
    
    success_count = 0
    for item in series_data:
        try:
            if item["value"] == ".":
                continue
                
            data = {
                "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                "close_price": float(item["value"]),
                "open_price": float(item["value"]),
                "high_price": float(item["value"]),
                "low_price": float(item["value"]),
            }
            
            if collector.upsert_data("vix_index", data):
                success_count += 1
                
        except Exception as e:
            collector.logger.error(f"Error processing VIX data item: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} VIX records")
    return success_count

class TreasuryCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.fiscaldata.treasury.gov/services/api/v1/accounting/od/daily_treasury_yield_curve"
        
    def get_yield_data(self, limit: int = 10000, date_range: str = None) -> List[Dict]:
        """
        Get Treasury yield curve data with bulk fetching support.
        date_range: Filter format like 'record_date:gte:2020-01-01,record_date:lte:2023-12-31'
        """
        params = {
            "format": "json",
            "sort": "record_date",  # Ascending for chronological processing
            "page[size]": limit
        }
        
        if date_range:
            params["filter"] = date_range
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "data" in data:
                records = data["data"]
                self.logger.info(f"Retrieved {len(records)} Treasury yield records")
                return records
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch Treasury yield data: {str(e)}")
            return []

def collect_treasury_yields(database_url=None):
    """Collect Treasury yield curve data with incremental updates."""
    collector = TreasuryCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="treasury_yields",
        default_lookback_days=5*365  # 5 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("Treasury yields data is already up to date")
        return 0
    
    # Build Treasury API date filter
    date_filter = f"record_date:gte:{start_date.strftime('%Y-%m-%d')},record_date:lte:{end_date.strftime('%Y-%m-%d')}"
    yield_data = collector.get_yield_data(date_range=date_filter)
    
    # Mapping of Treasury API fields to our maturity names
    maturity_mapping = {
        "1_mo": "1M",
        "2_mo": "2M", 
        "3_mo": "3M",
        "4_mo": "4M",
        "6_mo": "6M",
        "1_yr": "1Y",
        "2_yr": "2Y",
        "3_yr": "3Y",
        "5_yr": "5Y",
        "7_yr": "7Y",
        "10_yr": "10Y",
        "20_yr": "20Y",
        "30_yr": "30Y"
    }
    
    success_count = 0
    for record in yield_data:
        try:
            record_date = datetime.strptime(record["record_date"], "%Y-%m-%d").date()
            
            for api_field, maturity in maturity_mapping.items():
                if record.get(api_field) and record[api_field] != "":
                    data = {
                        "date": record_date,
                        "maturity": maturity,
                        "yield_rate": float(record[api_field])
                    }
                    
                    if collector.upsert_data("treasury_yields", data, conflict_columns=["date", "maturity"]):
                        success_count += 1
                        
        except Exception as e:
            collector.logger.error(f"Error processing Treasury yield data: {str(e)}")
            
    collector.logger.info(f"Successfully processed {success_count} Treasury yield records")
    return success_count


def collect_fred_treasury_yields(database_url=None):
    """
    Collect Treasury yield curve data from FRED with incremental updates.
    Uses multiple FRED series for different maturities.
    """
    collector = FREDCollector(database_url)
    
    # Get date range for collection (use new table)
    start_date, end_date = collector.get_date_range_for_collection(
        table="fred_treasury_yields",
        default_lookback_days=5*365  # 5 years of historical data
    )
    
    if start_date is None:
        collector.logger.info("FRED Treasury yields data is already up to date")
        return 0
    
    total_success_count = 0
    
    # Collect data for each Treasury series
    for series_id, maturity in FRED_TREASURY_SERIES.items():
        collector.logger.info(f"Collecting {maturity} Treasury yields (series: {series_id})")
        
        try:
            # Fetch data with date range for this specific series
            series_data = collector.get_series_data(
                series_id,
                observation_start=start_date.strftime("%Y-%m-%d"),
                observation_end=end_date.strftime("%Y-%m-%d")
            )
            
            series_success_count = 0
            for item in series_data:
                try:
                    if item["value"] == ".":
                        continue  # Skip missing values
                        
                    data = {
                        "date": datetime.strptime(item["date"], "%Y-%m-%d").date(),
                        "series_id": series_id,
                        "maturity": maturity,
                        "yield_rate": float(item["value"]),
                    }
                    
                    # Use series_id and date as conflict columns for FRED data
                    if collector.upsert_data("fred_treasury_yields", data, conflict_columns=["date", "series_id"]):
                        series_success_count += 1
                        
                except Exception as e:
                    collector.logger.error(f"Error processing {series_id} data item: {str(e)}")
                    
            collector.logger.info(f"Successfully processed {series_success_count} records for {maturity} yields")
            total_success_count += series_success_count
            
        except Exception as e:
            collector.logger.error(f"Failed to collect {series_id} ({maturity}) data: {str(e)}")
            continue
            
    collector.logger.info(f"Successfully processed {total_success_count} total Treasury yield records from FRED")
    return total_success_count


class PERatioCollector(BaseCollector):
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.multpl_url = "https://www.multpl.com/s-p-500-pe-ratio"
        self.shiller_url = "https://www.multpl.com/shiller-pe"
        
    def scrape_multpl_data(self, url: str) -> float:
        """Scrape P/E ratio data from multpl.com."""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the current value
            value_element = soup.find('div', {'id': 'current'})
            if value_element:
                value_text = value_element.text.strip()
                # Extract numeric value (remove any trailing text)
                import re
                match = re.search(r'([\d.]+)', value_text)
                if match:
                    return float(match.group(1))
                    
        except Exception as e:
            self.logger.error(f"Error scraping data from {url}: {str(e)}")
            
        return None

def collect_pe_ratios(database_url=None):
    """Collect P/E ratio data with daily updates."""
    collector = PERatioCollector(database_url)
    
    # Check if we already have today's data
    today = datetime.now().date()
    last_date = collector.get_last_record_date("pe_ratios")
    
    if last_date and last_date >= today:
        collector.logger.info("P/E ratios data is already up to date for today")
        return 0
    
    try:
        # Get current S&P 500 P/E ratio
        sp500_pe = collector.scrape_multpl_data(collector.multpl_url)
        
        # Get current Shiller P/E ratio
        shiller_pe = collector.scrape_multpl_data(collector.shiller_url)
        
        if sp500_pe or shiller_pe:
            data = {
                "date": today,
            }
            
            if sp500_pe:
                data["sp500_pe"] = sp500_pe
                
            if shiller_pe:
                data["sp500_shiller_pe"] = shiller_pe
                
            if collector.upsert_data("pe_ratios", data):
                collector.logger.info(f"Successfully collected P/E ratios: SP500={sp500_pe}, Shiller={shiller_pe}")
                return 1
                
    except Exception as e:
        collector.logger.error(f"Error collecting P/E ratios: {str(e)}")
        
    return 0
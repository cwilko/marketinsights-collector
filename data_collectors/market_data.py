import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List
from .base import BaseCollector
from .economic_indicators import FREDCollector

def collect_sp500(database_url=None):
    """Collect S&P 500 Index data from FRED."""
    collector = FREDCollector(database_url)
    series_data = collector.get_series_data("SP500")
    
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
    """Collect VIX data from FRED."""
    collector = FREDCollector(database_url)
    series_data = collector.get_series_data("VIXCLS")
    
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
        
    def get_yield_data(self, limit: int = 100) -> List[Dict]:
        """Get Treasury yield curve data."""
        params = {
            "format": "json",
            "sort": "-record_date",
            "page[size]": limit
        }
        
        try:
            data = self.make_request(self.base_url, params)
            if data and "data" in data:
                return data["data"]
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch Treasury yield data: {str(e)}")
            return []

def collect_treasury_yields(database_url=None):
    """Collect Treasury yield curve data."""
    collector = TreasuryCollector(database_url)
    yield_data = collector.get_yield_data()
    
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
    """Collect P/E ratio data."""
    collector = PERatioCollector(database_url)
    
    try:
        # Get current S&P 500 P/E ratio
        sp500_pe = collector.scrape_multpl_data(collector.multpl_url)
        
        # Get current Shiller P/E ratio
        shiller_pe = collector.scrape_multpl_data(collector.shiller_url)
        
        if sp500_pe or shiller_pe:
            data = {
                "date": datetime.now().date(),
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
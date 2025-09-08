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

class ONSCollector(BaseCollector):
    """Collector for UK Office for National Statistics data."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://api.beta.ons.gov.uk/v1"
        # ONS API is free and doesn't require API keys
        
    def get_datasets(self) -> List[Dict]:
        """Get available ONS datasets."""
        endpoint = f"{self.base_url}/datasets"
        try:
            data = self.make_request(endpoint, {})
            if data and "items" in data:
                self.logger.info(f"Retrieved {len(data['items'])} ONS datasets")
                return data["items"]
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch ONS datasets: {str(e)}")
            return []
    
    def get_latest_dataset_version(self, dataset_id: str, edition: str = "time-series") -> str:
        """Get the latest version number for a specific dataset and edition."""
        try:
            # Query the specific dataset edition endpoint to get latest version
            edition_endpoint = f"{self.base_url}/datasets/{dataset_id}/editions/{edition}"
            data = self.make_request(edition_endpoint, {})
            
            if data and "links" in data:
                links = data["links"]
                if "latest_version" in links:
                    latest_version_info = links["latest_version"]
                    if "href" in latest_version_info:
                        latest_version_href = latest_version_info["href"]
                        # Extract version number from URL
                        version_parts = latest_version_href.split('/')
                        if 'versions' in version_parts:
                            version_idx = version_parts.index('versions')
                            if version_idx + 1 < len(version_parts):
                                version = version_parts[version_idx + 1]
                                self.logger.info(f"Found latest version {version} for dataset {dataset_id} edition {edition}")
                                return version
            
            self.logger.warning(f"Could not determine latest version for dataset {dataset_id} edition {edition}")
            return "latest"
            
        except Exception as e:
            self.logger.error(f"Failed to get latest version for {dataset_id} edition {edition}: {str(e)}")
            return "latest"

    def get_dataset_data(self, dataset_id: str, version: str = None, edition: str = "time-series", 
                        time_constraint: str = "*", **dimensions) -> List[Dict]:
        """
        Get data for a specific ONS dataset with proper parameters.
        
        Args:
            dataset_id: ONS dataset identifier (e.g., 'cpih01', 'labour-market')
            version: Version number (if None, will automatically get latest version)
            edition: Dataset edition (default 'time-series', or 'PWT24', etc.)
            time_constraint: Time filter (default '*' for all time periods)
            **dimensions: Any dimension parameters (e.g., geography, aggregate, economicactivity, etc.)
        """
        # Always get the latest version if not specified
        if version is None:
            version = self.get_latest_dataset_version(dataset_id, edition)
        
        endpoint = f"{self.base_url}/datasets/{dataset_id}/editions/{edition}/versions/{version}/observations"
        
        params = {"time": time_constraint}
        # Add all dimension parameters
        params.update(dimensions)
            
        try:
            data = self.make_request(endpoint, params)
            if data and "observations" in data:
                observations = data["observations"]
                self.logger.info(f"Retrieved {len(observations)} observations for {dataset_id} v{version}")
                return observations
            elif data:
                self.logger.warning(f"No observations found for {dataset_id} v{version}. Response keys: {list(data.keys()) if isinstance(data, dict) else 'Not a dict'}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to fetch ONS data for {dataset_id} v{version}: {str(e)}")
            return []

class BankOfEnglandCollector(BaseCollector):
    """Collector for Bank of England interest rate and monetary policy data using IADB API."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
        # Bank of England IADB (Interactive Database) - based on datacareer.co.uk approach
        
    def get_bank_rate_data(self, series_code: str, start_date: str = "01/Jan/2000", 
                          end_date: str = None) -> List[Dict]:
        """
        Get Bank of England Bank Rate data using IADB API.
        
        Args:
            series_code: BoE series code (e.g., 'IUDBEDR' for daily, 'IUMABEDR' for monthly)
            start_date: Start date in DD/MON/YYYY format
            end_date: End date in DD/MON/YYYY format (defaults to today)
        """
        if end_date is None:
            from datetime import datetime
            end_date = datetime.now().strftime("%d/%b/%Y")
            
        url_endpoint = f"{self.base_url}?csv.x=yes"
        
        payload = {
            'Datefrom': start_date,
            'Dateto': end_date,
            'SeriesCodes': series_code,
            'CSVF': 'TN',  # Tabular format, no titles
            'UsingCodes': 'Y',
            'VPD': 'Y'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            self.logger.info(f"Fetching Bank Rate data for series {series_code} from {start_date} to {end_date}")
            response = self.session.get(url_endpoint, params=payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data using pandas for cleaner parsing
            import pandas as pd
            import io
            
            csv_data = response.text.strip()
            if not csv_data:
                self.logger.warning(f"Empty response for series {series_code}")
                return []
                
            try:
                # Use pandas to parse CSV data
                df = pd.read_csv(io.StringIO(csv_data))
                
                if df.empty:
                    self.logger.warning(f"No data rows found for series {series_code}")
                    return []
                
                # Extract data rows
                data_rows = []
                for _, row in df.iterrows():
                    try:
                        date_str = str(row.iloc[0]).strip()  # First column is date
                        rate_value = float(row.iloc[1])       # Second column is rate
                        
                        # Skip if rate is NaN or invalid
                        if pd.isna(rate_value):
                            continue
                            
                        data_rows.append({
                            'date': date_str,
                            'rate': rate_value
                        })
                        
                    except (ValueError, IndexError) as e:
                        self.logger.debug(f"Skipping invalid row: {str(e)}")
                        continue
                        
            except Exception as parse_error:
                self.logger.error(f"Failed to parse CSV data for {series_code}: {str(parse_error)}")
                # Fallback to manual CSV parsing
                import csv
                csv_reader = csv.reader(io.StringIO(csv_data))
                data_rows = []
                
                for i, row in enumerate(csv_reader):
                    if i == 0:  # Skip header
                        continue
                    if len(row) >= 2 and row[0] and row[1]:
                        try:
                            date_str = row[0].strip()
                            rate_str = row[1].strip()
                            
                            if not rate_str or rate_str in ['', 'N/A', '#N/A']:
                                continue
                                
                            rate_value = float(rate_str)
                            
                            data_rows.append({
                                'date': date_str,
                                'rate': rate_value
                            })
                            
                        except (ValueError, IndexError):
                            continue
            
            self.logger.info(f"Retrieved {len(data_rows)} Bank Rate observations for {series_code}")
            return data_rows
            
        except Exception as e:
            self.logger.error(f"Failed to fetch Bank Rate data for {series_code}: {str(e)}")
            return []
    
    def get_uk_gilt_yields(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Get UK gilt yields for 5Y, 10Y, and 20Y maturities from Bank of England IADB.
        
        Args:
            start_date: Start date in DD/MMM/YYYY format (e.g., "01/Jan/2020")
            end_date: End date in DD/MMM/YYYY format (e.g., "01/Sep/2025")
            
        Returns:
            List of dictionaries with date, maturity, and yield_rate
        """
        url_endpoint = 'http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes'
        
        # UK gilt yield series codes for daily nominal par yields
        series_codes = 'IUDSNPY,IUDMNPY,IUDLNPY'  # 5Y, 10Y, 20Y
        
        payload = {
            'Datefrom': start_date,
            'Dateto': end_date,
            'SeriesCodes': series_codes,
            'CSVF': 'TN',  # Tabular format, no titles
            'UsingCodes': 'Y',
            'VPD': 'Y'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        try:
            self.logger.info(f"Fetching UK gilt yields (5Y, 10Y, 20Y) from {start_date} to {end_date}")
            response = self.session.get(url_endpoint, params=payload, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data using pandas
            import pandas as pd
            import io
            
            csv_data = response.text.strip()
            if not csv_data:
                self.logger.warning("Empty response for UK gilt yields")
                return []
                
            try:
                # Use pandas to parse CSV data
                df = pd.read_csv(io.StringIO(csv_data))
                
                if df.empty:
                    self.logger.warning("No data rows found for UK gilt yields")
                    return []
                
                # Expected columns: DATE, IUDSNPY, IUDMNPY, IUDLNPY
                # Transform into individual maturity records
                yield_data = []
                
                for _, row in df.iterrows():
                    try:
                        date_str = str(row.iloc[0]).strip()  # First column is date
                        
                        # Parse BoE date format (e.g., "31 Jan 2024")
                        from datetime import datetime
                        obs_date = datetime.strptime(date_str, "%d %b %Y").date()
                        
                        # Extract yields for each maturity (columns 1, 2, 3)
                        maturities = [
                            ('5Y', 1),   # IUDSNPY - 5 Year
                            ('10Y', 2),  # IUDMNPY - 10 Year  
                            ('20Y', 3)   # IUDLNPY - 20 Year
                        ]
                        
                        for maturity, col_idx in maturities:
                            try:
                                if col_idx < len(row) and not pd.isna(row.iloc[col_idx]):
                                    yield_rate = float(row.iloc[col_idx])
                                    
                                    yield_data.append({
                                        'date': obs_date,
                                        'maturity': maturity,
                                        'yield_rate': yield_rate
                                    })
                                    
                            except (ValueError, IndexError) as e:
                                self.logger.debug(f"Skipping {maturity} yield for {date_str}: {str(e)}")
                                continue
                        
                    except (ValueError, TypeError) as e:
                        self.logger.debug(f"Skipping invalid date row: {str(e)}")
                        continue
                        
            except Exception as parse_error:
                self.logger.error(f"Failed to parse CSV data for gilt yields: {str(parse_error)}")
                return []
            
            self.logger.info(f"Retrieved {len(yield_data)} gilt yield observations across all maturities")
            return yield_data
            
        except Exception as e:
            self.logger.error(f"Failed to fetch UK gilt yields: {str(e)}")
            return []

def collect_cpi(database_url=None):
    """Collect Consumer Price Index data with incremental updates."""
    collector = BLSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="consumer_price_index"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("CPI data is already up to date")
        return 0
    
    # BLS uses years, so convert dates to year range
    if start_date is None:
        # Fetch all available historical data - BLS API supports back to early 2000s
        start_year = 2000  # Start from year 2000 to get maximum historical data
    else:
        start_year = start_date.year
    end_year = end_date.year
    
    series_data = collector.get_series_data("CUUR0000SA0", start_year, end_year)  # All items CPI-U
    
    # Process and sort data chronologically for YoY calculation
    processed_data = []
    for item in series_data:
        try:
            # Parse BLS date format (YYYY + MM)
            year = int(item["year"])
            period = item["period"]
            if period.startswith("M"):
                month = int(period[1:])
                date = datetime(year, month, 1).date()
                
                # Only process data within our target date range
                if (start_date and date < start_date) or date > end_date:
                    continue
            else:
                continue  # Skip non-monthly data
                
            processed_data.append({
                "date": date,
                "value": float(item["value"]),
            })
                
        except Exception as e:
            collector.logger.error(f"Error processing CPI data item: {str(e)}")
    
    # Sort chronologically for YoY calculation
    processed_data.sort(key=lambda x: x["date"])
    
    # Calculate year-over-year changes using database context
    bulk_data = []
    for i, current in enumerate(processed_data):
        yoy_change = None
        month_over_month_change = None
        
        # Calculate YoY using database lookup for 12-month-ago value
        twelve_months_ago = current["date"].replace(year=current["date"].year - 1)
        
        # First try to find 12-month-ago value in current processed data
        prev_year_value = None
        for prev in processed_data:
            if prev["date"] == twelve_months_ago:
                prev_year_value = prev["value"]
                break
        
        # If not found in processed data, query database
        if prev_year_value is None:
            prev_year_value = collector.get_cpi_value_for_date(twelve_months_ago, "uk_consumer_price_index")
        
        # Calculate YoY if we have the previous year value
        if prev_year_value is not None:
            yoy_change = ((current["value"] / prev_year_value) - 1) * 100
        
        # Calculate month-over-month change
        prev_month_value = None
        expected_prev_date = current["date"] - timedelta(days=32)  # Go back to previous month
        expected_prev_date = expected_prev_date.replace(day=1)  # First of that month
        
        # First try to find in current processed data
        if i > 0:
            prev_month = processed_data[i-1]
            if prev_month["date"] == expected_prev_date:
                prev_month_value = prev_month["value"]
        
        # If not found in processed data, query database
        if prev_month_value is None:
            prev_month_value = collector.get_cpi_value_for_date(expected_prev_date, "uk_consumer_price_index")
        
        # Calculate MoM if we have the previous month value
        if prev_month_value is not None:
            month_over_month_change = ((current["value"] / prev_month_value) - 1) * 100
        
        data = {
            "date": current["date"],
            "value": current["value"],
            "year_over_year_change": yoy_change,
            "month_over_month_change": month_over_month_change
        }
        
        bulk_data.append(data)
    
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
        table="federal_funds_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("Federal Funds Rate data is already up to date")
        return 0
    
    # Handle unlimited historical data fetch
    if start_date is None:
        # Fetch all available historical data - don't specify observation_start
        series_data = collector.get_series_data(
            "FEDFUNDS", 
            observation_end=end_date.strftime("%Y-%m-%d")
        )
    else:
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
        table="unemployment_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("Unemployment data is already up to date")
        return 0
    
    # BLS uses years, so convert dates to year range
    if start_date is None:
        # Fetch all available historical data - BLS API supports back to early 2000s
        start_year = 2000  # Start from year 2000 to get maximum historical data
    else:
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
                if (start_date and date < start_date) or date > end_date:
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
        table="daily_federal_funds_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("Daily Federal Funds Rate data is already up to date")
        return 0
    
    # Handle unlimited historical data fetch
    if start_date is None:
        # Fetch all available historical data - don't specify observation_start
        series_data = collector.get_series_data(
            "DFF", 
            observation_end=end_date.strftime('%Y-%m-%d')
        )
    else:
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
                
            # Parse quarter format (YYYYQN)
            time_period = item["TimePeriod"]
            year = int(time_period[:4])
            quarter = int(time_period[5])  # Extract quarter number after 'Q'
            quarter_month = quarter * 3  # Q1=3, Q2=6, Q3=9, Q4=12
            quarter_date = datetime(year, quarter_month, 1).date()
            
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

# UK Data Collection Functions

def collect_uk_cpi(database_url=None):
    """Collect UK Consumer Price Index data with incremental updates using ONS API."""
    collector = ONSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_consumer_price_index"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK CPI data is already up to date")
        return 0
    
    # Use CPIH01 dataset with proper ONS API dimensions
    # Based on your provided URL structure
    dataset_id = "cpih01"
    
    try:
        collector.logger.info(f"Attempting to fetch UK CPI data from dataset: {dataset_id}")
        
        # Use the correct ONS API structure with dimensions
        # geography=K02000001 (UK), aggregate=CP00 (All items CPIH)
        observations = collector.get_dataset_data(
            dataset_id=dataset_id,
            time_constraint="*",  # Get all time periods
            geography="K02000001",  # UK
            aggregate="CP00"  # All items CPIH
        )
        
        if not observations:
            collector.logger.error(f"No data returned from ONS dataset {dataset_id}")
            return 0
            
        collector.logger.info(f"Successfully retrieved {len(observations)} observations from {dataset_id}")
        
    except Exception as e:
        collector.logger.error(f"Failed to fetch data from {dataset_id}: {str(e)}")
        return 0
    
    # Process ONS observations data
    processed_data = []
    for obs in observations:
        try:
            if not isinstance(obs, dict):
                continue
                
            # ONS observation structure based on tidy data format
            obs_date = None
            obs_value = None
            
            # Extract date from dimensions (ONS tidy format)
            if "dimensions" in obs:
                dimensions = obs["dimensions"]
                if "Time" in dimensions:
                    time_info = dimensions["Time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                elif "time" in dimensions:
                    time_info = dimensions["time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                else:
                    time_str = None
                
                if time_str:
                    # ONS time format can be "Aug-15", "2015-08", etc.
                    try:
                        # Handle "Aug-15" format (3-letter month abbreviation)
                        if len(time_str) == 6 and "-" in time_str:  # MMM-YY
                            month_abbr, year_suffix = time_str.split("-")
                            # Convert 2-digit year to 4-digit
                            # Years 00-29 assume 20XX, years 30-99 assume 19XX (handles 1980s-1990s data)
                            year_int = int(year_suffix)
                            if year_int <= 29:
                                full_year = 2000 + year_int  # 00-29 -> 2000-2029
                            else:
                                full_year = 1900 + year_int  # 30-99 -> 1930-1999
                            # Convert month abbreviation to number
                            month_map = {
                                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                            }
                            if month_abbr in month_map:
                                obs_date = datetime(full_year, month_map[month_abbr], 1).date()
                        elif len(time_str) == 7 and "-" in time_str:  # YYYY-MM
                            obs_date = datetime.strptime(time_str + "-01", "%Y-%m-%d").date()
                        elif len(time_str) == 10:  # YYYY-MM-DD
                            obs_date = datetime.strptime(time_str, "%Y-%m-%d").date()
                    except Exception as parse_error:
                        collector.logger.debug(f"Could not parse date '{time_str}': {str(parse_error)}")
                        pass
            
            # Extract value from observation field
            if "observation" in obs:
                try:
                    obs_value = float(obs["observation"])
                except:
                    pass
            elif "value" in obs:
                try:
                    obs_value = float(obs["value"])
                except:
                    pass
            
            # Skip if we couldn't parse date or value
            if obs_date is None or obs_value is None:
                continue
                
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            processed_data.append({
                "date": obs_date,
                "value": obs_value,
            })
            
        except Exception as e:
            collector.logger.error(f"Error processing ONS observation: {str(e)}")
    
    if not processed_data:
        collector.logger.warning("No valid UK CPI data could be processed from ONS observations")
        return 0
    
    # Sort chronologically for YoY calculation
    processed_data.sort(key=lambda x: x["date"])
    collector.logger.info(f"Processing {len(processed_data)} UK CPI records from ONS")
    
    # Calculate year-over-year changes using database context
    bulk_data = []
    for i, current in enumerate(processed_data):
        yoy_change = None
        month_over_month_change = None
        
        # Calculate YoY using database lookup for 12-month-ago value
        twelve_months_ago = current["date"].replace(year=current["date"].year - 1)
        
        # First try to find 12-month-ago value in current processed data
        prev_year_value = None
        for prev in processed_data:
            if prev["date"] == twelve_months_ago:
                prev_year_value = prev["value"]
                break
        
        # If not found in processed data, query database
        if prev_year_value is None:
            prev_year_value = collector.get_cpi_value_for_date(twelve_months_ago, "uk_consumer_price_index")
        
        # Calculate YoY if we have the previous year value
        if prev_year_value is not None:
            yoy_change = ((current["value"] / prev_year_value) - 1) * 100
        
        # Calculate month-over-month change
        prev_month_value = None
        expected_prev_date = current["date"] - timedelta(days=32)  # Go back to previous month
        expected_prev_date = expected_prev_date.replace(day=1)  # First of that month
        
        # First try to find in current processed data
        if i > 0:
            prev_month = processed_data[i-1]
            if prev_month["date"] == expected_prev_date:
                prev_month_value = prev_month["value"]
        
        # If not found in processed data, query database
        if prev_month_value is None:
            prev_month_value = collector.get_cpi_value_for_date(expected_prev_date, "uk_consumer_price_index")
        
        # Calculate MoM if we have the previous month value
        if prev_month_value is not None:
            month_over_month_change = ((current["value"] / prev_month_value) - 1) * 100
        
        data = {
            "date": current["date"],
            "value": current["value"],
            "year_over_year_change": yoy_change,
            "month_over_month_change": month_over_month_change
        }
        
        bulk_data.append(data)
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("uk_consumer_price_index", bulk_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} UK CPI records")
        return success_count
    else:
        collector.logger.info("No valid UK CPI data to process")
        return 0

def collect_uk_unemployment(database_url=None):
    """Collect UK unemployment rate data with incremental updates using ONS API."""
    collector = ONSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_unemployment_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK unemployment data is already up to date")
        return 0
    
    # Use labour-market dataset with PWT24 edition for current data
    dataset_id = "labour-market"
    edition = "PWT24"  # People in Work Tables 2024 - has current data to 2025
    
    try:
        collector.logger.info(f"Attempting to fetch UK unemployment data from dataset: {dataset_id}")
        
        # Use the correct ONS API structure with dimensions
        # Based on discovered dimension codes:
        # geography=K02000001 (UK), economicactivity=unemployed, unitofmeasure=rates, 
        # seasonaladjustment=seasonal-adjustment, sex=all-adults, agegroups=16+
        observations = collector.get_dataset_data(
            dataset_id=dataset_id,
            edition=edition,
            time_constraint="*",  # Get all time periods
            geography="K02000001",  # UK
            economicactivity="unemployed",  # Unemployed
            unitofmeasure="rates",  # Rates (percentage)
            seasonaladjustment="seasonal-adjustment",  # Seasonally adjusted
            sex="all-adults",  # All adults
            agegroups="16+"  # 16+
        )
        
        if not observations:
            collector.logger.error(f"No data returned from ONS dataset {dataset_id}")
            return 0
            
        collector.logger.info(f"Successfully retrieved {len(observations)} observations from {dataset_id}")
        
    except Exception as e:
        collector.logger.error(f"Failed to fetch data from {dataset_id}: {str(e)}")
        return 0
    
    # Process ONS observations data
    processed_data = []
    for obs in observations:
        try:
            if not isinstance(obs, dict):
                continue
                
            # ONS observation structure based on tidy data format
            obs_date = None
            obs_value = None
            
            # Extract date from dimensions (ONS tidy format)
            if "dimensions" in obs:
                dimensions = obs["dimensions"]
                if "Time" in dimensions:
                    time_info = dimensions["Time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                elif "time" in dimensions:
                    time_info = dimensions["time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                else:
                    time_str = None
                
                if time_str:
                    # ONS unemployment data uses "jul-sep-2016" format (quarterly)
                    try:
                        if "-" in time_str and len(time_str) > 6:  # Handle rolling quarterly format
                            # Parse formats like "jul-sep-2016", "mar-may-2013", "jun-aug-2013"
                            parts = time_str.lower().split("-")
                            if len(parts) >= 2:
                                # Extract year (last part or look for 4-digit number)
                                year_str = None
                                for part in reversed(parts):
                                    if len(part) == 4 and part.isdigit():
                                        year_str = part
                                        break
                                
                                if year_str and len(parts) >= 2:
                                    year = int(year_str)
                                    # Parse rolling quarterly periods (e.g., "jul-sep", "mar-may")
                                    first_month_str = parts[0].strip()
                                    second_month_str = parts[1].strip() if not parts[1].isdigit() else parts[1].replace(year_str, '').strip()
                                    
                                    # Month name to number mapping
                                    month_names = {
                                        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                                        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                                    }
                                    
                                    if first_month_str in month_names and second_month_str in month_names:
                                        start_month = month_names[first_month_str]
                                        end_month = month_names[second_month_str]
                                        
                                        # Calculate the true middle month of the rolling 3-month period
                                        if end_month >= start_month:
                                            # Normal case: jul-sep (7,8,9) -> middle = 8
                                            middle_month = start_month + 1
                                            target_year = year
                                        else:
                                            # Year boundary case: need to handle carefully
                                            if first_month_str == 'nov' and second_month_str == 'jan':
                                                # nov-jan-2020: Nov 2019, Dec 2019, Jan 2020 -> middle = Dec 2019
                                                middle_month = 12
                                                target_year = year - 1
                                            elif first_month_str == 'dec' and second_month_str == 'feb':
                                                # dec-feb-2020: Dec 2019, Jan 2020, Feb 2020 -> middle = Jan 2020
                                                middle_month = 1
                                                target_year = year
                                            else:
                                                # Other year boundary cases
                                                middle_month = start_month + 1
                                                if middle_month > 12:
                                                    middle_month = 1
                                                    target_year = year + 1
                                                else:
                                                    target_year = year
                                        
                                        obs_date = datetime(target_year, middle_month, 1).date()
                        elif len(time_str) == 6 and "-" in time_str:  # MMM-YY format
                            month_abbr, year_suffix = time_str.split("-")
                            # Convert 2-digit year to 4-digit
                            # Years 00-29 assume 20XX, years 30-99 assume 19XX (handles 1980s-1990s data)
                            year_int = int(year_suffix)
                            if year_int <= 29:
                                full_year = 2000 + year_int  # 00-29 -> 2000-2029
                            else:
                                full_year = 1900 + year_int  # 30-99 -> 1930-1999
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }
                            if month_abbr.lower() in month_map:
                                obs_date = datetime(full_year, month_map[month_abbr.lower()], 1).date()
                    except Exception as parse_error:
                        collector.logger.debug(f"Could not parse unemployment date '{time_str}': {str(parse_error)}")
                        pass
            
            # Extract value from observation field
            if "observation" in obs:
                try:
                    obs_value = float(obs["observation"])
                except:
                    pass
            elif "value" in obs:
                try:
                    obs_value = float(obs["value"])
                except:
                    pass
            
            # Skip if we couldn't parse date or value
            if obs_date is None or obs_value is None:
                continue
            
            # Filter out raw counts, keep only percentage rates (unemployment rates should be < 100%)
            if obs_value > 100:
                collector.logger.warning(f"Filtering out raw count value {obs_value} at {obs_date} (expected percentage)")
                continue
                
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            processed_data.append({
                "date": obs_date,
                "rate": obs_value,  # Unemployment rate as percentage
            })
            
        except Exception as e:
            collector.logger.error(f"Error processing ONS unemployment observation: {str(e)}")
    
    if not processed_data:
        collector.logger.warning("No valid UK unemployment data could be processed from ONS observations")
        return 0
    
    # Sort chronologically
    processed_data.sort(key=lambda x: x["date"])
    collector.logger.info(f"Processing {len(processed_data)} UK unemployment records from ONS")
    
    # Bulk upsert all records
    if processed_data:
        success_count = collector.bulk_upsert_data("uk_unemployment_rate", processed_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} UK unemployment records")
        return success_count
    else:
        collector.logger.info("No valid UK unemployment data to process")
        return 0

def collect_uk_gdp(database_url=None):
    """Collect UK GDP data with incremental updates using ONS API."""
    collector = ONSCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_gross_domestic_product",
        date_column="date"  # Using date instead of quarter for monthly GDP data
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK GDP data is already up to date")
        return 0
    
    # Use gdp-to-four-decimal-places dataset with proper ONS API dimensions
    dataset_id = "gdp-to-four-decimal-places"
    
    try:
        collector.logger.info(f"Attempting to fetch UK GDP data from dataset: {dataset_id}")
        
        # Use the correct ONS API structure with dimensions
        # Based on discovered dimension codes:
        # geography=K02000001 (UK), unofficialstandardindustrialclassification=A--T (Monthly GDP)
        observations = collector.get_dataset_data(
            dataset_id=dataset_id,
            time_constraint="*",  # Get all time periods
            geography="K02000001",  # UK
            unofficialstandardindustrialclassification="A--T"  # A-T : Monthly GDP
        )
        
        if not observations:
            collector.logger.error(f"No data returned from ONS dataset {dataset_id}")
            return 0
            
        collector.logger.info(f"Successfully retrieved {len(observations)} observations from {dataset_id}")
        
    except Exception as e:
        collector.logger.error(f"Failed to fetch data from {dataset_id}: {str(e)}")
        return 0
    
    # Process ONS observations data
    processed_data = []
    for obs in observations:
        try:
            if not isinstance(obs, dict):
                continue
                
            # ONS observation structure based on tidy data format
            obs_date = None
            obs_value = None
            
            # Extract date from dimensions (ONS tidy format)
            if "dimensions" in obs:
                dimensions = obs["dimensions"]
                if "Time" in dimensions:
                    time_info = dimensions["Time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                elif "time" in dimensions:
                    time_info = dimensions["time"]
                    time_str = time_info.get("id", "") if isinstance(time_info, dict) else str(time_info)
                else:
                    time_str = None
                
                if time_str:
                    # ONS GDP data uses "Dec-98" format (monthly)
                    try:
                        if len(time_str) == 6 and "-" in time_str:  # MMM-YY format
                            month_abbr, year_suffix = time_str.split("-")
                            # Convert 2-digit year to 4-digit
                            # Years 00-29 assume 20XX, years 30-99 assume 19XX (handles 1980s-1990s data)
                            year_int = int(year_suffix)
                            if year_int <= 29:
                                full_year = 2000 + year_int  # 00-29 -> 2000-2029
                            else:
                                full_year = 1900 + year_int  # 30-99 -> 1930-1999
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
                                'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }
                            if month_abbr.lower() in month_map:
                                obs_date = datetime(full_year, month_map[month_abbr.lower()], 1).date()
                        elif len(time_str) == 7 and "-" in time_str:  # YYYY-MM
                            obs_date = datetime.strptime(time_str + "-01", "%Y-%m-%d").date()
                        elif len(time_str) == 10:  # YYYY-MM-DD
                            obs_date = datetime.strptime(time_str, "%Y-%m-%d").date()
                    except Exception as parse_error:
                        collector.logger.debug(f"Could not parse GDP date '{time_str}': {str(parse_error)}")
                        pass
            
            # Extract value from observation field
            if "observation" in obs:
                try:
                    obs_value = float(obs["observation"])
                except:
                    pass
            elif "value" in obs:
                try:
                    obs_value = float(obs["value"])
                except:
                    pass
            
            # Skip if we couldn't parse date or value
            if obs_date is None or obs_value is None:
                continue
                
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            processed_data.append({
                "date": obs_date,
                "gdp_index": obs_value,  # GDP index value
            })
            
        except Exception as e:
            collector.logger.error(f"Error processing ONS GDP observation: {str(e)}")
    
    if not processed_data:
        collector.logger.warning("No valid UK GDP data could be processed from ONS observations")
        return 0
    
    # Sort chronologically
    processed_data.sort(key=lambda x: x["date"])
    collector.logger.info(f"Processing {len(processed_data)} UK GDP records from ONS")
    
    # Bulk upsert all records
    if processed_data:
        success_count = collector.bulk_upsert_data("uk_gross_domestic_product", processed_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} UK GDP records")
        return success_count
    else:
        collector.logger.info("No valid UK GDP data to process")
        return 0

def collect_uk_monthly_bank_rate(database_url=None):
    """Collect UK Monthly Bank Rate data with incremental updates using Bank of England IADB API."""
    collector = BankOfEnglandCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_monthly_bank_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK Monthly Bank Rate data is already up to date")
        return 0
    
    # Use IUMABEDR dataset (Monthly Average Bank Rate)
    series_code = "IUMABEDR"
    
    try:
        collector.logger.info(f"Attempting to fetch UK Bank Rate data using series: {series_code}")
        
        # Convert date range to BoE format (DD/MON/YYYY)
        if start_date:
            boe_start_date = start_date.strftime("%d/%b/%Y")
        else:
            # Get all available historical data - IUMABEDR series goes back to January 1975
            boe_start_date = "01/Jan/1975"  # Bank Rate monthly average data available from Jan 1975
            
        boe_end_date = end_date.strftime("%d/%b/%Y")
        
        # Get Bank Rate data from BoE IADB API
        rate_data = collector.get_bank_rate_data(
            series_code=series_code,
            start_date=boe_start_date,
            end_date=boe_end_date
        )
        
        if not rate_data:
            collector.logger.error(f"No data returned from Bank of England for {series_code}")
            return 0
            
        collector.logger.info(f"Successfully retrieved {len(rate_data)} observations from Bank of England")
        
    except Exception as e:
        collector.logger.error(f"Failed to fetch data from Bank of England: {str(e)}")
        return 0
    
    # Process Bank of England data
    processed_data = []
    for item in rate_data:
        try:
            date_str = item["date"]
            rate_value = item["rate"]
            
            # Parse BoE date format (typically DD MMM YYYY, e.g., "01 Jan 2020")
            try:
                # Handle various BoE date formats
                if "/" in date_str:
                    # DD/MM/YYYY format
                    obs_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                elif " " in date_str and len(date_str.split()) == 3:
                    # DD MMM YYYY format
                    obs_date = datetime.strptime(date_str, "%d %b %Y").date()
                elif "-" in date_str:
                    # YYYY-MM-DD format
                    obs_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                else:
                    collector.logger.warning(f"Unknown date format: {date_str}")
                    continue
                    
            except Exception as date_error:
                collector.logger.debug(f"Could not parse Bank Rate date '{date_str}': {str(date_error)}")
                continue
                
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            processed_data.append({
                "date": obs_date,
                "rate": rate_value,  # Bank Rate as percentage
            })
            
        except Exception as e:
            collector.logger.error(f"Error processing Bank Rate data item: {str(e)}")
    
    if not processed_data:
        collector.logger.warning("No valid UK Bank Rate data could be processed")
        return 0
    
    # Sort chronologically
    processed_data.sort(key=lambda x: x["date"])
    collector.logger.info(f"Processing {len(processed_data)} UK Bank Rate records from Bank of England")
    
    # Bulk upsert all records
    if processed_data:
        success_count = collector.bulk_upsert_data("uk_monthly_bank_rate", processed_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} UK Monthly Bank Rate records")
        return success_count
    else:
        collector.logger.info("No valid UK Monthly Bank Rate data to process")
        return 0

def collect_uk_daily_bank_rate(database_url=None):
    """Collect UK Daily Bank Rate data with incremental updates using Bank of England IADB API."""
    collector = BankOfEnglandCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_daily_bank_rate"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK Daily Bank Rate data is already up to date")
        return 0
    
    # Use IUDBEDR dataset (Daily Bank Rate)
    series_code = "IUDBEDR"
    
    try:
        collector.logger.info(f"Attempting to fetch UK Daily Bank Rate data using series: {series_code}")
        
        # Convert date range to BoE format (DD/MON/YYYY)
        if start_date:
            boe_start_date = start_date.strftime("%d/%b/%Y")
        else:
            # Get all available historical data - IUDBEDR daily series goes back to January 1975
            boe_start_date = "01/Jan/1975"  # Daily Bank Rate data available from Jan 1975
            
        boe_end_date = end_date.strftime("%d/%b/%Y")
        
        # Get Bank Rate data from BoE IADB API
        rate_data = collector.get_bank_rate_data(
            series_code=series_code,
            start_date=boe_start_date,
            end_date=boe_end_date
        )
        
        if not rate_data:
            collector.logger.error(f"No data returned from Bank of England for {series_code}")
            return 0
            
        collector.logger.info(f"Successfully retrieved {len(rate_data)} observations from Bank of England")
        
    except Exception as e:
        collector.logger.error(f"Failed to fetch data from Bank of England: {str(e)}")
        return 0
    
    # Process Bank of England data
    processed_data = []
    for item in rate_data:
        try:
            date_str = item["date"]
            rate_value = item["rate"]
            
            # Parse BoE date format (typically DD MMM YYYY, e.g., "01 Jan 2020")
            try:
                # Handle various BoE date formats
                if "/" in date_str:
                    # DD/MM/YYYY format
                    obs_date = datetime.strptime(date_str, "%d/%m/%Y").date()
                elif " " in date_str and len(date_str.split()) == 3:
                    # DD MMM YYYY format
                    obs_date = datetime.strptime(date_str, "%d %b %Y").date()
                elif "-" in date_str:
                    # YYYY-MM-DD format
                    obs_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                else:
                    collector.logger.warning(f"Unknown date format: {date_str}")
                    continue
                    
            except Exception as date_error:
                collector.logger.debug(f"Could not parse Daily Bank Rate date '{date_str}': {str(date_error)}")
                continue
                
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            processed_data.append({
                "date": obs_date,
                "rate": rate_value,  # Bank Rate as percentage
            })
            
        except Exception as e:
            collector.logger.error(f"Error processing Daily Bank Rate data item: {str(e)}")
    
    if not processed_data:
        collector.logger.warning("No valid UK Daily Bank Rate data could be processed")
        return 0
    
    # Sort chronologically
    processed_data.sort(key=lambda x: x["date"])
    collector.logger.info(f"Processing {len(processed_data)} UK Daily Bank Rate records from Bank of England")
    
    # Bulk upsert all records
    if processed_data:
        success_count = collector.bulk_upsert_data("uk_daily_bank_rate", processed_data)
        collector.logger.info(f"Successfully bulk upserted {success_count} UK Daily Bank Rate records")
        return success_count
    else:
        collector.logger.info("No valid UK Daily Bank Rate data to process")
        return 0

def collect_uk_gilt_yields(database_url=None):
    """Collect UK gilt yields (5Y, 10Y, 20Y) data with incremental updates using Bank of England IADB."""
    from datetime import timedelta
    
    collector = BankOfEnglandCollector(database_url)
    
    # Get date range for collection
    start_date, end_date = collector.get_date_range_for_collection(
        table="uk_gilt_yields"
    )
    
    if start_date is None and end_date is None:
        collector.logger.info("UK gilt yields data is already up to date")
        return 0
    
    # Convert dates to BoE format (DD/MMM/YYYY)
    if start_date is None:
        # Get all available historical data - gilt yields available from 1970
        start_date_str = "01/Jan/1970"  # Start from earliest available gilt yields data
    else:
        start_date_str = start_date.strftime("%d/%b/%Y")
        
    end_date_str = end_date.strftime("%d/%b/%Y")
    
    # Fetch gilt yields data
    gilt_data = collector.get_uk_gilt_yields(start_date_str, end_date_str)
    
    if not gilt_data:
        collector.logger.info("No UK gilt yields data retrieved from Bank of England")
        return 0
    
    # Filter data within target date range
    bulk_data = []
    for item in gilt_data:
        try:
            obs_date = item["date"]
            
            # Only process data within our target date range
            if (start_date and obs_date < start_date) or obs_date > end_date:
                continue
                
            data = {
                "date": obs_date,
                "maturity": item["maturity"],
                "yield_rate": item["yield_rate"]
            }
            bulk_data.append(data)
            
        except Exception as e:
            collector.logger.error(f"Error processing gilt yield data for {item.get('date', 'unknown')}: {str(e)}")
    
    # Sort by date and maturity for consistency
    bulk_data.sort(key=lambda x: (x["date"], x["maturity"]))
    
    # Bulk upsert all records
    if bulk_data:
        success_count = collector.bulk_upsert_data("uk_gilt_yields", bulk_data, 
                                                 conflict_columns=['date', 'maturity'])
        collector.logger.info(f"Successfully bulk upserted {success_count} UK gilt yield records")
        return success_count
    else:
        collector.logger.info("No valid UK gilt yields data to process")
        return 0

class BoEYieldCurveCollector(BaseCollector):
    """Collector for comprehensive Bank of England yield curve data from ZIP files."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves"
        
        # Available data sources
        self.data_sources = {
            'nominal': {
                'current': 'latest-yield-curve-data.zip',
                'historical': 'glcnominalddata.zip',
                'file_prefix': 'GLC Nominal daily data',
                'sheet': '4. spot curve'
            },
            'real': {
                'current': 'latest-yield-curve-data.zip',
                'historical': 'glcrealddata.zip',
                'file_prefix': 'GLC Real daily data',
                'sheet': '4. spot curve'
            },
            'inflation': {
                'current': 'latest-yield-curve-data.zip', 
                'historical': 'glcinflationddata.zip',
                'file_prefix': 'GLC Inflation daily data',
                'sheet': '4. spot curve'
            },
            'ois': {
                'current': 'latest-yield-curve-data.zip',
                'historical': 'oisddata.zip',
                'file_prefix': 'OIS daily data',
                'sheet': '4. spot curve'
            }
        }
    
    def parse_yield_data(self, filename, yield_type, sheet_name='4. spot curve'):
        """Parse Bank of England yield curve data from Excel files."""
        import pandas as pd
        from datetime import datetime
        
        try:
            df = pd.read_excel(filename, sheet_name=sheet_name)
            
            # Get maturities from row 2 (skip first column)
            maturities = df.iloc[2, 1:].values
            maturities = [float(m) for m in maturities if pd.notna(m)]
            
            # Get data rows (skip first 3 rows which are headers)
            data_rows = df.iloc[3:]
            
            # Process each data row
            parsed_data = []
            for _, row in data_rows.iterrows():
                date_val = row.iloc[0]
                
                # Skip rows without dates
                if pd.isna(date_val) or not isinstance(date_val, datetime):
                    continue
                    
                # Get yield values (skip first column which is date, and handle potential empty second column)
                start_col = 2 if pd.isna(row.iloc[1]) else 1
                yield_values = row.iloc[start_col:start_col+len(maturities)].values
                
                # Create records for each maturity
                for i, (maturity, yield_val) in enumerate(zip(maturities, yield_values)):
                    if pd.notna(yield_val) and float(yield_val) > 0:  # Filter out zero/negative yields
                        parsed_data.append({
                            'date': date_val.date(),
                            'maturity_years': float(maturity),
                            'yield_rate': float(yield_val),
                            'yield_type': yield_type
                        })
            
            return parsed_data
            
        except Exception as e:
            self.logger.error(f"Error parsing yield data from {filename}: {str(e)}")
            return []
    
    def download_and_extract_zip(self, zip_filename, temp_dir='./temp_yield_data'):
        """Download and extract BoE yield curve ZIP file."""
        import requests
        import zipfile
        import os
        import tempfile
        
        try:
            # Create temp directory
            os.makedirs(temp_dir, exist_ok=True)
            
            # Download ZIP file with browser-like headers to bypass anti-bot detection
            url = f"{self.base_url}/{zip_filename}"
            self.logger.info(f"Downloading yield curve data from {url}")
            
            # Add realistic browser headers to avoid 403 Forbidden errors
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
            
            response = requests.get(url, headers=headers, stream=True, timeout=60)
            response.raise_for_status()
            
            zip_path = os.path.join(temp_dir, zip_filename)
            with open(zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Extract ZIP file
            extracted_files = []
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.filelist:
                    if file_info.filename.endswith('.xlsx'):
                        extracted_path = zip_ref.extract(file_info, temp_dir)
                        extracted_files.append(extracted_path)
            
            # Clean up ZIP file
            os.remove(zip_path)
            
            return extracted_files
            
        except Exception as e:
            self.logger.error(f"Error downloading/extracting {zip_filename}: {str(e)}")
            return []
    
    def collect_yield_type_data(self, yield_type='nominal', include_historical=True):
        """Collect data for a specific yield type (nominal, real, inflation, ois)."""
        import os
        import shutil
        from datetime import datetime, timedelta, date
        
        if yield_type not in self.data_sources:
            self.logger.error(f"Unsupported yield type: {yield_type}")
            return 0
        
        source_config = self.data_sources[yield_type]
        temp_dir = f'./temp_yield_data_{yield_type}'
        total_records = 0
        
        try:
            # Get date range for incremental collection
            end_date = datetime.now().date()
            start_date = None
            
            if self.database_url:
                try:
                    import psycopg2
                    conn = psycopg2.connect(self.database_url)
                    cursor = conn.cursor()
                    
                    # Check for existing data for this yield type
                    cursor.execute(f"""
                        SELECT MAX(date) FROM boe_yield_curves 
                        WHERE yield_type = %s
                    """, (yield_type,))
                    
                    result = cursor.fetchone()
                    if result and result[0]:
                        # Data exists, collect from next day
                        start_date = result[0] + timedelta(days=1)
                    else:
                        # No data exists, collect historical data
                        start_date = date(1979, 1, 1)  # BoE data starts from 1979
                    
                    cursor.close()
                    conn.close()
                    
                except Exception as e:
                    self.logger.warning(f"Could not check existing data for {yield_type}: {e}")
                    start_date = date(2020, 1, 1)  # Default to recent years if DB check fails
            else:
                # Safe mode - process recent data
                start_date = date(2020, 1, 1)
            
            if start_date and start_date > end_date:
                self.logger.info(f"BoE {yield_type} yield curve data is already up to date")
                return 0
            
            # Efficient Strategy: Start with latest file, only fetch historical if gap exists
            
            # Step 1: Always download and check latest current file first
            self.logger.info(f"Downloading latest {yield_type} yield curve data")
            current_files = self.download_and_extract_zip(source_config['current'], temp_dir)
            
            latest_data = []
            latest_coverage = None
            
            for file_path in current_files:
                filename = os.path.basename(file_path)
                if source_config['file_prefix'] in filename and 'current month' in filename:
                    self.logger.info(f"Processing latest file: {filename}")
                    data = self.parse_yield_data(file_path, yield_type, source_config['sheet'])
                    if data:
                        # Filter data from start_date onwards
                        filtered_data = [
                            d for d in data 
                            if (start_date is None or d['date'] >= start_date) and d['date'] <= end_date
                        ]
                        latest_data.extend(filtered_data)
                        
                        if data:  # Check full data range for coverage analysis
                            data_start = min(d['date'] for d in data)
                            data_end = max(d['date'] for d in data)
                            latest_coverage = (data_start, data_end)
                            self.logger.info(f"Latest file contains data from {data_start} to {data_end}")
            
            # Step 2: Check if latest file covers our needed date range
            gap_exists = False
            if start_date and latest_coverage:
                if latest_coverage[0] > start_date:
                    gap_days = (latest_coverage[0] - start_date).days
                    if gap_days > 0:
                        gap_exists = True
                        self.logger.info(f"Gap detected: {gap_days} days between {start_date} and {latest_coverage[0]}")
                    else:
                        self.logger.info(f"✅ Latest file fully covers required date range")
                else:
                    self.logger.info(f"✅ Latest file covers required date range (starts before {start_date})")
            
            # Step 3: If gap exists and historical collection enabled, fetch historical data
            if gap_exists and include_historical:
                self.logger.info(f"Downloading historical archive to fill gap for {yield_type}")
                historical_files = self.download_and_extract_zip(source_config['historical'], temp_dir)
                
                for file_path in historical_files:
                    filename = os.path.basename(file_path)
                    if source_config['file_prefix'] in filename and 'current month' not in filename:
                        self.logger.info(f"Processing historical file: {filename}")
                        data = self.parse_yield_data(file_path, yield_type, source_config['sheet'])
                        if data:
                            # Only collect data that fills the gap
                            gap_data = [
                                d for d in data 
                                if (start_date is None or d['date'] >= start_date) and 
                                   (latest_coverage is None or d['date'] < latest_coverage[0])
                            ]
                            latest_data.extend(gap_data)
                            if gap_data:
                                gap_range = f"{min(d['date'] for d in gap_data)} to {max(d['date'] for d in gap_data)}"
                                self.logger.info(f"Collected {len(gap_data)} gap records from {filename} ({gap_range})")
            elif gap_exists and not include_historical:
                self.logger.warning(f"Gap detected but historical collection disabled - some data may be missing")
            
            # Step 4: Store all collected data
            if latest_data:
                count = self.store_yield_data(latest_data)
                total_records += count
                
                # Log summary
                all_dates = sorted(set(d['date'] for d in latest_data))
                self.logger.info(f"✅ Stored {count} {yield_type} records from {all_dates[0]} to {all_dates[-1]} ({len(all_dates)} trading days)")
                
                if not gap_exists:
                    self.logger.info("🚀 Efficient collection: Only latest file needed")
                else:
                    self.logger.info("📁 Gap-fill collection: Latest + historical files used")
            else:
                self.logger.info(f"No new {yield_type} data to collect")
            
            return total_records
            
        except Exception as e:
            self.logger.error(f"Error collecting {yield_type} yield curve data: {str(e)}")
            return 0
        finally:
            # Clean up temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def store_yield_data(self, data_records):
        """Store yield curve data in database."""
        if not data_records:
            return 0
        
        # Prepare data for bulk upsert
        bulk_data = []
        for record in data_records:
            bulk_data.append({
                "date": record["date"],
                "maturity_years": record["maturity_years"], 
                "yield_rate": record["yield_rate"],
                "yield_type": record["yield_type"]
            })
        
        # Sort by date and maturity for consistency
        bulk_data.sort(key=lambda x: (x["date"], x["maturity_years"]))
        
        # Bulk upsert all records
        if bulk_data:
            success_count = self.bulk_upsert_data("boe_yield_curves", bulk_data,
                                                 conflict_columns=['date', 'maturity_years', 'yield_type'])
            return success_count
        
        return 0
    
    def collect_all_yield_types_optimized(self, yield_types, include_historical=True):
        """
        Optimized collection that downloads shared ZIP files once and processes all yield types.
        This prevents downloading the same latest-yield-curve-data.zip file multiple times.
        """
        import tempfile
        import os
        import shutil
        
        total_records = 0
        temp_dir = tempfile.mkdtemp(prefix='boe_yield_optimized_')
        
        try:
            self.logger.info("🚀 Starting optimized BoE yield curve collection")
            
            # Step 1 & 2: Download and extract latest ZIP file once (contains all yield types)
            latest_zip_name = 'latest-yield-curve-data.zip'
            
            self.logger.info(f"Downloading and extracting latest ZIP file once for all yield types: {latest_zip_name}")
            latest_files = self.download_and_extract_zip(latest_zip_name, temp_dir)
            
            # Step 3: Process each yield type from the same extracted files
            for yield_type in yield_types:
                self.logger.info(f"Processing {yield_type} yield curves from shared ZIP")
                
                if yield_type not in self.data_sources:
                    self.logger.warning(f"Unknown yield type: {yield_type}")
                    continue
                
                source_config = self.data_sources[yield_type]
                
                # Find files for this yield type in the extracted files
                latest_data = []
                for file_path in latest_files:
                    filename = os.path.basename(file_path)
                    if source_config['file_prefix'] in filename and 'current month' not in filename:
                        self.logger.info(f"Processing latest file: {filename}")
                        data = self.parse_yield_data(file_path, yield_type, source_config['sheet'])
                        if data:
                            latest_data.extend(data)
                            self.logger.info(f"Extracted {len(data)} {yield_type} records from {filename}")
                
                # Step 4: Determine date range and check if historical data needed
                start_date, end_date = self.get_date_range_for_collection(
                    table="boe_yield_curves",
                    default_lookback_days=5*365,
                    extra_where=f"yield_type = '{yield_type}'"
                )
                
                # Step 5: Handle historical data if needed (yield type specific)
                if include_historical and start_date and latest_data:
                    # Check if latest data covers the gap
                    data_start = min(d['date'] for d in latest_data)
                    if data_start > start_date:
                        gap_days = (data_start - start_date).days
                        self.logger.info(f"Gap detected for {yield_type}: {gap_days} days, downloading historical data")
                        
                        # Download and extract historical ZIP for this specific yield type
                        historical_zip_name = source_config['historical']
                        historical_temp_dir = os.path.join(temp_dir, f'{yield_type}_historical')
                        
                        try:
                            historical_files = self.download_and_extract_zip(historical_zip_name, historical_temp_dir)
                            
                            # Process historical files for gap filling
                            for file_path in historical_files:
                                filename = os.path.basename(file_path)
                                if source_config['file_prefix'] in filename and 'current month' not in filename:
                                    data = self.parse_yield_data(file_path, yield_type, source_config['sheet'])
                                    if data:
                                        # Only collect gap data
                                        gap_data = [d for d in data if start_date <= d['date'] < data_start]
                                        if gap_data:
                                            latest_data.extend(gap_data)
                                            self.logger.info(f"Added {len(gap_data)} historical {yield_type} records")
                        except Exception as e:
                            self.logger.warning(f"Could not download historical data for {yield_type}: {str(e)}")
                
                # Step 6: Store data for this yield type
                if latest_data:
                    count = self.store_yield_data(latest_data)
                    total_records += count
                    self.logger.info(f"Stored {count} {yield_type} yield curve records")
                else:
                    self.logger.info(f"No {yield_type} data to store")
            
            self.logger.info(f"✅ Optimized collection completed: {total_records} total records")
            return total_records
            
        except Exception as e:
            self.logger.error(f"Error in optimized collection: {str(e)}")
            raise
        finally:
            # Clean up temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    

def collect_boe_yield_curves(database_url=None, yield_types=['nominal', 'real', 'inflation', 'ois'], include_historical=True):
    """Collect comprehensive Bank of England yield curve data from ZIP files."""
    collector = BoEYieldCurveCollector(database_url)
    total_records = 0
    
    # OPTIMIZATION: Download shared ZIP files once and process all yield types
    # The latest-yield-curve-data.zip contains all 4 yield types, so we should download it once
    collector.logger.info(f"Starting BoE yield curve collection for {len(yield_types)} yield types: {yield_types}")
    
    try:
        # Download and process shared latest ZIP file once
        total_records += collector.collect_all_yield_types_optimized(yield_types, include_historical)
    except Exception as e:
        collector.logger.error(f"Error in optimized collection: {str(e)}")
        # Fallback to individual collection if optimized fails
        collector.logger.info("Falling back to individual yield type collection")
        for yield_type in yield_types:
            try:
                collector.logger.info(f"Starting collection of {yield_type} yield curves")
                count = collector.collect_yield_type_data(yield_type, include_historical)
                total_records += count
                collector.logger.info(f"Completed {yield_type} yield curves: {count} records")
            except Exception as e:
                collector.logger.error(f"Error collecting {yield_type} yield curves: {str(e)}")
    
    collector.logger.info(f"Total BoE yield curve records collected: {total_records}")
    return total_records
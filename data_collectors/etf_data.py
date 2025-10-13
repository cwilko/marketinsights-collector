"""
ETF Data Collectors for UK Market Analysis

This module collects ETF and NAV data from various sources including:
- iShares ETF data (NAV, holdings, performance)
- Vanguard ETF data
- Other UK gilt and bond ETF providers

Supports the Gilt Market Analysis Guide strategies for ETF arbitrage and premium/discount analysis.
"""

import requests
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from .base import BaseCollector


class iSharesETFCollector(BaseCollector):
    """Collector for iShares ETF data including NAV, holdings, and performance."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.ishares.com"
        
        # Known iShares UK ETFs relevant to gilt market analysis
        self.etf_configs = {
            'IGLT': {
                'name': 'iShares Core UK Gilts UCITS ETF',
                'product_id': '251806',
                'url_path': 'ishares-uk-gilts-ucits-etf',
                'description': 'Broad UK government bond exposure',
                'currency': 'GBP'
            },
            'INXG': {
                'name': 'iShares Index-Linked Gilts UCITS ETF', 
                'product_id': '251717',
                'url_path': 'ishares-indexlinked-gilts-ucits-etf',
                'description': 'UK inflation-linked government bonds',
                'currency': 'GBP'
            }
            # Add more ETFs as needed
        }


class VanguardETFCollector(BaseCollector):
    """Collector for Vanguard ETF data including historical NAV and prices."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.vanguard.co.uk"
        
        # Known Vanguard UK ETFs
        self.etf_configs = {
            'VGOV': {
                'name': 'Vanguard U.K. Gilt UCITS ETF (GBP) Distributing',
                'product_path': '/professional/product/etf/bond/9501/uk-gilt-ucits-etf-gbp-distributing',
                'description': 'UK government bond ETF for gilt market analysis',
                'currency': 'GBP'
            }
            # Add more Vanguard ETFs as needed
        }
        self.chrome_options = self._setup_chrome_options()
    
    def _setup_chrome_options(self):
        """Configure Chrome options for K8s-friendly headless scraping with undetected-chromedriver."""
        import undetected_chromedriver as uc
        options = uc.ChromeOptions()
        
        # Essential K8s options
        options.add_argument("--headless")
        options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        
        return options
    
    def _get_chrome_service(self):
        """Get Chrome service for both K8s and development environments."""
        import platform
        import os
        from selenium.webdriver.chrome.service import Service
        
        arch = platform.machine().lower()
        system = platform.system()
        
        # Try K8s/ARM64 Linux paths first (Chrome container)
        if arch in ['aarch64', 'arm64'] and system == 'Linux':
            # Check shared volume locations (init container installation)
            shared_paths = [
                '/shared/usr/bin/chromedriver',  # Most likely location from chromium-driver package
                '/shared/usr/lib/chromium-browser/chromedriver',
                '/shared/snap/chromium/current/usr/lib/chromium-browser/chromedriver'
            ]
            
            for path in shared_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using K8s ChromeDriver from init container: {path}")
                    
                    # Set library path for copied shared libraries
                    current_ld_path = os.environ.get('LD_LIBRARY_PATH', '')
                    shared_lib_paths = [
                        '/shared/lib/aarch64-linux-gnu',
                        '/shared/usr/lib/aarch64-linux-gnu', 
                        '/shared/usr/lib',
                        '/shared/lib'
                    ]
                    # Only add paths that exist
                    existing_paths = [p for p in shared_lib_paths if os.path.exists(p)]
                    if existing_paths:
                        new_ld_path = ':'.join(existing_paths)
                        if current_ld_path:
                            new_ld_path = f"{new_ld_path}:{current_ld_path}"
                        os.environ['LD_LIBRARY_PATH'] = new_ld_path
                        self.logger.info(f"Set LD_LIBRARY_PATH for K8s: {new_ld_path}")
                    
                    return Service(path)
            
            # Check standard K8s locations
            standard_paths = [
                '/usr/bin/chromedriver',
                '/usr/local/bin/chromedriver',
                '/opt/chromedriver/chromedriver'
            ]
            
            for path in standard_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using standard K8s ChromeDriver: {path}")
                    return Service(path)
        
        # Development environment fallback - use webdriver-manager
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver_path = ChromeDriverManager().install()
            self.logger.info(f"Using webdriver-manager ChromeDriver: {driver_path}")
            return Service(driver_path)
        except Exception as e:
            self.logger.warning(f"webdriver-manager failed: {e}")
        
        # Final fallback - let Selenium find ChromeDriver
        self.logger.info("Using default Selenium ChromeDriver discovery")
        return Service()
    
    def get_vanguard_etf_data(self, etf_ticker: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse Vanguard ETF historical price data using Selenium.
        
        Returns dictionary with processed price data.
        """
        if etf_ticker not in self.etf_configs:
            self.logger.error(f"ETF ticker {etf_ticker} not configured")
            return None
            
        config = self.etf_configs[etf_ticker]
        url = f"{self.base_url}{config['product_path']}"
        
        try:
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            import os
            import glob
            import pandas as pd
            from datetime import datetime
            
            # Setup Chrome with download directory
            download_dir = f'/tmp/vanguard_downloads_{etf_ticker}'
            os.makedirs(download_dir, exist_ok=True)
            
            # Add download preferences to existing chrome options
            chrome_options = self.chrome_options
            chrome_options.add_experimental_option('prefs', {
                'download.default_directory': download_dir,
                'download.prompt_for_download': False,
                'download.directory_upgrade': True,
                'safebrowsing.enabled': True
            })
            
            # Use undetected-chromedriver for automatic stealth
            import undetected_chromedriver as uc
            self.logger.info("Using undetected-chromedriver for automatic bot detection bypass")
            
            # For K8s ARM64, copy ChromeDriver to writable location for patching
            import platform
            import os
            import shutil
            import tempfile
            
            if platform.machine().lower() in ['aarch64', 'arm64']:
                # Copy ChromeDriver to writable temp location for undetected-chromedriver to patch
                temp_dir = tempfile.mkdtemp()
                temp_chromedriver = os.path.join(temp_dir, 'chromedriver')
                shutil.copy2('/usr/bin/chromedriver', temp_chromedriver)
                os.chmod(temp_chromedriver, 0o755)
                self.logger.info(f"ARM64 detected - copied ChromeDriver to writable location: {temp_chromedriver}")
                driver = uc.Chrome(options=chrome_options, driver_executable_path=temp_chromedriver, version_main=None)
            else:
                # Let undetected-chromedriver handle binary management for x86
                driver = uc.Chrome(options=chrome_options, version_main=None)
            
            try:
                self.logger.info(f"Loading Vanguard page for {etf_ticker}")
                driver.get(url)
                
                # Wait for page to load
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.TAG_NAME, 'body'))
                )
                time.sleep(15)  # Wait for Angular to render in K8s
                
                
                # Look for download-related elements with multiple strategies
                download_button = None
                
                # Try original selector
                try:
                    download_button = driver.find_element(
                        By.XPATH, 
                        '//button[contains(., "Download") and contains(., "prices")]'
                    )
                except:
                    pass
                
                # Try specific selectors for price download button
                if not download_button:
                    selectors = [
                        # Target the specific "Download XXXX prices" button
                        '//span[contains(text(), "Download") and contains(text(), "prices")]/parent::button',
                        '//button[.//span[contains(text(), "Download") and contains(text(), "prices")]]',
                        '//*[contains(text(), "Download") and contains(text(), "prices")]',
                        '//span[contains(text(), "Download") and contains(text(), "prices")]',
                        # Fallback to more generic selectors
                        '//button[contains(text(), "Download")]',
                        '//a[contains(text(), "Download")]',
                        '//button[contains(., "Download")]',
                        '//a[contains(., "Download")]'
                    ]
                    
                    for selector in selectors:
                        try:
                            download_button = driver.find_element(By.XPATH, selector)
                            break
                        except:
                            continue
                
                if not download_button:
                    self.logger.error("Could not find any download button - page might not have loaded properly")
                    # Save page source for debugging
                    page_source = driver.page_source
                    if len(page_source) < 1000:
                        self.logger.error(f"Page source seems incomplete ({len(page_source)} chars)")
                    return None
                
                # Clear any existing downloads
                existing_files = glob.glob(f'{download_dir}/*')
                for f in existing_files:
                    os.remove(f)
                
                self.logger.info(f"Found download button: {download_button.text}")
                driver.execute_script('arguments[0].click();', download_button)
                
                # Wait for download to complete
                downloaded_file = None
                for i in range(30):  # Wait up to 30 seconds
                    time.sleep(1)
                    files = glob.glob(f'{download_dir}/*')
                    if files:
                        complete_files = [f for f in files if not f.endswith('.crdownload')]
                        if complete_files:
                            downloaded_file = complete_files[0]
                            break
                
                if not downloaded_file:
                    self.logger.error(f"Download failed for {etf_ticker}")
                    return None
                
                self.logger.info(f"Downloaded: {os.path.basename(downloaded_file)}")
                
                # Parse the Excel file
                df = pd.read_excel(downloaded_file, skiprows=8)
                df = df.dropna(how='all')
                
                
                # Validate DataFrame has expected number of columns
                if df.shape[1] < 3:
                    self.logger.error(f"Expected at least 3 columns in Excel file, but found {df.shape[1]} columns")
                    self.logger.error(f"Available columns: {list(df.columns)}")
                    
                    # Try alternative parsing strategies
                    for skip_rows in [0, 1, 2, 5, 6, 7, 9, 10]:
                        try:
                            df_alt = pd.read_excel(downloaded_file, skiprows=skip_rows)
                            df_alt = df_alt.dropna(how='all')
                            if df_alt.shape[1] >= 3:
                                df = df_alt
                                break
                        except Exception as e:
                            continue
                    
                    # If still insufficient columns, file format may have changed
                    if df.shape[1] < 3:
                        raise ValueError(f"Cannot parse Excel file: expected at least 3 columns (Date, NAV, Market Price) but found {df.shape[1]} columns. The file format may have changed.")
                
                # Take only the first 3 columns (in case there are extra columns)
                df = df.iloc[:, :3]
                
                df.columns = ['Date', 'NAV_GBP', 'Market_Price_GBP']
                
                # Clean and convert data
                try:
                    # Handle date parsing with error checking
                    df['Date'] = pd.to_datetime(df['Date'], format='%d %b %Y', errors='coerce')
                    
                    # Clean and convert price columns with error handling
                    df['NAV_GBP'] = df['NAV_GBP'].astype(str).str.replace('£', '').str.replace(',', '')
                    df['Market_Price_GBP'] = df['Market_Price_GBP'].astype(str).str.replace('£', '').str.replace(',', '')
                    
                    # Convert to numeric with error handling
                    df['NAV_GBP'] = pd.to_numeric(df['NAV_GBP'], errors='coerce')
                    df['Market_Price_GBP'] = pd.to_numeric(df['Market_Price_GBP'], errors='coerce')
                    
                    # Remove rows with invalid data
                    df = df.dropna(subset=['Date', 'NAV_GBP', 'Market_Price_GBP'])
                    
                    if len(df) == 0:
                        raise ValueError("No valid data rows found after cleaning")
                    
                    # Calculate premium/discount
                    df['Premium_Discount'] = (df['Market_Price_GBP'] - df['NAV_GBP']) / df['NAV_GBP']
                    
                except Exception as e:
                    self.logger.error(f"Error cleaning data: {str(e)}")
                    raise
                
                # Add ETF metadata
                df['ETF_Ticker'] = etf_ticker
                df['Data_Source'] = 'Vanguard'
                df['Currency'] = config['currency']
                
                self.logger.info(f"Parsed {len(df)} price records for {etf_ticker}")
                
                # Clean up download file
                os.remove(downloaded_file)
                
                return {
                    'price_data': df,
                    'etf_config': config,
                    'records_count': len(df)
                }
                
            finally:
                driver.quit()
                
        except Exception as e:
            self.logger.error(f"Error collecting Vanguard ETF data for {etf_ticker}: {str(e)}")
            return None
    
    def save_etf_data_to_db(self, etf_data: Dict[str, Any]) -> int:
        """Save ETF data to database tables."""
        if not etf_data or 'price_data' not in etf_data:
            return 0
            
        df = etf_data['price_data']
        etf_ticker = df['ETF_Ticker'].iloc[0]
        
        try:
            import psycopg2
            conn = psycopg2.connect(self.database_url)
            
            with conn.cursor() as cur:
                # Insert both NAV and market price into etf_nav_history
                records_saved = 0
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO etf_nav_history (date, etf_ticker, nav, market_price, currency, data_source)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, etf_ticker) DO UPDATE SET
                        nav = EXCLUDED.nav,
                        market_price = EXCLUDED.market_price,
                        data_source = EXCLUDED.data_source,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        row['Date'], 
                        etf_ticker, 
                        row['NAV_GBP'], 
                        row['Market_Price_GBP'],
                        row['Currency'], 
                        row['Data_Source']
                    ))
                    records_saved += 1
                
                conn.commit()
                self.logger.info(f"Saved {records_saved} ETF records (NAV + market price) for {etf_ticker}")
                return records_saved
                
        except Exception as e:
            self.logger.error(f"Error saving ETF data to database: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()


class SSGAETFCollector(BaseCollector):
    """Collector for SSGA/SPDR ETF data including historical NAV."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.ssga.com"
        
        # Known SSGA UK ETFs
        self.etf_configs = {
            'GLTY': {
                'name': 'SPDR Bloomberg UK Gilt UCITS ETF (Dist)',
                'download_path': '/library-content/products/fund-data/etfs/emea/navhist-emea-en-sybg-gy.xlsx',
                'description': 'UK government bond ETF NAV tracking',
                'currency': 'GBP'
            }
            # Add more SSGA ETFs as needed
        }
    
    def get_ssga_etf_data(self, etf_ticker: str) -> Optional[Dict[str, Any]]:
        """
        Download and parse SSGA ETF historical NAV data.
        
        Returns dictionary with processed NAV data.
        """
        if etf_ticker not in self.etf_configs:
            self.logger.error(f"ETF ticker {etf_ticker} not configured")
            return None
            
        config = self.etf_configs[etf_ticker]
        download_url = f"{self.base_url}{config['download_path']}"
        
        try:
            import pandas as pd
            from datetime import datetime
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            self.logger.info(f"Downloading SSGA ETF data for {etf_ticker}")
            response = requests.get(download_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Parse the Excel file
            from io import BytesIO
            excel_file = BytesIO(response.content)
            
            # Read starting from row 6 (where headers are)
            df = pd.read_excel(excel_file, skiprows=5, engine='openpyxl')
            
            # Remove the header row itself and keep only data rows
            df = df.iloc[1:].copy()
            df.columns = ['Date', 'NAV', 'Shares_Outstanding', 'Total_Net_Assets']
            
            # Filter out rows that don't look like date/NAV data
            def is_valid_date_string(x):
                try:
                    if pd.isna(x) or not isinstance(x, str):
                        return False
                    # Check if it matches DD-MMM-YYYY pattern
                    if len(x.split('-')) == 3:
                        return True
                    return False
                except:
                    return False
            
            # Keep only rows with valid date strings
            valid_rows = df['Date'].apply(is_valid_date_string)
            df_clean = df[valid_rows].copy()
            
            # Convert data types
            df_clean['Date'] = pd.to_datetime(df_clean['Date'], format='%d-%b-%Y')
            df_clean['NAV'] = pd.to_numeric(df_clean['NAV'], errors='coerce')
            
            # Remove any remaining invalid rows
            df_clean = df_clean.dropna(subset=['Date', 'NAV'])
            
            # Add ETF metadata
            df_clean['ETF_Ticker'] = etf_ticker
            df_clean['Data_Source'] = 'SSGA'
            df_clean['Currency'] = config['currency']
            df_clean['Market_Price'] = None  # SSGA only provides NAV
            
            self.logger.info(f"Parsed {len(df_clean)} NAV records for {etf_ticker}")
            
            return {
                'nav_data': df_clean,
                'etf_config': config,
                'records_count': len(df_clean)
            }
            
        except Exception as e:
            self.logger.error(f"Error collecting SSGA ETF data for {etf_ticker}: {str(e)}")
            return None
    
    def save_etf_data_to_db(self, etf_data: Dict[str, Any]) -> int:
        """Save ETF NAV data to database."""
        if not etf_data or 'nav_data' not in etf_data:
            return 0
            
        df = etf_data['nav_data']
        etf_ticker = df['ETF_Ticker'].iloc[0]
        
        try:
            import psycopg2
            conn = psycopg2.connect(self.database_url)
            
            with conn.cursor() as cur:
                # Insert NAV data into etf_nav_history (market_price will be NULL)
                records_saved = 0
                for _, row in df.iterrows():
                    cur.execute("""
                        INSERT INTO etf_nav_history (date, etf_ticker, nav, market_price, currency, data_source)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, etf_ticker) DO UPDATE SET
                        nav = EXCLUDED.nav,
                        market_price = EXCLUDED.market_price,
                        data_source = EXCLUDED.data_source,
                        updated_at = CURRENT_TIMESTAMP
                    """, (
                        row['Date'], 
                        etf_ticker, 
                        row['NAV'], 
                        None,  # No market price from SSGA
                        row['Currency'], 
                        row['Data_Source']
                    ))
                    records_saved += 1
                
                conn.commit()
                self.logger.info(f"Saved {records_saved} NAV records for {etf_ticker}")
                return records_saved
                
        except Exception as e:
            self.logger.error(f"Error saving SSGA ETF data to database: {str(e)}")
            return 0
        finally:
            if conn:
                conn.close()


class iSharesETFCollector(BaseCollector):
    """Collector for iShares ETF data including NAV, holdings, and performance."""
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.ishares.com"
        
        # Known iShares UK ETFs relevant to gilt market analysis
        self.etf_configs = {
            'IGLT': {
                'name': 'iShares Core UK Gilts UCITS ETF',
                'product_id': '251806',
                'url_path': 'ishares-uk-gilts-ucits-etf',
                'description': 'Broad UK government bond exposure',
                'currency': 'GBP'
            },
            'INXG': {
                'name': 'iShares Index-Linked Gilts UCITS ETF', 
                'product_id': '251717',
                'url_path': 'ishares-indexlinked-gilts-ucits-etf',
                'description': 'UK inflation-linked government bonds',
                'currency': 'GBP'
            }
            # Add more ETFs as needed
        }
    
    def auto_discover_ajax_id(self, etf_ticker: str) -> Optional[str]:
        """
        Auto-discover current AJAX ID for ETF download using requests.
        
        Returns AJAX ID if found, None otherwise.
        """
        if etf_ticker not in self.etf_configs:
            return None
            
        config = self.etf_configs[etf_ticker]
        discover_url = f"{self.base_url}/uk/individual/en/products/{config['product_id']}/{config['url_path']}?siteEntryPassthrough=true"
        
        try:
            import re
            from bs4 import BeautifulSoup
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            response = requests.get(discover_url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for XLS download element (prioritize XLS over CSV)
            download_links = soup.find_all('a', class_='icon-xls-export')
            
            # First try to find XLS file type specifically
            for download_link in download_links:
                href = download_link.get('href')
                if href and '.ajax' in href and 'fileType=xls' in href:
                    ajax_match = re.search(r'/(\d{13})\.ajax', href)
                    if ajax_match:
                        ajax_id = ajax_match.group(1)
                        self.logger.info(f"Auto-discovered AJAX ID for {etf_ticker}: {ajax_id}")
                        return ajax_id
            
            # Fallback: look for XLS patterns in page content using regex
            xls_pattern = r'/(\d{13})\.ajax\?[^"]*fileType=xls'
            xls_matches = re.findall(xls_pattern, response.text)
            if xls_matches:
                ajax_id = xls_matches[0]  # Take first XLS match
                self.logger.info(f"Auto-discovered AJAX ID (regex) for {etf_ticker}: {ajax_id}")
                return ajax_id
                        
        except Exception as e:
            self.logger.warning(f"AJAX ID auto-discovery failed for {etf_ticker}: {e}")
                
        return None
    
    def get_etf_excel_data(self, etf_ticker: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Download and parse iShares ETF Excel data.
        
        Returns dictionary with worksheet names as keys and DataFrames as values.
        """
        if etf_ticker not in self.etf_configs:
            self.logger.error(f"ETF ticker {etf_ticker} not configured")
            return None
            
        config = self.etf_configs[etf_ticker]
        
        # Always discover AJAX ID from page
        ajax_id = self.auto_discover_ajax_id(etf_ticker)
        if not ajax_id:
            self.logger.error(f"Could not discover AJAX ID for {etf_ticker}")
            return None
        
        # Construct Excel download URL using discovered AJAX ID
        excel_url = f"{self.base_url}/uk/individual/en/products/{config['product_id']}/{config['url_path']}/{ajax_id}.ajax"
        
        # Generate filename from ETF name
        filename = f"{config['name'].replace(' ', '-')}_fund"
        excel_url += f"?fileType=xls&fileName={filename}&dataType=fund"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
            'Referer': f"{self.base_url}/uk/individual/en/products/{config['product_id']}"
        }
        
        try:
            self.logger.info(f"Downloading {etf_ticker} ETF data from iShares")
            response = requests.get(excel_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            # Handle Excel file (XML format)
            excel_file = BytesIO(response.content)
            
            # Try different engines based on file format
            try:
                # First try openpyxl for modern Excel files
                xl_file = pd.ExcelFile(excel_file, engine='openpyxl')
            except:
                try:
                    # Fall back to xlrd for older Excel files
                    xl_file = pd.ExcelFile(excel_file, engine='xlrd')
                except:
                    # Handle as XML Excel manually
                    self.logger.warning(f"Standard Excel engines failed for {etf_ticker}, using alternative parsing")
                    return self._parse_xml_excel(response.content, etf_ticker)
            
            # Read all worksheets
            worksheets = {}
            for sheet_name in xl_file.sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)
                    worksheets[sheet_name] = df
                    self.logger.info(f"Read {sheet_name} worksheet: {df.shape[0]} rows, {df.shape[1]} columns")
                except Exception as e:
                    self.logger.warning(f"Could not read worksheet {sheet_name}: {str(e)}")
            
            return worksheets
            
        except Exception as e:
            self.logger.error(f"Failed to download {etf_ticker} ETF data: {str(e)}")
            return None
    
    def _parse_xml_excel(self, content: bytes, etf_ticker: str) -> Optional[Dict[str, pd.DataFrame]]:
        """
        Fallback parser for XML Excel format when standard engines fail.
        """
        try:
            import xml.etree.ElementTree as ET
            import re
            from datetime import datetime
            
            content_str = content.decode('utf-8-sig')
            
            # Parse XML
            root = ET.fromstring(content_str)
            
            # Define namespace
            ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}
            
            worksheets = {}
            
            # Find all worksheets
            for worksheet in root.findall('.//ss:Worksheet', ns):
                sheet_name = worksheet.get('{urn:schemas-microsoft-com:office:spreadsheet}Name', 'Unknown')
                
                # Extract table data
                table = worksheet.find('.//ss:Table', ns)
                if table is not None:
                    rows_data = []
                    
                    # Get all rows
                    for row in table.findall('.//ss:Row', ns):
                        row_data = []
                        cells = row.findall('.//ss:Cell', ns)
                        
                        for cell in cells:
                            data_elem = cell.find('.//ss:Data', ns)
                            if data_elem is not None:
                                row_data.append(data_elem.text if data_elem.text else '')
                            else:
                                row_data.append('')
                        
                        if row_data:  # Only add non-empty rows
                            rows_data.append(row_data)
                    
                    # Convert to DataFrame
                    if rows_data:
                        # Find the maximum number of columns across all rows
                        max_cols = max(len(row) for row in rows_data)
                        
                        # Normalize all rows to have the same number of columns
                        normalized_rows = []
                        for row in rows_data:
                            normalized_row = row + [''] * (max_cols - len(row))
                            normalized_rows.append(normalized_row[:max_cols])
                        
                        # Use first row as headers if it looks like headers, otherwise generate column names
                        if len(normalized_rows) > 1 and normalized_rows[0]:
                            headers = [str(h) if h else f'Col_{i}' for i, h in enumerate(normalized_rows[0])]
                            data_rows = normalized_rows[1:]
                        else:
                            headers = [f'Col_{i}' for i in range(max_cols)]
                            data_rows = normalized_rows
                        
                        # Ensure headers length exactly matches max_cols
                        if len(headers) != max_cols:
                            headers = [f'Col_{i}' for i in range(max_cols)]
                        
                        # Create DataFrame
                        if data_rows:
                            df = pd.DataFrame(data_rows, columns=headers)
                            worksheets[sheet_name] = df
                            self.logger.info(f"Parsed {sheet_name} worksheet: {df.shape[0]} rows, {df.shape[1]} columns")
                        else:
                            worksheets[sheet_name] = pd.DataFrame()
                    else:
                        worksheets[sheet_name] = pd.DataFrame()
                else:
                    worksheets[sheet_name] = pd.DataFrame()
            
            self.logger.info(f"Successfully parsed XML Excel with worksheets: {list(worksheets.keys())}")
            return worksheets
            
        except Exception as e:
            self.logger.error(f"XML Excel parsing failed for {etf_ticker}: {str(e)}")
            return None
    
    def extract_historical_nav(self, worksheets: Dict[str, pd.DataFrame], etf_ticker: str) -> Optional[pd.DataFrame]:
        """
        Extract historical NAV data from Excel worksheets.
        
        Returns DataFrame with columns: date, nav, etf_ticker
        """
        # Look for historical data in common worksheet names
        historical_sheets = ['Historical', 'Performance', 'Nav History', 'NAV']
        
        for sheet_name in historical_sheets:
            if sheet_name in worksheets:
                df = worksheets[sheet_name]
                
                # Look for date and NAV columns
                date_cols = [col for col in df.columns if 'date' in str(col).lower() or 'as of' in str(col).lower()]
                nav_cols = [col for col in df.columns if 'nav' in str(col).lower() or 'price' in str(col).lower()]
                
                if date_cols and nav_cols:
                    # Extract historical NAV data
                    historical_data = df[[date_cols[0], nav_cols[0]]].copy()
                    historical_data.columns = ['date', 'nav']
                    historical_data['etf_ticker'] = etf_ticker
                    
                    # Clean and convert data types with flexible date parsing
                    # Handle both "Sep" and "Sept" formats (iShares format changes)
                    def parse_dates_flexible(date_series):
                        # First normalize "Sept" to "Sep" to match standard format
                        normalized_dates = date_series.astype(str).str.replace('/Sept/', '/Sep/', regex=False)
                        return pd.to_datetime(normalized_dates, format='%d/%b/%Y')
                    
                    try:
                        historical_data['date'] = parse_dates_flexible(historical_data['date'])
                    except ValueError:
                        # Fallback to flexible parsing if normalization doesn't work
                        historical_data['date'] = pd.to_datetime(historical_data['date'], format='mixed', dayfirst=True)
                    
                    historical_data['nav'] = pd.to_numeric(historical_data['nav'], errors='coerce')
                    
                    # Remove rows with missing data
                    historical_data = historical_data.dropna()
                    
                    self.logger.info(f"Extracted {len(historical_data)} historical NAV records for {etf_ticker}")
                    return historical_data
        
        self.logger.warning(f"No historical NAV data found for {etf_ticker}")
        return None
    
    def extract_holdings(self, worksheets: Dict[str, pd.DataFrame], etf_ticker: str) -> Optional[pd.DataFrame]:
        """
        Extract current holdings data from Excel worksheets.
        
        Returns DataFrame with ETF holdings information.
        """
        if 'Holdings' in worksheets:
            holdings_df = worksheets['Holdings'].copy()
            holdings_df['etf_ticker'] = etf_ticker
            holdings_df['as_of_date'] = datetime.now().date()
            
            self.logger.info(f"Extracted {len(holdings_df)} holdings for {etf_ticker}")
            return holdings_df
        
        return None
    
    def store_nav_data(self, nav_data: pd.DataFrame) -> int:
        """Store historical NAV data in database."""
        if nav_data is None or nav_data.empty:
            return 0
        
        # Prepare data for bulk upsert
        bulk_data = []
        for _, row in nav_data.iterrows():
            bulk_data.append({
                "date": row["date"].date(),
                "etf_ticker": row["etf_ticker"],
                "nav": float(row["nav"]),
                "data_source": "iShares"
            })
        
        # Bulk upsert
        if bulk_data:
            success_count = self.bulk_upsert_data("etf_nav_history", bulk_data,
                                                 conflict_columns=['date', 'etf_ticker'])
            return success_count
        
        return 0
    
    def store_holdings_data(self, holdings_data: pd.DataFrame) -> int:
        """Store ETF holdings data in database."""
        if holdings_data is None or holdings_data.empty:
            return 0
        
        # This would need to be implemented based on holdings data structure
        # For now, return placeholder
        self.logger.info(f"Holdings data structure: {holdings_data.columns.tolist()}")
        return len(holdings_data)


def collect_ishares_etf_nav(database_url=None, etf_tickers=['IGLT']) -> int:
    """
    Collect iShares ETF NAV data for specified tickers.
    
    Args:
        database_url: Database connection string (None for safe mode)
        etf_tickers: List of ETF tickers to collect (default: IGLT)
    
    Returns:
        Total number of NAV records collected
    """
    collector = iSharesETFCollector(database_url)
    total_records = 0
    
    collector.logger.info(f"Starting iShares ETF NAV collection for {len(etf_tickers)} ETFs: {etf_tickers}")
    
    for etf_ticker in etf_tickers:
        try:
            # Download Excel data
            worksheets = collector.get_etf_excel_data(etf_ticker)
            if not worksheets:
                collector.logger.warning(f"No data retrieved for {etf_ticker}")
                continue
            
            # Extract historical NAV data
            nav_data = collector.extract_historical_nav(worksheets, etf_ticker)
            if nav_data is not None:
                # Store NAV data
                count = collector.store_nav_data(nav_data)
                total_records += count
                collector.logger.info(f"Stored {count} NAV records for {etf_ticker}")
            
            # Extract and store holdings (optional)
            holdings_data = collector.extract_holdings(worksheets, etf_ticker)
            if holdings_data is not None:
                holdings_count = collector.store_holdings_data(holdings_data)
                collector.logger.info(f"Processed {holdings_count} holdings for {etf_ticker}")
            
        except Exception as e:
            collector.logger.error(f"Error collecting data for {etf_ticker}: {str(e)}")
            continue
    
    if total_records == 0:
        raise RuntimeError(f"Failed to collect any ETF NAV data for {etf_tickers}. Check logs for specific errors.")
    
    collector.logger.info(f"Total ETF NAV records collected: {total_records}")
    return total_records


def collect_etf_premium_discount_analysis(database_url=None, etf_tickers=['IGLT']) -> Dict[str, float]:
    """
    Collect current ETF prices and calculate premium/discount to NAV.
    
    This supports the ETF arbitrage strategies outlined in the Gilt Market Analysis Guide.
    
    Returns:
        Dictionary with ETF tickers and their current premium/discount percentages
    """
    # This would integrate with live price feeds to calculate:
    # Premium/Discount = (ETF_Price - NAV) / NAV
    
    # Placeholder implementation
    results = {}
    for ticker in etf_tickers:
        # This would fetch live ETF price and latest NAV
        results[ticker] = {
            'current_price': None,  # From market data feed
            'latest_nav': None,     # From NAV data
            'premium_discount': None,  # Calculated percentage
            'signal': None          # BUY/SELL/HOLD based on thresholds
        }
    
    return results


def collect_vanguard_etf_data(database_url=None, etf_tickers=['VGOV']) -> int:
    """
    Collect Vanguard ETF historical price data for specified tickers.
    
    Args:
        database_url: Database connection string (None for safe mode)
        etf_tickers: List of ETF tickers to collect (default: VGOV)
    
    Returns:
        Total number of records collected
    """
    collector = VanguardETFCollector(database_url)
    total_records = 0
    
    for ticker in etf_tickers:
        try:
            collector.logger.info(f"Collecting Vanguard ETF data for {ticker}")
            etf_data = collector.get_vanguard_etf_data(ticker)
            
            if etf_data and database_url:
                records_saved = collector.save_etf_data_to_db(etf_data)
                total_records += records_saved
                collector.logger.info(f"Saved {records_saved} records for {ticker}")
            elif etf_data:
                collector.logger.info(f"Safe mode: Would save {etf_data['records_count']} records for {ticker}")
                total_records += etf_data['records_count']
            else:
                collector.logger.error(f"Failed to collect data for {ticker}")
                
        except Exception as e:
            collector.logger.error(f"Error collecting {ticker}: {str(e)}")
    
    if total_records == 0:
        raise RuntimeError(f"Failed to collect any Vanguard ETF data for {etf_tickers}. Check logs for specific errors.")
    
    return total_records


def collect_ssga_etf_data(database_url=None, etf_tickers=['GLTY']) -> int:
    """
    Collect SSGA ETF historical NAV data for specified tickers.
    
    Args:
        database_url: Database connection string (None for safe mode)
        etf_tickers: List of ETF tickers to collect (default: GLTY)
    
    Returns:
        Total number of records collected
    """
    collector = SSGAETFCollector(database_url)
    total_records = 0
    
    for ticker in etf_tickers:
        try:
            collector.logger.info(f"Collecting SSGA ETF data for {ticker}")
            etf_data = collector.get_ssga_etf_data(ticker)
            
            if etf_data and database_url:
                records_saved = collector.save_etf_data_to_db(etf_data)
                total_records += records_saved
                collector.logger.info(f"Saved {records_saved} records for {ticker}")
            elif etf_data:
                collector.logger.info(f"Safe mode: Would save {etf_data['records_count']} records for {ticker}")
                total_records += etf_data['records_count']
            else:
                collector.logger.error(f"Failed to collect data for {ticker}")
                
        except Exception as e:
            collector.logger.error(f"Error collecting {ticker}: {str(e)}")
    
    if total_records == 0:
        raise RuntimeError(f"Failed to collect any SSGA ETF data for {etf_tickers}. Check logs for specific errors.")
    
    return total_records
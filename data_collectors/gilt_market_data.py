"""
Gilt market data collector for real-time broker prices.

This module requires additional dependencies:
- scipy>=1.11.0 (for YTM calculations)
- selenium>=4.15.0 (for web scraping) 
- webdriver-manager>=4.0.0 (for Chrome driver management)

Note: On ARM64 architectures, Chrome/ChromeDriver compatibility may be limited.
"""
import time
import re
import platform
from datetime import datetime
from typing import Dict, Any, List, Optional
from scipy.optimize import fsolve
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import os
try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None
from .base import BaseCollector


class GiltMarketCollector(BaseCollector):
    """
    Collector for real-time gilt market prices from Hargreaves Lansdown broker.
    Scrapes gilt prices and calculates yields, accrued interest, and after-tax metrics.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.hl.co.uk/shares/corporate-bonds-gilts/bond-prices/uk-gilts"
        self.chrome_options = self._setup_chrome_options()
    
    def _setup_chrome_options(self):
        """Configure Chrome options for Pi-friendly headless scraping."""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--remote-debugging-port=9222")
        return options
    
    def _get_chrome_service(self):
        """Get Chrome service for both Pi and development environments."""
        arch = platform.machine().lower()
        system = platform.system()
        
        # Try Pi-specific paths first (ARM64 Linux)
        if arch in ['aarch64', 'arm64'] and system == 'Linux':
            # Check shared volume locations (init container installation)
            shared_paths = [
                '/shared/usr/bin/chromedriver',  # Most likely location from chromium-driver package
                '/shared/usr/lib/chromium-browser/chromedriver',
                '/shared/snap/chromium/current/usr/lib/chromium-browser/chromedriver'
            ]
            
            for path in shared_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using Pi ChromeDriver from init container: {path}")
                    
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
                            os.environ['LD_LIBRARY_PATH'] = f"{new_ld_path}:{current_ld_path}"
                        else:
                            os.environ['LD_LIBRARY_PATH'] = new_ld_path
                        self.logger.info(f"Set LD_LIBRARY_PATH: {os.environ['LD_LIBRARY_PATH']}")
                    
                    # Also set chromium binary location if available
                    if os.path.exists('/shared/usr/bin/chromium'):
                        self.chrome_options.binary_location = '/shared/usr/bin/chromium'
                        self.logger.info("Using Chromium binary from shared volume")
                    return Service(path)
            
            # Fallback to system paths
            system_paths = [
                '/usr/bin/chromedriver',  # Most likely system location
                '/usr/lib/chromium-browser/chromedriver',
                '/snap/chromium/current/usr/lib/chromium-browser/chromedriver'
            ]
            
            for path in system_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using Pi ChromeDriver from system: {path}")
                    return Service(path)
        
        # Fallback to webdriver-manager for development environments
        if ChromeDriverManager is not None:
            try:
                self.logger.info("Using ChromeDriver from webdriver-manager (development environment)")
                return Service(ChromeDriverManager().install())
            except Exception as e:
                self.logger.warning(f"webdriver-manager failed: {e}")
        
        # Final fallback - check common system paths
        common_paths = [
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            '/opt/homebrew/bin/chromedriver',  # macOS with Homebrew
        ]
        
        for path in common_paths:
            if os.path.exists(path):
                self.logger.info(f"Using system ChromeDriver at: {path}")
                return Service(path)
        
        # No ChromeDriver found
        if arch in ['aarch64', 'arm64'] and system == 'Linux':
            raise RuntimeError("ChromeDriver not found - install with: apt install chromium-chromedriver")
        else:
            raise RuntimeError("ChromeDriver not found - install via webdriver-manager or system package manager")
    
    def calculate_accrued_interest(self, face_value: float, coupon_rate: float, 
                                 last_coupon_date: datetime, settlement_date: datetime, 
                                 payments_per_year: int = 2) -> float:
        """Calculate accrued interest since last coupon payment."""
        annual_coupon = coupon_rate * face_value
        coupon_payment = annual_coupon / payments_per_year
        
        # Days since last coupon payment
        days_since_coupon = (settlement_date - last_coupon_date).days
        
        # Days in coupon period (assume 6 months = 182.5 days for semi-annual)
        days_in_period = 365.25 / payments_per_year
        
        # Accrued interest = (Days since coupon / Days in period) * Coupon payment
        accrued = (days_since_coupon / days_in_period) * coupon_payment
        
        return accrued
    
    def estimate_coupon_dates(self, bond_name: str, maturity_date: datetime, 
                            settlement_date: datetime) -> datetime:
        """Estimate coupon payment dates based on maturity date."""
        # Extract maturity month/day from bond name or maturity date
        if maturity_date:
            mat_month = maturity_date.month
            mat_day = maturity_date.day
        else:
            # Default assumption for UK gilts
            mat_month = 7
            mat_day = 31
        
        year = settlement_date.year
        
        # Calculate the two coupon dates per year (6 months apart)
        if mat_month <= 6:
            coupon1 = datetime(year, mat_month, mat_day)
            coupon2 = datetime(year, mat_month + 6, mat_day)
        else:
            coupon1 = datetime(year, mat_month - 6, mat_day)
            coupon2 = datetime(year, mat_month, mat_day)
        
        # Handle edge cases for day of month
        try:
            coupon1 = datetime(year, coupon1.month, min(coupon1.day, 28))
            coupon2 = datetime(year, coupon2.month, min(coupon2.day, 28))
        except:
            coupon1 = datetime(year, coupon1.month, 28)
            coupon2 = datetime(year, coupon2.month, 28)
        
        # Find the most recent coupon date before settlement
        if settlement_date >= coupon2:
            return coupon2
        elif settlement_date >= coupon1:
            return coupon1
        else:
            # Must be from previous year
            try:
                if mat_month <= 6:
                    return datetime(year - 1, mat_month + 6, min(mat_day, 28))
                else:
                    return datetime(year - 1, mat_month, min(mat_day, 28))
            except:
                return datetime(year - 1, 7, 31)  # Default fallback
    
    def _determine_face_value(self, clean_price: float) -> float:
        """
        Determine face value based on clean price.
        HL quotes some bonds with £1 face value (typically when price < £2)
        and others with £100 face value.
        """
        if clean_price < 2.0:
            self.logger.debug(f"Using £1 face value for low price: {clean_price}")
            return 1.0  # £1 face value for low-priced bonds
        else:
            return 100.0  # £100 face value for standard bonds
    
    def extract_bond_identifiers(self, bond_name_text: str, bond_link_url: str) -> Dict[str, str]:
        """
        Extract bond identifiers from bond name text and link URL.
        
        Args:
            bond_name_text: Full text from the bond name cell
            bond_link_url: URL of the bond name link
            
        Returns:
            dict: Contains currency_code, isin, short_code, and combined_id
        """
        identifiers = {
            'currency_code': None,
            'isin': None,
            'short_code': None,
            'combined_id': None
        }
        
        # Extract Currency Code (prioritize from text, default to GBP for HL UK pages)
        currency_match = re.search(r'\b(GBP|USD|EUR)\b', bond_name_text)
        if currency_match:
            identifiers['currency_code'] = currency_match.group(1)
        else:
            identifiers['currency_code'] = 'GBP'  # Default for HL UK pages
        
        # Extract ISIN (12 characters: 2 letters + 10 alphanumeric)
        isin_match = re.search(r'\b[A-Z]{2}[A-Z0-9]{10}\b', bond_name_text)
        if isin_match:
            identifiers['isin'] = isin_match.group(0)
        
        # Extract Short Code from URL
        # URL format: https://www.hl.co.uk/shares/shares-search-results/{SHORT_CODE}
        if bond_link_url:
            url_parts = bond_link_url.split('/')
            if len(url_parts) > 0:
                short_code = url_parts[-1]  # Last part of URL
                if re.match(r'^[A-Z0-9]{6,10}$', short_code):  # Validate format
                    identifiers['short_code'] = short_code
        
        # Create combined ID if all parts are available
        if identifiers['currency_code'] and identifiers['isin'] and identifiers['short_code']:
            identifiers['combined_id'] = f"{identifiers['currency_code']} | {identifiers['isin']} | {identifiers['short_code']}"
        
        return identifiers
    
    def calculate_ytm_from_dirty(self, dirty_price: float, face_value: float, 
                               coupon_rate: float, years_to_maturity: float, 
                               payments_per_year: int = 2) -> Optional[float]:
        """Calculate YTM using dirty price."""
        coupon_payment = (coupon_rate * face_value) / payments_per_year
        total_periods = years_to_maturity * payments_per_year  # Keep as float, don't round to int
        
        # Handle edge case where bond has essentially no time remaining
        if total_periods <= 0.01:  # Less than ~3.6 days remaining
            self.logger.warning(f"Bond has minimal time remaining (years_to_maturity: {years_to_maturity})")
            return None
        
        def bond_price_equation(ytm):
            if ytm == 0:
                return coupon_payment * total_periods + face_value - dirty_price
            
            periodic_rate = ytm / payments_per_year
            
            # Avoid numerical issues with very negative rates
            if periodic_rate <= -0.99:
                return float('inf')
            
            try:
                present_value_coupons = coupon_payment * (1 - (1 + periodic_rate) ** -total_periods) / periodic_rate
                present_value_face = face_value / (1 + periodic_rate) ** total_periods
                return present_value_coupons + present_value_face - dirty_price
            except (OverflowError, ZeroDivisionError):
                return float('inf')
        
        # Use approximate YTM as initial guess
        initial_guess = (coupon_rate + (face_value - dirty_price) / years_to_maturity) / ((face_value + dirty_price) / 2)
        
        try:
            ytm_solution = fsolve(bond_price_equation, initial_guess, xtol=1e-8)[0]
            
            # Sanity check: YTM should be reasonable (between -50% and 100%)
            if -0.5 <= ytm_solution <= 1.0:
                return ytm_solution
            else:
                self.logger.warning(f"YTM calculation returned unreasonable value: {ytm_solution*100:.2f}%")
                return None
                
        except Exception as e:
            self.logger.warning(f"YTM calculation failed: {str(e)}")
            return None
    
    def calculate_after_tax_ytm(self, dirty_price: float, face_value: float, 
                              coupon_rate: float, years_to_maturity: float, 
                              tax_rate_on_coupons: float = 0.30, 
                              payments_per_year: int = 2) -> Optional[float]:
        """
        Calculate after-tax YTM where:
        - Coupon payments are taxed at tax_rate_on_coupons
        - Capital gains are tax-free (typical for UK gilts)
        """
        coupon_payment = (coupon_rate * face_value) / payments_per_year
        after_tax_coupon = coupon_payment * (1 - tax_rate_on_coupons)
        total_periods = int(years_to_maturity * payments_per_year)
        
        def after_tax_bond_equation(ytm):
            if ytm == 0:
                return after_tax_coupon * total_periods + face_value - dirty_price
            
            periodic_rate = ytm / payments_per_year
            # Present value of after-tax coupons
            present_value_coupons = after_tax_coupon * (1 - (1 + periodic_rate) ** -total_periods) / periodic_rate
            # Present value of face value (no tax on capital gains)
            present_value_face = face_value / (1 + periodic_rate) ** total_periods
            
            return present_value_coupons + present_value_face - dirty_price
        
        initial_guess = coupon_rate if coupon_rate > 0 else 0.05
        
        try:
            ytm_solution = fsolve(after_tax_bond_equation, initial_guess)[0]
            return ytm_solution
        except:
            return None
    
    def parse_maturity_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats and return datetime object."""
        date_formats = [
            '%d/%m/%Y', '%d %b %Y', '%d %B %Y',
            '%Y-%m-%d', '%b %Y', '%B %Y'
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue
        
        # Handle formats like "Jul 2025" by adding day 1
        try:
            parts = date_str.strip().split()
            if len(parts) == 2:
                return datetime.strptime(f"01 {date_str.strip()}", '%d %b %Y')
        except:
            pass
        
        return None
    
    def calculate_years_to_maturity(self, maturity_date: datetime) -> Optional[float]:
        """Calculate years from today to maturity date."""
        today = datetime.now()
        if maturity_date <= today:
            return None
        
        days_to_maturity = (maturity_date - today).days
        return days_to_maturity / 365.25
    
    def scrape_gilt_prices(self) -> List[Dict[str, Any]]:
        """Scrape UK Gilts and calculate YTM based on clean prices."""
        arch = platform.machine().lower()
        system = platform.system()
        
        try:
            return self._scrape_with_selenium()
        except Exception as e:
            # On ARM64 Linux (Pi), fail hard - don't fall back gracefully
            if arch in ['aarch64', 'arm64'] and system == 'Linux':
                self.logger.error(f"CRITICAL: Selenium scraping failed on ARM64 Linux: {str(e)}")
                self.logger.error("ChromeDriver/Chromium should be installed by init container")
                raise RuntimeError(f"ARM64 gilt market scraping failed: {e}") from e
            else:
                # On development environments, fall back gracefully
                self.logger.error(f"Selenium scraping failed: {str(e)}")
                self.logger.info("Falling back to empty dataset - browser automation not available")
                return []
    
    def _scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Scrape using Selenium WebDriver."""
        driver = None
        try:
            service = self._get_chrome_service()
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            driver.get(self.base_url)
            
            # Wait for content to load
            time.sleep(7)
            
            bonds = []
            settlement_date = datetime.now()
            
            # Look for table rows directly
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
            
            # Get header to understand structure
            if len(rows) > 0:
                header_cells = rows[0].find_elements(By.TAG_NAME, "th")
                if not header_cells:
                    header_cells = rows[0].find_elements(By.TAG_NAME, "td")
                
                headers = [cell.text.strip().lower() for cell in header_cells]
                self.logger.info(f"Table structure: {headers}")
                
                # Find column indices
                issuer_col = next((i for i, h in enumerate(headers) if 'issuer' in h), 0)
                coupon_col = next((i for i, h in enumerate(headers) if 'coupon' in h), 1)
                maturity_col = next((i for i, h in enumerate(headers) if 'maturity' in h), 2)
                price_col = next((i for i, h in enumerate(headers) if 'price' in h), 3)
            
            for i, row in enumerate(rows[1:]):  # Skip header
                cells = row.find_elements(By.TAG_NAME, "td")
                
                if len(cells) >= 4:
                    try:
                        # Get bond name/issuer and link information
                        bond_name_cell = cells[issuer_col]
                        bond_name_text = bond_name_cell.text.strip()  # Full cell text (includes ISIN)
                        
                        # Get bond link URL
                        bond_link_url = None
                        try:
                            bond_link = bond_name_cell.find_element(By.TAG_NAME, "a")
                            bond_name = bond_link.text.strip()  # Display name from link
                            bond_link_url = bond_link.get_attribute("href")
                        except:
                            bond_name = bond_name_text  # Fallback to full text
                        
                        # Extract bond identifiers (ISIN, currency, short code)
                        identifiers = self.extract_bond_identifiers(bond_name_text, bond_link_url)
                        
                        # Skip if not a Treasury bond
                        if 'treasury' not in bond_name.lower():
                            continue
                        
                        # Get coupon rate
                        coupon_text = cells[coupon_col].text.strip()
                        coupon_match = re.search(r'(\d+\.?\d*)', coupon_text)
                        if not coupon_match:
                            continue
                        coupon_rate = float(coupon_match.group(1)) / 100
                        
                        # Get maturity date
                        maturity_text = cells[maturity_col].text.strip()
                        maturity_date = self.parse_maturity_date(maturity_text)
                        if not maturity_date:
                            # Try to extract year from bond name and estimate
                            year_match = re.search(r'20\d{2}', bond_name)
                            if year_match:
                                year = int(year_match.group(0))
                                maturity_date = datetime(year, 7, 15)  # Estimate mid-year
                            else:
                                continue
                        
                        # Get clean price
                        price_text = cells[price_col].text.strip()
                        price_match = re.search(r'(\d+\.?\d*)', price_text)
                        if not price_match:
                            continue
                        
                        clean_price = float(price_match.group(1))
                        
                        # Validate price range (gilts typically trade 20-150)
                        if not (20 <= clean_price <= 200):
                            continue
                        
                        years_to_maturity = self.calculate_years_to_maturity(maturity_date)
                        if not years_to_maturity or years_to_maturity <= 0:
                            continue
                        
                        # Calculate accrued interest
                        # Determine face value based on clean price
                        face_value = self._determine_face_value(clean_price)
                        
                        last_coupon_date = self.estimate_coupon_dates(bond_name, maturity_date, settlement_date)
                        accrued_interest = self.calculate_accrued_interest(
                            face_value, coupon_rate, last_coupon_date, settlement_date
                        )
                        
                        # Calculate dirty price
                        dirty_price = clean_price + accrued_interest
                        
                        # Calculate YTM using dirty price
                        ytm = self.calculate_ytm_from_dirty(dirty_price, face_value, coupon_rate, years_to_maturity)
                        
                        # Calculate after-tax YTM (30% tax on coupons, no tax on capital gains)
                        after_tax_ytm = self.calculate_after_tax_ytm(dirty_price, face_value, coupon_rate, years_to_maturity, 0.30)
                        
                        # Create unique bond name for Treasury Strips and other duplicates
                        unique_bond_name = bond_name.split('\n')[0]  # Take first line
                        if 'treasury strip' in unique_bond_name.lower():
                            # For Treasury Strips, include maturity date to make unique
                            maturity_str = maturity_date.strftime('%b-%Y')
                            unique_bond_name = f"{unique_bond_name} {maturity_str}"
                        
                        bonds.append({
                            'bond_name': unique_bond_name,
                            'clean_price': clean_price,
                            'accrued_interest': accrued_interest,
                            'dirty_price': dirty_price,
                            'coupon_rate': coupon_rate,
                            'maturity_date': maturity_date,
                            'years_to_maturity': years_to_maturity,
                            'ytm': ytm,
                            'after_tax_ytm': after_tax_ytm,
                            'scraped_date': settlement_date.date(),
                            # Bond identifiers
                            'currency_code': identifiers['currency_code'],
                            'isin': identifiers['isin'],
                            'short_code': identifiers['short_code'],
                            'combined_id': identifiers['combined_id']
                        })
                        
                    except Exception as e:
                        self.logger.debug(f"Error processing bond row {i}: {str(e)}")
                        continue
            
            self.logger.info(f"Successfully scraped {len(bonds)} gilt market prices")
            return bonds
            
        except Exception as e:
            self.logger.error(f"Scraping error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


def collect_gilt_market_prices(database_url=None):
    """Collect real-time gilt market prices from Hargreaves Lansdown broker."""
    collector = GiltMarketCollector(database_url)
    
    try:
        # Scrape gilt prices from broker website
        gilt_data = collector.scrape_gilt_prices()
        
        if not gilt_data:
            collector.logger.info("No gilt market data found")
            return 0
        
        # Process and store data
        bulk_data = []
        for gilt in gilt_data:
            try:
                # Convert to database format (ensure NumPy types are converted to Python types)
                data = {
                    'bond_name': str(gilt['bond_name']) if gilt['bond_name'] is not None else None,
                    'clean_price': float(gilt['clean_price']) if gilt['clean_price'] is not None else None,
                    'accrued_interest': float(gilt['accrued_interest']) if gilt['accrued_interest'] is not None else None,
                    'dirty_price': float(gilt['dirty_price']) if gilt['dirty_price'] is not None else None,
                    'coupon_rate': float(gilt['coupon_rate']) if gilt['coupon_rate'] is not None else None,
                    'maturity_date': gilt['maturity_date'].date(),
                    'years_to_maturity': float(gilt['years_to_maturity']) if gilt['years_to_maturity'] is not None else None,
                    'ytm': float(gilt['ytm']) if gilt['ytm'] is not None else None,
                    'after_tax_ytm': float(gilt['after_tax_ytm']) if gilt['after_tax_ytm'] is not None else None,
                    'scraped_date': gilt['scraped_date'],
                    # Bond identifiers
                    'currency_code': str(gilt['currency_code']) if gilt['currency_code'] is not None else None,
                    'isin': str(gilt['isin']) if gilt['isin'] is not None else None,
                    'short_code': str(gilt['short_code']) if gilt['short_code'] is not None else None,
                    'combined_id': str(gilt['combined_id']) if gilt['combined_id'] is not None else None
                }
                bulk_data.append(data)
                
            except Exception as e:
                collector.logger.error(f"Error processing gilt data: {str(e)}")
                continue
        
        # Bulk upsert all records with custom conflict columns
        if bulk_data:
            success_count = collector.bulk_upsert_data(
                "gilt_market_prices", 
                bulk_data,
                conflict_columns=['bond_name', 'scraped_date']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(bulk_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} gilt market price records")
            return success_count
        else:
            collector.logger.info("No valid gilt market data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect gilt market prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task


class IndexLinkedGiltCollector(GiltMarketCollector):
    """
    Collector for real-time index-linked gilt prices from Hargreaves Lansdown.
    Scrapes index-linked gilt prices and calculates real yields.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.hl.co.uk/shares/corporate-bonds-gilts/bond-prices/uk-index-linked-gilts"
        # Override the chrome debug port
        self.chrome_options.add_argument("--remote-debugging-port=9223")
    
    def scrape_index_linked_gilt_prices(self) -> List[Dict[str, Any]]:
        """Scrape UK Index-Linked Gilts and calculate real yields."""
        driver = None
        bonds = []
        
        try:
            service = self._get_chrome_service()
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            
            self.logger.info(f"Loading index-linked gilts page: {self.base_url}")
            driver.get(self.base_url)
            
            # Wait for page to load
            time.sleep(7)
            
            # Look for table rows directly (same approach as working gilt collector)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
            
            if len(rows) == 0:
                self.logger.warning("No table rows found - page structure may have changed")
                return []
            
            self.logger.info(f"Found {len(rows)} table rows to process")
            settlement_date = datetime.now()
            
            # Get header to understand structure
            if len(rows) > 0:
                header_cells = rows[0].find_elements(By.TAG_NAME, "th")
                if not header_cells:
                    header_cells = rows[0].find_elements(By.TAG_NAME, "td")
                
                headers = [cell.text.strip().lower() for cell in header_cells]
                self.logger.info(f"Table structure: {headers}")
            
            for i, row in enumerate(rows[1:]):  # Skip header
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < 5:
                        self.logger.debug(f"Row {i}: Skipping row with only {len(cells)} cells (need 5+)")
                        continue
                    
                    # Extract bond data - index-linked gilt table structure
                    bond_name_cell = cells[0]
                    bond_name_text = bond_name_cell.text.strip()  # Full cell text (includes ISIN)
                    
                    # Get bond link information
                    bond_link_url = None
                    try:
                        bond_name_element = bond_name_cell.find_element(By.TAG_NAME, "a")
                        bond_name = bond_name_element.text.strip()  # Display name from link
                        bond_link_url = bond_name_element.get_attribute("href")
                    except Exception as e:
                        self.logger.debug(f"Row {i}: No bond name link found, skipping: {str(e)}")
                        continue
                    
                    # Extract bond identifiers (ISIN, currency, short code)
                    identifiers = self.extract_bond_identifiers(bond_name_text, bond_link_url)
                    
                    # Skip if not a Treasury bond
                    if 'Treasury' not in bond_name:
                        self.logger.debug(f"Row {i}: Skipping non-Treasury bond: '{bond_name}'")
                        continue
                    
                    # Extract clean price from Price column (column 3)
                    clean_price_text = cells[3].text.strip() if len(cells) > 3 else ""
                    clean_price_match = re.search(r'([0-9]+\.?[0-9]*)', clean_price_text)
                    if not clean_price_match:
                        self.logger.debug(f"Row {i}: No clean price found in '{clean_price_text}' for {bond_name}")
                        continue
                    
                    clean_price = float(clean_price_match.group(1))
                    
                    # Parse coupon rate from Coupon (%) column (column 1) and use as approximate real yield
                    # Index-linked gilts don't show real yield directly on this page
                    coupon_text = cells[1].text.strip() if len(cells) > 1 else ""
                    coupon_match = re.search(r'([0-9]+\.?[0-9]*)', coupon_text)
                    if not coupon_match:
                        self.logger.debug(f"Row {i}: No coupon rate found in '{coupon_text}' for {bond_name}")
                        continue
                    
                    coupon_rate = float(coupon_match.group(1)) / 100  # Convert percentage to decimal
                    
                    # Parse maturity date from Maturity column (column 2) - more reliable than bond name
                    maturity_text = cells[2].text.strip() if len(cells) > 2 else ""
                    maturity_date = None
                    
                    if maturity_text:
                        try:
                            # Try different common date formats from the Maturity column
                            date_formats = [
                                "%d %B %Y",      # "22 March 2026"
                                "%d %b %Y",      # "22 Mar 2026" 
                                "%B %d, %Y",     # "March 22, 2026"
                                "%b %d, %Y",     # "Mar 22, 2026"
                                "%d/%m/%Y",      # "22/03/2026"
                                "%m/%d/%Y",      # "03/22/2026" (US format)
                                "%Y-%m-%d",      # "2026-03-22" (ISO format)
                                "%d-%m-%Y",      # "22-03-2026"
                            ]
                            
                            for date_format in date_formats:
                                try:
                                    maturity_date = datetime.strptime(maturity_text, date_format)
                                    self.logger.debug(f"Row {i}: Parsed maturity date '{maturity_text}' as {maturity_date.strftime('%Y-%m-%d')} for '{bond_name}'")
                                    break
                                except ValueError:
                                    continue
                            
                            if maturity_date is None:
                                # If no date format worked, try to extract just the year
                                year_match = re.search(r'(\d{4})', maturity_text)
                                if year_match:
                                    maturity_year = int(year_match.group(1))
                                    maturity_date = datetime(maturity_year, 3, 22)  # Default to March 22
                                    self.logger.debug(f"Row {i}: Extracted year {maturity_year} from '{maturity_text}', using March 22 for '{bond_name}'")
                                else:
                                    self.logger.debug(f"Row {i}: Could not parse maturity date '{maturity_text}' for {bond_name}")
                                    continue
                                    
                        except Exception as e:
                            self.logger.debug(f"Row {i}: Error parsing maturity date '{maturity_text}' for {bond_name}: {str(e)}")
                            continue
                    else:
                        self.logger.debug(f"Row {i}: No maturity date found in Maturity column for {bond_name}")
                        continue
                    
                    # Calculate years to maturity
                    years_to_maturity = (maturity_date - settlement_date).days / 365.25
                    if years_to_maturity <= 0:
                        self.logger.debug(f"Row {i}: Bond already matured ({years_to_maturity:.2f} years) for {bond_name}")
                        continue
                    
                    # Coupon rate already parsed from Coupon (%) column above
                    
                    # Simplified accrued interest calculation (for now)
                    days_since_coupon = 90  # Estimate
                    accrued_interest = (coupon_rate / 2) * (days_since_coupon / 182.5)  # Semi-annual coupons
                    
                    dirty_price = clean_price + accrued_interest
                    
                    # Calculate proper YTM using bond pricing equation  
                    # Determine face value based on clean price
                    face_value = self._determine_face_value(clean_price)
                    try:
                        self.logger.debug(f"Row {i}: Calculating YTM for {bond_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        ytm = self.calculate_ytm_from_dirty(dirty_price, face_value, coupon_rate, years_to_maturity)
                        
                        if ytm is None:
                            raise ValueError(f"YTM calculation failed for {bond_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        self.logger.debug(f"Row {i}: YTM calculated successfully for {bond_name}: {ytm*100:.3f}%")
                    
                    except Exception as e:
                        self.logger.error(f"Row {i}: YTM calculation error for {bond_name}: {str(e)}")
                        raise
                    
                    # No tax calculation as requested - use YTM as real yield for index-linked gilts
                    real_yield = ytm
                    after_tax_real_yield = ytm  # Same as pre-tax YTM (no tax factors)
                    
                    # Ensure unique bond names
                    unique_bond_name = bond_name
                    existing_names = [b['bond_name'] for b in bonds]
                    counter = 1
                    while unique_bond_name in existing_names:
                        counter += 1
                        unique_bond_name = f"{bond_name} #{counter}"
                    
                    bonds.append({
                        'bond_name': unique_bond_name,
                        'clean_price': clean_price,
                        'accrued_interest': accrued_interest,
                        'dirty_price': dirty_price,
                        'coupon_rate': coupon_rate,
                        'maturity_date': maturity_date,
                        'years_to_maturity': years_to_maturity,
                        'real_yield': real_yield,  # Real yield instead of nominal YTM
                        'after_tax_real_yield': after_tax_real_yield,
                        'scraped_date': settlement_date.date(),
                        'inflation_assumption': 3.0,  # HL typically assumes 3% inflation
                        # Bond identifiers
                        'currency_code': identifiers['currency_code'],
                        'isin': identifiers['isin'],
                        'short_code': identifiers['short_code'],
                        'combined_id': identifiers['combined_id']
                    })
                    
                except Exception as e:
                    self.logger.debug(f"Row {i}: Exception processing index-linked bond: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully scraped {len(bonds)} index-linked gilt prices")
            rows_processed = len(rows) - 1  # Exclude header
            if rows_processed > len(bonds):
                self.logger.info(f"Note: Found {rows_processed} data rows but only scraped {len(bonds)} bonds - {rows_processed - len(bonds)} rows were filtered out")
            return bonds
        
        except Exception as e:
            self.logger.error(f"Scraping error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


def collect_index_linked_gilt_prices(database_url=None):
    """Collect real-time index-linked gilt prices from Hargreaves Lansdown."""
    collector = IndexLinkedGiltCollector(database_url)
    
    try:
        # Scrape index-linked gilt prices from broker website
        il_gilt_data = collector.scrape_index_linked_gilt_prices()
        
        if not il_gilt_data:
            collector.logger.info("No index-linked gilt data found")
            return 0
        
        # Process and store data
        bulk_data = []
        for gilt in il_gilt_data:
            try:
                # Convert to database format
                data = {
                    'bond_name': str(gilt['bond_name']) if gilt['bond_name'] is not None else None,
                    'clean_price': float(gilt['clean_price']) if gilt['clean_price'] is not None else None,
                    'accrued_interest': float(gilt['accrued_interest']) if gilt['accrued_interest'] is not None else None,
                    'dirty_price': float(gilt['dirty_price']) if gilt['dirty_price'] is not None else None,
                    'coupon_rate': float(gilt['coupon_rate']) if gilt['coupon_rate'] is not None else None,
                    'maturity_date': gilt['maturity_date'].date(),
                    'years_to_maturity': float(gilt['years_to_maturity']) if gilt['years_to_maturity'] is not None else None,
                    'real_yield': float(gilt['real_yield']) if gilt['real_yield'] is not None else None,
                    'after_tax_real_yield': float(gilt['after_tax_real_yield']) if gilt['after_tax_real_yield'] is not None else None,
                    'scraped_date': gilt['scraped_date'],
                    'inflation_assumption': float(gilt['inflation_assumption']) if gilt['inflation_assumption'] is not None else None,
                    # Bond identifiers
                    'currency_code': str(gilt['currency_code']) if gilt['currency_code'] is not None else None,
                    'isin': str(gilt['isin']) if gilt['isin'] is not None else None,
                    'short_code': str(gilt['short_code']) if gilt['short_code'] is not None else None,
                    'combined_id': str(gilt['combined_id']) if gilt['combined_id'] is not None else None
                }
                bulk_data.append(data)
                
            except Exception as e:
                collector.logger.error(f"Error processing index-linked gilt data: {str(e)}")
                continue
        
        # Bulk upsert all records
        if bulk_data:
            success_count = collector.bulk_upsert_data(
                "index_linked_gilt_prices", 
                bulk_data,
                conflict_columns=['bond_name', 'scraped_date']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(bulk_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} index-linked gilt price records")
            return success_count
        else:
            collector.logger.info("No valid index-linked gilt data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect index-linked gilt prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task


class CorporateBondCollector(GiltMarketCollector):
    """
    Collector for real-time corporate bond prices from Hargreaves Lansdown.
    Scrapes GBP corporate bond prices and calculates yields with credit risk analysis.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.hl.co.uk/shares/corporate-bonds-gilts/bond-prices/gbp-bonds"
        # Override the chrome options to use different debug port
        self.chrome_options.add_argument("--remote-debugging-port=9224")
    
    def _is_bond_tradeable(self, row_element) -> bool:
        """
        Check if a corporate bond can be traded online.
        
        Filters out bonds that have "Online dealing is not available" indicators
        in their action elements.
        
        Args:
            row_element: Selenium WebElement representing the table row
            
        Returns:
            bool: True if bond can be traded, False otherwise
        """
        try:
            cells = row_element.find_elements(By.TAG_NAME, "td")
            
            if len(cells) < 5:  # Need at least 5 columns including Actions
                return False
            
            # Check Actions column (column 4) for disabled indicators
            actions_cell = cells[4]
            action_elements = actions_cell.find_elements(By.TAG_NAME, "a") + actions_cell.find_elements(By.TAG_NAME, "button")
            
            for element in action_elements:
                element_title = element.get_attribute("title") or ""
                if "not available" in element_title.lower():
                    self.logger.debug(f"Bond not tradeable: {element_title}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"Error checking bond tradeability: {str(e)}")
            return False  # Safe default - skip if we can't determine
    
    def scrape_corporate_bond_prices(self) -> List[Dict[str, Any]]:
        """Scrape GBP Corporate Bonds and calculate yields with credit analysis."""
        driver = None
        bonds = []
        
        try:
            service = self._get_chrome_service()
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            
            self.logger.info(f"Loading corporate bonds page: {self.base_url}")
            driver.get(self.base_url)
            
            # Wait for page to load
            time.sleep(7)
            
            # Look for table rows directly (same approach as working gilt collector)
            rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
            
            if len(rows) == 0:
                self.logger.warning("No table rows found - page structure may have changed")
                return []
            
            self.logger.info(f"Found {len(rows)} table rows to process")
            settlement_date = datetime.now()
            
            # Tracking variables for filtering
            total_rows = len(rows) - 1  # Exclude header
            non_tradeable_count = 0
            na_maturity_count = 0
            processed_count = 0
            
            # Get header to understand structure
            if len(rows) > 0:
                header_cells = rows[0].find_elements(By.TAG_NAME, "th")
                if not header_cells:
                    header_cells = rows[0].find_elements(By.TAG_NAME, "td")
                
                headers = [cell.text.strip().lower() for cell in header_cells]
                self.logger.info(f"Table structure: {headers}")
            
            for i, row in enumerate(rows[1:]):  # Skip header
                try:
                    # Check if bond is tradeable first (filter out non-tradeable bonds)
                    if not self._is_bond_tradeable(row):
                        non_tradeable_count += 1
                        self.logger.debug(f"Row {i+1}: Skipping non-tradeable bond")
                        continue
                    
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < 5:
                        continue
                    
                    # Check for n/a maturity early (before parsing)
                    maturity_text = cells[2].text.strip() if len(cells) > 2 else ""
                    if maturity_text.lower() in ['n/a', 'na', '', '-']:
                        na_maturity_count += 1
                        self.logger.debug(f"Row {i+1}: Skipping bond with n/a maturity: {maturity_text}")
                        continue
                    
                    # Extract bond data - corporate bond table structure
                    bond_name_cell = cells[0]
                    bond_name_text = bond_name_cell.text.strip()  # Full cell text (includes ISIN)
                    
                    # Get bond link information
                    bond_link_url = None
                    try:
                        bond_name_element = bond_name_cell.find_element(By.TAG_NAME, "a")
                        bond_name = bond_name_element.text.strip()  # Display name from link
                        bond_link_url = bond_name_element.get_attribute("href")
                    except:
                        bond_name = bond_name_text  # Fallback to full text
                    
                    # Skip if empty or invalid name
                    if not bond_name:
                        continue
                    
                    # Extract bond identifiers (ISIN, currency, short code)
                    identifiers = self.extract_bond_identifiers(bond_name_text, bond_link_url)
                    
                    # Extract company name from bond name (usually first part before newline)
                    company_name = bond_name.split('\n')[0] if bond_name else "Unknown"
                    
                    # Extract clean price from Price column (column 3)
                    clean_price_text = cells[3].text.strip() if len(cells) > 3 else ""
                    clean_price_match = re.search(r'([0-9]+\.?[0-9]*)', clean_price_text)
                    if not clean_price_match:
                        continue
                    
                    clean_price = float(clean_price_match.group(1))
                    
                    # Skip bonds with unrealistic prices (bad source data)
                    if clean_price <= 0.1:  # Skip if price is 10p or less
                        self.logger.warning(f"Row {i}: Skipping {company_name} - unrealistic price: £{clean_price:.3f}")
                        continue
                    
                    # Parse coupon rate from Coupon (%) column (column 1) 
                    # Corporate bonds don't typically show YTM directly, use coupon as approximation
                    coupon_text = cells[1].text.strip() if len(cells) > 1 else ""
                    coupon_match = re.search(r'([0-9]+\.?[0-9]*)', coupon_text)
                    if not coupon_match:
                        continue
                    
                    coupon_rate = float(coupon_match.group(1)) / 100  # Convert percentage to decimal
                    
                    # YTM will be calculated after we get the maturity date and accrued interest
                    
                    # Parse maturity date from Maturity column (column 2)
                    maturity_text = cells[2].text.strip() if len(cells) > 2 else ""
                    maturity_date_match = re.search(r'(\d{4})', maturity_text)
                    if maturity_date_match:
                        maturity_year = int(maturity_date_match.group(1))
                        # Try to parse the full date if possible (e.g., "3 December 2032")
                        try:
                            # Common formats: "3 December 2032", "30 September 2026"
                            maturity_date = datetime.strptime(maturity_text, "%d %B %Y")
                        except ValueError:
                            # Fallback to mid-year estimate
                            maturity_date = datetime(maturity_year, 6, 15)
                    else:
                        # Fallback: try to extract from bond name
                        maturity_match = re.search(r'(20\d{2})', bond_name)
                        if maturity_match:
                            maturity_year = int(maturity_match.group(1))
                            maturity_date = datetime(maturity_year, 6, 15)
                        else:
                            continue
                    
                    # Calculate years to maturity
                    years_to_maturity = (maturity_date - settlement_date).days / 365.25
                    if years_to_maturity <= 0:
                        continue
                    
                    # Coupon rate already parsed from Coupon (%) column above
                    
                    # Calculate accrued interest (simplified)
                    days_since_coupon = 90  # Estimate
                    accrued_interest = (coupon_rate / 2) * (days_since_coupon / 182.5)  # Semi-annual coupons
                    
                    dirty_price = clean_price + accrued_interest
                    
                    # Calculate proper YTM using bond pricing equation
                    # Determine face value based on clean price
                    face_value = self._determine_face_value(clean_price)
                    try:
                        self.logger.debug(f"Row {i}: Calculating YTM for {company_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        ytm = self.calculate_ytm_from_dirty(dirty_price, face_value, coupon_rate, years_to_maturity)
                        
                        if ytm is None:
                            raise ValueError(f"YTM calculation failed for {company_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        self.logger.debug(f"Row {i}: YTM calculated successfully for {company_name}: {ytm*100:.3f}%")
                    
                    except Exception as e:
                        self.logger.error(f"Row {i}: YTM calculation error for {company_name}: {str(e)}")
                        raise
                    
                    # No tax calculation as requested
                    after_tax_ytm = ytm  # Same as pre-tax YTM
                    
                    # Try to extract credit rating if available (often in additional columns)
                    credit_rating = "NR"  # Not Rated default
                    if len(cells) > 5:
                        rating_text = cells[5].text.strip()
                        if rating_text and len(rating_text) <= 5:  # Typical rating format
                            credit_rating = rating_text
                    
                    # Ensure unique bond names
                    unique_bond_name = bond_name
                    existing_names = [b['bond_name'] for b in bonds]
                    counter = 1
                    while unique_bond_name in existing_names:
                        counter += 1
                        unique_bond_name = f"{bond_name} #{counter}"
                    
                    bonds.append({
                        'bond_name': unique_bond_name,
                        'company_name': company_name,
                        'clean_price': clean_price,
                        'accrued_interest': accrued_interest,
                        'dirty_price': dirty_price,
                        'coupon_rate': coupon_rate,
                        'maturity_date': maturity_date,
                        'years_to_maturity': years_to_maturity,
                        'ytm': ytm,
                        'after_tax_ytm': after_tax_ytm,
                        'credit_rating': credit_rating,
                        'scraped_date': settlement_date.date(),
                        # Bond identifiers
                        'currency_code': identifiers['currency_code'],
                        'isin': identifiers['isin'],
                        'short_code': identifiers['short_code'],
                        'combined_id': identifiers['combined_id']
                    })
                    processed_count += 1
                    
                except Exception as e:
                    self.logger.debug(f"Error processing corporate bond row {i}: {str(e)}")
                    continue
            
            # Log filtering summary
            self.logger.info(f"Corporate bond filtering summary:")
            self.logger.info(f"  Total rows processed: {total_rows}")
            self.logger.info(f"  Non-tradeable bonds filtered: {non_tradeable_count}")
            self.logger.info(f"  N/A maturity bonds filtered: {na_maturity_count}")
            self.logger.info(f"  Successfully processed bonds: {processed_count}")
            self.logger.info(f"  Final bond count: {len(bonds)}")
            
            self.logger.info(f"Successfully scraped {len(bonds)} corporate bond prices")
            return bonds
        
        except Exception as e:
            self.logger.error(f"Scraping error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass


def collect_corporate_bond_prices(database_url=None):
    """Collect real-time corporate bond prices from Hargreaves Lansdown."""
    collector = CorporateBondCollector(database_url)
    
    try:
        # Scrape corporate bond prices from broker website
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        if not corporate_bond_data:
            collector.logger.info("No corporate bond data found")
            return 0
        
        # Process and store data
        bulk_data = []
        for bond in corporate_bond_data:
            try:
                # Convert to database format
                data = {
                    'bond_name': str(bond['bond_name']) if bond['bond_name'] is not None else None,
                    'company_name': str(bond['company_name']) if bond['company_name'] is not None else None,
                    'clean_price': float(bond['clean_price']) if bond['clean_price'] is not None else None,
                    'accrued_interest': float(bond['accrued_interest']) if bond['accrued_interest'] is not None else None,
                    'dirty_price': float(bond['dirty_price']) if bond['dirty_price'] is not None else None,
                    'coupon_rate': float(bond['coupon_rate']) if bond['coupon_rate'] is not None else None,
                    'maturity_date': bond['maturity_date'].date(),
                    'years_to_maturity': float(bond['years_to_maturity']) if bond['years_to_maturity'] is not None else None,
                    'ytm': float(bond['ytm']) if bond['ytm'] is not None else None,
                    'after_tax_ytm': float(bond['after_tax_ytm']) if bond['after_tax_ytm'] is not None else None,
                    'credit_rating': str(bond['credit_rating']) if bond['credit_rating'] is not None else None,
                    'scraped_date': bond['scraped_date'],
                    # Bond identifiers
                    'currency_code': str(bond['currency_code']) if bond['currency_code'] is not None else None,
                    'isin': str(bond['isin']) if bond['isin'] is not None else None,
                    'short_code': str(bond['short_code']) if bond['short_code'] is not None else None,
                    'combined_id': str(bond['combined_id']) if bond['combined_id'] is not None else None
                }
                bulk_data.append(data)
                
            except Exception as e:
                collector.logger.error(f"Error processing corporate bond data: {str(e)}")
                continue
        
        # Bulk upsert all records
        if bulk_data:
            success_count = collector.bulk_upsert_data(
                "corporate_bond_prices", 
                bulk_data,
                conflict_columns=['bond_name', 'scraped_date']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(bulk_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} corporate bond price records")
            return success_count
        else:
            collector.logger.info("No valid corporate bond data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect corporate bond prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task


class AJBellCorporateBondCollector(BaseCollector):
    """
    Collector for real-time corporate bond prices from AJ Bell broker.
    Scrapes GBP corporate bond prices and calculates yields with comprehensive analysis.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.ajbell.co.uk/investment/bonds/corporate/prices"
        self.chrome_options = self._setup_chrome_options()
    
    def _setup_chrome_options(self):
        """Configure Chrome options for Pi-friendly headless scraping."""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox") 
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
        options.add_argument("--remote-debugging-port=9226")  # Different port from other scrapers
        return options
    
    def _get_chrome_service(self):
        """Get Chrome service for both Pi and development environments."""
        arch = platform.machine().lower()
        system = platform.system()
        
        # Try Pi-specific paths first (ARM64 Linux)
        if arch in ['aarch64', 'arm64'] and system == 'Linux':
            # Check shared volume locations (init container installation)
            shared_paths = [
                '/shared/usr/bin/chromedriver',
                '/shared/usr/lib/chromium-browser/chromedriver',
                '/shared/snap/chromium/current/usr/lib/chromium-browser/chromedriver'
            ]
            
            for path in shared_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using Pi ChromeDriver from init container: {path}")
                    
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
                            os.environ['LD_LIBRARY_PATH'] = f"{new_ld_path}:{current_ld_path}"
                        else:
                            os.environ['LD_LIBRARY_PATH'] = new_ld_path
                        self.logger.info(f"Set LD_LIBRARY_PATH: {os.environ['LD_LIBRARY_PATH']}")
                    
                    # Also set chromium binary location if available
                    if os.path.exists('/shared/usr/bin/chromium'):
                        self.chrome_options.binary_location = '/shared/usr/bin/chromium'
                        self.logger.info("Using Chromium binary from shared volume")
                    return Service(path)
            
            # Fallback to system paths
            system_paths = [
                '/usr/bin/chromedriver',
                '/usr/lib/chromium-browser/chromedriver',
                '/snap/chromium/current/usr/lib/chromium-browser/chromedriver'
            ]
            
            for path in system_paths:
                if os.path.exists(path):
                    self.logger.info(f"Using Pi ChromeDriver from system: {path}")
                    return Service(path)
        
        # Fallback to webdriver-manager for development environments
        if ChromeDriverManager is not None:
            try:
                self.logger.info("Using ChromeDriver from webdriver-manager (development environment)")
                return Service(ChromeDriverManager().install())
            except Exception as e:
                self.logger.warning(f"webdriver-manager failed: {e}")
        
        # Final fallback - check common system paths
        common_paths = [
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            '/opt/homebrew/bin/chromedriver',  # macOS with Homebrew
        ]
        
        for path in common_paths:
            if os.path.exists(path):
                self.logger.info(f"Using system ChromeDriver at: {path}")
                return Service(path)
        
        # No ChromeDriver found
        if arch in ['aarch64', 'arm64'] and system == 'Linux':
            raise RuntimeError("ChromeDriver not found - install with: apt install chromium-chromedriver")
        else:
            raise RuntimeError("ChromeDriver not found - install via webdriver-manager or system package manager")
    
    def _handle_cookie_consent(self, driver):
        """Handle cookie consent banner if present."""
        try:
            # Common cookie consent button selectors
            cookie_selectors = [
                "button[id*='accept']",
                "button[class*='accept']",
                "button[data-testid*='accept']",
                ".cookie-accept",
                ".accept-cookies",
                "#accept-cookies",
                "[aria-label*='accept']",
                "[title*='accept']"
            ]
            
            wait = WebDriverWait(driver, 5)
            
            for selector in cookie_selectors:
                try:
                    cookie_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    self.logger.info(f"Found cookie consent button with selector: {selector}")
                    cookie_button.click()
                    self.logger.info("Cookie consent accepted")
                    time.sleep(1)  # Wait for banner to disappear
                    return
                except:
                    continue
            
            # Try to find cookie buttons by text content
            try:
                buttons = driver.find_elements(By.TAG_NAME, "button")
                for button in buttons:
                    button_text = button.text.lower()
                    if any(keyword in button_text for keyword in ['accept', 'continue', 'agree', 'ok']):
                        self.logger.info(f"Found cookie button by text: '{button.text}'")
                        button.click()
                        self.logger.info("Cookie consent accepted by text search")
                        time.sleep(1)
                        return
            except:
                pass
            
            self.logger.debug("No cookie consent banner found or already accepted")
            
        except Exception as e:
            self.logger.warning(f"Error handling cookie consent: {e}")
    
    def _wait_for_content(self, driver):
        """Wait for dynamic content to load."""
        try:
            wait = WebDriverWait(driver, 10)
            
            # Wait for corporate bond price table specifically
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            self.logger.info("Corporate bond price table loaded successfully")
            
        except Exception as e:
            self.logger.warning(f"Table loading timeout: {e}")
    
    def _set_display_to_100(self, driver):
        """Set the display dropdown to show 100 entries."""
        try:
            # Look for display dropdown - common selectors
            dropdown_selectors = [
                "select[name*='display']",
                "select[id*='display']",
                "select[class*='display']",
                "select[aria-label*='display']",
                "select[aria-label*='entries']",
                "select[name*='entries']",
                "select[id*='entries']",
                "select",  # Fallback to any select element
            ]
            
            wait = WebDriverWait(driver, 5)
            
            for selector in dropdown_selectors:
                try:
                    dropdown_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    
                    # Check if this dropdown has a "100" option
                    select = Select(dropdown_element)
                    options = [option.text.strip() for option in select.options]
                    self.logger.info(f"Found dropdown with options: {options}")
                    
                    # Look for "100" option
                    if "100" in options:
                        self.logger.info(f"Setting display to 100 entries using selector: {selector}")
                        select.select_by_visible_text("100")
                        time.sleep(2)  # Wait for page to reload with more entries
                        return True
                    
                except Exception:
                    continue
            
            # If no dropdown found, try looking for any element with "100" text that might be clickable
            try:
                hundred_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '100')]")
                for element in hundred_elements:
                    if element.is_displayed() and element.is_enabled():
                        self.logger.info(f"Found clickable element with '100': {element.tag_name}")
                        element.click()
                        time.sleep(2)
                        return True
            except Exception as e:
                self.logger.debug(f"Error trying clickable 100 elements: {e}")
            
            self.logger.info("No display dropdown found or no 100 option available")
            return False
            
        except Exception as e:
            self.logger.error(f"Error setting display to 100: {e}")
            return False
    
    def parse_price(self, price_text: str) -> Optional[float]:
        """Parse price from text, handling various formats."""
        if not price_text:
            return None
        
        # Remove currency symbols and common formatting
        clean_text = re.sub(r'[£$€,]', '', price_text.strip())
        
        # Extract numeric value
        price_match = re.search(r'(\d+\.?\d*)', clean_text)
        if price_match:
            try:
                return float(price_match.group(1))
            except ValueError:
                return None
        return None
    
    def parse_percentage(self, percent_text: str) -> Optional[float]:
        """Parse percentage from text."""
        if not percent_text:
            return None
        
        # Remove % symbol and extract number
        clean_text = percent_text.replace('%', '').strip()
        percent_match = re.search(r'(\d+\.?\d*)', clean_text)
        if percent_match:
            try:
                return float(percent_match.group(1)) / 100  # Convert to decimal
            except ValueError:
                return None
        return None
    
    def parse_maturity_date(self, date_text: str) -> Optional[datetime]:
        """Parse maturity date from various formats."""
        if not date_text:
            return None
        
        date_formats = [
            '%d %B %Y',      # 4 December 2025
            '%d %b %Y',      # 4 Dec 2025
            '%d/%m/%Y',      # 04/12/2025
            '%d-%m-%Y',      # 04-12-2025
            '%Y-%m-%d',      # 2025-12-04
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_text.strip(), fmt)
            except ValueError:
                continue
        
        # Try to extract just year and estimate mid-year
        year_match = re.search(r'20\d{2}', date_text)
        if year_match:
            year = int(year_match.group(0))
            return datetime(year, 7, 15)  # Mid-year estimate
        
        return None
    
    def calculate_accrued_interest(self, face_value: float, coupon_rate: float, 
                                 last_coupon_date: datetime, settlement_date: datetime, 
                                 payments_per_year: int = 2) -> float:
        """Calculate accrued interest since last coupon payment."""
        annual_coupon = coupon_rate * face_value
        coupon_payment = annual_coupon / payments_per_year
        
        # Days since last coupon payment
        days_since_coupon = (settlement_date - last_coupon_date).days
        
        # Days in coupon period (assume 6 months = 182.5 days for semi-annual)
        days_in_period = 365.25 / payments_per_year
        
        # Accrued interest = (Days since coupon / Days in period) * Coupon payment
        accrued = (days_since_coupon / days_in_period) * coupon_payment
        
        return accrued
    
    def estimate_coupon_dates(self, bond_name: str, maturity_date: datetime, 
                            settlement_date: datetime) -> datetime:
        """Estimate coupon payment dates based on maturity date."""
        # Extract maturity month/day from bond name or maturity date
        if maturity_date:
            mat_month = maturity_date.month
            mat_day = maturity_date.day
        else:
            # Default assumption for corporate bonds
            mat_month = 7
            mat_day = 31
        
        year = settlement_date.year
        
        # Calculate the two coupon dates per year (6 months apart)
        if mat_month <= 6:
            coupon1 = datetime(year, mat_month, mat_day)
            coupon2 = datetime(year, mat_month + 6, mat_day)
        else:
            coupon1 = datetime(year, mat_month - 6, mat_day)
            coupon2 = datetime(year, mat_month, mat_day)
        
        # Handle edge cases for day of month
        try:
            coupon1 = datetime(year, coupon1.month, min(coupon1.day, 28))
            coupon2 = datetime(year, coupon2.month, min(coupon2.day, 28))
        except:
            coupon1 = datetime(year, coupon1.month, 28)
            coupon2 = datetime(year, coupon2.month, 28)
        
        # Find the most recent coupon date before settlement
        if settlement_date >= coupon2:
            return coupon2
        elif settlement_date >= coupon1:
            return coupon1
        else:
            # Must be from previous year
            try:
                if mat_month <= 6:
                    return datetime(year - 1, mat_month + 6, min(mat_day, 28))
                else:
                    return datetime(year - 1, mat_month, min(mat_day, 28))
            except:
                return datetime(year - 1, 7, 31)  # Default fallback
    
    def _determine_face_value(self, clean_price: float) -> float:
        """
        Determine face value based on clean price.
        AJ Bell typically quotes bonds with £100 face value.
        """
        if clean_price < 2.0:
            self.logger.debug(f"Using £1 face value for low price: {clean_price}")
            return 1.0  # £1 face value for low-priced bonds
        else:
            return 100.0  # £100 face value for standard bonds
    
    def calculate_ytm_from_dirty(self, dirty_price: float, face_value: float, 
                               coupon_rate: float, years_to_maturity: float, 
                               payments_per_year: int = 2) -> Optional[float]:
        """Calculate YTM using dirty price."""
        if fsolve is None:
            self.logger.warning("scipy not available - cannot calculate YTM")
            return None
            
        coupon_payment = (coupon_rate * face_value) / payments_per_year
        total_periods = years_to_maturity * payments_per_year  # Keep as float, don't round to int
        
        # Handle edge case where bond has essentially no time remaining
        if total_periods <= 0.01:  # Less than ~3.6 days remaining
            self.logger.warning(f"Bond has minimal time remaining (years_to_maturity: {years_to_maturity})")
            return None
        
        def bond_price_equation(ytm):
            if ytm == 0:
                return coupon_payment * total_periods + face_value - dirty_price
            
            periodic_rate = ytm / payments_per_year
            
            # Avoid numerical issues with very negative rates
            if periodic_rate <= -0.99:
                return float('inf')
            
            try:
                present_value_coupons = coupon_payment * (1 - (1 + periodic_rate) ** -total_periods) / periodic_rate
                present_value_face = face_value / (1 + periodic_rate) ** total_periods
                return present_value_coupons + present_value_face - dirty_price
            except (OverflowError, ZeroDivisionError):
                return float('inf')
        
        # Use approximate YTM as initial guess
        initial_guess = (coupon_rate + (face_value - dirty_price) / years_to_maturity) / ((face_value + dirty_price) / 2)
        
        try:
            ytm_solution = fsolve(bond_price_equation, initial_guess, xtol=1e-8)[0]
            
            # Sanity check: YTM should be reasonable (between -50% and 100%)
            if -0.5 <= ytm_solution <= 1.0:
                return ytm_solution
            else:
                self.logger.warning(f"YTM calculation returned unreasonable value: {ytm_solution*100:.2f}%")
                return None
                
        except Exception as e:
            self.logger.warning(f"YTM calculation failed: {str(e)}")
            return None
    
    def calculate_after_tax_ytm(self, dirty_price: float, face_value: float, 
                              coupon_rate: float, years_to_maturity: float, 
                              tax_rate_on_coupons: float = 0.30, 
                              payments_per_year: int = 2) -> Optional[float]:
        """
        Calculate after-tax YTM where:
        - Coupon payments are taxed at tax_rate_on_coupons
        - Capital gains are tax-free (typical for UK corporate bonds)
        """
        if fsolve is None:
            self.logger.warning("scipy not available - cannot calculate after-tax YTM")
            return None
            
        coupon_payment = (coupon_rate * face_value) / payments_per_year
        after_tax_coupon = coupon_payment * (1 - tax_rate_on_coupons)
        total_periods = int(years_to_maturity * payments_per_year)
        
        def after_tax_bond_equation(ytm):
            if ytm == 0:
                return after_tax_coupon * total_periods + face_value - dirty_price
            
            periodic_rate = ytm / payments_per_year
            # Present value of after-tax coupons
            present_value_coupons = after_tax_coupon * (1 - (1 + periodic_rate) ** -total_periods) / periodic_rate
            # Present value of face value (no tax on capital gains)
            present_value_face = face_value / (1 + periodic_rate) ** total_periods
            
            return present_value_coupons + present_value_face - dirty_price
        
        initial_guess = coupon_rate if coupon_rate > 0 else 0.05
        
        try:
            ytm_solution = fsolve(after_tax_bond_equation, initial_guess)[0]
            return ytm_solution
        except:
            return None
    
    def scrape_corporate_bond_prices(self) -> List[Dict[str, Any]]:
        """Scrape AJ Bell corporate bond prices."""
        arch = platform.machine().lower()
        system = platform.system()
        
        try:
            return self._scrape_with_selenium()
        except Exception as e:
            # On ARM64 Linux (Pi), fail hard - don't fall back gracefully
            if arch in ['aarch64', 'arm64'] and system == 'Linux':
                self.logger.error(f"CRITICAL: Selenium scraping failed on ARM64 Linux: {str(e)}")
                self.logger.error("ChromeDriver/Chromium should be installed by init container")
                raise RuntimeError(f"ARM64 AJ Bell corporate bond scraping failed: {e}") from e
            else:
                # On development environments, fall back gracefully
                self.logger.error(f"Selenium scraping failed: {str(e)}")
                self.logger.info("Falling back to empty dataset - browser automation not available")
                return []
    
    def _scrape_with_selenium(self) -> List[Dict[str, Any]]:
        """Scrape using Selenium WebDriver."""
        driver = None
        try:
            service = self._get_chrome_service()
            driver = webdriver.Chrome(service=service, options=self.chrome_options)
            
            self.logger.info(f"Loading AJ Bell corporate bond prices page: {self.base_url}")
            driver.get(self.base_url)
            
            # Wait for initial page load
            time.sleep(2)
            
            # Handle cookie consent if present
            self._handle_cookie_consent(driver)
            
            # Wait for content to load after cookie acceptance
            self._wait_for_content(driver)
            
            # Try to set display to 100 entries to get all corporate bonds
            self.logger.info("Attempting to set display to 100 entries...")
            if self._set_display_to_100(driver):
                self.logger.info("Successfully set display to 100 entries")
                # Wait for page to reload with more entries
                self._wait_for_content(driver)
            else:
                self.logger.info("Could not set display to 100, proceeding with default display")
            
            # Find the corporate bond price table
            table = driver.find_element(By.CSS_SELECTOR, "table")
            
            return self._parse_corporate_bond_table(table)
            
        except Exception as e:
            self.logger.error(f"AJ Bell corporate bond scraping error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _parse_corporate_bond_table(self, table_element) -> List[Dict[str, Any]]:
        """Parse the AJ Bell corporate bond price table."""
        try:
            rows = table_element.find_elements(By.TAG_NAME, "tr")
            if len(rows) < 2:  # Need header + at least one data row
                return []
            
            # Get headers
            header_row = rows[0]
            headers = [cell.text.strip().lower() for cell in header_row.find_elements(By.TAG_NAME, "th")]
            
            if not headers:
                headers = [cell.text.strip().lower() for cell in header_row.find_elements(By.TAG_NAME, "td")]
            
            self.logger.info(f"AJ Bell corporate bond table headers: {headers}")
            self.logger.info(f"Found {len(rows)} total rows (including header)")
            
            # Find column indices for AJ Bell structure
            name_col = self._find_column_index(headers, ['issuer', 'name', 'bond', 'security', 'company'])
            price_col = self._find_column_index(headers, ['price', 'clean price', 'mid'])
            coupon_col = self._find_column_index(headers, ['coupon', 'rate', '%'])
            maturity_col = self._find_column_index(headers, ['maturity', 'expiry', 'date'])
            
            corporate_bond_data = []
            for i, row in enumerate(rows[1:]):  # Skip header
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < 4:  # Need minimum columns
                        continue
                    
                    # Extract data with error handling
                    bond_name_full = cells[name_col].text.strip() if name_col is not None and name_col < len(cells) else ""
                    price_text = cells[price_col].text.strip() if price_col is not None and price_col < len(cells) else ""
                    coupon_text = cells[coupon_col].text.strip() if coupon_col is not None and coupon_col < len(cells) else ""
                    maturity_text = cells[maturity_col].text.strip() if maturity_col is not None and maturity_col < len(cells) else ""
                    
                    # Extract clean bond name (before ISIN codes)
                    if '\n' in bond_name_full:
                        bond_name = bond_name_full.split('\n')[0].strip()
                    else:
                        bond_name = bond_name_full
                    
                    # Skip if essential data is missing
                    if not bond_name or not price_text:
                        continue
                    
                    # Parse data
                    clean_price = self.parse_price(price_text)
                    coupon_rate = self.parse_percentage(coupon_text)
                    maturity_date = self.parse_maturity_date(maturity_text)
                    
                    if clean_price is None or coupon_rate is None or maturity_date is None:
                        continue
                    
                    # Calculate years to maturity
                    settlement_date = datetime.now()
                    years_to_maturity = (maturity_date - settlement_date).days / 365.25
                    if years_to_maturity <= 0:
                        continue
                    
                    # Extract company name from bond name
                    company_name = None
                    if bond_name:
                        # Company name is typically the first part before the coupon
                        company_parts = bond_name.split()
                        if len(company_parts) >= 2:
                            # Take first 2-3 words as company name, excluding "PLC", "LIMITED", etc.
                            company_words = []
                            for word in company_parts:
                                if word.upper() in ['PLC', 'LIMITED', 'LTD', 'GROUP', 'BANK', 'FINANCE']:
                                    company_words.append(word)
                                    break
                                elif '%' in word:  # Stop at coupon rate
                                    break
                                else:
                                    company_words.append(word)
                            company_name = ' '.join(company_words)
                    
                    # Determine face value for calculations
                    face_value = self._determine_face_value(clean_price)
                    
                    # Calculate accrued interest and dirty price
                    accrued_interest = None
                    dirty_price = None
                    ytm = None
                    after_tax_ytm = None
                    
                    try:
                        # Estimate last coupon date
                        last_coupon_date = self.estimate_coupon_dates(bond_name, maturity_date, settlement_date)
                        
                        # Calculate accrued interest
                        accrued_interest = self.calculate_accrued_interest(
                            face_value=face_value,
                            coupon_rate=coupon_rate,
                            last_coupon_date=last_coupon_date,
                            settlement_date=settlement_date
                        )
                        
                        # Calculate dirty price
                        dirty_price = clean_price + accrued_interest
                        
                        # Calculate YTM from dirty price
                        ytm = self.calculate_ytm_from_dirty(
                            dirty_price=dirty_price,
                            face_value=face_value,
                            coupon_rate=coupon_rate,
                            years_to_maturity=years_to_maturity
                        )
                        
                        # Calculate after-tax YTM
                        after_tax_ytm = self.calculate_after_tax_ytm(
                            dirty_price=dirty_price,
                            face_value=face_value,
                            coupon_rate=coupon_rate,
                            years_to_maturity=years_to_maturity,
                            tax_rate_on_coupons=0.30  # 30% tax on coupons
                        )
                        
                    except Exception as calc_error:
                        self.logger.warning(f"Error calculating derived values for {bond_name}: {calc_error}")
                        # Keep None values for failed calculations
                    
                    # Extract ISIN and short codes
                    isin = None
                    short_code = None
                    
                    # Look for ISIN pattern (2 letters + 10 alphanumeric)
                    isin_match = re.search(r'[A-Z]{2}[A-Z0-9]{10}', bond_name_full)
                    if isin_match:
                        isin = isin_match.group(0)
                    
                    # Extract short code (after second |, which is the actual code)
                    parts = bond_name_full.split('|')
                    if len(parts) >= 3:
                        short_code = parts[2].strip()  # Third part is the actual short code
                    elif len(parts) >= 2:
                        # Fallback: if only 2 parts, check if second part is ISIN or short code
                        second_part = parts[1].strip()
                        if re.match(r'^GB[A-Z0-9]{10}$', second_part):
                            short_code = None  # Second part is ISIN, no short code available
                        else:
                            short_code = second_part  # Second part is the short code
                    
                    # Create combined_id following same format as HL
                    combined_id = None
                    if isin and short_code:
                        combined_id = f"GBP | {isin} | {short_code}"
                    elif isin:
                        combined_id = f"GBP | {isin}"
                    
                    corporate_bond_record = {
                        'bond_name': bond_name,
                        'company_name': company_name,
                        'clean_price': clean_price,
                        'accrued_interest': accrued_interest,
                        'dirty_price': dirty_price,
                        'coupon_rate': coupon_rate,
                        'maturity_date': maturity_date,
                        'years_to_maturity': years_to_maturity,
                        'ytm': ytm,
                        'after_tax_ytm': after_tax_ytm,
                        'credit_rating': 'NR',  # AJ Bell doesn't provide credit ratings
                        'isin': isin,
                        'short_code': short_code,
                        'combined_id': combined_id,
                        'currency_code': 'GBP',   # All UK corporate bonds are GBP
                        'scraped_date': settlement_date.date(),
                        'source': 'AJ Bell'
                    }
                    
                    corporate_bond_data.append(corporate_bond_record)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing AJ Bell corporate bond row {i+1}: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(corporate_bond_data)} AJ Bell corporate bond records")
            return corporate_bond_data
            
        except Exception as e:
            self.logger.error(f"Error parsing AJ Bell corporate bond table: {e}")
            return []
    
    def _find_column_index(self, headers: List[str], keywords: List[str]) -> Optional[int]:
        """Find column index by matching keywords."""
        for i, header in enumerate(headers):
            for keyword in keywords:
                if keyword in header:
                    return i
        return None


def collect_ajbell_corporate_bond_prices(database_url=None):
    """Collect real-time corporate bond prices from AJ Bell broker."""
    collector = AJBellCorporateBondCollector(database_url)
    
    try:
        # Scrape corporate bond prices from broker website
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        if not corporate_bond_data:
            collector.logger.info("No AJ Bell corporate bond data found")
            return 0
        
        # Process and store data
        bulk_data = []
        for bond in corporate_bond_data:
            try:
                # Convert to database format matching corporate_bond_prices table structure
                isin_value = str(bond['isin']) if bond['isin'] is not None else None
                short_code_value = str(bond['short_code']) if bond['short_code'] is not None else None
                
                # Debug log field lengths to help identify constraint violations
                if isin_value and len(isin_value) > 10:
                    collector.logger.debug(f"ISIN longer than 10 chars for {bond['bond_name']}: '{isin_value}' ({len(isin_value)} chars)")
                if short_code_value and len(short_code_value) > 10:
                    collector.logger.warning(f"Short code longer than 10 chars for {bond['bond_name']}: '{short_code_value}' ({len(short_code_value)} chars) - truncating to 10 chars")
                    short_code_value = short_code_value[:10]  # Truncate to fit VARCHAR(10) constraint
                
                data = {
                    'bond_name': str(bond['bond_name']) if bond['bond_name'] is not None else None,
                    'company_name': str(bond['company_name']) if bond['company_name'] is not None else None,
                    'clean_price': float(bond['clean_price']) if bond['clean_price'] is not None else None,
                    'accrued_interest': float(bond['accrued_interest']) if bond['accrued_interest'] is not None else None,
                    'dirty_price': float(bond['dirty_price']) if bond['dirty_price'] is not None else None,
                    'coupon_rate': float(bond['coupon_rate']) if bond['coupon_rate'] is not None else None,
                    'maturity_date': bond['maturity_date'].date(),
                    'years_to_maturity': float(bond['years_to_maturity']) if bond['years_to_maturity'] is not None else None,
                    'ytm': float(bond['ytm']) if bond['ytm'] is not None else None,
                    'after_tax_ytm': float(bond['after_tax_ytm']) if bond['after_tax_ytm'] is not None else None,
                    'credit_rating': str(bond['credit_rating']) if bond['credit_rating'] is not None else 'NR',
                    'scraped_date': bond['scraped_date'],
                    'currency_code': str(bond['currency_code']),
                    'isin': isin_value,
                    'short_code': short_code_value,
                    'combined_id': str(bond['combined_id']) if bond['combined_id'] is not None else None
                }
                bulk_data.append(data)
                
            except Exception as e:
                collector.logger.error(f"Error processing AJ Bell corporate bond data: {str(e)}")
                continue
        
        # Bulk upsert all records
        if bulk_data:
            success_count = collector.bulk_upsert_data(
                "corporate_bond_prices", 
                bulk_data,
                conflict_columns=['bond_name', 'scraped_date']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(bulk_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} AJ Bell corporate bond price records")
            return success_count
        else:
            collector.logger.info("No valid AJ Bell corporate bond data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect AJ Bell corporate bond prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task
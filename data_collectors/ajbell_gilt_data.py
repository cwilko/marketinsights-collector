"""
AJ Bell gilt market data collector for real-time broker prices.

This module requires additional dependencies:
- scipy>=1.11.0 (for YTM calculations)
- selenium>=4.15.0 (for web scraping) 
- webdriver-manager>=4.0.0 (for Chrome driver management)

AJ Bell provides delayed gilt prices (approximately 15 minutes) for UK Treasury bonds.
Calculates accrued interest, dirty prices, YTM, and after-tax YTM from clean prices.
"""
import time
import re
import platform
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None

try:
    from scipy.optimize import fsolve
except ImportError:
    fsolve = None

from .base import BaseCollector


class AJBellGiltCollector(BaseCollector):
    """
    Collector for real-time gilt market prices from AJ Bell broker.
    Scrapes gilt prices with basic data including ISINs and maturity dates.
    """
    
    def __init__(self, database_url=None):
        super().__init__(database_url)
        self.base_url = "https://www.ajbell.co.uk/investment/bonds/gilts/prices"
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
        options.add_argument("--remote-debugging-port=9225")  # Different port from HL scraper
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
            
            wait = WebDriverWait(driver, 10)
            
            for selector in cookie_selectors:
                try:
                    cookie_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    self.logger.info(f"Found cookie consent button with selector: {selector}")
                    cookie_button.click()
                    self.logger.info("Cookie consent accepted")
                    time.sleep(2)  # Wait for banner to disappear
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
                        time.sleep(2)
                        return
            except:
                pass
            
            self.logger.debug("No cookie consent banner found or already accepted")
            
        except Exception as e:
            self.logger.warning(f"Error handling cookie consent: {e}")
    
    def _wait_for_content(self, driver):
        """Wait for dynamic content to load."""
        try:
            wait = WebDriverWait(driver, 15)
            
            # Wait for gilt price table specifically
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table")))
            self.logger.info("Gilt price table loaded successfully")
            
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
            
            wait = WebDriverWait(driver, 10)
            
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
                        time.sleep(3)  # Wait for page to reload with more entries
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
                        time.sleep(3)
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
            '%d %B %Y',      # 22 October 2025
            '%d %b %Y',      # 22 Oct 2025
            '%d/%m/%Y',      # 22/10/2025
            '%d-%m-%Y',      # 22-10-2025
            '%Y-%m-%d',      # 2025-10-22
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
    
    def scrape_gilt_prices(self) -> List[Dict[str, Any]]:
        """Scrape AJ Bell gilt prices."""
        arch = platform.machine().lower()
        system = platform.system()
        
        try:
            return self._scrape_with_selenium()
        except Exception as e:
            # On ARM64 Linux (Pi), fail hard - don't fall back gracefully
            if arch in ['aarch64', 'arm64'] and system == 'Linux':
                self.logger.error(f"CRITICAL: Selenium scraping failed on ARM64 Linux: {str(e)}")
                self.logger.error("ChromeDriver/Chromium should be installed by init container")
                raise RuntimeError(f"ARM64 AJ Bell gilt scraping failed: {e}") from e
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
            
            self.logger.info(f"Loading AJ Bell gilt prices page: {self.base_url}")
            driver.get(self.base_url)
            
            # Wait for initial page load
            time.sleep(3)
            
            # Handle cookie consent if present
            self._handle_cookie_consent(driver)
            
            # Wait for content to load after cookie acceptance
            self._wait_for_content(driver)
            
            # Try to set display to 100 entries to get all gilts
            self.logger.info("Attempting to set display to 100 entries...")
            if self._set_display_to_100(driver):
                self.logger.info("Successfully set display to 100 entries")
                # Wait for page to reload with more entries
                self._wait_for_content(driver)
            else:
                self.logger.info("Could not set display to 100, proceeding with default display")
            
            # Find the gilt price table
            table = driver.find_element(By.CSS_SELECTOR, "table")
            
            return self._parse_gilt_table(table)
            
        except Exception as e:
            self.logger.error(f"AJ Bell scraping error: {e}")
            return []
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    def _parse_gilt_table(self, table_element) -> List[Dict[str, Any]]:
        """Parse the AJ Bell gilt price table."""
        try:
            rows = table_element.find_elements(By.TAG_NAME, "tr")
            if len(rows) < 2:  # Need header + at least one data row
                return []
            
            # Get headers
            header_row = rows[0]
            headers = [cell.text.strip().lower() for cell in header_row.find_elements(By.TAG_NAME, "th")]
            
            if not headers:
                headers = [cell.text.strip().lower() for cell in header_row.find_elements(By.TAG_NAME, "td")]
            
            self.logger.info(f"AJ Bell table headers: {headers}")
            self.logger.info(f"Found {len(rows)} total rows (including header)")
            
            # Find column indices for AJ Bell structure
            name_col = self._find_column_index(headers, ['issuer', 'name', 'bond', 'gilt', 'security'])
            price_col = self._find_column_index(headers, ['price', 'clean price', 'mid'])
            coupon_col = self._find_column_index(headers, ['coupon', 'rate', '%'])
            maturity_col = self._find_column_index(headers, ['maturity', 'expiry', 'date'])
            
            gilt_data = []
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
                    if '|' in bond_name_full:
                        bond_name = bond_name_full.split('|')[0].strip()
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
                    
                    # Extract ISIN and short codes
                    isin = None
                    short_code = None
                    
                    # Look for ISIN pattern (GB followed by 10 alphanumeric)
                    isin_match = re.search(r'GB[A-Z0-9]{10}', bond_name_full)
                    if isin_match:
                        isin = isin_match.group(0)
                    
                    # Extract short code (between first | and second |)
                    parts = bond_name_full.split('|')
                    if len(parts) >= 2:
                        short_code = parts[1].strip()
                    
                    # Calculate years to maturity
                    settlement_date = datetime.now()
                    years_to_maturity = (maturity_date - settlement_date).days / 365.25
                    if years_to_maturity <= 0:
                        continue
                    
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
                    
                    # Create combined_id following same format as HL
                    combined_id = None
                    if isin and short_code:
                        combined_id = f"GBP | {isin} | {short_code}"
                    elif isin:
                        combined_id = f"GBP | {isin}"
                    
                    gilt_record = {
                        'bond_name': bond_name,
                        'clean_price': clean_price,
                        'accrued_interest': accrued_interest,
                        'dirty_price': dirty_price,
                        'coupon_rate': coupon_rate,
                        'maturity_date': maturity_date,
                        'years_to_maturity': years_to_maturity,
                        'ytm': ytm,
                        'after_tax_ytm': after_tax_ytm,
                        'isin': isin,
                        'short_code': short_code,
                        'combined_id': combined_id,
                        'currency_code': 'GBP',   # All UK gilts are GBP
                        'data_source': 'AJ_Bell',
                        'scraped_date': settlement_date.date(),
                        'source': 'AJ Bell'  # Keep for backwards compatibility
                    }
                    
                    gilt_data.append(gilt_record)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing AJ Bell row {i+1}: {e}")
                    continue
            
            self.logger.info(f"Successfully parsed {len(gilt_data)} AJ Bell gilt records")
            return gilt_data
            
        except Exception as e:
            self.logger.error(f"Error parsing AJ Bell table: {e}")
            return []
    
    def _find_column_index(self, headers: List[str], keywords: List[str]) -> Optional[int]:
        """Find column index by matching keywords."""
        for i, header in enumerate(headers):
            for keyword in keywords:
                if keyword in header:
                    return i
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
        - Capital gains are tax-free (typical for UK gilts)
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


def collect_ajbell_gilt_prices(database_url=None):
    """Collect real-time gilt market prices from AJ Bell broker."""
    collector = AJBellGiltCollector(database_url)
    
    try:
        # Scrape gilt prices from broker website
        gilt_data = collector.scrape_gilt_prices()
        
        if not gilt_data:
            collector.logger.info("No AJ Bell gilt market data found")
            return 0
        
        # Process and store data
        bulk_data = []
        for gilt in gilt_data:
            try:
                # Convert to database format matching new SQL structure
                data = {
                    'bond_name': str(gilt['bond_name']) if gilt['bond_name'] is not None else None,
                    'clean_price': float(gilt['clean_price']) if gilt['clean_price'] is not None else None,
                    'accrued_interest': gilt['accrued_interest'],  # None for AJ Bell
                    'dirty_price': gilt['dirty_price'],            # None for AJ Bell
                    'coupon_rate': float(gilt['coupon_rate']) if gilt['coupon_rate'] is not None else None,
                    'maturity_date': gilt['maturity_date'].date(),
                    'years_to_maturity': float(gilt['years_to_maturity']) if gilt['years_to_maturity'] is not None else None,
                    'ytm': gilt['ytm'],                           # None for AJ Bell
                    'after_tax_ytm': gilt['after_tax_ytm'],       # None for AJ Bell
                    'scraped_date': gilt['scraped_date'],
                    'currency_code': str(gilt['currency_code']),
                    'isin': str(gilt['isin']) if gilt['isin'] is not None else None,
                    'short_code': str(gilt['short_code']) if gilt['short_code'] is not None else None,
                    'combined_id': str(gilt['combined_id']) if gilt['combined_id'] is not None else None,
                    'data_source': str(gilt['data_source'])
                }
                bulk_data.append(data)
                
            except Exception as e:
                collector.logger.error(f"Error processing AJ Bell gilt data: {str(e)}")
                continue
        
        # Bulk upsert all records
        if bulk_data:
            success_count = collector.bulk_upsert_data(
                "ajbell_gilt_prices", 
                bulk_data,
                conflict_columns=['bond_name', 'scraped_date']
            )
            if success_count == 0:
                raise RuntimeError(f"Database upsert failed - no records were successfully stored despite having {len(bulk_data)} records to process")
            collector.logger.info(f"Successfully stored {success_count} AJ Bell gilt price records")
            return success_count
        else:
            collector.logger.info("No valid AJ Bell gilt market data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect AJ Bell gilt market prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task
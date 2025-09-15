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
    
    def calculate_ytm_from_dirty(self, dirty_price: float, face_value: float, 
                               coupon_rate: float, years_to_maturity: float, 
                               payments_per_year: int = 2) -> Optional[float]:
        """Calculate YTM using dirty price."""
        coupon_payment = (coupon_rate * face_value) / payments_per_year
        total_periods = int(years_to_maturity * payments_per_year)
        
        def bond_price_equation(ytm):
            if ytm == 0:
                return coupon_payment * total_periods + face_value - dirty_price
            
            periodic_rate = ytm / payments_per_year
            present_value_coupons = coupon_payment * (1 - (1 + periodic_rate) ** -total_periods) / periodic_rate
            present_value_face = face_value / (1 + periodic_rate) ** total_periods
            
            return present_value_coupons + present_value_face - dirty_price
        
        initial_guess = coupon_rate if coupon_rate > 0 else 0.05
        
        try:
            ytm_solution = fsolve(bond_price_equation, initial_guess)[0]
            return ytm_solution
        except:
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
                        # Get bond name/issuer
                        bond_name = cells[issuer_col].text.strip()
                        
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
                        last_coupon_date = self.estimate_coupon_dates(bond_name, maturity_date, settlement_date)
                        accrued_interest = self.calculate_accrued_interest(
                            100.0, coupon_rate, last_coupon_date, settlement_date
                        )
                        
                        # Calculate dirty price
                        dirty_price = clean_price + accrued_interest
                        
                        # Calculate YTM using dirty price
                        ytm = self.calculate_ytm_from_dirty(dirty_price, 100.0, coupon_rate, years_to_maturity)
                        
                        # Calculate after-tax YTM (30% tax on coupons, no tax on capital gains)
                        after_tax_ytm = self.calculate_after_tax_ytm(dirty_price, 100.0, coupon_rate, years_to_maturity, 0.30)
                        
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
                            'scraped_date': settlement_date.date()
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
                    'scraped_date': gilt['scraped_date']
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
                        continue
                    
                    # Extract bond data - index-linked gilt table structure
                    bond_name_element = cells[0].find_element(By.TAG_NAME, "a")
                    bond_name = bond_name_element.text.strip()
                    
                    # Skip if not a Treasury bond
                    if 'Treasury' not in bond_name:
                        continue
                    
                    # Extract clean price from Price column (column 3)
                    clean_price_text = cells[3].text.strip() if len(cells) > 3 else ""
                    clean_price_match = re.search(r'([0-9]+\.?[0-9]*)', clean_price_text)
                    if not clean_price_match:
                        continue
                    
                    clean_price = float(clean_price_match.group(1))
                    
                    # Parse coupon rate from Coupon (%) column (column 1) and use as approximate real yield
                    # Index-linked gilts don't show real yield directly on this page
                    coupon_text = cells[1].text.strip() if len(cells) > 1 else ""
                    coupon_match = re.search(r'([0-9]+\.?[0-9]*)', coupon_text)
                    if not coupon_match:
                        continue
                    
                    coupon_rate = float(coupon_match.group(1)) / 100  # Convert percentage to decimal
                    
                    # Parse maturity date from bond name
                    maturity_match = re.search(r'(\d{4})', bond_name)
                    if not maturity_match:
                        continue
                    
                    maturity_year = int(maturity_match.group(1))
                    
                    # Estimate maturity date (index-linked gilts often mature in March)
                    try:
                        maturity_date = datetime(maturity_year, 3, 22)  # Common IL gilt maturity date
                    except ValueError:
                        continue
                    
                    # Calculate years to maturity
                    years_to_maturity = (maturity_date - settlement_date).days / 365.25
                    if years_to_maturity <= 0:
                        continue
                    
                    # Coupon rate already parsed from Coupon (%) column above
                    
                    # Simplified accrued interest calculation (for now)
                    days_since_coupon = 90  # Estimate
                    accrued_interest = (coupon_rate / 2) * (days_since_coupon / 182.5)  # Semi-annual coupons
                    
                    dirty_price = clean_price + accrued_interest
                    
                    # Calculate proper YTM using bond pricing equation  
                    face_value = 100.0  # Standard face value
                    try:
                        self.logger.debug(f"Row {i}: Calculating YTM for {bond_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        ytm = self.calculate_ytm_from_dirty(dirty_price, face_value, coupon_rate, years_to_maturity)
                        
                        if ytm is None:
                            self.logger.warning(f"Row {i}: YTM calculation failed for {bond_name}, using coupon rate as fallback")
                            ytm = coupon_rate  # Fallback to coupon rate
                        else:
                            self.logger.debug(f"Row {i}: YTM calculated successfully for {bond_name}: {ytm*100:.3f}%")
                    
                    except Exception as e:
                        self.logger.error(f"Row {i}: YTM calculation error for {bond_name}: {str(e)}")
                        ytm = coupon_rate  # Fallback to coupon rate
                    
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
                        'inflation_assumption': 3.0  # HL typically assumes 3% inflation
                    })
                    
                except Exception as e:
                    self.logger.debug(f"Error processing index-linked bond row {i}: {str(e)}")
                    continue
            
            self.logger.info(f"Successfully scraped {len(bonds)} index-linked gilt prices")
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
                    'inflation_assumption': float(gilt['inflation_assumption']) if gilt['inflation_assumption'] is not None else None
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
                        continue
                    
                    # Extract bond data - corporate bond table structure
                    bond_name_element = cells[0].find_element(By.TAG_NAME, "a")
                    bond_name = bond_name_element.text.strip()
                    
                    # Skip if empty or invalid name
                    if not bond_name:
                        continue
                    
                    # Extract company name from bond name (usually first part before newline)
                    company_name = bond_name.split('\n')[0] if bond_name else "Unknown"
                    
                    # Extract clean price from Price column (column 3)
                    clean_price_text = cells[3].text.strip() if len(cells) > 3 else ""
                    clean_price_match = re.search(r'([0-9]+\.?[0-9]*)', clean_price_text)
                    if not clean_price_match:
                        continue
                    
                    clean_price = float(clean_price_match.group(1))
                    
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
                    face_value = 100.0  # Standard face value
                    try:
                        self.logger.debug(f"Row {i}: Calculating YTM for {company_name} - Price: {dirty_price}, Coupon: {coupon_rate*100:.3f}%, Years: {years_to_maturity:.2f}")
                        
                        ytm = self.calculate_ytm_from_dirty(dirty_price, face_value, coupon_rate, years_to_maturity)
                        
                        if ytm is None:
                            self.logger.warning(f"Row {i}: YTM calculation failed for {company_name}, using coupon rate as fallback")
                            ytm = coupon_rate  # Fallback to coupon rate
                        else:
                            self.logger.debug(f"Row {i}: YTM calculated successfully for {company_name}: {ytm*100:.3f}%")
                    
                    except Exception as e:
                        self.logger.error(f"Row {i}: YTM calculation error for {company_name}: {str(e)}")
                        ytm = coupon_rate  # Fallback to coupon rate
                    
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
                        'scraped_date': settlement_date.date()
                    })
                    
                except Exception as e:
                    self.logger.debug(f"Error processing corporate bond row {i}: {str(e)}")
                    continue
            
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
                    'scraped_date': bond['scraped_date']
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
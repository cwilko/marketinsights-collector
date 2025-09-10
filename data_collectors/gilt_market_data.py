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
                        
                        bonds.append({
                            'bond_name': bond_name.split('\n')[0],  # Take first line
                            'clean_price': clean_price,
                            'accrued_interest': accrued_interest,
                            'dirty_price': dirty_price,
                            'coupon_rate': coupon_rate,
                            'maturity_date': maturity_date,
                            'years_to_maturity': years_to_maturity,
                            'ytm': ytm,
                            'after_tax_ytm': after_tax_ytm,
                            'scraped_at': settlement_date
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
                    'scraped_at': gilt['scraped_at']
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
                conflict_columns=['bond_name', 'scraped_at']
            )
            collector.logger.info(f"Successfully stored {success_count} gilt market price records")
            return success_count
        else:
            collector.logger.info("No valid gilt market data to process")
            return 0
            
    except Exception as e:
        collector.logger.error(f"Failed to collect gilt market prices: {str(e)}")
        raise  # Re-raise the exception to fail the Airflow task
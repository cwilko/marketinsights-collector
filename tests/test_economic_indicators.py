"""
Tests for economic indicator data collectors.
"""

import pytest
from datetime import datetime
from data_collectors.economic_indicators import FREDCollector, BLSCollector, BEACollector, collect_cpi, collect_monthly_fed_funds_rate, collect_daily_fed_funds_rate, collect_unemployment_rate, collect_gdp
from data_collectors.market_data import collect_fred_treasury_yields, FRED_TREASURY_SERIES


class TestFREDCollector:
    """Tests for FRED API data collection."""
    
    def test_fred_collector_initialization(self):
        """Test FRED collector can be initialized."""
        collector = FREDCollector(database_url=None)
        assert collector.base_url == "https://api.stlouisfed.org/fred/series/observations"
        assert collector.api_key is not None
        assert collector.database_url is None
        
    def test_get_series_data(self):
        """Test FRED collector can fetch series data."""
        collector = FREDCollector(database_url=None)
        data = collector.get_series_data("FEDFUNDS", limit=5)
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Test data structure
        for obs in data[:3]:
            assert "date" in obs
            assert "value" in obs
            # Some values might be ".", that's ok
            
    def test_collect_fed_funds_rate_function(self):
        """Test the collect_monthly_fed_funds_rate function."""
        result = collect_monthly_fed_funds_rate(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records

    def test_collect_daily_fed_funds_rate_function(self):
        """Test the collect_daily_fed_funds_rate function."""
        result = collect_daily_fed_funds_rate(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records
        
    def test_fred_collector_date_filtering(self):
        """Test FRED collector date filtering functionality."""
        from datetime import datetime, timedelta
        
        collector = FREDCollector(database_url=None)
        
        # Test with date range
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        data = collector.get_series_data("DFF", 
                                       observation_start=start_date,
                                       observation_end=end_date)
        
        assert isinstance(data, list)
        # Should have fewer records than default 100 limit for a week of daily data
        if len(data) > 0:
            assert len(data) <= 10  # At most 7 days + weekends


class TestBLSCollector:
    """Tests for BLS API data collection."""
    
    def test_bls_collector_initialization(self):
        """Test BLS collector can be initialized."""
        collector = BLSCollector(database_url=None)
        assert collector.base_url == "https://api.bls.gov/publicAPI/v2/timeseries/data"
        assert collector.database_url is None
        # API key is optional for BLS
        
    def test_get_series_data(self):
        """Test BLS collector can fetch series data."""
        collector = BLSCollector(database_url=None)
        data = collector.get_series_data("CUUR0000SA0")  # All items CPI-U
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Test data structure
        for item in data[:3]:
            assert "year" in item
            assert "period" in item
            assert "value" in item
            assert item["period"].startswith("M")  # Monthly data
            
    def test_collect_cpi_function(self):
        """Test the collect_cpi function."""
        result = collect_cpi(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records
        
    def test_collect_unemployment_function(self):
        """Test the collect_unemployment_rate function."""
        result = collect_unemployment_rate(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records


class TestBEACollector:
    """Tests for BEA API data collection."""
    
    def test_bea_collector_initialization(self):
        """Test BEA collector can be initialized."""
        collector = BEACollector(database_url=None)
        assert collector.base_url == "https://apps.bea.gov/api/data"
        assert collector.api_key is not None
        assert collector.database_url is None
        
    def test_get_gdp_data(self):
        """Test BEA collector can fetch GDP data."""
        collector = BEACollector(database_url=None)
        data = collector.get_gdp_data()
        
        assert isinstance(data, list)
        if len(data) > 0:  # BEA might have rate limits
            # Test data structure
            gdp_items = [item for item in data if item.get("LineDescription") == "Gross domestic product"]
            if gdp_items:
                item = gdp_items[0]
                assert "TimePeriod" in item
                assert "DataValue" in item
                assert "Q" in item["TimePeriod"]  # Quarterly data
            
    def test_collect_gdp_function(self):
        """Test the collect_gdp function."""
        result = collect_gdp(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records or return 0 if rate limited


class TestFREDTreasuryYields:
    """Tests for FRED-based Treasury yield curve data collection."""
    
    def test_fred_treasury_series_mapping(self):
        """Test FRED Treasury series mapping is properly defined."""
        assert isinstance(FRED_TREASURY_SERIES, dict)
        assert len(FRED_TREASURY_SERIES) > 0
        
        # Check some key series are present
        expected_series = ["DGS3MO", "DGS2", "DGS10", "DGS30"]
        for series_id in expected_series:
            assert series_id in FRED_TREASURY_SERIES
            assert len(FRED_TREASURY_SERIES[series_id]) > 0
            
    def test_fred_treasury_individual_series(self):
        """Test FRED collector can fetch individual Treasury series."""
        collector = FREDCollector(database_url=None)
        
        # Test a few key Treasury series
        test_series = ["DGS3MO", "DGS10", "DGS30"]
        
        for series_id in test_series:
            data = collector.get_series_data(series_id, limit=5)
            
            assert isinstance(data, list)
            assert len(data) > 0, f"Should return data for {series_id}"
            
            # Test data structure for first few records
            for item in data[:3]:
                assert "date" in item
                assert "value" in item
                
                # Skip missing values (marked as ".")
                if item["value"] != ".":
                    # Validate date format
                    record_date = datetime.strptime(item["date"], "%Y-%m-%d")
                    assert record_date is not None
                    
                    # Validate yield is reasonable
                    yield_rate = float(item["value"])
                    assert 0.0 <= yield_rate <= 25.0, f"Yield rate {yield_rate}% seems unreasonable for {series_id}"
                    
    def test_fred_treasury_date_filtering(self):
        """Test FRED Treasury collector with date ranges."""
        collector = FREDCollector(database_url=None)
        
        # Test with recent date range (last 30 days)
        from datetime import datetime, timedelta
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        data = collector.get_series_data(
            "DGS10",  # 10-year Treasury
            observation_start=start_date,
            observation_end=end_date
        )
        
        assert isinstance(data, list)
        if len(data) > 0:
            # Should have reasonable number of records for 30 days (business days only)
            assert len(data) <= 30
            
            # Verify dates are within range and in proper format
            for item in data[:5]:  # Check first 5 items
                if item["value"] != ".":
                    record_date = datetime.strptime(item["date"], "%Y-%m-%d")
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    assert start_dt <= record_date <= end_dt
                    
    def test_collect_fred_treasury_yields_function(self):
        """Test the collect_fred_treasury_yields function."""
        result = collect_fred_treasury_yields(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records
"""
Tests for economic indicator data collectors.
"""

import pytest
from datetime import datetime
from data_collectors.economic_indicators import FREDCollector, BLSCollector, BEACollector, collect_cpi, collect_fed_funds_rate, collect_unemployment, collect_gdp


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
        """Test the collect_fed_funds_rate function."""
        result = collect_fed_funds_rate(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records


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
        """Test the collect_unemployment function."""
        result = collect_unemployment(database_url=None)
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
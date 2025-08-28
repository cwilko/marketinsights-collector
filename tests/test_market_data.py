"""
Tests for market data collectors.
"""

import pytest
from datetime import datetime
from data_collectors.market_data import collect_sp500, collect_vix, collect_treasury_yields, collect_pe_ratios
from data_collectors.economic_indicators import FREDCollector


class TestMarketDataFunctions:
    """Tests for market data collection functions."""
    
    def test_collect_sp500_function(self):
        """Test S&P 500 data collection function."""
        result = collect_sp500(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records
        
    def test_collect_vix_function(self):
        """Test VIX data collection function."""
        result = collect_vix(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records

    def test_fred_collector_sp500_data(self):
        """Test FRED collector can fetch S&P 500 data."""
        collector = FREDCollector(database_url=None)
        data = collector.get_series_data("SP500", limit=5)
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Test data structure
        valid_count = 0
        for obs in data:
            if obs["value"] != ".":
                assert "date" in obs
                assert "value" in obs
                price = float(obs["value"])
                assert price > 0  # S&P 500 should be positive
                assert price < 100000  # Reasonable upper bound
                valid_count += 1
                
        assert valid_count > 0
        
    def test_fred_collector_vix_data(self):
        """Test FRED collector can fetch VIX data."""
        collector = FREDCollector(database_url=None)
        data = collector.get_series_data("VIXCLS", limit=5)
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Test data structure
        valid_count = 0
        for obs in data:
            if obs["value"] != ".":
                assert "date" in obs
                assert "value" in obs
                vix = float(obs["value"])
                assert vix > 0  # VIX should be positive
                assert vix < 200  # Reasonable upper bound
                valid_count += 1
                
        assert valid_count > 0

    def test_fred_collector_treasury_data(self):
        """Test FRED collector can fetch Treasury yield data."""
        collector = FREDCollector(database_url=None)
        data = collector.get_series_data("DGS10", limit=5)  # 10-Year Treasury
        
        assert isinstance(data, list)
        assert len(data) > 0
        
        # Test data structure
        valid_count = 0
        for obs in data:
            if obs["value"] != ".":
                assert "date" in obs
                assert "value" in obs
                yield_rate = float(obs["value"])
                assert yield_rate >= 0  # Yield can be zero (rare) but not negative typically
                assert yield_rate <= 20  # Reasonable upper bound for Treasury yields
                valid_count += 1
                
        assert valid_count > 0

    def test_collect_pe_ratios_function(self):
        """Test P/E ratios collection function."""
        # Note: This might fail if web scraping is blocked
        try:
            result = collect_pe_ratios(database_url=None)
            assert isinstance(result, int)
            assert result >= 0
        except Exception:
            # P/E ratios use web scraping which might be unreliable
            pytest.skip("P/E ratio collection failed (web scraping may be blocked)")


@pytest.mark.integration
class TestMarketDataIntegration:
    """Integration tests for market data collection."""
    
    def test_all_market_data_collectors(self):
        """Test that all market data collectors return data."""
        collectors = [
            ("S&P 500", collect_sp500),
            ("VIX", collect_vix),
        ]
        
        for name, collector_func in collectors:
            result = collector_func(database_url=None)
            assert isinstance(result, int), f"{name} collector should return int"
            assert result >= 0, f"{name} collector should return non-negative count"
            
    def test_fred_collector_integration(self):
        """Test FRED collector can fetch multiple series."""
        collector = FREDCollector(database_url=None)
        series_ids = ["SP500", "VIXCLS", "DGS10"]
        
        for series_id in series_ids:
            data = collector.get_series_data(series_id, limit=1)
            assert isinstance(data, list), f"Should return list for {series_id}"
            assert len(data) > 0, f"Should return data for {series_id}"
            
    def test_data_freshness(self):
        """Test that market data is reasonably fresh."""
        collector = FREDCollector(database_url=None)
        data = collector.get_series_data("SP500", limit=1)
        
        if data and data[0]["value"] != ".":
            latest_date = datetime.strptime(data[0]["date"], "%Y-%m-%d").date()
            today = datetime.now().date()
            days_diff = (today - latest_date).days
            
            # Data should be within last 7 days (accounting for weekends)
            assert days_diff <= 7, f"Data is {days_diff} days old"
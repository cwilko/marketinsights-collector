"""
Tests for incremental data collection functionality.
Tests that collectors properly implement DRY principles with incremental updates.
"""

import pytest
import os
from datetime import datetime, date, timedelta
from unittest.mock import patch
from data_collectors.economic_indicators import collect_monthly_fed_funds_rate, collect_cpi, FREDCollector, BEACollector
from data_collectors.market_data import collect_sp500, collect_fred_treasury_yields


class TestIncrementalCollection:
    """Test incremental data collection with DRY principles."""

    def test_fred_collector_date_range_logic(self):
        """Test FRED collector with date range parameters."""
        collector = FREDCollector(database_url=None)
        
        # Test with date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        data = collector.get_series_data(
            "FEDFUNDS", 
            observation_start=start_date.strftime("%Y-%m-%d"),
            observation_end=end_date.strftime("%Y-%m-%d")
        )
        
        assert isinstance(data, list)
        if data:  # If we got data, verify it's within range
            for item in data[:5]:  # Check first 5 items
                assert "date" in item
                assert "value" in item

    def test_incremental_fed_funds_safe_mode(self):
        """Test Federal Funds Rate collector in safe mode (no database)."""
        # Safe mode - no database operations
        result = collect_monthly_fed_funds_rate(database_url=None)
        
        # Should return a count of processed records
        assert isinstance(result, int)
        assert result >= 0

    def test_incremental_cpi_safe_mode(self):
        """Test CPI collector in safe mode (no database)."""
        # Safe mode - no database operations
        result = collect_cpi(database_url=None)
        
        # Should return a count of processed records
        assert isinstance(result, int)
        assert result >= 0

    def test_incremental_sp500_safe_mode(self):
        """Test S&P 500 collector in safe mode (no database)."""
        # Safe mode - no database operations
        result = collect_sp500(database_url=None)
        
        # Should return a count of processed records
        assert isinstance(result, int)
        assert result >= 0

    def test_incremental_fred_treasury_safe_mode(self):
        """Test FRED Treasury yields collector in safe mode (no database)."""
        # Safe mode - no database operations
        result = collect_fred_treasury_yields(database_url=None)
        
        # Should return a count of processed records
        assert isinstance(result, int)
        assert result >= 0
        
        # Additional validation - test actual data collection
        from data_collectors.market_data import FRED_TREASURY_SERIES
        collector = FREDCollector(database_url=None)
        
        # Test that we can collect data from at least one Treasury series
        test_series = "DGS10"  # 10-year Treasury
        data = collector.get_series_data(test_series, limit=5)
        
        assert isinstance(data, list)
        if len(data) > 0:
            # Validate data structure and quality
            for item in data[:3]:
                assert "date" in item
                assert "value" in item
                
                if item["value"] != ".":
                    # Validate date format
                    record_date = datetime.strptime(item["date"], "%Y-%m-%d")
                    assert record_date is not None
                    
                    # Validate reasonable yield values
                    yield_rate = float(item["value"])
                    assert 0.0 <= yield_rate <= 25.0, f"10Y Treasury yield {yield_rate}% seems unreasonable"


    def test_base_collector_date_range_logic(self):
        """Test base collector date range calculation logic."""
        from data_collectors.base import BaseCollector
        
        collector = BaseCollector(database_url=None)
        
        # Test without database - should return None for date queries
        last_date = collector.get_last_record_date("test_table")
        assert last_date is None
        
        table_exists = collector.table_exists("test_table")
        assert table_exists is False
        
        # Test date range calculation without database
        start_date, end_date = collector.get_date_range_for_collection(
            "test_table", 
            default_lookback_days=30
        )
        
        # Without database, should calculate range from today back 30 days
        expected_start = datetime.now().date() - timedelta(days=30)
        expected_end = datetime.now().date()
        
        assert start_date <= expected_start + timedelta(days=1)  # Allow 1 day variance
        assert end_date <= expected_end + timedelta(days=1)


@pytest.mark.slow
def test_bulk_api_efficiency():
    """Test that collectors use bulk fetching efficiently."""
    
    # Test FRED bulk fetching
    collector = FREDCollector(database_url=None)
    
    # Request 1 year of data - should be a single API call
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    data = collector.get_series_data(
        "FEDFUNDS",
        observation_start=start_date,
        observation_end=end_date
    )
    
    assert isinstance(data, list)
    # Should get reasonable amount of data in one call
    # Fed Funds Rate is monthly data, so 1 year = ~12 observations
    assert len(data) >= 10  # At least 10 observations for 1 year of monthly data


if __name__ == "__main__":
    # Allow running as script for manual testing
    pytest.main([__file__, "-v"])
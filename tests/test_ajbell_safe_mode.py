"""
Simple safe mode test for AJ Bell gilt data collection.
"""

import pytest
from data_collectors.ajbell_gilt_data import collect_ajbell_gilt_prices


def test_ajbell_gilt_safe_mode():
    """Test AJ Bell gilt collection in safe mode (no database writes)."""
    # Test in safe mode - should return count but not store data
    result = collect_ajbell_gilt_prices(database_url=None)
    
    # Should return a positive integer count
    assert isinstance(result, int)
    assert result > 0
    
    # With dropdown functionality, should get significantly more than 25
    assert result > 50, f"Expected >50 gilts with dropdown, got {result}"
    
    print(f"AJ Bell safe mode test: {result} gilt records processed")


if __name__ == "__main__":
    test_ajbell_gilt_safe_mode()
    print("✅ AJ Bell safe mode test passed!")
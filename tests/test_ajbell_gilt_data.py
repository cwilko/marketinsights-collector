"""
Tests for AJ Bell gilt market data collection.

Tests the AJ Bell gilt price scraper functionality including:
- Basic data collection (safe mode)
- Dropdown functionality to get all 98 gilts
- Data validation and structure
- ISIN and bond identifier extraction
"""

import pytest
from datetime import datetime, date
from data_collectors.ajbell_gilt_data import collect_ajbell_gilt_prices, AJBellGiltCollector


class TestAJBellGiltCollection:
    """Test AJ Bell gilt price collection functionality."""
    
    def test_ajbell_gilt_collection_safe_mode(self):
        """Test AJ Bell gilt collection in safe mode (no database writes)."""
        # Test in safe mode - should return count but not store data
        result = collect_ajbell_gilt_prices(database_url=None)
        
        # Should return a positive count
        assert isinstance(result, int)
        assert result > 0
        print(f"AJ Bell gilt collection returned {result} records")
    
    def test_ajbell_gilt_collection_comprehensive(self):
        """Test that AJ Bell collector gets comprehensive gilt coverage using dropdown."""
        # Test the collector directly to get more detailed results
        collector = AJBellGiltCollector(database_url=None)
        gilt_data = collector.scrape_gilt_prices()
        
        # Should get significantly more than the default 25 gilts (proving dropdown works)
        assert len(gilt_data) > 50, f"Expected >50 gilts with dropdown functionality, got {len(gilt_data)}"
        
        # Should get a substantial number of gilts (allowing for market changes over time)
        # Using flexible thresholds since gilt issuance changes
        assert len(gilt_data) >= 70, f"Expected >=70 gilts with dropdown, got {len(gilt_data)} (may indicate dropdown not working)"
        
        # Upper bound check to catch potential parsing errors
        assert len(gilt_data) <= 150, f"Got {len(gilt_data)} gilts - suspiciously high, check for parsing errors"
        
        print(f"AJ Bell comprehensive collection: {len(gilt_data)} gilts (dropdown functionality working)")
        return gilt_data
    
    def test_ajbell_gilt_data_structure(self):
        """Test that AJ Bell gilt data has the correct structure and content."""
        collector = AJBellGiltCollector(database_url=None)
        gilt_data = collector.scrape_gilt_prices()
        
        assert len(gilt_data) > 0, "No gilt data returned"
        
        # Test first gilt record structure
        first_gilt = gilt_data[0]
        
        # Required fields
        required_fields = [
            'bond_name', 'clean_price', 'coupon_rate', 'maturity_date',
            'years_to_maturity', 'scraped_date', 'source'
        ]
        
        for field in required_fields:
            assert field in first_gilt, f"Missing required field: {field}"
        
        # Data type validation
        assert isinstance(first_gilt['bond_name'], str)
        assert isinstance(first_gilt['clean_price'], (int, float))
        assert isinstance(first_gilt['coupon_rate'], (int, float))
        assert isinstance(first_gilt['maturity_date'], datetime)
        assert isinstance(first_gilt['years_to_maturity'], (int, float))
        assert isinstance(first_gilt['scraped_date'], date)
        assert first_gilt['source'] == 'AJ Bell'
        
        # Value range validation
        assert 0 < first_gilt['clean_price'] < 1000, f"Invalid price: {first_gilt['clean_price']}"
        assert 0 <= first_gilt['coupon_rate'] <= 0.1, f"Invalid coupon: {first_gilt['coupon_rate']}"
        assert first_gilt['years_to_maturity'] > 0, f"Invalid years to maturity: {first_gilt['years_to_maturity']}"
        
        print(f"Data structure validation passed for {len(gilt_data)} gilts")
    
    def test_ajbell_gilt_maturity_range(self):
        """Test that AJ Bell gilts cover a comprehensive maturity range."""
        collector = AJBellGiltCollector(database_url=None)
        gilt_data = collector.scrape_gilt_prices()
        
        assert len(gilt_data) > 0, "No gilt data returned"
        
        maturity_years = [gilt['maturity_date'].year for gilt in gilt_data]
        min_year = min(maturity_years)
        max_year = max(maturity_years)
        
        # Should have gilts maturing in the near term
        assert min_year <= 2027, f"Shortest maturity too far out: {min_year}"
        
        # Should have long-term gilts (with dropdown should get ultra-long bonds)
        assert max_year >= 2050, f"Longest maturity too short: {max_year}"
        
        # Should have a good spread
        year_range = max_year - min_year
        assert year_range >= 25, f"Maturity range too narrow: {year_range} years"
        
        print(f"Maturity range: {min_year} - {max_year} ({year_range} years)")
    
    def test_ajbell_gilt_identifiers(self):
        """Test that AJ Bell gilts include proper bond identifiers."""
        collector = AJBellGiltCollector(database_url=None)
        gilt_data = collector.scrape_gilt_prices()
        
        assert len(gilt_data) > 0, "No gilt data returned"
        
        # Count gilts with various identifiers
        with_isin = sum(1 for gilt in gilt_data if gilt.get('isin'))
        with_short_code = sum(1 for gilt in gilt_data if gilt.get('short_code'))
        
        # Most gilts should have ISINs
        isin_percentage = with_isin / len(gilt_data)
        assert isin_percentage >= 0.8, f"Only {isin_percentage:.1%} of gilts have ISINs"
        
        # Most gilts should have short codes
        short_code_percentage = with_short_code / len(gilt_data)
        assert short_code_percentage >= 0.8, f"Only {short_code_percentage:.1%} of gilts have short codes"
        
        # Validate ISIN format for those that have them
        for gilt in gilt_data:
            if gilt.get('isin'):
                isin = gilt['isin']
                assert len(isin) == 12, f"Invalid ISIN length: {isin}"
                assert isin.startswith('GB'), f"Invalid ISIN prefix: {isin}"
        
        print(f"Identifier validation: {with_isin}/{len(gilt_data)} ISINs, {with_short_code}/{len(gilt_data)} short codes")
    
    def test_ajbell_gilt_index_linked_detection(self):
        """Test that AJ Bell collector properly identifies index-linked gilts."""
        collector = AJBellGiltCollector(database_url=None)
        gilt_data = collector.scrape_gilt_prices()
        
        assert len(gilt_data) > 0, "No gilt data returned"
        
        # Count index-linked gilts
        index_linked = [gilt for gilt in gilt_data if 'Index-Linked' in gilt['bond_name']]
        
        # Should have a reasonable number of index-linked gilts
        il_percentage = len(index_linked) / len(gilt_data)
        assert len(index_linked) >= 5, f"Too few index-linked gilts: {len(index_linked)}"
        assert 0.1 <= il_percentage <= 0.7, f"Index-linked percentage outside expected range: {il_percentage:.1%}"
        
        print(f"Index-linked gilts: {len(index_linked)}/{len(gilt_data)} ({il_percentage:.1%})")
    
    def test_ajbell_collector_error_handling(self):
        """Test that AJ Bell collector handles errors gracefully."""
        # Test with invalid database URL - should not crash
        try:
            result = collect_ajbell_gilt_prices(database_url="invalid://url")
            # If it doesn't crash, that's good - may return 0 or raise controlled exception
            assert isinstance(result, int)
        except Exception as e:
            # Should be a controlled exception, not a crash
            assert "failed" in str(e).lower() or "error" in str(e).lower()
        
        print("Error handling validation passed")


if __name__ == "__main__":
    # Run tests directly
    test_instance = TestAJBellGiltCollection()
    
    print("Testing AJ Bell Gilt Data Collection")
    print("=" * 50)
    
    try:
        test_instance.test_ajbell_gilt_collection_safe_mode()
        print("✅ Safe mode collection test passed")
    except Exception as e:
        print(f"❌ Safe mode collection test failed: {e}")
    
    try:
        test_instance.test_ajbell_gilt_collection_comprehensive()
        print("✅ Comprehensive collection test passed")
    except Exception as e:
        print(f"❌ Comprehensive collection test failed: {e}")
    
    try:
        test_instance.test_ajbell_gilt_data_structure()
        print("✅ Data structure test passed")
    except Exception as e:
        print(f"❌ Data structure test failed: {e}")
    
    try:
        test_instance.test_ajbell_gilt_maturity_range()
        print("✅ Maturity range test passed")
    except Exception as e:
        print(f"❌ Maturity range test failed: {e}")
    
    try:
        test_instance.test_ajbell_gilt_identifiers()
        print("✅ Identifier validation test passed")
    except Exception as e:
        print(f"❌ Identifier validation test failed: {e}")
    
    try:
        test_instance.test_ajbell_gilt_index_linked_detection()
        print("✅ Index-linked detection test passed")
    except Exception as e:
        print(f"❌ Index-linked detection test failed: {e}")
    
    try:
        test_instance.test_ajbell_collector_error_handling()
        print("✅ Error handling test passed")
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
    
    print("\nAll AJ Bell gilt collection tests completed!")
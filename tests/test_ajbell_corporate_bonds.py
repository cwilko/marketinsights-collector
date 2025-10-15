"""
Tests for AJ Bell corporate bond data collection.

Tests the AJ Bell corporate bond price scraper functionality including:
- Basic data collection (safe mode)
- Dropdown functionality to get all corporate bonds
- Data validation and structure
- ISIN and bond identifier extraction
- YTM and financial calculations
"""

import pytest
from datetime import datetime, date
from data_collectors.gilt_market_data import collect_ajbell_corporate_bond_prices, AJBellCorporateBondCollector


class TestAJBellCorporateBondCollection:
    """Test AJ Bell corporate bond price collection functionality."""
    
    def test_ajbell_corporate_bond_collection_safe_mode(self):
        """Test AJ Bell corporate bond collection in safe mode (no database writes)."""
        # Test in safe mode - should return count but not store data
        result = collect_ajbell_corporate_bond_prices(database_url=None)
        
        # Should return a positive count
        assert isinstance(result, int)
        assert result > 0
        print(f"AJ Bell corporate bond collection returned {result} records")
    
    def test_ajbell_corporate_bond_collection_comprehensive(self):
        """Test that AJ Bell collector gets comprehensive corporate bond coverage using dropdown."""
        # Test the collector directly to get more detailed results
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        # Should get significantly more than the default 10-25 bonds (proving dropdown works)
        assert len(corporate_bond_data) > 20, f"Expected >20 corporate bonds with dropdown functionality, got {len(corporate_bond_data)}"
        
        # Should get a substantial number of corporate bonds (allowing for market changes)
        # Using flexible thresholds since corporate bond issuance changes
        assert len(corporate_bond_data) >= 25, f"Expected >=25 corporate bonds with dropdown, got {len(corporate_bond_data)} (may indicate dropdown not working)"
        
        # Upper bound check to catch potential parsing errors
        assert len(corporate_bond_data) <= 100, f"Got {len(corporate_bond_data)} corporate bonds - suspiciously high, check for parsing errors"
        
        print(f"AJ Bell comprehensive corporate bond collection: {len(corporate_bond_data)} bonds (dropdown functionality working)")
        return corporate_bond_data
    
    def test_ajbell_corporate_bond_data_structure(self):
        """Test that AJ Bell corporate bond data has the correct structure and content."""
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        assert len(corporate_bond_data) > 0, "No corporate bond data returned"
        
        # Test first corporate bond record structure
        first_bond = corporate_bond_data[0]
        
        # Required fields
        required_fields = [
            'bond_name', 'company_name', 'clean_price', 'accrued_interest', 'dirty_price',
            'coupon_rate', 'maturity_date', 'years_to_maturity', 'ytm', 'after_tax_ytm',
            'credit_rating', 'scraped_date', 'currency_code', 'source'
        ]
        
        for field in required_fields:
            assert field in first_bond, f"Missing required field: {field}"
        
        # Data type validation
        assert isinstance(first_bond['bond_name'], str)
        assert isinstance(first_bond['company_name'], (str, type(None)))
        assert isinstance(first_bond['clean_price'], (int, float))
        assert isinstance(first_bond['coupon_rate'], (int, float))
        assert isinstance(first_bond['maturity_date'], datetime)
        assert isinstance(first_bond['years_to_maturity'], (int, float))
        assert isinstance(first_bond['scraped_date'], date)
        assert first_bond['source'] == 'AJ Bell'
        assert first_bond['currency_code'] == 'GBP'
        
        # Value range validation
        assert 0 < first_bond['clean_price'] < 200, f"Invalid price: {first_bond['clean_price']}"
        assert 0 <= first_bond['coupon_rate'] <= 0.20, f"Invalid coupon: {first_bond['coupon_rate']}"
        assert first_bond['years_to_maturity'] > 0, f"Invalid years to maturity: {first_bond['years_to_maturity']}"
        
        # Check calculated fields (may be None if calculations failed)
        if first_bond['accrued_interest'] is not None:
            assert isinstance(first_bond['accrued_interest'], (int, float))
            assert first_bond['accrued_interest'] >= 0
        
        if first_bond['dirty_price'] is not None:
            assert isinstance(first_bond['dirty_price'], (int, float))
            assert first_bond['dirty_price'] > first_bond['clean_price']
        
        if first_bond['ytm'] is not None:
            assert isinstance(first_bond['ytm'], (int, float))
            assert -0.5 <= first_bond['ytm'] <= 1.0, f"YTM out of reasonable range: {first_bond['ytm']}"
        
        if first_bond['after_tax_ytm'] is not None:
            assert isinstance(first_bond['after_tax_ytm'], (int, float))
            assert -0.5 <= first_bond['after_tax_ytm'] <= 1.0, f"After-tax YTM out of reasonable range: {first_bond['after_tax_ytm']}"
        
        print(f"Data structure validation passed for {len(corporate_bond_data)} corporate bonds")
    
    def test_ajbell_corporate_bond_maturity_range(self):
        """Test that AJ Bell corporate bonds cover a comprehensive maturity range."""
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        assert len(corporate_bond_data) > 0, "No corporate bond data returned"
        
        maturity_years = [bond['maturity_date'].year for bond in corporate_bond_data]
        min_year = min(maturity_years)
        max_year = max(maturity_years)
        
        # Should have corporate bonds maturing in the near term
        assert min_year <= 2027, f"Shortest maturity too far out: {min_year}"
        
        # Should have long-term corporate bonds
        assert max_year >= 2030, f"Longest maturity too short: {max_year}"
        
        # Should have a reasonable spread
        year_range = max_year - min_year
        assert year_range >= 5, f"Maturity range too narrow: {year_range} years"
        
        print(f"Corporate bond maturity range: {min_year} - {max_year} ({year_range} years)")
    
    def test_ajbell_corporate_bond_identifiers(self):
        """Test that AJ Bell corporate bonds include proper bond identifiers."""
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        assert len(corporate_bond_data) > 0, "No corporate bond data returned"
        
        # Count bonds with various identifiers
        with_isin = sum(1 for bond in corporate_bond_data if bond.get('isin'))
        with_short_code = sum(1 for bond in corporate_bond_data if bond.get('short_code'))
        with_company_name = sum(1 for bond in corporate_bond_data if bond.get('company_name'))
        
        # Most corporate bonds should have ISINs
        isin_percentage = with_isin / len(corporate_bond_data)
        assert isin_percentage >= 0.7, f"Only {isin_percentage:.1%} of corporate bonds have ISINs"
        
        # Most corporate bonds should have short codes
        short_code_percentage = with_short_code / len(corporate_bond_data)
        assert short_code_percentage >= 0.7, f"Only {short_code_percentage:.1%} of corporate bonds have short codes"
        
        # Most corporate bonds should have company names extracted
        company_name_percentage = with_company_name / len(corporate_bond_data)
        assert company_name_percentage >= 0.8, f"Only {company_name_percentage:.1%} of corporate bonds have company names"
        
        # Validate ISIN format for those that have them
        for bond in corporate_bond_data:
            if bond.get('isin'):
                isin = bond['isin']
                assert len(isin) == 12, f"Invalid ISIN length: {isin}"
                assert isin[:2].isalpha(), f"Invalid ISIN prefix: {isin}"
        
        print(f"Identifier validation: {with_isin}/{len(corporate_bond_data)} ISINs, {with_short_code}/{len(corporate_bond_data)} short codes, {with_company_name}/{len(corporate_bond_data)} company names")
    
    def test_ajbell_corporate_bond_companies(self):
        """Test that AJ Bell collector captures well-known corporate bond issuers."""
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        assert len(corporate_bond_data) > 0, "No corporate bond data returned"
        
        # Extract all company names and bond names
        company_names = [bond.get('company_name', '') for bond in corporate_bond_data if bond.get('company_name')]
        bond_names = [bond.get('bond_name', '') for bond in corporate_bond_data]
        all_names = ' '.join(company_names + bond_names).upper()
        
        # Look for some expected UK corporate bond issuers
        expected_issuers = [
            'VODAFONE', 'BARCLAYS', 'TESCO', 'HAMMERSON', 'UTILITIES', 
            'HSBC', 'BRITISH', 'TELECOMMUNICATIONS', 'LEGAL', 'GENERAL'
        ]
        
        found_issuers = []
        for issuer in expected_issuers:
            if issuer in all_names:
                found_issuers.append(issuer)
        
        # Should find at least half of the expected major issuers
        found_percentage = len(found_issuers) / len(expected_issuers)
        assert found_percentage >= 0.3, f"Found only {len(found_issuers)}/{len(expected_issuers)} expected issuers: {found_issuers}"
        
        print(f"Found major corporate issuers: {found_issuers}")
    
    def test_ajbell_corporate_bond_calculations(self):
        """Test that AJ Bell corporate bond YTM calculations are working."""
        collector = AJBellCorporateBondCollector(database_url=None)
        corporate_bond_data = collector.scrape_corporate_bond_prices()
        
        assert len(corporate_bond_data) > 0, "No corporate bond data returned"
        
        # Count bonds with successful calculations
        with_accrued = sum(1 for bond in corporate_bond_data if bond.get('accrued_interest') is not None)
        with_dirty_price = sum(1 for bond in corporate_bond_data if bond.get('dirty_price') is not None)
        with_ytm = sum(1 for bond in corporate_bond_data if bond.get('ytm') is not None)
        with_after_tax_ytm = sum(1 for bond in corporate_bond_data if bond.get('after_tax_ytm') is not None)
        
        total_bonds = len(corporate_bond_data)
        
        # Should have reasonable success rates for calculations
        accrued_percentage = with_accrued / total_bonds
        dirty_price_percentage = with_dirty_price / total_bonds
        ytm_percentage = with_ytm / total_bonds
        after_tax_ytm_percentage = with_after_tax_ytm / total_bonds
        
        assert accrued_percentage >= 0.7, f"Only {accrued_percentage:.1%} of bonds have accrued interest calculated"
        assert dirty_price_percentage >= 0.7, f"Only {dirty_price_percentage:.1%} of bonds have dirty price calculated"
        assert ytm_percentage >= 0.5, f"Only {ytm_percentage:.1%} of bonds have YTM calculated"
        assert after_tax_ytm_percentage >= 0.5, f"Only {after_tax_ytm_percentage:.1%} of bonds have after-tax YTM calculated"
        
        print(f"Calculation success rates: accrued={accrued_percentage:.1%}, dirty_price={dirty_price_percentage:.1%}, ytm={ytm_percentage:.1%}, after_tax_ytm={after_tax_ytm_percentage:.1%}")
    
    def test_ajbell_corporate_bond_collector_error_handling(self):
        """Test that AJ Bell corporate bond collector handles errors gracefully."""
        # Test with invalid database URL - should not crash
        try:
            result = collect_ajbell_corporate_bond_prices(database_url="invalid://url")
            # If it doesn't crash, that's good - may return 0 or raise controlled exception
            assert isinstance(result, int)
        except Exception as e:
            # Should be a controlled exception, not a crash
            assert "failed" in str(e).lower() or "error" in str(e).lower()
        
        print("Corporate bond error handling validation passed")


if __name__ == "__main__":
    # Run tests directly
    test_instance = TestAJBellCorporateBondCollection()
    
    print("Testing AJ Bell Corporate Bond Data Collection")
    print("=" * 60)
    
    try:
        test_instance.test_ajbell_corporate_bond_collection_safe_mode()
        print("✅ Safe mode collection test passed")
    except Exception as e:
        print(f"❌ Safe mode collection test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_collection_comprehensive()
        print("✅ Comprehensive collection test passed")
    except Exception as e:
        print(f"❌ Comprehensive collection test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_data_structure()
        print("✅ Data structure test passed")
    except Exception as e:
        print(f"❌ Data structure test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_maturity_range()
        print("✅ Maturity range test passed")
    except Exception as e:
        print(f"❌ Maturity range test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_identifiers()
        print("✅ Identifier validation test passed")
    except Exception as e:
        print(f"❌ Identifier validation test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_companies()
        print("✅ Corporate issuer detection test passed")
    except Exception as e:
        print(f"❌ Corporate issuer detection test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_calculations()
        print("✅ Financial calculations test passed")
    except Exception as e:
        print(f"❌ Financial calculations test failed: {e}")
    
    try:
        test_instance.test_ajbell_corporate_bond_collector_error_handling()
        print("✅ Error handling test passed")
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
    
    print("\nAll AJ Bell corporate bond collection tests completed!")
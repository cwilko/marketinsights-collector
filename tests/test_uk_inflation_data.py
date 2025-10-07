"""
Tests for UK Inflation Data Collector

Tests the UKInflationCollector class that processes ONS MM23 CSV data
and populates the uk_inflation database tables with CPI, CPIH, and RPI data.
"""

import pytest
import pandas as pd
import os

from data_collectors.uk_inflation_data import UKInflationCollector, collect_uk_inflation_data


class TestUKInflationDataIntegration:
    """Integration tests using the actual MM23.csv file."""
    
    @pytest.mark.skipif(
        not os.path.exists('/Users/cwilkin/Documents/Development/repos/econometrics/mm23.csv'),
        reason="MM23.csv file not available"
    )
    def test_uk_inflation_data_collection(self):
        """Test complete UK inflation data collection validates CPI, CPIH, and RPI output."""
        # Use existing local file for testing to avoid downloading during tests
        csv_path = '/Users/cwilkin/Documents/Development/repos/econometrics/mm23.csv'
        
        collector = UKInflationCollector(database_url=None)
        
        # For testing, we'll manually load the local file and test the parsing logic
        # Read the file and validate series detection
        df = pd.read_csv(csv_path, header=None, low_memory=False)
        columns = collector.find_series_columns(df)
        
        # Validate CPI series
        assert 'CPI' in columns
        assert 'indices' in columns['CPI']
        assert 'weights' in columns['CPI']
        assert '00' in columns['CPI']['indices']  # Headline CPI
        assert columns['CPI']['indices']['00'] is not None
        assert '00' in columns['CPI']['weights']  # CPI weights
        assert columns['CPI']['weights']['00'] is not None
        
        # Should find all 13 Level 1 CPI categories (00-12)
        level1_cpi_categories = [str(i).zfill(2) for i in range(13)]
        found_cpi_level1 = [cat for cat in level1_cpi_categories if cat in columns['CPI']['indices']]
        assert len(found_cpi_level1) == 13, f"Expected 13 Level 1 CPI categories, found {len(found_cpi_level1)}"
        
        # Validate CPIH series
        assert 'CPIH' in columns
        assert 'indices' in columns['CPIH']
        assert 'weights' in columns['CPIH']
        assert '00' in columns['CPIH']['indices']  # Headline CPIH
        assert columns['CPIH']['indices']['00'] is not None
        assert '00' in columns['CPIH']['weights']  # CPIH weights
        assert columns['CPIH']['weights']['00'] is not None
        
        # Should find all 13 Level 1 CPIH categories (00-12)
        found_cpih_level1 = [cat for cat in level1_cpi_categories if cat in columns['CPIH']['indices']]
        assert len(found_cpih_level1) == 13, f"Expected 13 Level 1 CPIH categories, found {len(found_cpih_level1)}"
        
        # Validate RPI series
        assert 'RPI' in columns
        assert 'indices' in columns['RPI']
        assert '00' in columns['RPI']['indices']  # RPI All Items
        assert columns['RPI']['indices']['00'] is not None
        
        # Validate COICOP hierarchy extraction
        all_coicop_codes = collector.extract_all_coicop_codes(df)
        assert len(all_coicop_codes) == 318, f"Expected exactly 318 COICOP categories (316 CPI + 2 CPIH-only), found {len(all_coicop_codes)}"
        
        # Validate hierarchy levels
        level_counts = {}
        for code, level in all_coicop_codes.items():
            level_counts[level] = level_counts.get(level, 0) + 1
        
        assert level_counts[1] == 13, f"Expected 13 Level 1 categories, found {level_counts[1]}"
        assert level_counts[2] == 42, f"Expected 42 Level 2 categories (40 base + 2 CPIH housing), found {level_counts[2]}"
        assert level_counts[3] == 71, f"Expected 71 Level 3 categories, found {level_counts[3]}"
        assert level_counts[4] == 192, f"Expected 192 Level 4 categories, found {level_counts[4]}"
        
        # Note: We don't test the full collection with download in unit tests to avoid network dependency
        # The collection logic is validated through the individual component tests above


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
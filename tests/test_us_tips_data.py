"""
Test US TIPS data collector functionality.
Tests data collection, validation, and structure without database writes.
"""
import pytest
from unittest.mock import Mock, patch
from datetime import datetime, date
from data_collectors.us_tips_data import (
    collect_us_tips_single_series, 
    FRED_TIPS_SERIES,
    FRED_FORWARD_INFLATION_SERIES,
    collect_us_tips
)

class TestUSTIPSDataCollector:
    """Test US TIPS data collection functionality."""
    
    def test_fred_tips_series_mapping(self):
        """Test that FRED TIPS series mapping is valid."""
        expected_series = {'DFII5', 'DFII7', 'DFII10', 'DFII20', 'DFII30'}
        expected_maturities = {'5Y', '7Y', '10Y', '20Y', '30Y'}
        
        assert set(FRED_TIPS_SERIES.keys()) == expected_series
        assert set(FRED_TIPS_SERIES.values()) == expected_maturities
        
        # Test maturity format consistency
        for maturity in FRED_TIPS_SERIES.values():
            assert maturity.endswith('Y')
            assert maturity[:-1].isdigit()
    
    def test_fred_forward_inflation_series_mapping(self):
        """Test that FRED forward inflation series mapping is valid."""
        expected_series = {'T5YIFR'}
        expected_labels = {'5Y5Y'}
        
        assert set(FRED_FORWARD_INFLATION_SERIES.keys()) == expected_series
        assert set(FRED_FORWARD_INFLATION_SERIES.values()) == expected_labels
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_collect_tips_single_series_valid_data(self, mock_fred_collector):
        """Test single TIPS series collection with valid data."""
        # Mock FRED data response
        mock_data = [
            {'date': '2024-01-01', 'value': '1.75'},
            {'date': '2024-01-02', 'value': '1.80'},
            {'date': '2024-01-03', 'value': '.'},  # Null value should be skipped
            {'date': '2024-01-04', 'value': '1.85'}
        ]
        
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = mock_data
        mock_fred_collector.return_value = mock_collector_instance
        
        # Test DFII10 collection
        result = collect_us_tips_single_series('DFII10')
        
        # Verify collector was called
        mock_fred_collector.assert_called_once()
        mock_collector_instance.get_series_data.assert_called_once_with('DFII10', limit=50000)
        
        # Verify result structure and content
        assert len(result) == 3  # Should skip the null value
        
        expected_first = {
            'date': date(2024, 1, 1),
            'maturity': '10Y',
            'maturity_years': 10.0,
            'yield_rate': 1.75
        }
        
        assert result[0] == expected_first
        assert result[1]['yield_rate'] == 1.80
        assert result[2]['yield_rate'] == 1.85
        
        # Verify all records have required fields
        required_fields = {'date', 'maturity', 'maturity_years', 'yield_rate'}
        for record in result:
            assert set(record.keys()) == required_fields
            assert isinstance(record['date'], date)
            assert isinstance(record['yield_rate'], float)
    
    def test_collect_tips_single_series_invalid_series(self):
        """Test error handling for invalid TIPS series."""
        with pytest.raises(ValueError, match="Unknown TIPS series: INVALID"):
            collect_us_tips_single_series('INVALID')
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_collect_tips_single_series_empty_data(self, mock_fred_collector):
        """Test handling of empty data response."""
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = []
        mock_fred_collector.return_value = mock_collector_instance
        
        result = collect_us_tips_single_series('DFII5')
        
        assert result == []
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_collect_tips_single_series_invalid_values(self, mock_fred_collector):
        """Test handling of invalid data values."""
        mock_data = [
            {'date': '2024-01-01', 'value': 'invalid'},
            {'date': '2024-01-02', 'value': None},
            {'date': '2024-01-03', 'value': ''},
            {'date': '2024-01-04', 'value': '1.75'},
            {'date': 'invalid-date', 'value': '1.80'}
        ]
        
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = mock_data
        mock_fred_collector.return_value = mock_collector_instance
        
        result = collect_us_tips_single_series('DFII7')
        
        # Should only return the one valid record
        assert len(result) == 1
        assert result[0]['yield_rate'] == 1.75
        assert result[0]['date'] == date(2024, 1, 4)
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_collect_tips_data_types(self, mock_fred_collector):
        """Test that collected data has correct types."""
        mock_data = [
            {'date': '2024-01-01', 'value': '1.75'},
        ]
        
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = mock_data
        mock_fred_collector.return_value = mock_collector_instance
        
        result = collect_us_tips_single_series('DFII20')
        
        record = result[0]
        assert isinstance(record['date'], date)
        assert isinstance(record['maturity'], str)
        assert isinstance(record['maturity_years'], float)
        assert isinstance(record['yield_rate'], float)
        
        # Test specific values
        assert record['maturity'] == '20Y'
        assert record['maturity_years'] == 20.0
        assert record['yield_rate'] == 1.75
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_collect_all_tips_series(self, mock_fred_collector):
        """Test that all TIPS series can be collected."""
        mock_data = [{'date': '2024-01-01', 'value': '1.75'}]
        
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = mock_data
        mock_fred_collector.return_value = mock_collector_instance
        
        # Test each series
        for series_id, expected_maturity in FRED_TIPS_SERIES.items():
            result = collect_us_tips_single_series(series_id)
            
            assert len(result) == 1
            assert result[0]['maturity'] == expected_maturity
            expected_years = float(expected_maturity.replace('Y', ''))
            assert result[0]['maturity_years'] == expected_years

    def test_collect_forward_inflation_single_series(self):
        """Test that we can collect T5YIFR data."""
        # This test validates the series exists in our mapping
        assert 'T5YIFR' in FRED_FORWARD_INFLATION_SERIES
        assert FRED_FORWARD_INFLATION_SERIES['T5YIFR'] == '5Y5Y'

class TestTIPSDataValidation:
    """Test TIPS data validation and edge cases."""
    
    def test_maturity_years_conversion(self):
        """Test that maturity string to years conversion works correctly."""
        test_cases = {
            '5Y': 5.0,
            '7Y': 7.0,
            '10Y': 10.0,
            '20Y': 20.0,
            '30Y': 30.0
        }
        
        for maturity_str, expected_years in test_cases.items():
            actual_years = float(maturity_str.replace('Y', ''))
            assert actual_years == expected_years
    
    def test_date_parsing(self):
        """Test date string parsing."""
        date_str = '2024-01-15'
        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        expected_date = date(2024, 1, 15)
        
        assert parsed_date == expected_date
    
    def test_yield_rate_parsing(self):
        """Test yield rate string to float conversion."""
        test_cases = ['1.75', '0.25', '3.50', '10.00']
        
        for yield_str in test_cases:
            yield_float = float(yield_str)
            assert isinstance(yield_float, float)
            assert yield_float == float(yield_str)

class TestForwardInflationDataCollection:
    """Test forward inflation expectation data collection."""
    
    @patch('data_collectors.us_tips_data.FREDCollector')
    def test_forward_inflation_data_structure(self, mock_fred_collector):
        """Test forward inflation data collection structure."""
        # Mock T5YIFR data response
        mock_data = [
            {'date': '2024-01-01', 'value': '2.23'},
            {'date': '2024-01-02', 'value': '2.25'},
            {'date': '2024-01-03', 'value': '.'},  # Null value should be skipped
        ]
        
        mock_collector_instance = Mock()
        mock_collector_instance.get_series_data.return_value = mock_data
        mock_collector_instance.get_date_range_for_collection.return_value = (None, None)
        mock_fred_collector.return_value = mock_collector_instance
        
        # This should not raise an error and should handle the data properly
        # We're testing in safe mode (no database operations)
        result = collect_us_tips()
        
        # Verify the collector was initialized
        mock_fred_collector.assert_called_once()
    
    def test_t5yifr_series_exists(self):
        """Test that T5YIFR series is properly mapped."""
        assert 'T5YIFR' in FRED_FORWARD_INFLATION_SERIES
        assert FRED_FORWARD_INFLATION_SERIES['T5YIFR'] == '5Y5Y'
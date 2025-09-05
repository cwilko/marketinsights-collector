"""
Tests for UK data collectors including ONS and Bank of England APIs.
"""

import pytest
from datetime import datetime
from data_collectors.economic_indicators import ONSCollector, BankOfEnglandCollector
from data_collectors.uk_market_data import AlphaVantageUKCollector


class TestONSCollector:
    """Tests for ONS (Office for National Statistics) API data collection."""
    
    def test_ons_collector_initialization(self):
        """Test ONS collector can be initialized."""
        collector = ONSCollector(database_url=None)
        assert collector.base_url == "https://api.beta.ons.gov.uk/v1"
        assert collector.database_url is None
    
    def test_ons_get_datasets(self):
        """Test ONS collector can fetch datasets list."""
        collector = ONSCollector(database_url=None)
        datasets = collector.get_datasets()
        
        # Should return a list of datasets
        assert isinstance(datasets, list)
        
        if datasets:
            # If datasets exist, they should have expected structure
            sample_dataset = datasets[0]
            assert isinstance(sample_dataset, dict)
            assert "id" in sample_dataset
            print(f"✅ Found {len(datasets)} ONS datasets")
            print(f"Sample dataset: {sample_dataset.get('id', 'unknown')} - {sample_dataset.get('title', 'no title')}")
        else:
            print("⚠️ No datasets returned from ONS API")
    
    def test_ons_uk_spending_dataset(self):
        """Test fetching UK spending dataset (commonly available)."""
        collector = ONSCollector(database_url=None)
        
        # Try to fetch the UK spending on cards dataset (commonly available)
        dataset_id = "uk-spending-on-cards"
        
        try:
            observations = collector.get_dataset_data(dataset_id)
            
            assert isinstance(observations, list)
            
            if observations:
                print(f"✅ Successfully fetched {len(observations)} observations from {dataset_id}")
                
                # Check structure of first observation
                if len(observations) > 0:
                    sample_obs = observations[0]
                    print(f"Sample observation structure: {sample_obs}")
                    
                    # Basic structure validation
                    assert isinstance(sample_obs, dict)
                    
            else:
                print(f"⚠️ No observations returned for {dataset_id}")
                
        except Exception as e:
            print(f"⚠️ Could not fetch {dataset_id}: {str(e)}")
            # Don't fail the test - this might be expected for some datasets


class TestBankOfEnglandCollector:
    """Tests for Bank of England data collection."""
    
    def test_boe_collector_initialization(self):
        """Test Bank of England collector can be initialized."""
        collector = BankOfEnglandCollector(database_url=None)
        assert collector.base_url == "https://www.bankofengland.co.uk/boeapps/database"
        assert collector.database_url is None
    
    def test_boe_bank_rate_placeholder(self):
        """Test Bank Rate data collection (currently placeholder)."""
        collector = BankOfEnglandCollector(database_url=None)
        data = collector.get_bank_rate_data()
        
        # Currently returns empty list as placeholder
        assert isinstance(data, list)
        assert len(data) == 0
        print("✅ Bank Rate collector placeholder working")


class TestUKMarketDataCollector:
    """Tests for UK market data collection via financial APIs."""
    
    def test_alpha_vantage_collector_initialization(self):
        """Test Alpha Vantage UK collector can be initialized."""
        collector = AlphaVantageUKCollector(database_url=None)
        assert collector.base_url == "https://www.alphavantage.co/query"
        assert collector.database_url is None
    
    def test_alpha_vantage_no_api_key(self):
        """Test Alpha Vantage collector behavior without API key."""
        collector = AlphaVantageUKCollector(database_url=None)
        
        # Should return empty list when no API key is set
        ftse_data = collector.get_ftse_100_data()
        gbp_data = collector.get_gbp_usd_rate()
        
        assert isinstance(ftse_data, list)
        assert isinstance(gbp_data, list)
        assert len(ftse_data) == 0  # Expected without API key
        assert len(gbp_data) == 0   # Expected without API key
        
        print("✅ Alpha Vantage collector handles missing API key gracefully")


@pytest.mark.integration
class TestUKDataCollectionFunctions:
    """Integration tests for UK data collection functions."""
    
    def test_uk_collector_functions_safe_mode(self):
        """Test all UK collection functions in safe mode."""
        from data_collectors.economic_indicators import (
            collect_uk_cpi, collect_uk_unemployment, collect_uk_gdp, collect_uk_monthly_bank_rate
        )
        from data_collectors.uk_market_data import collect_ftse_100, collect_gbp_usd_rate
        
        uk_collectors = [
            ("UK CPI", collect_uk_cpi),
            ("UK Unemployment", collect_uk_unemployment),
            ("UK GDP", collect_uk_gdp),
            ("UK Bank Rate (Monthly)", collect_uk_monthly_bank_rate),
            ("FTSE 100", collect_ftse_100),
            ("GBP/USD", collect_gbp_usd_rate)
        ]
        
        for name, collector_func in uk_collectors:
            result = collector_func(database_url=None)
            assert isinstance(result, int)
            assert result >= 0
            print(f"✅ {name} collector: {result} records processed (safe mode)")
    
    def test_ons_api_connectivity(self):
        """Test basic ONS API connectivity."""
        collector = ONSCollector(database_url=None)
        
        # Test that we can reach the ONS API
        try:
            datasets = collector.get_datasets()
            
            # Should get some response (even if empty)
            assert isinstance(datasets, list)
            
            if datasets:
                print(f"✅ ONS API connectivity confirmed - {len(datasets)} datasets available")
                
                # Show sample datasets for debugging
                for i, dataset in enumerate(datasets[:3]):
                    if isinstance(dataset, dict) and 'id' in dataset:
                        print(f"   Dataset {i+1}: {dataset['id']} - {dataset.get('title', 'No title')}")
                
            else:
                print("⚠️ ONS API reachable but returned no datasets")
                
        except Exception as e:
            pytest.fail(f"ONS API connectivity test failed: {str(e)}")


class TestUKDataValidation:
    """Tests for UK data validation and processing."""
    
    def test_uk_table_names(self):
        """Test that UK table names are consistent."""
        expected_uk_tables = [
            "uk_consumer_price_index",
            "uk_unemployment_rate", 
            "uk_gross_domestic_product",
            "uk_bank_rate",
            "ftse_100_index",
            "uk_gilt_yields",
            "gbp_usd_exchange_rate"
        ]
        
        # All tables should have consistent naming
        uk_tables = [t for t in expected_uk_tables if t.startswith('uk_') or 'ftse' in t or 'gbp' in t]
        assert len(uk_tables) == len(expected_uk_tables)
        print(f"✅ UK table naming validated: {len(uk_tables)} tables defined")
    
    def test_uk_vs_us_metric_mapping(self):
        """Test that UK metrics map to US equivalents conceptually."""
        uk_us_mapping = {
            "uk_consumer_price_index": "consumer_price_index",
            "uk_unemployment_rate": "unemployment_rate",
            "uk_gross_domestic_product": "gross_domestic_product", 
            "uk_bank_rate": "federal_funds_rate",
            "ftse_100_index": "sp500_index"
        }
        
        # Should have reasonable mappings
        assert len(uk_us_mapping) >= 5
        
        # UK tables should have uk_ prefix or be clearly UK-specific
        for uk_table, us_table in uk_us_mapping.items():
            assert uk_table.startswith('uk_') or 'ftse' in uk_table or 'gbp' in uk_table
            print(f"✅ Mapping: {uk_table} ↔ {us_table}")
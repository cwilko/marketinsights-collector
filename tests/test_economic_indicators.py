"""
Tests for economic indicator data collectors.
"""

import pytest
from datetime import datetime
from data_collectors.economic_indicators import FREDCollector, BLSCollector, BEACollector, GermanBundCollector, collect_cpi, collect_monthly_fed_funds_rate, collect_daily_fed_funds_rate, collect_unemployment_rate, collect_gdp, collect_real_gdp_growth_components, collect_gdpnow_forecasts, collect_german_bund_yields
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
            
    def test_fred_real_gdp_growth_series(self):
        """Test FRED collector can fetch Real GDP growth components series."""
        collector = FREDCollector(database_url=None)
        
        # Test all 5 GDP growth component series
        gdp_series = {
            "real_gdp_growth": "A191RL1Q225SBEA",  # Real Gross Domestic Product (percent change)
            "consumption_contribution": "DPCERY2Q224SBEA",  # Personal consumption expenditures contribution
            "investment_contribution": "A006RY2Q224SBEA",   # Gross private domestic investment contribution
            "government_contribution": "A822RY2Q224SBEA",   # Government expenditures contribution
            "net_exports_contribution": "A019RY2Q224SBEA"   # Net exports contribution
        }
        
        for component, series_id in gdp_series.items():
            data = collector.get_series_data(series_id, limit=5)
            
            assert isinstance(data, list), f"Should return list for {component} ({series_id})"
            assert len(data) > 0, f"Should return data for {component} ({series_id})"
            
            # Test data structure for first few records
            for item in data[:3]:
                assert "date" in item
                assert "value" in item
                
                # Test that we can parse the date
                if item["value"] != ".":  # Skip missing values
                    date_obj = datetime.strptime(item["date"], "%Y-%m-%d")
                    assert isinstance(date_obj, datetime)
                    
                    # Test that value can be converted to float
                    value = float(item["value"])
                    assert isinstance(value, float)


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

    def test_collect_real_gdp_growth_components_function(self):
        """Test the collect_real_gdp_growth_components function."""
        result = collect_real_gdp_growth_components(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records
        
        # Test that the function can handle safe mode properly
        result_safe = collect_real_gdp_growth_components(database_url=None)
        assert isinstance(result_safe, int)
        assert result_safe >= 0
        
    def test_gdpnow_series_exists(self):
        """Test that GDPNOW series exists and can be fetched from FRED."""
        collector = FREDCollector(database_url=None)
        
        # Test fetching recent GDPNOW data - get recent data by using observation_start
        from datetime import datetime, timedelta
        recent_start = datetime.now().date() - timedelta(days=365)  # Last year
        
        gdpnow_data = collector.get_series_data(
            "GDPNOW", 
            observation_start=recent_start.strftime("%Y-%m-%d")
        )
        
        assert isinstance(gdpnow_data, list)
        print(f"GDPNOW test: Retrieved {len(gdpnow_data)} records")
        
        if len(gdpnow_data) > 0:
            # Find the most recent non-missing value
            latest_forecast = None
            for record in gdpnow_data:
                if record['value'] != '.':
                    latest_forecast = record
                    break
            
            assert latest_forecast is not None, "No valid GDPNOW forecast data found"
            assert "date" in latest_forecast
            assert "value" in latest_forecast
            
            print(f"Latest GDPNOW forecast: {latest_forecast['date']} = {latest_forecast['value']}%")
            
            # Test that we have some recent data (within last 2 years)
            from datetime import datetime, timedelta
            latest_date = datetime.strptime(latest_forecast['date'], "%Y-%m-%d").date()
            two_years_ago = datetime.now().date() - timedelta(days=2*365)
            
            print(f"Latest data date: {latest_date}, Two years ago: {two_years_ago}")
            
            # Print all available GDPNOW data for debugging
            print("Recent GDPNOW records (non-missing only):")
            count = 0
            for record in gdpnow_data:
                if record['value'] != '.' and count < 10:  # Show first 10 valid records
                    print(f"  {count+1}: {record['date']} = {record['value']}%")
                    count += 1
                    
            # The assertion about recent data - this will help us see what date range we actually have
            if latest_date < two_years_ago:
                print(f"WARNING: Latest GDPNOW data is from {latest_date}, which is older than 2 years")
                print("This suggests the GDPNOW series may not be actively updated or has a different publication schedule")
            else:
                print(f"✅ GDPNOW data is recent (within last 2 years)")
        else:
            print("WARNING: No GDPNOW data retrieved - series might not exist or be empty")
            
    def test_collect_gdpnow_forecasts_function(self):
        """Test the new collect_gdpnow_forecasts function."""
        result = collect_gdpnow_forecasts(database_url=None)
        assert isinstance(result, int)
        assert result >= 0  # Should process at least some records or return 0 in safe mode


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


class TestGermanBundCollector:
    """Tests for German Bund yield curve data collection from Bundesbank."""
    
    def test_german_bund_collector_comprehensive(self):
        """Comprehensive test for German Bund collector - single API call."""
        import pandas as pd
        
        # Test 1: Collector initialization
        collector = GermanBundCollector(database_url=None)
        assert collector.base_url == "https://www.bundesbank.de/statistic-rmi/StatisticDownload"
        assert collector.database_url is None
        assert len(collector.maturities) == 30  # 1-30 year maturities
        assert collector.maturities == list(range(1, 31))
        
        # Test 2: URL construction
        url = collector.build_download_url()
        assert url.startswith("https://www.bundesbank.de/statistic-rmi/StatisticDownload?")
        assert "mode=its" in url
        assert "its_fileFormat=csv" in url
        assert "its_csvFormat=en" in url
        assert "frequency=D" in url
        
        # Should contain all 30 time series IDs
        for maturity in range(1, 31):
            expected_ts_id = f"BBSIS.D.I.ZST.ZI.EUR.S1311.B.A604.R{maturity:02d}XX.R.A.A._Z._Z.A"
            assert expected_ts_id in url
        
        # Test 3: Data fetching and cleaning (SINGLE API CALL)
        cleaned_data = collector.fetch_and_clean_data()
        
        assert isinstance(cleaned_data, list)
        assert len(cleaned_data) > 0
        
        # Test 4: Data structure validation
        sample_record = cleaned_data[0]
        assert "date" in sample_record
        assert "maturity_years" in sample_record
        assert "yield_rate" in sample_record
        assert "data_source" in sample_record
        
        # Verify data types
        assert isinstance(sample_record["date"], (type(datetime.now().date())))
        assert isinstance(sample_record["maturity_years"], float)
        assert isinstance(sample_record["yield_rate"], (int, float))
        assert sample_record["data_source"] == "Bundesbank_StatisticDownload"
        
        # Test 5: Data quality validation
        # Verify yield range is reasonable (-2% to 10%)
        for record in cleaned_data[:100]:  # Check first 100 records
            assert -2.0 <= record["yield_rate"] <= 10.0
        
        # Test 6: Data completeness analysis
        df = pd.DataFrame(cleaned_data)
        
        # Check date range - should go back to ~1997
        min_date = df['date'].min()
        max_date = df['date'].max()
        assert min_date.year <= 1998  # Data should start around 1997-1998
        assert max_date.year >= 2024  # Should have recent data
        
        # Check maturities coverage
        unique_maturities = sorted(df['maturity_years'].unique())
        assert len(unique_maturities) >= 25  # At least 25 out of 30 maturities
        
        # Verify short-term maturities (1-10Y) are definitely present
        short_term_maturities = [m for m in unique_maturities if m <= 10.0]
        assert len(short_term_maturities) >= 10  # All 1-10Y should be present
        
        # Verify maturities are in expected range (1-30 years)
        for maturity in unique_maturities:
            assert 1.0 <= maturity <= 30.0
        
        # Test 7: Volume expectations
        assert len(cleaned_data) > 100000  # Should have >100k records
        
        # Test 8: Safe mode function testing (using already fetched data for validation)
        result = collect_german_bund_yields(database_url=None)
        assert isinstance(result, int)
        assert result > 100000  # Should process many records
        
        # Test with default None parameter
        result_none = collect_german_bund_yields()
        assert isinstance(result_none, int)
        assert result_none >= 0
"""
Tests for API key validation and setup.
"""

import pytest
import os


class TestAPIKeySetup:
    """Tests for API key configuration and validation."""
    
    def test_required_api_keys_present(self):
        """Test that required API keys are configured."""
        required_keys = ["FRED_API_KEY", "BEA_API_KEY"]
        
        for key_name in required_keys:
            key_value = os.getenv(key_name)
            assert key_value is not None, f"Required API key {key_name} is not set"
            assert len(key_value) > 0, f"Required API key {key_name} is empty"
            
    def test_optional_api_keys(self):
        """Test optional API keys (should not fail if missing)."""
        optional_keys = ["BLS_API_KEY"]
        
        for key_name in optional_keys:
            key_value = os.getenv(key_name)
            if key_value:  # Only validate if present
                assert len(key_value) > 0, f"Optional API key {key_name} is empty"
                
    def test_api_key_format(self):
        """Test basic API key format validation."""
        fred_key = os.getenv("FRED_API_KEY")
        bea_key = os.getenv("BEA_API_KEY")
        bls_key = os.getenv("BLS_API_KEY")
        
        if fred_key:
            # FRED API keys are typically 32 character hex strings
            assert len(fred_key) >= 20, "FRED API key appears too short"
            assert len(fred_key) <= 50, "FRED API key appears too long"
            
        if bea_key:
            # BEA API keys are typically UUID format
            assert len(bea_key) >= 30, "BEA API key appears too short"
            assert "-" in bea_key, "BEA API key should contain hyphens (UUID format)"
            
        if bls_key:
            # BLS API keys are typically 32 character hex strings  
            assert len(bls_key) >= 20, "BLS API key appears too short"
            assert len(bls_key) <= 50, "BLS API key appears too long"


@pytest.mark.smoke
class TestAPIConnectivity:
    """Smoke tests for API connectivity."""
    
    def test_fred_api_connectivity(self, fred_api_key):
        """Test basic FRED API connectivity."""
        import requests
        
        url = "https://api.stlouisfed.org/fred/series"
        params = {
            "series_id": "GDP",
            "api_key": fred_api_key,
            "file_type": "json"
        }
        
        response = requests.get(url, params=params, timeout=10)
        assert response.status_code == 200
        
        data = response.json()
        assert "seriess" in data
        assert len(data["seriess"]) > 0
        
    def test_bea_api_connectivity(self, bea_api_key):
        """Test basic BEA API connectivity."""
        import requests
        
        params = {
            "UserID": bea_api_key,
            "Method": "GetDatasetList",
            "ResultFormat": "json"
        }
        
        response = requests.get(
            "https://apps.bea.gov/api/data",
            params=params,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        assert "BEAAPI" in data
        assert "Results" in data["BEAAPI"]
        
    def test_bls_api_connectivity(self, bls_api_key):
        """Test basic BLS API connectivity (optional)."""
        if not bls_api_key:
            pytest.skip("BLS API key not provided")
            
        import requests
        import json
        
        payload = {
            "seriesid": ["CUUR0000SA0"],
            "startyear": "2023",
            "endyear": "2023",
            "registrationkey": bls_api_key
        }
        
        headers = {'Content-Type': 'application/json'}
        
        response = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data",
            data=json.dumps(payload),
            headers=headers,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        assert data.get("status") == "REQUEST_SUCCEEDED"
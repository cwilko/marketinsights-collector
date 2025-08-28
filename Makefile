.PHONY: test test-smoke test-integration test-coverage clean install

# Install dependencies
install:
	pip install -r requirements.txt

# Run all tests
test:
	pytest

# Run quick smoke tests
test-smoke:
	pytest -m smoke

# Run integration tests  
test-integration:
	pytest -m integration

# Run tests with coverage
test-coverage:
	pytest --cov=data_collectors --cov-report=html --cov-report=term

# Run specific test file
test-economic:
	pytest tests/test_economic_indicators.py

test-market:
	pytest tests/test_market_data.py

test-keys:
	pytest tests/test_api_keys.py

test-db:
	pytest tests/test_database.py

# Run tests in parallel (if you have pytest-xdist installed)
test-parallel:
	pytest -n auto


# Clean up
clean:
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf __pycache__
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +

# Lint and format (if you add these tools later)
lint:
	flake8 data_collectors tests
	black --check data_collectors tests

format:
	black data_collectors tests
	isort data_collectors tests
# Integration Testing Guide

This project includes integration tests that validate UDFs against the real Olostep API.

## Setup

1. Get your Olostep API key from https://www.olostep.com/dashboard

2. Set the environment variable:
   ```bash
   export OLOSTEP_API_KEY="your-key-here"
   ```

   Or create a `.env` file:
   ```bash
   cp .env.example .env
   # Edit .env and add your API key
   ```

3. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

## Running Tests

### Unit tests only (no API key needed):
```bash
# Using the provided script (recommended)
./run_tests.sh tests/test_spark.py -v

# Or manually (sets Python environment)
PYSPARK_PYTHON=$(pwd)/venv/bin/python \
PYSPARK_DRIVER_PYTHON=$(pwd)/venv/bin/python \
pytest tests/test_spark.py -v
```

### Integration tests (requires API key):
```bash
export OLOSTEP_API_KEY="your-key-here"

# Using the provided script (recommended)
./run_tests.sh tests/test_integration.py -m integration -v

# Or manually
PYSPARK_PYTHON=$(pwd)/venv/bin/python \
PYSPARK_DRIVER_PYTHON=$(pwd)/venv/bin/python \
pytest tests/test_integration.py -m integration -v
```

### All tests:
```bash
./run_tests.sh tests/ -v
```

## What the Tests Do

**test_spark.py** - Unit tests with mocked API responses:
- `test_raises_without_api_key`: Validates error handling
- `test_registers_with_explicit_key`: Validates UDF registration
- `test_enrich_handles_error`: Validates error response parsing
- `test_search_empty_query`: Validates edge case handling

**test_integration.py** - Integration tests with real API:
- `test_enrich_with_real_api`: Enriches real company names (Google, Apple)
- `test_search_with_real_api`: Searches for real queries (Google CEO, Apple stock price)

## Troubleshooting

### Python version mismatch error
If you see: `[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.13 than that in driver: 3.11`

**Solution:** Use the `run_tests.sh` script which sets the correct Python paths:
```bash
./run_tests.sh tests/ -v
```

Or set environment variables manually:
```bash
export PYSPARK_PYTHON=$(pwd)/venv/bin/python
export PYSPARK_DRIVER_PYTHON=$(pwd)/venv/bin/python
pytest tests/ -v
```

See [PYTHON_VERSION_FIX.md](PYTHON_VERSION_FIX.md) for details.

### PySpark not found
Install the optional spark dependencies:
```bash
pip install -e ".[dev]"
```

### Java errors
Ensure Java 17+ is installed:
```bash
java -version  # Should show Java 17 or higher
```

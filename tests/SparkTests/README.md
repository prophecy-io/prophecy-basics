# Spark Tests

This directory contains unit tests for PySpark implementations of the gem files.

## Setup

1. Activate the virtual environment:
   ```bash
   source venv/bin/activate
   ```

2. Install test dependencies:
   ```bash
   pip install -r tests/SparkTests/requirements.txt
   ```

   Or install manually:
   ```bash
   pip install pytest pyspark
   ```

3. **Important**: The gem classes have framework dependencies that are typically available in the Prophecy environment. For testing, we directly instantiate gem classes and set their `props` attribute. If you encounter import errors when running tests, ensure all Prophecy framework dependencies are available, or tests may need to be run in the Prophecy environment.

## Running Tests

### Run all tests in this directory:
```bash
pytest tests/SparkTests/
```

### Run specific test file:
```bash
pytest tests/SparkTests/test_count_records.py
```

### Run with verbose output:
```bash
pytest tests/SparkTests/test_count_records.py -v
```

### Run specific test case:
```bash
pytest tests/SparkTests/test_count_records.py::test_count_records_normal_case
```

### Run with coverage:
```bash
pytest tests/SparkTests/test_count_records.py --cov=gems.CountRecords --cov-report=html
```

### Run tests and show print statements:
```bash
pytest tests/SparkTests/test_count_records.py -v -s
```

## Test Structure

Each test file follows this structure:
- **Fixtures**: Reusable test data and SparkSession
- **Test Functions**: Individual test cases with descriptive names
- **Helper Functions**: Utility functions for test setup

## Test Coverage

Currently implemented:
- ✅ `test_count_records.py` - Tests for CountRecords gem

Future tests:
- ⏳ DataCleansing tests
- ⏳ DataEncoderDecoder tests
- ⏳ DataMasking tests
- ⏳ And other gem files...

## Best Practices

- Each test should be independent and not rely on other tests
- Use descriptive test names that explain what is being tested
- Include docstrings for each test function
- Create realistic sample data for testing
- Test both happy path and edge cases
- Ensure tests clean up resources properly (fixtures handle this)


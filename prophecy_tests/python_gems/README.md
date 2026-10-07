# Python Gem Tests

This directory contains tests for the Python gem implementations (the `applyPython` DataFrame runtime in `gems/*.py`).

## 🚀 How to Run Tests Locally

### Prerequisites
- Python 3.11+
- Java 11+ (required for PySpark)

### Quick Start (Using Existing Virtual Environment)

If you have the project's virtual environment already set up:

```bash
# From project root
cd /path/to/prophecy-basics

# Activate the virtual environment
source venv/bin/activate

# Navigate to test directory
cd prophecy_tests/python_gems

# Set PYTHONPATH to include gems directory
export PYTHONPATH=../../gems:$PYTHONPATH

# Run all tests
pytest -v
```

### Step-by-Step Setup (New Environment)

If you need to create a fresh environment:

```bash
# From project root
cd prophecy_tests/python_gems

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# OR
venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt

# Set PYTHONPATH to include gems
export PYTHONPATH=../../gems:$PYTHONPATH

# Run tests
pytest -v
```

## 📝 Running Tests - Examples

### Run All Tests
```bash
cd prophecy_tests/python_gems
export PYTHONPATH=../../gems:$PYTHONPATH
pytest -v
```

### Run Specific Test File
```bash
# DataCleansing tests
pytest test_data_cleansing.py -v

# DataMasking tests
pytest test_data_masking.py -v
```

### Run Specific Test Function
```bash
# Run single test
pytest test_data_cleansing.py::test_trim_whitespace -v

# Run multiple specific tests
pytest test_data_masking.py::test_mask_default_characters test_data_masking.py::test_hash_method -v
```

### Run Tests by Pattern
```bash
# Run all tests with "mask" in the name
pytest -k "mask" -v

# Run all tests with "hash" in the name
pytest -k "hash" -v
```

### Run Multiple Test Files
```bash
pytest test_data_cleansing.py test_data_masking.py -v
```

### Run with Different Output Formats
```bash
# Short output
pytest --tb=short

# Show all test details
pytest -vv

# Quiet mode (only show failures)
pytest -q

# Show output from print statements
pytest -s
```

## 🔍 One-Liner Commands (From Project Root)

```bash
# Run all Python gem tests
cd prophecy_tests/python_gems && PYTHONPATH=../../gems:$PYTHONPATH pytest -v

# Run specific file
cd prophecy_tests/python_gems && PYTHONPATH=../../gems:$PYTHONPATH pytest test_data_cleansing.py -v

# Run specific test
cd prophecy_tests/python_gems && PYTHONPATH=../../gems:$PYTHONPATH pytest test_data_cleansing.py::test_trim_whitespace -v
```

## 📂 Test Files

Tests are grouped into one directory per gem.

| Directory | Description | Tests |
|-----------|-------------|-------|
| `DataCleansing/` | DataCleansing gem tests | 15 tests ✅ |
| `DataMasking/` | DataMasking gem tests | 18 tests ✅ |
| `Finance/` | Finance gem tests, all 15 finance functions | 39 tests ✅ |
| `conftest.py` | Shared fixtures and Prophecy mocks | - |

## 🛠️ Understanding the Test Setup

### Why PYTHONPATH?
The tests need to import gems from the `gems/` directory. Setting `PYTHONPATH` tells Python where to find them:

```python
from DataCleansing import DataCleansing  # Finds gems/DataCleansing.py
```

### conftest.py
- Automatically loaded by pytest
- Sets up Prophecy framework mocks (so gems can be imported without Prophecy installed)
- Provides `spark` fixture (SparkSession for all tests)

### Writing New Tests

Create new test files following this simple pattern:

```python
"""Test cases for MyGem's applyPython method."""

import pytest
from unittest.mock import Mock
from MyGem import MyGem

def create_gem(**props):
    """Helper to create gem with mocked props."""
    gem = MyGem()
    gem.props = Mock(**props)
    return gem

def test_my_scenario(spark):
    """Test description"""
    # 1. Create test data
    df = spark.createDataFrame([("data",)], ["col"])
    
    # 2. Create gem with properties
    gem = create_gem(columnName="col", someProperty=True)
    
    # 3. Call applyPython
    result = gem.applyPython(spark, df)
    
    # 4. Assert results
    assert result.count() == 1
```

## ⚠️ Troubleshooting

### "Java not found" Error
PySpark requires Java 11+:
```bash
# macOS
brew install openjdk@11

# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk

# Verify
java -version
```

### "Module not found" Error
Make sure PYTHONPATH includes the gems directory:
```bash
export PYTHONPATH=../../gems:$PYTHONPATH
echo $PYTHONPATH  # Verify it's set
```

### "PYTHON_VERSION_MISMATCH" Error

Spark refuses to run when its workers use a different Python minor version from the
driver, and it fails every Spark-backed test at once, so it looks like the gems broke
rather than the environment. `conftest.py` prevents this by pointing Spark at the
interpreter running pytest. To override it, set both variables yourself:

```bash
export PYSPARK_PYTHON=/path/to/python
export PYSPARK_DRIVER_PYTHON=/path/to/python
```

### Tests Pass Locally but Fail in CI
- Ensure you're using the same Python version (3.11+)
- Check that all dependencies are in `requirements.txt`
- Verify Java is available in CI environment

## 🎯 Best Practices

1. **Always set PYTHONPATH** before running tests
2. **Run tests before committing** to catch issues early
3. **Use descriptive test names** that explain what's being tested
4. **Keep tests simple** - one concept per test function
5. **Use fixtures** for reusable test data

## 📊 Test Results

Current test coverage:
- ✅ **DataCleansing/**: 15/15 passing (100%)
- ✅ **DataMasking/**: 18/18 passing (100%)
- ✅ **Finance/**: 39/39 passing (100%)

## 🔗 Related Documentation

- [Main Test Framework README](../README.md)
- [Contributing Guide](../CONTRIBUTING.md)
- [Quick Start Guide](../QUICK_START.md)

## Status
✅ **Active** - Tests implemented and running in CI/CD

# PySpark Tests

This directory contains tests for PySpark/Databricks functionality.

## Setup

```bash
cd prophecy_tests/pyspark
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running Tests

```bash
# Run all tests
pytest -v

# Run specific test file
pytest test_data_cleansing.py -v

# Run specific test
pytest test_data_cleansing.py::test_trim_whitespace -v

# Run with coverage
pytest --cov=. --cov-report=html
```

## Test Files

- `test_data_cleansing.py` - Tests for DataCleansing gem
- `test_data_encoder_decoder.py` - Tests for DataEncoderDecoder gem
- `test_data_masking.py` - Tests for DataMasking gem
- `test_json_parse.py` - Tests for JSONParse gem
- `test_text_to_columns.py` - Tests for TextToColumns gem
- `conftest.py` - Shared fixtures and Prophecy framework mocks

## Status
✅ **Active** - Tests implemented and running


# Prophecy Basics Test Framework

This directory contains the test framework for the Prophecy Basics dbt project, supporting multiple test types across different platforms.

## 📁 Test Structure

```
prophecy_tests/
├── pyspark/              # PySpark/Databricks tests (✅ Active - pytest)
│   ├── conftest.py
│   ├── pytest.ini
│   ├── requirements.txt
│   ├── test_data_cleansing.py
│   ├── test_data_encoder_decoder.py
│   ├── test_data_masking.py
│   ├── test_json_parse.py
│   └── test_text_to_columns.py
├── snowflake_sql/        # Snowflake SQL tests (✅ Active - LocalStack Snowflake)
│   ├── requirements.txt
│   ├── profiles.yml
│   ├── example_unit_tests.yml
│   ├── FindDuplicates/
│   │   ├── test_find_unique_records.sql
│   │   ├── test_find_duplicate_records.sql
│   │   ├── test_find_group_count.sql
│   │   └── README.md
│   └── README.md
├── databricks_sql/       # Databricks SQL tests (✅ Active - Databricks Runtime 16.4-LTS)
│   ├── requirements.txt
│   ├── profiles.yml
│   ├── example_unit_tests.yml
│   ├── FindDuplicates/
│   │   ├── test_find_unique_records.sql
│   │   ├── test_find_duplicate_records.sql
│   │   ├── test_find_group_count.sql
│   │   └── README.md
│   └── README.md
├── duckdb_sql/           # DuckDB SQL tests (⚠️ Coming Soon - dbt unit tests)
│   ├── requirements.txt
│   ├── profiles.yml
│   ├── example_unit_tests.yml
│   └── README.md
├── run_tests.sh          # Shell test runner
├── run_tests.py          # Python test runner (cross-platform)
└── test_config.yml       # Test configuration
```

## 🧪 Test Approaches

This framework supports two types of testing:

### 1. **pytest** (PySpark Tests)
- Tests Python gem implementations directly
- Uses unittest.mock for Prophecy framework mocking
- Runs with pytest test runner

### 2. **dbt unit tests** (SQL Platform Tests)
- Tests SQL macros using dbt's unit testing framework
- Defined in YAML files
- Runs with `dbt test --select test_type:unit`
- Platform-specific configurations via profiles.yml

## 🚀 Quick Start

### Running Tests Locally

#### Option 1: Using Python Test Runner (Recommended - Cross-platform)

```bash
# Run PySpark tests
python run_tests.py pyspark

# Run all tests
python run_tests.py all

# Run with verbose output and HTML report
python run_tests.py pyspark --verbose --html
```

#### Option 2: Using Shell Script (Unix/Linux/macOS)

```bash
# Make the script executable (first time only)
chmod +x run_tests.sh

# Run PySpark tests
./run_tests.sh pyspark

# Run all tests
./run_tests.sh all

# Run with options
./run_tests.sh pyspark --verbose --html
```

#### Option 3: Direct pytest (Manual)

```bash
# Navigate to test directory
cd pyspark

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Add gems to PYTHONPATH
export PYTHONPATH=../../gems:$PYTHONPATH

# Run tests
pytest -v
```

## 🔧 Test Runner Options

Both `run_tests.py` and `run_tests.sh` support the following options:

| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Enable verbose output |
| `--parallel` | `-p` | Enable parallel test execution |
| `--html` | `-h` | Generate HTML test report |

### Test Types

| Type | Description | Test Framework | Status |
|------|-------------|----------------|--------|
| `pyspark` | PySpark/Databricks gem tests | pytest | ✅ Active |
| `snowflake_sql` | Snowflake SQL macro tests | dbt singular tests | ✅ Active |
| `databricks_sql` | Databricks SQL macro tests | dbt singular tests | ✅ Active |
| `duckdb_sql` | DuckDB SQL macro tests | dbt singular tests | ✅ Active |
| `all` | Run all available tests | Mixed | ✅ Available |

## 🔄 CI/CD Integration

### GitHub Actions

The project includes a comprehensive GitHub Actions workflow that automatically runs tests on:
- Push to `main`, `develop`, or `deb/**` branches
- Pull requests to `main` or `develop`
- Manual workflow dispatch with test type selection

#### Triggering Workflow Manually

1. Go to the **Actions** tab in GitHub
2. Select **Prophecy Basics Test Suite**
3. Click **Run workflow**
4. Select test type from dropdown:
   - `pyspark` - Run only PySpark tests
   - `snowflake_sql` - Run only Snowflake tests
   - `databricks_sql` - Run only Databricks SQL tests
   - `duckdb_sql` - Run only DuckDB tests
   - `all` - Run all available tests
5. For Snowflake/Databricks tests, fill in the credential fields:
   - **Snowflake**: account, user, password, database, warehouse
   - **Databricks**: host, token, HTTP path
6. Click **Run workflow**

> **Note**: Credentials can also be pre-configured as repo secrets for automated runs (push/PR triggers). The workflow uses inputs first, then falls back to secrets.

#### Workflow Features

- ✅ Parameterized test execution
- ✅ Parallel test jobs for different platforms
- ✅ Automatic test result uploads
- ✅ HTML test reports
- ✅ Test summary in GitHub UI
- ✅ Job-level emojis for visual hierarchy (🔥 PySpark, ❄️ Snowflake, 🧱 Databricks, 🦆 DuckDB)

### Test Results and Artifacts

After each CI run, the following artifacts are available:
- Test result XML files (JUnit format)
- HTML test reports

These can be downloaded from the workflow run page.

## 🧪 Writing New Tests

### PySpark Tests (pytest)

1. Create a new test file in `pyspark/` directory with name `test_*.py`
2. Import required fixtures from `conftest.py`
3. Use the `spark` fixture for SparkSession
4. Follow existing test patterns

Example:

```python
import pytest
from unittest.mock import Mock

def test_my_feature(spark):
    """Test description"""
    # Create test data
    data = [(1, "test"), (2, "data")]
    df = spark.createDataFrame(data, ["id", "value"])
    
    # Your test logic
    result = df.filter("id > 1")
    
    # Assertions
    assert result.count() == 1
```

### SQL Platform Tests (dbt unit tests)

For Snowflake, Databricks SQL, and DuckDB tests, use dbt's unit testing framework:

1. Create unit test YAML files in your models or tests directory
2. Define tests following dbt unit test syntax
3. The test runner will automatically set up dbt and run tests

Example unit test structure:

```yaml
unit_tests:
  - name: test_my_macro
    model: my_model
    description: "Test description"
    given:
      - input: ref('source_table')
        format: dict
        rows:
          - {id: 1, name: 'Alice'}
          - {id: 2, name: 'Bob'}
    expect:
      format: dict
      rows:
        - {id: 1, name: 'ALICE'}
        - {id: 2, name: 'BOB'}
```

See example files in each test directory:
- `snowflake_sql/example_unit_tests.yml`
- `databricks_sql/example_unit_tests.yml`
- `duckdb_sql/example_unit_tests.yml`

### Adding Tests for SQL Platforms

1. Navigate to the appropriate directory (`snowflake_sql/`, `databricks_sql/`, `duckdb_sql/`)
2. Review `example_unit_tests.yml` for syntax examples
3. Create your unit tests in the main dbt project (in `models/` or `tests/` directory)
4. Ensure `profiles.yml` is correctly configured
5. Run tests using the test runner or `dbt test --select test_type:unit`

## 🔍 Test Configuration

Test configuration is managed in `test_config.yml`:

```yaml
test_suites:
  pyspark:
    enabled: true
    python_version: "3.11"
    requires_java: true
  # ... other suites
```

## 🐛 Troubleshooting

### Common Issues

#### Java Not Found (PySpark tests)

PySpark requires Java 8 or 11. Install Java:

```bash
# macOS
brew install openjdk@11

# Ubuntu/Debian
sudo apt-get install openjdk-11-jdk

# Verify
java -version
```

#### Import Errors

Make sure gems are in PYTHONPATH:

```bash
export PYTHONPATH=/path/to/prophecy-basics/gems:$PYTHONPATH
```

#### Prophecy Framework Mocks

The `conftest.py` file sets up mocks for Prophecy framework dependencies. If you see import errors related to Prophecy modules, check that `conftest.py` is in the test directory.

## 📝 Best Practices

1. **Test Isolation**: Each test should be independent and not rely on other tests
2. **Fixtures**: Use pytest fixtures for reusable test data and setup
3. **Assertions**: Use clear, descriptive assertions
4. **Documentation**: Add docstrings to test functions explaining what they test
5. **Naming**: Use descriptive test function names starting with `test_`
6. **Markers**: Use pytest markers to categorize tests (smoke, regression, slow)

## 🎯 Markers

Tests can be marked with the following markers:

```python
@pytest.mark.smoke  # Quick smoke tests
@pytest.mark.regression  # Full regression tests
@pytest.mark.slow  # Tests that take a long time
```

Run specific markers:

```bash
pytest -m smoke  # Run only smoke tests
pytest -m "not slow"  # Skip slow tests
```

## 📚 Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

## 🤝 Contributing

When adding new tests:
1. Follow the existing test structure
2. Update this README if adding new test types
3. Ensure tests pass locally before pushing
4. Add appropriate markers to tests
5. Update `requirements.txt` if adding dependencies


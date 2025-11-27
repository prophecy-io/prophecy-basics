# Contributing to Prophecy Basics Tests

Thank you for contributing to the Prophecy Basics test framework! This guide will help you add tests for new gems or platforms.

## 🎯 Quick Links

- [Adding PySpark Tests](#adding-pyspark-tests)
- [Adding Tests for New Platforms](#adding-tests-for-new-platforms)
- [Test Guidelines](#test-guidelines)
- [Running Tests](#running-tests)

## 📝 Adding PySpark Tests

### Step 1: Create Test File

Create a new test file in `pyspark/` with the naming convention `test_<gem_name>.py`:

```bash
touch pyspark/test_my_gem.py
```

### Step 2: Import Dependencies

```python
"""
Test cases for MyGem's applyPython method.

To run these tests:
1. cd prophecy_tests
2. python run_tests.py pyspark
"""

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Import your gem (Prophecy mocks are auto-set up via conftest.py)
from MyGem import MyGem
```

### Step 3: Create Helper Functions

```python
def create_my_gem_instance(**kwargs):
    """
    Helper function to create a MyGem instance with mocked props.
    
    Args:
        **kwargs: Properties to set on the instance
    
    Returns:
        MyGem instance with mocked props
    """
    instance = MyGem()
    instance.props = Mock()
    
    # Set default values
    instance.props.property1 = kwargs.get("property1", "default_value")
    instance.props.property2 = kwargs.get("property2", False)
    
    return instance
```

### Step 4: Create Fixtures

```python
@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    data = [
        (1, "Alice"),
        (2, "Bob"),
    ]
    return spark.createDataFrame(data, schema)
```

### Step 5: Write Tests

```python
def test_basic_functionality(spark, sample_dataframe):
    """
    Test basic functionality of MyGem.
    """
    instance = create_my_gem_instance(property1="value")
    result = instance.applyPython(spark, sample_dataframe)
    
    assert result.count() == 2
    assert result.columns == ["id", "name"]


def test_edge_case_empty_dataframe(spark):
    """
    Test handling of empty DataFrame.
    """
    schema = StructType([StructField("id", IntegerType(), True)])
    empty_df = spark.createDataFrame([], schema)
    
    instance = create_my_gem_instance()
    result = instance.applyPython(spark, empty_df)
    
    assert result.count() == 0


def test_null_handling(spark):
    """
    Test handling of null values.
    """
    schema = StructType([StructField("value", StringType(), True)])
    data = [(None,), ("test",)]
    df = spark.createDataFrame(data, schema)
    
    instance = create_my_gem_instance()
    result = instance.applyPython(spark, df)
    
    # Add your assertions
    assert result.count() == 2


@pytest.mark.slow
def test_large_dataset(spark):
    """
    Test with large dataset (marked as slow).
    """
    # Generate large dataset
    data = [(i, f"value_{i}") for i in range(10000)]
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", StringType(), True),
    ])
    df = spark.createDataFrame(data, schema)
    
    instance = create_my_gem_instance()
    result = instance.applyPython(spark, df)
    
    assert result.count() == 10000
```

### Step 6: Run Tests

```bash
# Run your new tests
cd prophecy_tests
python run_tests.py pyspark -v

# Run specific test file
cd pyspark
pytest test_my_gem.py -v

# Run specific test
pytest test_my_gem.py::test_basic_functionality -v
```

## 🆕 Adding Tests for New Platforms

### Snowflake SQL Example

1. **Create Test Structure**

```bash
cd prophecy_tests/snowflake_sql
touch conftest.py
touch test_snowflake_macro.py
```

2. **Update requirements.txt**

```txt
pytest==7.4.3
pytest-cov==4.1.0
snowflake-connector-python==3.5.0
snowflake-sqlalchemy==1.5.1
```

3. **Create conftest.py with Fixtures**

```python
import pytest
import snowflake.connector

@pytest.fixture(scope="session")
def snowflake_connection():
    """Create Snowflake connection for testing."""
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='PUBLIC'
    )
    yield conn
    conn.close()
```

4. **Write Tests**

```python
def test_snowflake_macro(snowflake_connection):
    """Test Snowflake macro functionality."""
    cursor = snowflake_connection.cursor()
    
    # Your test SQL
    cursor.execute("SELECT 1 as test_col")
    result = cursor.fetchone()
    
    assert result[0] == 1
```

5. **Update test_config.yml**

```yaml
test_suites:
  snowflake_sql:
    enabled: true
    description: "Snowflake SQL tests"
    # ...
```

## ✅ Test Guidelines

### General Principles

1. **Test Isolation**: Each test should be independent
2. **Clear Names**: Use descriptive test function names
3. **Documentation**: Add docstrings explaining what the test validates
4. **Assertions**: Use clear, specific assertions
5. **Edge Cases**: Test boundary conditions and edge cases

### Test Structure

Follow the Arrange-Act-Assert pattern:

```python
def test_something(spark):
    # Arrange: Set up test data
    data = [(1, "test")]
    df = spark.createDataFrame(data, ["id", "value"])
    instance = create_gem_instance()
    
    # Act: Execute the functionality
    result = instance.applyPython(spark, df)
    
    # Assert: Verify results
    assert result.count() == 1
    assert result.columns == ["id", "value"]
```

### What to Test

✅ **DO Test:**
- Happy path (normal operation)
- Edge cases (empty data, nulls, special characters)
- Error handling
- Data type preservation
- Column order preservation
- Schema validation
- Performance (for large datasets, mark as `@pytest.mark.slow`)

❌ **DON'T Test:**
- Implementation details
- Third-party library functionality
- Same thing multiple times

### Test Markers

Use pytest markers to categorize tests:

```python
@pytest.mark.smoke  # Quick validation tests
def test_basic_operation(spark):
    pass

@pytest.mark.regression  # Comprehensive tests
def test_all_scenarios(spark):
    pass

@pytest.mark.slow  # Tests that take > 5 seconds
def test_large_dataset(spark):
    pass

@pytest.mark.integration  # Tests requiring external services
def test_database_connection():
    pass
```

## 🏃 Running Tests

### Local Development

```bash
# Run all tests for a platform
python run_tests.py pyspark

# Run with coverage
python run_tests.py pyspark --coverage

# Run specific markers
cd pyspark
pytest -m smoke  # Only smoke tests
pytest -m "not slow"  # Skip slow tests

# Run with verbose output
python run_tests.py pyspark --verbose

# Generate HTML report
python run_tests.py pyspark --html
```

### CI/CD

Tests run automatically on push and PR. To manually trigger:
1. Go to Actions tab
2. Select "Prophecy Basics Test Suite"
3. Run workflow with desired test type

## 🐛 Debugging Tests

### View Full Output

```bash
pytest -vv --tb=long test_my_gem.py
```

### Run Single Test

```bash
pytest test_my_gem.py::test_specific_function -vv
```

### Drop into Debugger on Failure

```bash
pytest --pdb test_my_gem.py
```

### Print DataFrame for Inspection

```python
def test_debug(spark, sample_dataframe):
    result = transform(sample_dataframe)
    result.show()  # Print DataFrame
    result.printSchema()  # Print schema
    assert result.count() > 0
```

## 📊 Coverage

### Generate Coverage Report

```bash
python run_tests.py pyspark --coverage
```

### View HTML Coverage

```bash
# After running with coverage
open coverage/pyspark-html/index.html
```

### Coverage Goals

- Aim for >80% code coverage
- Focus on critical paths
- Don't sacrifice test quality for coverage numbers

## 🔍 Code Review Checklist

Before submitting a PR with tests:

- [ ] Tests pass locally
- [ ] Tests are well-documented
- [ ] Edge cases are covered
- [ ] Tests follow naming conventions
- [ ] Appropriate markers are used
- [ ] No hardcoded values (use fixtures/variables)
- [ ] Tests are isolated (no dependencies on other tests)
- [ ] Coverage is maintained or improved
- [ ] README updated if needed

## 🤝 Getting Help

- Check existing tests for patterns
- Review [pytest documentation](https://docs.pytest.org/)
- Ask questions in issues or discussions
- Reference PySpark testing guide for Spark-specific questions

## 📚 Additional Resources

- [pytest Documentation](https://docs.pytest.org/)
- [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Test-Driven Development](https://en.wikipedia.org/wiki/Test-driven_development)

---

Thank you for contributing to better test coverage! 🎉


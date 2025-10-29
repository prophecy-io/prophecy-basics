"""
To run these tests:
1. Activate virtual environment: source venv/bin/activate
2. Install pytest: pip install pytest
3. Run from project root: pytest tests/SparkTests/test_count_records.py
4. Run with verbose output: pytest tests/SparkTests/test_count_records.py -v
5. Run specific test: pytest tests/SparkTests/test_count_records.py::test_count_records_normal_case
6. Run with coverage: pytest tests/SparkTests/test_count_records.py --cov=tests.SparkTests.test_count_records
"""

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Import CountRecords - Prophecy mocks are automatically set up via conftest.py
from CountRecords import CountRecords

# Note: The spark fixture and Prophecy mocks are automatically set up via conftest.py


@pytest.fixture
def sample_dataframe(spark):
    """
    Create a sample DataFrame with multiple columns and rows for testing.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
    ])
    
    data = [
        (1, "Alice", 30, 50000.0),
        (2, "Bob", 25, 45000.0),
        (3, "Charlie", 35, 60000.0),
        (4, "David", 28, None),  # Null value
        (5, "Eve", None, 55000.0),  # Null in age
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_dataframe(spark):
    """
    Create an empty DataFrame with a schema.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    
    return spark.createDataFrame([], schema)


@pytest.fixture
def single_record_dataframe(spark):
    """
    Create a DataFrame with a single record.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
    ])
    
    data = [(1, "Single", 100)]
    
    return spark.createDataFrame(data, schema)


def create_count_records_instance(column_names=None, count_method="count_all_records"):
    """
    Helper function to create a CountRecords instance with mocked props.
    
    Args:
        column_names: List of column names to count (optional)
        count_method: Method to use for counting (default: "count_all_records")
        
    Returns:
        CountRecords instance with mocked props configured
    """
    # Create actual CountRecords instance
    instance = CountRecords()
    
    # Mock the props object with the values we need for testing
    instance.props = Mock()
    instance.props.column_names = column_names if column_names is not None else []
    instance.props.count_method = count_method
    
    return instance


def test_count_records_normal_case(spark, sample_dataframe):
    """
    Test counting records in a normal DataFrame with multiple rows.
    This tests the default count_all_records method.
    """
    # Arrange
    instance = create_count_records_instance(count_method="count_all_records")
    
    # Act
    result = instance.applyPython(spark, sample_dataframe)
    
    # Assert
    assert result is not None
    assert result.count() == 1  # Aggregation returns single row
    
    # Verify count is correct
    count_value = result.select("total_records").first()[0]
    assert count_value == 5  # 5 rows in sample_dataframe
    
    # Verify output schema
    expected_columns = ["total_records"]
    assert result.columns == expected_columns


def test_count_records_empty_dataframe(spark, empty_dataframe):
    """
    Test counting records in an empty DataFrame.
    Edge case: Should return 0, not error.
    """
    # Arrange
    instance = create_count_records_instance(count_method="count_all_records")
    
    # Act
    result = instance.applyPython(spark, empty_dataframe)
    
    # Assert
    assert result is not None
    assert result.count() == 1  # Aggregation always returns one row
    
    count_value = result.select("total_records").first()[0]
    assert count_value == 0  # Empty DataFrame should return 0


def test_count_records_single_record(spark, single_record_dataframe):
    """
    Test counting records in a DataFrame with a single record.
    Edge case: Minimal dataset.
    """
    # Arrange
    instance = create_count_records_instance(count_method="count_all_records")
    
    # Act
    result = instance.applyPython(spark, single_record_dataframe)
    
    # Assert
    assert result is not None
    count_value = result.select("total_records").first()[0]
    assert count_value == 1


def test_count_records_with_nulls(spark):
    """
    Test counting records in a DataFrame with null values.
    Edge case: Null handling.
    """
    # Arrange: Create DataFrame with nulls
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("nullable_col", StringType(), True),
    ])
    
    data = [
        (1, "Alice", "value1"),
        (2, "Bob", None),
        (3, "Charlie", "value2"),
        (4, "David", None),
    ]
    
    df = spark.createDataFrame(data, schema)
    instance = create_count_records_instance(count_method="count_all_records")
    
    # Act
    result = instance.applyPython(spark, df)
    
    # Assert: Total count should include all rows, even with nulls
    count_value = result.select("total_records").first()[0]
    assert count_value == 4


def test_count_non_null_records_no_columns(spark, sample_dataframe):
    """
    Test count_non_null_records method without specifying columns.
    Should fall back to total count.
    """
    # Arrange
    instance = create_count_records_instance(
        column_names=[],
        count_method="count_non_null_records"
    )
    
    # Act
    result = instance.applyPython(spark, sample_dataframe)
    
    # Assert
    assert result is not None
    count_value = result.select("total_records").first()[0]
    assert count_value == 5  # Should count all records


def test_count_non_null_records_with_columns(spark, sample_dataframe):
    """
    Test count_non_null_records method with specified columns.
    Should count non-null values in each column.
    """
    # Arrange
    instance = create_count_records_instance(
        column_names=["id", "name", "age"],
        count_method="count_non_null_records"
    )
    
    # Act
    result = instance.applyPython(spark, sample_dataframe)
    
    # Assert
    assert result is not None
    assert result.count() == 1
    
    # Verify all expected columns are present
    expected_columns = ["id_count", "name_count", "age_count"]
    assert set(result.columns) == set(expected_columns)
    
    # Verify counts: id and name should have 5 non-nulls, age should have 4 (one null)
    row = result.first()
    assert row["id_count"] == 5
    assert row["name_count"] == 5
    assert row["age_count"] == 4  # One null in age column


def test_count_distinct_records_no_columns(spark, sample_dataframe):
    """
    Test count_distinct_records method without specifying columns.
    Should fall back to total count.
    """
    # Arrange
    instance = create_count_records_instance(
        column_names=[],
        count_method="count_distinct_records"
    )
    
    # Act
    result = instance.applyPython(spark, sample_dataframe)
    
    # Assert
    assert result is not None
    count_value = result.select("total_records").first()[0]
    assert count_value == 5


def test_count_distinct_records_with_columns(spark):
    """
    Test count_distinct_records method with specified columns.
    Should count distinct non-null values in each column.
    """
    # Arrange: Create DataFrame with duplicate values
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("status", StringType(), True),
    ])
    
    data = [
        ("A", 10, "active"),
        ("A", 20, "active"),
        ("B", 10, "inactive"),
        ("B", 30, "active"),
        ("A", 10, None),  # Duplicate value with null in status
    ]
    
    df = spark.createDataFrame(data, schema)
    
    instance = create_count_records_instance(
        column_names=["category", "value", "status"],
        count_method="count_distinct_records"
    )
    
    # Act
    result = instance.applyPython(spark, df)
    
    # Assert
    assert result is not None
    
    row = result.first()
    assert row["category_distinct_count"] == 2  # A and B
    assert row["value_distinct_count"] == 3  # 10, 20, 30
    assert row["status_distinct_count"] == 2  # "active" and "inactive" (nulls excluded)


def test_count_records_output_schema(spark, sample_dataframe):
    """
    Verify the output DataFrame has correct schema structure.
    Tests schema validation for different count methods.
    """
    # Test count_all_records schema
    instance = create_count_records_instance(count_method="count_all_records")
    result = instance.applyPython(spark, sample_dataframe)
    
    assert len(result.columns) == 1
    assert "total_records" in result.columns
    assert result.schema["total_records"].dataType.typeName() == "long"
    
    # Test count_non_null_records with columns schema
    instance = create_count_records_instance(
        column_names=["id", "name"],
        count_method="count_non_null_records"
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    assert "id_count" in result.columns
    assert "name_count" in result.columns
    assert len(result.columns) == 2


def test_count_records_accuracy(spark):
    """
    Test count accuracy with various scenarios.
    Validates that counts are mathematically correct.
    """
    # Arrange: DataFrame with exactly 10 rows
    schema = StructType([StructField("id", IntegerType(), True)])
    data = [(i,) for i in range(1, 11)]  # 10 rows
    
    df = spark.createDataFrame(data, schema)
    instance = create_count_records_instance(count_method="count_all_records")
    
    # Act
    result = instance.applyPython(spark, df)
    
    # Assert
    count_value = result.select("total_records").first()[0]
    assert count_value == 10, f"Expected 10 records, got {count_value}"


def test_count_records_missing_column(spark, sample_dataframe):
    """
    Test behavior when a specified column doesn't exist.
    This should be handled gracefully by PySpark (will raise exception or return null).
    """
    # Arrange: Try to count a column that doesn't exist
    instance = create_count_records_instance(
        column_names=["nonexistent_column"],
        count_method="count_non_null_records"
    )
    
    # Act & Assert: Should raise AnalysisException
    with pytest.raises(Exception):  # PySpark raises AnalysisException
        instance.applyPython(spark, sample_dataframe)


def test_count_records_all_methods_comparison(spark, sample_dataframe):
    """
    Compare outputs of different count methods on the same DataFrame.
    Ensures consistency across methods.
    """
    # Test all three methods
    methods = ["count_all_records", "count_non_null_records", "count_distinct_records"]
    
    for method in methods:
        instance = create_count_records_instance(
            column_names=[],
            count_method=method
        )
        result = instance.applyPython(spark, sample_dataframe)
        
        assert result is not None
        assert result.count() == 1  # All should return single aggregated row


def test_count_records_multiple_column_types(spark):
    """
    Test counting with different column data types.
    Ensures the method works with various data types.
    """
    # Arrange: DataFrame with various types
    schema = StructType([
        StructField("string_col", StringType(), True),
        StructField("int_col", IntegerType(), True),
        StructField("double_col", DoubleType(), True),
    ])
    
    data = [
        ("text1", 1, 1.5),
        ("text2", 2, 2.5),
        ("text3", 3, 3.5),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    instance = create_count_records_instance(
        column_names=["string_col", "int_col", "double_col"],
        count_method="count_non_null_records"
    )
    
    # Act
    result = instance.applyPython(spark, df)
    
    # Assert
    row = result.first()
    assert row["string_col_count"] == 3
    assert row["int_col_count"] == 3
    assert row["double_col_count"] == 3


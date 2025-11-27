"""
Test cases for DataCleansing gem's applyPython method.

To run these tests:
1. Activate virtual environment: source venv/bin/activate
2. Install pytest: pip install pytest
3. Run from project root: pytest tests/SparkTests/test_data_cleansing.py
4. Run with verbose output: pytest tests/SparkTests/test_data_cleansing.py -v
5. Run specific test: pytest tests/SparkTests/test_data_cleansing.py::test_trim_whitespace
"""

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType

# Import DataCleansing - Prophecy mocks are automatically set up via conftest.py
from DataCleansing import DataCleansing

# Note: The spark fixture and Prophecy mocks are automatically set up via conftest.py


def create_data_cleansing_instance(**kwargs):
    """
    Helper function to create a DataCleansing instance with mocked props.
    
    Args:
        **kwargs: Properties to set on the instance (defaults provided)
    
    Returns:
        DataCleansing instance with mocked props
    """
    instance = DataCleansing()
    instance.props = Mock()
    
    # Set default values
    instance.props.removeRowNullAllCols = kwargs.get("removeRowNullAllCols", False)
    instance.props.columnNames = kwargs.get("columnNames", [])
    instance.props.replaceNullTextFields = kwargs.get("replaceNullTextFields", False)
    instance.props.replaceNullTextWith = kwargs.get("replaceNullTextWith", "NA")
    instance.props.replaceNullForNumericFields = kwargs.get("replaceNullForNumericFields", False)
    instance.props.replaceNullNumericWith = kwargs.get("replaceNullNumericWith", 0)
    instance.props.trimWhiteSpace = kwargs.get("trimWhiteSpace", False)
    instance.props.removeTabsLineBreaksAndDuplicateWhitespace = kwargs.get("removeTabsLineBreaksAndDuplicateWhitespace", False)
    instance.props.allWhiteSpace = kwargs.get("allWhiteSpace", False)
    instance.props.cleanLetters = kwargs.get("cleanLetters", False)
    instance.props.cleanPunctuations = kwargs.get("cleanPunctuations", False)
    instance.props.cleanNumbers = kwargs.get("cleanNumbers", False)
    instance.props.modifyCase = kwargs.get("modifyCase", "Keep original")
    instance.props.replaceNullDateFields = kwargs.get("replaceNullDateFields", False)
    instance.props.replaceNullDateWith = kwargs.get("replaceNullDateWith", "1970-01-01")
    instance.props.replaceNullTimeFields = kwargs.get("replaceNullTimeFields", False)
    instance.props.replaceNullTimeWith = kwargs.get("replaceNullTimeWith", "1970-01-01 00:00:00.0")
    
    return instance


@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame with various data types and nulls."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True),
        StructField("description", StringType(), True),
    ])
    data = [
        (1, "  Alice  ", 30, 50000.0, "Hello, World!"),
        (2, None, 25, None, "Test Data"),
        (3, "Bob", None, 60000.0, "  Multiple   Spaces  "),
        (4, "Charlie", 35, 70000.0, None),
        (5, "david", 28, 55000.0, "test123"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_dataframe(spark):
    """Create an empty DataFrame."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture
def dataframe_with_all_nulls(spark):
    """Create a DataFrame with rows where all columns are null."""
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", IntegerType(), True),
    ])
    data = [
        (None, None),
        ("value", 1),
        (None, None),
    ]
    return spark.createDataFrame(data, schema)


def test_no_cleansing_operations(spark, sample_dataframe):
    """
    Test that DataFrame remains unchanged when no cleansing operations are applied.
    """
    instance = create_data_cleansing_instance()
    result = instance.applyPython(spark, sample_dataframe)
    
    assert result.count() == sample_dataframe.count()
    assert result.columns == sample_dataframe.columns
    # Verify data is unchanged
    result_rows = sorted(result.collect(), key=lambda x: x.id)
    original_rows = sorted(sample_dataframe.collect(), key=lambda x: x.id)
    for r1, r2 in zip(result_rows, original_rows):
        assert r1 == r2


def test_remove_row_null_all_cols(spark, dataframe_with_all_nulls):
    """
    Test removing rows where all columns are null.
    """
    instance = create_data_cleansing_instance(removeRowNullAllCols=True)
    result = instance.applyPython(spark, dataframe_with_all_nulls)
    
    assert result.count() == 1  # Only one row should remain
    rows = result.collect()
    assert rows[0].col1 == "value"
    assert rows[0].col2 == 1


def test_replace_null_text_fields(spark, sample_dataframe):
    """
    Test replacing null values in string columns.
    """
    instance = create_data_cleansing_instance(
        columnNames=["name", "description"],
        replaceNullTextFields=True,
        replaceNullTextWith="UNKNOWN"
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    rows = result.collect()
    # Find row with null name (id=2)
    row_with_null = next((r for r in rows if r.id == 2), None)
    assert row_with_null is not None
    assert row_with_null.name == "UNKNOWN"
    
    # Find row with null description (id=4)
    row_with_null_desc = next((r for r in rows if r.id == 4), None)
    assert row_with_null_desc is not None
    assert row_with_null_desc.description == "UNKNOWN"


def test_replace_null_numeric_fields(spark, sample_dataframe):
    """
    Test replacing null values in numeric columns.
    """
    instance = create_data_cleansing_instance(
        columnNames=["age", "salary"],
        replaceNullForNumericFields=True,
        replaceNullNumericWith=999
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    rows = result.collect()
    # Find row with null age (id=3)
    row_with_null_age = next((r for r in rows if r.id == 3), None)
    assert row_with_null_age is not None
    assert row_with_null_age.age == 999
    
    # Find row with null salary (id=2)
    row_with_null_salary = next((r for r in rows if r.id == 2), None)
    assert row_with_null_salary is not None
    assert row_with_null_salary.salary == 999.0


def test_trim_whitespace(spark, sample_dataframe):
    """
    Test trimming leading and trailing whitespace from string columns.
    """
    instance = create_data_cleansing_instance(
        columnNames=["name", "description"],
        trimWhiteSpace=True
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    rows = result.collect()
    # Find row with whitespace (id=1)
    row = next((r for r in rows if r.id == 1), None)
    assert row is not None
    assert row.name == "Alice"  # Should be trimmed
    assert row.description == "Hello, World!"  # Should be trimmed


def test_remove_tabs_linebreaks_whitespace(spark):
    """
    Test removing tabs, line breaks, and duplicate whitespaces.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello\tWorld\nTest",),
        ("Multiple   Spaces   Here",),
        ("Tab\t\t\tTest",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        removeTabsLineBreaksAndDuplicateWhitespace=True
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    # All whitespace should be collapsed to single spaces
    assert "  " not in rows[0].text  # No double spaces
    assert "\t" not in rows[0].text  # No tabs
    assert "\n" not in rows[0].text  # No newlines


def test_remove_all_whitespace(spark):
    """
    Test removing all whitespace from string columns.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello World",),
        ("Test String",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        allWhiteSpace=True
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "HelloWorld"
    assert rows[1].text == "TestString"


def test_clean_letters(spark):
    """
    Test removing letters from string columns.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello123World",),
        ("Test456String",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        cleanLetters=True
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "123"
    assert rows[1].text == "456"


def test_clean_punctuations(spark):
    """
    Test removing punctuation from string columns.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello, World!",),
        ("Test-String@123",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        cleanPunctuations=True
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "TestString123"


def test_clean_numbers(spark):
    """
    Test removing numbers from string columns.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello123World",),
        ("Test456String789",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        cleanNumbers=True
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "HelloWorld"
    assert rows[1].text == "TestString"


def test_modify_case_lowercase(spark):
    """
    Test converting string to lowercase.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello World",),
        ("TEST STRING",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        modifyCase="makeLowercase"
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "hello world"
    assert rows[1].text == "test string"


def test_modify_case_uppercase(spark):
    """
    Test converting string to uppercase.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("Hello World",),
        ("test string",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        modifyCase="makeUppercase"
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "HELLO WORLD"
    assert rows[1].text == "TEST STRING"


def test_modify_case_titlecase(spark):
    """
    Test converting string to title case.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("hello world",),
        ("test string",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        modifyCase="makeTitlecase"
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "Test String"


def test_combined_operations(spark):
    """
    Test multiple cleansing operations applied together.
    """
    schema = StructType([StructField("text", StringType(), True)])
    data = [
        ("  Hello, World123!  ",),
        ("  TEST@456  ",),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["text"],
        trimWhiteSpace=True,
        cleanPunctuations=True,
        cleanNumbers=True,
        modifyCase="makeLowercase"
    )
    result = instance.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "hello world"
    assert rows[1].text == "test"


def test_only_selected_columns_cleansed(spark, sample_dataframe):
    """
    Test that only selected columns are cleansed, others remain unchanged.
    """
    instance = create_data_cleansing_instance(
        columnNames=["name"],
        trimWhiteSpace=True,
        modifyCase="makeUppercase"
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    rows = result.collect()
    # Find row with original name (id=1)
    row = next((r for r in rows if r.id == 1), None)
    assert row is not None
    assert row.name == "ALICE"  # Should be trimmed and uppercased
    # description should remain unchanged (not in columnNames)
    assert row.description == "Hello, World!"  # Original value preserved


def test_empty_dataframe(spark, empty_dataframe):
    """
    Test that empty DataFrame is handled correctly.
    """
    instance = create_data_cleansing_instance(
        columnNames=["name"],
        trimWhiteSpace=True
    )
    result = instance.applyPython(spark, empty_dataframe)
    
    assert result.count() == 0
    assert result.columns == empty_dataframe.columns


def test_replace_null_date_fields(spark):
    """
    Test replacing null values in date columns.
    """
    schema = StructType([
        StructField("event_date", DateType(), True),
        StructField("name", StringType(), True),
    ])
    from datetime import date
    data = [
        (date(2023, 1, 1), "Event1"),
        (None, "Event2"),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["event_date"],
        replaceNullDateFields=True,
        replaceNullDateWith="1970-01-01"
    )
    result = instance.applyPython(spark, df)
    
    # Verify schema is preserved (DateType should remain DateType, not StringType)
    assert isinstance(result.schema["event_date"].dataType, DateType), \
        f"Expected DateType but got {type(result.schema['event_date'].dataType)}"
    
    rows = result.collect()
    # Find row with null date (id=1, Event2)
    row_with_null = next((r for r in rows if r.name == "Event2"), None)
    assert row_with_null is not None
    assert row_with_null.event_date is not None
    # Date should be replaced with default


def test_replace_null_timestamp_fields(spark):
    """
    Test replacing null values in timestamp columns.
    """
    schema = StructType([
        StructField("created_at", TimestampType(), True),
        StructField("name", StringType(), True),
    ])
    from datetime import datetime
    data = [
        (datetime(2023, 1, 1, 12, 0, 0), "Record1"),
        (None, "Record2"),
    ]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["created_at"],
        replaceNullTimeFields=True,
        replaceNullTimeWith="1970-01-01 00:00:00.0"
    )
    result = instance.applyPython(spark, df)
    
    # Verify schema is preserved (TimestampType should remain TimestampType, not StringType)
    assert isinstance(result.schema["created_at"].dataType, TimestampType), \
        f"Expected TimestampType but got {type(result.schema['created_at'].dataType)}"
    
    rows = result.collect()
    # Find row with null timestamp (id=1, Record2)
    row_with_null = next((r for r in rows if r.name == "Record2"), None)
    assert row_with_null is not None
    assert row_with_null.created_at is not None
    # Timestamp should be replaced with default


def test_schema_preservation(spark, sample_dataframe):
    """
    Test that output schema matches input schema.
    """
    instance = create_data_cleansing_instance(
        columnNames=["name"],
        trimWhiteSpace=True
    )
    result = instance.applyPython(spark, sample_dataframe)
    
    assert result.schema == sample_dataframe.schema
    assert result.columns == sample_dataframe.columns


def test_column_order_preservation(spark):
    """
    Test that column order is preserved after cleansing.
    """
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True),
        StructField("col3", StringType(), True),
    ])
    data = [("  a  ", "  b  ", "  c  ",)]
    df = spark.createDataFrame(data, schema)
    
    instance = create_data_cleansing_instance(
        columnNames=["col2"],  # Only cleanse middle column
        trimWhiteSpace=True
    )
    result = instance.applyPython(spark, df)
    
    assert result.columns == ["col1", "col2", "col3"]
    rows = result.collect()
    assert rows[0].col1 == "  a  "  # Unchanged
    assert rows[0].col2 == "b"  # Trimmed
    assert rows[0].col3 == "  c  "  # Unchanged


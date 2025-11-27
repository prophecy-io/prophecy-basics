"""
Test cases for DataCleansing gem's applyPython method.

To run these tests:
    cd prophecy_tests
    python run_tests.py pyspark
"""

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from DataCleansing import DataCleansing


def create_gem(**kwargs):
    """Helper to create DataCleansing gem with mocked props."""
    gem = DataCleansing()
    gem.props = Mock()
    
    # Set default property values
    gem.props.removeRowNullAllCols = kwargs.get("removeRowNullAllCols", False)
    gem.props.columnNames = kwargs.get("columnNames", [])
    gem.props.replaceNullTextFields = kwargs.get("replaceNullTextFields", False)
    gem.props.replaceNullTextWith = kwargs.get("replaceNullTextWith", "NA")
    gem.props.replaceNullForNumericFields = kwargs.get("replaceNullForNumericFields", False)
    gem.props.replaceNullNumericWith = kwargs.get("replaceNullNumericWith", 0)
    gem.props.trimWhiteSpace = kwargs.get("trimWhiteSpace", False)
    gem.props.removeTabsLineBreaksAndDuplicateWhitespace = kwargs.get("removeTabsLineBreaksAndDuplicateWhitespace", False)
    gem.props.allWhiteSpace = kwargs.get("allWhiteSpace", False)
    gem.props.cleanLetters = kwargs.get("cleanLetters", False)
    gem.props.cleanPunctuations = kwargs.get("cleanPunctuations", False)
    gem.props.cleanNumbers = kwargs.get("cleanNumbers", False)
    gem.props.modifyCase = kwargs.get("modifyCase", "Keep original")
    gem.props.replaceNullDateFields = kwargs.get("replaceNullDateFields", False)
    gem.props.replaceNullDateWith = kwargs.get("replaceNullDateWith", "1970-01-01")
    gem.props.replaceNullTimeFields = kwargs.get("replaceNullTimeFields", False)
    gem.props.replaceNullTimeWith = kwargs.get("replaceNullTimeWith", "1970-01-01 00:00:00.0")
    
    return gem


# Test Fixtures
@pytest.fixture
def sample_df(spark):
    """Sample DataFrame with various data types and nulls."""
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
def all_nulls_df(spark):
    """DataFrame with rows where all columns are null."""
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", IntegerType(), True),
    ])
    data = [(None, None), ("value", 1), (None, None)]
    return spark.createDataFrame(data, schema)


# Tests
def test_no_operations(spark, sample_df):
    """DataFrame remains unchanged when no operations are applied."""
    gem = create_gem()
    result = gem.applyPython(spark, sample_df)
    
    assert result.count() == sample_df.count()
    assert result.columns == sample_df.columns


def test_remove_all_null_rows(spark, all_nulls_df):
    """Remove rows where all columns are null."""
    gem = create_gem(removeRowNullAllCols=True)
    result = gem.applyPython(spark, all_nulls_df)
    
    assert result.count() == 1
    row = result.collect()[0]
    assert row.col1 == "value"
    assert row.col2 == 1


def test_replace_null_text(spark, sample_df):
    """Replace null values in string columns."""
    gem = create_gem(
        columnNames=["name", "description"],
        replaceNullTextFields=True,
        replaceNullTextWith="UNKNOWN"
    )
    result = gem.applyPython(spark, sample_df)
    
    rows = result.collect()
    assert rows[1].name == "UNKNOWN"  # id=2 had null name
    assert rows[3].description == "UNKNOWN"  # id=4 had null description


def test_replace_null_numeric(spark, sample_df):
    """Replace null values in numeric columns."""
    gem = create_gem(
        columnNames=["age", "salary"],
        replaceNullForNumericFields=True,
        replaceNullNumericWith=999
    )
    result = gem.applyPython(spark, sample_df)
    
    rows = result.collect()
    assert rows[2].age == 999  # id=3 had null age
    assert rows[1].salary == 999.0  # id=2 had null salary


def test_trim_whitespace(spark, sample_df):
    """Trim leading and trailing whitespace."""
    gem = create_gem(
        columnNames=["name", "description"],
        trimWhiteSpace=True
    )
    result = gem.applyPython(spark, sample_df)
    
    rows = result.collect()
    assert rows[0].name == "Alice"  # Trimmed from "  Alice  "
    assert rows[0].description == "Hello, World!"


def test_remove_all_whitespace(spark):
    """Remove all whitespace from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello World",), ("Test String",)], schema)
    
    gem = create_gem(columnNames=["text"], allWhiteSpace=True)
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "HelloWorld"
    assert rows[1].text == "TestString"


def test_clean_letters(spark):
    """Remove letters from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello123",), ("Test456",)], schema)
    
    gem = create_gem(columnNames=["text"], cleanLetters=True)
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "123"
    assert rows[1].text == "456"


def test_clean_numbers(spark):
    """Remove numbers from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello123",), ("Test456",)], schema)
    
    gem = create_gem(columnNames=["text"], cleanNumbers=True)
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "Hello"
    assert rows[1].text == "Test"


def test_clean_punctuations(spark):
    """Remove punctuation from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello, World!",), ("Test-123",)], schema)
    
    gem = create_gem(columnNames=["text"], cleanPunctuations=True)
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "Test123"


def test_lowercase(spark):
    """Convert strings to lowercase."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("HELLO",), ("WORLD",)], schema)
    
    gem = create_gem(columnNames=["text"], modifyCase="makeLowercase")
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "hello"
    assert rows[1].text == "world"


def test_uppercase(spark):
    """Convert strings to uppercase."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("hello",), ("world",)], schema)
    
    gem = create_gem(columnNames=["text"], modifyCase="makeUppercase")
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "HELLO"
    assert rows[1].text == "WORLD"


def test_titlecase(spark):
    """Convert strings to title case."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("hello world",), ("test string",)], schema)
    
    gem = create_gem(columnNames=["text"], modifyCase="makeTitlecase")
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "Test String"


def test_combined_operations(spark):
    """Multiple operations applied together."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("  Hello123!  ",), ("  TEST456@  ",)], schema)
    
    gem = create_gem(
        columnNames=["text"],
        trimWhiteSpace=True,
        cleanNumbers=True,
        cleanPunctuations=True,
        modifyCase="makeLowercase"
    )
    result = gem.applyPython(spark, df)
    
    rows = result.collect()
    assert rows[0].text == "hello"
    assert rows[1].text == "test"


def test_only_selected_columns(spark, sample_df):
    """Only selected columns are cleansed, others remain unchanged."""
    gem = create_gem(
        columnNames=["name"],
        trimWhiteSpace=True,
        modifyCase="makeUppercase"
    )
    result = gem.applyPython(spark, sample_df)
    
    rows = result.collect()
    assert rows[0].name == "ALICE"  # Trimmed and uppercased
    assert rows[0].description == "Hello, World!"  # Unchanged


def test_empty_dataframe(spark):
    """Empty DataFrame is handled correctly."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema)
    
    gem = create_gem(columnNames=["name"], trimWhiteSpace=True)
    result = gem.applyPython(spark, empty_df)
    
    assert result.count() == 0
    assert result.columns == empty_df.columns

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from DataCleansing import DataCleansing


@pytest.fixture
def create_gem():
    """Factory fixture to create a DataCleansing gem with mocked props."""
    def _create(**kwargs):
        gem = DataCleansing()
        gem.props = Mock()

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
    return _create


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

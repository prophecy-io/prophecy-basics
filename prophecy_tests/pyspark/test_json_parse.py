"""
Test cases for JSONParse gem's applyPython method.

To run these tests:
1. Activate virtual environment: source venv/bin/activate
2. Run from project root: pytest tests/SparkTests/test_json_parse.py
3. Run with verbose output: pytest tests/SparkTests/test_json_parse.py -v
4. Run specific test: pytest tests/SparkTests/test_json_parse.py::test_parse_from_schema_basic -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from unittest.mock import Mock
import sys
import os

# Add gems directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../gems'))
from JSONParse import JSONParse


@pytest.fixture
def spark():
    """Create a SparkSession for testing."""
    spark = SparkSession.builder \
        .appName("JSONParseTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_dataframe(spark):
    """Create a sample DataFrame with JSON column."""
    data = [
        ('{"name": "John", "age": 30}',),
        ('{"name": "Jane", "age": 25}',),
        ('{"name": "Bob", "age": 35}',),
    ]
    schema = StructType([
        StructField("json_col", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def complex_json_dataframe(spark):
    """Create a DataFrame with complex nested JSON."""
    data = [
        ('{"root": {"person": {"id": 1, "name": {"first": "John", "last": "Doe"}, "age": 30}}}',),
        ('{"root": {"person": {"id": 2, "name": {"first": "Jane", "last": "Smith"}, "age": 25}}}',),
    ]
    schema = StructType([
        StructField("json_col", StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_dataframe(spark):
    """Create an empty DataFrame with JSON column."""
    schema = StructType([
        StructField("json_col", StringType(), True)
    ])
    return spark.createDataFrame([], schema)


def create_json_parse_instance(column_name, parsing_method, sample_record=None, sample_schema=None):
    """Helper function to create a JSONParse instance with mocked props."""
    instance = JSONParse()
    instance.props = Mock()
    instance.props.columnName = column_name
    instance.props.parsingMethod = parsing_method
    instance.props.sampleRecord = sample_record
    instance.props.sampleSchema = sample_schema
    return instance


class TestJSONParse:
    """Test suite for JSONParse applyPython method."""

    def test_parse_from_schema_basic(self, spark, sample_dataframe):
        """Test parsing JSON using explicit schema."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<name: STRING, age: INT>"
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        # Check that new column was added
        assert "json_col_parsed" in result.columns
        assert result.count() == 3
        
        # Check parsed data
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed.name == "John"
        assert rows[0].json_col_parsed.age == 30
        assert rows[1].json_col_parsed.name == "Jane"
        assert rows[1].json_col_parsed.age == 25

    def test_parse_from_sample_record_basic(self, spark, sample_dataframe):
        """Test parsing JSON using sample record to infer schema."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record='{"name": "John", "age": 30}'
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        # Check that new column was added
        assert "json_col_parsed" in result.columns
        assert result.count() == 3
        
        # Check parsed data (schema inferred might have age as BIGINT)
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed.name == "John"
        assert rows[0].json_col_parsed.age in [30, 30]  # Could be int or long
        assert rows[1].json_col_parsed.name == "Jane"

    def test_parse_from_schema_complex_nested(self, spark, complex_json_dataframe):
        """Test parsing complex nested JSON with explicit schema."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<root: STRUCT<person: STRUCT<id: INT, name: STRUCT<first: STRING, last: STRING>, age: INT>>>"
        )
        
        result = instance.applyPython(spark, complex_json_dataframe)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 2
        
        # Check nested structure
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed.root.person.id == 1
        assert rows[0].json_col_parsed.root.person.name.first == "John"
        assert rows[0].json_col_parsed.root.person.name.last == "Doe"

    def test_parse_from_sample_record_complex_nested(self, spark, complex_json_dataframe):
        """Test parsing complex nested JSON using sample record."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record='{"root": {"person": {"id": 1, "name": {"first": "John", "last": "Doe"}, "age": 30}}}'
        )
        
        result = instance.applyPython(spark, complex_json_dataframe)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 2
        
        # Check nested structure
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed.root.person.id == 1
        assert rows[0].json_col_parsed.root.person.name.first == "John"

    def test_parse_from_schema_with_newlines(self, spark, sample_dataframe):
        """Test that schema with newlines is handled correctly."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<name: STRING,\nage: INT>"
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 3

    def test_parse_from_sample_record_with_newlines(self, spark, sample_dataframe):
        """Test that sample record with newlines is handled correctly."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record='{\n  "name": "John",\n  "age": 30\n}'
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 3

    def test_empty_schema_returns_unchanged(self, spark, sample_dataframe):
        """Test that empty schema returns DataFrame unchanged."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema=""
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        # Should return unchanged (no parsed column added)
        assert "json_col_parsed" not in result.columns
        assert result.count() == 3
        assert result.columns == sample_dataframe.columns

    def test_empty_sample_record_returns_unchanged(self, spark, sample_dataframe):
        """Test that empty sample record returns DataFrame unchanged."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record=""
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        # Should return unchanged
        assert "json_col_parsed" not in result.columns
        assert result.count() == 3

    def test_whitespace_only_schema_returns_unchanged(self, spark, sample_dataframe):
        """Test that whitespace-only schema returns DataFrame unchanged."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="   "
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        assert "json_col_parsed" not in result.columns

    def test_whitespace_only_sample_record_returns_unchanged(self, spark, sample_dataframe):
        """Test that whitespace-only sample record returns DataFrame unchanged."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record="   "
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        assert "json_col_parsed" not in result.columns

    def test_invalid_parsing_method_returns_unchanged(self, spark, sample_dataframe):
        """Test that invalid parsing method returns DataFrame unchanged."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="invalid_method",
            sample_schema="STRUCT<name: STRING, age: INT>"
        )
        
        result = instance.applyPython(spark, sample_dataframe)
        
        assert "json_col_parsed" not in result.columns
        assert result.count() == 3

    def test_empty_dataframe(self, spark, empty_dataframe):
        """Test parsing with empty DataFrame."""
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<name: STRING, age: INT>"
        )
        
        result = instance.applyPython(spark, empty_dataframe)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 0

    def test_parse_from_schema_with_array(self, spark):
        """Test parsing JSON with array field."""
        data = [
            ('{"tags": ["python", "spark", "sql"]}',),
        ]
        df = spark.createDataFrame(data, ['json_col'])
        
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<tags: ARRAY<STRING>>"
        )
        
        result = instance.applyPython(spark, df)
        
        assert "json_col_parsed" in result.columns
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed.tags == ["python", "spark", "sql"]

    def test_parse_from_sample_record_with_array(self, spark):
        """Test parsing JSON with array using sample record."""
        data = [
            ('{"tags": ["python", "spark"]}',),
            ('{"tags": ["java", "scala"]}',),
        ]
        df = spark.createDataFrame(data, ['json_col'])
        
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record='{"tags": ["python", "spark"]}'
        )
        
        result = instance.applyPython(spark, df)
        
        assert "json_col_parsed" in result.columns
        rows = result.select("json_col_parsed").collect()
        assert "python" in rows[0].json_col_parsed.tags
        assert "spark" in rows[0].json_col_parsed.tags

    def test_parse_from_schema_preserves_other_columns(self, spark):
        """Test that other columns are preserved in output."""
        data = [
            ('{"name": "John", "age": 30}', 1, 'A'),
            ('{"name": "Jane", "age": 25}', 2, 'B'),
        ]
        df = spark.createDataFrame(data, ['json_col', 'id', 'category'])
        
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<name: STRING, age: INT>"
        )
        
        result = instance.applyPython(spark, df)
        
        # Check all columns are present
        assert "json_col" in result.columns
        assert "id" in result.columns
        assert "category" in result.columns
        assert "json_col_parsed" in result.columns
        
        # Check data preserved
        rows = result.select("id", "category", "json_col_parsed").collect()
        assert rows[0].id == 1
        assert rows[0].category == 'A'
        assert rows[1].id == 2
        assert rows[1].category == 'B'

    def test_parse_from_schema_with_null_json(self, spark):
        """Test parsing when JSON column contains null values."""
        data = [
            ('{"name": "John", "age": 30}',),
            (None,),
            ('{"name": "Jane", "age": 25}',),
        ]
        df = spark.createDataFrame(data, ['json_col'])
        
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSchema",
            sample_schema="STRUCT<name: STRING, age: INT>"
        )
        
        result = instance.applyPython(spark, df)
        
        assert "json_col_parsed" in result.columns
        assert result.count() == 3
        
        rows = result.select("json_col_parsed").collect()
        assert rows[0].json_col_parsed is not None
        assert rows[1].json_col_parsed is None  # null input produces null output
        assert rows[2].json_col_parsed is not None

    def test_parse_from_sample_record_with_single_quotes(self, spark):
        """Test that sample record with single quotes is handled correctly."""
        # JSON with single quotes in values (edge case)
        data = [
            ('{"name": "John\'s Store", "age": 30}',),
        ]
        df = spark.createDataFrame(data, ['json_col'])
        
        instance = create_json_parse_instance(
            column_name="json_col",
            parsing_method="parseFromSampleRecord",
            sample_record='{"name": "John\'s Store", "age": 30}'
        )
        
        result = instance.applyPython(spark, df)
        
        # Should handle escaping correctly
        assert "json_col_parsed" in result.columns
        assert result.count() == 1


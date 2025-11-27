"""
Test cases for DataMasking gem's applyPython method.

To run these tests:
    cd prophecy_tests
    python run_tests.py pyspark
"""

import pytest
from unittest.mock import Mock
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from DataMasking import DataMasking


def create_gem(**kwargs):
    """Helper to create DataMasking gem with mocked props."""
    gem = DataMasking()
    gem.props = Mock()
    
    # Set property values
    gem.props.column_names = kwargs.get("column_names", [])
    gem.props.masking_method = kwargs.get("masking_method", "")
    gem.props.masked_column_add_method = kwargs.get("masked_column_add_method", "inplace_substitute")
    gem.props.upper_char_substitute = kwargs.get("upper_char_substitute", "")
    gem.props.lower_char_substitute = kwargs.get("lower_char_substitute", "")
    gem.props.digit_char_substitute = kwargs.get("digit_char_substitute", "")
    gem.props.other_char_substitute = kwargs.get("other_char_substitute", "")
    gem.props.sha2_bit_length = kwargs.get("sha2_bit_length", "256")
    gem.props.prefix_suffix_option = kwargs.get("prefix_suffix_option", "Prefix")
    gem.props.prefix_suffix_added = kwargs.get("prefix_suffix_added", "")
    gem.props.combined_hash_column_name = kwargs.get("combined_hash_column_name", "")
    
    return gem


# Test Fixtures
@pytest.fixture
def sample_df(spark):
    """Sample DataFrame with various data types."""
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("password", StringType(), True),
        StructField("id", IntegerType(), True)
    ])
    data = [("John123", "SecretPass#456", 1)]
    return spark.createDataFrame(data, schema)


# Mask Method Tests
def test_mask_default_characters(spark, sample_df):
    """Test mask method with default character substitutions (inplace)."""
    gem = create_gem(
        column_names=["name", "password"],
        masking_method="mask",
        masked_column_add_method="inplace_substitute"
    )
    
    result = gem.applyPython(spark, sample_df)
    rows = result.collect()
    
    assert len(rows) == 1
    assert rows[0].name == "Xxxxnnn"  # John123 -> Xxxxnnn
    assert rows[0].password == "XxxxxxXxxx#nnn"  # SecretPass#456
    assert rows[0].id == 1


def test_mask_custom_characters(spark):
    """Test mask method with custom character substitutions."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John123",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        masked_column_add_method="inplace_substitute",
        upper_char_substitute="A",
        lower_char_substitute="a",
        digit_char_substitute="0",
        other_char_substitute="*"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "Aaaa000"  # John123 -> Aaaa000


def test_mask_new_column(spark):
    """Test mask method creating a new column with suffix."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John123",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Suffix",
        prefix_suffix_added="_masked"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John123"  # Original unchanged
    assert rows[0].name_masked == "Xxxxnnn"  # New masked column with suffix


def test_mask_null_retains_original(spark):
    """Test mask method with 'NULL' string to retain original characters."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John123",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        upper_char_substitute="NULL",
        lower_char_substitute="NULL",
        digit_char_substitute="NULL"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John123"  # All characters retained with NULL string


def test_mask_partial_masking(spark):
    """Test mask method with only digits masked, using NULL for letters."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John123",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        upper_char_substitute="NULL",
        lower_char_substitute="NULL",
        digit_char_substitute="0"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John000"  # Only digits masked, letters retained


# Hash Method Tests
def test_hash_method(spark):
    """Test hash masking method (returns integer)."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="hash"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    # Hash returns an integer value
    assert isinstance(rows[0].name, int)
    assert rows[0].name is not None


def test_hash_multiple_columns(spark):
    """Test hash masking on multiple columns."""
    schema = StructType([
        StructField("col1", StringType(), True),
        StructField("col2", StringType(), True)
    ])
    df = spark.createDataFrame([("value1", "value2")], schema)
    
    gem = create_gem(
        column_names=["col1", "col2"],
        masking_method="hash"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].col1 != "value1"
    assert rows[0].col2 != "value2"


def test_hash_new_column(spark):
    """Test hash masking creating a new column with suffix."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="hash",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Suffix",
        prefix_suffix_added="_hashed"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John"  # Original unchanged
    assert isinstance(rows[0].name_hashed, int)  # New hashed column


# SHA2 Method Tests
def test_sha2_256_default(spark):
    """Test SHA2 masking with 256-bit (default)."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="sha2",
        sha2_bit_length="256"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    # SHA2-256 produces 64 hex characters
    assert len(rows[0].name) == 64
    assert rows[0].name.isalnum()  # Hex string


def test_sha2_512(spark):
    """Test SHA2 masking with 512-bit."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="sha2",
        sha2_bit_length="512"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    # SHA2-512 produces 128 hex characters
    assert len(rows[0].name) == 128


def test_sha2_new_column(spark):
    """Test SHA2 masking creating a new column with suffix."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="sha2",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Suffix",
        prefix_suffix_added="_sha2"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John"  # Original unchanged
    assert len(rows[0].name_sha2) == 64  # SHA2-256 hash


# Prefix/Suffix Tests
def test_mask_with_prefix(spark):
    """Test mask method with prefix creating new column."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Prefix",
        prefix_suffix_added="MASKED_"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John"  # Original unchanged
    assert rows[0].MASKED_name == "Xxxx"  # New column with prefix


def test_mask_with_suffix(spark):
    """Test mask method with suffix creating new column."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Suffix",
        prefix_suffix_added="_HIDDEN"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John"  # Original unchanged
    assert rows[0].name_HIDDEN == "Xxxx"  # New column with suffix


def test_hash_with_prefix(spark):
    """Test hash method with prefix creating new column."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="hash",
        masked_column_add_method="add_new_column",
        prefix_suffix_option="Prefix",
        prefix_suffix_added="H_"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name == "John"  # Original unchanged
    assert isinstance(rows[0].H_name, int)  # New hashed column with prefix


# Combined Hash Tests
def test_hash_combined_columns(spark):
    """Test hash masking with combined columns into a new column."""
    schema = StructType([
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True)
    ])
    df = spark.createDataFrame([("John", "Doe")], schema)
    
    gem = create_gem(
        column_names=["first_name", "last_name"],
        masking_method="hash",
        masked_column_add_method="combinedHash_substitute",
        combined_hash_column_name="full_name_hash"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].first_name == "John"  # Originals unchanged
    assert rows[0].last_name == "Doe"
    assert "full_name_hash" in result.columns
    assert isinstance(rows[0].full_name_hash, int)  # Combined hash is integer


# Edge Cases
def test_empty_dataframe(spark):
    """Test masking on empty DataFrame."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask"
    )
    
    result = gem.applyPython(spark, df)
    
    assert result.count() == 0
    assert result.columns == df.columns


def test_null_values(spark):
    """Test masking with null values."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([(None,), ("John",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert rows[0].name is None  # Null remains null
    assert rows[1].name == "Xxxx"  # John masked


def test_multiple_rows(spark):
    """Test masking on multiple rows."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",), ("Jane",), ("Bob",)], schema)
    
    gem = create_gem(
        column_names=["name"],
        masking_method="mask"
    )
    
    result = gem.applyPython(spark, df)
    rows = result.collect()
    
    assert len(rows) == 3
    assert rows[0].name == "Xxxx"
    assert rows[1].name == "Xxxx"
    assert rows[2].name == "Xxx"

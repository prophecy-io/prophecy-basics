"""Tests for the hash masking method in DataMasking gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_hash_method(spark, create_gem):
    """Test hash masking method (returns integer)."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([("John",)], schema)

    gem = create_gem(
        column_names=["name"],
        masking_method="hash"
    )

    result = gem.applyPython(spark, df)
    rows = result.collect()

    assert isinstance(rows[0].name, int)
    assert rows[0].name is not None


def test_hash_multiple_columns(spark, create_gem):
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


def test_hash_new_column(spark, create_gem):
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

    assert rows[0].name == "John"
    assert isinstance(rows[0].name_hashed, int)


def test_hash_combined_columns(spark, create_gem):
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

    assert rows[0].first_name == "John"
    assert rows[0].last_name == "Doe"
    assert "full_name_hash" in result.columns
    assert isinstance(rows[0].full_name_hash, int)

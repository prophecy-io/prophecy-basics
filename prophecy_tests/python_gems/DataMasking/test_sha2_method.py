"""Tests for the SHA2 masking method in DataMasking gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_sha2_256_default(spark, create_gem):
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

    assert len(rows[0].name) == 64
    assert rows[0].name.isalnum()


def test_sha2_512(spark, create_gem):
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

    assert len(rows[0].name) == 128


def test_sha2_new_column(spark, create_gem):
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

    assert rows[0].name == "John"
    assert len(rows[0].name_sha2) == 64

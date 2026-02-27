"""Tests for prefix/suffix column creation in DataMasking gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_mask_with_prefix(spark, create_gem):
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

    assert rows[0].name == "John"
    assert rows[0].MASKED_name == "Xxxx"


def test_mask_with_suffix(spark, create_gem):
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

    assert rows[0].name == "John"
    assert rows[0].name_HIDDEN == "Xxxx"


def test_hash_with_prefix(spark, create_gem):
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

    assert rows[0].name == "John"
    assert isinstance(rows[0].H_name, int)

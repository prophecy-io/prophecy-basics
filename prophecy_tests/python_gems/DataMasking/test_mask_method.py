"""Tests for the mask masking method in DataMasking gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_mask_default_characters(spark, sample_df, create_gem):
    """Test mask method with default character substitutions (inplace)."""
    gem = create_gem(
        column_names=["name", "password"],
        masking_method="mask",
        masked_column_add_method="inplace_substitute"
    )

    result = gem.applyPython(spark, sample_df)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0].name == "Xxxxnnn"
    assert rows[0].password == "XxxxxxXxxx#nnn"
    assert rows[0].id == 1


def test_mask_custom_characters(spark, create_gem):
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

    assert rows[0].name == "Aaaa000"


def test_mask_new_column(spark, create_gem):
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

    assert rows[0].name == "John123"
    assert rows[0].name_masked == "Xxxxnnn"


def test_mask_null_retains_original(spark, create_gem):
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

    assert rows[0].name == "John123"


def test_mask_partial_masking(spark, create_gem):
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

    assert rows[0].name == "John000"

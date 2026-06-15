"""Tests for edge cases in DataMasking gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_empty_dataframe(spark, create_gem):
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


def test_null_values(spark, create_gem):
    """Test masking with null values."""
    schema = StructType([StructField("name", StringType(), True)])
    df = spark.createDataFrame([(None,), ("John",)], schema)

    gem = create_gem(
        column_names=["name"],
        masking_method="mask"
    )

    result = gem.applyPython(spark, df)
    rows = result.collect()

    assert rows[0].name is None
    assert rows[1].name == "Xxxx"


def test_multiple_rows(spark, create_gem):
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

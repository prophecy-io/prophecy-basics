"""Tests for whitespace operations in DataCleansing gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_trim_whitespace(spark, sample_df, create_gem):
    """Trim leading and trailing whitespace."""
    gem = create_gem(
        columnNames=["name", "description"],
        trimWhiteSpace=True
    )
    result = gem.applyPython(spark, sample_df)

    rows = result.collect()
    assert rows[0].name == "Alice"
    assert rows[0].description == "Hello, World!"


def test_remove_all_whitespace(spark, create_gem):
    """Remove all whitespace from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello World",), ("Test String",)], schema)

    gem = create_gem(columnNames=["text"], allWhiteSpace=True)
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "HelloWorld"
    assert rows[1].text == "TestString"

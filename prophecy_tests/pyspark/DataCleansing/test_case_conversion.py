"""Tests for case conversion operations in DataCleansing gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_lowercase(spark, create_gem):
    """Convert strings to lowercase."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("HELLO",), ("WORLD",)], schema)

    gem = create_gem(columnNames=["text"], modifyCase="makeLowercase")
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "hello"
    assert rows[1].text == "world"


def test_uppercase(spark, create_gem):
    """Convert strings to uppercase."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("hello",), ("world",)], schema)

    gem = create_gem(columnNames=["text"], modifyCase="makeUppercase")
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "HELLO"
    assert rows[1].text == "WORLD"


def test_titlecase(spark, create_gem):
    """Convert strings to title case."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("hello world",), ("test string",)], schema)

    gem = create_gem(columnNames=["text"], modifyCase="makeTitlecase")
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "Test String"

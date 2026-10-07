"""Tests for character cleaning operations in DataCleansing gem."""

from pyspark.sql.types import StructType, StructField, StringType


def test_clean_letters(spark, create_gem):
    """Remove letters from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello123",), ("Test456",)], schema)

    gem = create_gem(columnNames=["text"], cleanLetters=True)
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "123"
    assert rows[1].text == "456"


def test_clean_numbers(spark, create_gem):
    """Remove numbers from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello123",), ("Test456",)], schema)

    gem = create_gem(columnNames=["text"], cleanNumbers=True)
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "Hello"
    assert rows[1].text == "Test"


def test_clean_punctuations(spark, create_gem):
    """Remove punctuation from strings."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("Hello, World!",), ("Test-123",)], schema)

    gem = create_gem(columnNames=["text"], cleanPunctuations=True)
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "Hello World"
    assert rows[1].text == "Test123"

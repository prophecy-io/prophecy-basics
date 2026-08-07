"""Tests for combined operations and edge cases in DataCleansing gem."""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def test_combined_operations(spark, create_gem):
    """Multiple operations applied together."""
    schema = StructType([StructField("text", StringType(), True)])
    df = spark.createDataFrame([("  Hello123!  ",), ("  TEST456@  ",)], schema)

    gem = create_gem(
        columnNames=["text"],
        trimWhiteSpace=True,
        cleanNumbers=True,
        cleanPunctuations=True,
        modifyCase="makeLowercase"
    )
    result = gem.applyPython(spark, df)

    rows = result.collect()
    assert rows[0].text == "hello"
    assert rows[1].text == "test"


def test_only_selected_columns(spark, sample_df, create_gem):
    """Only selected columns are cleansed, others remain unchanged."""
    gem = create_gem(
        columnNames=["name"],
        trimWhiteSpace=True,
        modifyCase="makeUppercase"
    )
    result = gem.applyPython(spark, sample_df)

    rows = result.collect()
    assert rows[0].name == "ALICE"
    assert rows[0].description == "Hello, World!"


def test_empty_dataframe(spark, create_gem):
    """Empty DataFrame is handled correctly."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema)

    gem = create_gem(columnNames=["name"], trimWhiteSpace=True)
    result = gem.applyPython(spark, empty_df)

    assert result.count() == 0
    assert result.columns == empty_df.columns

"""Tests for null handling operations in DataCleansing gem."""


def test_no_operations(spark, sample_df, create_gem):
    """DataFrame remains unchanged when no operations are applied."""
    gem = create_gem()
    result = gem.applyPython(spark, sample_df)

    assert result.count() == sample_df.count()
    assert result.columns == sample_df.columns


def test_remove_all_null_rows(spark, all_nulls_df, create_gem):
    """Remove rows where all columns are null."""
    gem = create_gem(removeRowNullAllCols=True)
    result = gem.applyPython(spark, all_nulls_df)

    assert result.count() == 1
    row = result.collect()[0]
    assert row.col1 == "value"
    assert row.col2 == 1


def test_replace_null_text(spark, sample_df, create_gem):
    """Replace null values in string columns."""
    gem = create_gem(
        columnNames=["name", "description"],
        replaceNullTextFields=True,
        replaceNullTextWith="UNKNOWN"
    )
    result = gem.applyPython(spark, sample_df)

    rows = result.collect()
    assert rows[1].name == "UNKNOWN"
    assert rows[3].description == "UNKNOWN"


def test_replace_null_numeric(spark, sample_df, create_gem):
    """Replace null values in numeric columns."""
    gem = create_gem(
        columnNames=["age", "salary"],
        replaceNullForNumericFields=True,
        replaceNullNumericWith=999
    )
    result = gem.applyPython(spark, sample_df)

    rows = result.collect()
    assert rows[2].age == 999
    assert rows[1].salary == 999.0

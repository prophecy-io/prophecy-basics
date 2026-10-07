"""Fixtures for the Finance gem tests.

Expected values throughout this suite come from Excel's finance functions, and every
one of them was also produced by running macros/Finance.sql on DuckDB. A failure here
therefore means applyPython drifted away from either Excel or the SQL macro.
"""

import pytest
from pyspark.sql import Row
from Finance import Finance


@pytest.fixture
def create_gem():
    """Build a Finance gem backed by real (not mocked) properties.

    Using the actual dataclass means a test that misspells a property name fails
    loudly instead of silently exercising a default.
    """
    def _create(**kwargs):
        gem = Finance()
        gem.props = Finance.FinanceProperties(**kwargs)
        return gem
    return _create


@pytest.fixture
def one_row(spark, create_gem):
    """Run the gem over a single row of inputs and hand back the one result value."""
    def _run(inputs, **props):
        df = spark.createDataFrame([Row(**inputs)])
        gem = create_gem(outputColumn="finance_result", **props)
        return gem.applyPython(spark, df).collect()[0]["finance_result"]
    return _run


@pytest.fixture
def many_rows(spark, create_gem):
    """Run the gem over several rows and hand back the result column."""
    def _run(rows, **props):
        df = spark.createDataFrame([Row(**r) for r in rows])
        gem = create_gem(outputColumn="finance_result", **props)
        return [r["finance_result"] for r in gem.applyPython(spark, df).collect()]
    return _run

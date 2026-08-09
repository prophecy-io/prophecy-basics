"""The gem's solvers and the SQL macro must agree to the last digit.

Both run the same fixed-round bisection, so being merely close is not good enough.
The constants below were produced by running macros/Finance.sql on DuckDB. If this
file starts failing, applyPython and the macro have drifted apart.
"""

import datetime as dt
import pytest

IRR_FROM_SQL = 0.23375192852825866
XIRR_FROM_SQL = 0.09999999999999998
RATE_FROM_SQL = 0.05000067582742894

FLOWS = {"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0}


def test_irr_matches_the_macro_exactly(one_row):
    got = one_row(FLOWS, functionType="IRR",
                  valueColumns=["out", "f1", "f2", "f3"], nIterCol="60")
    assert got == IRR_FROM_SQL


def test_xirr_matches_the_macro_exactly(one_row):
    got = one_row({"out": -1000.0, "cash": 1100.0,
                   "d0": dt.date(2024, 1, 1), "d1": dt.date(2024, 12, 31)},
                  functionType="XIRR", valueColumns=["out", "cash"],
                  dateColumns=["d0", "d1"], nIterCol="60")
    assert got == XIRR_FROM_SQL


def test_rate_matches_the_macro_exactly(one_row):
    got = one_row({"nper": 10.0, "pmt": -1295.05, "pv": 10000.0, "fv": 0.0},
                  functionType="Rate",
                  nperCol="nper", pmtCol="pmt", pvCol="pv", fvCol="fv", nIterCol="60")
    assert got == RATE_FROM_SQL


@pytest.mark.parametrize("iterations", ["", "0", "-5"])
def test_a_missing_iteration_count_means_sixty(one_row, iterations):
    """The macro falls back to 60 rounds, so the gem has to make the same choice."""
    got = one_row(FLOWS, functionType="IRR",
                  valueColumns=["out", "f1", "f2", "f3"], nIterCol=iterations)
    assert got == IRR_FROM_SQL


def test_default_bracket_matches_the_macro(one_row):
    """Blank bounds mean -0.99 and 10 on both sides."""
    blank = one_row(FLOWS, functionType="IRR", valueColumns=["out", "f1", "f2", "f3"])
    spelled_out = one_row(FLOWS, functionType="IRR",
                          valueColumns=["out", "f1", "f2", "f3"],
                          loBoundCol="-0.99", hiBoundCol="10")
    assert blank == spelled_out == IRR_FROM_SQL


def test_an_unsupported_function_yields_null(one_row):
    """The macro emits NULL for a function it does not know; the gem must agree."""
    assert one_row({"a": 1.0}, functionType="NotAFunction") is None


def test_the_output_column_is_appended_without_dropping_inputs(spark, create_gem):
    """The macro does SELECT *, <expr>, so every input column has to survive."""
    from pyspark.sql import Row
    df = spark.createDataFrame([Row(rate=0.05, nper=10.0, pmt=-100.0, pv=-1000.0)])
    gem = create_gem(functionType="FV", outputColumn="my_result",
                     rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    out = gem.applyPython(spark, df)
    assert out.columns == ["rate", "nper", "pmt", "pv", "my_result"]

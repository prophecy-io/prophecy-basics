"""Tests for NPV, XNPV, MIRR and MXIRR in the Finance gem.

XNPV and MXIRR discount by the actual number of days between dates, so these cases
also cover the date handling.
"""

import datetime as dt
import pytest


def test_net_present_value(one_row):
    """100, 200 and 300 discounted at 10% are worth 481.59 today."""
    got = one_row({"rate": 0.10, "cf1": 100.0, "cf2": 200.0, "cf3": 300.0},
                  functionType="NPV",
                  rateCol="rate", valueColumns=["cf1", "cf2", "cf3"])
    assert got == pytest.approx(481.5928, abs=1e-3)


def test_net_present_value_discounts_the_first_flow(one_row):
    """NPV treats the first column as one period away, so it is not taken at face value."""
    got = one_row({"rate": 0.10, "cf1": 110.0}, functionType="NPV",
                  rateCol="rate", valueColumns=["cf1"])
    assert got == pytest.approx(100.0, abs=1e-9)


def test_a_field_may_hold_a_literal_instead_of_a_column(one_row):
    """Fields are read as expressions, so a typed-in rate works like a picked column."""
    from_column = one_row({"rate": 0.10, "cf1": 110.0}, functionType="NPV",
                          rateCol="rate", valueColumns=["cf1"])
    from_literal = one_row({"rate": 0.10, "cf1": 110.0}, functionType="NPV",
                           rateCol="0.10", valueColumns=["cf1"])
    assert from_literal == pytest.approx(from_column, abs=1e-9)


def test_dated_net_present_value_uses_actual_days(one_row):
    """A flow dated later is discounted further, purely because of the date gap."""
    near = one_row({"out": -1000.0, "cash": 1100.0, "rate": 0.10,
                    "d0": dt.date(2024, 1, 1), "d1": dt.date(2024, 12, 31)},
                   functionType="XNPV", rateCol="rate",
                   valueColumns=["out", "cash"], dateColumns=["d0", "d1"])
    far = one_row({"out": -1000.0, "cash": 1100.0, "rate": 0.10,
                   "d0": dt.date(2024, 1, 1), "d1": dt.date(2025, 12, 31)},
                  functionType="XNPV", rateCol="rate",
                  valueColumns=["out", "cash"], dateColumns=["d0", "d1"])
    assert near == pytest.approx(0.0, abs=1e-6)
    assert far < near


def test_modified_internal_rate_of_return(one_row):
    """-1000 then 400, 500 and 600 at 10% finance and 12% reinvest gives 18.45%."""
    got = one_row({"out": -1000.0, "f1": 400.0, "f2": 500.0, "f3": 600.0,
                   "fin": 0.10, "rei": 0.12},
                  functionType="MIRR", valueColumns=["out", "f1", "f2", "f3"],
                  financeRateCol="fin", reinvestRateCol="rei")
    assert got == pytest.approx(0.184466, abs=1e-5)


def test_dated_modified_internal_rate_of_return(one_row):
    """-1000 today against 1200 in 366 days gives 19.94%."""
    got = one_row({"out": -1000.0, "cash": 1200.0, "fin": 0.10, "rei": 0.12,
                   "d0": dt.date(2024, 1, 1), "d1": dt.date(2025, 1, 1)},
                  functionType="MXIRR", valueColumns=["out", "cash"],
                  dateColumns=["d0", "d1"],
                  financeRateCol="fin", reinvestRateCol="rei")
    assert got == pytest.approx(0.199402, abs=1e-5)


def test_mirr_separates_inflows_from_outflows(one_row):
    """Only negative flows are financed, so a larger outflow lowers the return."""
    small = one_row({"out": -1000.0, "f1": 400.0, "f2": 500.0, "f3": 600.0,
                     "fin": 0.10, "rei": 0.12},
                    functionType="MIRR", valueColumns=["out", "f1", "f2", "f3"],
                    financeRateCol="fin", reinvestRateCol="rei")
    large = one_row({"out": -1500.0, "f1": 400.0, "f2": 500.0, "f3": 600.0,
                     "fin": 0.10, "rei": 0.12},
                    functionType="MIRR", valueColumns=["out", "f1", "f2", "f3"],
                    financeRateCol="fin", reinvestRateCol="rei")
    assert large < small

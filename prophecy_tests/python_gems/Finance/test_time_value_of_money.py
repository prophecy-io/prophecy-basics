"""Tests for the FV, PV, PMT and NPER functions in the Finance gem.

Sign convention, matching Excel: money leaving your pocket is negative and money
arriving is positive.
"""

import pytest

LOAN = {"rate": 0.05, "nper": 10.0, "pmt": -100.0, "pv": -1000.0, "fv": 0.0}


def test_future_value(one_row):
    """1000 down plus 100 per period for 10 periods at 5% grows to 2886.68."""
    got = one_row(LOAN, functionType="FV",
                  rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    assert got == pytest.approx(2886.6839, abs=1e-3)


def test_future_value_at_zero_rate(one_row):
    """With no interest the balance is just the sum of the deposits."""
    got = one_row({"rate": 0.0, "nper": 10.0, "pmt": -100.0, "pv": -1000.0},
                  functionType="FV",
                  rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    assert got == pytest.approx(2000.0, abs=1e-9)


def test_present_value(one_row):
    """Ten payments of 100 at 5% are worth 772.17 today."""
    got = one_row(LOAN, functionType="PV",
                  rateCol="rate", nperCol="nper", pmtCol="pmt", fvCol="fv")
    assert got == pytest.approx(772.1735, abs=1e-3)


def test_payment(one_row):
    """Clearing a 10000 loan over 10 periods at 5% costs 1295.05 per period."""
    got = one_row({"rate": 0.05, "nper": 10.0, "pv": 10000.0, "fv": 0.0},
                  functionType="PMT",
                  rateCol="rate", nperCol="nper", pvCol="pv", fvCol="fv")
    assert got == pytest.approx(-1295.0457, abs=1e-3)


def test_number_of_periods(one_row):
    """NPER inverts PMT: that same payment implies 10 periods."""
    got = one_row({"rate": 0.05, "pmt": -1295.05, "pv": 10000.0, "fv": 0.0},
                  functionType="NPER",
                  rateCol="rate", pmtCol="pmt", pvCol="pv", fvCol="fv")
    assert got == pytest.approx(10.0, abs=1e-3)


def test_payment_at_period_start(one_row):
    """Paying at the start of each period earns one extra period of interest."""
    at_end = one_row(LOAN, functionType="FV", paymentType="0",
                     rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    at_start = one_row(LOAN, functionType="FV", paymentType="1",
                       rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    assert at_start > at_end
    assert at_start == pytest.approx(at_end + 100.0 * 0.05
                                     * ((1.05 ** 10 - 1) / 0.05), abs=1e-6)


def test_each_row_is_computed_independently(many_rows):
    """The gem is a plain column expression, so rows must not influence each other."""
    got = many_rows(
        [{"rate": 0.05, "nper": 10.0, "pmt": -100.0, "pv": -1000.0},
         {"rate": 0.10, "nper": 5.0, "pmt": -200.0, "pv": -500.0}],
        functionType="FV", rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    assert got[0] == pytest.approx(2886.6839, abs=1e-3)
    assert got[1] == pytest.approx(500 * 1.1 ** 5 + 200 * ((1.1 ** 5 - 1) / 0.1), abs=1e-3)


def test_null_input_yields_null(many_rows):
    """A null anywhere in the inputs produces a null result, not an error."""
    got = many_rows(
        [{"rate": 0.05, "nper": 10.0, "pmt": -100.0, "pv": -1000.0},
         {"rate": None, "nper": 10.0, "pmt": -100.0, "pv": -1000.0}],
        functionType="FV", rateCol="rate", nperCol="nper", pmtCol="pmt", pvCol="pv")
    assert got[0] is not None
    assert got[1] is None

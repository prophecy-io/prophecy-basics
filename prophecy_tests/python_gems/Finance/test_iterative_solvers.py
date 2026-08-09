"""Tests for the bisection solvers (IRR, XIRR, Rate) in the Finance gem."""

import datetime as dt
import pytest


def test_internal_rate_of_return(one_row):
    """-1000 followed by three inflows of 500 returns 23.375%."""
    got = one_row({"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0},
                  functionType="IRR", valueColumns=["out", "f1", "f2", "f3"])
    assert got == pytest.approx(0.233752, abs=1e-5)


def test_irr_is_the_rate_that_zeroes_npv(one_row):
    """Discounting the same flows at the IRR must leave nothing behind."""
    flows = {"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0}
    irr = one_row(flows, functionType="IRR", valueColumns=["out", "f1", "f2", "f3"])
    # NPV puts the first column one period out, so discount the tail against the outflow.
    npv_of_tail = one_row({**flows, "r": irr}, functionType="NPV",
                          rateCol="r", valueColumns=["f1", "f2", "f3"])
    assert npv_of_tail == pytest.approx(1000.0, abs=1e-4)


def test_dated_internal_rate_of_return(one_row):
    """-1000 today against 1100 in 365 days is exactly 10% a year."""
    got = one_row({"out": -1000.0, "cash": 1100.0,
                   "d0": dt.date(2024, 1, 1), "d1": dt.date(2024, 12, 31)},
                  functionType="XIRR", valueColumns=["out", "cash"],
                  dateColumns=["d0", "d1"])
    assert got == pytest.approx(0.10, abs=1e-6)


def test_rate(one_row):
    """The rate hidden in a 10000 loan repaid at 1295.05 for 10 periods is 5%."""
    got = one_row({"nper": 10.0, "pmt": -1295.05, "pv": 10000.0, "fv": 0.0},
                  functionType="Rate",
                  nperCol="nper", pmtCol="pmt", pvCol="pv", fvCol="fv")
    assert got == pytest.approx(0.05, abs=1e-4)


def test_rate_inverts_pmt(one_row):
    """PMT and Rate are inverses of one another."""
    payment = one_row({"rate": 0.07, "nper": 12.0, "pv": 25000.0, "fv": 0.0},
                      functionType="PMT",
                      rateCol="rate", nperCol="nper", pvCol="pv", fvCol="fv")
    recovered = one_row({"nper": 12.0, "pmt": payment, "pv": 25000.0, "fv": 0.0},
                        functionType="Rate",
                        nperCol="nper", pmtCol="pmt", pvCol="pv", fvCol="fv")
    assert recovered == pytest.approx(0.07, abs=1e-6)


def test_more_iterations_narrow_the_answer(one_row):
    """The bracket halves each round, so more rounds sit closer to the true rate."""
    flows = {"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0}
    coarse = one_row(flows, functionType="IRR",
                     valueColumns=["out", "f1", "f2", "f3"], nIterCol="8")
    fine = one_row(flows, functionType="IRR",
                   valueColumns=["out", "f1", "f2", "f3"], nIterCol="60")
    assert abs(fine - 0.233752) < abs(coarse - 0.233752)


def test_a_tighter_bracket_still_finds_the_root(one_row):
    """Narrowing the search range must not move the answer."""
    flows = {"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0}
    wide = one_row(flows, functionType="IRR", valueColumns=["out", "f1", "f2", "f3"])
    tight = one_row(flows, functionType="IRR", valueColumns=["out", "f1", "f2", "f3"],
                    loBoundCol="0.0", hiBoundCol="1.0")
    assert tight == pytest.approx(wide, abs=1e-6)


def test_null_cash_flow_yields_null(many_rows):
    """A null in the series produces null rather than a bogus rate."""
    got = many_rows(
        [{"out": -1000.0, "f1": 500.0, "f2": 500.0, "f3": 500.0},
         {"out": -1000.0, "f1": None, "f2": 500.0, "f3": 500.0}],
        functionType="IRR", valueColumns=["out", "f1", "f2", "f3"])
    assert got[0] == pytest.approx(0.233752, abs=1e-5)
    assert got[1] is None

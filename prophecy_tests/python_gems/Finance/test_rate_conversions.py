"""Tests for CAGR, EffectiveRate, NominalRate and FVSchedule in the Finance gem."""

import pytest


def test_compound_annual_growth_rate(one_row):
    """10000 growing to 19000 over 5 periods is 13.697% a period."""
    got = one_row({"begin": 10000.0, "end": 19000.0, "periods": 5.0},
                  functionType="CAGR",
                  beginValueCol="begin", endValueCol="end", periodsCol="periods")
    assert got == pytest.approx(0.136974, abs=1e-5)


def test_cagr_with_zero_beginning_value_is_null(one_row):
    """Growth from nothing is undefined rather than an error."""
    got = one_row({"begin": 0.0, "end": 19000.0, "periods": 5.0},
                  functionType="CAGR",
                  beginValueCol="begin", endValueCol="end", periodsCol="periods")
    assert got is None


def test_effective_rate(one_row):
    """12% nominal compounded monthly is 12.6825% effective."""
    got = one_row({"nominal": 0.12, "npery": 12.0},
                  functionType="EffectiveRate",
                  nominalRateCol="nominal", nperyCol="npery")
    assert got == pytest.approx(0.126825, abs=1e-6)


def test_nominal_rate(one_row):
    """NominalRate inverts EffectiveRate, landing back on 12%."""
    got = one_row({"effect": 0.126825, "npery": 12.0},
                  functionType="NominalRate",
                  effectRateCol="effect", nperyCol="npery")
    assert got == pytest.approx(0.12, abs=1e-5)


def test_rate_conversions_round_trip(one_row):
    """Converting nominal to effective and back must return the original rate."""
    effective = one_row({"nominal": 0.08, "npery": 4.0},
                        functionType="EffectiveRate",
                        nominalRateCol="nominal", nperyCol="npery")
    nominal = one_row({"effect": effective, "npery": 4.0},
                      functionType="NominalRate",
                      effectRateCol="effect", nperyCol="npery")
    assert nominal == pytest.approx(0.08, abs=1e-9)


def test_future_value_schedule(one_row):
    """1000 compounded at 5%, then 6%, then 7% ends at 1190.91."""
    got = one_row({"principal": 1000.0, "r1": 0.05, "r2": 0.06, "r3": 0.07},
                  functionType="FVSchedule",
                  principalCol="principal", valueColumns=["r1", "r2", "r3"])
    assert got == pytest.approx(1190.91, abs=1e-2)


def test_future_value_schedule_applies_rates_in_order(one_row):
    """Multiplication commutes, so reordering the same rates cannot change the total."""
    forward = one_row({"principal": 1000.0, "r1": 0.05, "r2": 0.06, "r3": 0.07},
                      functionType="FVSchedule",
                      principalCol="principal", valueColumns=["r1", "r2", "r3"])
    reversed_ = one_row({"principal": 1000.0, "r1": 0.05, "r2": 0.06, "r3": 0.07},
                        functionType="FVSchedule",
                        principalCol="principal", valueColumns=["r3", "r2", "r1"])
    assert forward == pytest.approx(reversed_, abs=1e-9)

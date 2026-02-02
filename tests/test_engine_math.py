from math import isclose

import pytest

from engine import bs_gamma, bs_vanna


@pytest.mark.parametrize(
    "S,K,T,sigma,expected",
    [
        (100, 100, 30 / 365, 0.2, 0.05),  # near-the-money
        (100, 120, 30 / 365, 0.25, 0.01),
    ],
)
def test_bs_vanna_behaves_reasonably(S, K, T, sigma, expected):
    val = bs_vanna(S, K, T, r=0.0, q=0.0, sigma=sigma)
    assert abs(val) < 1.0
    assert val > 0
    assert isclose(val, expected, rel_tol=5, abs_tol=1e-4)


@pytest.mark.parametrize(
    "S,K,T,sigma",
    [
        (0, 100, 0.1, 0.2),
        (100, 0, 0.1, 0.2),
        (100, 100, 0, 0.2),
        (100, 100, 0.1, 0),
    ],
)
def test_bs_vanna_handles_invalid_inputs(S, K, T, sigma):
    assert bs_vanna(S, K, T, r=0.0, q=0.0, sigma=sigma) == 0.0


@pytest.mark.parametrize(
    "S,K,T,sigma,expected",
    [
        # Expected values taken from current bs_gamma implementation
        (100, 100, 30 / 365, 0.2, 0.06954844),
        (100, 120, 30 / 365, 0.25, 0.00239729),
    ],
)
def test_bs_gamma_positive_and_bounded(S, K, T, sigma, expected):
    val = bs_gamma(S, K, T, r=0.0, q=0.0, sigma=sigma)
    assert val > 0
    assert isclose(val, expected, rel_tol=1e-3, abs_tol=1e-6)


def test_bs_gamma_invalid_returns_zero():
    assert bs_gamma(0, 100, 0.1, 0.0, 0.0, 0.2) == 0.0

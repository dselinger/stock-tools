from datetime import datetime, timezone

from core.cache import cache_key
from core.demo_data import demo_expiries, demo_gex_result, demo_vanna_result
from core.gamma_math import classify_gamma_regime, spot_vs_zero_gamma_pct
from core.web import scanner_max_workers


def test_cache_key_is_stable_and_normalized():
    key1 = cache_key(
        mode="vanna", symbol="spx", pct_window=0.1, next_only=True, expiry="2025-01-17"
    )
    key2 = cache_key(
        mode="vanna", symbol="SPX", pct_window=0.1000001, next_only=True, expiry="2025-01-17"
    )
    assert key1 == key2
    assert "SPX" in key1


def test_scanner_max_workers_bounds():
    assert scanner_max_workers(0) == 1
    assert scanner_max_workers(2) >= 1


def test_cache_key_tracks_gamma_schema_inputs():
    key = cache_key(
        mode="g",
        symbol="slv",
        pct_window=0.2,
        next_only=False,
        expiry="2026-03-13",
        expiry_mode="all",
        include_0dte=False,
        calc_version="gamma-v2",
    )

    assert "SLV" in key
    assert key.endswith(":0:::gamma-v2")


def test_gamma_regime_helpers():
    assert classify_gamma_regime(101.0, 100.0, 10.0) == "Long Gamma"
    assert classify_gamma_regime(99.0, 100.0, -10.0) == "Short Gamma"
    assert classify_gamma_regime(100.05, 100.0, 1e-8) == "Gamma Neutral"
    assert round(spot_vs_zero_gamma_pct(102.0, 100.0), 2) == 2.0


def test_demo_expiries_are_sorted_and_upcoming():
    expiries = demo_expiries("NVDA")

    assert expiries == sorted(expiries)
    assert len(expiries) >= 3
    today = datetime.now(timezone.utc).date().isoformat()
    assert all(expiry >= today for expiry in expiries)


def test_demo_payloads_use_current_demo_expiry_list():
    expiries = demo_expiries("NVDA")

    gex = demo_gex_result("NVDA")
    vanna = demo_vanna_result("NVDA")

    assert gex["meta"]["expiry"] in expiries
    assert vanna["meta"]["expiry"] in expiries

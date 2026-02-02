from core.cache import cache_key, recompute_flip_from_arrays, recompute_micro_flip_from_arrays
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


def test_recompute_flip_from_arrays():
    strikes = [100, 101, 102]
    gnet = [-5, 2, 6]
    val = recompute_flip_from_arrays(strikes, gnet)
    assert val is not None
    assert 100 <= val <= 102


def test_recompute_micro_flip_from_arrays():
    strikes = [100, 101, 102]
    gnet = [-2, -1, 3]
    val = recompute_micro_flip_from_arrays(strikes, gnet)
    assert val is not None
    assert 100 <= val <= 102

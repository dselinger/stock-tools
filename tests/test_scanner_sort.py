from core.web import sort_scanner_results


def test_sort_scanner_results_natural_order():
    results = [
        {"symbol": "BBB", "score": 1},
        {"symbol": "AAA", "score": 3},
        {"symbol": "CCC", "score": 2},
    ]
    tickers = ["AAA", "BBB", "CCC"]
    out = sort_scanner_results(results, tickers=tickers, sort_mode="natural")
    assert [r["symbol"] for r in out] == ["AAA", "BBB", "CCC"]


def test_sort_scanner_results_numeric_sorting_handles_none():
    results = [
        {"symbol": "AAA", "spot": 5},
        {"symbol": "BBB", "spot": 2},
        {"symbol": "CCC", "spot": None},
    ]
    out = sort_scanner_results(results, sort_mode="asc", sort_key="spot")
    assert [r["symbol"] for r in out] == ["BBB", "AAA", "CCC"]

    out_desc = sort_scanner_results(results, sort_mode="desc", sort_key="spot")
    assert [r["symbol"] for r in out_desc] == ["AAA", "BBB", "CCC"]


def test_sort_scanner_results_score_default():
    results = [
        {"symbol": "AAA", "score": -1.2},
        {"symbol": "BBB", "score": 0.2},
        {"symbol": "CCC", "score": 3},
    ]
    out = sort_scanner_results(results, sort_mode="score")
    assert [r["symbol"] for r in out] == ["CCC", "AAA", "BBB"]

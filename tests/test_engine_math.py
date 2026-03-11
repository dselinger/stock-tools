from datetime import date, timedelta
from math import isclose

import pytest

import views
from core.cache import cache_key
from core.gamma_math import gamma_solver_cache_token
from engine import (
    Job,
    _normalize_snapshot_chain_row,
    bs_gamma,
    bs_vanna,
    compute_gex_for_ticker,
    list_option_contracts,
)


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


def _fake_contracts(total: int = 2101, expiries: int = 11) -> list[dict]:
    rows = []
    base = date(2026, 3, 13)
    expiry_vals = [(base + timedelta(days=7 * idx)).isoformat() for idx in range(expiries)]
    for idx in range(total):
        expiry = expiry_vals[idx % len(expiry_vals)]
        strike = 95 + (idx % 11)
        opt_type = "call" if idx % 2 == 0 else "put"
        rows.append(
            {
                "ticker": f"OPT{idx:05d}",
                "strike_price": strike,
                "expiration_date": expiry,
                "contract_type": opt_type,
            }
        )
    return rows


@pytest.mark.asyncio
async def test_list_option_contracts_paginates_beyond_2000(monkeypatch):
    async def fake_polygon_get(client, path, params):
        cursor = params.get("cursor")
        if not cursor:
            return {
                "results": [{"ticker": f"A{i}", "expiration_date": "2026-03-13"} for i in range(1000)],
                "nextCursor": "page-2",
            }
        if cursor == "page-2":
            return {
                "results": [{"ticker": f"B{i}", "expiration_date": "2026-04-17"} for i in range(1000)],
                "nextCursor": "page-3",
            }
        return {
            "results": [{"ticker": f"C{i}", "expiration_date": "2026-05-15"} for i in range(1000)],
        }

    monkeypatch.setattr("engine.polygon_get", fake_polygon_get)

    contracts, diagnostics = await list_option_contracts(
        None,
        "MU",
        limit=None,
        return_diagnostics=True,
    )

    assert len(contracts) == 3000
    assert diagnostics["total_contracts_fetched"] == 3000
    assert diagnostics["total_expirations_fetched"] == 3
    assert diagnostics["provider_page_count"] == 3
    assert diagnostics["pagination_completed"] is True
    assert diagnostics["provider_truncation"] is False


@pytest.mark.asyncio
async def test_compute_gex_for_ticker_aggregate_mode_uses_all_fetched_contracts(monkeypatch):
    contracts = _fake_contracts()

    async def fake_list_option_contracts(client, underlying, limit=2000, return_diagnostics=False, max_pages=100):
        data = contracts if underlying == "MU" else []
        diag = {
            "underlying": underlying,
            "total_contracts_fetched": len(data),
            "total_expirations_fetched": len({row["expiration_date"] for row in data}),
            "pagination_completed": True,
            "provider_page_count": 3 if underlying == "MU" else 0,
            "provider_truncation": False,
            "limit_applied": limit,
        }
        return (data, diag) if return_diagnostics else data

    async def fake_fetch_aggregate_gamma_chain(
        client,
        symbol,
        *,
        spot,
        underlyings,
        contracts_by_underlying,
        provider_listing,
        max_pages_per_expiry=25,
        page_limit=250,
    ):
        rows = [
            {
                "ticker": row["ticker"],
                "strike": float(row["strike_price"]),
                "option_type": row["contract_type"],
                "oi": 10.0,
                "iv": 0.25,
                "expiry": row["expiration_date"],
                "t_years": 30 / 365,
                "contract_size": 100.0,
            }
            for row in contracts
        ]
        return rows, {
            "total_snapshot_rows": len(rows),
            "available_expirations": sorted({row["expiration_date"] for row in contracts}),
            "provider_page_count": 11,
            "pagination_completed": True,
            "provider_truncation": False,
        }

    monkeypatch.setattr("engine.list_option_contracts", fake_list_option_contracts)
    monkeypatch.setattr("engine.fetch_aggregate_gamma_chain", fake_fetch_aggregate_gamma_chain)

    job = Job(job_id="gex-test", session_id="test")
    df, meta = await compute_gex_for_ticker(
        job,
        "MU",
        100.0,
        pct_window=0.15,
        only_next_expiry=False,
        expiry_mode="all",
        include_0dte=True,
        remove_0dte=False,
        allowed_expiries=None,
        include_solver_curve=False,
    )

    provider = meta["provider_listing"]
    assert not df.empty
    assert provider["total_contracts_fetched"] == len(contracts)
    assert provider["total_expirations_fetched"] == 11
    assert provider["pagination_completed"] is True
    assert provider["provider_truncation"] is False
    assert meta["zero_gamma_diagnostics"]["total_contracts_fetched"] == len(contracts)
    assert len(meta["included_expirations"]) == 11
    assert provider["snapshot_fetch"]["total_snapshot_rows"] == len(contracts)


@pytest.mark.asyncio
async def test_scanner_summary_matches_ticker_with_expanded_contract_universe(monkeypatch):
    contracts = _fake_contracts()

    async def fake_list_option_contracts(client, underlying, limit=2000, return_diagnostics=False, max_pages=100):
        data = contracts if underlying == "MUX" else []
        diag = {
            "underlying": underlying,
            "total_contracts_fetched": len(data),
            "total_expirations_fetched": len({row["expiration_date"] for row in data}),
            "pagination_completed": True,
            "provider_page_count": 3 if underlying == "MUX" else 0,
            "provider_truncation": False,
            "limit_applied": limit,
        }
        return (data, diag) if return_diagnostics else data

    async def fake_fetch_aggregate_gamma_chain(
        client,
        symbol,
        *,
        spot,
        underlyings,
        contracts_by_underlying,
        provider_listing,
        max_pages_per_expiry=25,
        page_limit=250,
    ):
        rows = [
            {
                "ticker": row["ticker"],
                "strike": float(row["strike_price"]),
                "option_type": row["contract_type"],
                "oi": 10.0,
                "iv": 0.25,
                "expiry": row["expiration_date"],
                "t_years": 30 / 365,
                "contract_size": 100.0,
            }
            for row in contracts
        ]
        return rows, {
            "total_snapshot_rows": len(rows),
            "available_expirations": sorted({row["expiration_date"] for row in contracts}),
            "provider_page_count": 11,
            "pagination_completed": True,
            "provider_truncation": False,
        }

    monkeypatch.setattr("engine.list_option_contracts", fake_list_option_contracts)
    monkeypatch.setattr("engine.fetch_aggregate_gamma_chain", fake_fetch_aggregate_gamma_chain)
    monkeypatch.setattr(views.job_manager, "cache_get", lambda key, ttl: None)
    monkeypatch.setattr(views.job_manager, "cache_set", lambda key, value: None)
    monkeypatch.setattr(views, "disk_cache_get", lambda key, ttl: None)
    monkeypatch.setattr(views, "disk_cache_set", lambda key, payload: None)

    job = Job(job_id="ticker-test", session_id="test")
    _, meta = await compute_gex_for_ticker(
        job,
        "MUX",
        100.0,
        pct_window=0.15,
        only_next_expiry=False,
        expiry_mode="all",
        include_0dte=True,
        remove_0dte=False,
        allowed_expiries=None,
        include_solver_curve=False,
    )
    summary = await views._compute_gex_cached_summary(
        "MUX",
        100.0,
        0.15,
        expiry_key=None,
        expiry_mode="all",
        next_only=False,
        remove_0dte=False,
        allowed_expiries=None,
        label="all",
        parent_job_id=None,
    )

    assert summary["zero_gamma"] == pytest.approx(meta["zero_gamma"])
    assert summary["total_gamma_at_spot"] == pytest.approx(meta["total_gamma_at_spot"], rel=1e-3)
    assert summary["meta"]["provider_listing"]["total_contracts_fetched"] == len(contracts)
    assert summary["gamma_confidence"] in {"high", "medium", "low", None}


@pytest.mark.asyncio
async def test_compute_gex_single_expiry_uses_bulk_snapshot_path(monkeypatch):
    contracts = [
        {
            "ticker": "SLV260417C00080000",
            "strike_price": 80.0,
            "expiration_date": "2026-04-17",
            "contract_type": "call",
        },
        {
            "ticker": "SLV260417P00080000",
            "strike_price": 80.0,
            "expiration_date": "2026-04-17",
            "contract_type": "put",
        },
        {
            "ticker": "SLV260515C00085000",
            "strike_price": 85.0,
            "expiration_date": "2026-05-15",
            "contract_type": "call",
        },
    ]
    call_counts = {"bulk": 0, "per_contract": 0}

    async def fake_list_option_contracts(client, underlying, limit=2000, return_diagnostics=False, max_pages=100):
        data = contracts if underlying == "SLV" else []
        diag = {
            "underlying": underlying,
            "total_contracts_fetched": len(data),
            "total_expirations_fetched": len({row["expiration_date"] for row in data}),
            "pagination_completed": True,
            "provider_page_count": 1 if data else 0,
            "provider_truncation": False,
            "limit_applied": limit,
        }
        return (data, diag) if return_diagnostics else data

    async def fake_fetch_aggregate_gamma_chain(
        client,
        symbol,
        *,
        spot,
        underlyings,
        contracts_by_underlying,
        provider_listing,
        max_pages_per_expiry=25,
        page_limit=250,
    ):
        call_counts["bulk"] += 1
        selected = contracts_by_underlying.get("SLV", [])
        rows = [
            {
                "ticker": row["ticker"],
                "strike": float(row["strike_price"]),
                "option_type": row["contract_type"],
                "oi": 100.0,
                "iv": 0.25,
                "expiry": row["expiration_date"],
                "t_years": 30 / 365,
                "contract_size": 100.0,
            }
            for row in selected
        ]
        return rows, {
            "total_snapshot_rows": len(rows),
            "available_expirations": sorted({row["expiration_date"] for row in selected}),
            "provider_page_count": 1,
            "pagination_completed": True,
            "provider_truncation": False,
        }

    async def fake_fetch_oi_iv_by_filters(client, underlying, strike, expiry, option_type):
        call_counts["per_contract"] += 1
        return {"oi": 100.0, "iv": 0.25}

    async def fake_fetch_option_oi_iv(client, option_ticker):
        call_counts["per_contract"] += 1
        return {"oi": 100.0, "iv": 0.25}

    monkeypatch.setattr("engine.list_option_contracts", fake_list_option_contracts)
    monkeypatch.setattr("engine.fetch_aggregate_gamma_chain", fake_fetch_aggregate_gamma_chain)
    monkeypatch.setattr("engine.fetch_oi_iv_by_filters", fake_fetch_oi_iv_by_filters)
    monkeypatch.setattr("engine.fetch_option_oi_iv", fake_fetch_option_oi_iv)

    job = Job(job_id="gex-selected", session_id="test")
    df, meta = await compute_gex_for_ticker(
        job,
        "SLV",
        80.0,
        pct_window=0.20,
        only_next_expiry=False,
        expiry_mode="selected",
        expiry_override="2026-04-17",
        include_0dte=True,
        remove_0dte=False,
        allowed_expiries=None,
        include_solver_curve=False,
    )

    assert not df.empty
    assert meta["included_expirations"] == ["2026-04-17"]
    assert call_counts["bulk"] == 1
    assert call_counts["per_contract"] == 0


@pytest.mark.asyncio
async def test_aggregate_gamma_chain_cache_reuses_normalized_rows(monkeypatch):
    symbol = "MUC"
    contracts = _fake_contracts(total=220, expiries=4)
    call_counts = {"list": 0, "aggregate": 0}

    async def fake_list_option_contracts(client, underlying, limit=2000, return_diagnostics=False, max_pages=100):
        call_counts["list"] += 1
        data = contracts if underlying == symbol else []
        diag = {
            "underlying": underlying,
            "total_contracts_fetched": len(data),
            "total_expirations_fetched": len({row["expiration_date"] for row in data}),
            "pagination_completed": True,
            "provider_page_count": 1,
            "provider_truncation": False,
            "limit_applied": limit,
        }
        return (data, diag) if return_diagnostics else data

    async def fake_fetch_aggregate_gamma_chain(
        client,
        symbol,
        *,
        spot,
        underlyings,
        contracts_by_underlying,
        provider_listing,
        max_pages_per_expiry=25,
        page_limit=250,
    ):
        call_counts["aggregate"] += 1
        rows = [
            {
                "ticker": row["ticker"],
                "strike": float(row["strike_price"]),
                "option_type": row["contract_type"],
                "oi": 25.0,
                "iv": 0.21,
                "expiry": row["expiration_date"],
                "t_years": 20 / 365,
                "contract_size": 100.0,
            }
            for row in contracts
        ]
        return rows, {
            "total_snapshot_rows": len(rows),
            "available_expirations": sorted({row["expiration_date"] for row in contracts}),
            "provider_page_count": 4,
            "pagination_completed": True,
            "provider_truncation": False,
        }

    monkeypatch.setattr("engine.list_option_contracts", fake_list_option_contracts)
    monkeypatch.setattr("engine.fetch_aggregate_gamma_chain", fake_fetch_aggregate_gamma_chain)
    monkeypatch.setattr("engine.disk_cache_get", lambda key, ttl: None)
    monkeypatch.setattr("engine.disk_cache_set", lambda key, payload: None)

    key = cache_key(
        mode="gchain",
        symbol="MUC",
        pct_window=0.0,
        next_only=False,
        expiry="all",
        spot_override="100.00",
        expiry_mode="all",
        include_0dte=True,
        calc_version="gamma-chain-v1",
    )
    views.job_manager._cache.pop(key, None)

    job1 = Job(job_id="cache-1", session_id="test")
    await compute_gex_for_ticker(
        job1,
        symbol,
        100.0,
        pct_window=0.15,
        only_next_expiry=False,
        expiry_mode="all",
        include_0dte=True,
        remove_0dte=False,
        allowed_expiries=None,
        include_solver_curve=False,
    )
    job2 = Job(job_id="cache-2", session_id="test")
    await compute_gex_for_ticker(
        job2,
        symbol,
        100.0,
        pct_window=0.15,
        only_next_expiry=False,
        expiry_mode="all",
        include_0dte=True,
        remove_0dte=False,
        allowed_expiries=None,
        include_solver_curve=False,
    )

    assert call_counts["aggregate"] == 1


def test_solver_settings_propagate_through_gex_cache_key():
    key_default = cache_key(
        mode="g",
        symbol="SLV",
        pct_window=0.2,
        next_only=False,
        expiry="",
        expiry_mode="all",
        include_0dte=True,
        expiry_filter="2026-03-13,2026-04-17",
        solver_profile=gamma_solver_cache_token(None),
        calc_version="gamma-v4",
    )
    key_custom = cache_key(
        mode="g",
        symbol="SLV",
        pct_window=0.2,
        next_only=False,
        expiry="",
        expiry_mode="all",
        include_0dte=True,
        expiry_filter="2026-03-13,2026-04-17",
        solver_profile=gamma_solver_cache_token({"preset": "custom", "horizon": "m1", "band": "25"}),
        calc_version="gamma-v4",
    )

    assert key_default != key_custom


def test_normalize_snapshot_chain_row_infers_iv_from_option_price():
    row = _normalize_snapshot_chain_row(
        {
            "day": {"close": 12.5},
            "details": {
                "contract_type": "call",
                "expiration_date": "2026-03-20",
                "strike_price": 100.0,
                "ticker": "O:TEST260320C00100000",
            },
            "open_interest": 25,
        },
        spot=105.0,
    )

    assert row is not None
    assert row["iv"] > 0

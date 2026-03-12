from __future__ import annotations

import pytest

import views
from core.demo_data import demo_scanner_row
from core.gamma_math import derive_gamma_solver_confidence


async def _fake_price_context(_symbol: str) -> dict:
    return {"spot": 100.0, "day_change_pct": 0.15, "ah_change_pct": -0.05}


def _fake_summary_for_expiries(allowed_expiries: list[str] | None) -> dict:
    included = list(allowed_expiries or [])
    diagnostics = {
        "solver_contract_count": max(len(included), 1) * 12,
        "has_sign_crossing": True,
        "sign_change_intervals": [0],
        "solver_expansions_used": 0,
    }
    return {
        "net_gex": float(len(included) or 1) * 1_250_000.0,
        "total_gamma_at_spot": 3_500_000.0,
        "zero_gamma": 101.4,
        "gamma_confidence": derive_gamma_solver_confidence(diagnostics),
        "spot_vs_zero_gamma": "Below Zero Gamma",
        "spot_vs_zero_gamma_pct": -1.38,
        "gamma_regime": "Long Gamma",
        "meta": {"zero_gamma_diagnostics": diagnostics},
        "raw": {"strikes": [95.0, 100.0, 105.0], "gex_net": [-1.0, 2.0, 1.0]},
        "error": None,
    }


@pytest.mark.asyncio
async def test_scanner_confidence_matches_when_1dte_and_weekly_resolve_same_set(monkeypatch):
    async def fake_expiries(_symbol: str) -> list[str]:
        return ["2026-03-13", "2026-04-17", "2026-05-15"]

    async def fake_summary(
        symbol: str,
        spot: float,
        pct_window: float,
        *,
        allowed_expiries: list[str] | None = None,
        **_kwargs,
    ) -> dict:
        return _fake_summary_for_expiries(allowed_expiries)

    monkeypatch.setattr(views, "_today_iso", lambda: "2026-03-12")
    monkeypatch.setattr(views, "_fetch_price_context", _fake_price_context)
    monkeypatch.setattr(views, "_list_expiry_dates", fake_expiries)
    monkeypatch.setattr(views, "_compute_gex_cached_summary", fake_summary)

    row_1dte = await views._scanner_entry("SPY", 0.10, "1dte", False)
    row_weekly = await views._scanner_entry("SPY", 0.10, "weekly", False)

    assert row_1dte["scope_expirations"] == ["2026-03-13"]
    assert row_weekly["scope_expirations"] == ["2026-03-13"]
    assert row_1dte["resolved_expiration_key"] == row_weekly["resolved_expiration_key"]
    assert row_1dte["gamma_confidence"] == row_weekly["gamma_confidence"] == "high"


@pytest.mark.asyncio
async def test_scanner_term_shape_payload_keeps_explicit_anchor_slots(monkeypatch):
    async def fake_summary(
        symbol: str,
        spot: float,
        pct_window: float,
        *,
        allowed_expiries: list[str] | None = None,
        **_kwargs,
    ) -> dict:
        summary = _fake_summary_for_expiries(allowed_expiries)
        if allowed_expiries == ["2026-04-17"]:
            summary["net_gex"] = 18_000_000.0
        return summary

    monkeypatch.setattr(views, "_today_iso", lambda: "2026-03-12")
    monkeypatch.setattr(views, "_compute_gex_cached_summary", fake_summary)

    term_shape = await views._scanner_term_shape(
        "IWM",
        205.0,
        0.10,
        available_expiries=["2026-04-17"],
        remove_0dte=False,
    )

    anchors = term_shape["anchors"]
    assert [item["anchor"] for item in anchors] == ["W1", "M1", "M2"]
    assert anchors[0]["applicable"] is False
    assert anchors[0]["value"] is None
    assert anchors[1]["applicable"] is True
    assert anchors[1]["expiry"] == "2026-04-17"
    assert anchors[2]["applicable"] is False
    assert anchors[2]["value"] is None
    assert term_shape["bias"] is None
    assert term_shape["interpretation"] == "Unavailable"


@pytest.mark.asyncio
async def test_scanner_excluded_rows_keep_null_metrics_and_empty_bias(monkeypatch):
    async def fake_expiries(_symbol: str) -> list[str]:
        return ["2026-04-17", "2026-05-15"]

    monkeypatch.setattr(views, "_today_iso", lambda: "2026-03-12")
    monkeypatch.setattr(views, "_fetch_price_context", _fake_price_context)
    monkeypatch.setattr(views, "_list_expiry_dates", fake_expiries)

    row = await views._scanner_entry("TLT", 0.10, "0dte", False)

    assert row["excluded"] is True
    assert row["net_gex"] is None
    assert row["zero_gamma"] is None
    assert row["spot_density"] is None
    assert row["gamma_confidence"] is None
    assert row["term_shape_bias"] is None
    assert [item["anchor"] for item in row["term_shape"]["anchors"]] == ["W1", "M1", "M2"]
    assert all(item["value"] is None for item in row["term_shape"]["anchors"])


def test_demo_scanner_row_includes_term_shape_payload():
    row = demo_scanner_row("AAPL", 0.10, scope="all", include_0dte=True)

    assert "term_shape" in row
    assert row["term_shape_bias"] == row["term_shape"]["bias"]
    assert [item["anchor"] for item in row["term_shape"]["anchors"]] == ["W1", "M1", "M2"]
    assert all("applicable" in item for item in row["term_shape"]["anchors"])
    assert all("value" in item for item in row["term_shape"]["anchors"])

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import isclose

import core.gamma_math as gamma_math
from core.gamma_math import (
    build_gamma_profile,
    build_reduced_solver_universe,
    canonicalize_gex_payload,
    classify_gamma_regime,
    compute_net_gex,
    compute_option_gamma_at_spot,
    compute_total_gamma_curve,
    compute_zero_gamma,
    compute_zero_gamma_from_curve,
    default_gamma_solver_config,
    expiration_scope_expirations,
    gamma_solver_profile_label,
    has_next_trading_day_expiration,
    has_same_day_expiration,
    is_standard_monthly_expiration,
    next_monthly_expiration,
    normalize_gamma_solver_config,
    prepare_gamma_analysis,
    spot_vs_zero_gamma_label,
)


def test_static_cumulative_can_cross_zero_while_true_zero_gamma_is_absent():
    chain = [
        {"strike": 130, "option_type": "call", "oi": 400, "iv": 0.22, "t_years": 7 / 365, "expiry": "2026-04-17"},
        {"strike": 70, "option_type": "call", "oi": 400, "iv": 0.18, "t_years": 90 / 365, "expiry": "2026-04-17"},
        {"strike": 70, "option_type": "put", "oi": 25, "iv": 0.18, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 100, "option_type": "put", "oi": 50, "iv": 0.35, "t_years": 45 / 365, "expiry": "2026-04-17"},
        {"strike": 110, "option_type": "call", "oi": 400, "iv": 0.35, "t_years": 45 / 365, "expiry": "2026-04-17"},
        {"strike": 80, "option_type": "call", "oi": 400, "iv": 0.18, "t_years": 20 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)

    assert any(
        (profile["gex_cumulative"][idx - 1] < 0 < profile["gex_cumulative"][idx])
        or (profile["gex_cumulative"][idx - 1] > 0 > profile["gex_cumulative"][idx])
        for idx in range(1, len(profile["gex_cumulative"]))
    )
    assert profile["zero_gamma"] is None
    assert profile["gamma_regime"] == "Long Gamma"


def test_true_zero_gamma_can_exist_while_static_snapshot_looks_one_sided():
    chain = [
        {"strike": 80, "option_type": "call", "oi": 200, "iv": 0.30, "t_years": 40 / 365, "expiry": "2026-04-17"},
        {"strike": 100, "option_type": "call", "oi": 200, "iv": 0.28, "t_years": 40 / 365, "expiry": "2026-04-17"},
        {"strike": 120, "option_type": "put", "oi": 500, "iv": 0.26, "t_years": 40 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)

    assert all(value > 0 for value in profile["gex_cumulative"])
    assert profile["zero_gamma"] is not None
    assert profile["gamma_regime"] == "Long Gamma"


def test_no_zero_gamma_in_range_still_classifies_short_gamma():
    chain = [
        {"strike": 90, "option_type": "put", "oi": 300, "iv": 0.25, "t_years": 30 / 365, "expiry": "2026-04-17"},
        {"strike": 110, "option_type": "put", "oi": 220, "iv": 0.25, "t_years": 30 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)

    assert profile["zero_gamma"] is None
    assert profile["total_gamma_at_spot"] < 0
    assert profile["gamma_regime"] == "Short Gamma"


def test_no_zero_gamma_in_range_still_classifies_long_gamma():
    chain = [
        {"strike": 90, "option_type": "call", "oi": 300, "iv": 0.25, "t_years": 30 / 365, "expiry": "2026-04-17"},
        {"strike": 110, "option_type": "call", "oi": 220, "iv": 0.25, "t_years": 30 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)

    assert profile["zero_gamma"] is None
    assert profile["total_gamma_at_spot"] > 0
    assert profile["gamma_regime"] == "Long Gamma"


def test_net_gex_uses_spot_scaled_formula_while_solver_keeps_raw_signed_gamma():
    chain = [
        {"strike": 100, "option_type": "call", "oi": 10, "iv": 0.20, "t_years": 30 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)

    assert profile["net_gex"] > 0
    assert profile["total_gamma_at_spot"] > 0
    assert isclose(profile["net_gex"] * 100.0, profile["total_gamma_at_spot"], rel_tol=1e-9)
    assert isclose(compute_net_gex(chain, 100.0), profile["net_gex"], rel_tol=1e-9)
    assert profile["net_gex_formula"].endswith("* S")
    assert profile["raw_signed_gamma_formula"].endswith("* S^2")


def test_specific_expiration_and_all_expirations_share_inclusion_rules():
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=14)).isoformat()
    chain = [
        {"strike": 95, "option_type": "call", "oi": 250, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 105, "option_type": "put", "oi": 300, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 100, "option_type": "call", "oi": 180, "iv": 0.22, "t_years": 14 / 365, "expiry": future},
    ]

    selected_on = build_gamma_profile(chain, 100.0, expirations=[future], include_0dte=True, today_iso=today)
    selected_off = build_gamma_profile(chain, 100.0, expirations=[future], include_0dte=False, today_iso=today)
    all_on = build_gamma_profile(chain, 100.0, include_0dte=True, today_iso=today)
    all_off = build_gamma_profile(chain, 100.0, include_0dte=False, today_iso=today)

    assert selected_on["total_gamma_at_spot"] == selected_off["total_gamma_at_spot"]
    assert selected_on["contract_count"] == selected_off["contract_count"] == 1
    assert all_on["contract_count"] > all_off["contract_count"]
    assert all_on["total_gamma_at_spot"] == all_off["total_gamma_at_spot"]


def test_ticker_and_scanner_paths_match_same_solver_contract():
    chain = [
        {"strike": 95, "option_type": "call", "oi": 400, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 105, "option_type": "put", "oi": 500, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0)
    solver = compute_zero_gamma(chain, 100.0)
    regime = classify_gamma_regime(100.0, solver["zero_gamma"], solver["total_gamma_at_spot"])

    assert isclose(profile["zero_gamma"] or 0.0, solver["zero_gamma"] or 0.0, rel_tol=0, abs_tol=1e-9)
    assert isclose(
        profile["total_gamma_at_spot"] or 0.0,
        solver["total_gamma_at_spot"] or 0.0,
        rel_tol=0,
        abs_tol=1e-9,
    )
    assert profile["gamma_regime"] == regime


def test_numerical_noise_near_zero_is_stable():
    assert compute_zero_gamma_from_curve([90, 100, 110], [-1e-7, 5e-8, 2e-7]) is None
    assert classify_gamma_regime(100.0, None, 5e-7) == "Gamma Neutral"


def test_degenerate_rows_are_ignored_safely():
    curve = compute_total_gamma_curve(
        [
            {"strike": 100, "option_type": "call", "oi": 0, "iv": 0.2, "t_years": 30 / 365, "expiry": "2026-04-17"},
            {"strike": 100, "option_type": "put", "oi": 10, "iv": 0.0, "t_years": 30 / 365, "expiry": "2026-04-17"},
            {"strike": 100, "option_type": "call", "oi": 10, "iv": 0.2, "t_years": 0.0, "expiry": "2026-04-17"},
            {"strike": 100, "option_type": "call", "oi": 10, "iv": 0.2, "t_years": 30 / 365, "expiry": "2026-04-17"},
        ],
        [90.0, 100.0, 110.0],
    )

    assert curve["valid_contract_count"] == 1
    assert len(curve["spots"]) == 3
    assert all(isinstance(value, float) for value in curve["total_gamma"])
    assert compute_option_gamma_at_spot(100.0, 100.0, 0.0, 30 / 365) == 0.0


def test_canonicalize_gex_payload_uses_total_gamma_sign_for_regime():
    payload = canonicalize_gex_payload(
        {
            "strikes": [95, 105],
            "gex_calls": [5.0, 2.0],
            "gex_puts": [-1.0, -8.0],
            "meta": {
                "spot": 100.0,
                "zero_gamma": None,
                "total_gamma_at_spot": -2.0,
            },
        }
    )

    assert payload["meta"]["gamma_regime"] == "Short Gamma"
    assert payload["meta"]["spot_vs_zero_gamma"] == "No Zero Gamma in tested range"


def test_solver_diagnostics_include_band_and_curve_when_requested():
    chain = [
        {"strike": 95, "option_type": "call", "oi": 400, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 105, "option_type": "put", "oi": 500, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
    ]

    solver = compute_zero_gamma(chain, 100.0, include_curve=True)

    assert solver["diagnostics"]["solver_spot_min"] == 50.0
    assert solver["diagnostics"]["solver_spot_max"] == 150.0
    assert solver["diagnostics"]["valid_contract_count"] == 2
    assert solver["diagnostics"]["has_sign_crossing"] is True
    assert len(solver["diagnostics"]["curve"]) == solver["diagnostics"]["grid_point_count"]
    assert spot_vs_zero_gamma_label(101.0, solver["zero_gamma"]) in {
        "Above Zero Gamma",
        "At Zero Gamma",
    }


def test_zero_gamma_solver_uses_full_included_chain_not_chart_window():
    chain = [
        {"strike": 80, "option_type": "call", "oi": 200, "iv": 0.30, "t_years": 40 / 365, "expiry": "2026-04-17"},
        {"strike": 100, "option_type": "call", "oi": 200, "iv": 0.28, "t_years": 40 / 365, "expiry": "2026-04-17"},
        {"strike": 120, "option_type": "put", "oi": 500, "iv": 0.26, "t_years": 40 / 365, "expiry": "2026-04-17"},
    ]

    profile = build_gamma_profile(chain, 100.0, chart_strike_range=(95.0, 105.0))

    assert profile["strikes"] == [100.0]
    assert profile["zero_gamma"] is not None
    assert profile["total_gamma_at_spot"] is not None


def test_solver_refines_first_sign_change_interval():
    chain = [
        {"strike": 95, "option_type": "call", "oi": 400, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 105, "option_type": "put", "oi": 500, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
    ]

    baseline = compute_zero_gamma(chain, 100.0, steps=801, refinement_steps=161)
    refined = compute_zero_gamma(chain, 100.0, steps=11, refinement_steps=161)

    assert baseline["zero_gamma"] is not None
    assert refined["zero_gamma"] is not None
    assert abs(refined["zero_gamma"] - baseline["zero_gamma"]) < 0.25
    assert refined["diagnostics"]["first_sign_change_interval"] is not None


def test_remove_0dte_default_off_includes_same_day_expirations():
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    chain = [
        {"strike": 95, "option_type": "call", "oi": 250, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 100, "option_type": "call", "oi": 180, "iv": 0.22, "t_years": 7 / 365, "expiry": future},
    ]

    profile = build_gamma_profile(chain, 100.0, selected_scope="all", expirations=[today, future], today_iso=today)

    assert today in profile["included_expirations"]
    assert future in profile["included_expirations"]


def test_remove_0dte_on_excludes_same_day_expirations():
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    chain = [
        {"strike": 95, "option_type": "call", "oi": 250, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 100, "option_type": "call", "oi": 180, "iv": 0.22, "t_years": 7 / 365, "expiry": future},
    ]

    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        expirations=[today, future],
        remove_0dte=True,
        today_iso=today,
    )

    assert today not in profile["included_expirations"]


def test_standard_monthly_detection_requires_third_friday():
    assert is_standard_monthly_expiration("2026-03-20") is True
    assert is_standard_monthly_expiration("2026-04-17") is True
    assert is_standard_monthly_expiration("2026-04-16") is False
    assert is_standard_monthly_expiration("2026-03-27") is False


def test_expiration_scope_expirations_supports_0dte_1dte_weekly_monthly_m1_m2():
    expiries = [
        "2026-03-12",
        "2026-03-13",
        "2026-03-20",
        "2026-03-27",
        "2026-04-16",
        "2026-05-15",
    ]

    assert expiration_scope_expirations(expiries, "0dte", today_iso="2026-03-12") == ["2026-03-12"]
    assert expiration_scope_expirations(expiries, "1dte", today_iso="2026-03-12") == ["2026-03-13"]
    assert expiration_scope_expirations(expiries, "weekly", today_iso="2026-03-12") == [
        "2026-03-12",
        "2026-03-13",
    ]
    assert expiration_scope_expirations(expiries, "monthly", today_iso="2026-03-12") == [
        "2026-03-20",
        "2026-05-15",
    ]
    assert expiration_scope_expirations(expiries, "m1", today_iso="2026-03-12") == ["2026-03-20"]
    assert expiration_scope_expirations(expiries, "m2", today_iso="2026-03-12") == ["2026-05-15"]


def test_monthly_indexing_skips_non_monthly_weeklies():
    expiries = [
        "2026-03-12",
        "2026-03-13",
        "2026-03-20",
        "2026-03-27",
        "2026-04-17",
        "2026-05-15",
    ]

    assert expiration_scope_expirations(expiries, "monthly", today_iso="2026-03-12") == [
        "2026-03-20",
        "2026-04-17",
        "2026-05-15",
    ]
    assert expiration_scope_expirations(expiries, "m2", today_iso="2026-03-12") == ["2026-04-17"]


def test_monthly_and_trading_day_support_helpers_follow_available_expiries():
    expiries = ["2026-03-12", "2026-03-20", "2026-04-17"]

    assert has_same_day_expiration(expiries, today_iso="2026-03-12") is True
    assert has_next_trading_day_expiration(expiries, today_iso="2026-03-12") is False
    assert next_monthly_expiration(expiries, index=1, today_iso="2026-03-12") == "2026-03-20"
    assert next_monthly_expiration(expiries, index=2, today_iso="2026-03-12") == "2026-04-17"


def test_next_trading_day_requires_exact_next_business_day():
    expiries = ["2026-03-12", "2026-03-13", "2026-03-20"]

    assert has_next_trading_day_expiration(expiries, today_iso="2026-03-12") is True
    assert expiration_scope_expirations(expiries, "1dte", today_iso="2026-03-12") == ["2026-03-13"]


def test_default_gamma_solver_config_matches_standard_preset():
    cfg = normalize_gamma_solver_config(None)

    assert cfg == normalize_gamma_solver_config(default_gamma_solver_config())
    assert cfg["preset"] == "standard"
    assert cfg["horizon"] == "m2"
    assert cfg["band"] == "20"
    assert cfg["tail_handling"] == "moderate"
    assert cfg["refinement_mode"] == "balanced"
    assert gamma_solver_profile_label(cfg) == "Standard (Default)"


def test_custom_solver_horizon_changes_zero_gamma_deterministically():
    chain = [
        {"strike": 70, "option_type": "call", "oi": 400, "iv": 0.35, "t_years": 10 / 365, "expiry": "2026-03-20"},
        {"strike": 80, "option_type": "put", "oi": 450, "iv": 0.40, "t_years": 10 / 365, "expiry": "2026-03-20"},
        {"strike": 90, "option_type": "call", "oi": 500, "iv": 0.32, "t_years": 10 / 365, "expiry": "2026-03-20"},
        {"strike": 65, "option_type": "call", "oi": 1200, "iv": 0.55, "t_years": 60 / 365, "expiry": "2026-05-15"},
        {"strike": 70, "option_type": "call", "oi": 1800, "iv": 0.58, "t_years": 60 / 365, "expiry": "2026-05-15"},
        {"strike": 75, "option_type": "put", "oi": 2200, "iv": 0.62, "t_years": 60 / 365, "expiry": "2026-05-15"},
    ]

    near_term = build_gamma_profile(
        chain,
        80.0,
        selected_scope="all",
        solver_config={"preset": "near_term"},
    )
    full_chain = build_gamma_profile(
        chain,
        80.0,
        selected_scope="all",
        solver_config={"preset": "full_chain"},
    )

    assert near_term["zero_gamma"] is not None
    assert full_chain["zero_gamma"] is not None
    assert near_term["zero_gamma"] != full_chain["zero_gamma"]


def test_solver_diagnostics_reflect_selected_configuration():
    chain = [
        {"strike": 95, "option_type": "call", "oi": 400, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 105, "option_type": "put", "oi": 500, "iv": 0.22, "t_years": 20 / 365, "expiry": "2026-04-17"},
        {"strike": 100, "option_type": "call", "oi": 300, "iv": 0.25, "t_years": 55 / 365, "expiry": "2026-05-22"},
    ]

    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        solver_config={
            "preset": "custom",
            "horizon": "m1",
            "band": "25",
            "remove_0dte": True,
            "tail_handling": "aggressive",
            "refinement_mode": "high_precision",
        },
    )
    diagnostics = profile["zero_gamma_diagnostics"]

    assert diagnostics["solver_horizon"] == "m1"
    assert diagnostics["solver_band"] == "25"
    assert diagnostics["solver_remove_0dte"] is True
    assert diagnostics["tail_handling"] == "aggressive"
    assert diagnostics["refinement_mode"] == "high_precision"
    assert diagnostics["solver_profile_label"].startswith("Custom")


def test_selected_expiry_ignores_aggregate_filter_set():
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    chain = [
        {"strike": 95, "option_type": "call", "oi": 250, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 100, "option_type": "call", "oi": 180, "iv": 0.22, "t_years": 7 / 365, "expiry": future},
    ]

    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="selected",
        selected_expiry=future,
        expirations=[today],
        today_iso=today,
    )

    assert profile["included_expirations"] == [future]
    assert profile["contract_count"] == 1


def test_aggregate_custom_expiry_set_changes_charts_and_gamma_metrics():
    exp_a = "2026-04-17"
    exp_b = "2026-05-15"
    chain = [
        {"strike": 95, "option_type": "call", "oi": 300, "iv": 0.22, "t_years": 20 / 365, "expiry": exp_a},
        {"strike": 105, "option_type": "put", "oi": 500, "iv": 0.22, "t_years": 20 / 365, "expiry": exp_b},
    ]

    full_profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        expirations=[exp_a, exp_b],
        solver_config={"preset": "full_chain"},
    )
    filtered_profile = build_gamma_profile(chain, 100.0, selected_scope="all", expirations=[exp_a])

    assert full_profile["contract_count"] == 2
    assert filtered_profile["contract_count"] == 1
    assert filtered_profile["included_expirations"] == [exp_a]
    assert full_profile["total_gamma_at_spot"] != filtered_profile["total_gamma_at_spot"]
    assert filtered_profile["strikes"] == [95.0]


def test_all_expirations_net_gex_uses_page_universe_not_solver_horizon():
    chain = [
        {"strike": 75, "option_type": "call", "oi": 900, "iv": 0.30, "t_years": 10 / 365, "expiry": "2026-03-20"},
        {"strike": 80, "option_type": "put", "oi": 700, "iv": 0.30, "t_years": 10 / 365, "expiry": "2026-03-20"},
        {"strike": 70, "option_type": "call", "oi": 2000, "iv": 0.55, "t_years": 150 / 365, "expiry": "2026-08-14"},
    ]

    profile = build_gamma_profile(
        chain,
        80.0,
        selected_scope="all",
        expirations=["2026-03-20", "2026-08-14"],
        solver_config={"preset": "near_term"},
    )
    expected_page_net = compute_net_gex(
        chain,
        80.0,
        expirations=["2026-03-20", "2026-08-14"],
        selected_scope="all",
    )
    expected_solver_net = compute_net_gex(
        chain,
        80.0,
        expirations=["2026-03-20"],
        selected_scope="all",
    )

    assert isclose(profile["net_gex"], expected_page_net, rel_tol=1e-9)
    assert not isclose(profile["net_gex"], expected_solver_net, rel_tol=1e-9)
    assert profile["zero_gamma_diagnostics"]["page_expiries_used"] == 2
    assert profile["zero_gamma_diagnostics"]["expiries_used"] == 1


def test_diagnostics_include_inclusion_and_drop_audit_metadata():
    today = datetime.now(timezone.utc).date().isoformat()
    future = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
    chain = [
        {"strike": 95, "option_type": "call", "oi": 250, "iv": 0.24, "t_years": 1 / 365, "expiry": today},
        {"strike": 100, "option_type": "call", "oi": 180, "iv": 0.22, "t_years": 7 / 365, "expiry": future},
        {"strike": 100, "option_type": "put", "oi": 0, "iv": 0.22, "t_years": 7 / 365, "expiry": future},
        {"strike": 100, "option_type": "put", "oi": 10, "iv": 0.0, "t_years": 7 / 365, "expiry": future},
        {"strike": 101, "option_type": "call", "oi": 10, "iv": 0.2, "t_years": 7 / 365},
    ]

    solver = compute_zero_gamma(
        chain,
        100.0,
        selected_scope="all",
        selected_expirations=[today, future],
        remove_0dte=True,
        today_iso=today,
        include_curve=True,
    )
    diagnostics = solver["diagnostics"]

    assert diagnostics["available_expirations"] == [today, future]
    assert diagnostics["included_expirations"] == [future]
    assert diagnostics["excluded_expiration_reasons"][today] == "removed_0dte"
    assert diagnostics["dropped_rows_by_reason"]["zero_open_interest"] == 1
    assert diagnostics["dropped_rows_by_reason"]["invalid_implied_volatility"] == 1
    assert diagnostics["dropped_rows_by_reason"]["missing_expiry"] == 1
    assert diagnostics["included_row_count"] == 1
    assert len(diagnostics["included_contract_sample"]) == 1


def test_reduced_solver_universe_preserves_all_expiries():
    chain = []
    expiries = ["2026-04-17", "2026-05-15", "2026-06-19"]
    for expiry_idx, expiry in enumerate(expiries):
        for strike in (40, 50, 60, 80, 100, 120, 140, 150, 160):
            chain.append(
                {
                    "strike": strike,
                    "option_type": "call",
                    "oi": 50 + (expiry_idx * 10) + (200 if strike == 120 else 0),
                    "iv": 0.24,
                    "t_years": (20 + expiry_idx * 15) / 365,
                    "expiry": expiry,
                }
            )
            chain.append(
                {
                    "strike": strike,
                    "option_type": "put",
                    "oi": 40 + (expiry_idx * 10) + (180 if strike == 80 else 0),
                    "iv": 0.24,
                    "t_years": (20 + expiry_idx * 15) / 365,
                    "expiry": expiry,
                }
            )

    reduced = build_reduced_solver_universe(chain, 100.0)

    assert reduced["input_row_count"] == len(chain)
    assert reduced["included_row_count"] < len(chain)
    assert set(reduced["per_expiry_retained_row_counts"]) == set(expiries)
    assert all(count >= 2 for count in reduced["per_expiry_retained_row_counts"].values())
    assert reduced["kept_rows_by_reason"]["within_moneyness_band"] > 0


def test_aggregate_profile_reports_reduction_convergence_diagnostics():
    chain = []
    expiries = ["2026-04-17", "2026-05-15"]
    for expiry_idx, expiry in enumerate(expiries):
        for strike in range(70, 201):
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "call",
                    "oi": 60 + (5 if strike % 9 == 0 else 0),
                    "iv": 0.22 + expiry_idx * 0.01,
                    "t_years": (30 + expiry_idx * 20) / 365,
                    "expiry": expiry,
                }
            )
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "put",
                    "oi": 65 + (8 if strike % 11 == 0 else 0),
                    "iv": 0.24 + expiry_idx * 0.01,
                    "t_years": (30 + expiry_idx * 20) / 365,
                    "expiry": expiry,
                }
            )

    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        expirations=expiries,
        solver_config={"preset": "full_chain"},
    )
    diagnostics = profile["zero_gamma_diagnostics"]

    assert diagnostics["solver_universe_mode"] == "reduced_aggregate"
    assert diagnostics["adaptive_refinement_ran"] is True
    assert diagnostics["full_contract_count"] == len(chain)
    assert len(diagnostics["included_rows_per_pass"]) == 2
    assert len(diagnostics["rows_kept_by_reduction_reason_per_pass"]) == 2
    assert diagnostics["included_expirations_per_pass"][0] == expiries
    assert diagnostics["convergence_status"] in {
        "converged",
        "diverged",
        "found_on_refinement",
        "lost_on_refinement",
        "no_crossing",
    }


def test_diverged_aggregate_solver_falls_back_to_pass_1_root(monkeypatch):
    chain = []
    expiries = ["2026-03-20", "2026-04-17"]
    for expiry in expiries:
        for strike in range(60, 141):
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "call",
                    "oi": 200,
                    "iv": 0.25,
                    "t_years": 20 / 365,
                    "expiry": expiry,
                }
            )
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "put",
                    "oi": 220,
                    "iv": 0.28,
                    "t_years": 20 / 365,
                    "expiry": expiry,
                }
            )

    call_counter = {"count": 0}

    def fake_compute_zero_gamma(*args, **kwargs):
        call_counter["count"] += 1
        if call_counter["count"] == 1:
            return {
                "zero_gamma": 72.18,
                "total_gamma_at_spot": 700_000_000.0,
                "diagnostics": {"curve": [], "included_row_count": 100},
            }
        return {
            "zero_gamma": 38.95,
            "total_gamma_at_spot": 710_000_000.0,
            "diagnostics": {"curve": [], "included_row_count": 110},
        }

    monkeypatch.setattr(gamma_math, "compute_zero_gamma", fake_compute_zero_gamma)

    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        expirations=expiries,
        solver_config={"preset": "standard"},
    )
    diagnostics = profile["zero_gamma_diagnostics"]

    assert profile["zero_gamma"] == 72.18
    assert diagnostics["zero_gamma_pass_1"] == 72.18
    assert diagnostics["zero_gamma_pass_2"] == 38.95
    assert diagnostics["convergence_status"] == "diverged"
    assert diagnostics["published_root_source"] == "pass_1_fallback"


def test_aggregate_zero_gamma_calibration_reduces_far_tail_dominance():
    chain = []
    expiries = ["2026-04-17", "2026-05-15"]
    for expiry in expiries:
        for strike in range(60, 141):
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "call",
                    "oi": 120,
                    "iv": 0.22,
                    "t_years": 30 / 365,
                    "expiry": expiry,
                }
            )
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "put",
                    "oi": 110,
                    "iv": 0.24,
                    "t_years": 30 / 365,
                    "expiry": expiry,
                }
            )
        for strike in (25, 30, 35, 40, 45):
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "call",
                    "oi": 4000,
                    "iv": 0.35,
                    "t_years": 180 / 365,
                    "expiry": expiry,
                }
            )
            chain.append(
                {
                    "strike": float(strike),
                    "option_type": "put",
                    "oi": 4000,
                    "iv": 0.35,
                    "t_years": 180 / 365,
                    "expiry": expiry,
                }
            )

    prepared = prepare_gamma_analysis(chain, selected_scope="all", selected_expiration_set=expiries)
    full_solver = compute_zero_gamma(
        None,
        100.0,
        prepared_contracts=prepared["contracts"],
        inclusion=prepared["inclusion"],
    )
    profile = build_gamma_profile(
        chain,
        100.0,
        selected_scope="all",
        expirations=expiries,
        solver_config={"preset": "full_chain"},
    )

    assert full_solver["zero_gamma"] is not None
    assert profile["zero_gamma"] is not None
    assert profile["zero_gamma_diagnostics"]["solver_universe_mode"] == "reduced_aggregate"
    assert profile["zero_gamma"] > full_solver["zero_gamma"]
    assert profile["zero_gamma_diagnostics"]["rows_retained_outside_band_per_pass"][0] >= 0

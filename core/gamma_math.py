from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

CONTRACT_MULTIPLIER = 100.0
DEFAULT_RATE = 0.0
DEFAULT_DIVIDEND_YIELD = 0.0
ZERO_TOLERANCE = 1e-9
ROOT_EPSILON = 1e-6
GAMMA_NEUTRAL_BAND_RATIO = 0.002
ZERO_GAMMA_RANGE = (0.5, 1.5)
ZERO_GAMMA_STEPS = 401
ZERO_GAMMA_REFINEMENT_STEPS = 81
ZERO_GAMMA_MAX_EXPANSIONS = 3
AGGREGATE_SOLVER_CONTRACT_THRESHOLD = 250
REDUCED_SOLVER_MONEYNESS_BAND = 0.20
REDUCED_SOLVER_OI_THRESHOLD = 250.0
REDUCED_SOLVER_TOP_N_CALLS = 1
REDUCED_SOLVER_TOP_N_PUTS = 1
REDUCED_SOLVER_ATM_NEIGHBORHOOD = 2
REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY = 2
REDUCED_SOLVER_EXPANDED_MONEYNESS_BAND = 0.30
REDUCED_SOLVER_EXPANDED_OI_THRESHOLD = 100.0
REDUCED_SOLVER_EXPANDED_TOP_N_CALLS = 2
REDUCED_SOLVER_EXPANDED_TOP_N_PUTS = 2
REDUCED_SOLVER_EXPANDED_ATM_NEIGHBORHOOD = 4
ZERO_GAMMA_CONVERGENCE_TOLERANCE = 1.0
WEEKLY_HORIZON_DAYS = 7
MONTHLY_HORIZON_DAYS = 35
GAMMA_SOLVER_PRESET_DEFAULT = "standard"
GAMMA_SOLVER_HORIZON_DAYS: dict[str, int | None] = {
    "w1": 7,
    "w2": 14,
    "m1": 30,
    "m2": 45,
    "m3": 60,
    "all": None,
}
GAMMA_SOLVER_BAND_VALUES: dict[str, float] = {
    "15": 0.15,
    "20": 0.20,
    "25": 0.25,
    "30": 0.30,
    "adaptive": REDUCED_SOLVER_MONEYNESS_BAND,
}
GAMMA_SOLVER_PRESETS: dict[str, dict[str, Any]] = {
    "standard": {
        "preset": "standard",
        "horizon": "m2",
        "band": "20",
        "remove_0dte": False,
        "tail_handling": "moderate",
        "refinement_mode": "balanced",
    },
    "near_term": {
        "preset": "near_term",
        "horizon": "m1",
        "band": "20",
        "remove_0dte": False,
        "tail_handling": "minimal",
        "refinement_mode": "balanced",
    },
    "balanced": {
        "preset": "balanced",
        "horizon": "m2",
        "band": "25",
        "remove_0dte": False,
        "tail_handling": "moderate",
        "refinement_mode": "balanced",
    },
    "full_chain": {
        "preset": "full_chain",
        "horizon": "all",
        "band": "20",
        "remove_0dte": False,
        "tail_handling": "moderate",
        "refinement_mode": "balanced",
    },
}
SQRT_2PI = math.sqrt(2.0 * math.pi)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _date_key(raw_expiry: Any) -> str | None:
    if raw_expiry is None:
        return None
    text = str(raw_expiry).strip()
    if not text:
        return None
    return text[:10]


def _normalize_option_type(raw_type: Any) -> str | None:
    text = str(raw_type or "").strip().lower()
    if text.startswith("c"):
        return "call"
    if text.startswith("p"):
        return "put"
    return None


def _time_to_expiry_years(raw_expiry: Any, *, now_ts: float) -> float | None:
    expiry = _date_key(raw_expiry)
    if not expiry:
        return None
    try:
        exp_dt = datetime.fromisoformat(expiry).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return max((exp_dt.timestamp() - now_ts) / (365.0 * 24.0 * 3600.0), 1.0 / 365.0)


def _sign_bucket(value: float, epsilon: float = ROOT_EPSILON) -> int:
    if abs(value) <= epsilon:
        return 0
    return 1 if value > 0 else -1


def _interpolate_root(x0: float, y0: float, x1: float, y1: float) -> float | None:
    if not all(math.isfinite(value) for value in (x0, y0, x1, y1)):
        return None
    delta = y1 - y0
    if abs(delta) <= ZERO_TOLERANCE:
        return None
    return x0 + ((-y0) / delta) * (x1 - x0)


def compute_option_gamma_at_spot(
    spot: float,
    strike: float,
    implied_volatility: float,
    time_to_expiry_years: float,
    rate: float = DEFAULT_RATE,
    dividend_yield: float = DEFAULT_DIVIDEND_YIELD,
) -> float:
    spot_value = _safe_float(spot, default=0.0)
    strike_value = _safe_float(strike, default=0.0)
    iv_value = _safe_float(implied_volatility, default=0.0)
    time_value = _safe_float(time_to_expiry_years, default=0.0)
    rate_value = _safe_float(rate, default=DEFAULT_RATE)
    div_value = _safe_float(dividend_yield, default=DEFAULT_DIVIDEND_YIELD)
    if spot_value <= 0 or strike_value <= 0 or iv_value <= ZERO_TOLERANCE or time_value <= ZERO_TOLERANCE:
        return 0.0
    try:
        vol_sqrt_t = iv_value * math.sqrt(time_value)
        d1 = (math.log(spot_value / strike_value) + (rate_value - div_value + 0.5 * iv_value * iv_value) * time_value) / vol_sqrt_t
    except (ValueError, ZeroDivisionError):
        return 0.0
    return math.exp(-div_value * time_value) * _norm_pdf(d1) / (spot_value * iv_value * math.sqrt(time_value))


def compute_option_price_at_spot(
    spot: float,
    strike: float,
    implied_volatility: float,
    time_to_expiry_years: float,
    option_type: str,
    rate: float = DEFAULT_RATE,
    dividend_yield: float = DEFAULT_DIVIDEND_YIELD,
) -> float:
    option_key = _normalize_option_type(option_type)
    spot_value = _safe_float(spot, default=0.0)
    strike_value = _safe_float(strike, default=0.0)
    iv_value = _safe_float(implied_volatility, default=0.0)
    time_value = _safe_float(time_to_expiry_years, default=0.0)
    rate_value = _safe_float(rate, default=DEFAULT_RATE)
    div_value = _safe_float(dividend_yield, default=DEFAULT_DIVIDEND_YIELD)
    if option_key is None or spot_value <= 0 or strike_value <= 0 or iv_value <= ZERO_TOLERANCE or time_value <= ZERO_TOLERANCE:
        return 0.0
    try:
        sqrt_t = math.sqrt(time_value)
        vol_sqrt_t = iv_value * sqrt_t
        d1 = (
            math.log(spot_value / strike_value)
            + (rate_value - div_value + 0.5 * iv_value * iv_value) * time_value
        ) / vol_sqrt_t
        d2 = d1 - vol_sqrt_t
        disc_spot = spot_value * math.exp(-div_value * time_value)
        disc_strike = strike_value * math.exp(-rate_value * time_value)
        if option_key == "call":
            return max(disc_spot * _norm_cdf(d1) - disc_strike * _norm_cdf(d2), 0.0)
        return max(disc_strike * _norm_cdf(-d2) - disc_spot * _norm_cdf(-d1), 0.0)
    except (ValueError, ZeroDivisionError):
        return 0.0


def infer_implied_volatility_from_price(
    option_price: float,
    spot: float,
    strike: float,
    time_to_expiry_years: float,
    option_type: str,
    *,
    rate: float = DEFAULT_RATE,
    dividend_yield: float = DEFAULT_DIVIDEND_YIELD,
    min_vol: float = 1e-4,
    max_vol: float = 5.0,
    tol: float = 1e-4,
    max_iter: int = 80,
) -> float | None:
    option_key = _normalize_option_type(option_type)
    spot_value = _safe_float(spot, default=0.0)
    strike_value = _safe_float(strike, default=0.0)
    price_value = _safe_float(option_price, default=0.0)
    time_value = _safe_float(time_to_expiry_years, default=0.0)
    if option_key is None or spot_value <= 0 or strike_value <= 0 or price_value <= 0 or time_value <= ZERO_TOLERANCE:
        return None
    intrinsic = max(spot_value - strike_value, 0.0) if option_key == "call" else max(strike_value - spot_value, 0.0)
    if price_value + tol < intrinsic:
        return None
    if abs(price_value - intrinsic) <= tol:
        return min_vol
    low = max(min_vol, 1e-4)
    high = max(max_vol, low * 2.0)
    high_price = compute_option_price_at_spot(
        spot_value,
        strike_value,
        high,
        time_value,
        option_key,
        rate=rate,
        dividend_yield=dividend_yield,
    )
    expand_ct = 0
    while high_price < price_value and expand_ct < 8:
        high *= 2.0
        high_price = compute_option_price_at_spot(
            spot_value,
            strike_value,
            high,
            time_value,
            option_key,
            rate=rate,
            dividend_yield=dividend_yield,
        )
        expand_ct += 1
    if high_price < price_value:
        return None
    for _ in range(max(max_iter, 1)):
        mid = 0.5 * (low + high)
        mid_price = compute_option_price_at_spot(
            spot_value,
            strike_value,
            mid,
            time_value,
            option_key,
            rate=rate,
            dividend_yield=dividend_yield,
        )
        if abs(mid_price - price_value) <= tol:
            return mid
        if mid_price < price_value:
            low = mid
        else:
            high = mid
    return 0.5 * (low + high)


def _normalize_gamma_contract(raw: dict[str, Any], *, now_ts: float) -> dict[str, Any] | None:
    strike = _safe_float(
        raw.get("strike")
        or raw.get("strike_price")
        or raw.get("strikePrice")
        or raw.get("k"),
        default=float("nan"),
    )
    option_type = _normalize_option_type(raw.get("option_type") or raw.get("contract_type") or raw.get("type"))
    if not math.isfinite(strike) or option_type is None:
        return None
    expiry = _date_key(raw.get("expiry") or raw.get("expiration_date") or raw.get("expirationDate"))
    time_years = raw.get("t_years")
    if time_years is None:
        time_years = _time_to_expiry_years(expiry, now_ts=now_ts)
    else:
        time_years = _safe_float(time_years, default=0.0)
    # Keep spot-space solver math on raw signed gamma (S^2), but expose headline
    # Net GEX using the vendor-parity spot-scaled convention (S).
    return {
        "strike": strike,
        "option_type": option_type,
        "oi": max(_safe_float(raw.get("oi") or raw.get("open_interest") or raw.get("openInterest")), 0.0),
        "iv": max(_safe_float(raw.get("iv") or raw.get("implied_volatility"), default=0.0), 0.0),
        "contract_size": max(_safe_float(raw.get("contract_size"), CONTRACT_MULTIPLIER), 1.0),
        "expiry": expiry,
        "t_years": time_years,
        "rate": _safe_float(raw.get("rate"), DEFAULT_RATE),
        "dividend_yield": _safe_float(raw.get("dividend_yield") or raw.get("dividend"), DEFAULT_DIVIDEND_YIELD),
    }


def _new_drop_audit() -> dict[str, Any]:
    return {
        "input_row_count": 0,
        "normalized_row_count": 0,
        "included_row_count": 0,
        "dropped_row_count": 0,
        "dropped_rows_by_reason": {},
        "included_contract_sample": [],
    }


def _bump_drop_reason(audit: dict[str, Any], reason: str) -> None:
    dropped = dict(audit.get("dropped_rows_by_reason") or {})
    dropped[reason] = int(dropped.get(reason, 0)) + 1
    audit["dropped_rows_by_reason"] = dropped
    audit["dropped_row_count"] = int(audit.get("dropped_row_count", 0)) + 1


def _raw_drop_reason(raw: Any) -> str:
    if not isinstance(raw, dict):
        return "invalid_row_shape"
    strike = _safe_float(
        raw.get("strike")
        or raw.get("strike_price")
        or raw.get("strikePrice")
        or raw.get("k"),
        default=float("nan"),
    )
    if not math.isfinite(strike) or strike <= 0:
        return "invalid_strike"
    if _normalize_option_type(raw.get("option_type") or raw.get("contract_type") or raw.get("type")) is None:
        return "invalid_option_type"
    return "normalization_error"


def _contract_drop_reason(row: dict[str, Any]) -> str | None:
    expiry = _date_key(row.get("expiry"))
    strike = _safe_float(row.get("strike"), default=0.0)
    time_years = _safe_float(row.get("t_years"), default=0.0)
    iv = _safe_float(row.get("iv"), default=0.0)
    oi = _safe_float(row.get("oi"), default=0.0)
    if _normalize_option_type(row.get("option_type")) is None:
        return "invalid_option_type"
    if strike <= 0:
        return "invalid_strike"
    if not expiry:
        return "missing_expiry"
    if time_years <= ZERO_TOLERANCE:
        return "invalid_time_to_expiry"
    if iv <= ZERO_TOLERANCE:
        return "invalid_implied_volatility"
    if oi <= ZERO_TOLERANCE:
        return "zero_open_interest"
    return None


def normalize_gamma_chain(
    chain: Iterable[dict[str, Any]] | None,
    *,
    now_ts: float | None = None,
    audit: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    now = time.time() if now_ts is None else float(now_ts)
    normalized: list[dict[str, Any]] = []
    for raw in chain or []:
        if audit is not None:
            audit["input_row_count"] = int(audit.get("input_row_count", 0)) + 1
        if not isinstance(raw, dict):
            if audit is not None:
                _bump_drop_reason(audit, "invalid_row_shape")
            continue
        row = _normalize_gamma_contract(raw, now_ts=now)
        if row is None:
            if audit is not None:
                _bump_drop_reason(audit, _raw_drop_reason(raw))
            continue
        normalized.append(row)
        if audit is not None:
            audit["normalized_row_count"] = int(audit.get("normalized_row_count", 0)) + 1
    return normalized


def _sorted_expirations(values: Iterable[Any] | None) -> list[str]:
    return sorted({_date_key(value) for value in values or [] if _date_key(value)})


def resolve_gamma_expiration_selection(
    available_expirations: Sequence[str] | None,
    *,
    selected_scope: str = "selected",
    selected_expiry: str | None = None,
    selected_expiration_set: Sequence[str] | None = None,
    remove_0dte: bool = False,
    scanner_scope: str | None = None,
    today_iso: str | None = None,
) -> dict[str, Any]:
    today_key = today_iso or datetime.now(timezone.utc).date().isoformat()
    available = _sorted_expirations(available_expirations)
    scope_key = (scanner_scope or selected_scope or "selected").strip().lower()
    if scope_key in {"weekly", "monthly", "all"} and scanner_scope:
        scoped = scanner_scope_expirations(available, scope_key, today_iso=today_key)
    else:
        scoped = list(available)
    requested_set = (
        _sorted_expirations(selected_expiration_set) if selected_expiration_set is not None else None
    )
    selected_key = _date_key(selected_expiry)
    if scope_key == "selected":
        included = [selected_key] if selected_key and selected_key in scoped else []
        filter_mode = "selected"
    else:
        requested_lookup = set(requested_set or [])
        included = (
            list(scoped)
            if requested_set is None
            else [expiry for expiry in scoped if expiry in requested_lookup]
        )
        filter_mode = "custom" if requested_set is not None else "all"
    removed_0dte = []
    if remove_0dte and today_key:
        removed_0dte = [expiry for expiry in included if expiry == today_key]
        included = [expiry for expiry in included if expiry != today_key]
    excluded = [expiry for expiry in scoped if expiry not in set(included)]
    excluded_reason_map: dict[str, str] = {}
    requested_lookup = set(requested_set or [])
    for expiry in available:
        if expiry in included:
            excluded_reason_map[expiry] = "included"
            continue
        if scope_key in {"weekly", "monthly"} and expiry not in set(scoped):
            excluded_reason_map[expiry] = f"outside_{scope_key}_scope"
            continue
        if scope_key == "selected":
            excluded_reason_map[expiry] = (
                "selected_expiry"
                if expiry == selected_key
                else "outside_selected_expiry"
            )
            continue
        if requested_set is not None and expiry not in requested_lookup:
            excluded_reason_map[expiry] = "excluded_by_gamma_filters"
            continue
        if remove_0dte and expiry == today_key:
            excluded_reason_map[expiry] = "removed_0dte"
            continue
        excluded_reason_map[expiry] = "excluded"
    return {
        "available_expirations": available,
        "scoped_expirations": scoped,
        "included_expirations": included,
        "excluded_expirations": excluded,
        "excluded_expiration_reasons": excluded_reason_map,
        "selected_scope": scope_key,
        "selected_expiry": selected_key,
        "selected_expiration_set": (
            list(requested_set) if requested_set is not None else list(scoped)
        ),
        "custom_filter_active": requested_set is not None,
        "remove_0dte": bool(remove_0dte),
        "removed_0dte_expirations": removed_0dte,
        "filter_mode": filter_mode,
    }


def filter_gamma_chain(
    chain: Iterable[dict[str, Any]] | None,
    *,
    expirations: Sequence[str] | None = None,
    include_0dte: bool = True,
    today_iso: str | None = None,
) -> list[dict[str, Any]]:
    selected = {_date_key(exp) for exp in expirations or [] if _date_key(exp)}
    today_key = today_iso or datetime.now(timezone.utc).date().isoformat()
    filtered: list[dict[str, Any]] = []
    for row in chain or []:
        expiry = _date_key(row.get("expiry"))
        if selected and expiry not in selected:
            continue
        if not include_0dte and expiry == today_key:
            continue
        filtered.append(dict(row))
    return filtered


def _is_valid_contract_for_gamma(row: dict[str, Any]) -> bool:
    strike = _safe_float(row.get("strike"), default=0.0)
    time_years = _safe_float(row.get("t_years"), default=0.0)
    iv = _safe_float(row.get("iv"), default=0.0)
    oi = _safe_float(row.get("oi"), default=0.0)
    return (
        _normalize_option_type(row.get("option_type")) is not None
        and strike > 0
        and time_years > ZERO_TOLERANCE
        and iv > ZERO_TOLERANCE
        and oi > ZERO_TOLERANCE
    )


def _prepare_gamma_contracts(
    chain: Iterable[dict[str, Any]] | None,
    *,
    expirations: Sequence[str] | None = None,
    include_0dte: bool = True,
    today_iso: str | None = None,
    now_ts: float | None = None,
) -> list[dict[str, Any]]:
    normalized = normalize_gamma_chain(chain, now_ts=now_ts)
    filtered = filter_gamma_chain(
        normalized,
        expirations=expirations,
        include_0dte=include_0dte,
        today_iso=today_iso,
    )
    return [row for row in filtered if _is_valid_contract_for_gamma(row)]


def _solver_reduction_profile(
    *,
    moneyness_band: float,
    oi_threshold: float,
    top_n_calls: int,
    top_n_puts: int,
    atm_neighborhood: int,
    min_rows_per_expiry: int,
    name: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "moneyness_band": max(float(moneyness_band), 0.0),
        "oi_threshold": max(float(oi_threshold), 0.0),
        "top_n_calls": max(int(top_n_calls), 0),
        "top_n_puts": max(int(top_n_puts), 0),
        "atm_neighborhood": max(int(atm_neighborhood), 0),
        "min_rows_per_expiry": max(int(min_rows_per_expiry), 1),
    }


def default_reduced_solver_profiles() -> list[dict[str, Any]]:
    return [
        _solver_reduction_profile(
            moneyness_band=REDUCED_SOLVER_MONEYNESS_BAND,
            oi_threshold=REDUCED_SOLVER_OI_THRESHOLD,
            top_n_calls=REDUCED_SOLVER_TOP_N_CALLS,
            top_n_puts=REDUCED_SOLVER_TOP_N_PUTS,
            atm_neighborhood=REDUCED_SOLVER_ATM_NEIGHBORHOOD,
            min_rows_per_expiry=REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY,
            name="pass_1",
        ),
        _solver_reduction_profile(
            moneyness_band=REDUCED_SOLVER_EXPANDED_MONEYNESS_BAND,
            oi_threshold=REDUCED_SOLVER_EXPANDED_OI_THRESHOLD,
            top_n_calls=REDUCED_SOLVER_EXPANDED_TOP_N_CALLS,
            top_n_puts=REDUCED_SOLVER_EXPANDED_TOP_N_PUTS,
            atm_neighborhood=REDUCED_SOLVER_EXPANDED_ATM_NEIGHBORHOOD,
            min_rows_per_expiry=REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY,
            name="pass_2",
        ),
    ]


def default_gamma_solver_config() -> dict[str, Any]:
    return dict(GAMMA_SOLVER_PRESETS[GAMMA_SOLVER_PRESET_DEFAULT])


def _matching_gamma_solver_preset(config: dict[str, Any]) -> str:
    for preset_name, preset in GAMMA_SOLVER_PRESETS.items():
        if all(config.get(key) == preset.get(key) for key in ("horizon", "band", "remove_0dte", "tail_handling", "refinement_mode")):
            return preset_name
    return "custom"


def normalize_gamma_solver_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = dict(config or {})
    requested_preset = str(raw.get("preset") or GAMMA_SOLVER_PRESET_DEFAULT).strip().lower()
    base = dict(GAMMA_SOLVER_PRESETS.get(requested_preset) or default_gamma_solver_config())
    normalized = {
        "horizon": str(raw.get("horizon") or base["horizon"]).strip().lower(),
        "band": str(raw.get("band") or base["band"]).strip().lower().replace("±", "").replace("%", ""),
        "remove_0dte": bool(raw.get("remove_0dte", base["remove_0dte"])),
        "tail_handling": str(raw.get("tail_handling") or base["tail_handling"]).strip().lower(),
        "refinement_mode": str(raw.get("refinement_mode") or base["refinement_mode"]).strip().lower(),
    }
    if normalized["horizon"] not in GAMMA_SOLVER_HORIZON_DAYS:
        normalized["horizon"] = base["horizon"]
    if normalized["band"] not in GAMMA_SOLVER_BAND_VALUES:
        normalized["band"] = base["band"]
    if normalized["tail_handling"] not in {"minimal", "moderate", "aggressive"}:
        normalized["tail_handling"] = base["tail_handling"]
    if normalized["refinement_mode"] not in {"fast", "balanced", "high_precision"}:
        normalized["refinement_mode"] = base["refinement_mode"]
    normalized["preset"] = _matching_gamma_solver_preset(normalized)
    normalized["is_default"] = normalized["preset"] == GAMMA_SOLVER_PRESET_DEFAULT
    normalized["horizon_days"] = GAMMA_SOLVER_HORIZON_DAYS[normalized["horizon"]]
    normalized["band_value"] = GAMMA_SOLVER_BAND_VALUES[normalized["band"]]
    normalized["adaptive_band"] = normalized["band"] == "adaptive"
    return normalized


def gamma_solver_cache_token(config: dict[str, Any] | None = None) -> str:
    normalized = normalize_gamma_solver_config(config)
    return "|".join(
        [
            normalized["preset"],
            normalized["horizon"],
            normalized["band"],
            "1" if normalized["remove_0dte"] else "0",
            normalized["tail_handling"],
            normalized["refinement_mode"],
        ]
    )


def gamma_solver_profile_label(config: dict[str, Any] | None = None) -> str:
    normalized = normalize_gamma_solver_config(config)
    if normalized["preset"] == "standard":
        return "Standard (Default)"
    if normalized["preset"] == "near_term":
        return "Near-Term"
    if normalized["preset"] == "balanced":
        return "Balanced"
    if normalized["preset"] == "full_chain":
        return "Full Chain"
    band_text = "Adaptive" if normalized["adaptive_band"] else f"±{int(round(normalized['band_value'] * 100.0))}%"
    horizon = normalized["horizon"].upper()
    return f"Custom ({horizon} / {band_text})"


def gamma_solver_effective_expirations(
    expirations: Sequence[str] | None,
    *,
    horizon: str,
    today_iso: str | None = None,
) -> list[str]:
    selected = _sorted_expirations(expirations)
    max_days = GAMMA_SOLVER_HORIZON_DAYS.get(horizon)
    if max_days is None:
        return selected
    today_key = today_iso or datetime.now(timezone.utc).date().isoformat()
    try:
        today_date = datetime.fromisoformat(today_key).date()
    except ValueError:
        today_date = datetime.now(timezone.utc).date()
    bounded: list[str] = []
    for expiry in selected:
        try:
            exp_date = datetime.fromisoformat(expiry).date()
        except ValueError:
            continue
        delta_days = (exp_date - today_date).days
        if delta_days < 0:
            continue
        if delta_days <= max_days:
            bounded.append(expiry)
    return bounded


def gamma_solver_profiles(config: dict[str, Any] | None = None) -> tuple[list[dict[str, Any]], int, int]:
    normalized = normalize_gamma_solver_config(config)
    tail_profiles = {
        "minimal": {
            "pass_1": {"top_n_calls": 0, "top_n_puts": 0, "atm_neighborhood": 1, "oi_threshold": 500.0},
            "pass_2": {"top_n_calls": 1, "top_n_puts": 1, "atm_neighborhood": 2, "oi_threshold": 250.0},
        },
        "moderate": {
            "pass_1": {"top_n_calls": 1, "top_n_puts": 1, "atm_neighborhood": 2, "oi_threshold": 250.0},
            "pass_2": {"top_n_calls": 2, "top_n_puts": 2, "atm_neighborhood": 4, "oi_threshold": 100.0},
        },
        "aggressive": {
            "pass_1": {"top_n_calls": 2, "top_n_puts": 2, "atm_neighborhood": 3, "oi_threshold": 100.0},
            "pass_2": {"top_n_calls": 3, "top_n_puts": 3, "atm_neighborhood": 5, "oi_threshold": 50.0},
        },
    }
    refinement_profiles = {
        "fast": {"passes": 1, "steps": 241, "refinement_steps": 41},
        "balanced": {"passes": 2, "steps": ZERO_GAMMA_STEPS, "refinement_steps": ZERO_GAMMA_REFINEMENT_STEPS},
        "high_precision": {"passes": 2, "steps": 601, "refinement_steps": 161},
    }
    tail = tail_profiles[normalized["tail_handling"]]
    refinement = refinement_profiles[normalized["refinement_mode"]]
    base_band = normalized["band_value"]
    expanded_band = min(0.30, base_band + 0.10) if normalized["adaptive_band"] else base_band
    profiles = [
        _solver_reduction_profile(
            moneyness_band=base_band,
            oi_threshold=tail["pass_1"]["oi_threshold"],
            top_n_calls=tail["pass_1"]["top_n_calls"],
            top_n_puts=tail["pass_1"]["top_n_puts"],
            atm_neighborhood=tail["pass_1"]["atm_neighborhood"],
            min_rows_per_expiry=REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY,
            name="pass_1",
        )
    ]
    if refinement["passes"] > 1:
        profiles.append(
            _solver_reduction_profile(
                moneyness_band=expanded_band,
                oi_threshold=tail["pass_2"]["oi_threshold"],
                top_n_calls=tail["pass_2"]["top_n_calls"],
                top_n_puts=tail["pass_2"]["top_n_puts"],
                atm_neighborhood=tail["pass_2"]["atm_neighborhood"],
                min_rows_per_expiry=REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY,
                name="pass_2",
            )
        )
    return profiles, int(refinement["steps"]), int(refinement["refinement_steps"])


def build_reduced_solver_universe(
    contracts: Sequence[dict[str, Any]] | None,
    current_spot: float,
    *,
    profile: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = [dict(row) for row in (contracts or []) if isinstance(row, dict)]
    if not rows:
        selected_profile = dict(profile or default_reduced_solver_profiles()[0])
        return {
            "contracts": [],
            "profile": selected_profile,
            "input_row_count": 0,
            "included_row_count": 0,
            "dropped_row_count": 0,
            "kept_rows_by_reason": {},
            "per_expiry_available_row_counts": {},
            "per_expiry_retained_row_counts": {},
        }
    selected_profile = dict(profile or default_reduced_solver_profiles()[0])
    spot_value = max(_safe_float(current_spot, default=0.0), 0.01)
    band = max(_safe_float(selected_profile.get("moneyness_band"), default=REDUCED_SOLVER_MONEYNESS_BAND), 0.0)
    top_n_calls = max(int(selected_profile.get("top_n_calls", REDUCED_SOLVER_TOP_N_CALLS)), 0)
    top_n_puts = max(int(selected_profile.get("top_n_puts", REDUCED_SOLVER_TOP_N_PUTS)), 0)
    atm_neighborhood = max(int(selected_profile.get("atm_neighborhood", REDUCED_SOLVER_ATM_NEIGHBORHOOD)), 0)
    min_rows_per_expiry = max(int(selected_profile.get("min_rows_per_expiry", REDUCED_SOLVER_MIN_ROWS_PER_EXPIRY)), 1)
    band_min = spot_value * (1.0 - band)
    band_max = spot_value * (1.0 + band)

    indexed_rows = list(enumerate(rows))
    by_expiry: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for idx, row in indexed_rows:
        expiry = _date_key(row.get("expiry")) or "unknown"
        by_expiry.setdefault(expiry, []).append((idx, row))

    selected_indices: dict[int, str] = {}
    for expiry, expiry_rows in by_expiry.items():
        sorted_by_atm = sorted(
            expiry_rows,
            key=lambda item: (
                abs(_safe_float(item[1].get("strike"), default=spot_value) - spot_value),
                -_safe_float(item[1].get("oi"), default=0.0),
                _safe_float(item[1].get("strike"), default=spot_value),
            ),
        )
        for idx, row in expiry_rows:
            strike = _safe_float(row.get("strike"), default=float("nan"))
            if math.isfinite(strike) and band_min <= strike <= band_max:
                selected_indices.setdefault(idx, "within_moneyness_band")
        for option_type, top_n, reason in (
            ("call", top_n_calls, "top_call_tail"),
            ("put", top_n_puts, "top_put_tail"),
        ):
            if top_n <= 0:
                continue
            tail_rows = [
                (idx, row)
                for idx, row in expiry_rows
                if _normalize_option_type(row.get("option_type")) == option_type
                and not (band_min <= _safe_float(row.get("strike"), default=float("nan")) <= band_max)
            ]
            tail_rows.sort(
                key=lambda item: (
                    -_safe_float(item[1].get("oi"), default=0.0),
                    abs(_safe_float(item[1].get("strike"), default=spot_value) - spot_value),
                    _safe_float(item[1].get("strike"), default=spot_value),
                )
            )
            for idx, _row in tail_rows[:top_n]:
                selected_indices.setdefault(idx, reason)
        if atm_neighborhood > 0:
            for option_type in ("call", "put"):
                type_rows = [
                    (idx, row)
                    for idx, row in sorted_by_atm
                    if _normalize_option_type(row.get("option_type")) == option_type
                ]
                for idx, _row in type_rows[:atm_neighborhood]:
                    selected_indices.setdefault(idx, "atm_neighborhood")
        expiry_selected = [idx for idx, _row in expiry_rows if idx in selected_indices]
        if len(expiry_selected) < min_rows_per_expiry:
            for idx, _row in sorted_by_atm:
                if idx in selected_indices:
                    continue
                selected_indices[idx] = "expiry_representation"
                expiry_selected.append(idx)
                if len(expiry_selected) >= min_rows_per_expiry:
                    break

    reduced_rows = [rows[idx] for idx in sorted(selected_indices)]
    kept_rows_by_reason: dict[str, int] = {}
    per_expiry_retained: dict[str, int] = {}
    per_expiry_available = {expiry: len(expiry_rows) for expiry, expiry_rows in by_expiry.items()}
    rows_inside_band = 0
    rows_outside_band = 0
    for idx in sorted(selected_indices):
        reason = selected_indices[idx]
        kept_rows_by_reason[reason] = kept_rows_by_reason.get(reason, 0) + 1
        row = rows[idx]
        expiry = _date_key(row.get("expiry")) or "unknown"
        per_expiry_retained[expiry] = per_expiry_retained.get(expiry, 0) + 1
        strike = _safe_float(row.get("strike"), default=float("nan"))
        if math.isfinite(strike) and band_min <= strike <= band_max:
            rows_inside_band += 1
        else:
            rows_outside_band += 1
    return {
        "contracts": reduced_rows,
        "profile": selected_profile,
        "input_row_count": len(rows),
        "included_row_count": len(reduced_rows),
        "dropped_row_count": max(len(rows) - len(reduced_rows), 0),
        "kept_rows_by_reason": kept_rows_by_reason,
        "per_expiry_available_row_counts": per_expiry_available,
        "per_expiry_retained_row_counts": per_expiry_retained,
        "rows_retained_inside_band": rows_inside_band,
        "rows_retained_outside_band": rows_outside_band,
        "effective_moneyness_band": band,
    }


def prepare_gamma_analysis(
    chain: Iterable[dict[str, Any]] | None,
    *,
    selected_scope: str = "selected",
    selected_expiry: str | None = None,
    selected_expiration_set: Sequence[str] | None = None,
    remove_0dte: bool = False,
    scanner_scope: str | None = None,
    today_iso: str | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    drop_audit = _new_drop_audit()
    normalized = normalize_gamma_chain(chain, now_ts=now_ts, audit=drop_audit)
    inclusion = resolve_gamma_expiration_selection(
        [row.get("expiry") for row in normalized],
        selected_scope=selected_scope,
        selected_expiry=selected_expiry,
        selected_expiration_set=selected_expiration_set,
        remove_0dte=remove_0dte,
        scanner_scope=scanner_scope,
        today_iso=today_iso,
    )
    included_lookup = set(inclusion["included_expirations"])
    scoped_set = set(inclusion["scoped_expirations"])
    raw_included_rows = 0
    contracts: list[dict[str, Any]] = []
    for row in normalized:
        expiry = _date_key(row.get("expiry"))
        if not expiry:
            _bump_drop_reason(drop_audit, "missing_expiry")
            continue
        if expiry not in scoped_set:
            scope_key = inclusion.get("selected_scope", selected_scope)
            if scope_key in {"weekly", "monthly"}:
                _bump_drop_reason(drop_audit, f"outside_{scope_key}_scope")
            elif scope_key == "selected":
                _bump_drop_reason(drop_audit, "outside_selected_expiry")
            else:
                _bump_drop_reason(drop_audit, "outside_scope")
            continue
        if expiry not in included_lookup:
            reason = (
                inclusion.get("excluded_expiration_reasons", {}).get(expiry)
                or "excluded_expiration"
            )
            _bump_drop_reason(drop_audit, reason)
            continue
        raw_included_rows += 1
        drop_reason = _contract_drop_reason(row)
        if drop_reason is not None:
            _bump_drop_reason(drop_audit, drop_reason)
            continue
        contracts.append(dict(row))
        drop_audit["included_row_count"] = int(drop_audit.get("included_row_count", 0)) + 1
        if len(drop_audit["included_contract_sample"]) < 5:
            drop_audit["included_contract_sample"].append(
                {
                    "expiry": expiry,
                    "strike": _safe_float(row.get("strike"), default=0.0),
                    "option_type": _normalize_option_type(row.get("option_type")),
                    "oi": _safe_float(row.get("oi"), default=0.0),
                    "iv": _safe_float(row.get("iv"), default=0.0),
                    "t_years": _safe_float(row.get("t_years"), default=0.0),
                }
            )
    inclusion["included_row_count"] = len(contracts)
    inclusion["raw_included_row_count"] = raw_included_rows
    inclusion["invalid_row_count"] = max(raw_included_rows - len(contracts), 0)
    inclusion["drop_audit"] = drop_audit
    return {
        "contracts": contracts,
        "inclusion": inclusion,
    }


def _gamma_exposure_components_at_spot(
    option_row: dict[str, Any],
    spot: float,
) -> tuple[float, float, float, float] | None:
    option_type = _normalize_option_type(option_row.get("option_type"))
    if option_type is None:
        return None
    gamma = compute_option_gamma_at_spot(
        spot,
        _safe_float(option_row.get("strike"), default=0.0),
        _safe_float(option_row.get("iv"), default=0.0),
        _safe_float(option_row.get("t_years"), default=0.0),
        _safe_float(option_row.get("rate"), DEFAULT_RATE),
        _safe_float(option_row.get("dividend_yield"), DEFAULT_DIVIDEND_YIELD),
    )
    if gamma <= 0:
        return None
    oi = max(_safe_float(option_row.get("oi"), default=0.0), 0.0)
    if oi <= 0:
        return None
    contract_size = max(_safe_float(option_row.get("contract_size"), CONTRACT_MULTIPLIER), 1.0)
    sign = 1.0 if option_type == "call" else -1.0
    return sign, gamma, oi, contract_size


def compute_signed_gamma_exposure_at_spot(option_row: dict[str, Any], spot: float) -> float:
    components = _gamma_exposure_components_at_spot(option_row, spot)
    if components is None:
        return 0.0
    sign, gamma, oi, contract_size = components
    # Raw signed gamma used by the spot-space solver:
    # sign(call=+1, put=-1) * gamma(S) * open_interest * contract_multiplier * S^2
    exposure = sign * gamma * oi * contract_size * (float(spot) * float(spot))
    return 0.0 if abs(exposure) <= ZERO_TOLERANCE else exposure


def compute_spot_scaled_net_gex_at_spot(option_row: dict[str, Any], spot: float) -> float:
    components = _gamma_exposure_components_at_spot(option_row, spot)
    if components is None:
        return 0.0
    sign, gamma, oi, contract_size = components
    # Vendor-style headline Net GEX is closer to a spot-scaled convention:
    # sign(call=+1, put=-1) * gamma(S) * open_interest * contract_multiplier * S
    exposure = sign * gamma * oi * contract_size * float(spot)
    return 0.0 if abs(exposure) <= ZERO_TOLERANCE else exposure


def _total_gamma_for_contracts(contracts: Sequence[dict[str, Any]], spot: float) -> float:
    total = 0.0
    for contract in contracts:
        total += compute_signed_gamma_exposure_at_spot(contract, spot)
    if abs(total) <= ROOT_EPSILON:
        return 0.0
    return total


def _total_net_gex_for_contracts(contracts: Sequence[dict[str, Any]], spot: float) -> float:
    total = 0.0
    for contract in contracts:
        total += compute_spot_scaled_net_gex_at_spot(contract, spot)
    if abs(total) <= ROOT_EPSILON:
        return 0.0
    return total


def build_signed_gex_series(
    strikes: Sequence[Any] | None,
    *,
    gex_net: Sequence[Any] | None = None,
    gex_calls: Sequence[Any] | None = None,
    gex_puts: Sequence[Any] | None = None,
) -> list[dict[str, float]]:
    if not strikes:
        return []
    use_signed_legs = (
        gex_calls is not None
        and gex_puts is not None
        and len(strikes) == len(gex_calls) == len(gex_puts)
    )
    use_net = gex_net is not None and len(strikes) == len(gex_net)
    if not use_signed_legs and not use_net:
        return []
    grouped: dict[float, dict[str, float]] = {}
    for idx, raw_strike in enumerate(strikes):
        strike = _safe_float(raw_strike, default=float("nan"))
        if not math.isfinite(strike):
            continue
        row = grouped.setdefault(
            strike,
            {"strike": strike, "gex_calls": 0.0, "gex_puts": 0.0, "gex_net": 0.0},
        )
        if use_signed_legs:
            call_gex = _safe_float(gex_calls[idx])
            put_gex = _safe_float(gex_puts[idx])
            row["gex_calls"] += call_gex
            row["gex_puts"] += put_gex
            row["gex_net"] += call_gex + put_gex
        else:
            row["gex_net"] += _safe_float(gex_net[idx])
    return [grouped[strike] for strike in sorted(grouped)]


def cumulative_net_gex(gex_net: Sequence[Any] | None) -> list[float]:
    total = 0.0
    cumulative: list[float] = []
    for value in gex_net or []:
        total += _safe_float(value)
        cumulative.append(total)
    return cumulative


def compute_total_gamma_curve(
    chain: Iterable[dict[str, Any]] | None,
    spot_grid: Sequence[Any],
    *,
    include_0dte: bool = True,
    selected_expirations: Sequence[str] | None = None,
    selected_scope: str = "all",
    selected_expiry: str | None = None,
    remove_0dte: bool | None = None,
    prepared_contracts: Sequence[dict[str, Any]] | None = None,
    inclusion: dict[str, Any] | None = None,
    today_iso: str | None = None,
    now_ts: float | None = None,
) -> dict[str, Any]:
    remove_0dte_flag = bool(remove_0dte) if remove_0dte is not None else (not include_0dte)
    if prepared_contracts is None:
        prepared = prepare_gamma_analysis(
            chain,
            selected_scope=selected_scope,
            selected_expiry=selected_expiry,
            selected_expiration_set=selected_expirations,
            remove_0dte=remove_0dte_flag,
            today_iso=today_iso,
            now_ts=now_ts,
        )
        contracts = prepared["contracts"]
        inclusion_meta = prepared["inclusion"]
    else:
        contracts = [dict(row) for row in prepared_contracts]
        inclusion_meta = dict(inclusion or {})
    spots: list[float] = []
    total_gamma: list[float] = []
    for raw_spot in spot_grid:
        spot = _safe_float(raw_spot, default=float("nan"))
        if not math.isfinite(spot) or spot <= 0:
            continue
        spots.append(spot)
        total_gamma.append(_total_gamma_for_contracts(contracts, spot))
    return {
        "spots": spots,
        "total_gamma": total_gamma,
        "valid_contract_count": len(contracts),
        "inclusion": inclusion_meta,
    }


def _build_spot_grid(spot_min: float, spot_max: float, steps: int) -> list[float]:
    count = max(int(steps), 2)
    if abs(spot_max - spot_min) <= ZERO_TOLERANCE:
        return [spot_min, spot_max]
    return [
        spot_min + ((spot_max - spot_min) * idx / (count - 1))
        for idx in range(count)
    ]


def _find_sign_change_intervals(
    spots: Sequence[Any] | None,
    total_gamma_values: Sequence[Any] | None,
    *,
    epsilon: float = ROOT_EPSILON,
) -> list[dict[str, float]]:
    if not spots or not total_gamma_values or len(spots) != len(total_gamma_values):
        return []
    ordered = sorted(
        (
            (
                _safe_float(spots[idx], default=float("nan")),
                _safe_float(total_gamma_values[idx], default=float("nan")),
            )
            for idx in range(len(spots))
        ),
        key=lambda item: item[0],
    )
    nonzero_points: list[tuple[float, float]] = []
    for spot, total_gamma in ordered:
        if not math.isfinite(spot) or not math.isfinite(total_gamma):
            continue
        if abs(total_gamma) <= epsilon:
            continue
        nonzero_points.append((spot, total_gamma))
    intervals: list[dict[str, float]] = []
    for idx in range(1, len(nonzero_points)):
        left_spot, left_gamma = nonzero_points[idx - 1]
        right_spot, right_gamma = nonzero_points[idx]
        left_sign = _sign_bucket(left_gamma, epsilon)
        right_sign = _sign_bucket(right_gamma, epsilon)
        if left_sign == right_sign:
            continue
        interval = {
            "left_spot": left_spot,
            "left_gamma": left_gamma,
            "right_spot": right_spot,
            "right_gamma": right_gamma,
        }
        root = _interpolate_root(left_spot, left_gamma, right_spot, right_gamma)
        if root is not None and math.isfinite(root):
            interval["interpolated_root"] = root
        intervals.append(interval)
    return intervals


def compute_zero_gamma_from_curve(
    spots: Sequence[Any] | None,
    total_gamma_values: Sequence[Any] | None,
    *,
    epsilon: float = ROOT_EPSILON,
) -> float | None:
    intervals = _find_sign_change_intervals(spots, total_gamma_values, epsilon=epsilon)
    if not intervals:
        return None
    root = intervals[0].get("interpolated_root")
    return root if root is not None and math.isfinite(root) else None


def compute_zero_gamma(
    chain: Iterable[dict[str, Any]] | None,
    current_spot: float,
    *,
    include_0dte: bool = True,
    selected_expirations: Sequence[str] | None = None,
    selected_scope: str = "all",
    selected_expiry: str | None = None,
    remove_0dte: bool | None = None,
    prepared_contracts: Sequence[dict[str, Any]] | None = None,
    inclusion: dict[str, Any] | None = None,
    today_iso: str | None = None,
    now_ts: float | None = None,
    range_ratio: tuple[float, float] = ZERO_GAMMA_RANGE,
    steps: int = ZERO_GAMMA_STEPS,
    refinement_steps: int = ZERO_GAMMA_REFINEMENT_STEPS,
    max_expansions: int = ZERO_GAMMA_MAX_EXPANSIONS,
    include_curve: bool = False,
) -> dict[str, Any]:
    remove_0dte_flag = bool(remove_0dte) if remove_0dte is not None else (not include_0dte)
    if prepared_contracts is None:
        prepared = prepare_gamma_analysis(
            chain,
            selected_scope=selected_scope,
            selected_expiry=selected_expiry,
            selected_expiration_set=selected_expirations,
            remove_0dte=remove_0dte_flag,
            today_iso=today_iso,
            now_ts=now_ts,
        )
        contracts = prepared["contracts"]
        inclusion_meta = prepared["inclusion"]
    else:
        contracts = [dict(row) for row in prepared_contracts]
        inclusion_meta = dict(inclusion or {})
    spot_value = _safe_float(current_spot, default=0.0)
    if spot_value <= 0 or not contracts:
        diagnostics = {
            "solver_spot_min": (spot_value * min(range_ratio) if spot_value > 0 else None),
            "solver_spot_max": (spot_value * max(range_ratio) if spot_value > 0 else None),
            "total_gamma_at_min": None,
            "total_gamma_at_spot": None,
            "total_gamma_at_max": None,
            "has_sign_crossing": False,
            "valid_contract_count": len(contracts),
            "grid_point_count": 0,
            "first_sign_change_interval": None,
            "sign_change_intervals": [],
            "included_expirations": list(inclusion_meta.get("included_expirations") or []),
            "excluded_expirations": list(inclusion_meta.get("excluded_expirations") or []),
            "available_expirations": list(inclusion_meta.get("available_expirations") or []),
            "remove_0dte": bool(inclusion_meta.get("remove_0dte", remove_0dte_flag)),
            "selected_scope": inclusion_meta.get("selected_scope", selected_scope),
            "selected_expiry": inclusion_meta.get("selected_expiry", selected_expiry),
            "selected_expiration_set": list(
                inclusion_meta.get("selected_expiration_set") or (selected_expirations or [])
            ),
            "included_row_count": int(inclusion_meta.get("included_row_count", len(contracts))),
            "dropped_row_count": int(
                (inclusion_meta.get("drop_audit") or {}).get("dropped_row_count", 0)
            ),
            "dropped_rows_by_reason": dict(
                (inclusion_meta.get("drop_audit") or {}).get("dropped_rows_by_reason") or {}
            ),
            "excluded_expiration_reasons": dict(
                inclusion_meta.get("excluded_expiration_reasons") or {}
            ),
            "included_contract_sample": list(
                (inclusion_meta.get("drop_audit") or {}).get("included_contract_sample") or []
            ),
        }
        return {"zero_gamma": None, "total_gamma_at_spot": None, "diagnostics": diagnostics}
    lo_ratio, hi_ratio = range_ratio
    base_lo = min(lo_ratio, hi_ratio)
    base_hi = max(lo_ratio, hi_ratio)
    spot_min = max(spot_value * base_lo, 0.01)
    spot_max = max(spot_value * base_hi, spot_min)
    chosen_curve: dict[str, Any] | None = None
    intervals: list[dict[str, float]] = []
    expansions_used = 0
    for expansion in range(max(int(max_expansions), 0) + 1):
        lo = max(0.1, base_lo * (0.8 ** expansion))
        hi = max(lo, base_hi * (1.2 ** expansion))
        trial_min = max(spot_value * lo, 0.01)
        trial_max = max(spot_value * hi, trial_min)
        trial_curve = compute_total_gamma_curve(
            None,
            _build_spot_grid(trial_min, trial_max, steps),
            prepared_contracts=contracts,
            inclusion=inclusion_meta,
            include_0dte=True,
            selected_expirations=None,
            today_iso=today_iso,
            now_ts=now_ts,
        )
        trial_intervals = _find_sign_change_intervals(
            trial_curve["spots"],
            trial_curve["total_gamma"],
        )
        chosen_curve = trial_curve
        intervals = trial_intervals
        spot_min = trial_min
        spot_max = trial_max
        expansions_used = expansion
        if intervals:
            break
    total_gamma_at_spot = _total_gamma_for_contracts(contracts, spot_value)
    refined_interval = dict(intervals[0]) if intervals else None
    if refined_interval is not None:
        for _ in range(2):
            left_spot = _safe_float(refined_interval.get("left_spot"), default=float("nan"))
            right_spot = _safe_float(refined_interval.get("right_spot"), default=float("nan"))
            if not (math.isfinite(left_spot) and math.isfinite(right_spot) and right_spot > left_spot):
                break
            local_curve = compute_total_gamma_curve(
                None,
                _build_spot_grid(left_spot, right_spot, refinement_steps),
                prepared_contracts=contracts,
                inclusion=inclusion_meta,
                include_0dte=True,
                selected_expirations=None,
                today_iso=today_iso,
                now_ts=now_ts,
            )
            local_intervals = _find_sign_change_intervals(
                local_curve["spots"],
                local_curve["total_gamma"],
            )
            if not local_intervals:
                break
            refined_interval = dict(local_intervals[0])
            chosen_curve = local_curve
            intervals = local_intervals
    zero_gamma = None
    if refined_interval is not None:
        root = refined_interval.get("interpolated_root")
        if root is not None and math.isfinite(root):
            zero_gamma = float(root)
    diagnostics = {
        "solver_spot_min": spot_min,
        "solver_spot_max": spot_max,
        "total_gamma_at_min": (
            chosen_curve["total_gamma"][0]
            if chosen_curve and chosen_curve["total_gamma"]
            else None
        ),
        "total_gamma_at_spot": total_gamma_at_spot,
        "total_gamma_at_max": (
            chosen_curve["total_gamma"][-1]
            if chosen_curve and chosen_curve["total_gamma"]
            else None
        ),
        "has_sign_crossing": zero_gamma is not None,
        "valid_contract_count": len(contracts),
        "grid_point_count": len(chosen_curve["spots"]) if chosen_curve else 0,
        "first_sign_change_interval": refined_interval,
        "sign_change_intervals": intervals,
        "solver_expansions_used": expansions_used,
        "included_expirations": list(inclusion_meta.get("included_expirations") or []),
        "excluded_expirations": list(inclusion_meta.get("excluded_expirations") or []),
        "available_expirations": list(inclusion_meta.get("available_expirations") or []),
        "remove_0dte": bool(inclusion_meta.get("remove_0dte", remove_0dte_flag)),
        "selected_scope": inclusion_meta.get("selected_scope", selected_scope),
        "selected_expiry": inclusion_meta.get("selected_expiry", selected_expiry),
        "selected_expiration_set": list(
            inclusion_meta.get("selected_expiration_set") or (selected_expirations or [])
        ),
        "included_row_count": int(inclusion_meta.get("included_row_count", len(contracts))),
        "dropped_row_count": int(
            (inclusion_meta.get("drop_audit") or {}).get("dropped_row_count", 0)
        ),
        "dropped_rows_by_reason": dict(
            (inclusion_meta.get("drop_audit") or {}).get("dropped_rows_by_reason") or {}
        ),
        "excluded_expiration_reasons": dict(
            inclusion_meta.get("excluded_expiration_reasons") or {}
        ),
        "included_contract_sample": list(
            (inclusion_meta.get("drop_audit") or {}).get("included_contract_sample") or []
        ),
    }
    if include_curve:
        diagnostics["curve"] = [
            {
                "spot": chosen_curve["spots"][idx],
                "total_gamma": chosen_curve["total_gamma"][idx],
                "sign": _sign_bucket(chosen_curve["total_gamma"][idx]),
            }
            for idx in range(len(chosen_curve["spots"]))
        ]
    return {
        "zero_gamma": zero_gamma,
        "total_gamma_at_spot": total_gamma_at_spot,
        "diagnostics": diagnostics,
    }


def classify_gamma_regime(
    current_spot: float | None,
    zero_gamma: float | None,
    total_gamma_at_spot: float | None,
    *,
    neutral_band_ratio: float = GAMMA_NEUTRAL_BAND_RATIO,
    gamma_epsilon: float = ROOT_EPSILON,
) -> str:
    total_gamma = _safe_float(total_gamma_at_spot, default=float("nan"))
    if math.isfinite(total_gamma):
        if abs(total_gamma) <= gamma_epsilon:
            return "Gamma Neutral"
        return "Long Gamma" if total_gamma > 0 else "Short Gamma"
    spot_value = _safe_float(current_spot, default=float("nan"))
    zero_value = _safe_float(zero_gamma, default=float("nan"))
    if not (math.isfinite(spot_value) and math.isfinite(zero_value) and zero_value > 0):
        return "Gamma Regime Unavailable"
    if abs(spot_value - zero_value) <= abs(zero_value) * max(neutral_band_ratio, 0.0):
        return "Gamma Neutral"
    return "Long Gamma" if spot_value > zero_value else "Short Gamma"


def spot_vs_zero_gamma_pct(spot: float | None, zero_gamma: float | None) -> float | None:
    spot_value = _safe_float(spot, default=float("nan"))
    zero_value = _safe_float(zero_gamma, default=float("nan"))
    if not (math.isfinite(spot_value) and math.isfinite(zero_value) and zero_value != 0):
        return None
    return ((spot_value - zero_value) / zero_value) * 100.0


def spot_vs_zero_gamma_label(
    current_spot: float | None,
    zero_gamma: float | None,
    *,
    neutral_band_ratio: float = GAMMA_NEUTRAL_BAND_RATIO,
) -> str:
    spot_value = _safe_float(current_spot, default=float("nan"))
    zero_value = _safe_float(zero_gamma, default=float("nan"))
    if not (math.isfinite(spot_value) and math.isfinite(zero_value) and zero_value > 0):
        return "No Zero Gamma in tested range"
    if abs(spot_value - zero_value) <= abs(zero_value) * max(neutral_band_ratio, 0.0):
        return "At Zero Gamma"
    return "Above Zero Gamma" if spot_value > zero_value else "Below Zero Gamma"


def build_gamma_profile(
    chain: Iterable[dict[str, Any]] | None,
    current_spot: float,
    *,
    expirations: Sequence[str] | None = None,
    include_0dte: bool = True,
    selected_scope: str = "all",
    selected_expiry: str | None = None,
    remove_0dte: bool | None = None,
    chart_strike_range: tuple[float, float] | None = None,
    today_iso: str | None = None,
    now_ts: float | None = None,
    zero_gamma_range: tuple[float, float] = ZERO_GAMMA_RANGE,
    zero_gamma_steps: int = ZERO_GAMMA_STEPS,
    include_solver_curve: bool = False,
    solver_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    remove_0dte_flag = bool(remove_0dte) if remove_0dte is not None else (not include_0dte)
    normalized_solver_config = normalize_gamma_solver_config(solver_config)
    chart_prepared = prepare_gamma_analysis(
        chain,
        selected_scope=selected_scope,
        selected_expiry=selected_expiry,
        selected_expiration_set=expirations,
        remove_0dte=remove_0dte_flag,
        today_iso=today_iso,
        now_ts=now_ts,
    )
    contracts = chart_prepared["contracts"]
    inclusion = chart_prepared["inclusion"]
    solver_selected_expirations = list(inclusion.get("selected_expiration_set") or [])
    if selected_scope == "all":
        solver_selected_expirations = gamma_solver_effective_expirations(
            solver_selected_expirations,
            horizon=normalized_solver_config["horizon"],
            today_iso=today_iso,
        )
    elif selected_expiry:
        solver_selected_expirations = [_date_key(selected_expiry)] if _date_key(selected_expiry) else []
    solver_prepared = prepare_gamma_analysis(
        chain,
        selected_scope=selected_scope,
        selected_expiry=selected_expiry,
        selected_expiration_set=solver_selected_expirations if selected_scope == "all" else None,
        remove_0dte=bool(normalized_solver_config["remove_0dte"]),
        today_iso=today_iso,
        now_ts=now_ts,
    )
    solver_contracts = solver_prepared["contracts"]
    solver_inclusion = solver_prepared["inclusion"]
    chart_contracts = contracts
    if chart_strike_range is not None:
        lo_strike = _safe_float(chart_strike_range[0], default=float("-inf"))
        hi_strike = _safe_float(chart_strike_range[1], default=float("inf"))
        chart_contracts = [
            row
            for row in contracts
            if lo_strike <= _safe_float(row.get("strike"), default=float("nan")) <= hi_strike
        ]
    grouped: dict[float, dict[str, float]] = {}
    total_oi = 0.0
    for contract in chart_contracts:
        strike = _safe_float(contract.get("strike"), default=float("nan"))
        if not math.isfinite(strike):
            continue
        option_type = _normalize_option_type(contract.get("option_type"))
        exposure = compute_signed_gamma_exposure_at_spot(contract, current_spot)
        row = grouped.setdefault(
            strike,
            {"strike": strike, "gex_calls": 0.0, "gex_puts": 0.0, "gex_net": 0.0},
        )
        if option_type == "call":
            row["gex_calls"] += exposure
        elif option_type == "put":
            row["gex_puts"] += exposure
        row["gex_net"] = row["gex_calls"] + row["gex_puts"]
        total_oi += max(_safe_float(contract.get("oi"), default=0.0), 0.0)
    series = [grouped[strike] for strike in sorted(grouped)]
    strikes = [row["strike"] for row in series]
    gex_calls = [row["gex_calls"] for row in series]
    gex_puts = [row["gex_puts"] for row in series]
    gex_net = [row["gex_net"] for row in series]
    gex_cumulative = cumulative_net_gex(gex_net)
    page_net_gex = _total_net_gex_for_contracts(contracts, current_spot) if contracts else None
    full_total_gamma_at_spot = (
        _total_gamma_for_contracts(solver_contracts, current_spot) if solver_contracts else None
    )
    aggregate_solver_mode = (
        selected_scope == "all"
        and len(solver_inclusion.get("included_expirations") or []) > 1
        and len(solver_contracts) >= AGGREGATE_SOLVER_CONTRACT_THRESHOLD
    )
    if aggregate_solver_mode:
        pass_profiles, solver_steps, refinement_steps = gamma_solver_profiles(normalized_solver_config)
        pass_results: list[dict[str, Any]] = []
        for pass_idx, reduction_profile in enumerate(pass_profiles, start=1):
            reduced = build_reduced_solver_universe(
                solver_contracts,
                current_spot,
                profile=reduction_profile,
            )
            solver_pass = compute_zero_gamma(
                None,
                current_spot,
                prepared_contracts=reduced["contracts"],
                inclusion=solver_inclusion,
                include_0dte=True,
                selected_expirations=None,
                today_iso=today_iso,
                now_ts=now_ts,
                range_ratio=zero_gamma_range,
                steps=solver_steps,
                refinement_steps=refinement_steps,
                include_curve=(include_solver_curve and pass_idx == len(pass_profiles)),
            )
            pass_results.append(
                {
                    "reduced": reduced,
                    "solver": solver_pass,
                    "profile": reduction_profile,
                }
            )
        final_pass = pass_results[-1]
        selected_pass = final_pass
        pass_1_root = pass_results[0]["solver"]["zero_gamma"]
        pass_2_root = pass_results[-1]["solver"]["zero_gamma"]
        zero_gamma_delta = (
            abs(float(pass_2_root) - float(pass_1_root))
            if pass_1_root is not None and pass_2_root is not None
            else None
        )
        published_root_source = "pass_2"
        if pass_1_root is None and pass_2_root is None:
            convergence_status = "no_crossing"
            solver_confidence = "medium"
            published_root_source = "none"
        elif pass_1_root is None and pass_2_root is not None:
            convergence_status = "found_on_refinement"
            solver_confidence = "medium"
            published_root_source = "pass_2"
        elif pass_1_root is not None and pass_2_root is None:
            convergence_status = "lost_on_refinement"
            solver_confidence = "low"
            selected_pass = pass_results[0]
            published_root_source = "pass_1_fallback"
        elif zero_gamma_delta is not None and zero_gamma_delta <= ZERO_GAMMA_CONVERGENCE_TOLERANCE:
            convergence_status = "converged"
            solver_confidence = "high"
            published_root_source = "pass_2"
        else:
            convergence_status = "diverged"
            solver_confidence = "low"
            selected_pass = pass_results[0]
            published_root_source = "pass_1_fallback"
        solver = selected_pass["solver"]
        zero_gamma = solver["zero_gamma"]
        solver_diag = dict(solver["diagnostics"] or {})
        solver_diag.update(
            {
                "solver_universe_mode": "reduced_aggregate",
                "page_contract_count": len(contracts),
                "page_expiries_used": len(inclusion.get("included_expirations") or []),
                "adaptive_refinement_ran": True,
                "default_solver_band": pass_results[0]["reduced"]["effective_moneyness_band"],
                "final_effective_band": selected_pass["reduced"]["effective_moneyness_band"],
                "full_contract_count": len(solver_contracts),
                "solver_contract_count": len(selected_pass["reduced"]["contracts"]),
                "page_net_gex": page_net_gex,
                "total_gamma_at_spot": full_total_gamma_at_spot,
                "solver_total_gamma_at_spot": solver["total_gamma_at_spot"],
                "zero_gamma_pass_1": pass_1_root,
                "zero_gamma_pass_2": pass_2_root,
                "zero_gamma_delta": zero_gamma_delta,
                "solver_confidence": solver_confidence,
                "convergence_status": convergence_status,
                "published_root_source": published_root_source,
                "included_rows_per_pass": [
                    int(item["reduced"]["included_row_count"]) for item in pass_results
                ],
                "included_expirations_per_pass": [
                    list(solver_inclusion.get("included_expirations") or []) for _ in pass_results
                ],
                "reduction_profiles": [dict(item["profile"]) for item in pass_results],
                "rows_kept_by_reduction_reason_per_pass": [
                    dict(item["reduced"]["kept_rows_by_reason"]) for item in pass_results
                ],
                "rows_dropped_per_pass": [
                    int(item["reduced"]["dropped_row_count"]) for item in pass_results
                ],
                "rows_retained_inside_band_per_pass": [
                    int(item["reduced"]["rows_retained_inside_band"]) for item in pass_results
                ],
                "rows_retained_outside_band_per_pass": [
                    int(item["reduced"]["rows_retained_outside_band"]) for item in pass_results
                ],
                "per_expiry_retained_row_counts_per_pass": [
                    dict(item["reduced"]["per_expiry_retained_row_counts"])
                    for item in pass_results
                ],
                "per_expiry_available_row_counts": dict(
                    pass_results[-1]["reduced"]["per_expiry_available_row_counts"]
                ),
                "solver_horizon": normalized_solver_config["horizon"],
                "solver_horizon_days": normalized_solver_config["horizon_days"],
                "solver_band": normalized_solver_config["band"],
                "solver_band_value": normalized_solver_config["band_value"],
                "solver_remove_0dte": bool(normalized_solver_config["remove_0dte"]),
                "tail_handling": normalized_solver_config["tail_handling"],
                "refinement_mode": normalized_solver_config["refinement_mode"],
                "solver_preset": normalized_solver_config["preset"],
                "solver_profile_label": gamma_solver_profile_label(normalized_solver_config),
                "contracts_used": len(selected_pass["reduced"]["contracts"]),
                "expiries_used": len(solver_inclusion.get("included_expirations") or []),
                "effective_horizon": normalized_solver_config["horizon"],
                "effective_strike_band": selected_pass["reduced"]["effective_moneyness_band"],
                "included_expirations": list(solver_inclusion.get("included_expirations") or []),
                "excluded_expirations": list(solver_inclusion.get("excluded_expirations") or []),
                "excluded_expiration_reasons": dict(
                    solver_inclusion.get("excluded_expiration_reasons") or {}
                ),
            }
        )
        solver["diagnostics"] = solver_diag
        total_gamma_at_spot = full_total_gamma_at_spot
    else:
        _solver_steps = zero_gamma_steps
        _refinement_steps = ZERO_GAMMA_REFINEMENT_STEPS
        if selected_scope == "all":
            _profiles, _solver_steps, _refinement_steps = gamma_solver_profiles(normalized_solver_config)
        solver = compute_zero_gamma(
            None,
            current_spot,
            prepared_contracts=solver_contracts,
            inclusion=solver_inclusion,
            include_0dte=True,
            selected_expirations=None,
            today_iso=today_iso,
            now_ts=now_ts,
            range_ratio=zero_gamma_range,
            steps=_solver_steps,
            refinement_steps=_refinement_steps,
            include_curve=include_solver_curve,
        )
        total_gamma_at_spot = solver["total_gamma_at_spot"]
        zero_gamma = solver["zero_gamma"]
        solver_diag = dict(solver["diagnostics"] or {})
        solver_diag.setdefault("solver_universe_mode", "full")
        solver_diag.setdefault("page_contract_count", len(contracts))
        solver_diag.setdefault("page_expiries_used", len(inclusion.get("included_expirations") or []))
        solver_diag.setdefault("adaptive_refinement_ran", False)
        solver_diag.setdefault("full_contract_count", len(solver_contracts))
        solver_diag.setdefault("solver_contract_count", len(solver_contracts))
        solver_diag.setdefault("page_net_gex", page_net_gex)
        solver_diag.setdefault("solver_horizon", normalized_solver_config["horizon"])
        solver_diag.setdefault("solver_horizon_days", normalized_solver_config["horizon_days"])
        solver_diag.setdefault("solver_band", normalized_solver_config["band"])
        solver_diag.setdefault("solver_band_value", normalized_solver_config["band_value"])
        solver_diag.setdefault("solver_remove_0dte", bool(normalized_solver_config["remove_0dte"]))
        solver_diag.setdefault("tail_handling", normalized_solver_config["tail_handling"])
        solver_diag.setdefault("refinement_mode", normalized_solver_config["refinement_mode"])
        solver_diag.setdefault("solver_preset", normalized_solver_config["preset"])
        solver_diag.setdefault("solver_profile_label", gamma_solver_profile_label(normalized_solver_config))
        solver_diag.setdefault("contracts_used", len(solver_contracts))
        solver_diag.setdefault("expiries_used", len(solver_inclusion.get("included_expirations") or []))
        solver_diag.setdefault("effective_horizon", normalized_solver_config["horizon"])
        solver_diag.setdefault(
            "effective_strike_band",
            normalized_solver_config["band_value"],
        )
        solver_diag.setdefault("included_expirations", list(solver_inclusion.get("included_expirations") or []))
        solver_diag.setdefault("excluded_expirations", list(solver_inclusion.get("excluded_expirations") or []))
        solver_diag.setdefault(
            "excluded_expiration_reasons",
            dict(solver_inclusion.get("excluded_expiration_reasons") or {}),
        )
        solver["diagnostics"] = solver_diag
    if solver_contracts:
        gamma_regime = classify_gamma_regime(current_spot, zero_gamma, total_gamma_at_spot)
    else:
        gamma_regime = "Gamma Regime Unavailable"
    return {
        "strikes": strikes,
        "gex_calls": gex_calls,
        "gex_puts": gex_puts,
        "gex_net": gex_net,
        "gex_cumulative": gex_cumulative,
        "net_gex": page_net_gex if page_net_gex is not None else 0.0,
        "total_gamma_at_spot": total_gamma_at_spot,
        "raw_signed_gamma": total_gamma_at_spot,
        "zero_gamma": zero_gamma,
        "gamma_regime": gamma_regime,
        "spot_vs_zero_gamma": spot_vs_zero_gamma_label(current_spot, zero_gamma),
        "total_oi": total_oi,
        "contract_count": len(contracts),
        "chart_contract_count": len(chart_contracts),
        "solver_contract_count": len(solver_contracts),
        "solver_config": normalized_solver_config,
        "solver_profile_label": gamma_solver_profile_label(normalized_solver_config),
        "zero_gamma_diagnostics": solver["diagnostics"],
        "net_gex_formula": "sign * gamma(S) * open_interest * contract_multiplier * S",
        "raw_signed_gamma_formula": "sign * gamma(S) * open_interest * contract_multiplier * S^2",
        "included_expirations": list(inclusion["included_expirations"]),
        "excluded_expirations": list(inclusion["excluded_expirations"]),
    }


def compute_net_gex(
    chain: Iterable[dict[str, Any]] | None,
    current_spot: float,
    *,
    expirations: Sequence[str] | None = None,
    include_0dte: bool = True,
    selected_scope: str = "all",
    selected_expiry: str | None = None,
    remove_0dte: bool | None = None,
    today_iso: str | None = None,
    now_ts: float | None = None,
) -> float:
    remove_0dte_flag = bool(remove_0dte) if remove_0dte is not None else (not include_0dte)
    contracts = prepare_gamma_analysis(
        chain,
        selected_scope=selected_scope,
        selected_expiry=selected_expiry,
        selected_expiration_set=expirations,
        remove_0dte=remove_0dte_flag,
        today_iso=today_iso,
        now_ts=now_ts,
    )["contracts"]
    return _total_net_gex_for_contracts(contracts, _safe_float(current_spot, default=0.0))


def scanner_scope_expirations(
    expiries: Sequence[str] | None,
    scope: str,
    *,
    today_iso: str | None = None,
    weekly_horizon_days: int = WEEKLY_HORIZON_DAYS,
    monthly_horizon_days: int = MONTHLY_HORIZON_DAYS,
) -> list[str]:
    today_key = today_iso or datetime.now(timezone.utc).date().isoformat()
    try:
        today_date = datetime.fromisoformat(today_key).date()
    except ValueError:
        today_date = datetime.now(timezone.utc).date()
    normalized = sorted({_date_key(exp) for exp in expiries or [] if _date_key(exp)})
    if scope == "all":
        return normalized
    horizon_days = weekly_horizon_days if scope == "weekly" else monthly_horizon_days
    selected: list[str] = []
    for expiry in normalized:
        try:
            exp_date = datetime.fromisoformat(expiry).date()
        except ValueError:
            continue
        delta_days = (exp_date - today_date).days
        if delta_days < 0:
            continue
        if delta_days <= horizon_days:
            selected.append(expiry)
    return selected


def canonicalize_gex_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    clean = dict(payload or {})
    meta = dict(clean.get("meta") or {})
    series = build_signed_gex_series(
        clean.get("strikes") or [],
        gex_net=clean.get("gex_net"),
        gex_calls=clean.get("gex_calls"),
        gex_puts=clean.get("gex_puts"),
    )
    strikes = [row["strike"] for row in series]
    gex_calls = [row["gex_calls"] for row in series]
    gex_puts = [row["gex_puts"] for row in series]
    gex_net = [row["gex_net"] for row in series]
    gex_cumulative = cumulative_net_gex(gex_net)
    clean["strikes"] = strikes
    clean["gex_calls"] = gex_calls
    clean["gex_puts"] = gex_puts
    clean["gex_net"] = gex_net
    clean["gex_cumulative"] = gex_cumulative
    total_gamma_at_spot = (
        _safe_float(meta.get("total_gamma_at_spot"), default=float("nan"))
        if meta.get("total_gamma_at_spot") is not None
        else sum(gex_net)
    )
    if not math.isfinite(total_gamma_at_spot):
        total_gamma_at_spot = sum(gex_net)
    zero_gamma = meta.get("zero_gamma")
    zero_gamma_value = _safe_float(zero_gamma, default=float("nan")) if zero_gamma is not None else None
    if zero_gamma_value is not None and not math.isfinite(zero_gamma_value):
        zero_gamma_value = None
    meta["net_gex"] = (
        _safe_float(meta.get("net_gex"), default=total_gamma_at_spot)
        if meta.get("net_gex") is not None
        else total_gamma_at_spot
    )
    meta["total_gamma_at_spot"] = total_gamma_at_spot
    meta["raw_signed_gamma"] = total_gamma_at_spot
    meta["zero_gamma"] = zero_gamma_value
    meta["spot_vs_zero_gamma"] = spot_vs_zero_gamma_label(meta.get("spot"), zero_gamma_value)
    meta["gamma_regime"] = classify_gamma_regime(meta.get("spot"), zero_gamma_value, total_gamma_at_spot)
    meta.setdefault("net_gex_formula", "sign * gamma(S) * open_interest * contract_multiplier * S")
    meta.setdefault(
        "raw_signed_gamma_formula",
        "sign * gamma(S) * open_interest * contract_multiplier * S^2",
    )
    diagnostics = dict(meta.get("zero_gamma_diagnostics") or {})
    diagnostics.setdefault("total_gamma_at_spot", total_gamma_at_spot)
    diagnostics.setdefault("has_sign_crossing", zero_gamma_value is not None)
    solver_config = normalize_gamma_solver_config(meta.get("solver_config"))
    meta["solver_config"] = solver_config
    meta["solver_profile_label"] = meta.get("solver_profile_label") or gamma_solver_profile_label(solver_config)
    diagnostics.setdefault("solver_preset", solver_config["preset"])
    diagnostics.setdefault("solver_profile_label", meta["solver_profile_label"])
    diagnostics.setdefault("solver_horizon", solver_config["horizon"])
    diagnostics.setdefault("solver_band", solver_config["band"])
    diagnostics.setdefault("solver_remove_0dte", bool(solver_config["remove_0dte"]))
    diagnostics.setdefault("tail_handling", solver_config["tail_handling"])
    diagnostics.setdefault("refinement_mode", solver_config["refinement_mode"])
    meta["zero_gamma_diagnostics"] = diagnostics
    if isinstance(clean.get("zero_gamma_curve"), list):
        clean["zero_gamma_curve"] = [
            {
                "spot": _safe_float(point.get("spot"), default=float("nan")),
                "total_gamma": _safe_float(point.get("total_gamma"), default=0.0),
                "sign": int(point.get("sign", 0)),
            }
            for point in clean["zero_gamma_curve"]
            if isinstance(point, dict) and math.isfinite(_safe_float(point.get("spot"), default=float("nan")))
        ]
    for legacy_key in ("gex_flip", "gex_flip_micro", "macro_flip", "micro_flip"):
        meta.pop(legacy_key, None)
        clean.pop(legacy_key, None)
    clean["meta"] = meta
    return clean

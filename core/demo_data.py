"""Small, safe demo payloads used when DEMO_MODE=1.

These are meant for click-around demos without any API keys. Values are
synthetic but shaped like real responses so the UI renders normally.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
from typing import Dict

from core.gamma_math import classify_gamma_regime, spot_vs_zero_gamma_label, spot_vs_zero_gamma_pct


def _now_str() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _next_weekday(start: date, weekday: int) -> date:
    delta = (weekday - start.weekday()) % 7
    return start + timedelta(days=delta or 7)


def _third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    first_friday = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_friday + timedelta(days=14)


def demo_expiries(symbol: str | None = None) -> list[str]:
    """Return a stable set of upcoming demo expirations for demo mode pages."""
    today = datetime.now(UTC).date()
    first = _next_weekday(today, 4)  # Friday
    second = first + timedelta(days=7)
    third = first + timedelta(days=14)
    monthly = _third_friday(today.year, today.month)
    if monthly <= second:
        year = today.year + (1 if today.month == 12 else 0)
        month = 1 if today.month == 12 else today.month + 1
        monthly = _third_friday(year, month)
    expiries = sorted({first.isoformat(), second.isoformat(), third.isoformat(), monthly.isoformat()})
    return expiries


def _default_demo_expiry() -> str:
    expiries = demo_expiries()
    return expiries[0]


# Demo scanner rows keyed by symbol
DEMO_SCANNER_ROWS: Dict[str, dict] = {
    "AAPL": {
        "symbol": "AAPL",
        "spot": 192.34,
        "day_change_pct": 0.85,
        "ah_change_pct": -0.12,
        "net_gex": 18400000.0,
        "zero_gamma": 188.5,
        "message": "Demo data",
    },
    "MSFT": {
        "symbol": "MSFT",
        "spot": 412.18,
        "day_change_pct": -0.35,
        "ah_change_pct": 0.09,
        "net_gex": -9200000.0,
        "zero_gamma": 418.0,
        "message": "Demo data",
    },
    "SPY": {
        "symbol": "SPY",
        "spot": 505.40,
        "day_change_pct": 0.12,
        "ah_change_pct": 0.00,
        "net_gex": 126500000.0,
        "zero_gamma": 498.25,
        "message": "Demo data",
    },
    "QQQ": {
        "symbol": "QQQ",
        "spot": 435.25,
        "day_change_pct": 0.22,
        "ah_change_pct": -0.05,
        "net_gex": 31400000.0,
        "zero_gamma": 430.8,
        "message": "Demo data",
    },
    "NVDA": {
        "symbol": "NVDA",
        "spot": 138.10,
        "day_change_pct": 1.15,
        "ah_change_pct": 0.18,
        "net_gex": 58200000.0,
        "zero_gamma": 134.4,
        "message": "Demo data",
    },
}


def demo_scanner_row(
    symbol: str,
    pct_window: float | None = None,
    *,
    scope: str = "all",
    include_0dte: bool = True,
) -> dict:
    """Return a copy of a demo scanner row."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_SCANNER_ROWS.get(sym) or DEMO_SCANNER_ROWS["AAPL"]
    row = deepcopy(base)
    row["symbol"] = sym
    scope_key = (scope or "all").lower()
    scope_scale = {"weekly": 0.6, "monthly": 0.85, "all": 1.0}.get(scope_key, 1.0)
    zero_shift = {"weekly": -0.75, "monthly": -0.25, "all": 0.0}.get(scope_key, 0.0)
    row["scope"] = scope_key
    row["include_0dte"] = bool(include_0dte)
    row["net_gex"] = float(row.get("net_gex") or 0.0) * scope_scale
    row["total_gamma_at_spot"] = row["net_gex"] * float(row.get("spot") or 1.0)
    row["zero_gamma"] = float(row.get("zero_gamma") or row.get("spot") or 0.0) + zero_shift
    if not include_0dte:
        row["net_gex"] *= 0.92
        row["total_gamma_at_spot"] = row["net_gex"] * float(row.get("spot") or 1.0)
        row["zero_gamma"] += 0.35
    row["spot_vs_zero_gamma_pct"] = spot_vs_zero_gamma_pct(row.get("spot"), row.get("zero_gamma"))
    row["spot_vs_zero_gamma"] = spot_vs_zero_gamma_label(row.get("spot"), row.get("zero_gamma"))
    row["gamma_regime"] = classify_gamma_regime(
        row.get("spot"), row.get("zero_gamma"), row.get("total_gamma_at_spot")
    )
    row["as_of"] = _now_str()
    row["demo"] = True
    return row


DEMO_GEX_RESULTS: Dict[str, dict] = {
    "AAPL": {
        "strikes": [185, 190, 195, 200, 205, 210, 215, 220, 225, 230],
        "gex_net": [-2.3, -1.8, -0.9, 0.2, 1.4, 2.0, 1.6, 1.0, 0.4, -0.1],
        "gex_calls": [0.5, 0.9, 1.2, 1.5, 2.0, 2.1, 2.0, 1.8, 1.5, 1.2],
        "gex_puts": [-2.8, -2.7, -2.1, -1.3, -0.6, -0.1, -0.4, -0.8, -1.1, -1.3],
        "meta": {
            "expiry": "",
            "spot": 192.34,
            "zero_gamma": 188.5,
            "net_gex": -1.3,
            "total_gamma_at_spot": -250.04,
            "gamma_regime": "Short Gamma",
            "prev_close": 190.10,
        },
    },
    "MSFT": {
        "strikes": [390, 395, 400, 405, 410, 415, 420, 425, 430],
        "gex_net": [-1.1, -0.6, 0.1, 0.8, 1.5, 1.8, 1.4, 0.7, -0.2],
        "gex_calls": [0.7, 1.0, 1.4, 1.8, 2.1, 2.0, 1.8, 1.5, 1.0],
        "gex_puts": [-1.8, -1.6, -1.3, -1.0, -0.6, -0.2, -0.4, -0.8, -1.2],
        "meta": {
            "expiry": "",
            "spot": 412.18,
            "zero_gamma": 418.0,
            "net_gex": 6.6,
            "total_gamma_at_spot": 2720.39,
            "gamma_regime": "Long Gamma",
            "prev_close": 413.00,
        },
    },
    "SPY": {
        "strikes": [490, 495, 500, 505, 510, 515, 520],
        "gex_net": [-0.9, -0.4, 0.2, 0.9, 1.2, 0.8, 0.1],
        "gex_calls": [0.6, 0.9, 1.2, 1.5, 1.6, 1.3, 0.9],
        "gex_puts": [-1.5, -1.3, -1.0, -0.6, -0.4, -0.5, -0.8],
        "meta": {
            "expiry": "",
            "spot": 505.40,
            "zero_gamma": 498.25,
            "net_gex": 1.9,
            "total_gamma_at_spot": 960.26,
            "gamma_regime": "Long Gamma",
            "prev_close": 505.00,
        },
    },
    "QQQ": {
        "strikes": [420, 425, 430, 435, 440, 445, 450],
        "gex_net": [-0.8, -0.3, 0.1, 0.7, 1.0, 0.6, 0.0],
        "gex_calls": [0.5, 0.8, 1.1, 1.4, 1.5, 1.2, 0.9],
        "gex_puts": [-1.3, -1.1, -1.0, -0.7, -0.5, -0.6, -0.9],
        "meta": {
            "expiry": "",
            "spot": 435.25,
            "zero_gamma": 430.8,
            "net_gex": 1.3,
            "total_gamma_at_spot": 565.83,
            "gamma_regime": "Long Gamma",
            "prev_close": 434.80,
        },
    },
    "NVDA": {
        "strikes": [120, 125, 130, 135, 140, 145, 150],
        "gex_net": [-1.6, -1.0, -0.3, 0.5, 1.2, 1.5, 1.1],
        "gex_calls": [0.8, 1.1, 1.5, 1.9, 2.1, 2.0, 1.7],
        "gex_puts": [-2.4, -2.1, -1.8, -1.4, -0.9, -0.5, -0.6],
        "meta": {
            "expiry": "",
            "spot": 138.10,
            "zero_gamma": 134.4,
            "net_gex": 1.4,
            "total_gamma_at_spot": 193.34,
            "gamma_regime": "Long Gamma",
            "prev_close": 137.40,
        },
    },
}


def demo_gex_result(
    symbol: str,
    pct_window: float | None = None,
    *,
    expiry: str | None = None,
    expiry_mode: str = "selected",
    include_0dte: bool = True,
) -> dict:
    """Return a demo GEX payload matching the normal schema."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_GEX_RESULTS.get(sym) or DEMO_GEX_RESULTS["AAPL"]
    res = deepcopy(base)
    res["meta"] = res.get("meta", {})
    res["meta"]["symbol"] = sym
    res["meta"]["expiry"] = expiry if expiry_mode != "all" and expiry else _default_demo_expiry()
    res["meta"]["expiry_mode"] = expiry_mode
    res["meta"]["include_0dte"] = bool(include_0dte)
    res["meta"]["net_gex"] = sum(float(v) for v in res.get("gex_net") or [])
    res["meta"]["as_of"] = _now_str()
    res["meta"]["window_pct"] = pct_window * 100 if pct_window is not None else None
    res["meta"]["demo"] = True
    return res


DEMO_VANNA_RESULTS: Dict[str, dict] = {
    "AAPL": {
        "strikes": [180, 185, 190, 195, 200, 205, 210],
        "vanna_net": [-1.2, -0.6, 0.1, 0.9, 1.5, 1.0, 0.3],
        "vanna_calls": [0.4, 0.7, 1.1, 1.4, 1.6, 1.2, 0.8],
        "vanna_puts": [-1.6, -1.3, -1.0, -0.5, -0.1, -0.2, -0.5],
        "meta": {"expiry": "", "spot": 192.34},
    },
    "MSFT": {
        "strikes": [390, 395, 400, 405, 410, 415],
        "vanna_net": [-0.8, -0.2, 0.5, 1.0, 0.9, 0.4],
        "vanna_calls": [0.5, 0.9, 1.2, 1.5, 1.4, 1.0],
        "vanna_puts": [-1.3, -1.1, -0.7, -0.5, -0.5, -0.6],
        "meta": {"expiry": "", "spot": 412.18},
    },
    "SPY": {
        "strikes": [490, 495, 500, 505, 510, 515],
        "vanna_net": [-0.7, -0.3, 0.2, 0.7, 0.9, 0.5],
        "vanna_calls": [0.6, 0.9, 1.1, 1.4, 1.5, 1.2],
        "vanna_puts": [-1.3, -1.2, -0.9, -0.7, -0.6, -0.7],
        "meta": {"expiry": "", "spot": 505.40},
    },
    "QQQ": {
        "strikes": [420, 425, 430, 435, 440, 445],
        "vanna_net": [-0.9, -0.4, 0.1, 0.6, 0.8, 0.3],
        "vanna_calls": [0.5, 0.8, 1.0, 1.3, 1.4, 1.1],
        "vanna_puts": [-1.4, -1.2, -0.9, -0.7, -0.6, -0.8],
        "meta": {"expiry": "", "spot": 435.25},
    },
    "NVDA": {
        "strikes": [120, 125, 130, 135, 140, 145],
        "vanna_net": [-1.4, -0.9, -0.2, 0.6, 1.1, 0.8],
        "vanna_calls": [0.7, 1.0, 1.4, 1.8, 2.0, 1.6],
        "vanna_puts": [-2.1, -1.9, -1.6, -1.2, -0.9, -0.8],
        "meta": {"expiry": "", "spot": 138.10},
    },
}


def demo_vanna_result(symbol: str, pct_window: float | None = None) -> dict:
    """Return a demo Vanna payload matching the normal schema."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_VANNA_RESULTS.get(sym) or DEMO_VANNA_RESULTS["AAPL"]
    res = deepcopy(base)
    res["meta"] = res.get("meta", {})
    res["meta"]["symbol"] = sym
    res["meta"]["expiry"] = _default_demo_expiry()
    res["meta"]["as_of"] = _now_str()
    res["meta"]["window_pct"] = pct_window * 100 if pct_window is not None else None
    res["meta"]["demo"] = True
    return res

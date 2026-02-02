"""Small, safe demo payloads used when DEMO_MODE=1.

These are meant for click-around demos without any API keys. Values are
synthetic but shaped like real responses so the UI renders normally.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Dict, List


def _now_str() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")


# Demo scanner rows keyed by symbol
DEMO_SCANNER_ROWS: Dict[str, dict] = {
    "AAPL": {
        "symbol": "AAPL",
        "spot": 192.34,
        "day_change_pct": 0.85,
        "ah_change_pct": -0.12,
        "exp_move": 4.50,
        "exp_move_pct": 2.34,
        "exp_move_expiry": "2025-01-17",
        "weekly_expiry": "2025-01-17",
        "monthly_expiry": "2025-02-21",
        "next_expiry": "2025-01-17",
        "flip_weekly": 198.0,
        "flip_monthly": 205.0,
        "flip_total": 201.0,
        "distance_weekly_pct": 2.95,
        "distance_monthly_pct": 6.58,
        "distance_total_pct": 4.49,
        "top_gex_strike": 200.0,
        "top_gex_pct": 4.0,
        "gex_trend_pct": 7.8,
        "oi_trend": [1180, 1210, 1235, 1260, 1275],
        "message": "Demo data",
    },
    "MSFT": {
        "symbol": "MSFT",
        "spot": 412.18,
        "day_change_pct": -0.35,
        "ah_change_pct": 0.09,
        "exp_move": 6.20,
        "exp_move_pct": 1.50,
        "exp_move_expiry": "2025-01-17",
        "weekly_expiry": "2025-01-17",
        "monthly_expiry": "2025-02-21",
        "next_expiry": "2025-01-17",
        "flip_weekly": 405.0,
        "flip_monthly": 418.0,
        "flip_total": 410.0,
        "distance_weekly_pct": -1.74,
        "distance_monthly_pct": 1.41,
        "distance_total_pct": -0.53,
        "top_gex_strike": 420.0,
        "top_gex_pct": 1.90,
        "gex_trend_pct": -3.4,
        "oi_trend": [980, 990, 1010, 995, 1005],
        "message": "Demo data",
    },
    "SPY": {
        "symbol": "SPY",
        "spot": 505.40,
        "day_change_pct": 0.12,
        "ah_change_pct": 0.00,
        "exp_move": 5.80,
        "exp_move_pct": 1.15,
        "exp_move_expiry": "2025-01-17",
        "weekly_expiry": "2025-01-17",
        "monthly_expiry": "2025-02-21",
        "next_expiry": "2025-01-17",
        "flip_weekly": 500.0,
        "flip_monthly": 512.0,
        "flip_total": 507.0,
        "distance_weekly_pct": -1.07,
        "distance_monthly_pct": 1.31,
        "distance_total_pct": 0.32,
        "top_gex_strike": 510.0,
        "top_gex_pct": 0.91,
        "gex_trend_pct": 2.1,
        "oi_trend": [1500, 1525, 1540, 1535, 1550],
        "message": "Demo data",
    },
    "QQQ": {
        "symbol": "QQQ",
        "spot": 435.25,
        "day_change_pct": 0.22,
        "ah_change_pct": -0.05,
        "exp_move": 4.30,
        "exp_move_pct": 0.99,
        "exp_move_expiry": "2025-01-17",
        "weekly_expiry": "2025-01-17",
        "monthly_expiry": "2025-02-21",
        "next_expiry": "2025-01-17",
        "flip_weekly": 430.0,
        "flip_monthly": 442.0,
        "flip_total": 437.0,
        "distance_weekly_pct": -1.21,
        "distance_monthly_pct": 1.55,
        "distance_total_pct": 0.40,
        "top_gex_strike": 440.0,
        "top_gex_pct": 1.09,
        "gex_trend_pct": 3.6,
        "oi_trend": [1300, 1315, 1330, 1320, 1335],
        "message": "Demo data",
    },
    "NVDA": {
        "symbol": "NVDA",
        "spot": 138.10,
        "day_change_pct": 1.15,
        "ah_change_pct": 0.18,
        "exp_move": 6.90,
        "exp_move_pct": 5.00,
        "exp_move_expiry": "2025-01-17",
        "weekly_expiry": "2025-01-17",
        "monthly_expiry": "2025-02-21",
        "next_expiry": "2025-01-17",
        "flip_weekly": 142.0,
        "flip_monthly": 148.0,
        "flip_total": 145.0,
        "flip_weekly_micro": 141.5,
        "flip_monthly_micro": 147.5,
        "flip_total_micro": 144.5,
        "distance_weekly_pct": 2.82,
        "distance_monthly_pct": 7.18,
        "distance_total_pct": 5.00,
        "distance_weekly_micro_pct": 3.10,
        "distance_monthly_micro_pct": 7.50,
        "distance_total_micro_pct": 5.30,
        "top_gex_strike": 150.0,
        "top_gex_pct": 8.61,
        "gex_trend_pct": 12.4,
        "oi_trend": [2100, 2150, 2200, 2250, 2300],
        "message": "Demo data",
    },
}


def demo_scanner_row(symbol: str, pct_window: float | None = None) -> dict:
    """Return a copy of a demo scanner row."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_SCANNER_ROWS.get(sym) or DEMO_SCANNER_ROWS["AAPL"]
    row = deepcopy(base)
    row["symbol"] = sym
    # Recompute score based on distances (max absolute distance)
    dists: List[float] = []
    for key in ("distance_total_pct", "distance_weekly_pct", "distance_monthly_pct"):
        try:
            v = float(row.get(key))
            dists.append(abs(v))
        except Exception:
            continue
    row["score"] = max(dists) if dists else None
    micro_dists: List[float] = []
    for key in (
        "distance_total_micro_pct",
        "distance_weekly_micro_pct",
        "distance_monthly_micro_pct",
    ):
        try:
            v = float(row.get(key))
            micro_dists.append(abs(v))
        except Exception:
            continue
    row["score_micro"] = max(micro_dists) if micro_dists else row.get("score")
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
            "expiry": "2025-01-17",
            "spot": 192.34,
            "gex_flip": 198.0,
            "prev_close": 190.10,
        },
    },
    "MSFT": {
        "strikes": [390, 395, 400, 405, 410, 415, 420, 425, 430],
        "gex_net": [-1.1, -0.6, 0.1, 0.8, 1.5, 1.8, 1.4, 0.7, -0.2],
        "gex_calls": [0.7, 1.0, 1.4, 1.8, 2.1, 2.0, 1.8, 1.5, 1.0],
        "gex_puts": [-1.8, -1.6, -1.3, -1.0, -0.6, -0.2, -0.4, -0.8, -1.2],
        "meta": {
            "expiry": "2025-01-17",
            "spot": 412.18,
            "gex_flip": 404.0,
            "prev_close": 413.00,
        },
    },
    "SPY": {
        "strikes": [490, 495, 500, 505, 510, 515, 520],
        "gex_net": [-0.9, -0.4, 0.2, 0.9, 1.2, 0.8, 0.1],
        "gex_calls": [0.6, 0.9, 1.2, 1.5, 1.6, 1.3, 0.9],
        "gex_puts": [-1.5, -1.3, -1.0, -0.6, -0.4, -0.5, -0.8],
        "meta": {
            "expiry": "2025-01-17",
            "spot": 505.40,
            "gex_flip": 501.0,
            "prev_close": 505.00,
        },
    },
    "QQQ": {
        "strikes": [420, 425, 430, 435, 440, 445, 450],
        "gex_net": [-0.8, -0.3, 0.1, 0.7, 1.0, 0.6, 0.0],
        "gex_calls": [0.5, 0.8, 1.1, 1.4, 1.5, 1.2, 0.9],
        "gex_puts": [-1.3, -1.1, -1.0, -0.7, -0.5, -0.6, -0.9],
        "meta": {
            "expiry": "2025-01-17",
            "spot": 435.25,
            "gex_flip": 433.0,
            "prev_close": 434.80,
        },
    },
    "NVDA": {
        "strikes": [120, 125, 130, 135, 140, 145, 150],
        "gex_net": [-1.6, -1.0, -0.3, 0.5, 1.2, 1.5, 1.1],
        "gex_calls": [0.8, 1.1, 1.5, 1.9, 2.1, 2.0, 1.7],
        "gex_puts": [-2.4, -2.1, -1.8, -1.4, -0.9, -0.5, -0.6],
        "meta": {
            "expiry": "2025-01-17",
            "spot": 138.10,
            "gex_flip": 141.5,
            "prev_close": 137.40,
        },
    },
}


def demo_gex_result(symbol: str, pct_window: float | None = None) -> dict:
    """Return a demo GEX payload matching the normal schema."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_GEX_RESULTS.get(sym) or DEMO_GEX_RESULTS["AAPL"]
    res = deepcopy(base)
    res["meta"] = res.get("meta", {})
    res["meta"]["symbol"] = sym
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
        "meta": {"expiry": "2025-01-17", "spot": 192.34},
    },
    "MSFT": {
        "strikes": [390, 395, 400, 405, 410, 415],
        "vanna_net": [-0.8, -0.2, 0.5, 1.0, 0.9, 0.4],
        "vanna_calls": [0.5, 0.9, 1.2, 1.5, 1.4, 1.0],
        "vanna_puts": [-1.3, -1.1, -0.7, -0.5, -0.5, -0.6],
        "meta": {"expiry": "2025-01-17", "spot": 412.18},
    },
    "SPY": {
        "strikes": [490, 495, 500, 505, 510, 515],
        "vanna_net": [-0.7, -0.3, 0.2, 0.7, 0.9, 0.5],
        "vanna_calls": [0.6, 0.9, 1.1, 1.4, 1.5, 1.2],
        "vanna_puts": [-1.3, -1.2, -0.9, -0.7, -0.6, -0.7],
        "meta": {"expiry": "2025-01-17", "spot": 505.40},
    },
    "QQQ": {
        "strikes": [420, 425, 430, 435, 440, 445],
        "vanna_net": [-0.9, -0.4, 0.1, 0.6, 0.8, 0.3],
        "vanna_calls": [0.5, 0.8, 1.0, 1.3, 1.4, 1.1],
        "vanna_puts": [-1.4, -1.2, -0.9, -0.7, -0.6, -0.8],
        "meta": {"expiry": "2025-01-17", "spot": 435.25},
    },
    "NVDA": {
        "strikes": [120, 125, 130, 135, 140, 145],
        "vanna_net": [-1.4, -0.9, -0.2, 0.6, 1.1, 0.8],
        "vanna_calls": [0.7, 1.0, 1.4, 1.8, 2.0, 1.6],
        "vanna_puts": [-2.1, -1.9, -1.6, -1.2, -0.9, -0.8],
        "meta": {"expiry": "2025-01-17", "spot": 138.10},
    },
}


def demo_vanna_result(symbol: str, pct_window: float | None = None) -> dict:
    """Return a demo Vanna payload matching the normal schema."""
    sym = (symbol or "").upper().strip() or "AAPL"
    base = DEMO_VANNA_RESULTS.get(sym) or DEMO_VANNA_RESULTS["AAPL"]
    res = deepcopy(base)
    res["meta"] = res.get("meta", {})
    res["meta"]["symbol"] = sym
    res["meta"]["as_of"] = _now_str()
    res["meta"]["window_pct"] = pct_window * 100 if pct_window is not None else None
    res["meta"]["demo"] = True
    return res

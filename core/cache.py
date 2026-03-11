from __future__ import annotations

import json
import os
import time
from typing import Optional


def cache_key(
    *,
    mode: str,
    symbol: str,
    pct_window: float,
    next_only: bool,
    expiry: Optional[str],
    weight: Optional[str] = None,
    spot_override: Optional[str] = None,
    expiry_mode: Optional[str] = None,
    include_0dte: Optional[bool] = None,
    expiry_filter: Optional[str] = None,
    solver_profile: Optional[str] = None,
    calc_version: Optional[str] = None,
) -> str:
    """Stable cache key for server-side cache + dedupe."""
    sy = (symbol or "").upper().strip()
    ex = (expiry or "").strip()
    w = (weight or "").strip()
    so = (spot_override or "auto").strip()
    em = (expiry_mode or "").strip()
    i0 = "" if include_0dte is None else ("1" if include_0dte else "0")
    ef = (expiry_filter or "").strip()
    sp = (solver_profile or "").strip()
    cv = (calc_version or "").strip()
    try:
        pw = f"{float(pct_window):.6f}"
    except Exception:
        pw = str(pct_window)
    parts = [mode, sy, pw, "1" if next_only else "0", ex, w, so, em, i0, ef, sp, cv]
    return ":".join(parts)


def _disk_cache_path(key: str) -> str:
    fn = key.replace("/", "_").replace(":", "_")
    base = os.path.join("cache", "gex_disk")
    return os.path.join(base, f"{fn}.json")


def disk_cache_get(key: str, ttl_sec: int) -> Optional[dict]:
    path = _disk_cache_path(key)
    try:
        if not os.path.exists(path):
            return None
        age = time.time() - os.path.getmtime(path)
        if ttl_sec > 0 and age > ttl_sec:
            return None
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def disk_cache_set(key: str, payload: dict):
    path = _disk_cache_path(key)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    except Exception:
        pass


def _oi_history_path(symbol: str) -> str:
    return os.path.join("cache", "gex_history", f"{symbol.upper()}.json")


def load_oi_history(symbol: str) -> list[dict]:
    path = _oi_history_path(symbol)
    try:
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
            return raw if isinstance(raw, list) else []
    except Exception:
        return []


def save_oi_history(symbol: str, entries: list[dict]):
    path = _oi_history_path(symbol)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(entries, fh)
    except Exception:
        pass


def upsert_oi_history(
    symbol: str, oi_val: float | None, max_days: int = 7, max_points: int = 50
) -> list[dict]:
    """Store OI history for trend; keep last `max_days` days (approx) and limit length."""
    if oi_val is None:
        return load_oi_history(symbol)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
    now_ts = time.time()
    entries = load_oi_history(symbol)
    entries.append({"ts": now_iso, "oi": float(oi_val)})
    pruned = []
    cutoff = now_ts - max_days * 86400
    for e in entries:
        try:
            from datetime import datetime

            t = datetime.fromisoformat(str(e.get("ts")).replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if t >= cutoff:
            pruned.append({"ts": e.get("ts"), "oi": float(e.get("oi"))})
    pruned = pruned[-max_points:]
    try:
        save_oi_history(symbol, pruned)
    except Exception:
        pass
    return pruned
# --- GEX trend history (growth above highest call strike) ---
def _gex_trend_path(symbol: str) -> str:
    return os.path.join("cache", "gex_trend", f"{symbol.upper()}.json")


def load_gex_trend_history(symbol: str) -> list[dict]:
    path = _gex_trend_path(symbol)
    try:
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
            return raw if isinstance(raw, list) else []
    except Exception:
        return []


def save_gex_trend_history(symbol: str, entries: list[dict]):
    path = _gex_trend_path(symbol)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(entries, fh)
    except Exception:
        pass


def upsert_gex_trend_history(
    symbol: str, value: float | None, max_days: int = 7, max_points: int = 80
) -> list[dict]:
    """Persist trend metric; keep recent window similar to OI history."""
    if value is None:
        return load_gex_trend_history(symbol)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
    now_ts = time.time()
    entries = load_gex_trend_history(symbol)
    entries.append({"ts": now_iso, "value": float(value)})
    pruned = []
    cutoff = now_ts - max_days * 86400
    for e in entries:
        try:
            from datetime import datetime

            t = datetime.fromisoformat(str(e.get("ts")).replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
        if t >= cutoff and e.get("value") is not None:
            try:
                pruned.append({"ts": e.get("ts"), "value": float(e.get("value"))})
            except Exception:
                continue
    pruned = pruned[-max_points:]
    try:
        save_gex_trend_history(symbol, pruned)
    except Exception:
        pass
    return pruned

from __future__ import annotations

import asyncio
import math
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import httpx
import numpy as np
import pandas as pd

from core.cache import cache_key, disk_cache_get, disk_cache_set
from core.gamma_math import build_gamma_profile, infer_implied_volatility_from_price

# ------------------------------
# Config
# ------------------------------
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY", "")
# Allow overriding the API base for the Massive (formerly Polygon) endpoints.
MASSIVE_API_BASE = os.getenv("MASSIVE_API_BASE", "https://api.polygon.io")
CONTRACT_MULTIPLIER = 100
ALLOW_UNWEIGHTED_FALLBACK = os.getenv("ALLOW_UNWEIGHTED_FALLBACK", "0") == "1"
GAMMA_CHAIN_CACHE_VERSION = "gamma-chain-v1"


# ------------------------------
# Job manager with cancel
# ------------------------------
@dataclass
class Job:
    job_id: str
    session_id: str
    started_at: float = field(default_factory=time.time)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    status: str = "idle"
    progress: float = 0.0
    logs: List[str] = field(default_factory=list)
    result: Optional[dict] = None
    cache_metrics: Dict[str, int] = field(default_factory=dict)

    def log(self, msg: str):
        self.logs.append(msg)
        if len(self.logs) > 250:
            self.logs = self.logs[-250:]


class JobManager:
    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._by_session: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        # Map cache/run keys to running job ids for deduplication
        self._by_key: Dict[str, str] = {}
        # Simple in-memory result cache: key -> {"ts": epoch_seconds, "result": dict}
        self._cache: Dict[str, dict] = {}

    async def create(self, session_id: str, *, cancel_previous: bool = False) -> Job:
        """Create a new job for a session.

        By default, this no longer cancels any existing job for the same
        session. This allows multiple independent jobs (e.g., Vanna and GEX,
        or multiple tickers) to run concurrently within the same session.

        If callers want the previous session job cancelled (rare; typically
        only when replacing a like-for-like request within the same view),
        pass cancel_previous=True.
        """
        async with self._lock:
            if cancel_previous and session_id in self._by_session:
                jid = self._by_session.get(session_id)
                prev = self._jobs.get(jid) if jid else None
                if prev and prev.status in ("queued", "running"):
                    prev.cancel_event.set()
                    prev.status = "cancelling"
                    prev.log("Cancelled due to replacement in same session.")
            job_id = uuid.uuid4().hex
            job = Job(job_id=job_id, session_id=session_id)
            self._jobs[job_id] = job
            # Keep track of the most recent job for the session (non-exclusive)
            self._by_session[session_id] = job_id
            return job

    async def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    async def get_by_session(self, session_id: str) -> Optional[Job]:
        jid = self._by_session.get(session_id)
        return self._jobs.get(jid) if jid else None

    async def cancel(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if not job:
            return False
        job.cancel_event.set()
        job.status = "cancelling"
        job.log("Cancellation requested by user.")
        return True

    # --- Deduplication helpers ---
    async def get_running_by_key(self, key: str) -> Optional[Job]:
        jid = self._by_key.get(key)
        if not jid:
            return None
        job = self._jobs.get(jid)
        if not job:
            return None
        if job.status in ("queued", "running"):
            return job
        return None

    async def register_running_key(self, key: str, job: Job):
        async with self._lock:
            self._by_key[key] = job.job_id

    async def clear_running_key(self, key: str, job: Job | None = None):
        async with self._lock:
            if key in self._by_key:
                if not job or self._by_key.get(key) == job.job_id:
                    self._by_key.pop(key, None)

    # --- Server-side cache helpers ---
    def cache_get(self, key: str, ttl_sec: int = 300) -> Optional[dict]:
        try:
            ent = self._cache.get(key)
            if not ent:
                return None
            import time as _t

            if (_t.time() - float(ent.get("ts", 0))) > float(ttl_sec):
                return None
            return ent.get("result")
        except Exception:
            return None

    def cache_set(self, key: str, result: dict):
        try:
            import time as _t

            self._cache[key] = {"ts": _t.time(), "result": result}
        except Exception:
            pass


job_manager = JobManager()


# ------------------------------
# Unified event log (in-memory, ring)
# ------------------------------
class EventLog:
    def __init__(self, capacity: int = 5000):
        self._cap = max(100, capacity)
        self._items: List[dict] = []
        self._lock = asyncio.Lock()
        self._enabled: bool = True

    async def add(
        self,
        kind: str,
        route: str,
        message: str = "",
        meta: Optional[dict] = None,
        status: str = "info",
    ):
        from datetime import datetime, timezone

        if not self._enabled:
            return
        it = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
            "route": route,
            "message": message or "",
            "meta": meta or {},
            "status": (status or "info"),
        }
        async with self._lock:
            self._items.append(it)
            if len(self._items) > self._cap:
                self._items = self._items[-self._cap :]

    async def get(self, limit: int = 500) -> List[dict]:
        async with self._lock:
            if limit <= 0:
                return list(self._items)
            return self._items[-limit:]

    async def clear(self):
        async with self._lock:
            self._items.clear()

    async def set_enabled(self, enabled: bool):
        async with self._lock:
            self._enabled = bool(enabled)

    async def get_enabled(self) -> bool:
        async with self._lock:
            return self._enabled


event_log = EventLog()


# ------------------------------
# Math — Vanna
# ------------------------------
SQRT_2PI = math.sqrt(2 * math.pi)


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / SQRT_2PI


def _norm_cdf(x: float) -> float:
    k = 1.0 / (1.0 + 0.2316419 * abs(x))
    a1, a2, a3, a4, a5 = (
        0.319381530,
        -0.356563782,
        1.781477937,
        -1.821255978,
        1.330274429,
    )
    poly = ((((a5 * k + a4) * k + a3) * k + a2) * k + a1) * k
    nd = 1.0 - _norm_pdf(x) * poly
    return nd if x >= 0 else 1.0 - nd


def bs_d1_d2(S: float, K: float, T: float, r: float, q: float, sigma: float):
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0, 0.0
    vsqrtT = sigma * math.sqrt(T)
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / vsqrtT
    return d1, d1 - vsqrtT


def bs_vanna(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0
    d1, d2 = bs_d1_d2(S, K, T, r, q, sigma)
    return math.exp(-q * T) * _norm_pdf(d1) * (-d2 / max(sigma, 1e-8))


def bs_gamma(S: float, K: float, T: float, r: float, q: float, sigma: float) -> float:
    if S <= 0 or K <= 0 or sigma <= 0 or T <= 0:
        return 0.0
    d1, _ = bs_d1_d2(S, K, T, r, q, sigma)
    return math.exp(-q * T) * _norm_pdf(d1) / (S * sigma * math.sqrt(T))


# ------------------------------
# Polygon helpers
# ------------------------------
BASE = MASSIVE_API_BASE


async def polygon_get(client: httpx.AsyncClient, path: str, params: dict) -> dict:
    if not MASSIVE_API_KEY:
        return {"status": "error", "message": "Missing MASSIVE_API_KEY (or POLYGON_API_KEY)"}
    p = dict(params or {})
    p["apiKey"] = MASSIVE_API_KEY
    try:
        r = await client.get(f"{BASE}{path}", params=p, timeout=30)
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 429:
                # Bubble up a clearer signal to callers
                ra = e.response.headers.get("Retry-After") if hasattr(e, "response") else None
                raise RuntimeError(f"Massive (Polygon) rate limited (429). Retry-After={ra}") from e
            raise
        return r.json()
    except httpx.HTTPError as e:
        # Uniform error payload for network issues
        return {"status": "error", "message": str(e)}


def _extract_next_cursor(payload: dict | None) -> str | None:
    data = payload or {}
    next_cursor = data.get("next_url_cursor") or data.get("nextCursor")
    if next_cursor:
        return str(next_cursor)
    next_url = data.get("next_url")
    if not isinstance(next_url, str) or not next_url:
        return None
    try:
        from urllib.parse import parse_qs, urlparse

        values = parse_qs(urlparse(next_url).query).get("cursor") or []
        return str(values[0]) if values else None
    except Exception:
        return None


def _aggregate_gamma_chain_cache_key(symbol: str, spot: float | None = None) -> str:
    spot_key = None
    if spot is not None:
        try:
            spot_key = f"{float(spot):.2f}"
        except Exception:
            spot_key = str(spot)
    return cache_key(
        mode="gchain",
        symbol=symbol,
        pct_window=0.0,
        next_only=False,
        expiry="all",
        weight="",
        spot_override=spot_key,
        expiry_mode="all",
        include_0dte=True,
        expiry_filter="",
        calc_version=GAMMA_CHAIN_CACHE_VERSION,
    )


def _coalesce_numeric(snapshot: dict, paths: list[str]) -> float | None:
    for path in paths:
        cur: Any = snapshot
        try:
            for key in path.split("."):
                if isinstance(cur, dict) and key in cur:
                    cur = cur[key]
                else:
                    cur = None
                    break
            if cur is None:
                continue
            return float(cur)
        except Exception:
            continue
    return None


def _normalize_snapshot_chain_row(
    snapshot: dict[str, Any],
    *,
    fallback_contract: dict[str, Any] | None = None,
    spot: float | None = None,
) -> dict[str, Any] | None:
    if not isinstance(snapshot, dict):
        return None
    fallback = fallback_contract or {}
    details = snapshot.get("details") if isinstance(snapshot.get("details"), dict) else {}
    ticker = (
        snapshot.get("ticker")
        or details.get("ticker")
        or fallback.get("ticker")
        or fallback.get("symbol")
        or fallback.get("option_ticker")
    )
    strike = (
        _coalesce_numeric(snapshot, ["details.strike_price", "strike_price", "strike"])
        or _coalesce_numeric(fallback, ["strike_price", "strikePrice", "strike"])
    )
    expiry = (
        details.get("expiration_date")
        or snapshot.get("expiration_date")
        or snapshot.get("expirationDate")
        or fallback.get("expiration_date")
        or fallback.get("expirationDate")
        or fallback.get("exp_date")
    )
    option_type = (
        details.get("contract_type")
        or snapshot.get("contract_type")
        or snapshot.get("type")
        or fallback.get("contract_type")
        or fallback.get("type")
    )
    oi = _coalesce_numeric(snapshot, ["open_interest", "openInterest"]) or 0.0
    iv = _coalesce_numeric(
        snapshot,
        ["implied_volatility", "impliedVolatility", "greeks.iv", "greeks.implied_volatility"],
    )
    seconds_to_expiration = _coalesce_numeric(
        snapshot,
        ["timeframe.seconds_to_expiration", "seconds_to_expiration", "secondsToExpiration"],
    )
    t_years = None
    if seconds_to_expiration is not None and seconds_to_expiration > 0:
        t_years = float(seconds_to_expiration) / (365.0 * 24.0 * 3600.0)
    if t_years is None and expiry:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(str(expiry)[:10]).replace(tzinfo=timezone.utc)
            t_years = max((dt.timestamp() - time.time()) / (365.0 * 24.0 * 3600.0), 1.0 / 365.0)
        except Exception:
            t_years = None
    if (iv is None or iv <= 0) and spot is not None and t_years is not None and t_years > 0:
        option_price = _coalesce_numeric(
            snapshot,
            ["day.close", "last_trade.price", "last_quote.midpoint"],
        )
        if option_price is not None and option_price > 0:
            inferred_iv = infer_implied_volatility_from_price(
                option_price,
                float(spot),
                float(strike),
                float(t_years),
                str(option_type),
            )
            if inferred_iv is not None and inferred_iv > 0:
                iv = inferred_iv
    if iv is None:
        iv = 0.0
    if not ticker or strike is None or not expiry or not option_type:
        return None
    return {
        "ticker": ticker,
        "strike": float(strike),
        "option_type": option_type,
        "oi": float(oi or 0.0),
        "iv": float(iv or 0.0),
        "expiry": str(expiry)[:10],
        "t_years": float(t_years) if t_years is not None else None,
        "contract_size": CONTRACT_MULTIPLIER,
    }


async def fetch_spot(client: httpx.AsyncClient, symbol: str) -> Optional[float]:
    try:
        data = await polygon_get(
            client,
            f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}",
            {},
        )
        spot = (
            data.get("ticker", {}).get("lastTrade", {}).get("p")
            or data.get("ticker", {}).get("lastQuote", {}).get("P")
            or data.get("last", {}).get("price")
        )
        if spot:
            return float(spot)
    except Exception:
        pass
    try:
        data = await polygon_get(client, f"/v2/aggs/ticker/{symbol.upper()}/prev", {})
        results = data.get("results") or []
        if results:
            c = results[0].get("c")
            if c:
                return float(c)
    except Exception:
        pass
    return None


def fetch_spot_yahoo(symbol: str) -> Optional[float]:
    """Yahoo Finance spot fetch with multiple strategies and symbol mapping.

    - Maps common index/derivative underlyings (e.g., SPX -> ^GSPC) for Yahoo.
    - Tries fast_info.last_price, then 1m/1d history, then .info['regularMarketPrice'].
    Returns None if everything fails or yfinance is unavailable.
    """
    try:
        import yfinance as yf  # type: ignore

        mapping = {
            "SPX": "^GSPC",
            "NDX": "^NDX",
            "RUT": "^RUT",
            "DJI": "^DJI",
        }
        ysym = mapping.get(symbol.upper(), symbol)
        t = yf.Ticker(ysym)

        # 1) fast_info
        price: Optional[float] = None
        try:
            fi: Any = getattr(t, "fast_info", None)
            if fi:
                cand = []
                for key in ("last_price", "lastPrice", "last_trade_price", "lastTradePrice"):
                    v = getattr(fi, key, None)
                    if v is not None:
                        cand.append(v)
                if hasattr(fi, "get"):
                    for key in ("last_price", "regularMarketPrice", "lastPrice"):
                        v = fi.get(key)
                        if v is not None:
                            cand.append(v)
                cand = [float(x) for x in cand if x is not None]
                if cand:
                    price = cand[0]
        except Exception:
            price = None
        if price is not None and price > 0:
            return float(price)

        # 2) 1m intraday
        try:
            hist = t.history(period="1d", interval="1m")
            if hasattr(hist, "empty") and not hist.empty:
                last = hist["Close"].dropna()
                if len(last):
                    return float(last.iloc[-1])
        except Exception:
            pass

        # 3) 1d (previous close within day)
        try:
            hist = t.history(period="1d")
            if hasattr(hist, "empty") and not hist.empty:
                return float(hist["Close"].iloc[-1])
        except Exception:
            pass

        # 4) .info regularMarketPrice (slow)
        try:
            info = t.info  # type: ignore[attr-defined]
            v = info.get("regularMarketPrice") if isinstance(info, dict) else None
            if v:
                return float(v)
        except Exception:
            pass
    except Exception:
        return None
    return None


async def list_option_contracts(
    client: httpx.AsyncClient,
    underlying: str,
    limit: int | None = 2000,
    *,
    return_diagnostics: bool = False,
    max_pages: int = 100,
) -> List[dict] | tuple[List[dict], dict]:
    """Iterate Massive (Polygon) option contracts with robust cursor handling.

    Polygon may return either `next_url_cursor` (preferred), `nextCursor`, or
    a full `next_url` that includes a `cursor` query param. We must pass only the
    opaque cursor string back to the API, not the whole URL.
    We also request near-expiry first by sorting on expiration_date asc and
    applying expiration_date.gte=today so that 0DTE/next DTE are included in
    the first page even when the caller uses a limit.
    """
    out: List[dict] = []
    cursor: Optional[str] = None
    page_count = 0
    pagination_completed = True
    provider_truncation = False
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date().isoformat()
    while True:
        if page_count >= max(max_pages, 1):
            pagination_completed = False
            provider_truncation = True
            break
        page_count += 1
        params = {
            "underlying_ticker": underlying.upper(),
            "active": "true",
            "limit": min(limit, 1000) if limit else 1000,
            "sort": "expiration_date",
            "order": "asc",
            "expiration_date.gte": today,
        }
        if cursor:
            params["cursor"] = cursor
        data = await polygon_get(client, "/v3/reference/options/contracts", params)
        out.extend(data.get("results") or [])

        cursor = _extract_next_cursor(data)
        if limit and len(out) >= limit:
            out = out[:limit]
            provider_truncation = True
            break
        if not cursor:
            break
    diagnostics = {
        "underlying": underlying.upper(),
        "total_contracts_fetched": len(out),
        "total_expirations_fetched": len(
            {
                str(
                    c.get("expiration_date")
                    or c.get("expirationDate")
                    or c.get("exp_date")
                    or ""
                )[:10]
                for c in out
                if c.get("expiration_date") or c.get("expirationDate") or c.get("exp_date")
            }
        ),
        "pagination_completed": pagination_completed,
        "provider_page_count": page_count,
        "provider_truncation": provider_truncation,
        "limit_applied": limit,
    }
    if return_diagnostics:
        return out, diagnostics
    return out


async def list_option_snapshots(
    client: httpx.AsyncClient,
    underlying: str,
    *,
    expiry: str | None = None,
    limit: int = 250,
    max_pages: int = 25,
    return_diagnostics: bool = False,
) -> list[dict] | tuple[list[dict], dict]:
    out: list[dict] = []
    cursor: str | None = None
    page_count = 0
    pagination_completed = True
    provider_truncation = False
    while True:
        if page_count >= max(max_pages, 1):
            pagination_completed = False
            provider_truncation = True
            break
        page_count += 1
        params = {
            "limit": max(1, min(int(limit), 250)),
            "sort": "ticker",
            "order": "asc",
        }
        if expiry:
            params["expiration_date"] = str(expiry)[:10]
        if cursor:
            params["cursor"] = cursor
        data = await polygon_get(client, f"/v3/snapshot/options/{underlying.upper()}", params)
        results = []
        if isinstance(data, dict):
            raw_results = data.get("results") or data.get("result") or []
            if isinstance(raw_results, list):
                results = [row for row in raw_results if isinstance(row, dict)]
            elif isinstance(raw_results, dict):
                results = [raw_results]
            if data.get("status") == "error":
                pagination_completed = False
                provider_truncation = True
        out.extend(results)
        cursor = _extract_next_cursor(data if isinstance(data, dict) else {})
        if not cursor:
            break
    diagnostics = {
        "underlying": underlying.upper(),
        "expiry": (str(expiry)[:10] if expiry else None),
        "total_snapshot_rows": len(out),
        "provider_page_count": page_count,
        "pagination_completed": pagination_completed,
        "provider_truncation": provider_truncation,
    }
    if return_diagnostics:
        return out, diagnostics
    return out


async def fetch_aggregate_gamma_chain(
    client: httpx.AsyncClient,
    symbol: str,
    *,
    spot: float | None,
    underlyings: list[str],
    contracts_by_underlying: dict[str, list[dict]],
    provider_listing: dict[str, Any],
    max_pages_per_expiry: int = 25,
    page_limit: int = 250,
) -> tuple[list[dict], dict[str, Any]]:
    listing_lookup: dict[str, dict[str, Any]] = {}
    expiries_by_underlying: dict[str, list[str]] = {}
    for underlying, contracts in contracts_by_underlying.items():
        expiries: set[str] = set()
        for contract in contracts:
            ticker = (
                contract.get("ticker")
                or contract.get("symbol")
                or contract.get("option_ticker")
            )
            if ticker:
                listing_lookup[str(ticker)] = contract
            expiry = (
                contract.get("expiration_date")
                or contract.get("expirationDate")
                or contract.get("exp_date")
            )
            if expiry:
                expiries.add(str(expiry)[:10])
        expiries_by_underlying[underlying] = sorted(expiries)
    semaphore = asyncio.Semaphore(
        max(int(os.getenv("POLYGON_EXPIRY_SNAPSHOT_CONCURRENCY", "4")), 1)
    )

    async def _fetch_one(underlying: str, expiry: str) -> tuple[list[dict], dict[str, Any]]:
        async with semaphore:
            return await list_option_snapshots(
                client,
                underlying,
                expiry=expiry,
                limit=page_limit,
                max_pages=max_pages_per_expiry,
                return_diagnostics=True,
            )

    tasks = [
        asyncio.create_task(_fetch_one(underlying, expiry))
        for underlying in underlyings
        for expiry in expiries_by_underlying.get(underlying, [])
    ]
    chain_rows: list[dict] = []
    snapshot_details: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()
    for task in tasks:
        results, diagnostics = await task
        snapshot_details.append(diagnostics)
        for snapshot in results:
            ticker = (
                snapshot.get("ticker")
                or (snapshot.get("details") or {}).get("ticker")
            )
            if ticker and ticker in seen_tickers:
                continue
            row = _normalize_snapshot_chain_row(
                snapshot,
                spot=spot,
                fallback_contract=listing_lookup.get(str(ticker)) if ticker else None,
            )
            if row is None:
                continue
            seen_tickers.add(str(row.get("ticker")))
            chain_rows.append(row)
    snapshot_fetch = {
        "fetched_underlyings": list(underlyings),
        "available_expirations": sorted(
            {
                expiry
                for expiries in expiries_by_underlying.values()
                for expiry in expiries
            }
        ),
        "total_snapshot_rows": len(chain_rows),
        "provider_page_count": sum(
            int(detail.get("provider_page_count", 0) or 0) for detail in snapshot_details
        ),
        "pagination_completed": all(
            detail.get("pagination_completed", True) for detail in snapshot_details
        ),
        "provider_truncation": any(
            detail.get("provider_truncation", False) for detail in snapshot_details
        ),
        "details": snapshot_details,
        "provider_listing": dict(provider_listing or {}),
    }
    return chain_rows, snapshot_fetch


async def fetch_direct_expiry_gamma_chain(
    client: httpx.AsyncClient,
    *,
    spot: float | None,
    underlyings: list[str],
    expiries: list[str],
    provider_listing: dict[str, Any] | None = None,
    max_pages_per_expiry: int = 25,
    page_limit: int = 250,
) -> tuple[list[dict], dict[str, Any]]:
    target_expiries = sorted({str(expiry)[:10] for expiry in (expiries or []) if expiry})
    semaphore = asyncio.Semaphore(
        max(int(os.getenv("POLYGON_EXPIRY_SNAPSHOT_CONCURRENCY", "4")), 1)
    )

    async def _fetch_one(underlying: str, expiry: str) -> tuple[list[dict], dict[str, Any]]:
        async with semaphore:
            return await list_option_snapshots(
                client,
                underlying,
                expiry=expiry,
                limit=page_limit,
                max_pages=max_pages_per_expiry,
                return_diagnostics=True,
            )

    tasks = [
        asyncio.create_task(_fetch_one(underlying, expiry))
        for underlying in underlyings
        for expiry in target_expiries
    ]
    chain_rows: list[dict] = []
    snapshot_details: list[dict[str, Any]] = []
    seen_tickers: set[str] = set()
    for task in tasks:
        results, diagnostics = await task
        snapshot_details.append(diagnostics)
        for snapshot in results:
            ticker = snapshot.get("ticker") or (snapshot.get("details") or {}).get("ticker")
            if ticker and ticker in seen_tickers:
                continue
            row = _normalize_snapshot_chain_row(
                snapshot,
                spot=spot,
                fallback_contract=None,
            )
            if row is None:
                continue
            seen_tickers.add(str(row.get("ticker")))
            chain_rows.append(row)
    pagination_completed = all(
        detail.get("pagination_completed", True) for detail in snapshot_details
    )
    provider_truncation = any(
        detail.get("provider_truncation", False) for detail in snapshot_details
    )
    snapshot_fetch = {
        "fetch_path": "direct_expiry_snapshot",
        "fetched_underlyings": list(underlyings),
        "requested_expirations": target_expiries,
        "available_expirations": target_expiries,
        "selected_expiry_resolution_mode": "direct_snapshot",
        "selected_expiry_guaranteed": bool(target_expiries) and pagination_completed and not provider_truncation,
        "fallback_used": False,
        "listing_truncated": bool((provider_listing or {}).get("provider_truncation")),
        "total_contracts_fetched": int((provider_listing or {}).get("total_contracts_fetched") or 0),
        "total_expirations_fetched": int((provider_listing or {}).get("total_expirations_fetched") or 0),
        "total_snapshot_rows": len(chain_rows),
        "provider_page_count": sum(
            int(detail.get("provider_page_count", 0) or 0) for detail in snapshot_details
        ),
        "pagination_completed": pagination_completed,
        "provider_truncation": provider_truncation,
        "details": snapshot_details,
        "provider_listing": dict(provider_listing or {}),
    }
    return chain_rows, snapshot_fetch


async def snapshot_option(client: httpx.AsyncClient, option_ticker: str) -> dict:
    # Try the per-contract options snapshot path first, then fall back
    try:
        path = f"/v3/snapshot/options/{option_ticker}"
        data = await polygon_get(client, path, {})
        if isinstance(data, dict) and (
            data.get("results")
            or data.get("result")
            or data.get("open_interest")
            or data.get("implied_volatility")
        ):
            return data.get("results") or data.get("result") or data
    except Exception:
        pass
    try:
        data = await polygon_get(client, "/v3/snapshot", {"ticker": option_ticker})
        return data.get("results") or data.get("result") or data
    except Exception as e:
        return {"error": str(e)}


async def fetch_oi_iv_by_filters(
    client: httpx.AsyncClient,
    underlying: str,
    strike: float,
    expiry: str,
    option_type: str,
) -> dict:
    """Query Massive (Polygon) snapshots by underlying + filters to retrieve OI/IV.

    Uses path: /v3/snapshot/options/{underlying} with params strike_price, expiration_date, contract_type.
    Returns a dict with keys: oi, iv. T (seconds to expiration) is not guaranteed in this route.
    """
    try:
        params = {
            "strike_price": strike,
            "expiration_date": expiry[:10] if expiry else None,
            "contract_type": ("call" if option_type.lower().startswith("c") else "put"),
            "order": "asc",
            "limit": 1,
            "sort": "ticker",
        }
        # Remove Nones
        params = {k: v for k, v in params.items() if v is not None}
        data = await polygon_get(client, f"/v3/snapshot/options/{underlying.upper()}", params)
        res = None
        if isinstance(data, dict):
            arr = data.get("results") or data.get("result")
            if isinstance(arr, list) and arr:
                res = arr[0]
            elif isinstance(arr, dict):
                res = arr
        if not isinstance(res, dict):
            return {}
        oi = res.get("open_interest") or res.get("openInterest")
        iv = res.get("implied_volatility") or res.get("impliedVolatility")
        if iv is None:
            iv = (res.get("greeks") or {}).get("iv") or (res.get("greeks") or {}).get(
                "implied_volatility"
            )
        out = {}
        if oi is not None:
            try:
                out["oi"] = float(oi)
            except Exception:
                pass
        if iv is not None:
            try:
                out["iv"] = float(iv)
            except Exception:
                pass
        # Volume if available
        try:
            vol = (res.get("day") or {}).get("volume")
            if vol is not None:
                out["vol"] = float(vol)
        except Exception:
            pass
        if out:
            out["src"] = "filters"
        return out
    except Exception:
        return {}


async def fetch_option_oi_iv(client: httpx.AsyncClient, option_ticker: str) -> dict:
    """Try multiple Massive (Polygon) endpoints/paths to retrieve OI and IV for a contract.

    Returns a dict with possible keys: oi, iv, vol, seconds_to_expiration.
    """

    def _coalesce(d: dict, paths: list):
        for p in paths:
            try:
                cur = d
                for key in p.split("."):
                    if isinstance(cur, dict) and key in cur:
                        cur = cur[key]
                    else:
                        cur = None
                        break
                if cur is not None:
                    try:
                        return float(cur)
                    except Exception:
                        continue
            except Exception:
                continue
        return None

    def _find_by_keywords(d: dict, key_parts: list) -> Optional[float]:
        """Depth‑first search for a numeric value whose key path includes all key_parts.
        Returns the first positive numeric match.
        """
        matches: list[float] = []

        def _walk(node, path):
            if isinstance(node, dict):
                for k, v in node.items():
                    new_path = path + [str(k)]
                    # check at this node
                    if all(part in ".".join(new_path).lower() for part in key_parts):
                        try:
                            val = float(v)
                            if val is not None:
                                matches.append(val)
                        except Exception:
                            pass
                    _walk(v, new_path)
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    _walk(v, path + [str(i)])

        _walk(d, [])
        for m in matches:
            if m != 0.0:
                return m
        return matches[0] if matches else None

    # 1) Per-contract snapshot endpoint(s)
    try:
        s = await snapshot_option(client, option_ticker)
        if isinstance(s, dict):
            oi = _coalesce(
                s,
                [
                    "open_interest",
                    "openInterest",
                    "contract.open_interest",
                    "contract.openInterest",
                    "day.open_interest",
                    "day.openInterest",
                ],
            )
            if oi is None:
                oi = _find_by_keywords(s, ["open", "interest"])
            iv = _coalesce(
                s,
                [
                    "implied_volatility",
                    "impliedVolatility",
                    "greeks.iv",
                    "greeks.implied_volatility",
                ],
            )
            if iv is None:
                iv = _find_by_keywords(s, ["implied", "vol"]) or _find_by_keywords(s, ["iv"])
            sec = _coalesce(s, ["seconds_to_expiration", "secondsToExpiration"])
            # volume
            vol = _coalesce(s, ["day.volume"])
            if oi is not None or iv is not None or vol is not None:
                return {"oi": oi, "iv": iv, "vol": vol, "seconds_to_expiration": sec}
    except Exception:
        pass

    # 2) Reference contracts filtered by ticker (some accounts expose OI here)
    try:
        ref = await polygon_get(
            client, "/v3/reference/options/contracts", {"ticker": option_ticker}
        )
        res = None
        if isinstance(ref, dict):
            arr = ref.get("results") or ref.get("result")
            if isinstance(arr, list) and arr:
                res = arr[0]
            elif isinstance(arr, dict):
                res = arr
        if isinstance(res, dict):
            oi = _coalesce(res, ["open_interest", "openInterest"]) or _find_by_keywords(
                res, ["open", "interest"]
            )
            iv = _coalesce(res, ["implied_volatility", "impliedVolatility"]) or _find_by_keywords(
                res, ["implied", "vol"]
            )
            vol = _coalesce(res, ["day.volume"])
            if oi is not None or iv is not None or vol is not None:
                return {"oi": oi, "iv": iv, "vol": vol, "seconds_to_expiration": None}
    except Exception:
        pass

    return {}


# ------------------------------
# Core engine with Next-expiration filter
# ------------------------------
@dataclass
class VannaRow:
    symbol: str
    strike: float
    expiry: str
    option_type: str
    oi: float
    iv: float
    t_years: float
    vanna: float


async def compute_vanna_for_ticker(
    job: Job,
    symbol: str,
    spot: float,
    r: float = 0.0,
    q: float = 0.0,
    pct_window: float = 0.10,
    only_next_expiry: bool = True,
    expiry_override: str | None = None,
    weight_mode: str = "oi",
    expiry_mode: str = "",
    allowed_expiries: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    job.status = "running"
    job.progress = 0.03
    job.log(
        f"Compute {symbol} spot={spot:.2f} ±{pct_window*100:.2f}% window; next_expiry={only_next_expiry}; weight={weight_mode}; emode={(expiry_mode or 'selected')}"
    )

    lo, hi = spot * (1 - pct_window), spot * (1 + pct_window)
    requested_expiry_keys = {
        exp[:10] if isinstance(exp, str) else exp for exp in (allowed_expiries or []) if exp
    }
    async with httpx.AsyncClient() as client:
        # Fetch across multiple underlying forms to avoid missing weeklies/dailies (e.g., SPXW) or index namespace (I:SPX)
        sym = symbol.upper()
        underlyings = [sym, f"I:{sym}"]
        if not sym.endswith("W"):
            underlyings.append(sym + "W")
        contracts = []
        for u in underlyings:
            try:
                cs = await list_option_contracts(client, u, limit=2000)
                if cs:
                    contracts.extend(cs)
                    job.log(f"Fetched {len(cs)} contracts for {u}")
            except Exception as e:
                job.log(f"WARN: list contracts {u}: {e}")
        job.log(f"Total merged contracts: {len(contracts)}")
        if job.cancel_event.is_set():
            job.status = "cancelled"
            job.log("Cancelled pre-filter")
            raise asyncio.CancelledError()

        # First pass to find the nearest future expiry using contract metadata only
        next_expiry_key = expiry_override
        if not next_expiry_key and only_next_expiry:
            from datetime import datetime, timezone

            now = time.time()
            best_T = float("inf")
            total = max(len(contracts), 1)
            for i, c in enumerate(contracts, 1):
                try:
                    exp = (
                        c.get("expiration_date")
                        or c.get("expirationDate")
                        or c.get("exp_date")
                        or ""
                    )
                    if len(exp) >= 10:
                        dt = datetime.fromisoformat(exp[:10]).replace(tzinfo=timezone.utc)
                        T = (dt.timestamp() - now) / (365 * 24 * 3600)
                        if T and T > 0 and T < best_T:
                            best_T = T
                            next_expiry_key = exp
                except Exception:
                    pass
                if i % 500 == 0:
                    job.progress = min(0.06 + 0.02 * (i / total), 0.09)
            job.log(
                f"Next expiry derived quickly: {next_expiry_key or 'unknown'} (T≈{best_T if best_T!=float('inf') else 'n/a'})"
            )
            job.progress = max(job.progress, 0.09)

        # Build candidate list (metadata only)
        candidates: List[tuple] = []
        # Determine special expiry filtering
        try:
            from datetime import datetime, timezone

            today_iso = datetime.now(timezone.utc).date().isoformat()
        except Exception:
            today_iso = ""
        fmode = (expiry_mode or "").strip().lower()
        for c in contracts:
            opt_tkr = c.get("ticker") or c.get("symbol") or c.get("option_ticker")
            if not opt_tkr:
                continue
            strike = float(c.get("strike_price") or c.get("strikePrice") or c.get("strike") or 0)
            if not (lo <= strike <= hi):
                continue
            expiry = c.get("expiration_date") or c.get("expirationDate") or c.get("exp_date") or ""
            if requested_expiry_keys and expiry[:10] not in requested_expiry_keys:
                continue
            # Apply expiry filter precedence
            if fmode == "zero_dte":
                if today_iso and expiry[:10] != today_iso:
                    continue
            elif fmode == "exclude_selected" and expiry_override:
                if expiry == expiry_override:
                    continue
            elif next_expiry_key:
                if expiry != next_expiry_key:
                    continue
            otc = (c.get("contract_type") or c.get("type") or "").lower()
            option_type = (
                "call"
                if otc.startswith("c")
                else ("put" if otc.startswith("p") else ("call" if "C" in opt_tkr else "put"))
            )
            candidates.append((opt_tkr, strike, expiry, option_type))

        # Optional cap to reduce API load
        try:
            if candidates:
                sample = ", ".join([c[0] for c in candidates[:5]])
                job.log(f"Candidates pre-cap: {len(candidates)}; sample: {sample}")
        except Exception:
            pass
        cap = int(os.getenv("POLYGON_SNAPSHOT_CAP", "700"))
        if len(candidates) > cap:
            candidates.sort(key=lambda t: abs(t[1] - spot))
            candidates = candidates[:cap]
            job.log(
                f"Capped snapshot candidates to {len(candidates)} using POLYGON_SNAPSHOT_CAP={cap}"
            )
        else:
            job.log(f"Prefiltered to {len(candidates)} in-window contracts before snapshots")

        rows: List[VannaRow] = []
        total = max(len(candidates), 1)
        t0 = time.time()

        # Concurrency + simple backoff for snapshots
        concurrency = int(os.getenv("POLYGON_CONCURRENCY", "8"))
        sem = asyncio.Semaphore(max(concurrency, 1))

        async def fetch_snap(tkr: str, strike: float, expiry: str, opt_type: str) -> dict:
            delay = 0.25
            for attempt in range(1, 6):
                try:
                    async with sem:
                        # Prefer underlying+filters; fall back to per-ticker lookup
                        data = await fetch_oi_iv_by_filters(
                            client, symbol, strike, expiry, opt_type
                        )
                        if not data:
                            data = await fetch_option_oi_iv(client, tkr)
                    if data:
                        return data
                except Exception:
                    pass
                await asyncio.sleep(delay)
                delay = min(delay * 2, 2.0)
            return {}

        done = 0

        async def process_one(tkr: str, strike: float, expiry: str, option_type: str):
            nonlocal done
            if job.cancel_event.is_set():
                return
            snap = await fetch_snap(tkr, strike, expiry, option_type)
            oi = float(snap.get("oi") or 0.0)
            iv = float(snap.get("iv") or 0.25)
            sec_to_exp = snap.get("seconds_to_expiration")
            if isinstance(sec_to_exp, (int, float)) and sec_to_exp > 0:
                T = float(sec_to_exp) / (365 * 24 * 3600)
            else:
                try:
                    from datetime import datetime, timezone

                    if len(expiry) >= 10:
                        dt = datetime.fromisoformat(expiry[:10]).replace(tzinfo=timezone.utc)
                        T = max((dt.timestamp() - time.time()) / (365 * 24 * 3600), 1 / 365)
                    else:
                        T = 30 / 365
                except Exception:
                    T = 30 / 365
            v = bs_vanna(spot, strike, T, 0.0, 0.0, float(iv or 0.25))
            # weight selection
            w = 1.0
            if weight_mode == "oi":
                w = max(float(oi or 0.0), 0.0)
            elif weight_mode == "volume":
                w = max(float(snap.get("vol") or 0.0), 0.0)
            else:
                w = 1.0
            scaled = v * w * CONTRACT_MULTIPLIER
            rows.append(
                VannaRow(
                    symbol=symbol.upper(),
                    strike=strike,
                    expiry=expiry,
                    option_type=option_type,
                    oi=float(oi or 0.0),
                    iv=float(iv or 0.25),
                    t_years=float(T),
                    vanna=scaled,
                )
            )
            done += 1
            if done <= 5 and os.getenv("SNAPSHOT_SAMPLE_DEBUG", "0") == "1":
                job.log(f"sample: {tkr} oi={oi} iv={iv} T={T:.6f}")
            job.progress = min(0.1 + 0.85 * (done / total), 0.95)

        await asyncio.gather(*(process_one(*c) for c in candidates))
        job.log(f"Snapshots completed: {done}/{total}")

        df = pd.DataFrame([r.__dict__ for r in rows])
        if df.empty:
            job.progress = 1.0
            # Determine meta expiry display for empty set
            disp_exp = next(iter(requested_expiry_keys)) if len(requested_expiry_keys) == 1 else next_expiry_key
            if fmode == "zero_dte" and today_iso:
                disp_exp = today_iso
            elif fmode == "exclude_selected" and expiry_override:
                disp_exp = f"except {expiry_override}"
            elif len(requested_expiry_keys) > 1:
                disp_exp = f"{len(requested_expiry_keys)} expirations"
            return df, {
                "expiry": disp_exp,
                "spot": spot,
                "included_expirations": sorted(requested_expiry_keys),
            }
        # If all OI are zero, optionally fall back to unweighted vanna
        try:
            oi_pos_ct = int((df["oi"] > 0).sum())
            if oi_pos_ct == 0 and ALLOW_UNWEIGHTED_FALLBACK:
                job.log(
                    "All OI are zero — falling back to unweighted vanna (ALLOW_UNWEIGHTED_FALLBACK=1)"
                )

                def _raw_v(row):
                    try:
                        return bs_vanna(
                            spot,
                            float(row["strike"]),
                            float(row["t_years"]),
                            0.0,
                            0.0,
                            float(row["iv"]),
                        )
                    except Exception:
                        return 0.0

                df["vanna"] = df.apply(_raw_v, axis=1)
        except Exception:
            pass
        # Debug: OI distribution
        try:
            oi_pos = int((df["oi"] > 0).sum())
            job.log(f"Snapshot OI>0: {oi_pos}/{len(df)}")
        except Exception:
            pass

        agg = df.groupby(["strike", "option_type"], as_index=False)["vanna"].sum()
        pivot = agg.pivot(index="strike", columns="option_type", values="vanna").fillna(0.0)
        pivot = pivot.rename(columns={"call": "vanna_calls", "put": "vanna_puts"})
        pivot["vanna_net"] = pivot.get("vanna_calls", 0.0) + pivot.get("vanna_puts", 0.0)
        pivot = pivot.sort_index()

        # Debug: summary stats and top-5 by |vanna_net|
        try:
            vn = pivot["vanna_net"].to_numpy(dtype=float)
            if vn.size:
                vmin = float(np.min(vn))
                vmed = float(np.median(vn))
                vmax = float(np.max(vn))
                job.log(f"vanna_net stats: min={vmin:.3e} med={vmed:.3e} max={vmax:.3e}")
                idx = np.argsort(np.abs(vn))[-5:][::-1]
                strikes = pivot.index.to_numpy(dtype=float)[idx]
                tops = ", ".join([f"{strikes[i]:.2f}:{vn[idx[i]]:.3e}" for i in range(len(idx))])
                job.log(f"Top5 |vanna_net|: {tops}")
        except Exception:
            pass
        # Quick stats for UI header
        try:
            oi_sum = float(max(df["oi"].sum(), 0.0))
            iv_med = float(df["iv"].median()) if not df["iv"].empty else 0.0
            iv_mean = float(df["iv"].mean()) if not df["iv"].empty else 0.0
            n_contracts = int(len(df))
            n_strikes = int(len(pivot))
            stats = {
                "oi_sum": oi_sum,
                "iv_median": iv_med,
                "iv_mean": iv_mean,
                "n_contracts": n_contracts,
                "n_strikes": n_strikes,
            }
        except Exception:
            stats = {}

        job.progress = 1.0
        job.log(f"Completed in {time.time()-t0:.1f}s with {len(pivot)} strikes.")
        disp_exp = next(iter(requested_expiry_keys)) if len(requested_expiry_keys) == 1 else next_expiry_key
        if fmode == "zero_dte" and today_iso:
            disp_exp = today_iso
        elif fmode == "exclude_selected" and expiry_override:
            disp_exp = f"except {expiry_override}"
        elif len(requested_expiry_keys) > 1:
            disp_exp = f"{len(requested_expiry_keys)} expirations"
        return pivot.reset_index(), {
            "expiry": disp_exp,
            "spot": spot,
            "stats": stats,
            "expiry_mode": fmode or "selected",
            "included_expirations": sorted(requested_expiry_keys),
        }


async def compute_gex_for_ticker(
    job: Job,
    symbol: str,
    spot: float,
    pct_window: float = 0.10,
    only_next_expiry: bool = True,
    expiry_mode: str = "",
    expiry_override: str | None = None,
    include_0dte: bool = True,
    remove_0dte: bool | None = None,
    allowed_expiries: list[str] | None = None,
    include_solver_curve: bool = False,
    solver_config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Compute Net GEX by strike using Massive (Polygon) snapshots.

    Static strike-space gamma and raw signed gamma use:
    gamma * open_interest * CONTRACT_MULTIPLIER * S^2

    Headline Net GEX is exposed separately using a spot-scaled convention:
    gamma * open_interest * CONTRACT_MULTIPLIER * S

    Calls contribute +, puts -.
    """
    job.status = "running"
    job.progress = 0.03
    remove_0dte_flag = bool(remove_0dte) if remove_0dte is not None else (not include_0dte)
    job.log(
        f"Compute GEX {symbol} spot={spot:.2f} ±{pct_window*100:.2f}% chart window; "
        f"next_expiry={only_next_expiry}; emode={(expiry_mode or 'selected')}; "
        f"remove_0dte={remove_0dte_flag}"
    )

    lo, hi = spot * (1 - pct_window), spot * (1 + pct_window)
    fmode = (expiry_mode or "").strip().lower()
    requested_expiry_keys = {
        exp[:10] if isinstance(exp, str) else exp for exp in (allowed_expiries or []) if exp
    }
    selected_expiration_set = sorted(requested_expiry_keys) if requested_expiry_keys else None
    try:
        from datetime import datetime, timezone

        today_iso = datetime.now(timezone.utc).date().isoformat()
    except Exception:
        today_iso = ""
    async with httpx.AsyncClient() as client:
        sym = symbol.upper()
        underlyings = [sym, f"I:{sym}"]
        if not sym.endswith("W"):
            underlyings.append(sym + "W")
        provider_listing: dict[str, Any] = {
            "total_contracts_fetched": 0,
            "total_expirations_fetched": 0,
            "pagination_completed": True,
            "provider_page_count": 0,
            "provider_truncation": False,
            "details": [],
            "aggregate_chain_cache_source": None,
            "fetch_path": "unresolved",
            "selected_expiry_resolution_mode": (
                "not_applicable" if fmode == "all" else "not_resolved"
            ),
            "listing_truncated": False,
            "selected_expiry_guaranteed": False,
            "fallback_used": False,
        }
        contracts: list[dict] = []
        chain_rows: list[dict] = []
        contracts_by_underlying: dict[str, list[dict]] = {}
        aggregate_chain_cache_source: str | None = None
        expiry_override_key = expiry_override[:10] if isinstance(expiry_override, str) else expiry_override
        next_expiry_key = expiry_override
        direct_target_expiries: set[str] = set()
        if fmode != "all":
            if fmode == "zero_dte" and today_iso:
                direct_target_expiries.add(today_iso)
            elif expiry_override_key:
                direct_target_expiries.add(str(expiry_override_key)[:10])

        if fmode == "all":
            chain_cache_key = _aggregate_gamma_chain_cache_key(sym, spot)
            chain_ttl = int(os.getenv("GAMMA_CHAIN_CACHE_TTL_SEC", "300"))
            cached_chain = job_manager.cache_get(chain_cache_key, chain_ttl)
            if cached_chain is not None:
                aggregate_chain_cache_source = "memory"
            else:
                cached_chain = disk_cache_get(chain_cache_key, chain_ttl)
                if cached_chain is not None:
                    aggregate_chain_cache_source = "disk"
                    try:
                        job_manager.cache_set(chain_cache_key, cached_chain)
                    except Exception:
                        pass
            if isinstance(cached_chain, dict):
                contracts = list(cached_chain.get("contracts") or [])
                chain_rows = list(cached_chain.get("chain_rows") or [])
                contracts_by_underlying = {
                    str(k): list(v)
                    for k, v in dict(cached_chain.get("contracts_by_underlying") or {}).items()
                }
                provider_listing = dict(cached_chain.get("provider_listing") or {})
                provider_listing["aggregate_chain_cache_source"] = aggregate_chain_cache_source
                job.log(
                    f"Aggregate gamma chain cache hit ({aggregate_chain_cache_source}) "
                    f"with {len(chain_rows)} normalized rows"
                )

        if fmode != "all" and direct_target_expiries:
            direct_expiries = sorted(direct_target_expiries)
            snapshot_fetch: dict[str, Any] = {}
            chain_rows, snapshot_fetch = await fetch_direct_expiry_gamma_chain(
                client,
                spot=spot,
                underlyings=underlyings,
                expiries=direct_expiries,
                provider_listing=provider_listing,
                max_pages_per_expiry=int(
                    os.getenv("POLYGON_SNAPSHOT_MAX_PAGES_PER_EXPIRY", "25")
                ),
                page_limit=int(os.getenv("POLYGON_SNAPSHOT_PAGE_LIMIT", "250")),
            )
            provider_listing.update(
                {
                    "fetch_path": "direct_expiry_snapshot",
                    "selected_expiry_resolution_mode": "direct_snapshot",
                    "selected_expiry_guaranteed": bool(
                        snapshot_fetch.get("selected_expiry_guaranteed")
                    ),
                    "fallback_used": False,
                    "provider_page_count": int(snapshot_fetch.get("provider_page_count", 0) or 0),
                    "pagination_completed": bool(
                        snapshot_fetch.get("pagination_completed", True)
                    ),
                    "provider_truncation": bool(
                        snapshot_fetch.get("provider_truncation", False)
                    ),
                    "listing_truncated": bool(provider_listing.get("provider_truncation")),
                    "snapshot_fetch": snapshot_fetch,
                }
            )
            if next_expiry_key is None and len(direct_expiries) == 1:
                next_expiry_key = direct_expiries[0]
            job.log(
                "Direct selected-expiry snapshot fetch "
                f"for {','.join(direct_expiries)} returned {len(chain_rows)} normalized rows"
                + (
                    ""
                    if provider_listing.get("selected_expiry_guaranteed")
                    else " (coverage not guaranteed)"
                )
            )

        # If direct selected-expiry snapshots return no usable rows, fall back to
        # listing-based resolution instead of silently returning an empty chain.
        if not contracts and not chain_rows:
            provider_listing_details: list[dict] = []
            for underlying in underlyings:
                try:
                    listing_limit = None if fmode == "all" else 2000
                    contract_rows, listing_diag = await list_option_contracts(
                        client,
                        underlying,
                        limit=listing_limit,
                        return_diagnostics=True,
                    )
                    contracts_by_underlying[underlying] = list(contract_rows or [])
                    contracts.extend(contract_rows or [])
                    provider_listing_details.append(listing_diag)
                    job.log(
                        f"Fetched {len(contract_rows)} contracts for {underlying}"
                        f" across {listing_diag.get('provider_page_count')} pages"
                        + (
                            ""
                            if listing_diag.get("pagination_completed", True)
                            else " (pagination incomplete)"
                        )
                    )
                except Exception as e:
                    contracts_by_underlying[underlying] = []
                    job.log(f"WARN: list contracts {underlying}: {e}")
            provider_listing = {
                "total_contracts_fetched": len(contracts),
                "total_expirations_fetched": len(
                    {
                        str(
                            c.get("expiration_date")
                            or c.get("expirationDate")
                            or c.get("exp_date")
                            or ""
                        )[:10]
                        for c in contracts
                        if c.get("expiration_date") or c.get("expirationDate") or c.get("exp_date")
                    }
                ),
                "pagination_completed": all(
                    detail.get("pagination_completed", True) for detail in provider_listing_details
                ),
                "provider_page_count": sum(
                    int(detail.get("provider_page_count", 0) or 0)
                    for detail in provider_listing_details
                ),
                "provider_truncation": any(
                    detail.get("provider_truncation", False) for detail in provider_listing_details
                ),
                "details": provider_listing_details,
                "aggregate_chain_cache_source": aggregate_chain_cache_source,
                "fetch_path": "bulk_listing",
                "selected_expiry_resolution_mode": (
                    "aggregate_listing"
                    if fmode == "all"
                    else ("listing_search" if expiry_override_key else "next_expiry_from_listing")
                ),
                "listing_truncated": any(
                    detail.get("provider_truncation", False) for detail in provider_listing_details
                ),
                "selected_expiry_guaranteed": False,
                "fallback_used": False,
            }
            job.log(f"Total merged contracts: {len(contracts)}")

        if job.cancel_event.is_set():
            job.status = "cancelled"
            job.log("Cancelled pre-filter")
            raise asyncio.CancelledError()

        if not next_expiry_key and only_next_expiry:
            try:
                from datetime import datetime, timezone

                now = time.time()
                best_t = float("inf")
                for contract in contracts:
                    exp = (
                        contract.get("expiration_date")
                        or contract.get("expirationDate")
                        or contract.get("exp_date")
                        or ""
                    )
                    if len(str(exp)) < 10:
                        continue
                    try:
                        dt = datetime.fromisoformat(str(exp)[:10]).replace(tzinfo=timezone.utc)
                        time_to_expiry = (dt.timestamp() - now) / (365 * 24 * 3600)
                    except Exception:
                        continue
                    if time_to_expiry and time_to_expiry > 0 and time_to_expiry < best_t:
                        best_t = time_to_expiry
                        next_expiry_key = str(exp)
            except Exception:
                pass
            job.log(f"Next expiry derived quickly: {next_expiry_key or 'unknown'}")

        next_expiry_key_norm = next_expiry_key[:10] if isinstance(next_expiry_key, str) else next_expiry_key

        if fmode == "all":
            if not chain_rows:
                chain_rows, snapshot_fetch = await fetch_aggregate_gamma_chain(
                    client,
                    sym,
                    spot=spot,
                    underlyings=underlyings,
                    contracts_by_underlying=contracts_by_underlying,
                    provider_listing=provider_listing,
                    max_pages_per_expiry=int(
                        os.getenv("POLYGON_SNAPSHOT_MAX_PAGES_PER_EXPIRY", "25")
                    ),
                    page_limit=int(os.getenv("POLYGON_SNAPSHOT_PAGE_LIMIT", "250")),
                )
                provider_listing["snapshot_fetch"] = snapshot_fetch
                provider_listing["aggregate_chain_cache_source"] = aggregate_chain_cache_source
                chain_cache_payload = {
                    "contracts": contracts,
                    "contracts_by_underlying": contracts_by_underlying,
                    "chain_rows": chain_rows,
                    "provider_listing": provider_listing,
                }
                chain_cache_key = _aggregate_gamma_chain_cache_key(sym, spot)
                try:
                    job_manager.cache_set(chain_cache_key, chain_cache_payload)
                    disk_cache_set(chain_cache_key, chain_cache_payload)
                except Exception:
                    pass
                job.log(
                    "Aggregate gamma chain built from paginated expiry snapshots "
                    f"with {len(chain_rows)} normalized rows"
                )
            else:
                provider_listing.setdefault(
                    "snapshot_fetch",
                    {
                        "total_snapshot_rows": len(chain_rows),
                        "available_expirations": sorted(
                            {
                                str(row.get("expiry"))[:10]
                                for row in chain_rows
                                if row.get("expiry")
                            }
                        ),
                    },
                )
            profile = build_gamma_profile(
                chain_rows,
                spot,
                selected_scope="all",
                selected_expiry=None,
                expirations=selected_expiration_set,
                include_0dte=not remove_0dte_flag,
                remove_0dte=remove_0dte_flag,
                chart_strike_range=(lo, hi),
                today_iso=today_iso,
                include_solver_curve=include_solver_curve,
                solver_config=solver_config,
            )
        else:
            target_expiries: set[str] = set()
            if fmode == "zero_dte" and today_iso:
                target_expiries.add(today_iso)
            elif fmode == "exclude_selected":
                target_expiries = set()
            elif next_expiry_key_norm:
                target_expiries.add(next_expiry_key_norm)

            if target_expiries and not chain_rows and contracts_by_underlying:
                selected_contracts_by_underlying: dict[str, list[dict]] = {}
                for underlying, rows in contracts_by_underlying.items():
                    selected_contracts_by_underlying[underlying] = [
                        row
                        for row in (rows or [])
                        if (
                            (
                                row.get("expiration_date")
                                or row.get("expirationDate")
                                or row.get("exp_date")
                                or ""
                            )[:10]
                            in target_expiries
                        )
                    ]
                chain_rows, snapshot_fetch = await fetch_aggregate_gamma_chain(
                    client,
                    sym,
                    spot=spot,
                    underlyings=underlyings,
                    contracts_by_underlying=selected_contracts_by_underlying,
                    provider_listing=provider_listing,
                    max_pages_per_expiry=int(
                        os.getenv("POLYGON_SNAPSHOT_MAX_PAGES_PER_EXPIRY", "25")
                    ),
                    page_limit=int(os.getenv("POLYGON_SNAPSHOT_PAGE_LIMIT", "250")),
                )
                provider_listing["snapshot_fetch"] = snapshot_fetch
                provider_listing["fetch_path"] = "bulk_listing_then_expiry_snapshot"
                provider_listing["selected_expiry_resolution_mode"] = (
                    "listing_search" if expiry_override_key else "next_expiry_from_listing"
                )
                provider_listing["listing_truncated"] = bool(
                    provider_listing.get("provider_truncation", False)
                )
                provider_listing["selected_expiry_guaranteed"] = bool(
                    target_expiries
                    and not provider_listing.get("provider_truncation", False)
                    and set(snapshot_fetch.get("available_expirations") or []).issuperset(
                        set(target_expiries)
                    )
                )
                provider_listing["fallback_used"] = False
                job.log(
                    "Single-expiry gamma chain built from bulk expiry snapshots "
                    f"for {','.join(sorted(target_expiries))} with {len(chain_rows)} normalized rows"
                )

            if not chain_rows and contracts:
                candidates = []
                for contract in contracts:
                    ticker = contract.get("ticker") or contract.get("symbol") or contract.get("option_ticker")
                    if not ticker:
                        continue
                    try:
                        strike = float(
                            contract.get("strike_price")
                            or contract.get("strikePrice")
                            or contract.get("strike")
                            or 0
                        )
                    except Exception:
                        continue
                    exp_raw = (
                        contract.get("expiration_date")
                        or contract.get("expirationDate")
                        or contract.get("exp_date")
                        or ""
                    )
                    expiry = exp_raw[:10] if isinstance(exp_raw, str) else exp_raw
                    if fmode == "zero_dte":
                        if today_iso and expiry[:10] != today_iso:
                            continue
                    elif fmode == "exclude_selected" and expiry_override_key:
                        if expiry == expiry_override_key:
                            continue
                    elif next_expiry_key_norm and expiry != next_expiry_key_norm:
                        continue
                    opt_type_raw = (contract.get("contract_type") or contract.get("type") or "").lower()
                    option_type = (
                        "call"
                        if opt_type_raw.startswith("c")
                        else ("put" if opt_type_raw.startswith("p") else ("call" if "C" in ticker else "put"))
                    )
                    candidates.append((ticker, strike, expiry, option_type))
                job.log(
                    f"Prefiltered to {len(candidates)} included contracts before OI/IV snapshots; "
                    f"chart strike window remains [{lo:.2f}, {hi:.2f}]"
                )
                provider_listing["fetch_path"] = "listing_then_contract_snapshot"
                provider_listing["selected_expiry_resolution_mode"] = (
                    "listing_search" if expiry_override_key else "next_expiry_from_listing"
                )
                provider_listing["listing_truncated"] = bool(
                    provider_listing.get("provider_truncation", False)
                )
                provider_listing["selected_expiry_guaranteed"] = bool(
                    not provider_listing.get("provider_truncation", False)
                    and bool(next_expiry_key_norm or fmode == "zero_dte")
                )
                provider_listing["fallback_used"] = True

                concurrency = int(os.getenv("POLYGON_CONCURRENCY", "8"))
                sem = asyncio.Semaphore(max(concurrency, 1))

                async def fetch_snap_gex(tkr: str, strike: float, expiry: str, opt_type: str) -> dict:
                    delay = 0.25
                    for _ in range(5):
                        try:
                            async with sem:
                                data = await fetch_oi_iv_by_filters(
                                    client, symbol, strike, expiry, opt_type
                                )
                                if not data:
                                    data = await fetch_option_oi_iv(client, tkr)
                            if data:
                                return data
                        except Exception:
                            pass
                        await asyncio.sleep(delay)
                        delay = min(2.0, delay * 2)
                    return {}

                total = max(len(candidates), 1)
                for idx, (tkr, strike, expiry, opt_type) in enumerate(candidates, start=1):
                    if job.cancel_event.is_set():
                        job.status = "cancelled"
                        job.log("Cancelled during GEX snapshots")
                        raise asyncio.CancelledError()
                    snap = await fetch_snap_gex(tkr, strike, expiry, opt_type)
                    try:
                        from datetime import datetime, timezone

                        dt = datetime.fromisoformat(str(expiry)[:10]).replace(tzinfo=timezone.utc)
                        t_years = max((dt.timestamp() - time.time()) / (365 * 24 * 3600), 1 / 365)
                    except Exception:
                        t_years = 30 / 365
                    chain_rows.append(
                        {
                            "ticker": tkr,
                            "strike": strike,
                            "option_type": opt_type,
                            "oi": float(snap.get("oi") or 0.0),
                            "iv": float(snap.get("iv") or 0.25),
                            "expiry": expiry,
                            "t_years": t_years,
                            "contract_size": CONTRACT_MULTIPLIER,
                        }
                    )
                    if idx % 10 == 0:
                        job.progress = min(0.1 + 0.85 * (idx / total), 0.95)
            profile = build_gamma_profile(
                chain_rows,
                spot,
                selected_scope="selected",
                selected_expiry=next_expiry_key_norm,
                expirations=[next_expiry_key_norm] if next_expiry_key_norm else None,
                include_0dte=not remove_0dte_flag,
                remove_0dte=remove_0dte_flag,
                chart_strike_range=(lo, hi),
                today_iso=today_iso,
                include_solver_curve=include_solver_curve,
                solver_config=solver_config,
            )

        profile_diag = dict(profile.get("zero_gamma_diagnostics") or {})
        profile_diag.update(provider_listing)
        profile["zero_gamma_diagnostics"] = profile_diag
        profile_error = None
        if int(profile_diag.get("included_row_count") or 0) == 0:
            dropped = dict(profile_diag.get("dropped_rows_by_reason") or {})
            invalid_iv_rows = int(dropped.get("invalid_implied_volatility", 0) or 0)
            raw_included = int(
                profile_diag.get("raw_included_row_count")
                or profile_diag.get("input_row_count")
                or 0
            )
            if invalid_iv_rows and invalid_iv_rows >= raw_included:
                profile_error = (
                    "Provider returned no usable implied volatility data for the selected expirations"
                )
            else:
                profile_error = "No usable option contracts were available for the selected expirations"
        if not profile["strikes"]:
            job.progress = 1.0
            disp_exp = next_expiry_key
            if fmode == "zero_dte" and today_iso:
                disp_exp = today_iso
            expiry_label = (
                next(iter(requested_expiry_keys))
                if len(requested_expiry_keys) == 1
                else (
                    f"{len(profile.get('included_expirations') or [])} expirations"
                    if profile.get("included_expirations")
                    else disp_exp
                )
            )
            return pd.DataFrame(), {
                "expiry": expiry_label,
                "spot": spot,
                "net_gex": 0.0,
                "total_gamma_at_spot": None,
                "raw_signed_gamma": None,
                "zero_gamma": None,
                "gamma_regime": "Gamma Regime Unavailable",
                "spot_vs_zero_gamma": "No Zero Gamma in tested range",
                "zero_gamma_diagnostics": profile["zero_gamma_diagnostics"],
                "zero_gamma_curve": [],
                "expiry_mode": fmode or "selected",
                "total_oi": 0.0,
                "include_0dte": not remove_0dte_flag,
                "remove_0dte": bool(remove_0dte_flag),
                "provider_listing": provider_listing,
                "solver_config": dict(profile.get("solver_config") or {}),
                "solver_profile_label": profile.get("solver_profile_label"),
                "net_gex_formula": profile.get("net_gex_formula"),
                "raw_signed_gamma_formula": profile.get("raw_signed_gamma_formula"),
                "error": profile_error,
            }
        pivot = pd.DataFrame(
            {
                "strike": profile["strikes"],
                "gex_calls": profile["gex_calls"],
                "gex_puts": profile["gex_puts"],
                "gex_net": profile["gex_net"],
                "gex_cumulative": profile["gex_cumulative"],
            }
        )
        job.progress = 1.0
        try:
            zero_gamma = profile["zero_gamma"]
            if zero_gamma is not None and math.isfinite(zero_gamma):
                job.log(f"GEX completed with {len(pivot)} strikes. Zero gamma≈{zero_gamma:.2f}")
            else:
                job.log(
                    "GEX completed with "
                    f"{len(pivot)} strikes. No zero gamma in tested range; "
                    f"total gamma at spot={float(profile['total_gamma_at_spot'] or 0.0):.2f}"
                )
        except Exception:
            job.log(f"GEX completed with {len(pivot)} strikes.")
        disp_exp = next_expiry_key
        if fmode == "zero_dte" and today_iso:
            disp_exp = today_iso
        expiry_label = (
            next(iter(requested_expiry_keys))
            if len(requested_expiry_keys) == 1
            else (
                f"{len(profile.get('included_expirations') or [])} expirations"
                if profile.get("included_expirations")
                else disp_exp
            )
        )
        return pivot, {
            "expiry": expiry_label,
            "spot": spot,
            "net_gex": float(profile["net_gex"]),
            "total_gamma_at_spot": float(profile["total_gamma_at_spot"] or 0.0),
            "raw_signed_gamma": float(profile["raw_signed_gamma"] or 0.0),
            "zero_gamma": profile["zero_gamma"],
            "gamma_regime": profile["gamma_regime"],
            "spot_vs_zero_gamma": profile["spot_vs_zero_gamma"],
            "zero_gamma_diagnostics": profile["zero_gamma_diagnostics"],
            "zero_gamma_curve": (
                profile["zero_gamma_diagnostics"].get("curve")
                if include_solver_curve
                else []
            ),
            "expiry_mode": fmode or "selected",
            "total_oi": float(profile["total_oi"]),
            "include_0dte": not remove_0dte_flag,
            "remove_0dte": bool(remove_0dte_flag),
            "included_expirations": list(profile.get("included_expirations") or []),
            "excluded_expirations": list(profile.get("excluded_expirations") or []),
            "provider_listing": provider_listing,
            "solver_config": dict(profile.get("solver_config") or {}),
            "solver_profile_label": profile.get("solver_profile_label"),
            "net_gex_formula": profile.get("net_gex_formula"),
            "raw_signed_gamma_formula": profile.get("raw_signed_gamma_formula"),
            "error": profile_error,
        }

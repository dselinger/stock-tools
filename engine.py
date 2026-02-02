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

# ------------------------------
# Config
# ------------------------------
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY", "")
# Allow overriding the API base for the Massive (formerly Polygon) endpoints.
MASSIVE_API_BASE = os.getenv("MASSIVE_API_BASE", "https://api.polygon.io")
CONTRACT_MULTIPLIER = 100
ALLOW_UNWEIGHTED_FALLBACK = os.getenv("ALLOW_UNWEIGHTED_FALLBACK", "0") == "1"


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
    client: httpx.AsyncClient, underlying: str, limit: int = 2000
) -> List[dict]:
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
    tries = 0
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date().isoformat()
    while True:
        if tries > 50:
            break
        params = {
            "underlying_ticker": underlying.upper(),
            "active": "true",
            "limit": min(limit, 1000),
            "sort": "expiration_date",
            "order": "asc",
            "expiration_date.gte": today,
        }
        if cursor:
            params["cursor"] = cursor
        data = await polygon_get(client, "/v3/reference/options/contracts", params)
        out.extend(data.get("results") or [])

        # Prefer explicit cursor fields when available
        next_cursor = data.get("next_url_cursor") or data.get("nextCursor")

        # Fallback: extract cursor from full next_url
        if not next_cursor:
            nu = data.get("next_url")
            if isinstance(nu, str) and nu:
                try:
                    from urllib.parse import parse_qs, urlparse

                    qs = parse_qs(urlparse(nu).query)
                    cvals = qs.get("cursor") or []
                    if cvals:
                        next_cursor = cvals[0]
                except Exception:
                    next_cursor = None

        cursor = next_cursor
        if not cursor or len(out) >= limit:
            break
        tries += 1
    return out


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
) -> tuple[pd.DataFrame, dict]:
    job.status = "running"
    job.progress = 0.03
    job.log(
        f"Compute {symbol} spot={spot:.2f} ±{pct_window*100:.2f}% window; next_expiry={only_next_expiry}; weight={weight_mode}; emode={(expiry_mode or 'selected')}"
    )

    lo, hi = spot * (1 - pct_window), spot * (1 + pct_window)
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
            disp_exp = next_expiry_key
            if fmode == "zero_dte" and today_iso:
                disp_exp = today_iso
            elif fmode == "exclude_selected" and expiry_override:
                disp_exp = f"except {expiry_override}"
            return df, {"expiry": disp_exp, "spot": spot}
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
        disp_exp = next_expiry_key
        if fmode == "zero_dte" and today_iso:
            disp_exp = today_iso
        elif fmode == "exclude_selected" and expiry_override:
            disp_exp = f"except {expiry_override}"
        return pivot.reset_index(), {
            "expiry": disp_exp,
            "spot": spot,
            "stats": stats,
            "expiry_mode": fmode or "selected",
        }


async def compute_gex_for_ticker(
    job: Job,
    symbol: str,
    spot: float,
    pct_window: float = 0.10,
    only_next_expiry: bool = True,
    expiry_mode: str = "",
    expiry_override: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Compute Net GEX by strike using Massive (Polygon) snapshots.

    GEX per contract ≈ gamma * weight * CONTRACT_MULTIPLIER * S^2,
    with weight = open interest (contracts). Calls contribute +, puts -.
    """
    job.status = "running"
    job.progress = 0.03
    job.log(
        f"Compute GEX {symbol} spot={spot:.2f} ±{pct_window*100:.2f}% window; next_expiry={only_next_expiry}; emode={(expiry_mode or 'selected')}"
    )

    lo, hi = spot * (1 - pct_window), spot * (1 + pct_window)
    async with httpx.AsyncClient() as client:
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

        # Nearest expiry by metadata only
        next_expiry_key = expiry_override
        if not next_expiry_key and only_next_expiry:
            from datetime import datetime, timezone

            now = time.time()
            best_T = float("inf")
            for c in contracts:
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
            job.log(f"Next expiry derived quickly: {next_expiry_key or 'unknown'}")

        # Normalize expiry keys for consistent comparisons
        expiry_override_key = (
            expiry_override[:10] if isinstance(expiry_override, str) else expiry_override
        )
        next_expiry_key_norm = (
            next_expiry_key[:10] if isinstance(next_expiry_key, str) else next_expiry_key
        )
        # Candidates within window and (optionally) next expiry
        candidates = []
        try:
            from datetime import datetime, timezone

            today_iso = datetime.now(timezone.utc).date().isoformat()
        except Exception:
            today_iso = ""
        fmode = (expiry_mode or "").strip().lower()
        for c in contracts:
            tkr = c.get("ticker") or c.get("symbol") or c.get("option_ticker")
            if not tkr:
                continue
            try:
                k = float(c.get("strike_price") or c.get("strikePrice") or c.get("strike") or 0)
            except Exception:
                continue
            if not (lo <= k <= hi):
                continue
            exp_raw = c.get("expiration_date") or c.get("expirationDate") or c.get("exp_date") or ""
            exp = exp_raw[:10] if isinstance(exp_raw, str) else exp_raw
            # Apply expiry filter precedence
            if fmode == "zero_dte":
                if today_iso and exp[:10] != today_iso:
                    continue
            elif fmode == "exclude_selected" and expiry_override_key:
                if exp == expiry_override_key:
                    continue
            elif next_expiry_key_norm and exp != next_expiry_key_norm:
                continue
            typ_raw = (c.get("contract_type") or c.get("type") or "").lower()
            opt_type = (
                "call"
                if typ_raw.startswith("c")
                else (
                    "put" if typ_raw.startswith("p") else ("call" if "C" in (tkr or "") else "put")
                )
            )
            candidates.append((tkr, k, exp, opt_type))

        job.log(f"Prefiltered to {len(candidates)} in-window contracts before GEX snapshots")

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

        rows = []
        # Keep raw contract inputs to compute a proper flip level later
        raw_items: list[tuple[float, float, float, float, float]] = []  # (K, iv, T, oi, sgn)
        total_oi = 0.0
        done = 0
        total = max(len(candidates), 1)
        for tkr, k, exp, opt_type in candidates:
            if job.cancel_event.is_set():
                job.status = "cancelled"
                job.log("Cancelled during GEX snapshots")
                raise asyncio.CancelledError()
            snap = await fetch_snap_gex(tkr, k, exp, opt_type)
            oi = float(snap.get("oi") or 0.0)
            iv = float(snap.get("iv") or 0.25)
            try:
                total_oi += max(oi, 0.0)
            except Exception:
                pass
            # time to expiry
            try:
                from datetime import datetime, timezone

                if len(exp) >= 10:
                    dt = datetime.fromisoformat(exp[:10]).replace(tzinfo=timezone.utc)
                    T = max((dt.timestamp() - time.time()) / (365 * 24 * 3600), 1 / 365)
                else:
                    T = 30 / 365
            except Exception:
                T = 30 / 365
            gamma = bs_gamma(spot, k, T, 0.0, 0.0, iv)
            sgn = 1.0 if opt_type == "call" else -1.0
            gex = sgn * gamma * oi * CONTRACT_MULTIPLIER * (spot * spot)
            rows.append({"strike": k, "option_type": opt_type, "gex": gex})
            # Save raw components for a spot‑dependent root solve later
            try:
                raw_items.append((float(k), float(iv), float(T), float(max(oi, 0.0)), float(sgn)))
            except Exception:
                pass
            done += 1
            if done % 10 == 0:
                job.progress = min(0.1 + 0.85 * (done / total), 0.95)

        df = pd.DataFrame(rows)
        if df.empty:
            job.progress = 1.0
            disp_exp = next_expiry_key
            if fmode == "zero_dte" and today_iso:
                disp_exp = today_iso
            return df, {"expiry": disp_exp, "spot": spot}
        agg = df.groupby(["strike", "option_type"], as_index=False)["gex"].sum()
        pivot = agg.pivot(index="strike", columns="option_type", values="gex").fillna(0.0)
        pivot = pivot.rename(columns={"call": "gex_calls", "put": "gex_puts"})
        pivot["gex_net"] = pivot.get("gex_calls", 0.0) + pivot.get("gex_puts", 0.0)
        pivot = pivot.sort_index()
        # Compute a more principled flip: price S where total net GEX(S) == 0.
        # If we cannot bracket a root, fall back to the cumulative-crossing proxy.
        flip = None
        try:
            import numpy as _np

            def net_gex_at(S: float) -> float:
                # Sum over all contracts using current candidate price S
                tot = 0.0
                SS = S * S
                for K, iv, T, oi, sgn in raw_items:
                    if oi <= 0 or iv <= 0 or T <= 0:
                        continue
                    try:
                        g = bs_gamma(S, K, T, 0.0, 0.0, iv)
                        tot += sgn * g * oi * CONTRACT_MULTIPLIER * SS
                    except Exception:
                        continue
                return float(tot)

            # Define a search window around observed strikes; widen modestly
            if len(raw_items) >= 2:
                strikes = _np.array([it[0] for it in raw_items], dtype=float)
                s_min = float(_np.nanmin(strikes))
                s_max = float(_np.nanmax(strikes))
            else:
                s_min = float(spot * max(0.5, 1 - 2 * pct_window))
                s_max = float(spot * (1 + 2 * pct_window))
            lo_s = min(s_min, spot) * 0.98
            hi_s = max(s_max, spot) * 1.02
            if not math.isfinite(lo_s) or not math.isfinite(hi_s) or hi_s <= lo_s:
                lo_s, hi_s = spot * (1 - pct_window), spot * (1 + pct_window)

            # Sample the function to find brackets where it changes sign
            grid = _np.linspace(lo_s, hi_s, 121)
            vals = _np.array([net_gex_at(float(s)) for s in grid], dtype=float)

            # Prefer a root nearest to the current spot
            sign_changes: list[tuple[int, float]] = []
            for i in range(1, len(grid)):
                a, b = vals[i - 1], vals[i]
                if not (_np.isfinite(a) and _np.isfinite(b)):
                    continue
                if (a == 0) or (b == 0) or (a < 0) != (b < 0):
                    # bracket index and mid distance to spot for ordering
                    mid = 0.5 * (grid[i - 1] + grid[i])
                    sign_changes.append((i, abs(mid - spot)))

            def refine_root(a_s: float, b_s: float) -> float:
                fa, fb = net_gex_at(a_s), net_gex_at(b_s)
                # bisection with a few iterations; guard for identical signs
                if fa == 0:
                    return float(a_s)
                if fb == 0:
                    return float(b_s)
                if fa * fb > 0:
                    # no bracket; return linear interpolation guess
                    t = 0.5
                    return float(a_s + t * (b_s - a_s))
                lo, hi = a_s, b_s
                for _ in range(20):
                    mid = 0.5 * (lo + hi)
                    fm = net_gex_at(mid)
                    if fm == 0 or abs(hi - lo) < 1e-4:
                        return float(mid)
                    if (fa < 0) != (fm < 0):
                        hi, fb = mid, fm
                    else:
                        lo, fa = mid, fm
                return float(0.5 * (lo + hi))

            if sign_changes:
                # Pick the bracket whose midpoint is closest to spot
                sign_changes.sort(key=lambda t: t[1])
                idx = sign_changes[0][0]
                flip = refine_root(float(grid[idx - 1]), float(grid[idx]))
            else:
                # Fallback: choose point with minimal |net_gex_at(S)| in grid
                j = int(_np.argmin(_np.abs(vals))) if len(vals) else None
                if j is not None and len(grid):
                    flip = float(grid[int(j)])

            # If the above failed, fall back to the original cumulative-crossing proxy
            if flip is None or not math.isfinite(flip):
                xs = _np.array(pivot.index.tolist(), dtype=float)
                yn = _np.array(pivot["gex_net"].tolist(), dtype=float)
                csum = _np.cumsum(yn)
                for i in range(1, len(xs)):
                    if csum[i - 1] <= 0 <= csum[i] or csum[i - 1] >= 0 >= csum[i]:
                        x0, x1 = xs[i - 1], xs[i]
                        y0, y1 = csum[i - 1], csum[i]
                        if (y1 - y0) != 0:
                            t = -y0 / (y1 - y0)
                            flip = float(x0 + t * (x1 - x0))
                        else:
                            flip = float(x0)
                        break
        except Exception:
            # Keep None; UI will just omit the line
            pass
        job.progress = 1.0
        try:
            if flip is not None and math.isfinite(flip):
                job.log(f"GEX completed with {len(pivot)} strikes. Flip≈{flip:.2f}")
            else:
                job.log(f"GEX completed with {len(pivot)} strikes. Flip unavailable")
        except Exception:
            job.log(f"GEX completed with {len(pivot)} strikes.")
        disp_exp = next_expiry_key
        if fmode == "zero_dte" and today_iso:
            disp_exp = today_iso
        return pivot.reset_index(), {
            "expiry": disp_exp,
            "spot": spot,
            "gex_flip": flip,
            "expiry_mode": fmode or "selected",
            "total_oi": float(total_oi),
        }

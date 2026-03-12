from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
import pandas as pd
from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from core.cache import (
    cache_key,
    disk_cache_get,
    disk_cache_set,
)
from core.demo_data import demo_expiries, demo_gex_result, demo_scanner_row, demo_vanna_result
from core.gamma_math import (
    canonicalize_gex_payload,
    derive_gamma_solver_confidence,
    expiration_scope_expirations,
    has_next_trading_day_expiration,
    has_same_day_expiration,
    is_standard_monthly_expiration,
    monthly_expiration_dates,
    gamma_solver_cache_token,
    gamma_solver_profile_label,
    normalize_gamma_solver_config,
    normalize_expiration_scope,
    next_monthly_expiration,
    next_trading_day_expiration,
    scanner_scope_expirations,
    spot_vs_zero_gamma_label,
    spot_vs_zero_gamma_pct,
    term_shape_anchor_expirations,
)
from core.web import (
    FALLBACK_FAVS,
    FAVICON_SVG,
    SCANNER_DEFAULTS,
    render_template,
    scanner_max_workers,
)
from engine import (
    Job,
    compute_gex_for_ticker,
    compute_vanna_for_ticker,
    event_log,
    fetch_spot_yahoo,
    job_manager,
    polygon_get,
)
from engine import (
    fetch_spot as fetch_spot_polygon,
)
from routes.debug import router as debug_router
from routes.events import router as events_router
from session_utils import SESSION_COOKIE, get_session_id, signer

router = APIRouter()
router.include_router(debug_router)
router.include_router(events_router)

DEMO_MODE_DEFAULT = os.getenv("DEMO_MODE", "0") == "1"
DEMO_MODE_COOKIE = "demo_mode"
EXPIRY_CACHE_TTL_SEC = max(30, int(os.getenv("EXPIRY_CACHE_TTL_SEC", "300")))
EXPIRY_LIST_TIMEOUT_SEC = max(8.0, float(os.getenv("EXPIRY_LIST_TIMEOUT_SEC", "25")))
EXPIRY_LIST_MAX_PAGES = max(8, int(os.getenv("EXPIRY_LIST_MAX_PAGES_PER_UNDERLYING", "48")))
EXPIRY_LIST_TARGET_COUNT = max(8, int(os.getenv("EXPIRY_LIST_TARGET_COUNT", "24")))
EXPIRY_LIST_MAX_COUNT = max(8, int(os.getenv("EXPIRY_LIST_MAX_COUNT", "128")))
_EXPIRY_CACHE: dict[str, dict[str, Any]] = {}
EXPIRATION_SCOPE_CHOICES = ("0dte", "1dte", "weekly", "monthly", "m1", "m2", "all")
GEX_CALC_VERSION = "gamma-v5"


def _canonicalize_gex_result(payload: dict) -> dict:
    """Keep all GEX views and caches aligned on the same canonical gamma series."""
    return canonicalize_gex_payload(payload)


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _build_expiry_scope_metadata(expiries: list[str], *, today_iso: str | None = None) -> dict[str, Any]:
    today_key = today_iso or _today_iso()
    monthly = monthly_expiration_dates(expiries, today_iso=today_key)
    scope_support = {
        "0dte": has_same_day_expiration(expiries, today_iso=today_key),
        "1dte": has_next_trading_day_expiration(expiries, today_iso=today_key),
        "weekly": bool(expiration_scope_expirations(expiries, "weekly", today_iso=today_key)),
        "monthly": bool(monthly),
        "m1": next_monthly_expiration(expiries, index=1, today_iso=today_key) is not None,
        "m2": next_monthly_expiration(expiries, index=2, today_iso=today_key) is not None,
        "all": bool(expiries),
    }
    return {
        "monthly_expirations": monthly,
        "scope_support": scope_support,
        "scope_expirations": {
            scope: expiration_scope_expirations(expiries, scope, today_iso=today_key)
            for scope in EXPIRATION_SCOPE_CHOICES
        },
        "next_trading_day_expiration": next_trading_day_expiration(expiries, today_iso=today_key),
    }


def _resolve_requested_expiration_scope(
    available_expiries: list[str],
    *,
    requested_scope: str,
    selected_expiry: str | None = None,
    selected_expirations: list[str] | None = None,
    remove_0dte: bool = False,
    today_iso: str | None = None,
) -> dict[str, Any]:
    today_key = today_iso or _today_iso()
    available = [str(exp)[:10] for exp in (available_expiries or []) if exp]
    scope_key = normalize_expiration_scope(requested_scope, default="selected")
    explicit_expiry = str(selected_expiry)[:10] if selected_expiry else None
    manual_set = [str(exp)[:10] for exp in (selected_expirations or []) if exp]
    manual_set = [exp for exp in manual_set if exp in set(available)]
    if scope_key == "selected":
        chosen = explicit_expiry if explicit_expiry in set(available) else (available[0] if available else None)
        return {
            "scope": "selected",
            "expiry_override": chosen,
            "allowed_expiries": [chosen] if chosen else [],
            "supported": chosen is not None,
            "remove_0dte": bool(remove_0dte),
        }
    if scope_key == "all":
        allowed = manual_set if manual_set else list(available)
    else:
        allowed = expiration_scope_expirations(available, scope_key, today_iso=today_key)
    if remove_0dte and scope_key not in {"selected", "0dte"}:
        allowed = [exp for exp in allowed if exp != today_key]
    return {
        "scope": scope_key,
        "expiry_override": None,
        "allowed_expiries": allowed,
        "supported": bool(allowed),
        "remove_0dte": bool(remove_0dte),
    }


def _apply_expiration_meta(
    meta: dict[str, Any] | None,
    *,
    scope: str,
    available_expiries: list[str],
    selected_expirations: list[str],
    selected_expiry: str | None,
    remove_0dte: bool,
) -> dict[str, Any]:
    clean = dict(meta or {})
    diagnostics = dict(clean.get("zero_gamma_diagnostics") or {})
    clean["expiry_mode"] = scope
    clean["selected_expirations"] = list(selected_expirations)
    clean["available_expirations"] = list(available_expiries)
    clean["remove_0dte"] = bool(remove_0dte)
    clean["monthly_expirations"] = monthly_expiration_dates(available_expiries, today_iso=_today_iso())
    clean["expiry_is_monthly"] = bool(clean.get("expiry") and is_standard_monthly_expiration(clean.get("expiry")))
    if scope == "selected":
        clean["selected_expiry"] = selected_expiry
    diagnostics["selected_scope"] = scope
    diagnostics["selected_expiry"] = selected_expiry
    diagnostics["selected_expiration_set"] = list(selected_expirations)
    diagnostics["available_expirations"] = list(available_expiries)
    diagnostics["remove_0dte"] = bool(remove_0dte)
    clean["zero_gamma_diagnostics"] = diagnostics
    return clean


def _scanner_spot_density(raw: dict[str, Any], spot: float | None) -> float | None:
    if not isinstance(raw, dict) or spot is None:
        return None
    try:
        spot_value = float(spot)
    except Exception:
        return None
    strikes = [float(value) for value in (raw.get("strikes") or [])]
    gex_net = [float(value) for value in (raw.get("gex_net") or [])]
    if not strikes or len(strikes) != len(gex_net):
        return None
    total_abs = sum(abs(value) for value in gex_net)
    if total_abs <= 0:
        return None
    local_band = max(abs(spot_value) * 0.02, 1.0)
    local_abs = sum(
        abs(gex_net[idx])
        for idx in range(len(strikes))
        if abs(strikes[idx] - spot_value) <= local_band
    )
    if local_abs <= 0:
        nearest = sorted(
            range(len(strikes)),
            key=lambda idx: (abs(strikes[idx] - spot_value), strikes[idx]),
        )[:6]
        local_abs = sum(abs(gex_net[idx]) for idx in nearest)
    return max(0.0, min(100.0, (local_abs / total_abs) * 100.0))


def _term_shape_bias_interpretation(bias: float | None) -> str:
    if bias is None:
        return "Unavailable"
    if bias >= 0.2:
        return "Front-loaded"
    if bias <= -0.2:
        return "Back-loaded"
    return "Balanced"


def _compute_term_shape_bias(anchor_values: dict[str, float | None]) -> float | None:
    w1 = anchor_values.get("w1")
    m1 = anchor_values.get("m1")
    m2 = anchor_values.get("m2")
    if w1 is not None:
        back = [value for value in (m1, m2) if value is not None]
        if not back:
            return None
        back_value = sum(back) / len(back)
        denom = abs(w1) + abs(back_value)
        if denom <= 0:
            return None
        return max(-1.0, min(1.0, (w1 - back_value) / denom))
    if m1 is not None and m2 is not None:
        denom = abs(m1) + abs(m2)
        if denom <= 0:
            return None
        return max(-1.0, min(1.0, (m1 - m2) / denom))
    return None


def _empty_term_shape() -> dict[str, Any]:
    return {
        "bias": None,
        "anchors": [
            {"anchor": "W1", "expiry": None, "value": None, "monthly": False, "applicable": False},
            {"anchor": "M1", "expiry": None, "value": None, "monthly": True, "applicable": False},
            {"anchor": "M2", "expiry": None, "value": None, "monthly": True, "applicable": False},
        ],
        "interpretation": "Unavailable",
    }


async def _scanner_term_shape(
    symbol: str,
    spot: float,
    pct_window: float,
    *,
    available_expiries: list[str],
    remove_0dte: bool,
    parent_job_id: Optional[str] = None,
) -> dict[str, Any]:
    available = [str(exp)[:10] for exp in (available_expiries or []) if exp]
    anchors = term_shape_anchor_expirations(available)
    anchor_rows: list[dict[str, Any]] = []
    anchor_values: dict[str, float | None] = {"w1": None, "m1": None, "m2": None}
    for anchor_name in ("w1", "m1", "m2"):
        expiry = anchors.get(anchor_name)
        if not expiry:
            anchor_rows.append(
                {
                    "anchor": anchor_name.upper(),
                    "expiry": None,
                    "value": None,
                    "monthly": anchor_name in {"m1", "m2"},
                    "applicable": False,
                }
            )
            continue
        summary = await _compute_gex_cached_summary(
            symbol,
            spot,
            pct_window,
            expiry_key=expiry,
            expiry_mode="all",
            next_only=False,
            remove_0dte=remove_0dte,
            allowed_expiries=[expiry],
            label=f"term_shape_{anchor_name}",
            parent_job_id=parent_job_id,
        )
        value = summary.get("net_gex")
        try:
            anchor_values[anchor_name] = float(value) if value is not None else None
        except Exception:
            anchor_values[anchor_name] = None
        anchor_rows.append(
            {
                "anchor": anchor_name.upper(),
                "expiry": expiry,
                "value": anchor_values[anchor_name],
                "monthly": bool(expiry and is_standard_monthly_expiration(expiry)),
                "applicable": True,
            }
        )
    bias = _compute_term_shape_bias(anchor_values)
    return {
        "bias": bias,
        "anchors": anchor_rows,
        "interpretation": _term_shape_bias_interpretation(bias),
    }


def _payload_solver_config(payload: dict | None) -> dict:
    raw = (payload or {}).get("solver_config")
    return normalize_gamma_solver_config(raw if isinstance(raw, dict) else None)


def _payload_remove_0dte(payload: dict | None, *, default: bool = False) -> bool:
    data = payload or {}
    if "remove_0dte" in data:
        return bool(data.get("remove_0dte"))
    if "include_0dte" in data:
        return not bool(data.get("include_0dte"))
    return bool(default)


def get_demo_mode(request: Request | None = None) -> bool:
    """Demo mode can be toggled via cookie; fallback to env default."""
    cookie_val = None
    try:
        if request and request.cookies:
            cookie_val = request.cookies.get(DEMO_MODE_COOKIE)
    except Exception:
        cookie_val = None
    if cookie_val is not None:
        return str(cookie_val).strip().lower() in {"1", "true", "yes", "on"}
    return DEMO_MODE_DEFAULT


async def _cache_metric(job_id: Optional[str], key: str):
    """Increment per-job cache metrics for debugging."""
    if not job_id:
        return
    try:
        job = await job_manager.get(job_id)
        if not job:
            return
        job.cache_metrics[key] = job.cache_metrics.get(key, 0) + 1
    except Exception:
        pass


@router.get("/favicon.svg")
async def favicon_svg():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")


@router.get("/favicon.ico")
async def favicon_ico():
    # Redirect to SVG favicon (modern browsers support SVG favicons).
    # If you prefer a real .ico later, we can swap this to serve bytes.
    return RedirectResponse(url="/favicon.svg")


@router.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        await event_log.add("page", "/", "home")
    except Exception:
        pass
    import json as _json

    favs_json = _json.dumps(FALLBACK_FAVS)
    return render_template(
        "home.html",
        request,
        {
            "title": "Dave's Stock Tools",
            "favs_json": favs_json,
            "demo_mode_enabled": get_demo_mode(request),
            "demo_mode_default": DEMO_MODE_DEFAULT,
        },
    )


@router.get("/scanner", response_class=HTMLResponse)
async def scanner_page(request: Request):
    try:
        await event_log.add("page", "/scanner", "open")
    except Exception:
        pass
    return render_template(
        "scanner.html", request, {"title": "Gamma Scanner", "defaults_json": SCANNER_DEFAULTS}
    )


@router.get("/ticker", response_class=HTMLResponse)
async def ticker_page(request: Request, symbol: Optional[str] = Query(default=None)):
    return await _render_ticker(request, symbol)


@router.get("/ticker/{symbol}", response_class=HTMLResponse)
async def ticker_page_path(request: Request, symbol: str = Path(...)):
    return await _render_ticker(request, symbol)


async def _render_ticker(request: Request, symbol: Optional[str]):
    sym = (symbol or "").upper()
    try:
        await event_log.add("page", "/ticker", "open", {"symbol": sym})
    except Exception:
        pass
    return render_template(
        "ticker.html", request, {"title": f"{sym or 'Ticker'} - Vanna", "symbol": sym}
    )


@router.get("/gexticker", response_class=HTMLResponse)
async def gex_ticker_page(request: Request, symbol: Optional[str] = Query(default=None)):
    return await _render_gex_ticker(request, symbol)


@router.get("/gexticker/{symbol}", response_class=HTMLResponse)
async def gex_ticker_page_path(request: Request, symbol: str = Path(...)):
    return await _render_gex_ticker(request, symbol)


async def _render_gex_ticker(request: Request, symbol: Optional[str]):
    sym = (symbol or "").upper()
    try:
        await event_log.add("page", "/gexticker", "open", {"symbol": sym})
    except Exception:
        pass
    return render_template(
        "gex.html", request, {"title": f"{sym or 'Ticker'} - GEX", "symbol": sym}
    )


@router.get("/api/demo-mode")
async def api_get_demo_mode(request: Request):
    return {"enabled": get_demo_mode(request), "default": DEMO_MODE_DEFAULT}


@router.post("/api/demo-mode")
async def api_set_demo_mode(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    enabled = bool(payload.get("enabled"))
    resp = JSONResponse({"enabled": enabled})
    resp.set_cookie(
        DEMO_MODE_COOKIE, "1" if enabled else "0", max_age=60 * 60 * 24 * 90, samesite="lax"
    )
    try:
        await event_log.add("api", "/api/demo-mode", "set", {"enabled": enabled})
    except Exception:
        pass
    return resp


# ------------------------------
# API
# ------------------------------
@router.post("/api/start")
async def api_start(request: Request):
    payload = await request.json()
    symbol = (payload.get("symbol") or "").upper().strip()
    if not symbol:
        raise HTTPException(400, detail="Missing symbol")
    spot_override = payload.get("spot_override")
    next_only = bool(payload.get("next_only", True))
    try:
        pct_window = float(payload.get("pct_window", 0.10))
    except Exception:
        pct_window = 0.10
    expiry_sel = payload.get("expiry") or None
    expiry_mode = normalize_expiration_scope(payload.get("expiry_mode"), default="selected")
    selected_expirations = payload.get("selected_expirations")
    if not isinstance(selected_expirations, list):
        selected_expirations = None
    selected_expirations = [str(exp)[:10] for exp in (selected_expirations or []) if exp]
    demo_mode = get_demo_mode(request)
    available_expiries = demo_expiries(symbol) if demo_mode else await _list_expiry_dates(symbol)
    resolved_scope = _resolve_requested_expiration_scope(
        available_expiries,
        requested_scope=expiry_mode,
        selected_expiry=expiry_sel,
        selected_expirations=selected_expirations if expiry_mode == "all" else None,
        remove_0dte=False,
    )
    effective_scope = resolved_scope["scope"]
    effective_expiry = resolved_scope["expiry_override"]
    effective_expiries = list(resolved_scope["allowed_expiries"] or [])
    if not resolved_scope["supported"]:
        raise HTTPException(400, detail=f"No expirations available for {effective_scope.upper()}")
    # Session: avoid redirect here; set cookie inline if missing
    sid = get_session_id(request)
    set_cookie = False
    if not sid:
        import uuid

        token = signer.sign(uuid.uuid4().hex).decode("utf-8")
        sid = token
        set_cookie = True
    if demo_mode:
        job = await job_manager.create(sid)
        job.status = "done"
        job.progress = 1.0
        job.result = demo_vanna_result(symbol, pct_window)
        try:
            meta = _apply_expiration_meta(
                job.result.get("meta") or {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=False,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            meta["job_id"] = job.job_id
            job.result["meta"] = meta
        except Exception:
            pass
        resp = JSONResponse({"ok": True, "job_id": job.job_id, "demo": True})
        if set_cookie:
            resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
        resp.set_cookie(
            DEMO_MODE_COOKIE, "1" if demo_mode else "0", max_age=60 * 60 * 24 * 90, samesite="lax"
        )
        return resp
    # Server-side cache + dedupe
    try:
        await event_log.add(
            "api",
            "/api/start",
            "request",
            {"symbol": symbol, "expiry": effective_expiry, "next_only": next_only, "scope": effective_scope},
        )
    except Exception:
        pass
    ttl = int(os.getenv("SERVER_CACHE_TTL_SEC", "300"))
    key = cache_key(
        mode="v",
        symbol=symbol,
        pct_window=pct_window,
        next_only=(False if effective_expiry else next_only),
        expiry=effective_expiry,
        weight=(payload.get("weight") or "oi"),
        spot_override=(str(spot_override).strip() if spot_override else None),
        expiry_mode=effective_scope,
        expiry_filter=",".join(sorted(effective_expiries)),
    )
    # 1) Attach to a running identical job
    running = await job_manager.get_running_by_key(key)
    if running:
        running.log(f"Attach: another session joined this run for {symbol} (key)")
        return JSONResponse({"ok": True, "job_id": running.job_id})

    # 2) Serve from server cache if still warm
    cached = job_manager.cache_get(key, ttl)
    if cached is not None:
        job = await job_manager.create(sid)
        job.status = "done"
        job.progress = 1.0
        job.result = dict(cached or {})
        if isinstance(job.result.get("meta"), dict):
            meta = _apply_expiration_meta(
                job.result.get("meta") or {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=False,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            job.result["meta"] = meta
        job.log(f"Cache hit (server, ttl={ttl}s) for {symbol}")
        try:
            await event_log.add(
                "api",
                "/api/start",
                "cache_hit",
                {
                    "symbol": symbol,
                    "expiry": expiry_sel,
                    "next_only": next_only,
                    "ttl": ttl,
                    "key": key,
                    "job_id": job.job_id,
                },
                status="success",
            )
        except Exception:
            pass
        resp = JSONResponse({"ok": True, "job_id": job.job_id})
        if set_cookie:
            resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
        return resp

    # 3) Create a fresh job
    job = await job_manager.create(sid)
    job.status = "queued"
    job.progress = 0.01
    job.log(f"Job {job.job_id} created for {symbol} (next_only={next_only}).")
    await job_manager.register_running_key(key, job)

    async def run():
        try:
            try:
                await event_log.add(
                    "job",
                    "/api/start",
                    "start",
                    {
                        "symbol": symbol,
                        "expiry": effective_expiry,
                        "next_only": next_only,
                        "pct_window": pct_window,
                        "scope": effective_scope,
                    },
                    status="info",
                )
            except Exception:
                pass
            # Resolve spot
            if spot_override:
                try:
                    spot = float(str(spot_override).replace(",", "").strip())
                    job.log(f"Using user spot override: {spot}")
                except Exception:
                    job.log("Invalid spot override; falling back to auto.")
                    spot = None
            else:
                spot = None
            async with httpx.AsyncClient() as client:
                if spot is None:
                    job.log("Auto-detecting spot from Yahoo…")
                    sp = fetch_spot_yahoo(symbol)
                    if sp is None:
                        # Add diagnostics for Yahoo failures to help debug env/setup
                        try:
                            import importlib.metadata as _md
                            import importlib.util as _util

                            have = _util.find_spec("yfinance") is not None
                            if not have:
                                job.log("Yahoo: yfinance not installed in server environment")
                            else:
                                try:
                                    ver = _md.version("yfinance")
                                except Exception:
                                    ver = "unknown"
                                job.log(f"Yahoo: yfinance present (v{ver}) but returned no price")
                        except Exception:
                            pass
                        job.log("Yahoo failed; trying Massive (Polygon)…")
                        sp = await fetch_spot_polygon(client, symbol)
                    if sp is None:
                        job.log("Could not auto-detect spot; please provide override.")
                        job.status = "error"
                        job.progress = 1.0
                        return
                    spot = sp
                    job.log(f"Detected spot {spot:.2f}")
            # Try after-hours and pre-market (Yahoo), best-effort
            spot_ah = None
            spot_pm = None
            try:
                import yfinance as _yf  # type: ignore

                _map = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "DJI": "^DJI"}
                ysym = _map.get(symbol.upper(), symbol)
                _t = _yf.Ticker(ysym)
                val = None
                try:
                    fi = getattr(_t, "fast_info", None)
                    if fi:
                        for k in ("post_market_price", "postMarketPrice", "post_market_last_price"):
                            v = getattr(fi, k, None)
                            if v is not None:
                                val = v
                                break
                        if val is None and hasattr(fi, "get"):
                            for k in ("postMarketPrice", "post_market_price"):
                                v = fi.get(k)
                                if v is not None:
                                    val = v
                                    break
                except Exception:
                    val = None
                if val is None:
                    try:
                        inf = _t.info  # type: ignore[attr-defined]
                        if isinstance(inf, dict):
                            val = inf.get("postMarketPrice")
                    except Exception:
                        val = None
                if val is not None:
                    spot_ah = float(val)
                # pre-market
                try:
                    pmv = None
                    fi = getattr(_t, "fast_info", None)
                    if fi:
                        for k in ("pre_market_price", "preMarketPrice", "pre_market_last_price"):
                            v = getattr(fi, k, None)
                            if v is not None:
                                pmv = v
                                break
                        if pmv is None and hasattr(fi, "get"):
                            for k in ("preMarketPrice", "pre_market_price"):
                                v = fi.get(k)
                                if v is not None:
                                    pmv = v
                                    break
                    if pmv is None:
                        try:
                            inf = _t.info  # type: ignore[attr-defined]
                            if isinstance(inf, dict):
                                pmv = inf.get("preMarketPrice")
                        except Exception:
                            pmv = None
                    if pmv is not None:
                        spot_pm = float(pmv)
                except Exception:
                    spot_pm = None
            except Exception:
                spot_ah = None
            job.log(f"Using window ±{pct_window*100:.2f}% around spot")
            df, meta = await compute_vanna_for_ticker(
                job,
                symbol,
                spot,
                pct_window=pct_window,
                only_next_expiry=(False if effective_expiry else next_only),
                expiry_override=effective_expiry,
                weight_mode=(payload.get("weight") or "oi"),
                expiry_mode=("selected" if effective_scope == "selected" else "all"),
                allowed_expiries=(effective_expiries if effective_scope != "selected" else None),
            )
            meta = _apply_expiration_meta(
                meta if isinstance(meta, dict) else {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=False,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            if job.cancel_event.is_set():
                job.status = "cancelled"
                return
            if df is None or df.empty:
                job.result = {
                    "strikes": [],
                    "vanna_net": [],
                    "vanna_calls": [],
                    "vanna_puts": [],
                    "meta": meta,
                }
                job.status = "done"
                job.progress = 1.0
                return
            strikes = df["strike"].astype(float).round(2).tolist()
            vnet = df["vanna_net"].astype(float).tolist()
            vc = (
                (df["vanna_calls"] if "vanna_calls" in df.columns else pd.Series([0.0] * len(df)))
                .astype(float)
                .tolist()
            )
            vp = (
                (df["vanna_puts"] if "vanna_puts" in df.columns else pd.Series([0.0] * len(df)))
                .astype(float)
                .tolist()
            )
            # add prev_close via Massive (Polygon) prev bar
            prev_close = None
            try:
                async with httpx.AsyncClient() as _client2:
                    dprev = await polygon_get(
                        _client2, f"/v2/aggs/ticker/{symbol.upper()}/prev", {}
                    )
                    arr = (dprev or {}).get("results") or []
                    if arr:
                        prev_close = arr[0].get("c")
            except Exception:
                prev_close = None
            # derive rth_close and prev_close_yest from Yahoo daily history (best-effort)
            rth_close = None
            prev_close_yest = None
            try:
                import yfinance as _yf  # type: ignore

                _map = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "DJI": "^DJI"}
                ysym = _map.get(symbol.upper(), symbol)
                _t = _yf.Ticker(ysym)
                hd = _t.history(period="5d")
                if hasattr(hd, "empty") and not hd.empty:
                    closes = hd.get("Close")
                    if closes is not None:
                        closes = closes.dropna()
                        if len(closes) >= 1:
                            rth_close = float(closes.iloc[-1])
                        if len(closes) >= 2:
                            prev_close_yest = float(closes.iloc[-2])
            except Exception:
                pass
            job.result = {
                "strikes": strikes,
                "vanna_net": vnet,
                "vanna_calls": vc,
                "vanna_puts": vp,
                "meta": {
                    "expiry": meta.get("expiry"),
                    "spot": float(spot),
                    **({"spot_ah": float(spot_ah)} if spot_ah is not None else {}),
                    **({"spot_pm": float(spot_pm)} if spot_pm is not None else {}),
                    **({"prev_close": float(prev_close)} if prev_close is not None else {}),
                    **({"rth_close": float(rth_close)} if rth_close is not None else {}),
                    **(
                        {"prev_close_yest": float(prev_close_yest)}
                        if prev_close_yest is not None
                        else {}
                    ),
                    **({"stats": meta.get("stats")} if meta.get("stats") else {}),
                    "expiry_mode": meta.get("expiry_mode"),
                    "selected_expirations": list(meta.get("selected_expirations") or []),
                    "available_expirations": list(meta.get("available_expirations") or []),
                    "monthly_expirations": list(meta.get("monthly_expirations") or []),
                    "expiry_is_monthly": bool(meta.get("expiry_is_monthly")),
                    "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": symbol,
                    "job_id": job.job_id,
                },
            }
            job.status = "done"
            job.progress = 1.0
            # server cache set
            try:
                job_manager.cache_set(key, job.result)
            except Exception:
                pass
            try:
                await event_log.add(
                    "job",
                    "/api/start",
                    "done",
                    {
                        "symbol": symbol,
                        "expiry": meta.get("expiry"),
                        "job_id": job.job_id,
                        "n_strikes": len(strikes),
                    },
                    status="success",
                )
            except Exception:
                pass
            try:
                await event_log.add(
                    "job", "/api/start", "done", {"symbol": symbol, "job_id": job.job_id}
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            job.status = "cancelled"
        except Exception as e:  # noqa: BLE001
            job.log(f"FATAL: {type(e).__name__}: {e}")
            try:
                import traceback as _tb

                await event_log.add(
                    "job",
                    "/api/start",
                    "error",
                    {
                        "job_id": job.job_id,
                        "symbol": symbol,
                        "expiry": expiry_sel,
                        "error": str(e),
                        "trace": _tb.format_exc(),
                        "logs": job.logs[-50:],
                    },
                    status="error",
                )
            except Exception:
                pass
            job.status = "error"
            job.progress = 1.0
        finally:
            try:
                await job_manager.clear_running_key(key, job)
            except Exception:
                pass

    asyncio.create_task(run())
    resp = JSONResponse({"ok": True, "job_id": job.job_id})
    if set_cookie:
        resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
    return resp


@router.get("/api/status")
async def api_status(job_id: str):
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Unknown job")
    try:
        await event_log.add(
            "api",
            "/api/status",
            "poll",
            {"job_id": job_id, "job_status": job.status, "progress": job.progress},
            status="info",
        )
    except Exception:
        pass
    return JSONResponse(
        {
            "status": job.status,
            "progress": job.progress,
            "logs": job.logs[-120:],
            "has_result": bool(getattr(job, "result", None) is not None),
        }
    )


@router.get("/api/result")
async def api_result(job_id: str):
    try:
        await event_log.add("api", "/api/result", "poll", {"job_id": job_id})
    except Exception:
        pass
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Unknown job")
    if getattr(job, "result", None) is None:
        # If the job finished but no result payload was set, return an empty
        # structure to let the UI render blanks rather than spinning forever.
        if getattr(job, "status", None) == "done":
            return JSONResponse(
                {
                    "strikes": [],
                    "vanna_net": [],
                    "vanna_calls": [],
                    "vanna_puts": [],
                }
            )
        return Response(status_code=204)
    return JSONResponse(job.result)


async def _list_expiry_dates(symbol: str) -> list[str]:
    """Return upcoming expiries (ascending yyyy-mm-dd) using Massive/Polygon filters."""
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date().isoformat()
    sym = symbol.upper()
    cached = _EXPIRY_CACHE.get(sym)
    if cached:
        age = time.time() - float(cached.get("ts") or 0.0)
        if age <= EXPIRY_CACHE_TTL_SEC:
            return list(cached.get("expiries") or [])

    underlyings = [sym]
    if sym in {"SPX", "NDX", "RUT", "DJI"}:
        underlyings = [f"I:{sym}", sym]
    if not sym.endswith("W"):
        underlyings.append(sym + "W")
    underlyings = list(dict.fromkeys(underlyings))

    seen = set()
    out = []
    completed = True
    try:
        async with asyncio.timeout(EXPIRY_LIST_TIMEOUT_SEC):
            async with httpx.AsyncClient() as client:
                for u in underlyings:
                    try:
                        params = {
                            "underlying_ticker": u,
                            "active": "true",
                            "limit": 1000,
                            "sort": "expiration_date",
                            "order": "asc",
                            "expiration_date.gte": today,
                        }
                        cursor = None
                        loops = 0
                        while True:
                            loops += 1
                            q = dict(params)
                            if cursor:
                                q["cursor"] = cursor
                            data = await polygon_get(client, "/v3/reference/options/contracts", q)
                            results = data.get("results") or []
                            for c in results:
                                e = c.get("expiration_date") or c.get("expirationDate") or c.get("exp_date")
                                if e:
                                    d = str(e)[:10]
                                    if d >= today and d not in seen:
                                        seen.add(d)
                                        out.append(d)
                            if len(out) >= EXPIRY_LIST_MAX_COUNT or len(out) >= EXPIRY_LIST_TARGET_COUNT:
                                break
                            cursor = data.get("next_url_cursor") or data.get("nextCursor")
                            if not cursor:
                                try:
                                    next_url = data.get("next_url")
                                except Exception:
                                    next_url = None
                                if isinstance(next_url, str) and next_url:
                                    try:
                                        from urllib.parse import parse_qs, urlparse

                                        qs = parse_qs(urlparse(next_url).query)
                                        cvals = qs.get("cursor") or []
                                        if cvals:
                                            cursor = cvals[0]
                                    except Exception:
                                        cursor = None
                            if loops >= EXPIRY_LIST_MAX_PAGES:
                                completed = False
                            if not cursor or loops >= EXPIRY_LIST_MAX_PAGES:
                                break
                    except Exception:
                        continue
                    if len(out) >= EXPIRY_LIST_MAX_COUNT or len(out) >= EXPIRY_LIST_TARGET_COUNT:
                        break
    except TimeoutError:
        completed = False

    if out:
        sorted_out = sorted(out)[:EXPIRY_LIST_MAX_COUNT]
        if completed or len(sorted_out) >= EXPIRY_LIST_TARGET_COUNT:
            _EXPIRY_CACHE[sym] = {"ts": time.time(), "expiries": sorted_out}
        return sorted_out
    if cached:
        return list(cached.get("expiries") or [])
    return []


@router.get("/api/expiries")
async def api_list_expiries(request: Request, symbol: str):
    try:
        await event_log.add("api", "/api/expiries", "request", {"symbol": symbol})
    except Exception:
        pass
    if get_demo_mode(request):
        expiries = demo_expiries(symbol)
        return JSONResponse(
            {
                "symbol": symbol.upper(),
                "expiries": expiries,
                **_build_expiry_scope_metadata(expiries),
            }
        )
    exp = await _list_expiry_dates(symbol)
    return JSONResponse(
        {
            "symbol": symbol.upper(),
            "expiries": exp,
            **_build_expiry_scope_metadata(exp),
        }
    )


def _pct_delta(val: float | None, ref: float | None) -> Optional[float]:
    try:
        if val is None or ref is None or ref == 0:
            return None
        return (float(val) - float(ref)) / float(ref) * 100.0
    except Exception:
        return None


async def _fetch_price_context(symbol: str) -> dict:
    """Best-effort price + reference closes for scanner rows."""
    sym = symbol.upper()
    spot = None
    prev_close = None
    try:
        spot = fetch_spot_yahoo(sym)
    except Exception:
        spot = None
    async with httpx.AsyncClient() as client:
        if spot is None:
            try:
                spot = await fetch_spot_polygon(client, sym)
            except Exception:
                spot = None
        try:
            data_prev = await polygon_get(client, f"/v2/aggs/ticker/{sym}/prev", {})
            prevs = (data_prev or {}).get("results") or []
            if prevs:
                prev_close = prevs[0].get("c")
        except Exception:
            prev_close = None
    spot_ah = None
    rth_close = None
    prev_close_yest = None
    try:
        import yfinance as _yf  # type: ignore

        _map = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "DJI": "^DJI"}
        ysym = _map.get(sym, sym)
        _t = _yf.Ticker(ysym)
        fi = getattr(_t, "fast_info", None)
        if fi:
            for k in ("post_market_price", "postMarketPrice", "post_market_last_price"):
                v = getattr(fi, k, None)
                if v is not None:
                    spot_ah = float(v)
                    break
            if spot_ah is None and hasattr(fi, "get"):
                for k in ("postMarketPrice", "post_market_price"):
                    v = fi.get(k)
                    if v is not None:
                        spot_ah = float(v)
                        break
        hist = None
        try:
            hist = _t.history(period="5d")
        except Exception:
            hist = None
        if hist is not None and hasattr(hist, "empty") and not hist.empty:
            closes = hist.get("Close")
            if closes is not None:
                closes = closes.dropna()
                if len(closes) >= 1:
                    rth_close = float(closes.iloc[-1])
                if len(closes) >= 2:
                    prev_close_yest = float(closes.iloc[-2])
    except Exception:
        pass
    day_ref = prev_close_yest if prev_close_yest is not None else prev_close
    day_change_pct = _pct_delta(spot, day_ref)
    ah_change_pct = _pct_delta(spot_ah, rth_close)
    ah_change_abs = None
    try:
        if spot_ah is not None and rth_close is not None:
            ah_change_abs = float(spot_ah) - float(rth_close)
    except Exception:
        ah_change_abs = None
    return {
        "spot": (float(spot) if spot is not None else None),
        "spot_ah": (float(spot_ah) if spot_ah is not None else None),
        "prev_close": (float(prev_close) if prev_close is not None else None),
        "prev_close_yest": (float(prev_close_yest) if prev_close_yest is not None else None),
        "rth_close": (float(rth_close) if rth_close is not None else None),
        "day_change_pct": (float(day_change_pct) if day_change_pct is not None else None),
        "ah_change_pct": (float(ah_change_pct) if ah_change_pct is not None else None),
        "ah_change_abs": (float(ah_change_abs) if ah_change_abs is not None else None),
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


async def _compute_gex_cached_summary(
    symbol: str,
    spot: float,
    pct_window: float,
    *,
    expiry_key: Optional[str],
    expiry_mode: str,
    next_only: bool,
    remove_0dte: bool,
    allowed_expiries: list[str] | None = None,
    label: str = "",
    parent_job_id: Optional[str] = None,
) -> dict:
    ttl = int(os.getenv("SERVER_CACHE_TTL_SEC", "300"))
    key = cache_key(
        mode="g",
        symbol=symbol,
        pct_window=pct_window,
        next_only=next_only,
        expiry=expiry_key,
        weight="",
        spot_override=None,
        expiry_mode=expiry_mode,
        include_0dte=(not remove_0dte),
        expiry_filter=",".join(sorted(allowed_expiries or [])),
        solver_profile=gamma_solver_cache_token(None),
        calc_version=GEX_CALC_VERSION,
    )
    cached = job_manager.cache_get(key, ttl)
    res: dict
    cache_source = None
    if cached is not None:
        res = cached
        cache_source = "memory"
        await _cache_metric(parent_job_id, "gex_memory_hit")
    else:
        disk = disk_cache_get(key, ttl)
        if disk is not None:
            res = disk
            cache_source = "disk"
            await _cache_metric(parent_job_id, "gex_disk_hit")
            try:
                job_manager.cache_set(key, res)
            except Exception:
                pass
        else:
            await _cache_metric(parent_job_id, "gex_miss")
            try:
                job = await job_manager.create(f"scanner:{symbol}")
            except Exception:
                from engine import Job

                job = Job(job_id="scanner-" + symbol, session_id="scanner")
            try:
                await event_log.add(
                    "job",
                    "/api/scanner/gex",
                    "scanner:gex",
                    {
                        "symbol": symbol,
                        "label": label or "all",
                        "expiry": expiry_key or "all",
                        "expiry_mode": expiry_mode,
                        "remove_0dte": remove_0dte,
                        "cached": False,
                        "parent_job_id": parent_job_id,
                        "cache_source": cache_source,
                    },
                )
            except Exception:
                pass
            try:
                df, meta = await compute_gex_for_ticker(
                    job,
                    symbol,
                    spot,
                    pct_window=pct_window,
                    only_next_expiry=next_only,
                    expiry_mode=expiry_mode,
                    expiry_override=None,
                    include_0dte=(not remove_0dte),
                    remove_0dte=remove_0dte,
                    allowed_expiries=allowed_expiries,
                )
            except Exception as e:  # noqa: BLE001
                job.status = "error"
                job.log(f"scanner gex error: {e}")
                res = {
                    "strikes": [],
                    "gex_net": [],
                    "gex_calls": [],
                    "gex_puts": [],
                    "meta": {"error": str(e)},
                }
            else:
                if df is None or df.empty:
                    res = {
                        "strikes": [],
                        "gex_net": [],
                        "gex_calls": [],
                        "gex_puts": [],
                        "meta": {
                            "expiry": (meta.get("expiry") if isinstance(meta, dict) else None),
                            "spot": float(spot),
                            "pct_window": float(pct_window),
                            "include_0dte": bool(not remove_0dte),
                            "remove_0dte": bool(remove_0dte),
                            "net_gex": (
                                float(meta.get("net_gex"))
                                if isinstance(meta, dict) and meta.get("net_gex") is not None
                                else 0.0
                            ),
                            "total_gamma_at_spot": (
                                float(meta.get("total_gamma_at_spot"))
                                if isinstance(meta, dict)
                                and meta.get("total_gamma_at_spot") is not None
                                else 0.0
                            ),
                            "zero_gamma": (
                                float(meta.get("zero_gamma"))
                                if isinstance(meta, dict) and meta.get("zero_gamma") is not None
                                else None
                            ),
                            "spot_vs_zero_gamma": (
                                meta.get("spot_vs_zero_gamma")
                                if isinstance(meta, dict)
                                else "No Zero Gamma in tested range"
                            ),
                            "gamma_regime": (
                                meta.get("gamma_regime")
                                if isinstance(meta, dict)
                                else "Gamma Regime Unavailable"
                            ),
                            "zero_gamma_diagnostics": (
                                dict(meta.get("zero_gamma_diagnostics") or {})
                                if isinstance(meta, dict)
                                else {}
                            ),
                            "provider_listing": (
                                dict(meta.get("provider_listing") or {})
                                if isinstance(meta, dict)
                                else {}
                            ),
                            **(
                                {"total_oi": float(meta.get("total_oi"))}
                                if isinstance(meta, dict) and meta.get("total_oi") is not None
                                else {}
                            ),
                        },
                    }
                else:
                    strikes = df["strike"].astype(float).round(2).tolist()
                    gnet = df["gex_net"].astype(float).tolist()
                    gc = (
                        (
                            df["gex_calls"]
                            if "gex_calls" in df.columns
                            else pd.Series([0.0] * len(df))
                        )
                        .astype(float)
                        .tolist()
                    )
                    gp = (
                        (df["gex_puts"] if "gex_puts" in df.columns else pd.Series([0.0] * len(df)))
                        .astype(float)
                        .tolist()
                    )
                    _meta = {
                        "expiry": (meta.get("expiry") if isinstance(meta, dict) else None),
                        "spot": float(spot),
                        "pct_window": float(pct_window),
                        "include_0dte": bool(not remove_0dte),
                        "remove_0dte": bool(remove_0dte),
                        "net_gex": (
                            float(meta.get("net_gex"))
                            if isinstance(meta, dict) and meta.get("net_gex") is not None
                            else float(sum(gnet))
                        ),
                        "total_gamma_at_spot": (
                            float(meta.get("total_gamma_at_spot"))
                            if isinstance(meta, dict) and meta.get("total_gamma_at_spot") is not None
                            else float(sum(gnet))
                        ),
                        "zero_gamma": (
                            float(meta.get("zero_gamma"))
                            if isinstance(meta, dict) and meta.get("zero_gamma") is not None
                            else None
                        ),
                        "spot_vs_zero_gamma": (
                            meta.get("spot_vs_zero_gamma")
                            if isinstance(meta, dict)
                            else spot_vs_zero_gamma_label(
                                spot,
                                meta.get("zero_gamma") if isinstance(meta, dict) else None,
                            )
                        ),
                        "gamma_regime": (
                            meta.get("gamma_regime")
                            if isinstance(meta, dict)
                            else "Gamma Regime Unavailable"
                        ),
                        "zero_gamma_diagnostics": (
                            dict(meta.get("zero_gamma_diagnostics") or {})
                            if isinstance(meta, dict)
                            else {}
                        ),
                        "provider_listing": (
                            dict(meta.get("provider_listing") or {})
                            if isinstance(meta, dict)
                            else {}
                        ),
                    }
                    try:
                        if isinstance(meta, dict) and meta.get("total_oi") is not None:
                            _meta["total_oi"] = float(meta.get("total_oi"))
                    except Exception:
                        pass
                    res = {
                        "strikes": strikes,
                        "gex_net": gnet,
                        "gex_calls": gc,
                        "gex_puts": gp,
                        "gex_cumulative": (
                            df["gex_cumulative"].astype(float).tolist()
                            if "gex_cumulative" in df.columns
                            else []
                        ),
                        "zero_gamma_curve": [],
                        "meta": _meta,
                    }
                res = _canonicalize_gex_result(res)
            cache_source = cache_source or "disk"
            try:
                job_manager.cache_set(key, res)
            except Exception:
                pass
            try:
                disk_cache_set(key, res)
            except Exception:
                pass
            try:
                await event_log.add(
                    "job",
                    "/api/scanner/gex",
                    "scanner:gex_cache_store",
                    {
                        "symbol": symbol,
                        "label": label or "all",
                        "expiry": expiry_key or "all",
                        "expiry_mode": expiry_mode,
                        "remove_0dte": remove_0dte,
                        "parent_job_id": parent_job_id,
                    },
                )
            except Exception:
                pass
    res = _canonicalize_gex_result(res)
    try:
        await event_log.add(
            "job",
            "/api/scanner/gex",
            "scanner:gex",
            {
                "symbol": symbol,
                "label": label or "all",
                "expiry": expiry_key or "all",
                "expiry_mode": expiry_mode,
                "remove_0dte": remove_0dte,
                "cached": bool(cache_source),
                "cache_source": cache_source,
                "parent_job_id": parent_job_id,
            },
        )
    except Exception:
        pass
    try:
        await event_log.add(
            "job",
            "/api/scanner/gex",
            "scanner:gex_done",
            {
                "symbol": symbol,
                "label": label or "all",
                "expiry": expiry_key or "all",
                "expiry_mode": expiry_mode,
                "remove_0dte": remove_0dte,
                "parent_job_id": parent_job_id,
                "net_gex": res.get("meta", {}).get("net_gex"),
                "zero_gamma": res.get("meta", {}).get("zero_gamma"),
                "cache_source": cache_source,
            },
        )
    except Exception:
        pass
    zero_gamma = None
    try:
        zero_gamma = res.get("meta", {}).get("zero_gamma")
    except Exception:
        zero_gamma = None
    net_gex = None
    try:
        net_gex = res.get("meta", {}).get("net_gex")
    except Exception:
        net_gex = None
    total_gamma_at_spot = None
    try:
        total_gamma_at_spot = res.get("meta", {}).get("total_gamma_at_spot")
    except Exception:
        total_gamma_at_spot = None
    return {
        "net_gex": (float(net_gex) if net_gex is not None else None),
        "total_gamma_at_spot": (
            float(total_gamma_at_spot) if total_gamma_at_spot is not None else None
        ),
        "zero_gamma": (float(zero_gamma) if zero_gamma is not None else None),
        "gamma_confidence": derive_gamma_solver_confidence(
            ((res.get("meta") or {}).get("zero_gamma_diagnostics") or {})
        ),
        "gamma_regime": (res.get("meta") or {}).get("gamma_regime") or "Gamma Regime Unavailable",
        "spot_vs_zero_gamma": (res.get("meta") or {}).get("spot_vs_zero_gamma")
        or spot_vs_zero_gamma_label(spot, zero_gamma),
        "spot_vs_zero_gamma_pct": spot_vs_zero_gamma_pct(spot, zero_gamma),
        "zero_gamma_diagnostics": (res.get("meta") or {}).get("zero_gamma_diagnostics") or {},
        "meta": res.get("meta") or {},
        "raw": res,
        "error": (res.get("meta") or {}).get("error"),
    }

async def _scanner_entry(
    symbol: str,
    pct_window: float,
    scope: str,
    remove_0dte: bool,
    parent_job_id: Optional[str] = None,
    demo_mode: bool = False,
) -> dict:
    sym = symbol.upper().strip()
    if demo_mode:
        return demo_scanner_row(sym, pct_window, scope=scope, include_0dte=(not remove_0dte))
    row = {"symbol": sym}
    if not sym:
        row["error"] = "Missing symbol"
        return row
    price = await _fetch_price_context(sym)
    row.update(price)
    if price.get("spot") is None:
        row["error"] = "Spot unavailable"
        return row
    expiries = await _list_expiry_dates(sym)
    today_iso = _today_iso()
    scope_key = normalize_expiration_scope(scope, default="all")
    expiry_meta = _build_expiry_scope_metadata(expiries, today_iso=today_iso)
    resolved_scope = _resolve_requested_expiration_scope(
        expiries,
        requested_scope=scope_key,
        remove_0dte=remove_0dte,
        today_iso=today_iso,
    )
    scope_expiries = list(resolved_scope["allowed_expiries"] or [])
    row["next_expiry"] = expiries[0] if expiries else None
    row["scope"] = scope_key
    row["scope_expiry_count"] = len(scope_expiries)
    row["scope_expirations"] = scope_expiries
    row["resolved_expiration_key"] = ",".join(scope_expiries)
    row["available_expirations"] = expiries
    row["monthly_expirations"] = expiry_meta["monthly_expirations"]
    row["scope_support"] = expiry_meta["scope_support"]
    row["supported"] = bool(resolved_scope["supported"])
    row["excluded"] = not bool(resolved_scope["supported"])
    row["remove_0dte"] = bool(remove_0dte)
    row["include_0dte"] = bool(not remove_0dte)
    selected_display_expiry = scope_expiries[0] if len(scope_expiries) == 1 else None
    row["selected_expiry"] = selected_display_expiry
    row["selected_expiry_is_monthly"] = bool(
        selected_display_expiry and is_standard_monthly_expiration(selected_display_expiry)
    )
    if not resolved_scope["supported"]:
        row["net_gex"] = None
        row["total_gamma_at_spot"] = None
        row["zero_gamma"] = None
        row["front_gex_bias"] = None
        row["term_shape_bias"] = None
        row["term_shape"] = _empty_term_shape()
        row["spot_density"] = None
        row["gamma_confidence"] = None
        row["spot_vs_zero_gamma"] = "No Zero Gamma in tested range"
        row["spot_vs_zero_gamma_pct"] = None
        row["gamma_regime"] = "Gamma Regime Unavailable"
        row["error"] = f"No expirations available for {scope_key.upper()}"
        row["excluded_reason"] = f"unsupported_{scope_key}"
        return row
    try:
        summary = await _compute_gex_cached_summary(
            sym,
            price["spot"],
            pct_window,
            expiry_key=",".join(scope_expiries) if scope_expiries else None,
            expiry_mode="all",
            next_only=False,
            remove_0dte=remove_0dte,
            allowed_expiries=scope_expiries if scope_expiries else expiries,
            label=scope_key,
            parent_job_id=parent_job_id,
        )
    except Exception as e:  # noqa: BLE001
        summary = {
            "net_gex": None,
            "total_gamma_at_spot": None,
            "zero_gamma": None,
            "spot_vs_zero_gamma": "No Zero Gamma in tested range",
            "gamma_regime": "Gamma Regime Unavailable",
            "error": str(e),
        }
    row["net_gex"] = summary.get("net_gex")
    row["total_gamma_at_spot"] = summary.get("total_gamma_at_spot")
    row["zero_gamma"] = summary.get("zero_gamma")
    row["gamma_confidence"] = summary.get("gamma_confidence")
    row["spot_vs_zero_gamma"] = summary.get("spot_vs_zero_gamma")
    row["spot_vs_zero_gamma_pct"] = summary.get("spot_vs_zero_gamma_pct")
    row["gamma_regime"] = summary.get("gamma_regime") or "Gamma Regime Unavailable"
    row["message"] = summary.get("meta", {}).get("expiry") if isinstance(summary, dict) else ""
    row["error"] = summary.get("error") if isinstance(summary, dict) else None
    row["excluded_reason"] = None
    row["spot_density"] = _scanner_spot_density(summary.get("raw") or {}, price.get("spot"))
    term_shape = await _scanner_term_shape(
        sym,
        float(price["spot"]),
        pct_window,
        available_expiries=expiries,
        remove_0dte=remove_0dte,
        parent_job_id=parent_job_id,
    )
    row["term_shape"] = term_shape
    row["term_shape_bias"] = term_shape.get("bias")
    return row


@router.post("/api/scanner/scan")
async def api_scanner_scan(request: Request):
    """Return gamma scanner rows for a list of tickers."""
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    scope = normalize_expiration_scope(payload.get("scope"), default="all")
    if scope not in EXPIRATION_SCOPE_CHOICES:
        scope = "all"
    remove_0dte = _payload_remove_0dte(payload, default=False)
    symbols_raw = payload.get("symbols") or []
    try:
        default_pct = float(payload.get("pct_window", 0.10))
    except Exception:
        default_pct = 0.10
    symbols: list[tuple[str, float]] = []
    for s in symbols_raw:
        if not s:
            continue
        if isinstance(s, dict):
            sym = (s.get("symbol") or s.get("ticker") or "").upper().strip()
            try:
                pw = float(s.get("pct_window", default_pct))
            except Exception:
                pw = default_pct
        else:
            sym = str(s).upper().strip()
            pw = default_pct
        if sym:
            symbols.append((sym, pw))
    symbols = symbols[:20]
    demo_mode = get_demo_mode(request)
    try:
        await event_log.add(
            "api", "/api/scanner/scan", "request", {"symbols": [s for s, _ in symbols]}
        )
    except Exception:
        pass
    if demo_mode:
        rows = [
            demo_scanner_row(sym, pw, scope=scope, include_0dte=(not remove_0dte))
            for sym, pw in symbols
        ]
        return JSONResponse(
            {
                "results": rows,
                "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "demo": True,
                "scope": scope,
                "include_0dte": bool(not remove_0dte),
                "remove_0dte": bool(remove_0dte),
            }
        )
    if not symbols:
        return JSONResponse({"results": [], "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    concurrency = scanner_max_workers(len(symbols))
    sem = asyncio.Semaphore(concurrency)

    async def _run(sym: str, pw: float):
        async with sem:
            return await _scanner_entry(sym, pw, scope, remove_0dte, None, demo_mode)

    t0 = time.time()
    rows = await asyncio.gather(*[_run(sym, pw) for sym, pw in symbols])
    duration = time.time() - t0
    try:
        await event_log.add(
            "api",
            "/api/scanner/scan",
            "response",
            {"symbols": [s for s, _ in symbols], "count": len(rows)},
            status="success",
        )
    except Exception:
        pass
    return JSONResponse(
        {
            "results": rows,
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_secs": duration,
            "scope": scope,
            "include_0dte": bool(not remove_0dte),
            "remove_0dte": bool(remove_0dte),
        }
    )


@router.post("/api/scanner/start")
async def api_scanner_start(request: Request):
    """Kick off a scanner job (parallelized) and return job_id for polling."""
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    scope = normalize_expiration_scope(payload.get("scope"), default="all")
    if scope not in EXPIRATION_SCOPE_CHOICES:
        scope = "all"
    remove_0dte = _payload_remove_0dte(payload, default=False)
    symbols_raw = payload.get("symbols") or []
    try:
        default_pct = float(payload.get("pct_window", 0.10))
    except Exception:
        default_pct = 0.10
    symbols: list[tuple[str, float]] = []
    for s in symbols_raw:
        if not s:
            continue
        if isinstance(s, dict):
            sym = (s.get("symbol") or s.get("ticker") or "").upper().strip()
            try:
                pw = float(s.get("pct_window", default_pct))
            except Exception:
                pw = default_pct
        else:
            sym = str(s).upper().strip()
            pw = default_pct
        if sym:
            symbols.append((sym, pw))
    symbols = symbols[:20]
    if not symbols:
        raise HTTPException(400, detail="No symbols")
    demo_mode = get_demo_mode(request)
    sid = get_session_id(request)
    set_cookie = False
    if not sid:
        import uuid

        token = signer.sign(uuid.uuid4().hex).decode("utf-8")
        sid = token
        set_cookie = True
    if demo_mode:
        job = await job_manager.create(sid)
        job.status = "done"
        job.progress = 1.0
        job.result = {
            "results": [
                demo_scanner_row(sym, pw, scope=scope, include_0dte=(not remove_0dte))
                for sym, pw in symbols
            ],
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_secs": 0.0,
            "demo": True,
            "scope": scope,
            "include_0dte": bool(not remove_0dte),
            "remove_0dte": bool(remove_0dte),
        }
        resp = JSONResponse({"ok": True, "job_id": job.job_id, "demo": True})
        if set_cookie:
            resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
        resp.set_cookie(
            DEMO_MODE_COOKIE, "1" if demo_mode else "0", max_age=60 * 60 * 24 * 90, samesite="lax"
        )
        return resp
    job = await job_manager.create(sid)
    job.status = "queued"
    job.progress = 0.0
    job.scan_state = {"total": len(symbols), "done": 0, "last": None, "started": time.time()}
    try:
        await event_log.add(
            "api",
            "/api/scanner/start",
            "request",
            {"symbols": [s for s, _ in symbols], "job_id": job.job_id},
        )
    except Exception:
        pass

    async def run():
        try:
            await event_log.add(
                "job", "/api/scanner/start", "start", {"symbols": symbols}, status="info"
            )
        except Exception:
            pass
        concurrency = scanner_max_workers(len(symbols))
        sem = asyncio.Semaphore(concurrency)
        total = len(symbols)
        results = []
        started_ts = time.time()

        async def _run_one(sym: str, pw: float):
            async with sem:
                return await _scanner_entry(
                    sym, pw, scope, remove_0dte, job.job_id, demo_mode
                )

        tasks = [asyncio.create_task(_run_one(sym, pw)) for sym, pw in symbols]
        done_ct = 0
        job.status = "running"
        job.progress = 0.01
        loop_started = time.time()
        try:
            for coro in asyncio.as_completed(tasks):
                if job.cancel_event.is_set():
                    job.status = "cancelled"
                    job.progress = 1.0
                    for t in tasks:
                        t.cancel()
                    return
                res = await coro
                results.append(res)
                done_ct += 1
                job.progress = min(0.1 + 0.9 * (done_ct / max(total, 1)), 0.99)
                job.scan_state = {
                    "total": total,
                    "done": done_ct,
                    "last": res.get("symbol"),
                    "started": started_ts,
                    "elapsed": time.time() - started_ts,
                }
                try:
                    await event_log.add(
                        "job",
                        "/api/scanner/start",
                        "progress",
                        {
                            "job_id": job.job_id,
                            "done": done_ct,
                            "total": total,
                            "last": res.get("symbol"),
                            "elapsed": time.time() - started_ts,
                        },
                        status="info",
                    )
                except Exception:
                    pass
        except asyncio.CancelledError:
            job.status = "cancelled"
            job.progress = 1.0
            return
        except Exception as e:
            try:
                await event_log.add(
                    "job",
                    "/api/scanner/start",
                    "error",
                    {"error": str(e), "symbols": symbols},
                    status="error",
                )
            except Exception:
                pass
            job.status = "error"
            job.progress = 1.0
            job.result = {"error": str(e)}
            return
        job.result = {
            "results": results,
            "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_secs": time.time() - loop_started,
            "cache_metrics": dict(getattr(job, "cache_metrics", {}) or {}),
            "scope": scope,
            "include_0dte": bool(not remove_0dte),
            "remove_0dte": bool(remove_0dte),
        }
        job.status = "done"
        job.progress = 1.0
        job.log(f"cache metrics: {job.cache_metrics}")
        try:
            await event_log.add(
                "job",
                "/api/scanner/start",
                "done",
                {"symbols": symbols, "cache_metrics": job.cache_metrics},
                status="success",
            )
        except Exception:
            pass

    asyncio.create_task(run())
    resp = JSONResponse({"ok": True, "job_id": job.job_id})
    if set_cookie:
        resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
    return resp


@router.get("/api/scanner/status")
async def api_scanner_status(job_id: str):
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Job not found")
    st = {
        "status": job.status,
        "progress": job.progress,
        "job_id": job.job_id,
    }
    if getattr(job, "scan_state", None):
        st.update(job.scan_state)
    try:
        await event_log.add(
            "api",
            "/api/scanner/status",
            "poll",
            {
                "job_id": job_id,
                "status": job.status,
                "progress": job.progress,
                "done": st.get("done"),
                "total": st.get("total"),
            },
        )
    except Exception:
        pass
    return JSONResponse(st)


@router.get("/api/scanner/result")
async def api_scanner_result(job_id: str):
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Job not found")
    if job.status != "done":
        return Response(status_code=204)
    return JSONResponse(job.result or {})


@router.post("/api/scanner/stop")
async def api_scanner_stop(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    job_id = payload.get("job_id")
    if not job_id:
        raise HTTPException(400, detail="Missing job_id")
    ok = await job_manager.cancel(job_id)
    try:
        await event_log.add("api", "/api/scanner/stop", "request", {"job_id": job_id, "ok": ok})
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)})


# ------------------------------
# GEX API
# ------------------------------
@router.post("/api/gex/start")
async def api_gex_start(request: Request):
    payload = await request.json()
    symbol = (payload.get("symbol") or "").upper().strip()
    if not symbol:
        raise HTTPException(400, detail="Missing symbol")
    spot_override = payload.get("spot_override")
    next_only = bool(payload.get("next_only", True))
    try:
        pct_window = float(payload.get("pct_window", 0.10))
    except Exception:
        pct_window = 0.10
    expiry_sel = payload.get("expiry") or None
    expiry_mode = normalize_expiration_scope(payload.get("expiry_mode"), default="selected")
    remove_0dte = _payload_remove_0dte(payload, default=False)
    selected_expirations = payload.get("selected_expirations")
    if not isinstance(selected_expirations, list):
        selected_expirations = None
    selected_expirations = [str(exp)[:10] for exp in (selected_expirations or []) if exp]
    solver_config = _payload_solver_config(payload)
    demo_mode = get_demo_mode(request)
    available_expiries = demo_expiries(symbol) if demo_mode else await _list_expiry_dates(symbol)
    resolved_scope = _resolve_requested_expiration_scope(
        available_expiries,
        requested_scope=expiry_mode,
        selected_expiry=expiry_sel,
        selected_expirations=selected_expirations if expiry_mode == "all" else None,
        remove_0dte=remove_0dte,
    )
    effective_scope = resolved_scope["scope"]
    effective_expiry = resolved_scope["expiry_override"]
    effective_expiries = list(resolved_scope["allowed_expiries"] or [])
    if not resolved_scope["supported"]:
        raise HTTPException(400, detail=f"No expirations available for {effective_scope.upper()}")

    sid = get_session_id(request)
    set_cookie = False
    if not sid:
        import uuid

        token = signer.sign(uuid.uuid4().hex).decode("utf-8")
        sid = token
        set_cookie = True
    if demo_mode:
        job = await job_manager.create(sid)
        job.status = "done"
        job.progress = 1.0
        job.result = _canonicalize_gex_result(
            demo_gex_result(
                symbol,
                pct_window,
                expiry=effective_expiry,
                expiry_mode=effective_scope,
                include_0dte=(not remove_0dte),
            )
        )
        try:
            meta = _apply_expiration_meta(
                job.result.get("meta") or {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=remove_0dte,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            meta["solver_config"] = dict(solver_config)
            meta["solver_profile_label"] = gamma_solver_profile_label(solver_config)
            meta["job_id"] = job.job_id
            job.result["meta"] = meta
        except Exception:
            pass
        resp = JSONResponse({"ok": True, "job_id": job.job_id, "demo": True})
        if set_cookie:
            resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
        resp.set_cookie(
            DEMO_MODE_COOKIE, "1" if demo_mode else "0", max_age=60 * 60 * 24 * 90, samesite="lax"
        )
        return resp

    # Server-side cache + dedupe for GEX
    ttl = int(os.getenv("SERVER_CACHE_TTL_SEC", "300"))
    key = cache_key(
        mode="g",
        symbol=symbol,
        pct_window=pct_window,
        next_only=(False if effective_expiry else next_only),
        expiry=effective_expiry,
        weight="",
        spot_override=(str(spot_override).strip() if spot_override else None),
        expiry_mode=effective_scope,
        include_0dte=(not remove_0dte),
        expiry_filter=",".join(sorted(effective_expiries)),
        solver_profile=gamma_solver_cache_token(solver_config),
        calc_version=GEX_CALC_VERSION,
    )
    try:
        await event_log.add(
            "api",
            "/api/gex/start",
            "request",
            {"symbol": symbol, "expiry": effective_expiry, "next_only": next_only, "scope": effective_scope},
        )
    except Exception:
        pass
    running = await job_manager.get_running_by_key(key)
    if running:
        running.log(f"Attach: another session joined GEX run for {symbol}")
        return JSONResponse({"ok": True, "job_id": running.job_id})
    cached = job_manager.cache_get(key, ttl)
    if cached is not None:
        job = await job_manager.create(sid)
        job.status = "done"
        job.progress = 1.0
        res = _canonicalize_gex_result(cached)
        if isinstance(res.get("meta"), dict):
            meta = _apply_expiration_meta(
                res.get("meta") or {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=remove_0dte,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            res["meta"] = meta
        job.result = res
        job.log(f"Cache hit (server, ttl={ttl}s) for {symbol} — GEX")
        try:
            await event_log.add(
                "api",
                "/api/gex/start",
                "cache_hit",
                {
                    "symbol": symbol,
                    "expiry": expiry_sel,
                    "next_only": next_only,
                    "ttl": ttl,
                    "key": key,
                    "job_id": job.job_id,
                },
                status="success",
            )
        except Exception:
            pass
        resp = JSONResponse({"ok": True, "job_id": job.job_id})
        if set_cookie:
            resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
        return resp

    job = await job_manager.create(sid)
    job.status = "queued"
    job.progress = 0.01
    job.log(f"GEX Job {job.job_id} created for {symbol} (next_only={next_only}).")
    await job_manager.register_running_key(key, job)

    async def run():
        t_job_start = time.time()
        try:
            try:
                await event_log.add(
                    "job",
                    "/api/gex/start",
                    "start",
                    {
                        "symbol": symbol,
                        "expiry": effective_expiry,
                        "next_only": (False if effective_expiry else next_only),
                        "pct_window": pct_window,
                        "remove_0dte": remove_0dte,
                        "selected_expirations": effective_expiries,
                        "scope": effective_scope,
                        "solver_config": solver_config,
                    },
                status="info",
            )
            except Exception:
                pass
            # Resolve spot
            if spot_override:
                try:
                    spot = float(str(spot_override).replace(",", "").strip())
                    job.log(f"Using user spot override: {spot}")
                except Exception:
                    job.log("Invalid spot override; falling back to auto.")
                    spot = None
            else:
                spot = None
            async with httpx.AsyncClient() as client:
                t_price_start = time.time()
                if spot is None:
                    job.log("Auto-detecting spot from Yahoo…")
                    sp = fetch_spot_yahoo(symbol)
                    if sp is None:
                        job.log("Yahoo failed; trying Massive (Polygon)…")
                        sp = await fetch_spot_polygon(client, symbol)
                    if sp is None:
                        job.log("Could not auto-detect spot; please provide override.")
                        job.status = "error"
                        job.progress = 1.0
                        return
                    spot = sp
                    try:
                        job.log(f"Detected spot {spot:.2f}")
                    except Exception:
                        job.log(f"Detected spot {spot}")
                else:
                    try:
                        job.log(f"Using spot {spot:.2f}")
                    except Exception:
                        job.log(f"Using spot {spot}")
                # previous close via Massive (Polygon) prev bar
                try:
                    data_prev = await polygon_get(
                        client, f"/v2/aggs/ticker/{symbol.upper()}/prev", {}
                    )
                    prevs = (data_prev or {}).get("results") or []
                    if prevs:
                        prev_close = prevs[0].get("c")
                    else:
                        prev_close = None
                except Exception:
                    prev_close = None
                t_price_end = time.time()
            # Try after-hours spot best-effort
            spot_ah = None
            try:
                import yfinance as _yf  # type: ignore

                _map = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "DJI": "^DJI"}
                ysym = _map.get(symbol.upper(), symbol)
                _t = _yf.Ticker(ysym)
                val = None
                try:
                    fi = getattr(_t, "fast_info", None)
                    if fi:
                        for k in ("post_market_price", "postMarketPrice", "post_market_last_price"):
                            v = getattr(fi, k, None)
                            if v is not None:
                                val = v
                                break
                        if val is None and hasattr(fi, "get"):
                            for k in ("postMarketPrice", "post_market_price"):
                                v = fi.get(k)
                                if v is not None:
                                    val = v
                                    break
                except Exception:
                    val = None
                if val is None:
                    try:
                        inf = _t.info  # type: ignore[attr-defined]
                        if isinstance(inf, dict):
                            val = inf.get("postMarketPrice")
                    except Exception:
                        val = None
                if val is not None:
                    spot_ah = float(val)
            except Exception:
                spot_ah = None
            job.log(f"Using window ±{pct_window*100:.2f}% around spot")
            t_compute_start = time.time()
            df, meta = await compute_gex_for_ticker(
                job,
                symbol,
                spot,
                pct_window=pct_window,
                only_next_expiry=(False if effective_expiry else next_only),
                expiry_mode=("selected" if effective_scope == "selected" else "all"),
                expiry_override=effective_expiry,
                include_0dte=(not remove_0dte),
                remove_0dte=remove_0dte,
                allowed_expiries=(effective_expiries if effective_scope != "selected" else None),
                include_solver_curve=True,
                solver_config=solver_config,
            )
            t_compute_end = time.time()
            meta = _apply_expiration_meta(
                meta if isinstance(meta, dict) else {},
                scope=effective_scope,
                available_expiries=available_expiries,
                selected_expirations=effective_expiries,
                selected_expiry=effective_expiry,
                remove_0dte=remove_0dte,
            )
            if effective_scope == "selected" and effective_expiry:
                meta["expiry"] = effective_expiry
            if job.cancel_event.is_set():
                job.status = "cancelled"
                return
            if df is None or df.empty:
                job.result = _canonicalize_gex_result(
                    {
                    "strikes": [],
                    "gex_net": [],
                    "gex_calls": [],
                    "gex_puts": [],
                    "meta": {
                        "expiry": meta.get("expiry"),
                        "spot": float(spot),
                        **({"spot_ah": float(spot_ah)} if spot_ah is not None else {}),
                        **({"prev_close": float(prev_close)} if prev_close is not None else {}),
                        "net_gex": (
                            float(meta.get("net_gex")) if meta.get("net_gex") is not None else 0.0
                        ),
                        "total_gamma_at_spot": (
                            float(meta.get("total_gamma_at_spot"))
                            if meta.get("total_gamma_at_spot") is not None
                            else 0.0
                        ),
                        "zero_gamma": (
                            float(meta.get("zero_gamma"))
                            if meta.get("zero_gamma") is not None
                            else None
                        ),
                        "spot_vs_zero_gamma": meta.get("spot_vs_zero_gamma")
                        or "No Zero Gamma in tested range",
                        "gamma_regime": meta.get("gamma_regime") or "Gamma Regime Unavailable",
                        "zero_gamma_diagnostics": dict(meta.get("zero_gamma_diagnostics") or {}),
                        "include_0dte": bool(not remove_0dte),
                        "remove_0dte": bool(remove_0dte),
                        "expiry_mode": meta.get("expiry_mode"),
                        "selected_expirations": list(effective_expiries or []),
                        "available_expirations": list(available_expiries or []),
                        "monthly_expirations": list(meta.get("monthly_expirations") or []),
                        "expiry_is_monthly": bool(meta.get("expiry_is_monthly")),
                        "solver_config": dict(meta.get("solver_config") or solver_config),
                        "solver_profile_label": meta.get("solver_profile_label")
                        or gamma_solver_profile_label(solver_config),
                        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "symbol": symbol,
                        "provider_listing": dict(meta.get("provider_listing") or {}),
                    },
                    "zero_gamma_curve": list(meta.get("zero_gamma_curve") or []),
                    }
                )
                job.status = "done"
                job.progress = 1.0
                return
            strikes = df["strike"].astype(float).round(2).tolist()
            gnet = df["gex_net"].astype(float).tolist()
            gc = (
                (df["gex_calls"] if "gex_calls" in df.columns else pd.Series([0.0] * len(df)))
                .astype(float)
                .tolist()
            )
            gp = (
                (df["gex_puts"] if "gex_puts" in df.columns else pd.Series([0.0] * len(df)))
                .astype(float)
                .tolist()
            )
            # derive rth_close and prev_close_yest via Yahoo daily history (best-effort)
            rth_close = None
            prev_close_yest = None
            try:
                import yfinance as _yf  # type: ignore

                _map = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "DJI": "^DJI"}
                ysym = _map.get(symbol.upper(), symbol)
                _t = _yf.Ticker(ysym)
                hd = _t.history(period="5d")
                if hasattr(hd, "empty") and not hd.empty:
                    c = hd.get("Close")
                    if c is not None:
                        c = c.dropna()
                        if len(c) >= 1:
                            rth_close = float(c.iloc[-1])
                        if len(c) >= 2:
                            prev_close_yest = float(c.iloc[-2])
            except Exception:
                pass
            _meta = {
                "expiry": meta.get("expiry"),
                "spot": float(spot),
                "net_gex": float(meta.get("net_gex")) if meta.get("net_gex") is not None else float(sum(gnet)),
                "total_gamma_at_spot": (
                    float(meta.get("total_gamma_at_spot"))
                    if meta.get("total_gamma_at_spot") is not None
                    else float(sum(gnet))
                ),
                "zero_gamma": (
                    float(meta.get("zero_gamma")) if meta.get("zero_gamma") is not None else None
                ),
                "spot_vs_zero_gamma": meta.get("spot_vs_zero_gamma") or "No Zero Gamma in tested range",
                "gamma_regime": meta.get("gamma_regime") or "Gamma Regime Unavailable",
                "zero_gamma_diagnostics": dict(meta.get("zero_gamma_diagnostics") or {}),
                "include_0dte": bool(not remove_0dte),
                "remove_0dte": bool(remove_0dte),
                "expiry_mode": meta.get("expiry_mode"),
                "selected_expirations": list(effective_expiries or []),
                "available_expirations": list(available_expiries or []),
                "monthly_expirations": list(meta.get("monthly_expirations") or []),
                "expiry_is_monthly": bool(meta.get("expiry_is_monthly")),
                "solver_config": dict(meta.get("solver_config") or solver_config),
                "solver_profile_label": meta.get("solver_profile_label")
                or gamma_solver_profile_label(solver_config),
                "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "provider_listing": dict(meta.get("provider_listing") or {}),
            }
            if spot_ah is not None:
                _meta["spot_ah"] = float(spot_ah)
            if prev_close is not None:
                _meta["prev_close"] = float(prev_close)
            if rth_close is not None:
                _meta["rth_close"] = float(rth_close)
            if prev_close_yest is not None:
                _meta["prev_close_yest"] = float(prev_close_yest)
            try:
                _meta["timings"] = {
                    "price_sec": round(max(0.0, t_price_end - t_price_start), 3),
                    "compute_sec": round(max(0.0, t_compute_end - t_compute_start), 3),
                    "total_sec": round(max(0.0, time.time() - t_job_start), 3),
                }
                job.log(
                    f"timings: price={_meta['timings']['price_sec']}s compute={_meta['timings']['compute_sec']}s total={_meta['timings']['total_sec']}s"
                )
            except Exception:
                pass
            job.result = _canonicalize_gex_result(
                {
                "strikes": strikes,
                "gex_net": gnet,
                "gex_calls": gc,
                "gex_puts": gp,
                "gex_cumulative": (
                    df["gex_cumulative"].astype(float).tolist()
                    if "gex_cumulative" in df.columns
                    else []
                ),
                "zero_gamma_curve": list(meta.get("zero_gamma_curve") or []),
                "meta": _meta,
                }
            )
            job.status = "done"
            job.progress = 1.0
            try:
                job_manager.cache_set(key, job.result)
            except Exception:
                pass
            try:
                disk_cache_set(key, job.result)
            except Exception:
                pass
            try:
                await event_log.add(
                    "job", "/api/gex/start", "done", {"symbol": symbol, "job_id": job.job_id}
                )
            except Exception:
                pass
        except asyncio.CancelledError:
            job.status = "cancelled"
        except Exception as e:
            job.log(f"FATAL: {type(e).__name__}: {e}")
            job.status = "error"
            job.progress = 1.0
            try:
                import traceback as _tb

                await event_log.add(
                    "job",
                    "/api/gex/start",
                    "error",
                    {
                        "job_id": job.job_id,
                        "symbol": symbol,
                        "expiry": expiry_sel,
                        "error": str(e),
                        "trace": _tb.format_exc(),
                        "logs": job.logs[-50:],
                    },
                    status="error",
                )
            except Exception:
                pass
        finally:
            try:
                await job_manager.clear_running_key(key, job)
            except Exception:
                pass

    asyncio.create_task(run())
    resp = JSONResponse({"ok": True, "job_id": job.job_id})
    if set_cookie:
        resp.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax")
    return resp


@router.post("/api/gex/solver-preview")
async def api_gex_solver_preview(request: Request):
    payload = await request.json()
    symbol = (payload.get("symbol") or "").upper().strip()
    if not symbol:
        raise HTTPException(400, detail="Missing symbol")
    spot_override = payload.get("spot_override")
    try:
        pct_window = float(payload.get("pct_window", 0.10))
    except Exception:
        pct_window = 0.10
    expiry_sel = payload.get("expiry") or None
    expiry_mode = normalize_expiration_scope(payload.get("expiry_mode"), default="selected")
    remove_0dte = _payload_remove_0dte(payload, default=False)
    selected_expirations = payload.get("selected_expirations")
    if not isinstance(selected_expirations, list):
        selected_expirations = None
    selected_expirations = [str(exp)[:10] for exp in (selected_expirations or []) if exp]
    solver_config = _payload_solver_config(payload)
    available_expiries = await _list_expiry_dates(symbol)
    resolved_scope = _resolve_requested_expiration_scope(
        available_expiries,
        requested_scope=expiry_mode,
        selected_expiry=expiry_sel,
        selected_expirations=selected_expirations if expiry_mode == "all" else None,
        remove_0dte=remove_0dte,
    )
    effective_scope = resolved_scope["scope"]
    effective_expiry = resolved_scope["expiry_override"]
    effective_expiries = list(resolved_scope["allowed_expiries"] or [])
    if not resolved_scope["supported"]:
        raise HTTPException(400, detail=f"No expirations available for {effective_scope.upper()}")
    ttl = int(os.getenv("SERVER_CACHE_TTL_SEC", "300"))
    key = cache_key(
        mode="g",
        symbol=symbol,
        pct_window=pct_window,
        next_only=False,
        expiry=effective_expiry,
        weight="",
        spot_override=(str(spot_override).strip() if spot_override else None),
        expiry_mode=effective_scope,
        include_0dte=(not remove_0dte),
        expiry_filter=",".join(sorted(effective_expiries)),
        solver_profile=gamma_solver_cache_token(solver_config),
        calc_version=GEX_CALC_VERSION,
    )
    cached = job_manager.cache_get(key, ttl) or disk_cache_get(key, ttl)
    if cached is not None:
        result = _canonicalize_gex_result(cached)
        meta = _apply_expiration_meta(
            result.get("meta") or {},
            scope=effective_scope,
            available_expiries=available_expiries,
            selected_expirations=effective_expiries,
            selected_expiry=effective_expiry,
            remove_0dte=remove_0dte,
        )
        if effective_scope == "selected" and effective_expiry:
            meta["expiry"] = effective_expiry
        diagnostics = dict(meta.get("zero_gamma_diagnostics") or {})
        return JSONResponse(
            {
                "source": "cache",
                "net_gex": meta.get("net_gex"),
                "zero_gamma": meta.get("zero_gamma"),
                "total_gamma_at_spot": meta.get("total_gamma_at_spot"),
                "gamma_regime": meta.get("gamma_regime"),
                "gamma_confidence": diagnostics.get("solver_confidence"),
                "solver_profile_label": meta.get("solver_profile_label")
                or gamma_solver_profile_label(solver_config),
                "solver_config": dict(meta.get("solver_config") or solver_config),
                "diagnostics": diagnostics,
            }
        )

    spot = None
    if spot_override:
        try:
            spot = float(str(spot_override).replace(",", "").strip())
        except Exception:
            spot = None
    if spot is None:
        async with httpx.AsyncClient() as client:
            spot = fetch_spot_yahoo(symbol)
            if spot is None:
                spot = await fetch_spot_polygon(client, symbol)
    if spot is None:
        raise HTTPException(400, detail="Spot unavailable")
    job = Job(job_id="gex-preview", session_id="preview")
    _df, meta = await compute_gex_for_ticker(
        job,
        symbol,
        float(spot),
        pct_window=pct_window,
        only_next_expiry=(False if effective_expiry else False),
        expiry_mode=("selected" if effective_scope == "selected" else "all"),
        expiry_override=effective_expiry,
        include_0dte=(not remove_0dte),
        remove_0dte=remove_0dte,
        allowed_expiries=(effective_expiries if effective_scope != "selected" else None),
        include_solver_curve=False,
        solver_config=solver_config,
    )
    meta = _apply_expiration_meta(
        meta if isinstance(meta, dict) else {},
        scope=effective_scope,
        available_expiries=available_expiries,
        selected_expirations=effective_expiries,
        selected_expiry=effective_expiry,
        remove_0dte=remove_0dte,
    )
    if effective_scope == "selected" and effective_expiry:
        meta["expiry"] = effective_expiry
    diagnostics = dict(meta.get("zero_gamma_diagnostics") or {})
    return JSONResponse(
        {
            "source": "live",
            "net_gex": meta.get("net_gex"),
            "zero_gamma": meta.get("zero_gamma"),
            "total_gamma_at_spot": meta.get("total_gamma_at_spot"),
            "gamma_regime": meta.get("gamma_regime"),
            "gamma_confidence": diagnostics.get("solver_confidence"),
            "solver_profile_label": meta.get("solver_profile_label")
            or gamma_solver_profile_label(solver_config),
            "solver_config": dict(meta.get("solver_config") or solver_config),
            "diagnostics": diagnostics,
        }
    )


@router.get("/api/gex/status")
async def api_gex_status(job_id: str):
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Unknown job")
    try:
        await event_log.add(
            "api",
            "/api/gex/status",
            "poll",
            {"job_id": job_id, "job_status": job.status, "progress": job.progress},
            status="info",
        )
    except Exception:
        pass
    return JSONResponse({"status": job.status, "progress": job.progress, "logs": job.logs[-120:]})


@router.get("/api/gex/result")
async def api_gex_result(job_id: str):
    try:
        await event_log.add("api", "/api/gex/result", "poll", {"job_id": job_id})
    except Exception:
        pass
    job = await job_manager.get(job_id)
    if not job:
        raise HTTPException(404, detail="Unknown job")
    if getattr(job, "result", None) is None:
        # If the job has flipped to done but the result payload has not been
        # attached yet, keep the client polling instead of rendering a blank chart.
        return JSONResponse(
            {
                "status": "pending",
                "job_status": getattr(job, "status", None),
                "progress": getattr(job, "progress", None),
                "logs": job.logs[-50:],
            }
        )
    return JSONResponse(job.result)


@router.get("/api/gex/zero-gamma-curve")
async def api_gex_zero_gamma_curve(job_id: str):
    job = await job_manager.get(job_id)
    if not job or not getattr(job, "result", None):
        raise HTTPException(404, detail="No result for job")
    result = job.result or {}
    meta = result.get("meta") or {}
    diagnostics = meta.get("zero_gamma_diagnostics") or {}
    curve = result.get("zero_gamma_curve") or diagnostics.get("curve") or []
    return JSONResponse(
        {
            "job_id": job_id,
            "symbol": meta.get("symbol"),
            "expiry": meta.get("expiry"),
            "spot": meta.get("spot"),
            "zero_gamma": meta.get("zero_gamma"),
            "total_gamma_at_spot": meta.get("total_gamma_at_spot"),
            "gamma_regime": meta.get("gamma_regime"),
            "solver_spot_min": diagnostics.get("solver_spot_min"),
            "solver_spot_max": diagnostics.get("solver_spot_max"),
            "first_sign_change_interval": diagnostics.get("first_sign_change_interval"),
            "sign_change_intervals": diagnostics.get("sign_change_intervals") or [],
            "available_expirations": diagnostics.get("available_expirations") or [],
            "included_expirations": diagnostics.get("included_expirations") or [],
            "excluded_expirations": diagnostics.get("excluded_expirations") or [],
            "excluded_expiration_reasons": diagnostics.get("excluded_expiration_reasons") or {},
            "selected_scope": diagnostics.get("selected_scope") or meta.get("expiry_mode"),
            "selected_expiry": diagnostics.get("selected_expiry"),
            "selected_expiration_set": diagnostics.get("selected_expiration_set") or [],
            "remove_0dte": diagnostics.get("remove_0dte", meta.get("remove_0dte")),
            "included_row_count": diagnostics.get("included_row_count"),
            "dropped_row_count": diagnostics.get("dropped_row_count"),
            "dropped_rows_by_reason": diagnostics.get("dropped_rows_by_reason") or {},
            "included_contract_sample": diagnostics.get("included_contract_sample") or [],
            "total_contracts_fetched": diagnostics.get("total_contracts_fetched"),
            "total_expirations_fetched": diagnostics.get("total_expirations_fetched"),
            "pagination_completed": diagnostics.get("pagination_completed"),
            "provider_page_count": diagnostics.get("provider_page_count"),
            "provider_truncation": diagnostics.get("provider_truncation"),
            "fetch_path": diagnostics.get("fetch_path"),
            "selected_expiry_resolution_mode": diagnostics.get("selected_expiry_resolution_mode"),
            "listing_truncated": diagnostics.get("listing_truncated"),
            "selected_expiry_guaranteed": diagnostics.get("selected_expiry_guaranteed"),
            "fallback_used": diagnostics.get("fallback_used"),
            "diagnostics": diagnostics,
            "curve": curve,
        }
    )


@router.post("/api/gex/stop")
async def api_gex_stop(request: Request):
    payload = await request.json()
    job_id = payload.get("job_id")
    if not job_id:
        raise HTTPException(400, detail="Missing job_id")
    ok = await job_manager.cancel(job_id)
    if not ok:
        raise HTTPException(404, detail="Unknown job")
    return JSONResponse({"ok": True})


@router.get("/api/gex/export/pairs")
async def gex_export_pairs(job_id: str, top: int | None = None):
    try:
        await event_log.add("api", "/api/gex/export/pairs", "request", {"job": job_id})
    except Exception:
        pass
    job = await job_manager.get(job_id)
    if not job or not getattr(job, "result", None):
        raise HTTPException(404, detail="No result for job")
    res = job.result
    strikes = res.get("strikes", [])
    gnet = res.get("gex_net", [])
    if not top:
        top = _autoscale_top(gnet)
    pairs = _format_pairs(strikes, gnet, top_n=top)
    return JSONResponse({"pairs": pairs, "top": top, "meta": res.get("meta") or {}})


@router.get("/api/gex/export/pine")
async def gex_export_pine(job_id: str, top: int | None = None):
    try:
        await event_log.add("api", "/api/gex/export/pine", "request", {"job": job_id})
    except Exception:
        pass
    job = await job_manager.get(job_id)
    if not job or not getattr(job, "result", None):
        raise HTTPException(404, detail="No result for job")
    res = job.result
    strikes = res.get("strikes", [])
    gnet = res.get("gex_net", [])
    if not top:
        top = _autoscale_top(gnet)
    pairs = _format_pairs(strikes, gnet, top_n=top)
    meta = res.get("meta") or {}
    symbol = meta.get("symbol") or ""
    expiry = meta.get("expiry") or ""
    as_of = meta.get("as_of") or ""
    spot = meta.get("spot")
    sig = f"symbol={symbol} exp={expiry} spot={spot} as_of={as_of} top={top} job={job_id}"
    pine = f"""//@version=6
// Signature: {sig}
indicator("Net GEX Levels", overlay=true, max_lines_count=500)
dataStr = "{pairs}"
showLabels = input.bool(false, "Show Labels")
widthScale = input.float(1.0, "Line Width Scale", minval=0.1, maxval=5.0)

color_pos = color.new(color.from_hex("#10b981"), 0)
color_neg = color.new(color.from_hex("#ef4444"), 0)

pairStr = str.split(dataStr, ";")
var float maxAbs = 0.0
for i = 0 to array.size(pairStr) - 1
    p = array.get(pairStr, i)
    kv = str.split(p, ",")
    if array.size(kv) >= 2
        v = str.tonumber(array.get(kv,1))
        if not na(v)
            maxAbs := math.max(maxAbs, math.abs(v))

var line[] lines = array.new_line()
for i = 0 to array.size(pairStr) - 1
    p = array.get(pairStr, i)
    kv = str.split(p, ",")
    if array.size(kv) >= 2
        k = str.tonumber(array.get(kv,0))
        v = str.tonumber(array.get(kv,1))
        w = 1
        if maxAbs > 0 and not na(v)
            w := math.round(math.max(1, math.min(5, 1 + 3 * (math.abs(v)/maxAbs) * widthScale)))
        ln = line.new(bar_index-500, k, bar_index, k, xloc=xloc.bar_index, extend=extend.right,
            color=(v>=0? color_pos : color_neg), width=w)
        array.push(lines, ln)
        if showLabels and not na(k)
            kStr = str.tostring(k, format.mintick)
            vStr = str.tostring(v, format.mintick)
            label.new(bar_index, k, text=kStr + " | " + vStr, xloc=xloc.bar_index,
                style=label.style_label_left, textcolor=color.white, color=color.new(color.black, 80))
"""
    return JSONResponse({"pine": pine, "pairs": pairs, "top": top, "meta": meta, "signature": sig})


# ---------- TradingView export helpers ----------
def _autoscale_top(values, top_min=12, top_max=60, cum_target=0.80):
    arr = [abs(float(v)) for v in values]
    total = sum(arr) or 1.0
    order = sorted(arr, reverse=True)
    acc = 0.0
    n = 0
    for i, v in enumerate(order):
        acc += v
        if acc / total >= cum_target:
            n = i + 1
            break
    if n == 0:
        n = top_min
    return max(top_min, min(top_max, n))


def _format_pairs(strikes, vnet, top_n=None):
    items = list(zip([float(s) for s in strikes], [float(v) for v in vnet]))
    items.sort(key=lambda kv: abs(kv[1]), reverse=True)
    if top_n:
        items = items[:top_n]
    items.sort(key=lambda kv: kv[0])
    out = []
    for k, v in items:
        kint = int(round(k))
        ks = str(kint if abs(k - kint) < 1e-6 else k)
        out.append(f"{ks},{round(v,2)}")
    return ";".join(out)


@router.get("/api/export/pairs")
async def export_pairs(job_id: str, top: int | None = None):
    job = await job_manager.get(job_id)
    if not job or not getattr(job, "result", None):
        raise HTTPException(404, detail="No result for job")
    res = job.result
    strikes = res.get("strikes", [])
    vnet = res.get("vanna_net", [])
    if not top:
        top = _autoscale_top(vnet)
    pairs = _format_pairs(strikes, vnet, top_n=top)
    return JSONResponse({"pairs": pairs, "top": top, "meta": res.get("meta") or {}})


@router.get("/api/export/pine")
async def export_pine(job_id: str, top: int | None = None):
    job = await job_manager.get(job_id)
    if not job or not getattr(job, "result", None):
        raise HTTPException(404, detail="No result for job")
    res = job.result
    strikes = res.get("strikes", [])
    vnet = res.get("vanna_net", [])
    if not top:
        top = _autoscale_top(vnet)
    pairs = _format_pairs(strikes, vnet, top_n=top)
    meta = res.get("meta") or {}
    symbol = meta.get("symbol") or ""
    expiry = meta.get("expiry") or ""
    as_of = meta.get("as_of") or ""
    spot = meta.get("spot")
    sig = f"symbol={symbol} exp={expiry} spot={spot} as_of={as_of} top={top} job={job_id}"
    pine = f"""//@version=6
// Signature: {sig}
indicator("Net Vanna Levels", overlay=true, max_lines_count=500)
dataStr = "{pairs}"
showLabels = input.bool(false, "Show Labels")
widthScale = input.float(1.0, "Line Width Scale", minval=0.1, maxval=5.0)

// Colors (positive/negative)
color_pos = color.new(#0003b8, 0)
color_neg = color.new(#ffe202, 0)

// Parse pairs and find max |vanna| for width scaling
pairStr = str.split(dataStr, ";")
var float maxAbs = 0.0
for i = 0 to array.size(pairStr) - 1
    p = array.get(pairStr, i)
    kv = str.split(p, ",")
    if array.size(kv) >= 2
        v = str.tonumber(array.get(kv,1))
        if not na(v)
            maxAbs := math.max(maxAbs, math.abs(v))

// Draw lines (and labels if enabled)
var line[] lines = array.new_line()
for i = 0 to array.size(pairStr) - 1
    p = array.get(pairStr, i)
    kv = str.split(p, ",")
    if array.size(kv) >= 2
        k = str.tonumber(array.get(kv,0))
        v = str.tonumber(array.get(kv,1))
        w = 1
        if maxAbs > 0 and not na(v)
            w := math.round(math.max(1, math.min(5, 1 + 3 * (math.abs(v)/maxAbs) * widthScale)))
        ln = line.new(bar_index-500, k, bar_index, k, xloc=xloc.bar_index, extend=extend.right,
            color=(v>=0? color_pos : color_neg), width=w)
        array.push(lines, ln)
        if showLabels and not na(k)
            kStr = str.tostring(k, format.mintick)
            vStr = str.tostring(v, format.mintick)
            label.new(bar_index, k, text=kStr + " | " + vStr, xloc=xloc.bar_index,
                style=label.style_label_left, textcolor=color.white, color=color.new(color.black, 80))
"""
    return JSONResponse({"pine": pine, "pairs": pairs, "top": top, "meta": meta, "signature": sig})


@router.post("/api/stop")
async def api_stop(request: Request):
    payload = await request.json()
    job_id = payload.get("job_id")
    if not job_id:
        raise HTTPException(400, detail="Missing job_id")
    ok = await job_manager.cancel(job_id)
    if not ok:
        raise HTTPException(404, detail="Unknown job")
    return JSONResponse({"ok": True})


@router.get("/healthz")
async def healthz():
    return {"ok": True}

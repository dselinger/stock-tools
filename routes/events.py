from __future__ import annotations

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from core.web import render_template
from engine import event_log, job_manager, polygon_get
from session_utils import signer

router = APIRouter()


@router.get("/events/{symbol}", response_class=HTMLResponse)
async def ticker_events_page(request: Request, symbol: str):
    sym = (symbol or "").upper().strip()
    ssr_html = "<div class='muted'>(loading…)</div>"
    try:
        data = await _fetch_events_data(sym, nocache=True)
        arr = (data.get("polygon") or [])[:15]

        def _esc(s: str) -> str:
            return (
                str(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#39;")
            )

        from urllib.parse import urlparse

        try:
            from zoneinfo import ZoneInfo

            _tz = ZoneInfo("America/Los_Angeles")
        except Exception:  # pragma: no cover
            _tz = None
        from datetime import datetime, timezone

        def _fmt_when(s: str) -> str:
            try:
                if not s:
                    return ""
                iso = str(s).replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(_tz) if _tz else dt
                return dt.strftime("%Y-%m-%d %H:%M %Z")
            except Exception:
                return _esc(s)

        if arr:
            items = []
            for a in arr:
                raw_url = a.get("article_url") or a.get("url") or "#"
                title = _esc(a.get("title") or "")
                url = _esc(raw_url)
                pub = _fmt_when(a.get("published") or "")
                dom = ""
                try:
                    d = urlparse(raw_url).netloc.split(":")[0]
                    if d.startswith("www."):
                        d = d[4:]
                    dom = d
                except Exception:
                    dom = ""
                suffix = f" — ({_esc(dom)}) -- {_esc(pub)}" if (dom or pub) else ""
                items.append(
                    f"<li><a href='{url}' target='_blank' rel='noopener'>{title}</a><span class='muted'>{suffix}</span></li>"
                )
            ssr_html = "<ul>" + "".join(items) + "</ul>"
        else:
            ssr_html = "<div class='muted'>(no recent news)</div>"
    except Exception:
        ssr_html = "<div class='muted'>(error loading server-side)</div>"
    try:
        await event_log.add("page", "/events", "open", {"symbol": sym})
    except Exception:
        pass
    return render_template(
        "events.html", request, {"title": f"{sym} — Events", "symbol": sym, "ssr_html": ssr_html}
    )


@router.get("/econ", response_class=HTMLResponse)
async def economic_events_page(request: Request):
    ssr_econ_html = "<div class='muted'>(loading…)</div>"
    try:
        data = await _fetch_events_data(None, nocache=True)
        econ = (data.get("econ") or [])[:15]

        def _esc(s: str) -> str:
            return (
                str(s)
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace('"', "&quot;")
                .replace("'", "&#39;")
            )

        from urllib.parse import urlparse

        try:
            from zoneinfo import ZoneInfo

            _tz = ZoneInfo("America/Los_Angeles")
        except Exception:  # pragma: no cover
            _tz = None

        def _fmt_pub(pub: str) -> str:
            try:
                from email.utils import parsedate_to_datetime

                dt = parsedate_to_datetime(pub)
                if _tz is not None:
                    dt = dt.astimezone(_tz)
                return dt.strftime("%Y-%m-%d %H:%M %Z")
            except Exception:
                return _esc(pub)

        if econ:
            items = []
            for a in econ:
                raw_url = a.get("url") or "#"
                title = _esc(a.get("title") or "")
                url = _esc(raw_url)
                pub = _fmt_pub(a.get("published") or "")
                try:
                    d = urlparse(raw_url).netloc.split(":")[0]
                    if d.startswith("www."):
                        d = d[4:]
                    dom = d
                except Exception:
                    dom = ""
                suffix = f" — ({_esc(dom)}) -- {_esc(pub)}" if (dom or pub) else ""
                items.append(
                    f"<li><a href='{url}' target='_blank' rel='noopener'>{title}</a><span class='muted'>{suffix}</span></li>"
                )
            ssr_econ_html = "<ul>" + "".join(items) + "</ul>"
        else:
            ssr_econ_html = "<div class='muted'>(no recent economic headlines)</div>"
    except Exception:
        ssr_econ_html = "<div class='muted'>(error loading server-side)</div>"
    try:
        await event_log.add("page", "/econ", "open")
    except Exception:
        pass
    return render_template(
        "econ.html", request, {"title": "Economic News", "ssr_html": ssr_econ_html}
    )


async def _fetch_events_data(symbol: str | None, nocache: bool = False) -> dict:
    ttl = 60
    sym = (symbol or "").upper().strip()
    cache_key = f"events:{sym or 'ALL'}"
    if not nocache:
        cached = job_manager.cache_get(cache_key, ttl)
        if cached is not None:
            return cached

    out: dict = {"polygon": [], "econ": [], "urls": {}}
    try:
        async with httpx.AsyncClient() as client:
            if sym:
                try:
                    try:
                        await event_log.add("api", "/api/events", "polygon_start", {"symbol": sym})
                    except Exception:
                        pass
                    params = {"ticker": sym, "limit": 20, "order": "desc", "sort": "published_utc"}
                    try:
                        from urllib.parse import urlencode

                        q = dict(params)
                        q["apiKey"] = "YOUR_API_KEY"
                        out["urls"]["polygon"] = (
                            "https://api.polygon.io/v2/reference/news?" + urlencode(q)
                        )
                    except Exception:
                        pass
                    data = await polygon_get(client, "/v2/reference/news", params)
                    try:
                        if isinstance(data, dict) and data.get("status") == "error":
                            await event_log.add(
                                "api",
                                "/api/events",
                                "polygon_error",
                                {"symbol": sym, "message": data.get("message", "")},
                                status="error",
                            )
                    except Exception:
                        pass
                    arts = data.get("results") or [] if isinstance(data, dict) else []
                    from datetime import datetime, timedelta, timezone

                    now = datetime.now(timezone.utc)
                    cutoff = now - timedelta(days=7)
                    for a in arts:
                        try:
                            aid = (
                                a.get("id") or a.get("article_id") or a.get("url") or a.get("title")
                            )
                            pub_raw = a.get("published_utc") or a.get("published_at") or ""
                            dt = None
                            try:
                                if pub_raw:
                                    s = str(pub_raw).replace("Z", "+00:00")
                                    dt = datetime.fromisoformat(s)
                            except Exception:
                                dt = None
                            if dt is not None and dt >= cutoff:
                                out["polygon"].append(
                                    {
                                        "id": str(aid),
                                        "title": a.get("title") or "",
                                        "url": a.get("article_url") or a.get("url") or "",
                                        "published": pub_raw,
                                    }
                                )
                        except Exception:
                            continue
                except Exception:
                    pass
            try:
                try:
                    await event_log.add("api", "/api/events", "rss_start", {})
                except Exception:
                    pass
                rss_url = "https://www.investing.com/rss/news_301.rss"
                out["urls"]["rss"] = rss_url
                r = await client.get(rss_url, timeout=20)
                r.raise_for_status()
                import xml.etree.ElementTree as ET

                root = ET.fromstring(r.text)
                for item in root.findall(".//item"):
                    try:
                        title = (item.findtext("title") or "").strip()
                        link = (item.findtext("link") or "").strip()
                        pub = (item.findtext("pubDate") or "").strip()
                        out["econ"].append(
                            {"id": link or title, "title": title, "url": link, "published": pub}
                        )
                    except Exception:
                        continue
            except Exception as e:
                try:
                    await event_log.add(
                        "api", "/api/events", "rss_error", {"error": str(e)}, status="error"
                    )
                except Exception:
                    pass
    except Exception:
        pass

    try:
        job_manager.cache_set(cache_key, out)
    except Exception:
        pass
    return out


@router.get("/api/events")
async def api_events(symbol: str | None = None, nocache: int = 0):
    try:
        await event_log.add("api", "/api/events", "request", {"symbol": symbol or ""})
    except Exception:
        pass
    out = await _fetch_events_data(symbol, nocache=bool(nocache))
    try:
        sym = (symbol or "").upper().strip()
        await event_log.add(
            "api",
            "/api/events",
            "response",
            {
                "symbol": sym,
                "polygon": len(out.get("polygon") or []),
                "econ": len(out.get("econ") or []),
            },
            status="success",
        )
    except Exception:
        pass
    return JSONResponse(out)


@router.post("/api/events/ack")
async def api_events_ack(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    seen = payload.get("seen") or {}
    import json as _json

    try:
        token = signer.sign(_json.dumps({"seen": seen})).decode("utf-8")
    except Exception:
        token = None
    resp = JSONResponse({"ok": True})
    if token:
        resp.set_cookie("EV_SEEN", token, httponly=True, samesite="lax")
    return resp

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from core.web import render_template
from engine import event_log

router = APIRouter()


@router.get("/debug", response_class=HTMLResponse)
async def debug_page(request: Request):
    return render_template("debug.html", request, {"title": "Unified Debug Console"})


@router.get("/api/debug/logs")
async def api_debug_logs(limit: int = 500):
    try:
        items = await event_log.get(limit)
    except Exception:
        items = []
    return JSONResponse({"logs": items})


@router.post("/api/debug/client")
async def api_debug_client(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    message = str(payload.get("message") or "")
    status = str(payload.get("status") or "info")
    meta = payload.get("meta") or {}
    try:
        await event_log.add("client", "/client", message, meta, status=status)
    except Exception:
        pass
    return JSONResponse({"ok": True})


@router.get("/api/debug/config")
async def api_debug_config():
    try:
        en = await event_log.get_enabled()
    except Exception:
        en = True
    return JSONResponse({"enabled": en})


@router.post("/api/debug/logging")
async def api_debug_logging(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    enabled = bool(payload.get("enabled", True))
    try:
        await event_log.set_enabled(enabled)
    except Exception:
        pass
    return JSONResponse({"ok": True, "enabled": enabled})


@router.post("/api/debug/clear")
async def api_debug_clear():
    try:
        await event_log.clear()
    except Exception:
        pass
    return JSONResponse({"ok": True})

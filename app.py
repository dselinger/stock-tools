from __future__ import annotations

import os
import uuid

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from itsdangerous import BadSignature

from core import __version__
from session_utils import SESSION_COOKIE, SessionSet, ensure_session_and_return, signer
from views import router as views_router


def _cors_origins() -> list[str]:
    raw = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    if not raw:
        return []
    if raw == "*":
        return ["*"]
    return [item.strip() for item in raw.split(",") if item.strip()]


app = FastAPI(title="Dave's Stock Tools", version=__version__)
cors_origins = _cors_origins()
cors_allow_credentials = os.getenv("CORS_ALLOW_CREDENTIALS", "1") == "1"
if cors_origins:
    if cors_origins == ["*"] and cors_allow_credentials:
        cors_allow_credentials = False
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=cors_allow_credentials,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.middleware("http")
async def ensure_session_cookie(request: Request, call_next):
    # For API routes, avoid redirects during fetch(). Set cookie inline if missing/invalid.
    if request.url.path.startswith("/api"):
        sid = request.cookies.get(SESSION_COOKIE)
        valid = False
        if sid:
            try:
                signer.unsign(sid, max_age=60 * 60 * 24 * 30)
                valid = True
            except BadSignature:
                valid = False
        response = await call_next(request)
        if not valid:
            raw = uuid.uuid4().hex
            token = signer.sign(raw).decode("utf-8")
            response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax")
        return response
    # For page routes, keep redirect behavior to establish cookie before rendering
    try:
        _ = await ensure_session_and_return(request)
    except SessionSet as e:
        return e.response
    return await call_next(request)


# Mount views
app.include_router(views_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

from __future__ import annotations

import os
import uuid
from typing import Optional

from fastapi import Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, TimestampSigner

SIGNER_SECRET = os.getenv("SESSION_SIGNER_SECRET", "change-me")
SESSION_COOKIE = "va_session"

signer = TimestampSigner(SIGNER_SECRET)


class SessionSet(Exception):
    def __init__(self, response: RedirectResponse):
        self.response = response


async def ensure_session_and_return(request: Request) -> str:
    sid: Optional[str] = request.cookies.get(SESSION_COOKIE)
    if not sid:
        raw = uuid.uuid4().hex
        token = signer.sign(raw).decode("utf-8")
        response = RedirectResponse(url=str(request.url))
        response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax")
        raise SessionSet(response)
    try:
        signer.unsign(sid, max_age=60 * 60 * 24 * 30)
    except BadSignature:
        raw = uuid.uuid4().hex
        token = signer.sign(raw).decode("utf-8")
        response = RedirectResponse(url=str(request.url))
        response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax")
        raise SessionSet(response)
    return sid


def get_session_id(request: Request) -> Optional[str]:
    return request.cookies.get(SESSION_COOKIE)

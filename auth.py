"""
Authentication & multi-tenant authorization.

- Passwords are hashed with bcrypt (never stored or logged in plaintext).
- Sessions are signed JWTs, delivered as an httpOnly cookie (XSS-resistant —
  JS on the page can never read the token) with a Bearer-header fallback for
  scripts/CLI tools (e.g. diagnose.py) that can't hold cookies.
- Every authenticated request resolves to a CurrentUser carrying org_id and
  role. Route handlers and db.py functions use that org_id to scope every
  query — that application-level filter is the REAL tenant boundary, not
  Postgres RLS (the app uses the Supabase service-role key, which bypasses
  RLS entirely; RLS here is a defense-in-depth backstop, not the mechanism).
"""

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request

JWT_SECRET = os.getenv("JWT_SECRET", "")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_HOURS = int(os.getenv("JWT_EXPIRE_HOURS", "12"))
SESSION_COOKIE = "session"
ADMIN_SESSION_COOKIE = "admin_session"
COOKIE_SECURE = os.getenv("COOKIE_SECURE", "true").lower() != "false"  # set false only for plain-http local dev

if not JWT_SECRET:
    raise RuntimeError(
        "JWT_SECRET is not set. Generate one and add it to your .env:\n"
        '  python3 -c "import secrets; print(secrets.token_hex(32))"'
    )
if len(JWT_SECRET) < 32:
    raise RuntimeError("JWT_SECRET is too short (<32 chars) — generate a proper random secret, not a guessable string.")


# ── Passwords ──────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


# ── Tokens ─────────────────────────────────────────────────────────────────

def create_access_token(user_id: str, org_id: Optional[str], role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": user_id,
        "org_id": org_id,
        "role": role,
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXPIRE_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Session expired — please log in again.")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Invalid session.")


def set_session_cookie(response, token: str, is_admin: bool = False) -> None:
    cookie_name = ADMIN_SESSION_COOKIE if is_admin else SESSION_COOKIE
    response.set_cookie(
        key=cookie_name,
        value=token,
        httponly=True,
        secure=COOKIE_SECURE,
        samesite="strict",
        max_age=JWT_EXPIRE_HOURS * 3600,
        path="/",
    )


def clear_session_cookie(response, is_admin: bool = False) -> None:
    cookie_name = ADMIN_SESSION_COOKIE if is_admin else SESSION_COOKIE
    response.delete_cookie(cookie_name, path="/")


# ── Request-scoped identity ───────────────────────────────────────────────

class CurrentUser:
    def __init__(self, user_id: str, org_id: Optional[str], role: str):
        self.user_id = user_id
        self.org_id = org_id
        self.role = role

    @property
    def is_platform_admin(self) -> bool:
        return self.role == "platform_admin"


def _extract_token(request: Request) -> Optional[str]:
    # Admin API routes prefer the admin cookie; everything else prefers the tenant cookie.
    # Both cookies are sent on every request (same path "/"), we just pick the right one
    # based on which route is being hit, so both sessions coexist in one browser.
    is_admin_route = request.url.path.startswith("/api/admin") or request.url.path == "/admin"
    if is_admin_route:
        token = request.cookies.get(ADMIN_SESSION_COOKIE)
    else:
        token = request.cookies.get(SESSION_COOKIE)
    if token:
        return token
    auth_header = request.headers.get("authorization", "")
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:]
    return None


async def get_current_user(request: Request) -> CurrentUser:
    token = _extract_token(request)
    if not token:
        raise HTTPException(401, "Not authenticated.")
    payload = decode_access_token(token)
    return CurrentUser(user_id=payload["sub"], org_id=payload.get("org_id"), role=payload.get("role", "agent"))


def require_org_user(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    """Any logged-in user that belongs to a tenant org (blocks platform_admin-only routes from leaking org data by accident)."""
    if not user.org_id:
        raise HTTPException(403, "This endpoint requires a tenant account.")
    return user


def require_platform_admin(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
    if not user.is_platform_admin:
        raise HTTPException(403, "Platform admin access required.")
    return user


def require_org_role(*roles: str):
    """Use for actions within a tenant that only certain roles should do (e.g. owner-only)."""
    async def checker(user: CurrentUser = Depends(require_org_user)) -> CurrentUser:
        if user.role not in roles and not user.is_platform_admin:
            raise HTTPException(403, "Insufficient permissions for this action.")
        return user
    return checker

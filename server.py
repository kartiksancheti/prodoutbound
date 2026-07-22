import asyncio
import csv
import io
import json
import logging
import os
import random
import re
import uuid
import time as _time
_START_TIME = _time.time()
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv(override=False)  # only fills gaps — VPS env vars always win. Must run BEFORE
                              # importing auth, which validates JWT_SECRET at import time.

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from auth import (
    CurrentUser, get_current_user, require_org_user, require_platform_admin,
    hash_password, verify_password, create_access_token, set_session_cookie, clear_session_cookie,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbound-server")

app = FastAPI(title="OutboundAI", version="2.0.0")

# ── CORS ──────────────────────────────────────────────────────────────────────
# The dashboard is served by this same app, so same-origin requests never hit
# CORS at all. ALLOWED_ORIGINS only matters for a genuinely separate origin
# (a different admin panel, local dev on another port, etc). Deny by default.
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["Content-Type", "Authorization"],
)

# ── Rate limiting ─────────────────────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── APScheduler ───────────────────────────────────────────────────────────────
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

scheduler = AsyncIOScheduler()

E164_RE = re.compile(r"^\+[1-9]\d{6,14}$")
MAX_CSV_BYTES = 2 * 1024 * 1024  # 2MB
MAX_CSV_ROWS = 2000


def _validate_e164(phone: str) -> str:
    phone = (phone or "").strip()
    if not E164_RE.match(phone):
        raise ValueError(f"'{phone}' is not a valid E.164 number (e.g. +14155551234)")
    return phone


class DispatchBlocked(Exception):
    """Raised by dispatch_call when a call should NOT go out — quota, suspension, bad profile, etc.
    Not an HTTPException because dispatch_call is also called from the background campaign runner,
    which isn't in an HTTP request context."""
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


_BLOCKED_STATUS = {
    "org_not_found": 404, "org_suspended": 403,
    "profile_not_found": 404, "quota_exceeded": 429,
}


# ── LiveKit API helper ────────────────────────────────────────────────────────

def _lk():
    from livekit import api as lk_api
    return lk_api.LiveKitAPI(
        url=os.getenv("LIVEKIT_URL", ""),
        api_key=os.getenv("LIVEKIT_API_KEY", ""),
        api_secret=os.getenv("LIVEKIT_API_SECRET", ""),
    )


async def dispatch_call(
    org_id: str,
    phone_number: str,
    lead_name: str = "there",
    business_name: str = "our company",
    service_type: str = "our service",
    system_prompt: Optional[str] = None,
    agent_profile_id: Optional[str] = None,
) -> dict:
    """Dispatch a single outbound call via LiveKit agent dispatch, scoped to one tenant.
    Raises DispatchBlocked (never HTTPException) if the call should not proceed."""
    from db import get_organization, claim_call_slot, get_agent_profile

    org = await get_organization(org_id)
    if not org:
        raise DispatchBlocked("org_not_found", "Organization not found.")
    if org.get("status") != "active":
        raise DispatchBlocked("org_suspended", "This account is suspended — contact support.")

    if agent_profile_id:
        profile = await get_agent_profile(agent_profile_id, org_id)
        if not profile:
            raise DispatchBlocked("profile_not_found", "Agent profile not found for this organization.")

    # Atomic claim-then-validate: increments first, so concurrent dispatches
    # (e.g. a campaign blast) can never both slip through on a stale read.
    new_count = await claim_call_slot(org_id)
    max_per_day = org.get("max_calls_per_day", 500)
    if new_count > max_per_day:
        raise DispatchBlocked("quota_exceeded", f"Daily call quota reached ({max_per_day}/day).")

    from livekit import api as lk_api

    room_name = f"call-{phone_number.replace('+', '')}-{random.randint(1000, 9999)}"
    metadata = {
        "org_id": org_id,
        "phone_number": phone_number,
        "lead_name": lead_name,
        "business_name": business_name,
        "service_type": service_type,
        "outbound_number": org.get("outbound_number"),
        "transfer_number": org.get("transfer_number"),
    }
    if system_prompt:
        metadata["system_prompt"] = system_prompt
    if agent_profile_id:
        metadata["agent_profile_id"] = agent_profile_id

    client = _lk()
    try:
        dispatch = await client.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                agent_name="outbound-caller",
                room=room_name,
                metadata=json.dumps(metadata),
            )
        )
        return {"success": True, "room": room_name, "dispatch_id": dispatch.id}
    finally:
        await client.aclose()


# ── Campaign runner ───────────────────────────────────────────────────────────

async def run_campaign(campaign_id: str, org_id: str) -> None:
    from db import get_campaign, update_campaign_run_stats, log_error

    campaign = await get_campaign(campaign_id, org_id)
    if not campaign:
        logger.error("Campaign %s not found for org %s", campaign_id, org_id)
        return

    try:
        contacts = json.loads(campaign.get("contacts_json", "[]"))
    except Exception:
        contacts = []

    delay = int(campaign.get("call_delay_seconds", 3))
    system_prompt = campaign.get("system_prompt")
    agent_profile_id = campaign.get("agent_profile_id")
    dispatched = 0
    failed = 0

    logger.info("Running campaign %s (org %s) with %d contacts", campaign_id, org_id, len(contacts))

    for contact in contacts:
        phone = (contact.get("phone") or "").strip()
        if not phone:
            failed += 1
            continue
        try:
            await dispatch_call(
                org_id=org_id,
                phone_number=phone,
                lead_name=contact.get("name", "there"),
                business_name=contact.get("business_name", "our company"),
                service_type=contact.get("service_type", "our service"),
                system_prompt=system_prompt,
                agent_profile_id=agent_profile_id,
            )
            dispatched += 1
        except DispatchBlocked as exc:
            if exc.code == "quota_exceeded":
                logger.warning("Campaign %s stopped early — quota reached for org %s", campaign_id, org_id)
                await log_error("campaign", "Stopped early — daily quota reached", org_id=org_id)
                break
            failed += 1
            await log_error("campaign", f"Failed call to {phone}", exc.message, org_id=org_id)
        except Exception as exc:
            logger.error("Failed to dispatch call to %s: %s", phone, exc)
            await log_error("campaign", f"Failed call to {phone}", str(exc), org_id=org_id)
            failed += 1

        if delay > 0:
            await asyncio.sleep(delay)

    await update_campaign_run_stats(campaign_id, dispatched, failed)
    logger.info("Campaign %s done: %d dispatched, %d failed", campaign_id, dispatched, failed)


def _schedule_campaign(campaign: dict) -> None:
    """Register a campaign with APScheduler based on its schedule_type."""
    cid = campaign["id"]
    org_id = campaign["org_id"]
    stype = campaign.get("schedule_type", "once")
    stime = campaign.get("schedule_time", "09:00")

    job_id = f"campaign_{cid}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)

    if stype == "once":
        scheduler.add_job(
            run_campaign, "date",
            run_date=datetime.now(),
            args=[cid, org_id],
            id=job_id,
            replace_existing=True,
        )
    elif stype == "daily":
        h, m = (stime.split(":") + ["0"])[:2]
        scheduler.add_job(
            run_campaign, CronTrigger(hour=int(h), minute=int(m)),
            args=[cid, org_id], id=job_id, replace_existing=True,
        )
    elif stype == "weekdays":
        h, m = (stime.split(":") + ["0"])[:2]
        scheduler.add_job(
            run_campaign, CronTrigger(day_of_week="mon-fri", hour=int(h), minute=int(m)),
            args=[cid, org_id], id=job_id, replace_existing=True,
        )


# ── Pydantic models ───────────────────────────────────────────────────────────

class SingleCallRequest(BaseModel):
    phone_number: str
    lead_name: str = "there"
    business_name: str = "our company"
    service_type: str = "our service"
    system_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None


class CampaignCreate(BaseModel):
    name: str
    contacts: list
    schedule_type: str = "once"
    schedule_time: str = "09:00"
    call_delay_seconds: int = 3
    system_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None


class SettingsSave(BaseModel):
    settings: dict


class CallNotesPatch(BaseModel):
    notes: str


class AgentProfileCreate(BaseModel):
    name: str
    voice: str = "Aoede"
    model: str = "gemini-3.1-flash-live-preview"
    system_prompt: Optional[str] = None
    enabled_tools: list = []
    is_default: bool = False


class AgentProfileUpdate(BaseModel):
    name: Optional[str] = None
    voice: Optional[str] = None
    model: Optional[str] = None
    system_prompt: Optional[str] = None
    enabled_tools: Optional[list] = None
    is_default: Optional[bool] = None


class LoginRequest(BaseModel):
    email: str
    password: str


class OrganizationCreate(BaseModel):
    name: str
    slug: str
    outbound_number: Optional[str] = None
    transfer_number: Optional[str] = None
    max_calls_per_day: int = 500


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    status: Optional[str] = None
    outbound_number: Optional[str] = None
    transfer_number: Optional[str] = None
    max_calls_per_day: Optional[int] = None



class SupportTicketCreate(BaseModel):
    subject: str
    message: str
    priority: str = "normal"


class PasswordResetRequest(BaseModel):
    password: str

class SupportTicketUpdate(BaseModel):
    status: str
    admin_note: Optional[str] = None

class AdminUserCreate(BaseModel):
    email: str
    password: str
    org_id: str
    role: str = "admin"


# ── Dashboard ─────────────────────────────────────────────────────────────────


@app.get("/admin", response_class=HTMLResponse)
async def serve_admin():
    """Platform admin dashboard — served at /admin"""
    import os
    path = os.path.join(os.path.dirname(__file__), "ui", "admin.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>Admin UI not found</h1>", status_code=404)

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    # Stays public on purpose — it's a static shell with no data in it.
    # Every API call the JS inside it makes is auth-gated; a logged-out
    # visitor sees only the login form (handled client-side).
    path = os.path.join(os.path.dirname(__file__), "ui", "index.html")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse(content="<h1>OutboundAI</h1><p>UI not found. Check ui/index.html</p>")


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
@limiter.limit("5/minute")
async def login(req: LoginRequest, request: Request, response: Response):
    from db import get_user_by_email, get_organization, touch_last_login

    user_row = await get_user_by_email(req.email)
    if not user_row or not verify_password(req.password, user_row["password_hash"]):
        raise HTTPException(401, "Invalid email or password.")
    if not user_row.get("is_active", True):
        raise HTTPException(403, "This account has been disabled.")

    org = None
    if user_row.get("org_id"):
        org = await get_organization(user_row["org_id"])
        if not org:
            raise HTTPException(403, "Organization not found.")
        if org.get("status") != "active":
            raise HTTPException(403, "This account's organization is suspended.")

    token = create_access_token(user_row["id"], user_row.get("org_id"), user_row["role"])
    is_admin = user_row["role"] == "platform_admin"
    set_session_cookie(response, token, is_admin=is_admin)
    await touch_last_login(user_row["id"])
    return {"ok": True, "role": user_row["role"], "org_name": org["name"] if org else None}


@app.post("/api/auth/logout")
async def logout(response: Response):
    clear_session_cookie(response, is_admin=False)
    clear_session_cookie(response, is_admin=True)
    return {"ok": True}


@app.get("/api/auth/me")
async def me(user: CurrentUser = Depends(get_current_user)):
    from db import get_organization
    org = await get_organization(user.org_id) if user.org_id else None
    return {
        "user_id": user.user_id, "role": user.role, "org_id": user.org_id,
        "org_name": org["name"] if org else None,
        "outbound_number": org.get("outbound_number") if org else None,
    }


# ── Platform admin: tenant management ──────────────────────────────────────


@app.get("/api/health")
async def health():
    uptime = int(_time.time() - _START_TIME)
    scheduler_ok = scheduler.running
    return {
        "status": "ok" if scheduler_ok else "degraded",
        "version": "2.0.0",
        "uptime": f"{uptime // 3600}h {(uptime % 3600) // 60}m",
        "scheduler": "running" if scheduler_ok else "stopped",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

@app.get("/api/admin/organizations")
async def admin_list_orgs(user: CurrentUser = Depends(require_platform_admin)):
    from db import list_organizations
    return await list_organizations()


@app.post("/api/admin/organizations")
async def admin_create_org(req: OrganizationCreate, user: CurrentUser = Depends(require_platform_admin)):
    from db import create_organization, get_organization_by_slug
    if await get_organization_by_slug(req.slug):
        raise HTTPException(409, "Slug already in use.")
    org_id = await create_organization(req.name, req.slug, req.outbound_number, req.transfer_number, req.max_calls_per_day)
    return {"id": org_id}


@app.patch("/api/admin/organizations/{org_id}")
async def admin_update_org(org_id: str, req: OrganizationUpdate, user: CurrentUser = Depends(require_platform_admin)):
    from db import update_organization
    updates = req.model_dump(exclude_none=True)
    ok = await update_organization(org_id, updates)
    if not ok:
        raise HTTPException(404, "Organization not found")
    return {"updated": True}


@app.post("/api/admin/users")
async def admin_create_user(req: AdminUserCreate, user: CurrentUser = Depends(require_platform_admin)):
    from db import create_user, get_user_by_email, get_organization
    if not await get_organization(req.org_id):
        raise HTTPException(404, "Organization not found")
    if await get_user_by_email(req.email):
        raise HTTPException(409, "Email already registered")
    try:
        pw_hash = hash_password(req.password)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    uid = await create_user(req.email, pw_hash, req.org_id, req.role)
    return {"id": uid}


# ── Admin: platform-wide stats ────────────────────────────────────────────────

@app.get("/api/admin/stats")
async def admin_stats(user: CurrentUser = Depends(require_platform_admin)):
    """Platform-wide summary for the admin dashboard."""
    from db import _sdb
    db = _sdb()

    orgs   = db.table("organizations").select("id,name,slug,status,max_calls_per_day,created_at").execute()
    users  = db.table("users").select("id,org_id,role,last_login_at").execute()
    calls  = db.table("call_logs").select("id,org_id,outcome,duration_seconds,timestamp").execute()
    errors = db.table("error_logs").select("id,org_id,level,timestamp").execute()

    # Per-org call counts
    from collections import defaultdict
    org_calls = defaultdict(int)
    org_booked = defaultdict(int)
    org_duration = defaultdict(list)
    for c in calls.data:
        org_calls[c["org_id"]] += 1
        if c.get("outcome") == "booked":
            org_booked[c["org_id"]] += 1
        if c.get("duration_seconds"):
            org_duration[c["org_id"]].append(c["duration_seconds"])

    # Today's totals
    from datetime import date
    today = date.today().isoformat()
    today_calls = [c for c in calls.data if (c.get("timestamp") or "").startswith(today)]

    tenants = []
    for org in orgs.data:
        oid = org["id"]
        org_user_count = sum(1 for u in users.data if u["org_id"] == oid)
        avg_dur = round(sum(org_duration[oid]) / len(org_duration[oid])) if org_duration[oid] else 0
        tenants.append({
            **org,
            "total_calls": org_calls[oid],
            "booked": org_booked[oid],
            "booking_rate": round(org_booked[oid] / org_calls[oid] * 100) if org_calls[oid] else 0,
            "avg_duration": avg_dur,
            "user_count": org_user_count,
        })

    return {
        "total_tenants": len(orgs.data),
        "active_tenants": sum(1 for o in orgs.data if o["status"] == "active"),
        "total_calls_all_time": len(calls.data),
        "total_calls_today": len(today_calls),
        "total_errors_today": sum(1 for e in errors.data if (e.get("timestamp") or "").startswith(today)),
        "tenants": tenants,
    }


@app.get("/api/admin/tenants/{org_id}/calls")
async def admin_tenant_calls(org_id: str, limit: int = 50, user: CurrentUser = Depends(require_platform_admin)):
    """Drill-down: recent calls for a specific tenant."""
    from db import _sdb
    db = _sdb()
    r = db.table("call_logs").select("*").eq("org_id", org_id)\
        .order("timestamp", desc=True).limit(limit).execute()
    return r.data


# ── Support tickets ───────────────────────────────────────────────────────────

@app.get("/api/support/tickets")
async def list_my_tickets(user: CurrentUser = Depends(require_org_user)):
    """Tenant: list their own tickets."""
    from db import _sdb
    db = _sdb()
    r = db.table("support_tickets").select("*")\
        .eq("org_id", user.org_id).order("created_at", desc=True).execute()
    return r.data


@app.post("/api/support/tickets")
async def create_ticket(req: SupportTicketCreate, user: CurrentUser = Depends(require_org_user)):
    """Tenant: raise a support ticket."""
    import httpx, os
    from db import _sdb, get_organization
    db = _sdb()

    org = await get_organization(user.org_id)
    org_name = org["name"] if org else "Unknown"

    ticket_id = str(uuid.uuid4())
    db.table("support_tickets").insert({
        "id": ticket_id,
        "org_id": user.org_id,
        "org_name": org_name,
        "subject": req.subject,
        "message": req.message,
        "priority": req.priority,
        "status": "open",
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }).execute()

    # ── Slack notification ────────────────────────────────────────────────────
    slack_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if slack_url:
        priority_emoji = {"high": "🔴", "normal": "🟡", "low": "🟢"}.get(req.priority, "🟡")
        payload = {
            "text": f"{priority_emoji} *New Support Ticket* from *{org_name}*",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"{priority_emoji} Support Ticket — {org_name}"}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Subject:*\n{req.subject}"},
                    {"type": "mrkdwn", "text": f"*Priority:*\n{req.priority.capitalize()}"},
                ]},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"*Message:*\n{req.message}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f"<https://aivoice.nxtautomation.online/admin|Open Admin Dashboard>"}},
            ]
        }
        try:
            async with httpx.AsyncClient() as client:
                await client.post(slack_url, json=payload, timeout=5)
        except Exception as e:
            logger.warning("Slack notification failed: %s", e)

    # ── Email notification (via simple SMTP or just log for now) ─────────────
    # Add SMTP config to .env: ALERT_EMAIL_TO, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
    alert_email = os.getenv("ALERT_EMAIL_TO", "")
    smtp_host   = os.getenv("SMTP_HOST", "")
    if alert_email and smtp_host:
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(
                f"New support ticket from {org_name}\n\n"
                f"Subject: {req.subject}\nPriority: {req.priority}\n\n{req.message}\n\n"
                f"Manage at: https://aivoice.nxtautomation.online/admin"
            )
            msg["Subject"] = f"[OutboundAI] Support Ticket — {org_name}: {req.subject}"
            msg["From"]    = os.getenv("SMTP_USER", alert_email)
            msg["To"]      = alert_email
            with smtplib.SMTP_SSL(smtp_host, int(os.getenv("SMTP_PORT", "465"))) as s:
                s.login(os.getenv("SMTP_USER", ""), os.getenv("SMTP_PASS", ""))
                s.send_message(msg)
        except Exception as e:
            logger.warning("Email notification failed: %s", e)

    return {"id": ticket_id, "status": "open"}


@app.get("/api/admin/support/tickets")
async def admin_list_tickets(status: Optional[str] = None, user: CurrentUser = Depends(require_platform_admin)):
    """Admin: list all tickets, optionally filtered by status."""
    from db import _sdb
    db = _sdb()
    q = db.table("support_tickets").select("*").order("created_at", desc=True)
    if status:
        q = q.eq("status", status)
    r = q.execute()
    return r.data


@app.patch("/api/admin/support/tickets/{ticket_id}")
async def admin_update_ticket(ticket_id: str, req: SupportTicketUpdate, user: CurrentUser = Depends(require_platform_admin)):
    """Admin: update ticket status / add note."""
    from db import _sdb
    db = _sdb()
    updates = {
        "status": req.status,
        "updated_at": datetime.utcnow().isoformat(),
    }
    if req.admin_note is not None:
        updates["admin_note"] = req.admin_note
    db.table("support_tickets").update(updates).eq("id", ticket_id).execute()
    return {"updated": True}




# ── Admin: tenant users ───────────────────────────────────────────────────────

@app.get("/api/admin/tenants/{org_id}/users")
async def admin_tenant_users(org_id: str, user: CurrentUser = Depends(require_platform_admin)):
    from db import list_org_users
    return await list_org_users(org_id)


@app.post("/api/admin/tenants/{org_id}/users/{user_id}/reset-password")
async def admin_reset_password(org_id: str, user_id: str, req: PasswordResetRequest, user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    new_password = req.password
    if not new_password or len(new_password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")
    pw_hash = hash_password(new_password)
    db = _sdb()
    db.table("users").update({"password_hash": pw_hash}).eq("id", user_id).eq("org_id", org_id).execute()
    return {"reset": True}


@app.post("/api/admin/tenants/{org_id}/users/{user_id}/toggle")
async def admin_toggle_user(org_id: str, user_id: str, user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    db = _sdb()
    u = db.table("users").select("is_active").eq("id", user_id).eq("org_id", org_id).execute()
    if not u.data:
        raise HTTPException(404, "User not found")
    new_status = not u.data[0]["is_active"]
    db.table("users").update({"is_active": new_status}).eq("id", user_id).execute()
    return {"is_active": new_status}


# ── Admin: tenant quota usage today ──────────────────────────────────────────

@app.get("/api/admin/tenants/{org_id}/quota")
async def admin_tenant_quota(org_id: str, user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb, get_organization
    from datetime import date
    db = _sdb()
    org = await get_organization(org_id)
    if not org:
        raise HTTPException(404, "Organization not found")
    today = date.today().isoformat()
    r = db.table("org_call_counters").select("count").eq("org_id", org_id).eq("day", today).execute()
    used = r.data[0]["count"] if r.data else 0
    return {"used": used, "cap": org["max_calls_per_day"], "remaining": max(0, org["max_calls_per_day"] - used)}


# ── Admin: tenant error logs ──────────────────────────────────────────────────

@app.get("/api/admin/tenants/{org_id}/errors")
async def admin_tenant_errors(org_id: str, limit: int = 50, user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    db = _sdb()
    r = db.table("error_logs").select("*").eq("org_id", org_id).order("timestamp", desc=True).limit(limit).execute()
    return r.data


# ── Admin: platform-wide error logs ──────────────────────────────────────────

@app.get("/api/admin/errors")
async def admin_all_errors(limit: int = Query(200), level: Optional[str] = Query(None), user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    db = _sdb()
    q = db.table("error_logs").select("*").order("timestamp", desc=True).limit(limit)
    if level:
        q = q.eq("level", level)
    r = q.execute()
    return r.data


# ── Admin: platform-wide campaigns ───────────────────────────────────────────

@app.get("/api/admin/campaigns")
async def admin_all_campaigns(user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    db = _sdb()
    r = db.table("campaigns").select("id,org_id,name,status,schedule_type,schedule_time,total_dispatched,total_failed,created_at,last_run_at").order("created_at", desc=True).execute()
    campaigns = r.data or []
    orgs_r = db.table("organizations").select("id,name").execute()
    org_map = {o["id"]: o["name"] for o in (orgs_r.data or [])}
    for c in campaigns:
        c["org_name"] = org_map.get(c["org_id"], "Unknown")
    return campaigns


@app.patch("/api/admin/campaigns/{campaign_id}/status")
async def admin_update_campaign_status(campaign_id: str, status: str = Query(...), user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    db = _sdb()
    r = db.table("campaigns").update({"status": status}).eq("id", campaign_id).execute()
    if not r.data:
        raise HTTPException(404, "Campaign not found")
    return {"updated": True}


# ── Admin: export tenant calls as CSV ────────────────────────────────────────

@app.get("/api/admin/tenants/{org_id}/calls/export")
async def admin_export_calls(org_id: str, user: CurrentUser = Depends(require_platform_admin)):
    from db import _sdb
    from fastapi.responses import StreamingResponse
    import csv as _csv, io as _io
    db = _sdb()
    r = db.table("call_logs").select("*").eq("org_id", org_id).order("timestamp", desc=True).limit(5000).execute()
    output = _io.StringIO()
    writer = _csv.DictWriter(output, fieldnames=["timestamp","phone_number","lead_name","outcome","duration_seconds","notes"])
    writer.writeheader()
    for row in (r.data or []):
        writer.writerow({k: row.get(k, "") for k in writer.fieldnames})
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=calls_{org_id[:8]}.csv"}
    )


# ── Tenant: quota usage today ─────────────────────────────────────────────────

@app.get("/api/quota")
async def get_my_quota(user: CurrentUser = Depends(require_org_user)):
    from db import _sdb, get_organization
    from datetime import date
    db = _sdb()
    org = await get_organization(user.org_id)
    if not org:
        raise HTTPException(404, "Organization not found")
    today = date.today().isoformat()
    r = db.table("org_call_counters").select("count").eq("org_id", user.org_id).eq("day", today).execute()
    used = r.data[0]["count"] if r.data else 0
    cap  = org["max_calls_per_day"]
    return {"used": used, "cap": cap, "remaining": max(0, cap - used), "percent": round(used / cap * 100) if cap else 0}


# ── Tenant: export call logs as CSV ──────────────────────────────────────────

@app.get("/api/calls/export")
async def export_calls_csv(user: CurrentUser = Depends(require_org_user)):
    from db import _sdb
    from fastapi.responses import StreamingResponse
    import csv as _csv, io as _io
    db = _sdb()
    r = db.table("call_logs").select("*").eq("org_id", user.org_id).order("timestamp", desc=True).limit(5000).execute()
    output = _io.StringIO()
    writer = _csv.DictWriter(output, fieldnames=["timestamp","phone_number","lead_name","outcome","duration_seconds","notes","recording_url"])
    writer.writeheader()
    for row in r.data:
        writer.writerow({k: row.get(k, "") for k in writer.fieldnames})
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=call_logs.csv"}
    )


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats_route(user: CurrentUser = Depends(require_org_user)):
    from db import get_stats
    return await get_stats(user.org_id)


# ── Single call ───────────────────────────────────────────────────────────────

@app.post("/api/call/single")
@limiter.limit("30/minute")
async def single_call(req: SingleCallRequest, request: Request, user: CurrentUser = Depends(require_org_user)):
    try:
        phone = _validate_e164(req.phone_number)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    try:
        result = await dispatch_call(
            org_id=user.org_id, phone_number=phone, lead_name=req.lead_name,
            business_name=req.business_name, service_type=req.service_type,
            system_prompt=req.system_prompt, agent_profile_id=req.agent_profile_id,
        )
    except DispatchBlocked as exc:
        raise HTTPException(_BLOCKED_STATUS.get(exc.code, 400), exc.message)
    return result


# ══════════════════════════════════════════════════════════════════════════
# Public demo (NXT Automation showcase landing page)
# ══════════════════════════════════════════════════════════════════════════
# Unauthenticated on purpose — this is what the public-facing demo landing
# page calls. Hard-locked to ONE org + ONE agent profile via env vars, so it
# can never be used to place calls through any other tenant. Both routes are
# rate-limited per IP on top of that.
#
# Required env vars (.env):
#   DEMO_ORG_ID             — org_id for the "NXT Automation" tenant
#   DEMO_AGENT_PROFILE_ID   — agent_profiles.id for the NXT Automation profile
#   DEMO_BUSINESS_NAME      — defaults to "NXT Automation"
#   DEMO_SERVICE_TYPE       — defaults to "a live demo of the voice agent"
#   TURNSTILE_SECRET_KEY    — secret key from your Cloudflare Turnstile widget
#                             (the "Call me now" form on the demo page verifies
#                             a Turnstile token here before any real SIP call
#                             gets dispatched — protects against bot spam
#                             running up real telephony costs)
#
# Also add the landing page's domain to ALLOWED_ORIGINS if it's hosted
# somewhere other than this server, e.g.:
#   ALLOWED_ORIGINS=https://nxtautomation.com,https://www.nxtautomation.com

DEMO_ORG_ID = os.getenv("DEMO_ORG_ID", "")
DEMO_AGENT_PROFILE_ID = os.getenv("DEMO_AGENT_PROFILE_ID", "")
DEMO_BUSINESS_NAME = os.getenv("DEMO_BUSINESS_NAME", "NXT Automation")
DEMO_SERVICE_TYPE = os.getenv("DEMO_SERVICE_TYPE", "a live demo of the voice agent")


class DemoCallRequest(BaseModel):
    phone_number: str
    lead_name: str = "there"
    turnstile_token: str = ""


class DemoTokenRequest(BaseModel):
    name: str


async def _verify_turnstile(token: str, remote_ip: str) -> bool:
    """Calls Cloudflare's siteverify API. Returns True only if the token is
    genuinely valid, unexpired (5 min window), and hasn't been used before.
    The client-side widget alone protects nothing — anyone can send any
    string as turnstile_token directly to this API without it. This is the
    part that actually matters."""
    secret = os.getenv("TURNSTILE_SECRET_KEY", "")
    if not secret:
        # Not configured yet — fail closed rather than silently letting
        # everything through, so a missing env var is loud, not invisible.
        logger.warning("TURNSTILE_SECRET_KEY not set — rejecting demo call.")
        return False
    if not token:
        return False

    import httpx
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                json={"secret": secret, "response": token, "remoteip": remote_ip},
                timeout=5,
            )
        result = resp.json()
        if not result.get("success"):
            logger.warning("Turnstile verification failed: %s", result.get("error-codes"))
        return bool(result.get("success"))
    except Exception as e:
        logger.warning("Turnstile verification request failed: %s", e)
        return False


@app.post("/api/demo/call")
@limiter.limit("3/minute")
async def demo_call(req: DemoCallRequest, request: Request):
    """Public 'call me' — dispatches a real outbound call to the visitor's phone.
    Guarded by Cloudflare Turnstile since every submission costs real SIP
    telephony money regardless of whether the number is genuine."""
    if not DEMO_ORG_ID:
        raise HTTPException(503, "Demo isn't configured yet — set DEMO_ORG_ID.")

    remote_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "")
    if not await _verify_turnstile(req.turnstile_token, remote_ip):
        raise HTTPException(400, "Verification failed — please try again.")

    try:
        phone = _validate_e164(req.phone_number)
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    lead_name = (req.lead_name or "there").strip()[:60] or "there"

    try:
        await dispatch_call(
            org_id=DEMO_ORG_ID,
            phone_number=phone,
            lead_name=lead_name,
            business_name=DEMO_BUSINESS_NAME,
            service_type=DEMO_SERVICE_TYPE,
            agent_profile_id=DEMO_AGENT_PROFILE_ID or None,
        )
    except DispatchBlocked as exc:
        raise HTTPException(_BLOCKED_STATUS.get(exc.code, 400), exc.message)

    return {"success": True}


@app.post("/api/demo/token")
@limiter.limit("6/minute")
async def demo_token(req: DemoTokenRequest, request: Request):
    """Public 'push to talk' — browser mic <-> agent, live in a LiveKit room.
    No phone_number in the dispatch metadata, so agent.py just joins the room
    and waits instead of trying to SIP-dial anyone. No telephony cost per
    session, so no Turnstile gate here — the browser-mic path is already
    rate-limited and free regardless of who triggers it."""
    if not DEMO_ORG_ID:
        raise HTTPException(503, "Demo isn't configured yet — set DEMO_ORG_ID.")

    name = (req.name or "Guest").strip()[:40] or "Guest"
    room_name = f"demo-{uuid.uuid4().hex[:10]}"

    from livekit import api as lk_api

    metadata = {
        "org_id": DEMO_ORG_ID,
        "lead_name": name,
        "business_name": DEMO_BUSINESS_NAME,
        "service_type": DEMO_SERVICE_TYPE,
    }
    if DEMO_AGENT_PROFILE_ID:
        metadata["agent_profile_id"] = DEMO_AGENT_PROFILE_ID

    client = _lk()
    try:
        await client.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                agent_name="outbound-caller",
                room=room_name,
                metadata=json.dumps(metadata),
            )
        )
    finally:
        await client.aclose()

    access_token = (
        lk_api.AccessToken(os.getenv("LIVEKIT_API_KEY", ""), os.getenv("LIVEKIT_API_SECRET", ""))
        .with_identity(f"web-{uuid.uuid4().hex[:8]}")
        .with_name(name)
        .with_grants(lk_api.VideoGrants(room_join=True, room=room_name))
        .to_jwt()
    )

    return {"token": access_token, "url": os.getenv("LIVEKIT_URL", ""), "room": room_name}


# ── Batch CSV call ────────────────────────────────────────────────────────────

@app.post("/api/call/batch")
@limiter.limit("5/minute")
async def batch_call(
    request: Request,
    file: UploadFile = File(...),
    business_name: str = Query("our company"),
    service_type: str = Query("our service"),
    delay_seconds: int = Query(3),
    agent_profile_id: Optional[str] = Query(None),
    user: CurrentUser = Depends(require_org_user),
):
    content = await file.read()
    if len(content) > MAX_CSV_BYTES:
        raise HTTPException(400, f"CSV too large — max {MAX_CSV_BYTES // (1024 * 1024)}MB.")

    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    contacts = []
    skipped_invalid = 0
    for row in reader:
        if len(contacts) >= MAX_CSV_ROWS:
            break
        phone = (row.get("phone") or row.get("Phone") or row.get("phone_number") or "").strip()
        name = (row.get("name") or row.get("Name") or row.get("lead_name") or "there").strip()
        if not phone:
            continue
        try:
            phone = _validate_e164(phone)
        except ValueError:
            skipped_invalid += 1
            continue
        contacts.append({"phone": phone, "name": name})

    if not contacts:
        raise HTTPException(400, "No valid E.164 phone numbers found. Ensure a 'phone' column exists (e.g. +14155551234).")

    dispatched = 0
    failed = 0
    errors = []
    stopped_early = False

    for contact in contacts:
        try:
            await dispatch_call(
                org_id=user.org_id,
                phone_number=contact["phone"],
                lead_name=contact["name"],
                business_name=business_name,
                service_type=service_type,
                agent_profile_id=agent_profile_id,
            )
            dispatched += 1
        except DispatchBlocked as exc:
            if exc.code == "quota_exceeded":
                stopped_early = True
                break
            failed += 1
            errors.append({"phone": contact["phone"], "error": exc.message})
        except Exception as exc:
            failed += 1
            errors.append({"phone": contact["phone"], "error": str(exc)})

        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

    return {
        "dispatched": dispatched, "failed": failed,
        "skipped_invalid_numbers": skipped_invalid,
        "stopped_early_quota_exceeded": stopped_early,
        "errors": errors[:10],
    }


# ── Appointments ──────────────────────────────────────────────────────────────

@app.get("/api/appointments")
async def list_appointments(date: Optional[str] = Query(None), user: CurrentUser = Depends(require_org_user)):
    from db import get_all_appointments
    return await get_all_appointments(user.org_id, date)


@app.delete("/api/appointments/{appointment_id}")
async def cancel_appointment_route(appointment_id: str, user: CurrentUser = Depends(require_org_user)):
    from db import cancel_appointment
    ok = await cancel_appointment(user.org_id, appointment_id)
    if not ok:
        raise HTTPException(404, "Appointment not found or already cancelled")
    return {"cancelled": True}


# ── Call logs ─────────────────────────────────────────────────────────────────

@app.get("/api/calls")
async def list_calls(page: int = Query(1), limit: int = Query(20), user: CurrentUser = Depends(require_org_user)):
    from db import get_all_calls
    return await get_all_calls(user.org_id, page, limit)


@app.patch("/api/calls/{call_id}/notes")
async def update_notes(call_id: str, body: CallNotesPatch, user: CurrentUser = Depends(require_org_user)):
    from db import update_call_notes
    ok = await update_call_notes(user.org_id, call_id, body.notes)
    if not ok:
        raise HTTPException(404, "Call not found")
    return {"updated": True}


# ── CRM / Contacts ────────────────────────────────────────────────────────────

@app.get("/api/contacts")
async def list_contacts(user: CurrentUser = Depends(require_org_user)):
    from db import get_contacts
    return await get_contacts(user.org_id)


@app.get("/api/contacts/{phone}/history")
async def contact_history(phone: str, user: CurrentUser = Depends(require_org_user)):
    from db import get_calls_by_phone, get_appointments_by_phone, get_contact_memory
    calls = await get_calls_by_phone(user.org_id, phone)
    appointments = await get_appointments_by_phone(user.org_id, phone)
    memories = await get_contact_memory(user.org_id, phone)
    return {"calls": calls, "appointments": appointments, "memories": memories}


# ── Campaigns ─────────────────────────────────────────────────────────────────

@app.get("/api/campaigns")
async def list_campaigns(user: CurrentUser = Depends(require_org_user)):
    from db import get_all_campaigns
    return await get_all_campaigns(user.org_id)


@app.post("/api/campaigns")
async def create_campaign_route(req: CampaignCreate, user: CurrentUser = Depends(require_org_user)):
    from db import create_campaign as db_create, get_campaign
    contacts_json = json.dumps(req.contacts)
    cid = await db_create(
        org_id=user.org_id, name=req.name, contacts_json=contacts_json,
        schedule_type=req.schedule_type, schedule_time=req.schedule_time,
        call_delay_seconds=req.call_delay_seconds, system_prompt=req.system_prompt,
        agent_profile_id=req.agent_profile_id,
    )
    campaign = await get_campaign(cid, user.org_id)
    if campaign:
        _schedule_campaign(campaign)
    return {"id": cid}


@app.get("/api/campaigns/{campaign_id}")
async def get_campaign_detail(campaign_id: str, user: CurrentUser = Depends(require_org_user)):
    from db import get_campaign
    campaign = await get_campaign(campaign_id, user.org_id)
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    return campaign


@app.patch("/api/campaigns/{campaign_id}/status")
async def update_campaign_status_route(campaign_id: str, status: str = Query(...), user: CurrentUser = Depends(require_org_user)):
    from db import update_campaign_status
    ok = await update_campaign_status(user.org_id, campaign_id, status)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    return {"updated": True}


@app.delete("/api/campaigns/{campaign_id}")
async def delete_campaign_route(campaign_id: str, user: CurrentUser = Depends(require_org_user)):
    from db import delete_campaign as db_delete
    job_id = f"campaign_{campaign_id}"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    ok = await db_delete(user.org_id, campaign_id)
    if not ok:
        raise HTTPException(404, "Campaign not found")
    return {"deleted": True}


@app.post("/api/campaigns/{campaign_id}/run")
@limiter.limit("10/minute")
async def run_campaign_now(campaign_id: str, request: Request, user: CurrentUser = Depends(require_org_user)):
    from db import get_campaign
    campaign = await get_campaign(campaign_id, user.org_id)
    if not campaign:
        raise HTTPException(404, "Campaign not found")
    asyncio.create_task(run_campaign(campaign_id, user.org_id))
    return {"started": True}


# ── Settings (platform infra credentials — admin only) ───────────────────────

@app.get("/api/settings")
async def get_settings(user: CurrentUser = Depends(require_platform_admin)):
    from db import get_all_settings
    return await get_all_settings()


@app.post("/api/settings")
async def save_settings_route(body: SettingsSave, user: CurrentUser = Depends(require_platform_admin)):
    from db import save_settings as db_save, get_all_settings
    allowed_keys = set((await get_all_settings()).keys())
    filtered = {k: v for k, v in body.settings.items() if k in allowed_keys}
    ignored = [k for k in body.settings if k not in allowed_keys]
    await db_save(filtered)
    for k, v in filtered.items():
        if v:
            os.environ[k] = str(v)
    return {"saved": True, "ignored_keys": ignored}


# ── Error logs / Live logs ────────────────────────────────────────────────────

@app.get("/api/logs")
async def get_logs_route(
    level: Optional[str] = Query(None),
    source: Optional[str] = Query(None),
    limit: int = Query(200),
    user: CurrentUser = Depends(require_org_user),
):
    from db import get_logs
    return await get_logs(user.org_id, level, source, limit)


@app.delete("/api/logs")
async def clear_logs_route(user: CurrentUser = Depends(require_org_user)):
    from db import clear_errors
    await clear_errors(user.org_id)
    return {"cleared": True}


# ── Default prompt (backed by the org's default agent profile) ──────────────
# The dashboard's "Prompt Editor" page used to read/write a global settings key
# that nothing on the backend ever consumed during a call — a pre-existing
# dead feature. This wires it to the org's default agent_profiles row instead,
# which agent.py actually reads at dial time, so editing it here now does
# something real.

class DefaultPromptSave(BaseModel):
    system_prompt: str


@app.get("/api/default-prompt")
async def get_default_prompt(user: CurrentUser = Depends(require_org_user)):
    from db import get_default_agent_profile
    profile = await get_default_agent_profile(user.org_id)
    return {"system_prompt": profile.get("system_prompt") if profile else None}


@app.post("/api/default-prompt")
async def save_default_prompt(body: DefaultPromptSave, user: CurrentUser = Depends(require_org_user)):
    from db import get_default_agent_profile, update_agent_profile, create_agent_profile
    profile = await get_default_agent_profile(user.org_id)
    if profile:
        await update_agent_profile(user.org_id, profile["id"], {"system_prompt": body.system_prompt})
    else:
        await create_agent_profile(org_id=user.org_id, name="Default", system_prompt=body.system_prompt, is_default=True)
    return {"saved": True}


# ── Agent profiles ────────────────────────────────────────────────────────────

@app.get("/api/agent-profiles")
async def list_agent_profiles(user: CurrentUser = Depends(require_org_user)):
    from db import get_all_agent_profiles
    return await get_all_agent_profiles(user.org_id)


@app.post("/api/agent-profiles")
async def create_agent_profile_route(req: AgentProfileCreate, user: CurrentUser = Depends(require_org_user)):
    from db import create_agent_profile as db_create
    pid = await db_create(
        org_id=user.org_id, name=req.name, voice=req.voice, model=req.model,
        system_prompt=req.system_prompt, enabled_tools=json.dumps(req.enabled_tools),
        is_default=req.is_default,
    )
    return {"id": pid}


@app.patch("/api/agent-profiles/{profile_id}")
async def update_agent_profile_route(profile_id: str, req: AgentProfileUpdate, user: CurrentUser = Depends(require_org_user)):
    from db import update_agent_profile as db_update
    updates = req.model_dump(exclude_none=True)
    if "enabled_tools" in updates:
        updates["enabled_tools"] = json.dumps(updates["enabled_tools"])
    if "is_default" in updates:
        updates["is_default"] = 1 if updates["is_default"] else 0
    ok = await db_update(user.org_id, profile_id, updates)
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"updated": True}


@app.delete("/api/agent-profiles/{profile_id}")
async def delete_agent_profile_route(profile_id: str, user: CurrentUser = Depends(require_org_user)):
    from db import delete_agent_profile as db_delete
    ok = await db_delete(user.org_id, profile_id)
    if not ok:
        raise HTTPException(404, "Profile not found")
    return {"deleted": True}


@app.post("/api/agent-profiles/{profile_id}/default")
async def set_default_profile_route(profile_id: str, user: CurrentUser = Depends(require_org_user)):
    from db import set_default_agent_profile
    await set_default_agent_profile(user.org_id, profile_id)
    return {"updated": True}


# ── Lifecycle ─────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    from db import init_db, get_all_campaigns
    init_db()
    scheduler.start()
    # Re-register active campaigns across every tenant
    try:
        campaigns = await get_all_campaigns()  # org_id=None → all orgs, startup-only use
        active = [c for c in campaigns if c.get("status") == "active" and c.get("schedule_type") in ("daily", "weekdays")]
        for c in active:
            _schedule_campaign(c)
        logger.info("Loaded %d active campaigns into scheduler", len(active))
    except Exception as exc:
        logger.warning("Could not load campaigns: %s", exc)


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown(wait=False)

# ── Vobiz Recording Webhook ───────────────────────────────────────────────────
@app.post("/api/webhooks/vobiz-recording")
async def vobiz_recording_webhook(request: Request):
    """Receives recording events from Vobiz after each call completes."""
    try:
        body = await request.json()
    except Exception:
        body = await request.body()
        body = {"raw": body.decode("utf-8", errors="replace")}

    logger.info("📼 Vobiz webhook received: %s", json.dumps(body))

    # Extract fields — log first, match later once we know the payload format
    recording_url = (
        body.get("recording_url") or
        body.get("recordingUrl") or
        body.get("recording") or
        body.get("media_url") or
        body.get("url") or ""
    )
    phone_number = (
        body.get("to_number") or
        body.get("to") or
        body.get("phone_number") or
        body.get("destination") or
        body.get("called_number") or
        body.get("dest_number") or ""
    )
    call_id = body.get("call_id") or body.get("callId") or body.get("uuid") or ""

    logger.info("📼 Vobiz recording_url=%s phone=%s call_id=%s", recording_url, phone_number, call_id)

    if recording_url and phone_number:
        try:
            from db import _sdb
            db = _sdb()
            # Match by phone number, update most recent call without a recording
            r = db.table("call_logs").select("id").eq("phone_number", phone_number).is_("recording_url", "null").order("timestamp", desc=True).limit(1).execute()
            if r.data:
                db.table("call_logs").update({"recording_url": recording_url}).eq("id", r.data[0]["id"]).execute()
                logger.info("📼 Recording URL saved for %s", phone_number)
        except Exception as e:
            logger.error("📼 Failed to save recording URL: %s", e)

    return {"status": "ok"}

# ── Vobiz Recording Proxy ─────────────────────────────────────────────────────
@app.get("/api/recording/proxy")
async def proxy_recording(url: str, user: CurrentUser = Depends(require_org_user)):
    """Proxy Vobiz recording with auth headers so browser can play it."""
    import httpx
    auth_id    = os.getenv("VOBIZ_AUTH_ID", "")
    auth_token = os.getenv("VOBIZ_AUTH_TOKEN", "")
    if not auth_id or not auth_token:
        raise HTTPException(503, "Recording credentials not configured")
    if "vobiz.ai" not in url:
        raise HTTPException(400, "Invalid recording URL")
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, headers={
                "X-Auth-ID": auth_id,
                "X-Auth-Token": auth_token,
            }, timeout=30, follow_redirects=True)

        # Convert stereo to mono so both voices are audible in browser
        from fastapi.responses import Response
        import io
        try:
            from pydub import AudioSegment
            audio = AudioSegment.from_file(io.BytesIO(r.content))
            if audio.channels > 1:
                mono = audio.set_channels(1)
                buf = io.BytesIO()
                mono.export(buf, format="mp3")
                return Response(
                    content=buf.getvalue(),
                    media_type="audio/mpeg",
                    headers={"Content-Disposition": "inline"},
                )
        except Exception as conv_err:
            logger.warning("Audio conversion failed, serving raw: %s", conv_err)

        return Response(
            content=r.content,
            media_type=r.headers.get("content-type", "audio/wav"),
            headers={"Content-Disposition": "inline"},
        )
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch recording: {e}")

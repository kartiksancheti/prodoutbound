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


class AdminUserCreate(BaseModel):
    email: str
    password: str
    org_id: str
    role: str = "admin"


# ── Dashboard ─────────────────────────────────────────────────────────────────

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
    set_session_cookie(response, token)
    await touch_last_login(user_row["id"])
    return {"ok": True, "role": user_row["role"], "org_name": org["name"] if org else None}


@app.post("/api/auth/logout")
async def logout(response: Response):
    clear_session_cookie(response)
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

import os
import uuid
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict


def _default(key: str, fallback: str = "") -> str:
    """Always read live from the process environment — VPS env vars are the single source of truth."""
    return os.getenv(key, fallback)


# Platform-level secrets only. Tenant-specific config (outbound number,
# transfer number, agent profiles) lives in the organizations / agent_profiles
# tables instead — see organizations functions below.
SENSITIVE_KEYS = {
    "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET", "GOOGLE_API_KEY",
    "VOBIZ_PASSWORD", "TWILIO_AUTH_TOKEN", "SUPABASE_SERVICE_KEY",
    "AWS_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY",
    "DEEPGRAM_API_KEY", "JWT_SECRET",
}


def _sdb():
    from supabase import create_client
    return create_client(_default("SUPABASE_URL"), _default("SUPABASE_SERVICE_KEY"))


async def _adb():
    url = _default("SUPABASE_URL")
    key = _default("SUPABASE_SERVICE_KEY")
    try:
        # supabase-py >= 2.4.0 public API
        from supabase import acreate_client
        return await acreate_client(url, key)
    except (ImportError, AttributeError):
        # supabase-py 2.0.x – 2.3.x internal API
        from supabase._async.client import create_client
        return await create_client(url, key)


def init_db() -> None:
    url = _default("SUPABASE_URL")
    key = _default("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("⚠️  SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")
        return
    try:
        db = _sdb()
        db.table("settings").select("key").limit(1).execute()
        print("✅ Supabase connected")
    except Exception as exc:
        print(f"⚠️  Supabase connection failed: {exc}")
        print("   Run supabase_schema.sql in your Supabase Dashboard → SQL Editor")


# ── Platform settings (admin-only; infra credentials, not tenant config) ──

async def get_all_settings() -> dict:
    db = await _adb()
    result = await db.table("settings").select("key, value").execute()
    KNOWN_KEYS = [
        "LIVEKIT_URL", "LIVEKIT_API_KEY", "LIVEKIT_API_SECRET",
        "GOOGLE_API_KEY", "GEMINI_MODEL", "GEMINI_TTS_VOICE", "USE_GEMINI_REALTIME",
        "VOBIZ_SIP_DOMAIN", "VOBIZ_USERNAME", "VOBIZ_PASSWORD",
        "VOBIZ_OUTBOUND_NUMBER",  # fallback caller ID for orgs without their own number
        "OUTBOUND_TRUNK_ID", "DEFAULT_TRANSFER_NUMBER",  # platform-wide fallback only
        "DEEPGRAM_API_KEY", "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_FROM_NUMBER",
        "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_ENDPOINT_URL", "S3_REGION", "S3_BUCKET",
        "ENABLED_TOOLS",
    ]
    out: dict = {}
    for k in KNOWN_KEYS:
        env_val = _default(k)
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(env_val)}
        else:
            out[k] = {"value": env_val, "configured": bool(env_val)}
    for row in (result.data or []):
        k, v = row["key"], row["value"]
        if k == "TEST_KEY" or k not in KNOWN_KEYS:
            continue
        if k in SENSITIVE_KEYS:
            out[k] = {"value": "", "configured": bool(v)}
        else:
            out[k] = {"value": v, "configured": bool(v)}
    return out


async def save_settings(data: dict) -> None:
    db = await _adb()
    updated_at = datetime.now().isoformat()
    rows = [
        {"key": k, "value": str(v), "updated_at": updated_at}
        for k, v in data.items()
        if v is not None and v != ""
    ]
    if rows:
        await db.table("settings").upsert(rows, on_conflict="key").execute()


async def get_setting(key: str, default: str = "") -> str:
    db = await _adb()
    result = await db.table("settings").select("value").eq("key", key).maybe_single().execute()
    if result and result.data:
        return result.data["value"]
    return _default(key) or default


async def set_setting(key: str, value: str) -> None:
    db = await _adb()
    await db.table("settings").upsert(
        {"key": key, "value": value, "updated_at": datetime.now().isoformat()},
        on_conflict="key",
    ).execute()


async def get_enabled_tools() -> list:
    """Platform-wide default tool list, used only as a fallback when an org has no agent profile configured."""
    raw = await get_setting("ENABLED_TOOLS", "")
    if not raw:
        return []
    try:
        import json
        result = json.loads(raw)
        return result if isinstance(result, list) else []
    except Exception:
        return []


# ── Organizations ───────────────────────────────────────────────────────────

async def create_organization(
    name: str, slug: str, outbound_number: Optional[str] = None,
    transfer_number: Optional[str] = None, max_calls_per_day: int = 500,
) -> str:
    org_id = str(uuid.uuid4())
    db = await _adb()
    await db.table("organizations").insert({
        "id": org_id, "name": name, "slug": slug, "status": "active",
        "outbound_number": outbound_number, "transfer_number": transfer_number,
        "max_calls_per_day": max_calls_per_day, "created_at": datetime.now().isoformat(),
    }).execute()
    return org_id


async def get_organization(org_id: str) -> Optional[dict]:
    db = await _adb()
    result = await db.table("organizations").select("*").eq("id", org_id).maybe_single().execute()
    return result.data if result else None


async def get_organization_by_slug(slug: str) -> Optional[dict]:
    db = await _adb()
    result = await db.table("organizations").select("*").eq("slug", slug).maybe_single().execute()
    return result.data if result else None


async def list_organizations() -> list:
    db = await _adb()
    result = await db.table("organizations").select("*").order("created_at", desc=True).execute()
    return result.data or []


async def update_organization(org_id: str, updates: dict) -> bool:
    db = await _adb()
    result = await db.table("organizations").update(updates).eq("id", org_id).execute()
    return len(result.data or []) > 0


# ── Users ────────────────────────────────────────────────────────────────────

async def create_user(email: str, password_hash: str, org_id: Optional[str], role: str = "admin") -> str:
    user_id = str(uuid.uuid4())
    db = await _adb()
    await db.table("users").insert({
        "id": user_id, "org_id": org_id, "email": email.strip().lower(),
        "password_hash": password_hash, "role": role, "is_active": True,
        "created_at": datetime.now().isoformat(),
    }).execute()
    return user_id


async def get_user_by_email(email: str) -> Optional[dict]:
    db = await _adb()
    result = await db.table("users").select("*").eq("email", email.strip().lower()).maybe_single().execute()
    return result.data if result else None


async def get_user_by_id(user_id: str) -> Optional[dict]:
    db = await _adb()
    result = await db.table("users").select("*").eq("id", user_id).maybe_single().execute()
    return result.data if result else None


async def list_org_users(org_id: str) -> list:
    db = await _adb()
    result = await db.table("users").select("id, email, role, is_active, created_at, last_login_at").eq("org_id", org_id).execute()
    return result.data or []


async def touch_last_login(user_id: str) -> None:
    db = await _adb()
    await db.table("users").update({"last_login_at": datetime.now().isoformat()}).eq("id", user_id).execute()


async def set_user_active(org_id: str, user_id: str, is_active: bool) -> bool:
    """org_id is required here so one tenant can never deactivate another tenant's user by guessing an id."""
    db = await _adb()
    result = await db.table("users").update({"is_active": is_active}).eq("id", user_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


# ── Call quota (atomic, race-free under concurrent dispatch) ─────────────────

async def claim_call_slot(org_id: str) -> int:
    """Atomically increments today's counter and returns the new count.
    Caller compares against the org's max_calls_per_day — see server.py."""
    db = await _adb()
    today = datetime.now().strftime("%Y-%m-%d")
    result = await db.rpc("increment_org_call_counter", {"p_org_id": org_id, "p_day": today}).execute()
    # supabase-py returns the scalar function result in result.data
    return int(result.data) if result.data is not None else 1


async def get_call_count_today(org_id: str) -> int:
    db = await _adb()
    today = datetime.now().strftime("%Y-%m-%d")
    result = await db.table("org_call_counters").select("count").eq("org_id", org_id).eq("day", today).maybe_single().execute()
    return result.data["count"] if result and result.data else 0


# ── Error logs ────────────────────────────────────────────────────────────────

async def log_error(source: str, message: str, detail: str = "", level: str = "error", org_id: Optional[str] = None) -> None:
    try:
        db = await _adb()
        row = {
            "id": str(uuid.uuid4()), "source": source, "level": level,
            "message": message[:500], "detail": detail[:2000],
            "timestamp": datetime.now().isoformat(),
        }
        if org_id:
            row["org_id"] = org_id
        await db.table("error_logs").insert(row).execute()
    except Exception:
        pass


async def get_logs(org_id: Optional[str], level: Optional[str] = None, source: Optional[str] = None, limit: int = 200) -> list:
    """org_id=None is only valid for the platform_admin route — it returns platform-wide logs."""
    db = await _adb()
    query = db.table("error_logs").select("*").order("timestamp", desc=True).limit(limit)
    if org_id:
        query = query.eq("org_id", org_id)
    if level:
        query = query.eq("level", level)
    if source:
        query = query.eq("source", source)
    result = await query.execute()
    return result.data or []


async def clear_errors(org_id: Optional[str]) -> None:
    db = await _adb()
    query = db.table("error_logs").delete()
    if org_id:
        query = query.eq("org_id", org_id)
    else:
        query = query.neq("id", "")
    await query.execute()


# ── Appointments ──────────────────────────────────────────────────────────────

async def insert_appointment(org_id: str, name: str, phone: str, date: str, time: str, service: str) -> str:
    full_id = str(uuid.uuid4())
    booking_id = full_id[:8].upper()
    db = await _adb()
    await db.table("appointments").insert({
        "id": full_id, "org_id": org_id, "name": name, "phone": phone,
        "date": date, "time": time, "service": service,
        "status": "booked", "created_at": datetime.now().isoformat(),
    }).execute()
    return booking_id


async def check_slot(org_id: str, date: str, time: str) -> bool:
    """Returns True if slot is available (no existing booking) for this org."""
    db = await _adb()
    result = await (
        db.table("appointments").select("id")
        .eq("org_id", org_id).eq("date", date).eq("time", time).eq("status", "booked")
        .maybe_single().execute()
    )
    return result.data is None


async def get_next_available(org_id: str, date: str, time: str) -> str:
    try:
        dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError:
        dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    for _ in range(7 * 24):
        dt += timedelta(hours=1)
        if 9 <= dt.hour < 18:
            if await check_slot(org_id, dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")):
                return f"{dt.strftime('%Y-%m-%d')} at {dt.strftime('%H:%M')}"
    return "no open slots found in the next 7 days"


async def get_all_appointments(org_id: str, date_filter: Optional[str] = None) -> list:
    db = await _adb()
    query = db.table("appointments").select("*").eq("org_id", org_id).order("date").order("time")
    if date_filter:
        query = query.eq("date", date_filter)
    result = await query.execute()
    return result.data or []


async def cancel_appointment(org_id: str, appointment_id: str) -> bool:
    db = await _adb()
    result = await (
        db.table("appointments").update({"status": "cancelled"})
        .eq("id", appointment_id).eq("org_id", org_id).eq("status", "booked").execute()
    )
    return len(result.data or []) > 0


async def get_appointments_by_phone(org_id: str, phone: str) -> list:
    db = await _adb()
    result = await db.table("appointments").select("*").eq("org_id", org_id).eq("phone", phone).order("date", desc=True).execute()
    return result.data or []


# ── Call logs ─────────────────────────────────────────────────────────────────

async def log_call(
    org_id: str, phone_number: str, lead_name: Optional[str], outcome: str, reason: str,
    duration_seconds: int, recording_url: Optional[str] = None, notes: Optional[str] = None,
) -> None:
    db = await _adb()
    row: dict = {
        "id": str(uuid.uuid4()), "org_id": org_id, "phone_number": phone_number, "lead_name": lead_name,
        "outcome": outcome, "reason": reason, "duration_seconds": duration_seconds,
        "timestamp": datetime.now().isoformat(),
    }
    if recording_url:
        row["recording_url"] = recording_url
    if notes:
        row["notes"] = notes
    await db.table("call_logs").insert(row).execute()


async def get_all_calls(org_id: str, page: int = 1, limit: int = 20) -> list:
    db = await _adb()
    offset = (page - 1) * limit
    result = await (
        db.table("call_logs").select("*").eq("org_id", org_id)
        .order("timestamp", desc=True).range(offset, offset + limit - 1).execute()
    )
    return result.data or []


async def get_calls_by_phone(org_id: str, phone: str) -> list:
    db = await _adb()
    result = await db.table("call_logs").select("*").eq("org_id", org_id).eq("phone_number", phone).order("timestamp", desc=True).execute()
    return result.data or []


async def update_call_notes(org_id: str, call_id: str, notes: str) -> bool:
    db = await _adb()
    result = await db.table("call_logs").update({"notes": notes}).eq("id", call_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


async def get_contacts(org_id: str) -> list:
    db = await _adb()
    result = await db.table("call_logs").select("*").eq("org_id", org_id).order("timestamp", desc=True).execute()
    rows = result.data or []
    contacts: dict = {}
    for row in rows:
        phone = row["phone_number"]
        if phone not in contacts:
            contacts[phone] = {
                "phone_number": phone, "lead_name": row.get("lead_name"),
                "total_calls": 0, "booked": 0,
                "last_call": row["timestamp"], "last_outcome": row.get("outcome"),
            }
        contacts[phone]["total_calls"] += 1
        if row.get("outcome") == "booked":
            contacts[phone]["booked"] += 1
    return sorted(contacts.values(), key=lambda c: c["last_call"], reverse=True)


# ── Stats ─────────────────────────────────────────────────────────────────────

async def get_stats(org_id: str) -> dict:
    db = await _adb()
    rows = (await db.table("call_logs").select("outcome, duration_seconds, timestamp").eq("org_id", org_id).execute()).data or []
    total_calls    = len(rows)
    booked         = sum(1 for r in rows if r.get("outcome") == "booked")
    not_interested = sum(1 for r in rows if r.get("outcome") == "not_interested")
    durations      = [r["duration_seconds"] for r in rows if r.get("duration_seconds")]
    avg_dur        = sum(durations) / len(durations) if durations else 0
    booking_rate   = round((booked / total_calls * 100) if total_calls else 0, 1)
    outcomes: dict = {}
    for r in rows:
        o = r.get("outcome") or "unknown"
        outcomes[o] = outcomes.get(o, 0) + 1
    daily: dict = defaultdict(int)
    for r in rows:
        ts = (r.get("timestamp") or "")[:10]
        if ts:
            daily[ts] += 1
    today = datetime.now().date()
    timeline = [
        {"date": (today - timedelta(days=i)).isoformat(), "count": daily.get((today - timedelta(days=i)).isoformat(), 0)}
        for i in range(13, -1, -1)
    ]
    dur_sum: dict = defaultdict(float)
    dur_cnt: dict = defaultdict(int)
    for r in rows:
        o = r.get("outcome") or "unknown"
        sec = r.get("duration_seconds")
        if sec:
            dur_sum[o] += sec
            dur_cnt[o] += 1
    duration_by_outcome = {o: dur_sum[o] / dur_cnt[o] for o in dur_sum}
    return {
        "total_calls": total_calls, "booked": booked, "not_interested": not_interested,
        "avg_duration_seconds": round(avg_dur, 1), "booking_rate_percent": booking_rate,
        "outcomes": outcomes, "timeline": timeline, "duration_by_outcome": duration_by_outcome,
    }


# ── Campaigns ─────────────────────────────────────────────────────────────────

async def create_campaign(
    org_id: str, name: str, contacts_json: str, schedule_type: str = "once",
    schedule_time: str = "09:00", call_delay_seconds: int = 3,
    system_prompt: Optional[str] = None, agent_profile_id: Optional[str] = None,
) -> str:
    campaign_id = str(uuid.uuid4())
    db = await _adb()
    row: dict = {
        "id": campaign_id, "org_id": org_id, "name": name, "status": "active",
        "contacts_json": contacts_json, "schedule_type": schedule_type,
        "schedule_time": schedule_time, "call_delay_seconds": call_delay_seconds,
        "created_at": datetime.now().isoformat(), "total_dispatched": 0, "total_failed": 0,
    }
    if system_prompt:
        row["system_prompt"] = system_prompt
    if agent_profile_id:
        row["agent_profile_id"] = agent_profile_id
    await db.table("campaigns").insert(row).execute()
    return campaign_id


async def get_all_campaigns(org_id: Optional[str] = None) -> list:
    """org_id=None is only used internally at startup to re-register every active campaign across all tenants."""
    db = await _adb()
    query = db.table("campaigns").select("*").order("created_at", desc=True)
    if org_id:
        query = query.eq("org_id", org_id)
    result = await query.execute()
    return result.data or []


async def get_campaign(campaign_id: str, org_id: Optional[str] = None) -> Optional[dict]:
    """Pass org_id from the request whenever available — it prevents one tenant from reading
    another tenant's campaign by guessing/enumerating ids. org_id=None is only for the internal
    background runner, which already trusts the campaign_id it was scheduled with."""
    db = await _adb()
    query = db.table("campaigns").select("*").eq("id", campaign_id)
    if org_id:
        query = query.eq("org_id", org_id)
    result = await query.maybe_single().execute()
    return result.data if result else None


async def update_campaign_status(org_id: str, campaign_id: str, status: str) -> bool:
    db = await _adb()
    result = await db.table("campaigns").update({"status": status}).eq("id", campaign_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


async def update_campaign_run_stats(campaign_id: str, dispatched: int, failed: int) -> None:
    db = await _adb()
    await db.table("campaigns").update({
        "last_run_at": datetime.now().isoformat(),
        "total_dispatched": dispatched, "total_failed": failed, "status": "completed",
    }).eq("id", campaign_id).execute()


async def delete_campaign(org_id: str, campaign_id: str) -> bool:
    db = await _adb()
    result = await db.table("campaigns").delete().eq("id", campaign_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


# ── Contact Memory ────────────────────────────────────────────────────────────

async def add_contact_memory(org_id: str, phone: str, insight: str) -> None:
    db = await _adb()
    await db.table("contact_memory").insert({
        "id": str(uuid.uuid4()), "org_id": org_id, "phone_number": phone,
        "insight": insight[:1000], "created_at": datetime.now().isoformat(),
    }).execute()


async def get_contact_memory(org_id: str, phone: str) -> list:
    db = await _adb()
    result = await (
        db.table("contact_memory").select("insight, created_at")
        .eq("org_id", org_id).eq("phone_number", phone).order("created_at", desc=True).limit(20).execute()
    )
    return result.data or []


async def compress_contact_memory(org_id: str, phone: str, compressed: str) -> None:
    db = await _adb()
    await db.table("contact_memory").delete().eq("org_id", org_id).eq("phone_number", phone).execute()
    await db.table("contact_memory").insert({
        "id": str(uuid.uuid4()), "org_id": org_id, "phone_number": phone,
        "insight": compressed[:2000], "created_at": datetime.now().isoformat(),
    }).execute()


# ── Agent Profiles ────────────────────────────────────────────────────────────

async def get_all_agent_profiles(org_id: str) -> list:
    db = await _adb()
    result = await db.table("agent_profiles").select("*").eq("org_id", org_id).order("created_at").execute()
    return result.data or []


async def get_agent_profile(profile_id: str, org_id: Optional[str] = None) -> Optional[dict]:
    """Pass org_id whenever it's known (every caller except the agent worker's metadata-driven
    lookup, which validates org_id explicitly right after this call — see agent.py)."""
    db = await _adb()
    query = db.table("agent_profiles").select("*").eq("id", profile_id)
    if org_id:
        query = query.eq("org_id", org_id)
    result = await query.maybe_single().execute()
    return result.data if result else None


async def create_agent_profile(
    org_id: str, name: str, voice: str = "Aoede", model: str = "gemini-3.1-flash-live-preview",
    system_prompt: Optional[str] = None, enabled_tools: str = "[]", is_default: bool = False,
) -> str:
    profile_id = str(uuid.uuid4())
    db = await _adb()
    if is_default:
        await db.table("agent_profiles").update({"is_default": 0}).eq("org_id", org_id).execute()
    await db.table("agent_profiles").insert({
        "id": profile_id, "org_id": org_id, "name": name, "voice": voice, "model": model,
        "system_prompt": system_prompt, "enabled_tools": enabled_tools,
        "is_default": 1 if is_default else 0, "created_at": datetime.now().isoformat(),
    }).execute()
    return profile_id


async def update_agent_profile(org_id: str, profile_id: str, updates: dict) -> bool:
    db = await _adb()
    if updates.get("is_default"):
        await db.table("agent_profiles").update({"is_default": 0}).eq("org_id", org_id).execute()
    result = await db.table("agent_profiles").update(updates).eq("id", profile_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


async def delete_agent_profile(org_id: str, profile_id: str) -> bool:
    db = await _adb()
    result = await db.table("agent_profiles").delete().eq("id", profile_id).eq("org_id", org_id).execute()
    return len(result.data or []) > 0


async def set_default_agent_profile(org_id: str, profile_id: str) -> None:
    db = await _adb()
    await db.table("agent_profiles").update({"is_default": 0}).eq("org_id", org_id).execute()
    await db.table("agent_profiles").update({"is_default": 1}).eq("id", profile_id).eq("org_id", org_id).execute()


async def get_default_agent_profile(org_id: str) -> Optional[dict]:
    db = await _adb()
    result = await db.table("agent_profiles").select("*").eq("org_id", org_id).eq("is_default", 1).maybe_single().execute()
    return result.data if result else None

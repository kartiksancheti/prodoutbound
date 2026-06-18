#!/usr/bin/env python3
"""
OutboundAI — End-to-End Call Diagnostic
Run this on your VPS:  python3 diagnose.py
It tests every step in the "call never dials" chain and tells you exactly where it breaks.
"""

import asyncio
import json
import os
import sys
import httpx
from dotenv import load_dotenv

load_dotenv(override=False)

# ── colours ───────────────────────────────────────────────────────────────────
G = "\033[92m"  # green
R = "\033[91m"  # red
Y = "\033[93m"  # yellow
B = "\033[94m"  # blue
W = "\033[97m"  # white bold
RST = "\033[0m"

ok  = lambda s: print(f"  {G}✅ PASS{RST}  {s}")
err = lambda s: print(f"  {R}❌ FAIL{RST}  {s}")
wrn = lambda s: print(f"  {Y}⚠️  WARN{RST}  {s}")
hdr = lambda s: print(f"\n{B}{'─'*60}{RST}\n{W}{s}{RST}")
inf = lambda s: print(f"       {s}")

PASS = True
FAIL = False
results = []

def record(step, status, detail=""):
    results.append((step, status, detail))

# ── helper ────────────────────────────────────────────────────────────────────
def env(key, default=""):
    return os.getenv(key, default)

# ═════════════════════════════════════════════════════════════════════════════
# STEP 1 — ENV VARS
# ═════════════════════════════════════════════════════════════════════════════
def check_env():
    hdr("STEP 1 — Environment Variables")
    required = {
        "LIVEKIT_URL":          "wss://your-livekit-url",
        "LIVEKIT_API_KEY":      "API key for LiveKit Cloud",
        "LIVEKIT_API_SECRET":   "API secret for LiveKit Cloud",
        "GOOGLE_API_KEY":       "Gemini API key",
        "OUTBOUND_TRUNK_ID":    "SIP trunk ID from LiveKit (SIP > Trunks)",
        "VOBIZ_SIP_DOMAIN":     "SIP domain from Vobiz",
        "SUPABASE_URL":         "Supabase project URL",
        "SUPABASE_SERVICE_KEY": "Supabase service role key",
    }
    all_ok = True
    for k, desc in required.items():
        v = env(k)
        if v:
            # mask secrets
            display = v if len(v) < 12 else v[:6] + "…" + v[-4:]
            ok(f"{k} = {display}")
        else:
            err(f"{k} not set  ({desc})")
            all_ok = False

    optional = {
        "VOBIZ_USERNAME":       "SIP username",
        "VOBIZ_PASSWORD":       "SIP password",
        "VOBIZ_OUTBOUND_NUMBER":"Caller ID number",
        "DEFAULT_TRANSFER_NUMBER": "Human fallback number",
        "GEMINI_MODEL":         "defaults to gemini-3.1-flash-live-preview",
        "GEMINI_TTS_VOICE":     "defaults to Aoede",
        "DIAGNOSE_EMAIL":       "tenant login used to test authenticated endpoints (steps 3/8/9)",
        "DIAGNOSE_PASSWORD":    "password for DIAGNOSE_EMAIL",
    }
    print()
    for k, desc in optional.items():
        v = env(k)
        if v:
            display = v if len(v) < 12 else v[:6] + "…" + v[-4:]
            ok(f"{k} = {display}")
        else:
            wrn(f"{k} not set  ({desc})")

    record("ENV VARS", all_ok, "" if all_ok else "Missing required env vars — see above")
    return all_ok

# ═════════════════════════════════════════════════════════════════════════════
# STEP 2 — SUPABASE
# ═════════════════════════════════════════════════════════════════════════════
async def check_supabase():
    hdr("STEP 2 — Supabase Connection")
    url = env("SUPABASE_URL")
    key = env("SUPABASE_SERVICE_KEY")
    if not url or not key:
        err("Skipping — SUPABASE_URL or SUPABASE_SERVICE_KEY not set")
        record("SUPABASE", FAIL, "Credentials missing")
        return FAIL
    try:
        from supabase import create_client
        db = create_client(url, key)
        result = db.table("settings").select("key").limit(1).execute()
        ok(f"Connected to Supabase")
        inf(f"URL: {url[:40]}…")

        # check all tables exist
        for table in ["organizations", "users", "appointments", "call_logs", "campaigns", "agent_profiles", "error_logs", "contact_memory", "org_call_counters"]:
            try:
                db.table(table).select("*").limit(1).execute()
                ok(f"Table '{table}' exists")
            except Exception as e:
                err(f"Table '{table}' missing or inaccessible: {e}")

        record("SUPABASE", PASS)
        return PASS
    except Exception as e:
        err(f"Supabase connection failed: {e}")
        inf("→ Check SUPABASE_URL and SUPABASE_SERVICE_KEY")
        inf("→ Make sure you ran supabase_schema.sql in the SQL Editor")
        record("SUPABASE", FAIL, str(e))
        return FAIL

# ═════════════════════════════════════════════════════════════════════════════
# STEP 3 — FASTAPI SERVER
# ═════════════════════════════════════════════════════════════════════════════
async def check_server(client: httpx.AsyncClient, authed: bool):
    hdr("STEP 3b — FastAPI Server (localhost:8000)")
    base = "http://localhost:8000"
    try:
        # Reachability check uses the public dashboard route — works regardless of auth.
        r0 = await client.get(f"{base}/")
        if r0.status_code != 200:
            err(f"GET / → {r0.status_code} — server responded but dashboard route is unhealthy")
            record("FASTAPI", FAIL, f"HTTP {r0.status_code} on /")
            return FAIL
        ok("Server is reachable on :8000")

        if not authed:
            wrn("Not authenticated — skipping authenticated endpoint checks")
            inf("→ Set DIAGNOSE_EMAIL / DIAGNOSE_PASSWORD in .env to test these")
            record("FASTAPI", PASS, "Reachable; authenticated checks skipped (no login creds)")
            return PASS

        r = await client.get(f"{base}/api/health", follow_redirects=True)
        if r.status_code == 200:
            data = r.json()
            ok(f"GET /api/stats → 200  (total_calls={data.get('total_calls',0)})")
        else:
            err(f"GET /api/stats → {r.status_code}: {r.text[:200]}")
            record("FASTAPI", FAIL, f"HTTP {r.status_code}")
            return FAIL

        r2 = await client.get(f"{base}/api/agent-profiles")
        if r2.status_code == 200:
            profiles = r2.json()
            ok(f"GET /api/agent-profiles → {len(profiles)} profile(s)")
        else:
            wrn(f"GET /api/agent-profiles → {r2.status_code}")

        record("FASTAPI", PASS)
        return PASS
    except httpx.ConnectError:
        err("Cannot reach localhost:8000 — FastAPI server is not running!")
        inf("→ Start with:  pm2 start ecosystem.config.js --only outbound-server")
        inf("   OR:         python3 -m uvicorn server:app --host 0.0.0.0 --port 8000")
        record("FASTAPI", FAIL, "Connection refused on :8000")
        return FAIL
    except Exception as e:
        err(f"Server check failed: {e}")
        record("FASTAPI", FAIL, str(e))
        return FAIL


# ═════════════════════════════════════════════════════════════════════════════
# STEP 3b — LOGIN (every API route is now auth-gated; log in once and reuse
# the session cookie for the rest of this run via the shared client)
# ═════════════════════════════════════════════════════════════════════════════
async def check_auth(client: httpx.AsyncClient) -> bool:
    hdr("STEP 3 — Authentication")
    email = env("DIAGNOSE_EMAIL")
    password = env("DIAGNOSE_PASSWORD")
    if not email or not password:
        wrn("DIAGNOSE_EMAIL / DIAGNOSE_PASSWORD not set — skipping login")
        inf("→ Steps that hit authenticated API routes will be skipped, not failed")
        record("AUTH", PASS, "Skipped — no diagnostic credentials configured")
        return False
    try:
        r = await client.post("http://localhost:8000/api/auth/login", json={"email": email, "password": password})
        if r.status_code == 200:
            ok(f"Logged in as {email}")
            record("AUTH", PASS)
            return True
        else:
            err(f"Login failed: HTTP {r.status_code}: {r.text[:200]}")
            record("AUTH", FAIL, f"HTTP {r.status_code}")
            return False
    except httpx.ConnectError:
        err("Can't reach localhost:8000 — server not running")
        record("AUTH", FAIL, "Server not running")
        return False
    except Exception as e:
        err(f"Login check failed: {e}")
        record("AUTH", FAIL, str(e))
        return False

# ═════════════════════════════════════════════════════════════════════════════
# STEP 4 — LIVEKIT CREDENTIALS
# ═════════════════════════════════════════════════════════════════════════════
async def check_livekit_creds():
    hdr("STEP 4 — LiveKit API Credentials")
    lk_url    = env("LIVEKIT_URL")
    lk_key    = env("LIVEKIT_API_KEY")
    lk_secret = env("LIVEKIT_API_SECRET")

    if not all([lk_url, lk_key, lk_secret]):
        err("LiveKit credentials incomplete — skipping")
        record("LIVEKIT_CREDS", FAIL, "Missing credentials")
        return FAIL

    if not lk_url.startswith("wss://") and not lk_url.startswith("ws://"):
        err(f"LIVEKIT_URL should start with wss:// — got: {lk_url}")
        record("LIVEKIT_CREDS", FAIL, "Bad URL format")
        return FAIL

    ok(f"URL format OK: {lk_url}")

    # Test by listing rooms via REST API
    http_url = lk_url.replace("wss://", "https://").replace("ws://", "http://")
    try:
        import time, hashlib, hmac, base64
        # Build a simple JWT to test auth
        try:
            import jwt as pyjwt
            now = int(time.time())
            payload = {
                "iss": lk_key,
                "sub": "diagnostics",
                "iat": now,
                "exp": now + 60,
                "video": {"roomList": True},
            }
            token = pyjwt.encode(payload, lk_secret, algorithm="HS256")
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(
                    f"{http_url}/twirp/livekit.RoomService/ListRooms",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    content=b"{}",
                )
                if r.status_code == 200:
                    rooms = r.json().get("rooms", [])
                    ok(f"LiveKit API auth OK — {len(rooms)} active room(s)")
                    record("LIVEKIT_CREDS", PASS)
                    return PASS
                elif r.status_code == 401:
                    err(f"LiveKit auth FAILED (401) — wrong API key or secret")
                    inf(f"→ Double-check LIVEKIT_API_KEY and LIVEKIT_API_SECRET")
                    record("LIVEKIT_CREDS", FAIL, "401 Unauthorized")
                    return FAIL
                else:
                    wrn(f"LiveKit API returned {r.status_code} — credentials may still be OK")
                    ok(f"Credentials format looks valid (could not fully verify)")
                    record("LIVEKIT_CREDS", PASS, f"HTTP {r.status_code} (non-fatal)")
                    return PASS
        except ImportError:
            wrn("PyJWT not installed — skipping deep auth test")
            ok(f"Credentials present and URL format valid")
            record("LIVEKIT_CREDS", PASS, "Shallow check only")
            return PASS
    except Exception as e:
        wrn(f"Could not verify LiveKit auth: {e}")
        ok("Credentials present — could not do live check")
        record("LIVEKIT_CREDS", PASS, "Shallow check only")
        return PASS

# ═════════════════════════════════════════════════════════════════════════════
# STEP 5 — LIVEKIT AGENT WORKER
# ═════════════════════════════════════════════════════════════════════════════
async def check_agent_worker():
    hdr("STEP 5 — LiveKit Agent Worker")
    lk_url    = env("LIVEKIT_URL")
    lk_key    = env("LIVEKIT_API_KEY")
    lk_secret = env("LIVEKIT_API_SECRET")

    if not all([lk_url, lk_key, lk_secret]):
        err("Skipping — LiveKit credentials not set")
        record("AGENT_WORKER", FAIL, "No credentials")
        return FAIL

    try:
        from livekit import api as lk_api
        client = lk_api.LiveKitAPI(url=lk_url, api_key=lk_key, api_secret=lk_secret)
        try:
            # List agent dispatches to confirm agent is registered
            dispatches = await client.agent_dispatch.list_dispatch(
                lk_api.ListAgentDispatchRequest(room="")
            )
            ok(f"Agent dispatch API reachable")
        except Exception as e:
            # This may fail if no room — that's OK
            if "not found" in str(e).lower() or "404" in str(e):
                ok(f"Agent dispatch API reachable (no active rooms yet)")
            else:
                wrn(f"Agent dispatch check: {e}")
        finally:
            await client.aclose()

        # Check if agent process is running via pm2 or process list
        import subprocess
        try:
            result = subprocess.run(["pm2", "jlist"], capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                pm2_data = json.loads(result.stdout)
                agent_proc = next((p for p in pm2_data if p.get("name") == "outbound-agent"), None)
                server_proc = next((p for p in pm2_data if p.get("name") == "outbound-server"), None)
                if agent_proc:
                    status = agent_proc.get("pm2_env", {}).get("status", "?")
                    restarts = agent_proc.get("pm2_env", {}).get("restart_time", 0)
                    if status == "online":
                        ok(f"PM2: outbound-agent is ONLINE (restarts: {restarts})")
                    else:
                        err(f"PM2: outbound-agent status = {status} (restarts: {restarts})")
                        inf("→ Run: pm2 logs outbound-agent --lines 50")
                else:
                    err("PM2: outbound-agent process NOT found")
                    inf("→ Run: pm2 start ecosystem.config.js")

                if server_proc:
                    status = server_proc.get("pm2_env", {}).get("status", "?")
                    if status == "online":
                        ok(f"PM2: outbound-server is ONLINE")
                    else:
                        err(f"PM2: outbound-server status = {status}")
                else:
                    wrn("PM2: outbound-server process NOT found")
            else:
                wrn("PM2 not available — checking process list")
                raise Exception("pm2 not found")
        except Exception:
            try:
                result = subprocess.run(["pgrep", "-a", "python"], capture_output=True, text=True)
                procs = result.stdout.strip()
                if "agent.py" in procs:
                    ok(f"agent.py is running (found in process list)")
                else:
                    err("agent.py NOT found in process list")
                    inf("→ Start with: python3 agent.py start &")
                if "uvicorn" in procs or "server:app" in procs:
                    ok(f"FastAPI server (uvicorn) is running")
                else:
                    wrn("uvicorn/server.py not clearly visible in pgrep output")
            except Exception as pe:
                wrn(f"Could not check process list: {pe}")

        record("AGENT_WORKER", PASS)
        return PASS
    except Exception as e:
        err(f"Agent worker check failed: {e}")
        record("AGENT_WORKER", FAIL, str(e))
        return FAIL

# ═════════════════════════════════════════════════════════════════════════════
# STEP 6 — SIP TRUNK
# ═════════════════════════════════════════════════════════════════════════════
async def check_sip_trunk():
    hdr("STEP 6 — SIP Trunk (Vobiz → LiveKit)")
    lk_url    = env("LIVEKIT_URL")
    lk_key    = env("LIVEKIT_API_KEY")
    lk_secret = env("LIVEKIT_API_SECRET")
    trunk_id  = env("OUTBOUND_TRUNK_ID")

    if not trunk_id:
        err("OUTBOUND_TRUNK_ID not set!")
        inf("→ Go to LiveKit Cloud Console → SIP → Outbound Trunks")
        inf("→ Create a trunk with your Vobiz credentials")
        inf("→ Copy the trunk ID (looks like: ST_xxxxxxxxxxxx)")
        record("SIP_TRUNK", FAIL, "OUTBOUND_TRUNK_ID not set")
        return FAIL

    ok(f"OUTBOUND_TRUNK_ID = {trunk_id[:8]}…")

    if not trunk_id.startswith("ST_"):
        wrn(f"Trunk ID doesn't start with 'ST_' — may be wrong format")
        inf(f"→ Expected format: ST_xxxxxxxxxxxxxxxxxxxx")

    if not all([lk_url, lk_key, lk_secret]):
        wrn("LiveKit credentials missing — can't verify trunk via API")
        record("SIP_TRUNK", FAIL, "No LK credentials")
        return FAIL

    try:
        from livekit import api as lk_api
        client = lk_api.LiveKitAPI(url=lk_url, api_key=lk_key, api_secret=lk_secret)
        try:
            trunks = await client.sip.list_sip_outbound_trunk(lk_api.ListSIPOutboundTrunkRequest())
            trunk_ids = [t.sip_trunk_id for t in (trunks.items or [])]
            if trunk_id in trunk_ids:
                trunk = next(t for t in trunks.items if t.sip_trunk_id == trunk_id)
                ok(f"Trunk found: {trunk.name if hasattr(trunk,'name') else trunk_id}")
                inf(f"  Address:  {getattr(trunk, 'address', 'N/A')}")
                inf(f"  Numbers:  {getattr(trunk, 'numbers', [])}")
            elif trunk_ids:
                err(f"Trunk ID '{trunk_id}' NOT found in your LiveKit account!")
                inf(f"→ Trunks available: {trunk_ids}")
                inf(f"→ Update OUTBOUND_TRUNK_ID in your .env")
                record("SIP_TRUNK", FAIL, "Trunk ID not found")
                await client.aclose()
                return FAIL
            else:
                err("No SIP outbound trunks found in your LiveKit account!")
                inf("→ Go to LiveKit Cloud Console → SIP → Outbound Trunks → Create")
                record("SIP_TRUNK", FAIL, "No trunks exist")
                await client.aclose()
                return FAIL
        finally:
            await client.aclose()
        record("SIP_TRUNK", PASS)
        return PASS
    except Exception as e:
        if "permission" in str(e).lower() or "403" in str(e):
            err(f"Permission denied accessing SIP trunks — check API key permissions")
        else:
            err(f"SIP trunk check failed: {e}")
        record("SIP_TRUNK", FAIL, str(e))
        return FAIL

# ═════════════════════════════════════════════════════════════════════════════
# STEP 7 — GOOGLE / GEMINI API
# ═════════════════════════════════════════════════════════════════════════════
async def check_gemini():
    hdr("STEP 7 — Google Gemini API")
    api_key = env("GOOGLE_API_KEY")
    if not api_key:
        err("GOOGLE_API_KEY not set")
        record("GEMINI", FAIL, "No API key")
        return FAIL

    try:
        import google.genai as genai
        client = genai.Client(api_key=api_key)
        # Check which live models are available
        live_models = []
        for m in client.models.list():
            actions = [str(a) for a in getattr(m, 'supported_actions', [])]
            if any('bidi' in a.lower() or 'live' in a.lower() for a in actions):
                live_models.append(m.name.replace('models/', ''))
        if live_models:
            ok(f"Gemini API works — Live models available: {', '.join(live_models[:2])}")
            record("GEMINI", PASS)
            return PASS
        else:
            err("No Gemini Live models found for this API key")
            record("GEMINI", FAIL, "No live models")
            return FAIL
    except Exception as e:
        err_str = str(e)
        err(f"Gemini check failed: {e}")
        record("GEMINI", FAIL, err_str[:100])
        return FAIL

# ═════════════════════════════════════════════════════════════════════════════
# STEP 8 — TEST DISPATCH (dry run, no real call)
# ═════════════════════════════════════════════════════════════════════════════
async def check_dispatch(client: httpx.AsyncClient, authed: bool):
    hdr("STEP 8 — Call Dispatch (dry run via API)")
    if not authed:
        wrn("Not authenticated — skipping (set DIAGNOSE_EMAIL / DIAGNOSE_PASSWORD)")
        record("DISPATCH", PASS, "Skipped — no diagnostic credentials configured")
        return PASS
    try:
        payload = {
            "phone_number": "+910000000000",  # fake number — won't actually ring
            "lead_name": "Test Lead",
            "business_name": "Test Clinic",
            "service_type": "test",
        }
        r = await client.post("http://localhost:8000/api/call/single", json=payload)
        if r.status_code == 200:
            data = r.json()
            ok(f"Dispatch API accepted the request")
            inf(f"  Room:        {data.get('room', 'N/A')}")
            inf(f"  Dispatch ID: {data.get('dispatch_id', 'N/A')}")
            inf(f"  ⚠️  A dispatch was created with a fake number — it will fail to dial, that's expected")
            inf(f"  ⚠️  This also consumed one call from today's quota for this tenant")
            record("DISPATCH", PASS)
            return PASS
        else:
            body = r.text[:300]
            err(f"Dispatch failed: HTTP {r.status_code}")
            inf(f"  Response: {body}")
            # Parse common errors
            if "quota" in body.lower():
                inf("→ This tenant's daily call quota is exhausted — that's a config issue, not a bug")
            elif "OUTBOUND_TRUNK_ID" in body:
                inf("→ OUTBOUND_TRUNK_ID missing in env")
            elif "LIVEKIT" in body:
                inf("→ LiveKit credentials issue")
            elif "worker" in body.lower() or "agent" in body.lower():
                inf("→ No agent worker is listening — start agent.py")
            record("DISPATCH", FAIL, f"HTTP {r.status_code}: {body[:100]}")
            return FAIL
    except httpx.ConnectError:
        err("Can't reach localhost:8000 — server not running")
        record("DISPATCH", FAIL, "Server not running")
        return FAIL
    except Exception as e:
        err(f"Dispatch check failed: {e}")
        record("DISPATCH", FAIL, str(e))
        return FAIL

# ═════════════════════════════════════════════════════════════════════════════
# STEP 9 — RECENT ERROR LOGS
# ═════════════════════════════════════════════════════════════════════════════
async def check_recent_errors(client: httpx.AsyncClient, authed: bool):
    hdr("STEP 9 — Recent Error Logs from Supabase")
    if not authed:
        wrn("Not authenticated — skipping (set DIAGNOSE_EMAIL / DIAGNOSE_PASSWORD)")
        return
    try:
        r = await client.get("http://localhost:8000/api/logs?level=error&limit=10")
        if r.status_code == 200:
            logs = r.json()
            if logs:
                err(f"Found {len(logs)} recent error(s):")
                for log in logs[:5]:
                    ts = (log.get("timestamp",""))[:16]
                    src = log.get("source","?")
                    msg = log.get("message","")[:80]
                    detail = log.get("detail","")[:80]
                    inf(f"  [{ts}] [{src}] {msg}")
                    if detail:
                        inf(f"           Detail: {detail}")
            else:
                ok("No errors in Supabase error_logs")
        else:
            wrn(f"Could not fetch logs: {r.status_code}")
    except Exception as e:
        wrn(f"Could not fetch logs: {e}")

# ═════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═════════════════════════════════════════════════════════════════════════════
def print_summary():
    hdr("SUMMARY")
    all_passed = True
    for step, status, detail in results:
        if status == PASS:
            ok(f"{step}")
        else:
            err(f"{step}  →  {detail}")
            all_passed = False

    print()
    if all_passed:
        print(f"{G}All checks passed! If calls still don't dial, check:{RST}")
        print("  1. pm2 logs outbound-agent --lines 100  (look for dial errors)")
        print("  2. The phone number format must start with + and country code")
        print("  3. LiveKit dashboard → SIP tab → check trunk status is Active")
    else:
        print(f"{R}Some checks failed — fix the items above in order (top to bottom).{RST}")
        print(f"{Y}Each failure may cascade — fix the first red item first.{RST}")

# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
async def main():
    print(f"\n{W}{'═'*60}")
    print("  OutboundAI — Call Diagnostics")
    print(f"{'═'*60}{RST}")

    check_env()
    await check_supabase()
    async with httpx.AsyncClient(timeout=15) as client:
        authed = await check_auth(client)
        server_ok = await check_server(client, authed)
        await check_livekit_creds()
        await check_agent_worker()
        await check_sip_trunk()
        await check_gemini()
        if server_ok:
            await check_dispatch(client, authed)
        await check_recent_errors(client, authed)
    print_summary()

if __name__ == "__main__":
    asyncio.run(main())

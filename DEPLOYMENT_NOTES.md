# OutboundAI — Multi-Tenant Migration & Deployment Notes

This pass converts OutboundAI from a single-tenant demo into a multi-tenant
platform with real authentication, removes Cal.com entirely, and gives each
business its own outbound caller ID. VPS hardening (nginx/TLS/firewall),
load testing, and monitoring/backups are **separate, later passes** — this
one is the data model + auth + API hardening foundation everything else sits on.

## What changed

**Multi-tenancy.** Two new tables, `organizations` and `users`. Every
tenant-owned table (`appointments`, `call_logs`, `campaigns`,
`agent_profiles`, `contact_memory`, `error_logs`) gained an `org_id` column.
Every database function in `db.py` now takes `org_id` and filters by it —
that application-level filter is the real tenant boundary. Postgres RLS is
also turned on with deny-all policies as a defense-in-depth backstop, but
since the app connects with the Supabase **service role key**, which
bypasses RLS entirely, RLS isn't what's actually isolating tenants day to
day — don't mistake "RLS is on" for "tenants are isolated."

**Real authentication.** `auth.py` is new: bcrypt password hashing, JWT
sessions delivered as an httpOnly cookie (so JS on the page can never read
the token), with a Bearer-header fallback for scripts. Every API route
except `/api/auth/login` now requires a logged-in user. There's no public
self-serve signup — you create each tenant with `create_tenant.py` (see
below). `/api/settings` (the infrastructure credentials — LiveKit, Google,
Supabase, etc.) is now platform-admin-only; it was previously wide open to
anyone with no auth at all, which meant anyone who found the dashboard URL
could overwrite your LiveKit or Supabase credentials.

**Cal.com — removed entirely.** `book_calcom` / `cancel_calcom` are deleted
from `tools.py`, not just disabled. The `CALCOM_*` settings group is gone
from the dashboard, and `calcom_booking_uid` is dropped from the
`appointments` table. The internal `appointments` table remains the single
source of truth for bookings, same as before.

**Per-tenant phone numbers.** `organizations.outbound_number` holds each
business's caller ID. You keep one shared Vobiz/LiveKit outbound trunk —
no need to provision a separate trunk per tenant. At dial time, `agent.py`
passes the org's number via LiveKit's `sip_number` field on
`CreateSIPParticipantRequest` ("Optional SIP From number to use"). **You
still need to confirm with Vobiz that each tenant's number is allow-listed
as a valid "From" on your trunk account** — SIP providers generally restrict
which numbers an account can present as caller ID, to prevent spoofing.
`organizations.transfer_number` is the same idea for "transfer to a human" —
previously every tenant's transfer calls went to one hardcoded global
number (almost certainly yours), which would have been a real bug for every
business except the first one.

**API hardening.** CORS now reads `ALLOWED_ORIGINS` from env (comma-
separated) instead of `*`; the dashboard is served same-origin so it doesn't
need CORS at all, this only matters if you ever add a separate frontend.
Rate limiting (slowapi) on login (5/min), single-call dispatch (30/min),
batch upload (5/min), and manual campaign runs (10/min) — IP-based, to blunt
brute-force and runaway-script abuse. Each org now has `max_calls_per_day`,
enforced with an atomic "claim slot, then validate" counter (a Postgres
function, `increment_org_call_counter`) so concurrent dispatches — e.g. a
campaign blasting through its contact list — can't race past the cap.
Phone numbers are validated as E.164 before dispatch. CSV batch upload is
capped at 2MB / 2000 rows and validates each number, where before it was
unbounded.

**The Prompt Editor page was already broken, fixed it along the way.** It
saved to a `DEFAULT_SYSTEM_PROMPT` settings key that nothing on the backend
ever read back during a call — pre-existing in the original code, not
something this pass introduced. Since `/api/settings` is now admin-only
anyway, leaving it wired that way would have made it silently fail for every
tenant. It's now backed by the org's default `agent_profiles` row instead,
which `agent.py` actually reads at dial time — so editing it now does
something real for the first time.

**`diagnose.py`** now logs in (via `DIAGNOSE_EMAIL` / `DIAGNOSE_PASSWORD` env
vars) before hitting any API route, since everything is auth-gated now. If
those aren't set, it skips the authenticated checks with a clear note
instead of reporting them as failures.

## Deploying this

1. **Run the schema migration.** Open `supabase_schema.sql` in the Supabase
   SQL Editor and run the whole file — every statement is `IF NOT EXISTS` /
   idempotent, safe to run against your existing database. It adds
   `organizations`, `users`, `org_id` columns everywhere, the call-quota
   counter table and function, RLS policies, and drops the unused
   `calcom_booking_uid` column.

2. **Add new required env vars** to `.env` on the VPS:
   ```
   JWT_SECRET=<generate with: python3 -c "import secrets; print(secrets.token_hex(32))">
   ALLOWED_ORIGINS=                      # leave empty unless you add a separate frontend origin
   COOKIE_SECURE=true                    # see TLS warning below
   ```

3. **Install new dependencies:**
   ```
   pip install -r requirements.txt --break-system-packages
   ```
   (adds `pyjwt`, `bcrypt`, `slowapi`)

4. **Create your first tenant:**
   ```
   python3 create_tenant.py
   ```
   Walks you through business name, outbound number, transfer number, daily
   call cap, and the first login (role `owner`). You'll need to do this once
   per business you onboard — there's no self-serve signup yet.

5. **Restart both PM2 processes** so they pick up the new code and env vars.

## Things you need to know before going live

**COOKIE_SECURE and TLS.** The session cookie defaults to `Secure`, meaning
browsers won't send it over plain HTTP. If you're not behind TLS yet (the
nginx/TLS setup is the next phase, not done in this pass), login will
silently fail — the cookie gets set but never sent back. Either get TLS in
place before flipping this on for real users, or set `COOKIE_SECURE=false`
temporarily for plain-HTTP testing only, and switch it back before any real
tenant logs in over the open internet.

**Vobiz caller-ID allow-listing.** Set `outbound_number` for a tenant before
relying on it — if Vobiz hasn't allow-listed that number on your account,
calls will likely still go out using the trunk's default number instead of
silently failing, so test it, don't assume.

**India telemarketing regulations (TRAI/TCCCPR).** The Vobiz SIP integration
and `Asia/Kolkata` default elsewhere in the codebase suggest your tenants
are likely calling into India. Outbound cold-calling in India is subject to
TRAI's telecom commercial communication rules — DND registry checks,
registered 140-series headers for promotional calls, and related compliance
obligations. This is a legal/regulatory question for you and your tenants
to verify, not something code changes can resolve — flagging it here so it
doesn't get missed.

**What still isn't done.** This pass is the data model, auth, and API
hardening foundation. Three things remain, deliberately scoped as separate
follow-ups so each can be reviewed on its own:
- VPS hardening — nginx + TLS, ufw firewall rules, fail2ban, process
  supervision beyond PM2's defaults, log rotation.
- Load testing — concurrent LiveKit/Gemini Live sessions, Supabase
  connection pooling under load, SIP trunk concurrent-call limits, and
  whether the agent worker's current `instances: 1` PM2 config handles your
  target volume (50/day pilot ramping to 50–500/day) or needs adjustment.
- Monitoring, alerting, and backups.

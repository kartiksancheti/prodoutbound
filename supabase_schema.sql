-- ═══════════════════════════════════════════════════════
-- OutboundAI — Complete Database Schema
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS appointments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    service TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'booked',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS call_logs (
    id TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    lead_name TEXT,
    outcome TEXT,
    reason TEXT,
    duration_seconds INTEGER,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_logs (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'error',
    message TEXT NOT NULL,
    detail TEXT,
    timestamp TEXT NOT NULL
);

ALTER TABLE appointments  DISABLE ROW LEVEL SECURITY;
ALTER TABLE call_logs     DISABLE ROW LEVEL SECURITY;
ALTER TABLE settings      DISABLE ROW LEVEL SECURITY;
ALTER TABLE error_logs    DISABLE ROW LEVEL SECURITY;

ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS recording_url TEXT;
ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS notes TEXT;

CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    contacts_json TEXT NOT NULL DEFAULT '[]',
    schedule_type TEXT NOT NULL DEFAULT 'once',
    schedule_time TEXT DEFAULT '09:00',
    call_delay_seconds INTEGER DEFAULT 3,
    system_prompt TEXT,
    created_at TEXT NOT NULL,
    last_run_at TEXT,
    total_dispatched INTEGER DEFAULT 0,
    total_failed INTEGER DEFAULT 0
);
ALTER TABLE campaigns DISABLE ROW LEVEL SECURITY;

ALTER TABLE appointments ADD COLUMN IF NOT EXISTS calcom_booking_uid TEXT;

CREATE TABLE IF NOT EXISTS contact_memory (
    id TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    insight TEXT NOT NULL,
    created_at TEXT NOT NULL
);
ALTER TABLE contact_memory DISABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_contact_memory_phone ON contact_memory (phone_number);

ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS agent_profile_id TEXT;

CREATE TABLE IF NOT EXISTS agent_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    voice TEXT NOT NULL DEFAULT 'Aoede',
    model TEXT NOT NULL DEFAULT 'gemini-3.1-flash-live-preview',
    system_prompt TEXT,
    enabled_tools TEXT DEFAULT '[]',
    is_default INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);
ALTER TABLE agent_profiles DISABLE ROW LEVEL SECURITY;

-- ═══════════════════════════════════════════════════════
-- Migration 002 — Multi-tenant foundation + RLS hardening
-- Safe to re-run: every statement is idempotent.
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',       -- active | suspended
    outbound_number TEXT,                         -- this org's caller ID (E.164), provisioned on the shared Vobiz trunk
    max_calls_per_day INTEGER NOT NULL DEFAULT 500,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    org_id TEXT REFERENCES organizations(id) ON DELETE CASCADE,  -- NULL only for platform_admin
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'admin',           -- platform_admin | owner | admin | agent
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TEXT NOT NULL,
    last_login_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_users_org ON users(org_id);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- Tag every tenant-owned table with org_id
ALTER TABLE appointments    ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);
ALTER TABLE call_logs       ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);
ALTER TABLE campaigns       ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);
ALTER TABLE agent_profiles  ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);
ALTER TABLE contact_memory  ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);
ALTER TABLE error_logs      ADD COLUMN IF NOT EXISTS org_id TEXT REFERENCES organizations(id);

CREATE INDEX IF NOT EXISTS idx_appointments_org   ON appointments(org_id);
CREATE INDEX IF NOT EXISTS idx_call_logs_org       ON call_logs(org_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_org       ON campaigns(org_id);
CREATE INDEX IF NOT EXISTS idx_agent_profiles_org  ON agent_profiles(org_id);
CREATE INDEX IF NOT EXISTS idx_contact_memory_org  ON contact_memory(org_id);
CREATE INDEX IF NOT EXISTS idx_error_logs_org      ON error_logs(org_id);

-- Track daily call volume per org for quota enforcement (cheap counter table beats COUNT(*) on call_logs at scale)
CREATE TABLE IF NOT EXISTS org_call_counters (
    org_id TEXT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    day TEXT NOT NULL,           -- YYYY-MM-DD
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (org_id, day)
);

-- ── RLS ───────────────────────────────────────────────────────────────────
-- IMPORTANT: the app connects with the Supabase SERVICE ROLE key, which
-- bypasses RLS entirely. Tenant isolation is enforced in application code
-- (every query is filtered by org_id derived from the verified JWT).
-- These policies are a defense-in-depth safety net: if the anon/public key
-- ever leaks or gets used directly from a browser, it reads/writes nothing.
ALTER TABLE organizations   ENABLE ROW LEVEL SECURITY;
ALTER TABLE users            ENABLE ROW LEVEL SECURITY;
ALTER TABLE appointments    ENABLE ROW LEVEL SECURITY;
ALTER TABLE call_logs       ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns       ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_profiles  ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_memory  ENABLE ROW LEVEL SECURITY;
ALTER TABLE error_logs      ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings        ENABLE ROW LEVEL SECURITY;
ALTER TABLE org_call_counters ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS deny_all_organizations   ON organizations;
DROP POLICY IF EXISTS deny_all_users           ON users;
DROP POLICY IF EXISTS deny_all_appointments    ON appointments;
DROP POLICY IF EXISTS deny_all_call_logs       ON call_logs;
DROP POLICY IF EXISTS deny_all_campaigns       ON campaigns;
DROP POLICY IF EXISTS deny_all_agent_profiles  ON agent_profiles;
DROP POLICY IF EXISTS deny_all_contact_memory  ON contact_memory;
DROP POLICY IF EXISTS deny_all_error_logs      ON error_logs;
DROP POLICY IF EXISTS deny_all_settings        ON settings;
DROP POLICY IF EXISTS deny_all_org_call_counters ON org_call_counters;

CREATE POLICY deny_all_organizations    ON organizations    FOR ALL USING (false);
CREATE POLICY deny_all_users            ON users             FOR ALL USING (false);
CREATE POLICY deny_all_appointments     ON appointments      FOR ALL USING (false);
CREATE POLICY deny_all_call_logs        ON call_logs         FOR ALL USING (false);
CREATE POLICY deny_all_campaigns        ON campaigns         FOR ALL USING (false);
CREATE POLICY deny_all_agent_profiles   ON agent_profiles    FOR ALL USING (false);
CREATE POLICY deny_all_contact_memory   ON contact_memory    FOR ALL USING (false);
CREATE POLICY deny_all_error_logs       ON error_logs        FOR ALL USING (false);
CREATE POLICY deny_all_settings         ON settings          FOR ALL USING (false);
CREATE POLICY deny_all_org_call_counters ON org_call_counters FOR ALL USING (false);

-- Cal.com is gone — this column is now unused dead weight, safe to drop.
ALTER TABLE appointments DROP COLUMN IF EXISTS calcom_booking_uid;

-- Each tenant's own "transfer to a human" destination — without this, every
-- tenant's transfer_to_human call would ring whatever number is in the
-- platform-wide DEFAULT_TRANSFER_NUMBER env var, i.e. probably your phone.
ALTER TABLE organizations ADD COLUMN IF NOT EXISTS transfer_number TEXT;

-- Atomic "claim a call slot" — avoids the check-then-act race where two
-- concurrent dispatches (e.g. a campaign blast) both read count=499/500 and
-- both proceed. Increments first; the caller rejects the call if the
-- returned count exceeds the org's cap (the counter staying incremented on
-- a rejected attempt is intentional — it stops retry-hammering from sneaking
-- extra calls through).
CREATE OR REPLACE FUNCTION increment_org_call_counter(p_org_id TEXT, p_day TEXT)
RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    INSERT INTO org_call_counters (org_id, day, count)
    VALUES (p_org_id, p_day, 1)
    ON CONFLICT (org_id, day)
    DO UPDATE SET count = org_call_counters.count + 1
    RETURNING count INTO new_count;
    RETURN new_count;
END;
$$ LANGUAGE plpgsql;

import asyncio
import json
import logging
import logging.handlers
import os
import ssl
import certifi
from typing import Optional

import aiohttp
from dotenv import load_dotenv

_orig_ssl = ssl.create_default_context
def _certifi_ssl(purpose=ssl.Purpose.SERVER_AUTH, **kwargs):
    if not kwargs.get("cafile") and not kwargs.get("capath") and not kwargs.get("cadata"):
        kwargs["cafile"] = certifi.where()
    return _orig_ssl(purpose, **kwargs)
ssl.create_default_context = _certifi_ssl

from livekit import agents, api
from livekit.agents import Agent, AgentSession, RoomInputOptions
from livekit.plugins import noise_cancellation, silero

from db import init_db, log_error, get_enabled_tools, get_agent_profile
from prompts import build_prompt
from tools import AppointmentTools

load_dotenv(override=False)

# ── File-based logging so PM2 captures everything ────────────────────────────
_LOG_FILE = os.path.join(os.path.dirname(__file__), "agent_calls.log")

# Single handler — PM2 captures stdout, file is backup
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
# Clear any existing handlers to prevent duplicates
_root.handlers.clear()
_sh = logging.StreamHandler()
_sh.setFormatter(_fmt)
_fh = logging.handlers.RotatingFileHandler(_LOG_FILE, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8")
_fh.setFormatter(_fmt)
_root.addHandler(_sh)
_root.addHandler(_fh)
_root.propagate = False
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("livekit.agents").setLevel(logging.WARNING)
logging.getLogger("livekit.plugins.google").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

logger = logging.getLogger("agent")

# ── NC model cached at module level ──────────────────────────────────────────
_NC_MODEL: Optional[noise_cancellation.BVCTelephony] = None

def _get_nc() -> noise_cancellation.BVCTelephony:
    global _NC_MODEL
    if _NC_MODEL is None:
        _NC_MODEL = noise_cancellation.BVCTelephony()
        logger.info("✅ NC model cached")
    return _NC_MODEL

try:
    _get_nc()
except Exception:
    pass


async def _log(level: str, msg: str, detail: str = "") -> None:
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        pass


def load_db_settings_to_env() -> None:
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_SERVICE_KEY", "")
    if not url or not key:
        return
    try:
        from supabase import create_client
        client = create_client(url, key)
        result = client.table("settings").select("key, value").execute()
        for row in (result.data or []):
            if row.get("value"):
                os.environ[row["key"]] = row["value"]
    except Exception as exc:
        logger.warning("Could not load DB settings: %s", exc)


# ── Google plugin detection ───────────────────────────────────────────────────
_google_realtime = None
_google_beta_realtime = None
_google_llm = None
_google_tts = None

try:
    from livekit.plugins import google as _gp
    try:
        _google_realtime = _gp.realtime.RealtimeModel
    except AttributeError:
        pass
    try:
        _google_beta_realtime = _gp.beta.realtime.RealtimeModel
    except AttributeError:
        pass
    try:
        _google_llm = _gp.LLM
        _google_tts = _gp.TTS
    except AttributeError:
        pass
except ImportError:
    logger.warning("livekit-plugins-google not installed")

_deepgram_stt = None
try:
    from livekit.plugins import deepgram as _dg
    _deepgram_stt = _dg.STT
except ImportError:
    pass


def _build_session(tools: list, system_prompt: str) -> AgentSession:
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-live-001")

    if gemini_model in "gemini-2.0-flash":
        logger.warning("⚠️  Model '%s' is deprecated — switching to gemini-2.0-flash-live-001", gemini_model)
        gemini_model = "gemini-2.0-flash-live-001"

    gemini_voice = os.getenv("GEMINI_TTS_VOICE", "Aoede")
    use_realtime = os.getenv("USE_GEMINI_REALTIME", "true").lower() != "false"
    RealtimeClass = _google_realtime or (_google_beta_realtime if use_realtime else None)

    if use_realtime and RealtimeClass is not None:
        logger.info("🎙  Gemini Live | model=%s voice=%s", gemini_model, gemini_voice)

        kw: dict = dict(model=gemini_model, voice=gemini_voice, instructions=system_prompt)

        try:
            from google.genai import types as _gt
            kw["realtime_input_config"] = _gt.RealtimeInputConfig(
                automatic_activity_detection=_gt.AutomaticActivityDetection(
                    end_of_speech_sensitivity=_gt.EndSensitivity.END_SENSITIVITY_HIGH,
                    silence_duration_ms=800,
                    prefix_padding_ms=100,
                ),
            )
            kw["session_resumption"] = _gt.SessionResumptionConfig(transparent=True)
            kw["proactivity"] = True
            kw["api_version"] = "v1alpha"
            kw["context_window_compression"] = _gt.ContextWindowCompressionConfig(
                trigger_tokens=25600,
                sliding_window=_gt.SlidingWindow(target_tokens=12800),
            )
            logger.info("✅ VAD config applied — silence_duration_ms=800")
        except Exception as e:
            logger.warning("VAD config skipped: %s", e)

        return AgentSession(llm=RealtimeClass(**kw), tools=tools)

    if _google_llm is None:
        raise RuntimeError("No Google AI backend available.")

    logger.info("🎙  Pipeline mode | Deepgram STT + Gemini LLM + Google TTS")
    stt = _deepgram_stt(model="nova-2-phonecall") if _deepgram_stt else None
    tts = _google_tts() if _google_tts else None
    vad = silero.VAD.load()
    return AgentSession(stt=stt, llm=_google_llm(model=gemini_model), tts=tts, vad=vad, tools=tools)


def _prewarm(proc: agents.JobProcess) -> None:
    logger.info("🔥 Prewarming Gemini connection...")
    try:
        from prompts import DEFAULT_SYSTEM_PROMPT
        dummy_prompt = DEFAULT_SYSTEM_PROMPT.format(
            lead_name="there", business_name="our company", service_type="our service"
        )
        proc.userdata["warm_session"] = _build_session([], dummy_prompt)
        logger.info("✅ Gemini WS pre-warmed successfully")
    except Exception as e:
        logger.warning("Pre-warm failed (non-fatal): %s", e)
        proc.userdata["warm_session"] = None


# ── FIX: Silence watchdog — force disconnect after N seconds of no audio ──────
SILENCE_TIMEOUT_SEC = 45

async def _silence_watchdog(sip_hungup: asyncio.Event, ctx: agents.JobContext,
                             tool_ctx, phone_number: str) -> None:
    """
    Monitors silence by polling a shared timestamp updated by audio frame events.
    Uses a simple time-based check every 2s instead of async track subscription
    to avoid livekit SDK version compatibility issues.
    """
    logger.info("🔇 Silence watchdog started — %ds timeout", SILENCE_TIMEOUT_SEC)

    # Use a mutable container so the callback can update it
    state = {"last_audio_time": asyncio.get_event_loop().time(), "audio_seen": False}

    from livekit import rtc

    def _on_track_subscribed(track, publication, participant):
        if not hasattr(track, 'kind') or track.kind != rtc.TrackKind.KIND_AUDIO:
            return
        logger.info("🔇 Watchdog subscribed to audio track: %s", participant.identity)

        async def _consume():
            try:
                stream = rtc.AudioStream(track=track)
                async for ev in stream:
                    if sip_hungup.is_set():
                        break
                    state["last_audio_time"] = asyncio.get_event_loop().time()
                    state["audio_seen"] = True
            except Exception as e:
                logger.warning("🔇 Audio stream error: %s", e)

        asyncio.ensure_future(_consume())

    # Also check existing tracks already subscribed before watchdog started
    for participant in ctx.room.remote_participants.values():
        for pub in participant.track_publications.values():
            if pub.track and hasattr(pub.track, 'kind') and pub.track.kind == rtc.TrackKind.KIND_AUDIO:
                logger.info("🔇 Watchdog found existing audio track: %s", participant.identity)

                async def _consume_existing(t=pub.track):
                    try:
                        stream = rtc.AudioStream(track=t)
                        async for ev in stream:
                            if sip_hungup.is_set():
                                break
                            state["last_audio_time"] = asyncio.get_event_loop().time()
                            state["audio_seen"] = True
                    except Exception as e:
                        logger.warning("🔇 Existing audio stream error: %s", e)

                asyncio.ensure_future(_consume_existing())

    ctx.room.on("track_subscribed", _on_track_subscribed)

    # Wait up to 20s for first audio (ring + answer delay)
    for _ in range(20):
        if sip_hungup.is_set():
            return
        await asyncio.sleep(1.0)
        if state["audio_seen"]:
            break
    else:
        logger.warning("🔇 No audio ever received after 20s — force-ending call")
        await _log("warning", "Force-ended: no audio received", f"phone={phone_number}")
        sip_hungup.set()
        try:
            await tool_ctx.end_call(outcome="no_answer", reason="no audio received")
        except Exception:
            pass
        try:
            await ctx.room.disconnect()
        except Exception:
            pass
        return

    logger.info("🔇 Audio confirmed — watching for %ds silence", SILENCE_TIMEOUT_SEC)

    # Main watchdog loop — check every 2s
    while not sip_hungup.is_set():
        await asyncio.sleep(2.0)
        if sip_hungup.is_set():
            break
        elapsed = asyncio.get_event_loop().time() - state["last_audio_time"]
        if elapsed >= SILENCE_TIMEOUT_SEC:
            logger.warning("🔇 %ds silence — force-ending call for %s",
                           SILENCE_TIMEOUT_SEC, phone_number)
            await _log("warning", f"Force-ended after {SILENCE_TIMEOUT_SEC}s silence",
                       f"phone={phone_number}")
            sip_hungup.set()
            try:
                await tool_ctx.end_call(outcome="no_answer", reason=f"{SILENCE_TIMEOUT_SEC}s silence")
            except Exception:
                pass
            try:
                await ctx.room.disconnect()
            except Exception:
                pass
            return

    logger.info("🔇 Silence watchdog done")


# ── Browser (push-to-talk) safety net ─────────────────────────────────────
# The model doesn't always call end_call() even after saying its closing
# line out loud — without this, a browser demo session can hang open
# indefinitely (LiveKit force-kills the job ~2 min later with no clean
# end_call logged). Mirrors _silence_watchdog used for phone calls.
BROWSER_SILENCE_TIMEOUT_SEC = 45


async def _browser_silence_watchdog(done: asyncio.Event, ctx: agents.JobContext, tool_ctx) -> None:
    logger.info("🔇 Browser silence watchdog started — %ds timeout", BROWSER_SILENCE_TIMEOUT_SEC)
    state = {"last_audio_time": asyncio.get_event_loop().time()}

    from livekit import rtc

    def _on_track_subscribed(track, publication, participant):
        if not hasattr(track, "kind") or track.kind != rtc.TrackKind.KIND_AUDIO:
            return

        async def _consume():
            try:
                stream = rtc.AudioStream(track=track)
                async for ev in stream:
                    if done.is_set():
                        break
                    state["last_audio_time"] = asyncio.get_event_loop().time()
            except Exception as e:
                logger.warning("🔇 Browser watchdog audio stream error: %s", e)

        asyncio.ensure_future(_consume())

    ctx.room.on("track_subscribed", _on_track_subscribed)
    for participant in ctx.room.remote_participants.values():
        for pub in participant.track_publications.values():
            if pub.track and hasattr(pub.track, "kind") and pub.track.kind == rtc.TrackKind.KIND_AUDIO:
                async def _consume_existing(t=pub.track):
                    try:
                        stream = rtc.AudioStream(track=t)
                        async for ev in stream:
                            if done.is_set():
                                break
                            state["last_audio_time"] = asyncio.get_event_loop().time()
                    except Exception as e:
                        logger.warning("🔇 Browser watchdog existing audio error: %s", e)
                asyncio.ensure_future(_consume_existing())

    while not done.is_set():
        await asyncio.sleep(2.0)
        if done.is_set():
            break
        elapsed = asyncio.get_event_loop().time() - state["last_audio_time"]
        if elapsed >= BROWSER_SILENCE_TIMEOUT_SEC:
            logger.warning("🔇 %ds silence — force-ending browser session %s",
                           BROWSER_SILENCE_TIMEOUT_SEC, ctx.room.name)
            try:
                await tool_ctx.end_call(outcome="no_answer", reason=f"{BROWSER_SILENCE_TIMEOUT_SEC}s silence, browser demo")
            except Exception:
                pass
            try:
                await ctx.room.disconnect()
            except Exception:
                pass
            done.set()
            return


# ── FIX: LiveKit Egress recording to S3 ──────────────────────────────────────
async def _start_recording(ctx: agents.JobContext, room_name: str) -> Optional[str]:
    """
    Starts a LiveKit Egress room composite recording.
    Saves to S3 and returns the recording URL, or None if not configured.
    """
    s3_bucket   = os.getenv("S3_BUCKET", "")
    s3_key_id   = os.getenv("S3_ACCESS_KEY_ID", "")
    s3_secret   = os.getenv("S3_SECRET_ACCESS_KEY", "")
    s3_endpoint = os.getenv("S3_ENDPOINT_URL", "")
    s3_region   = os.getenv("S3_REGION", "ap-south-1")

    if not all([s3_bucket, s3_key_id, s3_secret]):
        logger.info("⏺  Recording skipped — S3 not configured")
        return None

    try:
        from livekit.api.egress_service import EgressService
        from livekit.api import RoomCompositeEgressRequest, EncodedFileOutput, S3Upload

        lk_url     = os.getenv("LIVEKIT_URL", "").replace("wss://", "https://").replace("ws://", "http://")
        lk_api_key = os.getenv("LIVEKIT_API_KEY", "")
        lk_secret  = os.getenv("LIVEKIT_API_SECRET", "")

        filepath = f"recordings/{room_name}.ogg"

        req = RoomCompositeEgressRequest(
            room_name=room_name,
            audio_only=True,
            file_outputs=[
                EncodedFileOutput(
                    filepath=filepath,
                    s3=S3Upload(
                        access_key=s3_key_id,
                        secret=s3_secret,
                        bucket=s3_bucket,
                        region=s3_region,
                        endpoint=s3_endpoint or "",
                        force_path_style=True,
                    ),
                )
            ],
        )

        async with aiohttp.ClientSession() as session:
            svc = EgressService(session, lk_url, lk_api_key, lk_secret)
            info = await svc.start_room_composite_egress(req)

        logger.info("⏺  Recording started — egress_id=%s path=%s", info.egress_id, filepath)
        if s3_endpoint:
            recording_url = f"{s3_endpoint.rstrip('/')}/{s3_bucket}/{filepath}"
        else:
            recording_url = f"https://{s3_bucket}.s3.{s3_region}.amazonaws.com/{filepath}"

        return recording_url

    except Exception as exc:
        logger.warning("⏺  Recording start failed (non-fatal): %s", exc)
        return None


async def _stop_recording(ctx: agents.JobContext, room_name: str, tool_ctx=None) -> None:
    """Stop any active egress for this room, then generate signed URL and update DB."""
    try:
        from livekit.api.egress_service import EgressService
        from livekit.api import ListEgressRequest, StopEgressRequest

        lk_url     = os.getenv("LIVEKIT_URL", "").replace("wss://", "https://").replace("ws://", "http://")
        lk_api_key = os.getenv("LIVEKIT_API_KEY", "")
        lk_secret  = os.getenv("LIVEKIT_API_SECRET", "")

        async with aiohttp.ClientSession() as session:
            svc = EgressService(session, lk_url, lk_api_key, lk_secret)
            active = await svc.list_egress(ListEgressRequest(room_name=room_name, active=True))
            for e in (active.items or []):
                try:
                    await svc.stop_egress(StopEgressRequest(egress_id=e.egress_id))
                    logger.info("⏹  Recording stopped — egress_id=%s", e.egress_id)
                except Exception:
                    pass

        # Wait a few seconds for file to land on S3, then generate signed URL
        if tool_ctx and tool_ctx.recording_url:
            await asyncio.sleep(5)
            try:
                from supabase import create_client
                s3_bucket = os.getenv("S3_BUCKET", "")
                sb = create_client(os.getenv("SUPABASE_URL",""), os.getenv("SUPABASE_SERVICE_KEY",""))
                filepath = f"recordings/{room_name}.ogg"
                signed = sb.storage.from_(s3_bucket).create_signed_url(filepath, expires_in=60*60*24*30)
                signed_url = signed.get("signedURL") or signed.get("signed_url") or signed.get("signedUrl","")
                if signed_url:
                    tool_ctx.recording_url = signed_url
                    # Update the call log in DB with signed URL
                    from db import _sdb
                    db = _sdb()
                    db.table("call_logs").update({"recording_url": signed_url}).eq("org_id", tool_ctx.org_id).eq("recording_url", tool_ctx.recording_url).execute()
                    logger.info("⏺  Signed URL saved to DB")
            except Exception as se:
                logger.warning("⏺  Post-call signed URL failed: %s", se)

    except Exception as exc:
        logger.warning("⏺  Recording stop failed (non-fatal): %s", exc)


async def entrypoint(ctx: agents.JobContext) -> None:
    phone_number: Optional[str] = None
    lead_name     = "there"
    business_name = "our company"
    service_type  = "our service"
    custom_prompt: Optional[str] = None
    agent_profile_id: Optional[str] = None
    org_id: Optional[str] = None
    outbound_number: Optional[str] = None
    transfer_number: Optional[str] = None

    for meta_src in [(ctx.job.metadata if ctx.job else None), ctx.room.metadata]:
        if not meta_src:
            continue
        try:
            d = json.loads(meta_src)
            phone_number     = d.get("phone_number", phone_number)
            lead_name        = d.get("lead_name", lead_name)
            business_name    = d.get("business_name", business_name)
            service_type     = d.get("service_type", service_type)
            custom_prompt    = d.get("system_prompt", custom_prompt)
            agent_profile_id = d.get("agent_profile_id", agent_profile_id)
            org_id           = d.get("org_id", org_id)
            outbound_number  = d.get("outbound_number", outbound_number)
            transfer_number  = d.get("transfer_number", transfer_number)
        except Exception:
            pass

    if not org_id:
        logger.error("❌ Job metadata has no org_id — refusing to run.")
        await _log("error", "Job dispatched without org_id", f"room={ctx.room.name}")
        return

    logger.info("📞 Incoming job | org=%s phone=%s lead=%s business=%s service=%s",
                org_id, phone_number, lead_name, business_name, service_type)

    # ── Load agent profile ────────────────────────────────────────────────────
    profile_tools = []
    if agent_profile_id:
        try:
            profile = await get_agent_profile(agent_profile_id, org_id)
            if profile:
                if profile.get("voice"): os.environ["GEMINI_TTS_VOICE"] = profile["voice"]
                if profile.get("model"): os.environ["GEMINI_MODEL"]     = profile["model"]
                custom_prompt = custom_prompt or profile.get("system_prompt")
                profile_tools = json.loads(profile.get("enabled_tools", "[]") or "[]")
                logger.info("✅ Profile loaded: %s", profile.get("name"))
            else:
                logger.warning("⚠️  agent_profile_id %s not found for org %s — using defaults",
                               agent_profile_id, org_id)
        except Exception as exc:
            logger.warning("Profile load failed: %s", exc)

    system_prompt = build_prompt(lead_name, business_name, service_type, custom_prompt)
    enabled_tools = profile_tools or await get_enabled_tools()

    # ── Connect to room ───────────────────────────────────────────────────────
    await ctx.connect()
    logger.info("✅ Connected to room: %s", ctx.room.name)

    tool_ctx = AppointmentTools(ctx, org_id, phone_number, lead_name, transfer_number=transfer_number)

    DISABLED_TOOLS = {"send_sms_confirmation"}

    if enabled_tools:
        filtered = [t for t in enabled_tools if t not in DISABLED_TOOLS]
    else:
        filtered = [
            t.__name__ for t in tool_ctx.build_tool_list([])
            if t.__name__ not in DISABLED_TOOLS
        ]

    tools = tool_ctx.build_tool_list(filtered)
    logger.info("🔧 Active tools: %s", [t.__name__ for t in tools])

    # ── Build session ─────────────────────────────────────────────────────────
    logger.info("🆕 Building fresh Gemini session")
    session = _build_session(tools, system_prompt)

    class _Agent(Agent):
        def __init__(self):
            super().__init__(instructions=system_prompt, tools=tools)

    try:
        from livekit.agents import RoomOptions
        await session.start(
            room=ctx.room,
            agent=_Agent(),
            room_options=RoomOptions(noise_cancellation=_get_nc()),
        )
    except Exception:
        await session.start(
            room=ctx.room,
            agent=_Agent(),
            room_input_options=RoomInputOptions(noise_cancellation=_get_nc()),
        )

    logger.info("✅ Session started — ready to dial")

    # ── Dial out ──────────────────────────────────────────────────────────────
    if phone_number:
        user_present = any(
            "sip_" in p.identity or phone_number.replace("+", "") in p.identity
            for p in ctx.room.remote_participants.values()
        )

        if not user_present:
            trunk_id = os.getenv("OUTBOUND_TRUNK_ID", "")
            if not trunk_id:
                logger.error("❌ OUTBOUND_TRUNK_ID not set — cannot dial")
                return

            answered    = asyncio.Event()
            sip_hungup  = asyncio.Event()
            room_closed = asyncio.Event()

            sip_participant_identity: Optional[str] = None
            expected_identity = f"sip_{phone_number.replace('+', '')}"

            @ctx.room.on("participant_connected")
            def _on_connect(p):
                nonlocal sip_participant_identity
                logger.info("👤 Participant connected: %s", p.identity)
                if (
                    p.identity == expected_identity
                    or "sip_" in p.identity
                    or phone_number.replace("+", "") in p.identity
                ):
                    sip_participant_identity = p.identity
                    logger.info("📲 SIP answered: %s → identity=%s", phone_number, p.identity)
                    answered.set()

            @ctx.room.on("participant_disconnected")
            def _on_disconnect(p):
                logger.info("👤 Participant disconnected: %s", p.identity)
                if sip_participant_identity and p.identity == sip_participant_identity:
                    logger.info("📴 SIP hangup confirmed: %s", phone_number)
                    sip_hungup.set()
                elif "sip_" in p.identity or phone_number.replace("+", "") in p.identity:
                    logger.info("📴 SIP hangup (pattern match): %s", p.identity)
                    sip_hungup.set()
                else:
                    logger.info("ℹ️  Non-SIP participant left (ignored): %s", p.identity)

            @ctx.room.on("disconnected")
            def _on_room_close(*_):
                logger.info("🚪 Room closed")
                room_closed.set()
                sip_hungup.set()

            # ── FIX: AMD (Answering Machine Detection) ────────────────────────
            # LiveKit SIP detects voicemail/IVR and fires a SIP header attribute.
            # We watch for it and end the call immediately if detected.
            @ctx.room.on("sip_dtmf_received")
            def _on_sip_event(*args, **kwargs):
                pass  # not used but keeps the handler registered

            # AMD is signalled via participant attributes set by LiveKit SIP
            @ctx.room.on("participant_attributes_changed")
            def _on_attrs(changed_attrs, participant):
                amd_result = changed_attrs.get("sip.callStatus") or changed_attrs.get("amd_result", "")
                if amd_result in ("voicemail", "machine", "machine_end_beep",
                                  "machine_end_silence", "machine_end_other"):
                    logger.info("📵 Voicemail/AMD detected (%s) — ending call", amd_result)
                    asyncio.ensure_future(_handle_voicemail(tool_ctx, sip_hungup, ctx))

            # ── Dial ──────────────────────────────────────────────────────────
            caller_id = outbound_number or os.getenv("VOBIZ_OUTBOUND_NUMBER", "")
            try:
                request_kwargs = dict(
                    room_name=ctx.room.name,
                    sip_trunk_id=trunk_id,
                    sip_call_to=phone_number,
                    participant_identity=expected_identity,
                    wait_until_answered=False,
                )
                if caller_id:
                    request_kwargs["sip_number"] = caller_id

                await ctx.api.sip.create_sip_participant(
                    api.CreateSIPParticipantRequest(**request_kwargs)
                )
                logger.info("📡 Ringing %s… (caller ID: %s)", phone_number, caller_id or "trunk default")
            except Exception as exc:
                logger.error("❌ Dial failed for %s: %s", phone_number, exc)
                await _log("error", f"Dial failed for {phone_number}", str(exc))
                return

            # ── Wait for answer ───────────────────────────────────────────────
            try:
                await asyncio.wait_for(answered.wait(), timeout=45.0)
                await asyncio.sleep(0.1)
                logger.info("🗣  Call answered — Gemini Live is active for %s", phone_number)

                # Recording disabled — S3 signature issue with Supabase
                # recording_url = await _start_recording(ctx, ctx.room.name)

                # Trigger Priya to speak first
                try:
                    from google.genai import types as _gt
                    realtime_model = session._llm
                    for rs in list(realtime_model._sessions):
                        rs._send_client_event(
                            _gt.LiveClientRealtimeInput(text="hello")
                        )
                        logger.info("✅ Sent hello text to trigger Priya greeting")
                        break
                except Exception as e:
                    logger.warning("Hello trigger failed: %s", e)

                # ── FIX: Start silence watchdog ───────────────────────────────
                watchdog_task = asyncio.ensure_future(
                    _silence_watchdog(sip_hungup, ctx, tool_ctx, phone_number)
                )

            except asyncio.TimeoutError:
                logger.warning("⏱  No answer after 45s: %s", phone_number)
                await _log("warning", f"No answer: {phone_number}")
                try:
                    await tool_ctx.end_call(outcome="no_answer", reason="no answer after 45s")
                except Exception:
                    pass
                return

            # ── Wait for call to end ──────────────────────────────────────────
            logger.info("⏳ Call in progress: %s", phone_number)
            await sip_hungup.wait()
            logger.info("✅ Call ended: %s", phone_number)

            # Cancel watchdog if still running
            try:
                watchdog_task.cancel()
            except Exception:
                pass

            # Recording disabled
            # await _stop_recording(ctx, ctx.room.name, tool_ctx)

            try:
                await ctx.room.disconnect()
            except Exception:
                pass
            return

    # No phone — keep alive until room closes
    done = asyncio.Event()

    @ctx.room.on("disconnected")
    def _done(*_): done.set()

    watchdog_task = asyncio.ensure_future(
        _browser_silence_watchdog(done, ctx, tool_ctx)
    )

    await done.wait()
    try:
        watchdog_task.cancel()
    except Exception:
        pass
    logger.info("🏁 Session done")


async def _handle_voicemail(tool_ctx, sip_hungup: asyncio.Event,
                             ctx: agents.JobContext) -> None:
    """Called when AMD detects a voicemail/machine. Ends call immediately."""
    if sip_hungup.is_set():
        return
    try:
        await tool_ctx.end_call(outcome="voicemail", reason="AMD detected answering machine")
    except Exception:
        pass
    sip_hungup.set()
    try:
        await ctx.room.disconnect()
    except Exception:
        pass


if __name__ == "__main__":
    load_db_settings_to_env()
    init_db()
    agents.cli.run_app(
        agents.WorkerOptions(
            entrypoint_fnc=entrypoint,
            prewarm_fnc=_prewarm,
            agent_name="outbound-caller",
        )
    )

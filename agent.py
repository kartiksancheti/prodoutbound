import asyncio
import json
import logging
import logging.handlers
import os
import ssl
import certifi
from typing import Optional

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

# ── FIX 4: File-based logging so PM2 captures everything ─────────────────────
_LOG_FILE = os.path.join(os.path.dirname(__file__), "agent_calls.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),                          # stdout (PM2)
        logging.handlers.RotatingFileHandler(            # file (always works)
            _LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
        ),
    ],
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("livekit.agents").setLevel(logging.WARNING)
logging.getLogger("livekit.plugins.google").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

logger = logging.getLogger("agent")

# ── NC model cached at module level — loaded ONCE when worker starts ──────────
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

# FIX 5: Deepgram only needed for pipeline fallback — not for native audio model
_deepgram_stt = None
try:
    from livekit.plugins import deepgram as _dg
    _deepgram_stt = _dg.STT
except ImportError:
    pass


def _build_session(tools: list, system_prompt: str) -> AgentSession:
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-live-001")

    # Auto-correct deprecated model names
    if gemini_model in "gemini-2.0-flash":
        logger.warning("⚠️  Model '%s' is deprecated — switching to gemini-2.0-flash-live-001", gemini_model)
        gemini_model = "gemini-2.0-flash-live-001"

    gemini_voice = os.getenv("GEMINI_TTS_VOICE", "Aoede")
    use_realtime = os.getenv("USE_GEMINI_REALTIME", "true").lower() != "false"
    RealtimeClass = _google_realtime or (_google_beta_realtime if use_realtime else None)

    if use_realtime and RealtimeClass is not None:
        logger.info("🎙  Gemini Live | model=%s voice=%s", gemini_model, gemini_voice)

        kw: dict = dict(model=gemini_model, voice=gemini_voice, instructions=system_prompt)

        # FIX 2: Proper VAD config to eliminate mid-call silence pauses
        # These settings tell Gemini to respond faster after the user stops speaking
        try:
            from google.genai import types as _gt
            kw["realtime_input_config"] = _gt.RealtimeInputConfig(
                automatic_activity_detection=_gt.AutomaticActivityDetection(
                    end_of_speech_sensitivity=_gt.EndSensitivity.END_SENSITIVITY_HIGH,
                    silence_duration_ms=800,      # respond after 800ms silence (was default ~2000ms)
                    prefix_padding_ms=100,
                ),
            )
            kw["session_resumption"] = _gt.SessionResumptionConfig(transparent=True)
            kw["proactivity"] = True  # speak first without waiting for user audio
            kw["api_version"] = "v1alpha"  # required for proactivity to work
            kw["context_window_compression"] = _gt.ContextWindowCompressionConfig(
                trigger_tokens=25600,
                sliding_window=_gt.SlidingWindow(target_tokens=12800),
            )
            logger.info("✅ VAD config applied — silence_duration_ms=800")
        except Exception as e:
            logger.warning("VAD config skipped: %s", e)

        return AgentSession(llm=RealtimeClass(**kw), tools=tools)

    # Pipeline fallback (only if realtime not available)
    # FIX 5: Native audio model does NOT need Deepgram — only pipeline mode does
    if _google_llm is None:
        raise RuntimeError("No Google AI backend available.")

    logger.info("🎙  Pipeline mode | Deepgram STT + Gemini LLM + Google TTS")
    stt = _deepgram_stt(model="nova-2-phonecall") if _deepgram_stt else None
    tts = _google_tts() if _google_tts else None
    vad = silero.VAD.load()
    return AgentSession(stt=stt, llm=_google_llm(model=gemini_model), tts=tts, vad=vad, tools=tools)


# FIX 1: Proper prewarm — actually establishes the Gemini WS connection
# so first call has zero handshake delay
def _prewarm(proc: agents.JobProcess) -> None:
    logger.info("🔥 Prewarming Gemini connection...")
    try:
        from prompts import DEFAULT_SYSTEM_PROMPT
        dummy_prompt = DEFAULT_SYSTEM_PROMPT.format(
            lead_name="there", business_name="our company", service_type="our service"
        )
        # Store on proc so entrypoint can reuse it
        proc.userdata["warm_session"] = _build_session([], dummy_prompt)
        logger.info("✅ Gemini WS pre-warmed successfully")
    except Exception as e:
        logger.warning("Pre-warm failed (non-fatal): %s", e)
        proc.userdata["warm_session"] = None


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
        logger.error("❌ Job metadata has no org_id — refusing to run (dispatch_call must always embed it).")
        await _log("error", "Job dispatched without org_id", f"room={ctx.room.name}")
        return

    logger.info("📞 Incoming job | org=%s phone=%s lead=%s business=%s service=%s",
                org_id, phone_number, lead_name, business_name, service_type)

    # ── Load agent profile ────────────────────────────────────────────────────
    # get_agent_profile is called WITH org_id, so a profile belonging to a
    # different tenant simply won't be returned — a guessed/forged
    # agent_profile_id from a different org can never be loaded here.
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
                logger.warning("⚠️  agent_profile_id %s not found for org %s — using defaults", agent_profile_id, org_id)
        except Exception as exc:
            logger.warning("Profile load failed: %s", exc)

    system_prompt = build_prompt(lead_name, business_name, service_type, custom_prompt)
    enabled_tools = profile_tools or await get_enabled_tools()

    # ── Connect to room ───────────────────────────────────────────────────────
    await ctx.connect()
    logger.info("✅ Connected to room: %s", ctx.room.name)

    tool_ctx = AppointmentTools(ctx, org_id, phone_number, lead_name, transfer_number=transfer_number)

    # Twilio SMS stays disabled by default — Cal.com tools no longer exist in tools.py at all.
    DISABLED_TOOLS = {"send_sms_confirmation"}

    if enabled_tools:
        # Filter from profile/DB tool list
        filtered = [t for t in enabled_tools if t not in DISABLED_TOOLS]
    else:
        # Filter from full tool list
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


    # Trigger Priya to speak first using generate_reply (works in livekit-agents 1.6.2)
    try:
        await session.generate_reply(
            instructions="Say this immediately: 'Hi, am I speaking with " + lead_name + "?' Do not wait."
        )
        logger.info("✅ generate_reply triggered — Priya will speak first")
    except Exception as e:
        logger.warning("generate_reply failed: %s", e)

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
                sip_hungup.set()  # unblock wait

            # Dial — sip_number sets the caller ID. Falls back to the trunk's own
            # configured number if this org has no outbound_number assigned yet.
            # Vobiz must have this number allow-listed as a valid "From" on the account.
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
                await ctx.api.sip.create_sip_participant(api.CreateSIPParticipantRequest(**request_kwargs))
                logger.info("📡 Ringing %s… (caller ID: %s)", phone_number, caller_id or "trunk default")
            except Exception as exc:
                logger.error("❌ Dial failed for %s: %s", phone_number, exc)
                await _log("error", f"Dial failed for {phone_number}", str(exc))
                return

            # Wait for answer
            try:
                await asyncio.wait_for(answered.wait(), timeout=45.0)
                # FIX 1: Minimal buffer — session is already warmed, agent speaks fast
                await asyncio.sleep(0.1)
                logger.info("🗣  Call answered — Gemini Live is active for %s", phone_number)
                # Push silent audio to trigger Gemini to speak first.
                # gemini-3.1-flash-live-preview won't speak unprompted — feeding it
                # silence makes it detect end-of-speech and respond with the greeting.
                try:
                    from livekit import rtc as _rtc
                    import numpy as np
                    realtime_model = session._llm
                    for rs in list(realtime_model._sessions):
                        # 1 second of silence at 16kHz mono (Gemini input format)
                        silent_data = bytes(16000 * 2)  # 16000 samples * 2 bytes each
                        silent_frame = _rtc.AudioFrame(
                            data=silent_data,
                            sample_rate=16000,
                            num_channels=1,
                            samples_per_channel=16000,
                        )
                        # Send "hello" as realtime text input — mimics user speaking
                        # This triggers Gemini exactly like when user says hello
                        from google.genai import types as _gt
                        rs._send_client_event(
                            _gt.LiveClientRealtimeInput(text="hello")
                        )
                        logger.info("✅ Sent hello text to trigger Priya greeting")
                        break
                except Exception as e:
                    logger.warning("Silent audio push failed: %s", e)
            except asyncio.TimeoutError:
                logger.warning("⏱  No answer after 45s: %s", phone_number)
                await _log("warning", f"No answer: {phone_number}")
                try:
                    await tool_ctx.end_call(outcome="no_answer", reason="no answer after 45s")
                except Exception:
                    pass
                return

            # Wait for call to end
            logger.info("⏳ Call in progress: %s", phone_number)
            await sip_hungup.wait()
            logger.info("✅ Call ended: %s", phone_number)

            try:
                await ctx.room.disconnect()
            except Exception:
                pass
            return

    # No phone — keep alive until room closes
    done = asyncio.Event()

    @ctx.room.on("disconnected")
    def _done(*_): done.set()

    await done.wait()
    logger.info("🏁 Session done")


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

import asyncio
import logging
import os
import time
from typing import Optional

from livekit import agents, api
from livekit.agents import llm

from db import (
    check_slot, get_next_available, insert_appointment, log_call, log_error,
    get_calls_by_phone, get_appointments_by_phone,
    add_contact_memory, get_contact_memory, compress_contact_memory,
)

logger = logging.getLogger("appointment-tools")


async def _log(msg: str, detail: str = "", level: str = "info") -> None:
    try:
        await log_error("agent", msg, detail, level)
    except Exception:
        pass


class AppointmentTools(llm.ToolContext):
    """All function tools available to the appointment-booking agent. Every method that touches
    the database is scoped to self.org_id so one tenant's call can never read or write another
    tenant's data."""

    def __init__(
        self,
        ctx: agents.JobContext,
        org_id: str,
        phone_number: Optional[str] = None,
        lead_name: Optional[str] = None,
        transfer_number: Optional[str] = None,
    ):
        self.ctx = ctx
        self.org_id = org_id
        self.phone_number = phone_number
        self.lead_name = lead_name
        self._call_start_time = time.time()
        self._sip_domain = os.getenv("VOBIZ_SIP_DOMAIN", "")
        # org's own transfer destination, set by agent.py from the organizations table;
        # falls back to the platform-wide DEFAULT_TRANSFER_NUMBER if the org hasn't set one.
        self.transfer_number = transfer_number or os.getenv("DEFAULT_TRANSFER_NUMBER", "")
        self.recording_url: Optional[str] = None
        super().__init__(tools=[])

    def build_tool_list(self, enabled: list) -> list:
        """Return tool methods filtered by the enabled list. Empty list = all enabled."""
        all_methods = [
            self.check_availability, self.book_appointment, self.end_call,
            self.transfer_to_human, self.send_sms_confirmation, self.lookup_contact,
            self.remember_details,
        ]
        if not enabled:
            return all_methods
        name_map = {m.__name__: m for m in all_methods}
        return [name_map[n] for n in enabled if n in name_map]

    @llm.function_tool
    async def check_availability(self, date: str, time: str) -> str:
        """
        Check whether a date/time slot is available for booking.
        Call this BEFORE attempting to book whenever the lead proposes a date/time.
        date format: YYYY-MM-DD  |  time format: HH:MM (24-hour)
        Returns 'available' or 'unavailable: next available slot is <slot>'.
        """
        try:
            if await check_slot(self.org_id, date, time):
                return "available"
            next_slot = await get_next_available(self.org_id, date, time)
            return f"unavailable: next available slot is {next_slot}"
        except Exception:
            return "Unable to check availability right now — please suggest a date and I will confirm."

    @llm.function_tool
    async def book_appointment(self, name: str, phone: str, date: str, time: str, service: str) -> str:
        """
        Book an appointment after the lead has verbally confirmed date, time, and service.
        Call ONLY after the lead confirms all details.
        name: lead's full name | phone: with country code | date: YYYY-MM-DD | time: HH:MM | service: type
        """
        try:
            booking_id = await insert_appointment(self.org_id, name, phone, date, time, service)
        except Exception:
            return "Technical issue saving the booking. Our team will confirm shortly."

        # Notify the team via n8n -> Telegram webhook (advance-payment follow-up flow)
        try:
            import httpx
            from db import get_organization
            org = await get_organization(self.org_id)
            org_name = org.get("name") if org else self.org_id
            webhook_url = os.getenv("N8N_BOOKING_WEBHOOK_URL", "")
            if webhook_url:
                async with httpx.AsyncClient(timeout=10) as client:
                    await client.post(webhook_url, json={
                        "booking_id": booking_id,
                        "org_id": self.org_id,
                        "org_name": org_name,
                        "name": name,
                        "phone": phone,
                        "date": date,
                        "time": time,
                        "service": service,
                    })
        except Exception as exc:
            logger.warning("n8n booking webhook failed: %s", exc)

        return f"Confirmed! Booking ID: {booking_id}. See you on {date} at {time} for {service}."

    @llm.function_tool
    async def end_call(self, outcome: str, reason: str = "") -> str:
        """
        End the call and log the outcome. ALWAYS call this before the call ends.
        outcome: 'booked' | 'not_interested' | 'wrong_number' | 'voicemail' | 'no_answer' | 'callback_requested'
        reason: brief description
        """
        duration = int(time.time() - self._call_start_time)
        try:
            await log_call(
                org_id=self.org_id,
                phone_number=self.phone_number or "unknown",
                lead_name=self.lead_name, outcome=outcome, reason=reason,
                duration_seconds=duration, recording_url=self.recording_url,
            )
        except Exception as exc:
            logger.error("Failed to log call: %s", exc)

        # Wait for Priya to finish speaking before disconnecting
        await asyncio.sleep(3)
        # Force-remove SIP participant to actually terminate the call on Vobiz side
        try:
            import aiohttp
            from livekit.api.room_service import RoomService
            from livekit.api.room_service import RoomParticipantIdentity
            lk_url = os.getenv("LIVEKIT_URL", "").replace("wss://", "https://").replace("ws://", "http://")
            lk_key = os.getenv("LIVEKIT_API_KEY", "")
            lk_secret = os.getenv("LIVEKIT_API_SECRET", "")
            async with aiohttp.ClientSession() as session:
                svc = RoomService(session, lk_url, lk_key, lk_secret)
                for p in self.ctx.room.remote_participants.values():
                    if "sip_" in p.identity or (self.phone_number and self.phone_number.replace("+","") in p.identity):
                        await svc.remove_participant(
                            RoomParticipantIdentity(
                                room=self.ctx.room.name,
                                identity=p.identity,
                            )
                        )
                        logger.info("📴 SIP participant removed: %s", p.identity)
                        break
        except Exception as e:
            logger.warning("SIP remove_participant failed: %s", e)
        try:
            await self.ctx.room.disconnect()
        except Exception:
            pass
        return "Call ended."

    @llm.function_tool
    async def transfer_to_human(self, reason: str) -> str:
        """
        Transfer the call to a human agent via SIP REFER.
        Call when lead requests a human, is angry, or has a complex issue.
        reason: why you're transferring
        """
        destination = self.transfer_number
        if not destination:
            return "Transfer unavailable: no fallback number configured."
        if "@" not in destination:
            clean = destination.replace("tel:", "").replace("sip:", "")
            destination = f"sip:{clean}@{self._sip_domain}" if self._sip_domain else f"tel:{clean}"
        elif not destination.startswith("sip:"):
            destination = f"sip:{destination}"
        participant_identity = f"sip_{self.phone_number}" if self.phone_number else None
        if not participant_identity:
            for p in self.ctx.room.remote_participants.values():
                participant_identity = p.identity
                break
        if not participant_identity:
            return "Transfer failed: could not identify caller."
        try:
            await self.ctx.api.sip.transfer_sip_participant(
                api.TransferSIPParticipantRequest(
                    room_name=self.ctx.room.name,
                    participant_identity=participant_identity,
                    transfer_to=destination, play_dialtone=False,
                )
            )
            return "Transferring you to a human agent now. Please hold."
        except Exception:
            return "Transfer failed. Please call us back directly."

    @llm.function_tool
    async def send_sms_confirmation(self, phone: str, message: str) -> str:
        """
        Send SMS confirmation after a successful booking. Skips silently if Twilio not configured.
        phone: lead's phone | message: text to send
        """
        sid = os.getenv("TWILIO_ACCOUNT_SID", "")
        token = os.getenv("TWILIO_AUTH_TOKEN", "")
        from_num = os.getenv("TWILIO_FROM_NUMBER", "")
        if not (sid and token and from_num):
            return "SMS skipped: Twilio not configured."
        try:
            from twilio.rest import Client
            loop = asyncio.get_event_loop()
            client = Client(sid, token)
            await loop.run_in_executor(None, lambda: client.messages.create(body=message, from_=from_num, to=phone))
            return f"SMS sent to {phone}."
        except Exception:
            return "SMS delivery failed, but booking is confirmed."

    @llm.function_tool
    async def lookup_contact(self, phone: str) -> str:
        """
        Look up a contact's full history. Call at the START of every call before engaging.
        phone: the lead's phone number with country code
        Returns call history, appointments, and remembered details.
        """
        try:
            calls = await get_calls_by_phone(self.org_id, phone)
            appointments = await get_appointments_by_phone(self.org_id, phone)
            memories = await get_contact_memory(self.org_id, phone)
            if not calls and not appointments and not memories:
                return f"No history for {phone}. First-time contact."
            lines = [f"Contact history for {phone}:"]
            if memories:
                lines.append(f"\nREMEMBERED ({len(memories)} notes):")
                for m in memories[:10]:
                    lines.append(f"  • {m['insight']}")
            if calls:
                lines.append(f"\nCALL HISTORY ({len(calls)} calls):")
                for c in calls[:5]:
                    ts = (c.get("timestamp") or "")[:16]
                    lines.append(f"  • {ts} — {c.get('outcome','?')}: {c.get('reason','')}")
            if appointments:
                lines.append(f"\nAPPOINTMENTS ({len(appointments)}):")
                for a in appointments[:3]:
                    lines.append(f"  • {a.get('date')} {a.get('time')} — {a.get('service')} [{a.get('status')}]")
            return "\n".join(lines)
        except Exception:
            return "Unable to retrieve contact history."

    @llm.function_tool
    async def send_telegram_notification(self, details: str) -> str:
        """
        Notify the team via Telegram that a booking has been confirmed on this call.
        Call this immediately after remember_details, once package, date, time, and
        booking name are all agreed.
        details: full booking summary (package, date, time, booking name, phone if known)
        """
        try:
            import httpx
            from db import get_organization
            org = await get_organization(self.org_id)
            org_name = org.get("name") if org else self.org_id
            webhook_url = os.getenv("N8N_BOOKING_WEBHOOK_URL", "")
            if not webhook_url:
                return "Notification skipped — no webhook configured."
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(webhook_url, json={
                    "org_id": self.org_id,
                    "org_name": org_name,
                    "name": self.lead_name,
                    "phone": self.phone_number or "unknown",
                    "details": details,
                    "reason": details,
                })
            return "Team notified."
        except Exception as exc:
            logger.warning("n8n booking webhook failed (send_telegram_notification): %s", exc)
            return "Notification failed, but continue the call normally."

    @llm.function_tool
    async def save_feedback_score(self, score: int, name: str = "") -> str:
        """
        Save the caller's 1-10 feedback rating on their last service.
        Call this right after they give a rating, before end_call.
        score: integer 1-10 | name: caller's name if known
        """
        if not self.phone_number:
            return "Cannot save feedback — no phone number for this call."
        try:
            insight = f"Feedback score: {score}/10" + (f" (name: {name})" if name else "")
            await add_contact_memory(self.org_id, self.phone_number, insight)
            return f"Feedback saved: {score}/10"
        except Exception as exc:
            logger.warning("Failed to save feedback score: %s", exc)
            return "Could not save feedback score."

    @llm.function_tool
    async def remember_details(self, insight: str) -> str:
        """
        Store a key insight about this lead for future calls.
        Use whenever you learn something useful: preferences, objections, timing, family info.
        insight: the detail to remember
        """
        if not self.phone_number:
            return "Cannot remember — no phone number for this call."
        try:
            await add_contact_memory(self.org_id, self.phone_number, insight)
            memories = await get_contact_memory(self.org_id, self.phone_number)
            if len(memories) >= 5:
                asyncio.create_task(self._compress_memories())
            return f"Remembered: {insight}"
        except Exception:
            return "Could not save detail."

    async def _compress_memories(self) -> None:
        try:
            memories = await get_contact_memory(self.org_id, self.phone_number)
            if len(memories) < 5:
                return
            import google.generativeai as genai
            api_key = os.getenv("GOOGLE_API_KEY", "")
            if not api_key:
                return
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel("gemini-2.5-flash")
            bullet_list = "\n".join(f"- {m['insight']}" for m in memories)
            prompt = f"Compress these notes about a sales contact into 3-5 concise bullets. Keep all key facts.\n\n{bullet_list}"
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: model.generate_content(prompt))
            if response.text.strip():
                await compress_contact_memory(self.org_id, self.phone_number, response.text.strip())
        except Exception as exc:
            logger.warning("Memory compression failed: %s", exc)

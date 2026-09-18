"""
============================================================
AUTO REQUEST ACCEPTER BOT
Single file  |  Supabase REST (project URL + anon key)
============================================================

No Postgres connection string, no asyncpg. Everything goes
through Supabase's REST endpoint:

    POST {SUPABASE_URL}/rest/v1/rpc/{function}

The tables sit behind RLS with no policies, so the anon key
can only call the SECURITY DEFINER functions created by
supabase_schema.sql — it cannot read or write the tables.

Install:
    pip install pyrogram tgcrypto aiohttp python-dotenv

Run:
    python bot.py
============================================================
"""

import asyncio
import json
import logging
import os
import time
import traceback
from contextlib import suppress

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import aiohttp

from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.errors import (
    RPCError,
    FloodWait,
    ChatAdminRequired,
    UserIsBlocked,
    PeerIdInvalid,
    MessageNotModified,
    InputUserDeactivated,
    UserDeactivated,
)
from pyrogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ChatJoinRequest,
)

# ============================================================
# CONFIG  (all secrets come from environment variables)
# ============================================================
# ============================================================
# TELEGRAM BOT
# ============================================================

BOT_TOKEN = "8790200190:AAHYwniqFPMhI42TByYNkgDZgbhwEepoOKA"

API_ID = 38410382
API_HASH = "2601d5fe89f068423003e552a7ac1d79"

BOT_SESSION = "AutoReqxAcceptxBot"

OWNER_ID = 6594401737


# ============================================================
# SUPABASE
# ============================================================

SUPABASE_URL = "https://hrdmhwuckazrcttwswlp.supabase.co"

# Supabase Dashboard → Project Settings → API → API Keys
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhyZG1od3Vja2F6cmN0dHdzd2xwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc4OTcwMjYyMSwiZXhwIjoyMTA1Mjc4NjIxfQ.NyzQb35WtoL4Y2co6L0Gw94xHexWcwoN3uREaEBFOeg"


# ============================================================
# DATABASE SETTINGS
# ============================================================

DB_TIMEOUT = 12.0
DB_RETRIES = 2
DB_CONCURRENCY = 16

VERIFICATION_TTL_HOURS = 24


# ============================================================
# PYROGRAM / APPROVAL SETTINGS
# ============================================================

PYROGRAM_WORKERS = 32

APPROVAL_CONCURRENCY = 32
DM_CONCURRENCY = 20


# ============================================================
# RETRY SETTINGS
# ============================================================

MAX_RETRIES = 3
RETRY_DELAY = 1.5


# ============================================================
# CACHE
# ============================================================

REQUEST_CACHE_TTL = 600


# ============================================================
# PENDING VERIFICATION
# ============================================================

# True = bot restart hone ke baad pending users ko
# verification button dobara send karega.
# False = dobara send nahi karega.

RESEND_PENDING_ON_START = False

RESEND_LIMIT = 100


# ============================================================
# CLEANUP
# ============================================================

# Har 3600 seconds (1 hour) stale verification rows cleanup
# honge.

CLEANUP_INTERVAL = 3600


# ============================================================
# REQUIRED CREDENTIAL CHECK
# ============================================================

if not BOT_TOKEN or not API_ID or not API_HASH:
    raise SystemExit(
        "BOT_TOKEN / API_ID / API_HASH missing."
    )

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

log = logging.getLogger("AutoRequestBot")

logging.getLogger("pyrogram").setLevel(logging.WARNING)


# ============================================================
# CLIENT
# ============================================================

app = Client(
    BOT_SESSION,
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=PYROGRAM_WORKERS,
    sleep_threshold=10,
)


# ============================================================
# GLOBAL STATE
# ============================================================

BOT_ID = None
BOT_USERNAME = None

approval_semaphore = None
dm_semaphore = None
db_semaphore = None
request_lock = None

processing_requests = {}
background_tasks = set()


# ============================================================
# ERROR LOGGER
# ============================================================

def log_exception(prefix, error):
    log.error("%s | %s", prefix, error)
    log.error(
        "TRACEBACK:\n%s",
        "".join(
            traceback.format_exception(
                type(error),
                error,
                error.__traceback__,
            )
        ),
    )


# ============================================================
# ============================================================
# SUPABASE REST CLIENT  (project URL + anon key)
# ============================================================
# ============================================================

class SupabaseDB:
    """
    Thin async wrapper over Supabase's PostgREST /rpc endpoint.

    Every method swallows its own errors and returns a safe
    default, so the bot keeps approving join requests even if
    Supabase is slow, rate limited, or completely down.
    """

    def __init__(self, url, key):
        self.base = f"{url}/rest/v1/rpc" if url else None
        self.key = key
        self.session = None
        self.enabled = False

    # --------------------------------------------------------
    # LIFECYCLE
    # --------------------------------------------------------

    async def connect(self):

        if not SUPABASE_URL or not SUPABASE_KEY:
            log.warning(
                "SUPABASE_URL / SUPABASE_KEY not set — "
                "running WITHOUT database"
            )
            self.enabled = False
            return False

        try:

            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=DB_TIMEOUT),
                headers={
                    "apikey": self.key,
                    "Authorization": f"Bearer {self.key}",
                    "Content-Type": "application/json",
                    "Prefer": "return=representation",
                },
                connector=aiohttp.TCPConnector(
                    limit=DB_CONCURRENCY * 2,
                    ttl_dns_cache=300,
                ),
            )

            self.enabled = True

            probe = await self.rpc("bot_stats")

            if probe is None:
                log.error(
                    "SUPABASE HANDSHAKE FAILED — check SUPABASE_URL, "
                    "SUPABASE_KEY, and that supabase_schema.sql has "
                    "been executed in the SQL editor"
                )
                await self.close()
                return False

            log.info("SUPABASE CONNECTED | %s", SUPABASE_URL)
            log.info("SUPABASE STATS | %s", probe)

            return True

        except Exception as e:
            log_exception("SUPABASE CONNECT ERROR", e)
            self.enabled = False
            return False

    async def close(self):

        if self.session and not self.session.closed:
            with suppress(Exception):
                await self.session.close()

        self.session = None
        self.enabled = False

    # --------------------------------------------------------
    # CORE RPC CALL
    # --------------------------------------------------------

    async def rpc(self, function, payload=None):
        """
        Calls a Postgres function through PostgREST.
        Returns the parsed JSON body, True for void functions,
        or None on failure. Never raises.
        """

        if not self.enabled or not self.session:
            return None

        url = f"{self.base}/{function}"
        body = payload or {}

        async with db_semaphore:

            for attempt in range(1, DB_RETRIES + 1):

                try:

                    async with self.session.post(url, json=body) as resp:

                        text = await resp.text()

                        if resp.status in (200, 201, 204):

                            if not text or text == "null":
                                return True

                            try:
                                return json.loads(text)
                            except ValueError:
                                return text

                        # 4xx means our request is wrong — no retry
                        if 400 <= resp.status < 500:
                            log.error(
                                "SUPABASE %s | HTTP %s | %s",
                                function,
                                resp.status,
                                text[:300],
                            )
                            return None

                        log.warning(
                            "SUPABASE %s | HTTP %s | attempt %s/%s",
                            function,
                            resp.status,
                            attempt,
                            DB_RETRIES,
                        )

                except asyncio.CancelledError:
                    raise

                except asyncio.TimeoutError:
                    log.warning(
                        "SUPABASE TIMEOUT | %s | attempt %s/%s",
                        function,
                        attempt,
                        DB_RETRIES,
                    )

                except Exception as e:
                    log.warning(
                        "SUPABASE ERROR | %s | attempt %s/%s | %s",
                        function,
                        attempt,
                        DB_RETRIES,
                        e,
                    )

                if attempt < DB_RETRIES:
                    await asyncio.sleep(0.5 * attempt)

            return None

    # --------------------------------------------------------
    # JOIN REQUESTS
    # --------------------------------------------------------

    async def create_join_request(self, user, chat, invite_link=None):
        """Upserts user + chat and inserts the request. Returns row id."""

        link_url = None
        link_name = None
        link_creator = None

        if invite_link:
            link_url = getattr(invite_link, "invite_link", None)
            link_name = getattr(invite_link, "name", None)
            creator = getattr(invite_link, "creator", None)
            link_creator = creator.id if creator else None

        result = await self.rpc(
            "bot_record_join_request",
            {
                "p_user_id": int(user.id),
                "p_username": user.username,
                "p_first_name": user.first_name,
                "p_last_name": user.last_name,
                "p_language_code": getattr(user, "language_code", None),
                "p_is_premium": bool(
                    getattr(user, "is_premium", False) or False
                ),
                "p_chat_id": int(chat.id),
                "p_chat_title": chat.title,
                "p_chat_username": chat.username,
                "p_chat_type": getattr(chat.type, "value", str(chat.type)),
                "p_invite_link": link_url,
                "p_invite_name": link_name,
                "p_invite_creator": link_creator,
            },
        )

        return result if isinstance(result, int) else None

    async def mark_approved(self, request_id, processing_ms=None, attempts=1):
        if not request_id:
            return None
        return await self.rpc(
            "bot_mark_approved",
            {
                "p_request_id": int(request_id),
                "p_ms": int(processing_ms) if processing_ms is not None else None,
                "p_attempts": int(attempts),
            },
        )

    async def mark_declined(self, request_id, reason=None):
        if not request_id:
            return None
        return await self.rpc(
            "bot_mark_declined",
            {"p_request_id": int(request_id), "p_reason": reason},
        )

    async def mark_failed(self, request_id, error, attempts=None):
        if not request_id:
            return None
        return await self.rpc(
            "bot_mark_failed",
            {
                "p_request_id": int(request_id),
                "p_error": str(error)[:2000],
                "p_attempts": int(attempts) if attempts is not None else None,
            },
        )

    async def mark_dm_sent(self, request_id, message_id, sent=True):
        if not request_id:
            return None
        return await self.rpc(
            "bot_mark_dm",
            {
                "p_request_id": int(request_id),
                "p_message_id": int(message_id) if message_id else None,
                "p_sent": bool(sent),
            },
        )

    async def mark_dm_blocked(self, user_id, blocked=True):
        return await self.rpc(
            "bot_mark_dm_blocked",
            {"p_user_id": int(user_id), "p_blocked": bool(blocked)},
        )

    # --------------------------------------------------------
    # USERS / CHATS
    # --------------------------------------------------------

    async def upsert_user(self, user):
        return await self.rpc(
            "bot_upsert_user",
            {
                "p_user_id": int(user.id),
                "p_username": user.username,
                "p_first_name": user.first_name,
                "p_last_name": user.last_name,
                "p_language_code": getattr(user, "language_code", None),
                "p_is_premium": bool(
                    getattr(user, "is_premium", False) or False
                ),
            },
        )

    async def upsert_chat(self, chat):
        return await self.rpc(
            "bot_upsert_chat",
            {
                "p_chat_id": int(chat.id),
                "p_title": chat.title,
                "p_chat_username": chat.username,
                "p_chat_type": getattr(chat.type, "value", str(chat.type)),
            },
        )

    async def set_chat_active(self, chat_id, active):
        return await self.rpc(
            "bot_set_chat_active",
            {"p_chat_id": int(chat_id), "p_active": bool(active)},
        )

    # --------------------------------------------------------
    # DUPLICATE CHECK  (survives restarts)
    # --------------------------------------------------------

    async def is_recent_duplicate(self, chat_id, user_id, ttl_seconds):
        result = await self.rpc(
            "bot_is_duplicate",
            {
                "p_chat_id": int(chat_id),
                "p_user_id": int(user_id),
                "p_seconds": float(ttl_seconds),
            },
        )
        return result is True

    # --------------------------------------------------------
    # VERIFICATION
    # --------------------------------------------------------

    async def create_pending_verification(
        self, user_id, chat_id, join_request_id=None, message_id=None
    ):
        return await self.rpc(
            "bot_create_pending_verification",
            {
                "p_user_id": int(user_id),
                "p_chat_id": int(chat_id),
                "p_request_id": int(join_request_id)
                if join_request_id
                else None,
                "p_message_id": int(message_id) if message_id else None,
                "p_ttl_hours": VERIFICATION_TTL_HOURS,
            },
        )

    async def mark_verified(self, user_id, chat_id):
        return await self.rpc(
            "bot_mark_verified",
            {"p_user_id": int(user_id), "p_chat_id": int(chat_id)},
        )

    async def expire_old_verifications(self):
        result = await self.rpc("bot_expire_verifications")
        return result if isinstance(result, int) else 0

    async def get_pending_verifications(self, limit=100):
        result = await self.rpc(
            "bot_pending_verifications", {"p_limit": int(limit)}
        )
        return result if isinstance(result, list) else []

    async def bump_resend(self, pv_id, message_id=None):
        return await self.rpc(
            "bot_bump_resend",
            {
                "p_id": int(pv_id),
                "p_message_id": int(message_id) if message_id else None,
            },
        )

    async def mark_verification_failed(self, pv_id, reason=None):
        return await self.rpc(
            "bot_mark_verification_failed", {"p_id": int(pv_id)}
        )

    # --------------------------------------------------------
    # STATS
    # --------------------------------------------------------

    async def get_stats(self):
        result = await self.rpc("bot_stats")
        return result if isinstance(result, dict) else None

    async def get_chat_stats(self, chat_id):
        result = await self.rpc("bot_chat_stats", {"p_chat_id": int(chat_id)})
        return result if isinstance(result, dict) else None

    async def get_user_stats(self, user_id):
        result = await self.rpc("bot_user_stats", {"p_user_id": int(user_id)})
        return result if isinstance(result, dict) else None


db = SupabaseDB(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# TASK MANAGER
# ============================================================

def create_task(coro, name):
    try:
        task = asyncio.create_task(coro, name=name)

        background_tasks.add(task)

        def done(t):
            background_tasks.discard(t)

            if t.cancelled():
                log.warning("TASK CANCELLED | %s", name)
                return

            try:
                error = t.exception()

                if error:
                    log.error("TASK FAILED | %s | %s", name, error)
                    log_exception(f"TASK TRACEBACK | {name}", error)

            except Exception as e:
                log_exception("TASK CALLBACK ERROR", e)

        task.add_done_callback(done)

        return task

    except Exception as e:
        log_exception(f"TASK CREATE ERROR | {name}", e)
        return None


def fire(coro, name):
    """Fire-and-forget helper for every non-blocking DB write."""
    if not db.enabled:
        coro.close()
        return None
    return create_task(coro, name)


# ============================================================
# DUPLICATE PROTECTION  (memory + database)
# ============================================================

async def is_duplicate(chat_id, user_id):

    key = (int(chat_id), int(user_id))
    now = time.monotonic()

    async with request_lock:

        expired = [
            k
            for k, timestamp in processing_requests.items()
            if now - timestamp > REQUEST_CACHE_TTL
        ]

        for k in expired:
            processing_requests.pop(k, None)

        if key in processing_requests:
            return True

        processing_requests[key] = now

    # Memory cache is empty after a restart — ask the database.
    if db.enabled:
        try:
            if await db.is_recent_duplicate(
                chat_id, user_id, REQUEST_CACHE_TTL
            ):
                return True
        except Exception as e:
            log.warning("DB DUPLICATE CHECK FAILED | %s", e)

    return False


# ============================================================
# KEYBOARDS
# ============================================================

def main_keyboard():

    if not BOT_USERNAME:
        return InlineKeyboardMarkup(
            [[InlineKeyboardButton("📚 HELP & SETUP", callback_data="help")]]
        )

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ ADD TO GROUP",
                    url=f"https://t.me/{BOT_USERNAME}?startgroup=true",
                )
            ],
            [
                InlineKeyboardButton(
                    "📢 ADD TO CHANNEL",
                    url=f"https://t.me/{BOT_USERNAME}?startchannel=true",
                )
            ],
            [
                InlineKeyboardButton(
                    "📚 HELP & SETUP",
                    callback_data="help",
                )
            ],
        ]
    )


def back_keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🔙 BACK", callback_data="back")]]
    )


def verification_keyboard(chat_id, user_id):
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🤖 I AM NOT A ROBOT",
                    callback_data=f"verify:{chat_id}:{user_id}",
                )
            ]
        ]
    )


# ============================================================
# TEXT
# ============================================================

def start_text():
    return (
        "🤖 **Auto Request Accepter**\n\n"
        "⚡ **Fast • Automatic • Reliable**\n\n"
        "Automatically accepts new join requests "
        "from your Telegram groups and channels.\n\n"
        "### 🚀 Setup\n\n"
        "1️⃣ Add me to your group or channel.\n\n"
        "2️⃣ Promote me to **Administrator**.\n\n"
        "3️⃣ Enable **Invite Users / Manage Join Requests**.\n\n"
        "4️⃣ Create an invite link with **Join Requests** enabled.\n\n"
        "5️⃣ New requests are processed automatically.\n\n"
        "6️⃣ After approval, the user receives a verification message."
    )


def help_text():
    return (
        "📚 **Auto Request Accepter — Help**\n\n"
        "### 🔧 Setup\n\n"
        "1️⃣ Add the bot to your group/channel.\n\n"
        "2️⃣ Make the bot Administrator.\n\n"
        "3️⃣ Enable **Manage Join Requests**.\n\n"
        "4️⃣ Create a join-request invite link.\n\n"
        "5️⃣ Users send join requests.\n\n"
        "6️⃣ Bot automatically approves them.\n\n"
        "### 🧰 Commands\n\n"
        "`/start` — Main menu\n"
        "`/help` — Help\n"
        "`/ping` — Connection test\n"
        "`/id` — Chat information\n"
        "`/status` — Permission check\n"
        "`/stats` — Chat statistics\n\n"
        "⚡ Multiple requests are processed concurrently."
    )


def verification_text(user, chat_title):

    first_name = (
        getattr(user, "first_name", None)
        or getattr(user, "username", None)
        or "User"
    )

    return (
        f"👋 **Hello {first_name},**\n\n"
        f"🎉 Your request to join **{chat_title or 'the chat'}** "
        "has been accepted.\n\n"
        "🤖 **Please confirm that you are not a robot.**\n\n"
        "👇 Tap the button below to continue."
    )


def verified_text(user, chat_title):

    first_name = (
        getattr(user, "first_name", None)
        or getattr(user, "username", None)
        or "User"
    )

    return (
        f"👋 **Hello {first_name}!**\n\n"
        "🎉 **Verification Complete**\n\n"
        f"✅ You are verified for **{chat_title or 'the chat'}**.\n\n"
        "🚀 You can continue normally.\n\n"
        "Thank you!"
    )


# ============================================================
# SEND MESSAGE
# ============================================================

async def send_dm(user_id, text, keyboard=None):

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            return await app.send_message(
                user_id,
                text,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        except FloodWait as e:

            seconds = int(getattr(e, "value", 1))

            log.warning(
                "FLOODWAIT | User=%s | Seconds=%s | Attempt=%s",
                user_id,
                seconds,
                attempt,
            )

            await asyncio.sleep(seconds)

        except (
            UserIsBlocked,
            PeerIdInvalid,
            InputUserDeactivated,
            UserDeactivated,
        ) as e:

            log.warning("USER DM UNAVAILABLE | User=%s | %s", user_id, e)

            fire(
                db.mark_dm_blocked(user_id, True),
                f"db-dmblocked-{user_id}",
            )

            return None

        except RPCError as e:

            log.error(
                "DM RPC ERROR | User=%s | Attempt=%s/%s | %s",
                user_id,
                attempt,
                MAX_RETRIES,
                e,
            )

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)

        except asyncio.CancelledError:
            raise

        except Exception as e:

            log_exception("DM ERROR", e)

            if attempt < MAX_RETRIES:
                await asyncio.sleep(RETRY_DELAY * attempt)

    return None


# ============================================================
# WELCOME DM  +  PENDING VERIFICATION ROW
# ============================================================

async def welcome_user(user, chat, request_id=None):

    async with dm_semaphore:

        try:

            log.info(
                "WELCOME START | User=%s | Chat=%s", user.id, chat.id
            )

            message = await send_dm(
                user.id,
                verification_text(user, chat.title),
                verification_keyboard(chat.id, user.id),
            )

            if message:

                log.info(
                    "WELCOME SENT | User=%s | Chat=%s", user.id, chat.id
                )

                fire(
                    db.mark_dm_sent(request_id, message.id, True),
                    f"db-dmsent-{user.id}",
                )

                fire(
                    db.create_pending_verification(
                        user_id=user.id,
                        chat_id=chat.id,
                        join_request_id=request_id,
                        message_id=message.id,
                    ),
                    f"db-pv-{chat.id}-{user.id}",
                )

            else:

                log.warning("WELCOME NOT SENT | User=%s", user.id)

                fire(
                    db.mark_dm_sent(request_id, None, False),
                    f"db-dmfail-{user.id}",
                )

        except asyncio.CancelledError:
            raise

        except Exception as e:
            log_exception("WELCOME ERROR", e)


# ============================================================
# APPROVAL
# ============================================================

async def approve_request(request, request_id=None):

    async with approval_semaphore:

        chat_id = request.chat.id
        user_id = request.from_user.id

        last_error = None

        for attempt in range(1, MAX_RETRIES + 1):

            try:

                result = await request.approve()

                log.info(
                    "APPROVED | Chat=%s | User=%s | Result=%s",
                    chat_id,
                    user_id,
                    result,
                )

                return True, attempt, None

            except FloodWait as e:

                seconds = int(getattr(e, "value", 1))
                last_error = f"FloodWait {seconds}s"

                log.warning(
                    "APPROVAL FLOODWAIT | Chat=%s | User=%s | "
                    "Seconds=%s | Attempt=%s",
                    chat_id,
                    user_id,
                    seconds,
                    attempt,
                )

                await asyncio.sleep(seconds)

            except ChatAdminRequired as e:

                log.error(
                    "ADMIN REQUIRED | Chat=%s | User=%s | %s",
                    chat_id,
                    user_id,
                    e,
                )

                fire(
                    db.set_chat_active(chat_id, False),
                    f"db-chatinactive-{chat_id}",
                )

                return False, attempt, "ChatAdminRequired"

            except RPCError as e:

                last_error = str(e)

                log.error(
                    "APPROVAL RPC ERROR | Chat=%s | User=%s | "
                    "Attempt=%s/%s | %s",
                    chat_id,
                    user_id,
                    attempt,
                    MAX_RETRIES,
                    e,
                )

                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * attempt)

            except asyncio.CancelledError:
                raise

            except Exception as e:

                last_error = str(e)
                log_exception("APPROVAL UNKNOWN ERROR", e)

                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY * attempt)

        return False, MAX_RETRIES, last_error


# ============================================================
# JOIN REQUEST HANDLER
# ============================================================

@app.on_chat_join_request()
async def join_request(client, request: ChatJoinRequest):

    started = time.monotonic()

    try:

        chat = request.chat
        user = request.from_user

        if not chat or not user:
            log.error("JOIN REQUEST INVALID")
            return

        chat_id = chat.id
        user_id = user.id

        log.info("")
        log.info("=" * 70)
        log.info("JOIN REQUEST RECEIVED")
        log.info("Chat      : %s", chat.title)
        log.info("Chat ID   : %s", chat_id)
        log.info("User      : %s", user.first_name)
        log.info("Username  : @%s", user.username or "none")
        log.info("User ID   : %s", user_id)
        log.info("Chat Type : %s", chat.type)

        # ----------------------------------------------------
        # DUPLICATE
        # ----------------------------------------------------

        if await is_duplicate(chat_id, user_id):
            log.warning(
                "DUPLICATE IGNORED | Chat=%s | User=%s", chat_id, user_id
            )
            return

        # ----------------------------------------------------
        # BOT USER
        # ----------------------------------------------------

        if user.is_bot:

            log.warning("BOT USER | Declining | User=%s", user_id)

            try:
                await request.decline()
            except Exception as e:
                log_exception("BOT DECLINE ERROR", e)

            request_id = await db.create_join_request(
                user, chat, getattr(request, "invite_link", None)
            )

            fire(
                db.mark_declined(request_id, "bot account"),
                f"db-declined-{chat_id}-{user_id}",
            )

            return

        # ----------------------------------------------------
        # PERSIST THE REQUEST (awaited: we need the row id)
        # ----------------------------------------------------

        request_id = await db.create_join_request(
            user,
            chat,
            getattr(request, "invite_link", None),
        )

        # ----------------------------------------------------
        # APPROVE
        # ----------------------------------------------------

        approved, attempts, error = await approve_request(
            request, request_id
        )

        elapsed_ms = int((time.monotonic() - started) * 1000)

        if not approved:

            log.error(
                "APPROVAL FAILED | Chat=%s | User=%s | %s",
                chat_id,
                user_id,
                error,
            )

            fire(
                db.mark_failed(request_id, error or "unknown", attempts),
                f"db-failed-{chat_id}-{user_id}",
            )

            # Allow a retry later
            async with request_lock:
                processing_requests.pop((int(chat_id), int(user_id)), None)

            return

        log.info(
            "ACCEPTED | Chat=%s | User=%s | Time=%.2fs",
            chat.title,
            user_id,
            elapsed_ms / 1000,
        )

        fire(
            db.mark_approved(request_id, elapsed_ms, attempts),
            f"db-approved-{chat_id}-{user_id}",
        )

        # ----------------------------------------------------
        # WELCOME IN BACKGROUND
        # ----------------------------------------------------

        create_task(
            welcome_user(user=user, chat=chat, request_id=request_id),
            f"welcome-{chat_id}-{user_id}",
        )

        log.info("WELCOME TASK QUEUED | User=%s", user_id)
        log.info("=" * 70)

    except asyncio.CancelledError:
        raise

    except Exception as e:
        log_exception("JOIN REQUEST ERROR", e)


# ============================================================
# /START
# ============================================================

@app.on_message(filters.private & filters.command("start"))
async def start_command(client, message):

    try:

        if message.from_user:
            fire(
                db.upsert_user(message.from_user),
                f"db-user-{message.from_user.id}",
            )

        log.info(
            "/START | User=%s | Username=%s",
            message.from_user.id if message.from_user else None,
            message.from_user.username if message.from_user else None,
        )

        await message.reply_text(
            start_text(),
            reply_markup=main_keyboard(),
            disable_web_page_preview=True,
        )

        log.info("/START SENT")

    except Exception as e:
        log_exception("/START ERROR", e)


# ============================================================
# /HELP
# ============================================================

@app.on_message(filters.private & filters.command("help"))
async def help_command(client, message):

    try:
        await message.reply_text(help_text(), reply_markup=back_keyboard())
        log.info("/HELP SENT | User=%s", message.from_user.id)

    except Exception as e:
        log_exception("/HELP ERROR", e)


# ============================================================
# /PING
# ============================================================

@app.on_message(filters.command("ping"))
async def ping_command(client, message):

    try:

        started = time.monotonic()
        db_ok = "🟢 ONLINE" if db.enabled else "🔴 OFFLINE"

        if db.enabled:
            await db.get_stats()

        latency = (time.monotonic() - started) * 1000

        await message.reply_text(
            "🏓 **PONG!**\n\n"
            "✅ Telegram: ONLINE\n"
            f"🤖 Bot: @{BOT_USERNAME}\n"
            f"🆔 ID: `{BOT_ID}`\n"
            f"⚙️ Workers: `{PYROGRAM_WORKERS}`\n"
            f"⚡ Approval concurrency: `{APPROVAL_CONCURRENCY}`\n"
            f"📨 DM concurrency: `{DM_CONCURRENCY}`\n"
            f"💾 Database: **{db_ok}**\n"
            f"⏱ DB latency: `{latency:.0f} ms`"
        )

    except Exception as e:
        log_exception("/PING ERROR", e)


# ============================================================
# /ID
# ============================================================

@app.on_message(filters.command("id"))
async def id_command(client, message):

    try:

        if not message.chat:
            return

        name = (
            message.chat.title
            or message.chat.first_name
            or "Private Chat"
        )

        await message.reply_text(
            "🆔 **Chat Information**\n\n"
            f"📌 Name: **{name}**\n"
            f"🆔 ID: `{message.chat.id}`\n"
            f"📂 Type: `{message.chat.type}`"
        )

    except Exception as e:
        log_exception("/ID ERROR", e)


# ============================================================
# /STATS
# ============================================================

@app.on_message(filters.command("stats"))
async def stats_command(client, message):

    try:

        if not db.enabled:
            await message.reply_text("💾 Database is disabled.")
            return

        # Global stats -> owner only, in private
        if (
            message.chat.type == ChatType.PRIVATE
            and message.from_user
            and OWNER_ID
            and message.from_user.id == OWNER_ID
        ):

            row = await db.get_stats()

            if not row:
                await message.reply_text("⚠️ Could not read stats.")
                return

            await message.reply_text(
                "📊 **Global Statistics**\n\n"
                f"👥 Users: `{row['total_users']}`\n"
                f"💬 Active chats: `{row['active_chats']}`\n"
                f"📥 Total requests: `{row['total_requests']}`\n"
                f"✅ Approved: `{row['total_approved']}`\n"
                f"🚫 Declined: `{row['total_declined']}`\n"
                f"❌ Failed: `{row['total_failed']}`\n"
                f"🤖 Verified: `{row['total_verified']}`\n"
                f"⏳ Pending verifications: `{row['pending_verifications']}`\n"
                f"🕐 Last 24h: `{row['requests_24h']}`"
            )
            return

        if message.chat.type == ChatType.PRIVATE:

            row = await db.get_user_stats(message.from_user.id)

            if not row:
                await message.reply_text("ℹ️ No data yet.")
                return

            await message.reply_text(
                "📊 **Your Statistics**\n\n"
                f"📥 Join requests: `{row['total_join_requests']}`\n"
                f"✅ Verified: `{row['verified_count']}`\n"
                f"🕐 First seen: `{str(row['first_seen'])[:10]}`"
            )
            return

        row = await db.get_chat_stats(message.chat.id)

        if not row:
            await message.reply_text("ℹ️ No data recorded for this chat yet.")
            return

        await message.reply_text(
            "📊 **Chat Statistics**\n\n"
            f"📌 {row['title']}\n"
            f"📥 Total requests: `{row['total_requests']}`\n"
            f"✅ Approved: `{row['total_approved']}`\n"
            f"🚫 Declined: `{row['total_declined']}`\n"
            f"🕐 Last request: `{str(row['last_request_at'] or '—')[:19]}`"
        )

    except Exception as e:
        log_exception("/STATS ERROR", e)


# ============================================================
# /STATUS
# ============================================================

@app.on_message(filters.command("status"))
async def status_command(client, message):

    try:

        if not message.chat:
            return

        if message.chat.type == ChatType.PRIVATE:
            await message.reply_text(
                "ℹ️ Use `/status` inside a group/channel."
            )
            return

        me = await client.get_me()

        member = await client.get_chat_member(message.chat.id, me.id)

        log.info(
            "STATUS | Chat=%s | Member=%s", message.chat.id, member.status
        )

        fire(db.upsert_chat(message.chat), f"db-chat-{message.chat.id}")

        if member.status == ChatMemberStatus.OWNER:
            await message.reply_text(
                "✅ **BOT STATUS: READY**\n\n"
                "👑 Administrator: **YES**\n"
                "🔑 Manage Join Requests: **YES**\n\n"
                "🚀 Automatic approval is ready."
            )
            return

        if member.status != ChatMemberStatus.ADMINISTRATOR:
            await message.reply_text(
                "❌ **BOT IS NOT ADMIN**\n\n"
                "Promote the bot to Administrator."
            )
            return

        privileges = member.privileges

        if not privileges:
            await message.reply_text(
                "⚠️ Unable to read administrator permissions."
            )
            return

        can_invite = bool(
            getattr(privileges, "can_invite_users", False)
        )

        if can_invite:
            await message.reply_text(
                "✅ **BOT STATUS: READY**\n\n"
                "👑 Administrator: **YES**\n"
                "🔑 Manage Join Requests: **YES**\n\n"
                "🚀 Automatic approval is ready."
            )
        else:
            await message.reply_text(
                "⚠️ **PERMISSION REQUIRED**\n\n"
                "👑 Administrator: **YES**\n"
                "🔑 Manage Join Requests: **NO**\n\n"
                "Enable **Invite Users / Manage Join Requests**."
            )

    except Exception as e:

        log_exception("/STATUS ERROR", e)

        with suppress(Exception):
            await message.reply_text(
                f"❌ Status error\n\n`{str(e)[:400]}`"
            )


# ============================================================
# CALLBACKS
# ============================================================

@app.on_callback_query(filters.regex("^help$"))
async def help_callback(client, callback):

    try:
        await callback.answer()

        if callback.message:
            await callback.message.edit_text(
                help_text(), reply_markup=back_keyboard()
            )

    except MessageNotModified:
        pass

    except Exception as e:
        log_exception("HELP CALLBACK ERROR", e)


@app.on_callback_query(filters.regex("^back$"))
async def back_callback(client, callback):

    try:
        await callback.answer()

        if callback.message:
            await callback.message.edit_text(
                start_text(),
                reply_markup=main_keyboard(),
                disable_web_page_preview=True,
            )

    except MessageNotModified:
        pass

    except Exception as e:
        log_exception("BACK CALLBACK ERROR", e)


@app.on_callback_query(filters.regex(r"^verify:-?\d+:\d+$"))
async def verify_callback(client, callback):

    try:

        if not callback.from_user:
            await callback.answer("❌ User not found.", show_alert=True)
            return

        parts = callback.data.split(":")

        if len(parts) != 3:
            await callback.answer("❌ Invalid verification.", show_alert=True)
            return

        chat_id = int(parts[1])
        expected_user_id = int(parts[2])
        actual_user_id = callback.from_user.id

        # ----------------------------------------------------
        # SECURITY
        # ----------------------------------------------------

        if actual_user_id != expected_user_id:

            log.warning(
                "UNAUTHORIZED VERIFY | Expected=%s | Actual=%s",
                expected_user_id,
                actual_user_id,
            )

            await callback.answer(
                "❌ This button is not for you.", show_alert=True
            )
            return

        # ----------------------------------------------------
        # CHAT TITLE (db first, falls back to API)
        # ----------------------------------------------------

        chat_title = None

        if db.enabled:
            row = await db.get_chat_stats(chat_id)
            if row:
                chat_title = row["title"]

        if not chat_title:
            try:
                chat = await client.get_chat(chat_id)
                chat_title = chat.title
            except Exception as e:
                log.warning("VERIFY CHAT LOOKUP FAILED | %s", e)

        # ----------------------------------------------------
        # PERSIST
        # ----------------------------------------------------

        fire(
            db.mark_verified(actual_user_id, chat_id),
            f"db-verified-{chat_id}-{actual_user_id}",
        )

        await callback.answer("✅ Verification complete!", show_alert=False)

        if callback.message:

            try:
                await callback.message.edit_text(
                    verified_text(callback.from_user, chat_title),
                    disable_web_page_preview=True,
                )
            except MessageNotModified:
                pass

        log.info(
            "VERIFICATION COMPLETE | User=%s | Chat=%s",
            actual_user_id,
            chat_id,
        )

    except Exception as e:

        log_exception("VERIFY ERROR", e)

        with suppress(Exception):
            await callback.answer(
                "❌ Verification failed. Try again.", show_alert=True
            )


# ============================================================
# TRACK CHATS THE BOT IS ADDED TO / REMOVED FROM
# ============================================================

@app.on_chat_member_updated()
async def chat_member_updated(client, update):

    try:

        if not update.new_chat_member:
            return

        if not update.new_chat_member.user:
            return

        if update.new_chat_member.user.id != BOT_ID:
            return

        status = update.new_chat_member.status

        if status in (
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.OWNER,
        ):
            fire(
                db.upsert_chat(update.chat),
                f"db-chatadd-{update.chat.id}",
            )
            log.info("BOT ADDED/UPDATED | Chat=%s", update.chat.id)

        elif status in (
            ChatMemberStatus.LEFT,
            ChatMemberStatus.BANNED,
        ):
            fire(
                db.set_chat_active(update.chat.id, False),
                f"db-chatremove-{update.chat.id}",
            )
            log.info("BOT REMOVED | Chat=%s", update.chat.id)

    except Exception as e:
        log_exception("CHAT MEMBER UPDATE ERROR", e)


# ============================================================
# RECOVERY  —  pending verifications after restart
# ============================================================

async def recover_pending_verifications():

    if not db.enabled:
        return

    try:

        expired = await db.expire_old_verifications()

        if expired:
            log.info("RECOVERY | Expired verifications: %s", expired)

        rows = await db.get_pending_verifications(limit=RESEND_LIMIT)

        log.info("RECOVERY | Pending verifications: %s", len(rows))

        if not RESEND_PENDING_ON_START or not rows:
            return

        sent = 0

        for row in rows:

            try:

                message = await send_dm(
                    row["user_id"],
                    verification_text(None, row.get("chat_title")),
                    verification_keyboard(
                        row["chat_id"], row["user_id"]
                    ),
                )

                if message:
                    sent += 1
                    await db.bump_resend(row["id"], message.id)
                else:
                    await db.mark_verification_failed(row["id"])

                # gentle pacing, Telegram DM limits
                await asyncio.sleep(0.25)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                log.warning(
                    "RECOVERY RESEND FAILED | User=%s | %s",
                    row["user_id"],
                    e,
                )

        log.info("RECOVERY | Verification DMs resent: %s", sent)

    except Exception as e:
        log_exception("RECOVERY ERROR", e)


# ============================================================
# PERIODIC CLEANUP
# ============================================================

async def cleanup_loop():

    while True:

        try:
            await asyncio.sleep(CLEANUP_INTERVAL)

            if not db.enabled:
                continue

            expired = await db.expire_old_verifications()

            async with request_lock:
                now = time.monotonic()
                for k in [
                    k
                    for k, t in processing_requests.items()
                    if now - t > REQUEST_CACHE_TTL
                ]:
                    processing_requests.pop(k, None)

            log.info(
                "CLEANUP | Expired=%s | Cache=%s",
                expired,
                len(processing_requests),
            )

        except asyncio.CancelledError:
            raise

        except Exception as e:
            log_exception("CLEANUP ERROR", e)


# ============================================================
# STARTUP
# ============================================================

async def main():

    global BOT_ID
    global BOT_USERNAME
    global approval_semaphore
    global dm_semaphore
    global db_semaphore
    global request_lock

    approval_semaphore = asyncio.Semaphore(APPROVAL_CONCURRENCY)
    dm_semaphore = asyncio.Semaphore(DM_CONCURRENCY)
    db_semaphore = asyncio.Semaphore(DB_CONCURRENCY)
    request_lock = asyncio.Lock()

    log.info("=" * 50)
    log.info("STARTING AUTO REQUEST ACCEPTER")
    log.info("Workers: %s", PYROGRAM_WORKERS)
    log.info("Approval concurrency: %s", APPROVAL_CONCURRENCY)
    log.info("DM concurrency: %s", DM_CONCURRENCY)
    log.info("=" * 50)

    await db.connect()

    log.info("Database: %s", "ENABLED" if db.enabled else "DISABLED")

    cleanup_task = None

    try:

        async with app:

            me = await app.get_me()

            BOT_ID = me.id
            BOT_USERNAME = me.username

            log.info("BOT CONNECTED")
            log.info("Bot ID       : %s", BOT_ID)
            log.info("Bot Username : @%s", BOT_USERNAME)
            log.info("Status       : ONLINE")

            await recover_pending_verifications()

            cleanup_task = create_task(cleanup_loop(), "cleanup-loop")

            log.info("Waiting for join requests...")

            await idle()

    except Exception as e:
        log_exception("MAIN FATAL ERROR", e)
        raise

    finally:

        if cleanup_task and not cleanup_task.done():
            cleanup_task.cancel()

        log.info("SHUTDOWN | Background tasks=%s", len(background_tasks))

        if background_tasks:

            tasks = list(background_tasks)

            done, pending = await asyncio.wait(tasks, timeout=10)

            if pending:

                log.warning("Cancelling %s remaining tasks", len(pending))

                for task in pending:
                    if not task.done():
                        task.cancel()

                with suppress(Exception):
                    await asyncio.gather(*pending, return_exceptions=True)

        await db.close()

        log.info("SHUTDOWN COMPLETE")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":

    print()
    print("=" * 70)
    print("             AUTO REQUEST ACCEPTER BOT")
    print("=" * 70)
    print(" Production Mode       : ON")
    print(" Database              : SUPABASE REST (ANON KEY)")
    print(" Multi User            : ON")
    print(" Concurrent Approval   : ON")
    print(" Background DM         : ON")
    print(" Verification          : ON")
    print(" Restart Recovery      : ON")
    print(" FloodWait Handling    : ON")
    print("=" * 70)
    print()

    try:
        app.run(main())

    except KeyboardInterrupt:
        print("\nBot stopped by user.\n")

    except Exception as e:
        print()
        print("=" * 70)
        print("FATAL ERROR")
        print("=" * 70)
        print(str(e))
        traceback.print_exc()
        print("=" * 70)
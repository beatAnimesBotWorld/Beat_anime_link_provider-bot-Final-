#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
        UNIFIED ANIME BOT — OVERPOWERED EDITION v5.0  (~20k lines)
================================================================================
Features:
  ✅ Fully working clone bot support (all menus, force-sub, deep links)
  ✅ User-friendly error messages in plain language (non-technical DM only)
  ✅ Admin gets technical error details separately
  ✅ Conversation-safe: deleted first message never breaks session
  ✅ Bold ! exclamation loading animation on /start
  ✅ Commands auto-register in BOTH main bot and every clone bot
  ✅ All menus fully connected — zero dead buttons
  ✅ Manga tracking with full MangaDex: chapters, pages, status
  ✅ Complete broadcast system (Normal / Auto-delete / Pin / Schedule)
  ✅ Complete upload manager (anime captions, multi-quality)
  ✅ Full auto-forward with filters, replacements, delays, bulk
  ✅ Feature flags: maintenance, clone redirect, error DMs, etc.
  ✅ Full user management: ban, unban, search, export, delete
  ✅ Complete post generation: anime, manga, movie, TV show (AniList+TMDB)
  ✅ Complete category settings: templates, buttons, watermarks, logos
  ✅ Admin panel with image banners, deep-link gen, stats
  ✅ All text is <b>bold</b> throughout
  ✅ Auto-delete previous messages everywhere
  ✅ Robust error handling — no crashes, no unhandled callbacks
================================================================================
"""

# ================================================================================
#                                   IMPORTS
# ================================================================================

import os
import sys
import json
import time
import uuid
import math
import asyncio
import logging
import logging.handlers
import html
import re
import csv
import hashlib
import traceback
import threading
from io import StringIO, BytesIO
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional, Dict, List, Tuple, Any, Union, Set, Callable
from contextlib import asynccontextmanager

import requests
import aiohttp
import psutil

try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False

try:
    import img2pdf
    IMG2PDF_AVAILABLE = True
except ImportError:
    IMG2PDF_AVAILABLE = False

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,
    constants,
    InputMediaPhoto,
    InputMediaVideo,
    InputMediaDocument,
    ChatMember,
    CallbackQuery,
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeAllPrivateChats,
    InlineQueryResultArticle,
    InputTextMessageContent,
    ChatPermissions,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
    JobQueue,
    InlineQueryHandler,
    ConversationHandler,
)
from telegram.error import (
    TelegramError,
    Forbidden,
    BadRequest,
    NetworkError,
    TimedOut,
    RetryAfter,
)
from telegram.constants import ParseMode

from database_safe import *
try:
    from health_check import health_server
except ImportError:
    class _HealthServerStub:
        async def start(self): pass
        async def stop(self): pass
    health_server = _HealthServerStub()

# ================================================================================
#                                LOGGING SETUP
# ================================================================================

os.makedirs("logs", exist_ok=True)

_fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_datefmt = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    format=_fmt,
    datefmt=_datefmt,
    level=logging.INFO,
    handlers=[
        logging.handlers.RotatingFileHandler(
            "logs/bot.log", maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
        ),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger("bot")
db_logger = logging.getLogger("database")
api_logger = logging.getLogger("api")
broadcast_logger = logging.getLogger("broadcast")
error_logger = logging.getLogger("errors")

for name in ["httpx", "httpcore", "telegram", "apscheduler"]:
    logging.getLogger(name).setLevel(logging.WARNING)

# ================================================================================
#                           ENVIRONMENT CONFIGURATION
# ================================================================================

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "")
ADMIN_ID: int = int(os.getenv("ADMIN_ID", "0"))
OWNER_ID: int = int(os.getenv("OWNER_ID", str(ADMIN_ID)))

# Timing
LINK_EXPIRY_MINUTES: int = int(os.getenv("LINK_EXPIRY_MINUTES", "5"))
BROADCAST_CHUNK_SIZE: int = int(os.getenv("BROADCAST_CHUNK_SIZE", "1000"))
BROADCAST_MIN_USERS: int = int(os.getenv("BROADCAST_MIN_USERS", "5000"))
BROADCAST_INTERVAL_MIN: int = int(os.getenv("BROADCAST_INTERVAL_MIN", "20"))
RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "0.05"))

# Ports
PORT: int = int(os.environ.get("PORT", 8080))
WEBHOOK_URL: str = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/") + "/"

# Source content
WELCOME_SOURCE_CHANNEL: int = int(os.getenv("WELCOME_SOURCE_CHANNEL", "-1002530952988"))
WELCOME_SOURCE_MESSAGE_ID: int = int(os.getenv("WELCOME_SOURCE_MESSAGE_ID", "32"))

# Public links / branding
PUBLIC_ANIME_CHANNEL_URL: str = os.getenv("PUBLIC_ANIME_CHANNEL_URL", "https://t.me/BeatAnime")
REQUEST_CHANNEL_URL: str = os.getenv("REQUEST_CHANNEL_URL", "https://t.me/Beat_Hindi_Dubbed")
ADMIN_CONTACT_USERNAME: str = os.getenv("ADMIN_CONTACT_USERNAME", "Beat_Anime_Ocean")
BOT_NAME: str = os.getenv("BOT_NAME", "Anime Bot")

# Image panels
HELP_IMAGE_URL: str = os.getenv("HELP_IMAGE_URL", "")
SETTINGS_IMAGE_URL: str = os.getenv("SETTINGS_IMAGE_URL", "")
STATS_IMAGE_URL: str = os.getenv("STATS_IMAGE_URL", "")
ADMIN_PANEL_IMAGE_URL: str = os.getenv("ADMIN_PANEL_IMAGE_URL", "")
WELCOME_IMAGE_URL: str = os.getenv("WELCOME_IMAGE_URL", "")
BROADCAST_PANEL_IMAGE_URL: str = os.getenv("BROADCAST_PANEL_IMAGE_URL", "")

# Sticker
TRANSITION_STICKER_ID: str = os.getenv("TRANSITION_STICKER", "")

# External APIs
TMDB_API_KEY: str = os.getenv("TMDB_API_KEY", "")

# Global runtime state
BOT_USERNAME: str = ""
I_AM_CLONE: bool = False
BOT_START_TIME: float = time.time()
_clone_bot_cache: Dict[str, Any] = {}
_clone_tasks: Dict[str, Any] = {}  # running clone asyncio tasks

# ── In-memory API cache (performance optimization) ────────────────────────────
_api_cache: Dict[str, Any] = {}
_API_CACHE_TTL: int = 300  # 5 minutes

# ── Filter system (DM/group/user/chat filtering) ──────────────────────────────
filters_config: Dict[str, Any] = {
    "global": {"dm": True, "group": True},
    "commands": {},
    "banned_users": set(),
    "disabled_chats": set(),
}


def _passes_filter(update: "Update", command: str = "") -> bool:
    """Check if a message passes the filter system. Returns False to block."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat:
        return True
    uid = user.id
    cid = chat.id
    # Always allow admins
    if uid in (ADMIN_ID, OWNER_ID):
        return True
    # Banned users
    if uid in filters_config["banned_users"]:
        return False
    # Disabled chats
    if cid in filters_config["disabled_chats"]:
        return False
    # Global DM/group toggle
    is_private = chat.type == "private"
    if is_private and not filters_config["global"].get("dm", True):
        return False
    if not is_private and not filters_config["global"].get("group", True):
        return False
    # Per-command filter
    if command and command in filters_config["commands"]:
        cmd_cfg = filters_config["commands"][command]
        if is_private and not cmd_cfg.get("dm", True):
            return False
        if not is_private and not cmd_cfg.get("group", True):
            return False
    return True


def _cache_get(key: str) -> Optional[Any]:
    """Get a value from the API cache if not expired."""
    entry = _api_cache.get(key)
    if entry and (time.time() - entry["ts"]) < _API_CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(key: str, data: Any) -> None:
    """Store a value in the API cache."""
    _api_cache[key] = {"data": data, "ts": time.time()}
    # Trim cache to prevent unbounded growth
    if len(_api_cache) > 500:
        oldest = min(_api_cache, key=lambda k: _api_cache[k]["ts"])
        _api_cache.pop(oldest, None)

# ================================================================================
#                           STATE MACHINE CONSTANTS
# ================================================================================

# Channel states
(
    ADD_CHANNEL_USERNAME,
    ADD_CHANNEL_TITLE,
    ADD_CHANNEL_JBR,
) = range(3)

# Link states
(
    GENERATE_LINK_IDENTIFIER,
    GENERATE_LINK_TITLE,
) = range(3, 5)

# Clone states
(ADD_CLONE_TOKEN,) = range(5, 6)

# Backup / move
(
    SET_BACKUP_CHANNEL,
    PENDING_MOVE_TARGET,
) = range(6, 8)

# Broadcast states
(
    PENDING_BROADCAST,
    PENDING_BROADCAST_OPTIONS,
    PENDING_BROADCAST_CONFIRM,
    SCHEDULE_BROADCAST_DATETIME,
    SCHEDULE_BROADCAST_MSG,
) = range(8, 13)

# Category settings states
(
    SET_CATEGORY_TEMPLATE,
    SET_CATEGORY_BRANDING,
    SET_CATEGORY_BUTTONS,
    SET_CATEGORY_CAPTION,
    SET_CATEGORY_THUMBNAIL,
    SET_CATEGORY_FONT,
    SET_CATEGORY_LOGO,
    SET_CATEGORY_LOGO_POS,
    SET_WATERMARK_TEXT,
    SET_WATERMARK_POS,
) = range(13, 23)

# Auto-forward states
(
    AF_ADD_CONNECTION_SOURCE,
    AF_ADD_CONNECTION_TARGET,
    AF_ADD_FILTER_WORD,
    AF_ADD_BLACKLIST_WORD,
    AF_ADD_WHITELIST_WORD,
    AF_ADD_REPLACEMENT_PATTERN,
    AF_ADD_REPLACEMENT_VALUE,
    AF_SET_DELAY,
    AF_SET_CAPTION,
    AF_BULK_FORWARD_COUNT,
) = range(23, 33)

# Auto manga states
(
    AU_ADD_MANGA_TITLE,
    AU_ADD_MANGA_TARGET,
    AU_REMOVE_MANGA,
    AU_CUSTOM_INTERVAL,
) = range(33, 37)

# Upload states
(
    UPLOAD_SET_CAPTION,
    UPLOAD_SET_SEASON,
    UPLOAD_SET_EPISODE,
    UPLOAD_SET_TOTAL,
    UPLOAD_SET_CHANNEL,
) = range(36, 41)

# User management states
(
    BAN_USER_INPUT,
    UNBAN_USER_INPUT,
    DELETE_USER_INPUT,
    SEARCH_USER_INPUT,
) = range(41, 45)

# Fill title
PENDING_FILL_TITLE = 45

# Settings
(
    SET_FEATURE_FLAG,
    SET_LINK_EXPIRY,
    SET_BOT_NAME,
    SET_WELCOME_MSG,
    SET_ADMIN_CONTACT,
) = range(46, 51)

# Manga
(
    MANGA_SEARCH_INPUT,
) = range(51, 52)

# Auto-manga delivery states
(
    AU_MANGA_CUSTOM_INTERVAL,
) = range(52, 53)

# Conversation dictionaries
user_states: Dict[int, int] = {}
user_data_temp: Dict[int, Dict[str, Any]] = {}


# ================================================================================
#                          UPLOAD MANAGER GLOBALS
# ================================================================================

DEFAULT_CAPTION = (
    "<b>◈ {anime_name}</b>\n\n"
    "<b>- Season:</b> {season}\n"
    "<b>- Episode:</b> {episode}\n"
    "<b>- Audio track:</b> Hindi | Official\n"
    "<b>- Quality:</b> {quality}\n"
    "<blockquote>"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>POWERED BY:</b> @beeetanime\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>MAIN Channel:</b> @Beat_Hindi_Dubbed\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>Group:</b> @Beat_Anime_Discussion\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱"
    "</blockquote>"
)

ALL_QUALITIES: List[str] = ["480p", "720p", "1080p", "4K", "2160p"]

upload_progress: Dict[str, Any] = {
    "target_chat_id": None,
    "anime_name": "Anime Name",
    "season": 1,
    "episode": 1,
    "total_episode": 1,
    "video_count": 0,
    "selected_qualities": ["480p", "720p", "1080p"],
    "base_caption": DEFAULT_CAPTION,
    "auto_caption_enabled": True,
    "forward_mode": "copy",     # copy | move
    "protect_content": False,
}

upload_lock = asyncio.Lock()


# ================================================================================
#                         BROADCAST MODE CONSTANTS
# ================================================================================

class BroadcastMode:
    NORMAL = "normal"
    AUTO_DELETE = "auto_delete"
    PIN = "pin"
    DELETE_PIN = "delete_pin"
    SILENT = "silent"


# ================================================================================
#                       TEXT UTILITIES & CONVERTERS
# ================================================================================

SMALL_CAPS_MAP: Dict[str, str] = {
    "a": "ᴀ", "b": "ʙ", "c": "ᴄ", "d": "ᴅ", "e": "ᴇ", "f": "ғ", "g": "ɢ",
    "h": "ʜ", "i": "ɪ", "j": "ᴊ", "k": "ᴋ", "l": "ʟ", "m": "ᴍ", "n": "ɴ",
    "o": "ᴏ", "p": "ᴘ", "q": "ǫ", "r": "ʀ", "s": "s", "t": "ᴛ", "u": "ᴜ",
    "v": "ᴠ", "w": "ᴡ", "x": "x", "y": "ʏ", "z": "ᴢ",
}
SMALL_CAPS_MAP.update({k.upper(): v for k, v in SMALL_CAPS_MAP.items()})

MATH_BOLD_MAP: Dict[str, str] = {
    "A": "𝗔", "B": "𝗕", "C": "𝗖", "D": "𝗗", "E": "𝗘", "F": "𝗙", "G": "𝗚",
    "H": "𝗛", "I": "𝗜", "J": "𝗝", "K": "𝗞", "L": "𝗟", "M": "𝗠", "N": "𝗡",
    "O": "𝗢", "P": "𝗣", "Q": "𝗤", "R": "𝗥", "S": "𝗦", "T": "𝗧", "U": "𝗨",
    "V": "𝗩", "W": "𝗪", "X": "𝗫", "Y": "𝗬", "Z": "𝗭",
    "a": "𝗮", "b": "𝗯", "c": "𝗰", "d": "𝗱", "e": "𝗲", "f": "𝗳", "g": "𝗴",
    "h": "𝗵", "i": "𝗶", "j": "𝗷", "k": "𝗸", "l": "𝗹", "m": "𝗺", "n": "𝗻",
    "o": "𝗼", "p": "𝗽", "q": "𝗾", "r": "𝗿", "s": "𝘀", "t": "𝘁", "u": "𝘂",
    "v": "𝘃", "w": "𝘄", "x": "𝘅", "y": "𝘆", "z": "𝘇",
    "0": "𝟬", "1": "𝟭", "2": "𝟮", "3": "𝟯", "4": "𝟰",
    "5": "𝟱", "6": "𝟲", "7": "𝟳", "8": "𝟴", "9": "𝟵",
}


def small_caps(text: str) -> str:
    """Convert ASCII letters to Unicode small caps, skipping HTML tags."""
    result, inside_tag = [], False
    for ch in text:
        if ch == "<":
            inside_tag = True
            result.append(ch)
        elif ch == ">":
            inside_tag = False
            result.append(ch)
        elif inside_tag:
            result.append(ch)
        else:
            result.append(SMALL_CAPS_MAP.get(ch, ch))
    return "".join(result)


def math_bold(text: str) -> str:
    """Convert text to Unicode math bold for button labels."""
    return "".join(MATH_BOLD_MAP.get(ch, ch) for ch in text)


def bold_button(label: str, **kwargs) -> InlineKeyboardButton:
    """Return an InlineKeyboardButton with math-bold label text."""
    return InlineKeyboardButton(math_bold(label), **kwargs)


def b(text: str) -> str:
    """Wrap text in HTML bold tags."""
    return f"<b>{text}</b>"


def code(text: str) -> str:
    """Wrap text in HTML code tags."""
    return f"<code>{text}</code>"


def bq(content: str, expandable: bool = False) -> str:
    """Wrap text in a proper HTML blockquote tag."""
    tag = "blockquote expandable" if expandable else "blockquote"
    return f"<{tag}>{content}</{tag.split()[0]}>"


def e(text: str) -> str:
    """HTML-escape text safely."""
    return html.escape(str(text))


def strip_html(text: str) -> str:
    """Strip all HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", str(text))


def truncate(text: str, max_len: int = 200, suffix: str = "…") -> str:
    """Truncate text to max_len characters."""
    t = str(text)
    return t if len(t) <= max_len else t[: max_len - len(suffix)] + suffix


def format_number(n: int) -> str:
    """Format large numbers with commas."""
    return f"{n:,}"


def format_size(bytes_val: int) -> str:
    """Human-readable file size."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val //= 1024
    return f"{bytes_val:.2f} PB"


def format_duration(seconds: int) -> str:
    """Format seconds into h m s string."""
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}m")
    parts.append(f"{s}s")
    return " ".join(parts)


def parse_date(d: Optional[Dict]) -> str:
    """Parse AniList date dict {'year':x,'month':y,'day':z} to readable string."""
    if not d:
        return "Unknown"
    try:
        parts = []
        if d.get("day"):
            parts.append(str(d["day"]))
        if d.get("month"):
            import calendar
            parts.append(calendar.month_abbr[d["month"]])
        if d.get("year"):
            parts.append(str(d["year"]))
        return " ".join(parts) if parts else "Unknown"
    except Exception:
        return "Unknown"


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def paginate(items: list, page: int, per_page: int = 10) -> Tuple[list, int, int]:
    """Return (page_items, total_pages, current_page)."""
    total = len(items)
    total_pages = max(1, math.ceil(total / per_page))
    page = max(0, min(page, total_pages - 1))
    start = page * per_page
    return items[start : start + per_page], total_pages, page


# ================================================================================
#                         USER-FRIENDLY ERROR MESSAGES
# ================================================================================

class UserFriendlyError:
    """
    Translates technical errors into plain, friendly language
    that non-coders can understand.
    """

    FRIENDLY_MAP: Dict[str, str] = {
        "forbidden": (
            "🚫 <b>Bot can't message this user</b>\n\n"
            "The user has blocked the bot or deleted their account."
        ),
        "chat not found": (
            "🔍 <b>Chat not found</b>\n\n"
            "The channel or group doesn't exist, or the bot hasn't been added there."
        ),
        "bot is not a member": (
            "🤖 <b>Bot is not in the channel</b>\n\n"
            "Please add the bot to the channel as an admin first."
        ),
        "not enough rights": (
            "🔐 <b>Missing permissions</b>\n\n"
            "The bot doesn't have admin rights in that channel. "
            "Please make the bot an admin with appropriate permissions."
        ),
        "message to edit not found": (
            "💬 <b>Message was deleted</b>\n\n"
            "The message was already deleted, so it couldn't be updated. This is harmless."
        ),
        "message is not modified": (
            "✏️ <b>Nothing changed</b>\n\n"
            "The message already shows the latest information."
        ),
        "query is too old": (
            "⏰ <b>Button expired</b>\n\n"
            "This button is too old. Please tap the menu button again to get a fresh one."
        ),
        "retry after": (
            "⏳ <b>Telegram rate limit</b>\n\n"
            "Too many messages sent too quickly. The bot will automatically retry shortly."
        ),
        "timed out": (
            "⌛ <b>Connection timed out</b>\n\n"
            "The request took too long. Please try again."
        ),
        "network error": (
            "🌐 <b>Network issue</b>\n\n"
            "There was a connection problem. Please try again in a moment."
        ),
        "invalid token": (
            "🔑 <b>Invalid bot token</b>\n\n"
            "The bot token provided doesn't work. Please check it and try again."
        ),
        "wrong file identifier": (
            "🖼 <b>File not available</b>\n\n"
            "This file is no longer accessible. Please send it again."
        ),
        "parse entities": (
            "📝 <b>Text formatting error</b>\n\n"
            "There was an issue formatting the message. This has been logged."
        ),
        "peer_id_invalid": (
            "👤 <b>User ID is invalid</b>\n\n"
            "That user ID doesn't exist or can't be reached."
        ),
    }

    GENERIC_USER_MSG = (
        "😅 <b>Something went wrong</b>\n\n"
        "Don't worry — this isn't your fault. "
        "The issue has been automatically reported to our team."
    )

    @staticmethod
    def get_user_message(error: Exception) -> str:
        """Return a friendly message for the user."""
        err_str = str(error).lower()
        for key, msg in UserFriendlyError.FRIENDLY_MAP.items():
            if key in err_str:
                return msg
        return UserFriendlyError.GENERIC_USER_MSG

    @staticmethod
    def get_admin_message(error: Exception, context_info: str = "") -> str:
        """Return a technical message for the admin."""
        err_type = type(error).__name__
        err_detail = str(error)
        tb = traceback.format_exc()
        tb_short = tb[-1500:] if len(tb) > 1500 else tb
        return (
            f"<b>⚠️ Technical Error</b>\n"
            f"<b>Type:</b> <code>{e(err_type)}</code>\n"
            f"<b>Detail:</b> <code>{e(err_detail[:300])}</code>\n"
            + (f"<b>Context:</b> <code>{e(context_info[:200])}</code>\n" if context_info else "")
            + f"\n<pre>{e(tb_short)}</pre>"
        )

    @staticmethod
    def is_ignorable(error: Exception) -> bool:
        """Return True for errors that are harmless and shouldn't be reported."""
        ignorable = [
            "query is too old",
            "message is not modified",
            "message to edit not found",
            "have no rights to send",
        ]
        err_str = str(error).lower()
        return any(ig in err_str for ig in ignorable)


# ================================================================================
#                           SAFE TELEGRAM SEND HELPERS
# ================================================================================

async def safe_delete(bot: Bot, chat_id: int, message_id: int) -> bool:
    """Delete a message safely, ignoring all errors."""
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        return True
    except Exception:
        return False


async def safe_answer(
    query: CallbackQuery,
    text: str = "",
    show_alert: bool = False,
) -> None:
    """Answer a callback query, silently ignoring timeout errors."""
    try:
        await query.answer(text=text, show_alert=show_alert)
    except Exception:
        pass


async def safe_send_message(
    bot: Bot,
    chat_id: int,
    text: str,
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    disable_web_page_preview: bool = True,
) -> Optional[Any]:
    """Send a message safely with proper error handling."""
    try:
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            disable_web_page_preview=disable_web_page_preview,
        )
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after + 1)
        try:
            return await bot.send_message(
                chat_id=chat_id, text=text, parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
        except Exception:
            return None
    except Exception as exc:
        logger.debug(f"safe_send_message failed to {chat_id}: {exc}")
        return None


async def safe_edit_text(
    query: CallbackQuery,
    text: str,
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> Optional[Any]:
    """Edit a message text safely; fall back to sending new message."""
    try:
        return await query.edit_message_text(
            text=text, parse_mode=parse_mode, reply_markup=reply_markup
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return None
        # fall through to send new message
    except Exception:
        pass
    try:
        chat_id = query.message.chat_id
        return await safe_send_message(
            query.message.get_bot(),
            chat_id, text, parse_mode, reply_markup
        )
    except Exception as exc:
        logger.debug(f"safe_edit_text fallback failed: {exc}")
    return None


async def safe_edit_caption(
    query: CallbackQuery,
    caption: str,
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> Optional[Any]:
    """Edit a message caption safely."""
    try:
        return await query.edit_message_caption(
            caption=caption, parse_mode=parse_mode, reply_markup=reply_markup
        )
    except Exception:
        return await safe_edit_text(query, caption, parse_mode, reply_markup)


async def safe_reply(
    update: Update,
    text: str,
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    disable_web_page_preview: bool = True,
) -> Optional[Any]:
    """Reply to a message or callback query safely."""
    try:
        if update.message:
            return await update.message.reply_text(
                text, parse_mode=parse_mode, reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
        elif update.callback_query and update.callback_query.message:
            return await update.callback_query.message.reply_text(
                text, parse_mode=parse_mode, reply_markup=reply_markup,
                disable_web_page_preview=disable_web_page_preview,
            )
        elif update.effective_chat:
            bot = update._bot
            return await safe_send_message(
                bot, update.effective_chat.id, text, parse_mode, reply_markup
            )
    except Exception as exc:
        logger.debug(f"safe_reply failed: {exc}")
    return None


async def safe_send_photo(
    bot: Bot,
    chat_id: int,
    photo: Any,
    caption: str = "",
    parse_mode: str = ParseMode.HTML,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
) -> Optional[Any]:
    """Send photo, fall back to text-only if photo fails."""
    try:
        return await bot.send_photo(
            chat_id=chat_id, photo=photo, caption=caption,
            parse_mode=parse_mode, reply_markup=reply_markup,
        )
    except Exception as exc:
        logger.debug(f"safe_send_photo failed: {exc}")
        if caption:
            return await safe_send_message(bot, chat_id, caption, parse_mode, reply_markup)
    return None


async def delete_update_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Delete the user's trigger message.
    Never deletes on mobile if it's the ONLY message (prevents exit).
    Skips /start to preserve conversation start safety.
    """
    msg = update.message
    if not msg:
        return
    msg_text = msg.text or ""
    if msg_text.startswith("/start"):
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if user_id == ADMIN_ID and user_states.get(user_id) in (
        PENDING_BROADCAST, PENDING_BROADCAST_OPTIONS, PENDING_BROADCAST_CONFIRM
    ):
        return
    try:
        await msg.delete()
    except Exception:
        pass


async def delete_bot_prompt(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    """Delete the previously stored bot prompt message."""
    msg_id = context.user_data.pop("bot_prompt_message_id", None)
    if msg_id and context.bot:
        await safe_delete(context.bot, chat_id, msg_id)


async def store_bot_prompt(
    context: ContextTypes.DEFAULT_TYPE, msg: Any
) -> None:
    """Store a bot message ID so it can be deleted later."""
    if msg and hasattr(msg, "message_id"):
        context.user_data["bot_prompt_message_id"] = msg.message_id


# ================================================================================
#                     CONVERSATION SAFETY — ANTI-EXIT SYSTEM
# ================================================================================
#
# On mobile Telegram, if the ONLY message in a DM is deleted, the app
# exits the conversation. To prevent this, we:
#   1. Always pin a "safety anchor" message on first /start
#   2. Use loading animation with bold "❗" so there's always a visible message
#   3. Never delete the last message in a conversation
#
# ================================================================================

_safety_anchors: Dict[int, int] = {}   # chat_id → message_id of anchor


async def ensure_safety_anchor(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    """
    Send (or update) a silent anchor message that prevents the
    mobile Telegram exit-on-delete-last-message bug.
    """
    if chat_id in _safety_anchors:
        return
    try:
        anchor = await context.bot.send_message(
            chat_id,
            "<b>❗</b>",
            parse_mode=ParseMode.HTML,
            disable_notification=True,
        )
        _safety_anchors[chat_id] = anchor.message_id
    except Exception as exc:
        logger.debug(f"Safety anchor failed for {chat_id}: {exc}")


async def loading_animation_start(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
) -> Optional[Any]:
    """
    Send a bold loading animation with ❗ as first message.
    The message stays visible (acts as safety anchor) then gets deleted after.
    Returns the message object.
    """
    frames = ["❗", "❗ 𝗟𝗼𝗮𝗱𝗶𝗻𝗴", "❗ 𝗟𝗼𝗮𝗱𝗶𝗻𝗴.", "❗ 𝗟𝗼𝗮𝗱𝗶𝗻𝗴..", "❗ 𝗟𝗼𝗮𝗱𝗶𝗻𝗴..."]
    msg = None
    try:
        msg = await context.bot.send_message(chat_id, b(frames[0]), parse_mode=ParseMode.HTML)
        _safety_anchors[chat_id] = msg.message_id   # register as anchor
        for frame in frames[1:]:
            await asyncio.sleep(0.25)
            try:
                await msg.edit_text(b(frame), parse_mode=ParseMode.HTML)
            except Exception:
                break
        await asyncio.sleep(0.5)
    except Exception as exc:
        logger.debug(f"loading_animation_start failed: {exc}")
    return msg


async def loading_animation_end(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, msg: Optional[Any]
) -> None:
    """Delete the loading message (but only if it's not the last one)."""
    if not msg:
        return
    if _safety_anchors.get(chat_id) == msg.message_id:
        del _safety_anchors[chat_id]
    await safe_delete(context.bot, chat_id, msg.message_id)


async def send_transition_sticker(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    """Send transition sticker BEFORE loading animation."""
    if not TRANSITION_STICKER_ID:
        return
    try:
        sticker_msg = await context.bot.send_sticker(chat_id, TRANSITION_STICKER_ID)
        await asyncio.sleep(1.5)
        await safe_delete(context.bot, chat_id, sticker_msg.message_id)
    except Exception as exc:
        logger.debug(f"Transition sticker failed: {exc}")


# ================================================================================
#                          MAINTENANCE / BAN BLOCK SCREENS
# ================================================================================

async def send_maintenance_block(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Show maintenance message to non-existing users."""
    backup_url = get_setting("backup_channel_url", "")
    text = (
        b("🔧 Bot Under Maintenance") + "\n\n"
        + bq(
            b("We are doing some scheduled maintenance right now.\n\n")
            + "<b>Existing members can still access the bot.\n"
            "New members, please wait for us to come back online.</b>",
        ) + "\n\n"
        + b("Stay updated via our backup channel.")
    )
    keyboard = []
    if backup_url:
        keyboard.append([InlineKeyboardButton("📢 Backup Channel", url=backup_url)])
    markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    try:
        if update.callback_query:
            await safe_edit_text(update.callback_query, text, reply_markup=markup)
        elif update.effective_chat:
            await safe_send_message(
                context.bot, update.effective_chat.id, text, reply_markup=markup
            )
    except Exception as exc:
        logger.debug(f"send_maintenance_block error: {exc}")


async def send_ban_screen(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Show a user-friendly ban screen."""
    text = (
        b("🚫 You have been restricted") + "\n\n"
        + bq(
            b("Your access to this bot has been suspended.\n\n")
            + b("If you think this is a mistake, please contact the admin.")
        ) + "\n\n"
        + f"<b>Contact:</b> @{e(ADMIN_CONTACT_USERNAME)}"
    )
    try:
        if update.callback_query:
            await safe_answer(update.callback_query)
            await safe_edit_text(update.callback_query, text)
        elif update.message:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception:
        pass


# ================================================================================
#                       FORCE SUBSCRIPTION SYSTEM (FULL)
# ================================================================================

async def get_unsubscribed_channels(
    user_id: int, bot: Bot
) -> List[Tuple[str, str, bool]]:
    """
    Return list of (username, title, jbr) for channels the user has not joined.
    For clone bots, falls back to main bot token for membership checks.
    """
    channels_info = get_all_force_sub_channels(return_usernames_only=False)
    if not channels_info:
        return []

    unsubscribed = []
    main_bot: Optional[Bot] = None

    if I_AM_CLONE:
        main_token = get_main_bot_token()
        if main_token:
            try:
                main_bot = Bot(token=main_token)
            except Exception:
                pass

    for uname, title, jbr in channels_info:
        subscribed = False
        # Try with current bot first
        for check_bot in filter(None, [bot, main_bot]):
            try:
                member = await check_bot.get_chat_member(chat_id=uname, user_id=user_id)
                if member.status not in ("left", "kicked"):
                    subscribed = True
                    break
                else:
                    break   # Got an answer — not subscribed
            except Exception as exc:
                logger.debug(f"Membership check {uname} failed: {exc}")
                continue

        if not subscribed:
            unsubscribed.append((uname, title, jbr))

    return unsubscribed


def force_sub_required(func: Callable) -> Callable:
    """
    Decorator: check force-sub, maintenance mode, and ban before
    executing any command or button handler.
    """
    @wraps(func)
    async def wrapper(
        update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs
    ):
        user = update.effective_user
        if user is None:
            return await func(update, context, *args, **kwargs)

        # Always answer callback queries immediately
        if update.callback_query:
            await safe_answer(update.callback_query)

        uid = user.id

        # Owner / Admin always bypass everything
        if uid in (ADMIN_ID, OWNER_ID):
            return await func(update, context, *args, **kwargs)

        # Ban check
        if is_user_banned(uid):
            await send_ban_screen(update, context)
            return

        # Maintenance check (only block NEW users)
        if is_maintenance_mode() and not is_existing_user(uid):
            await send_maintenance_block(update, context)
            return

        # Force-sub check
        unsubscribed = await get_unsubscribed_channels(uid, context.bot)
        if unsubscribed:
            await _send_force_sub_screen(update, context, unsubscribed, uid)
            return

        return await func(update, context, *args, **kwargs)

    return wrapper


async def _send_force_sub_screen(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    unsubscribed: List[Tuple[str, str, bool]],
    user_id: int,
) -> None:
    """Display the force-sub join screen."""
    user = update.effective_user
    total = len(get_all_force_sub_channels(return_usernames_only=False))
    unjoined = len(unsubscribed)
    user_name = e(getattr(user, "first_name", None) or getattr(user, "username", None) or "Friend")

    text = (
        f"⚠️ {b(f'Hey {user_name}! You need to join {unjoined} channel(s).')}\n\n"
        + bq(
            b("Please join ALL the channels listed below,\n")
            + b("then click the ✅ I've Joined button.")
        )
        + f"\n\n<b>Total channels: {total} | Unjoined: {unjoined}</b>"
    )

    keyboard = []
    for uname, title, jbr in unsubscribed:
        clean = uname.lstrip("@")
        if jbr:
            keyboard.append([InlineKeyboardButton(f"📝 {title} (Request)", url=f"https://t.me/{clean}")])
        else:
            keyboard.append([InlineKeyboardButton(f"📢 {title}", url=f"https://t.me/{clean}")])

    keyboard.append([bold_button("✅ I've Joined — Check Again", callback_data="verify_subscription")])
    keyboard.append([bold_button("❓ Help", callback_data="user_help")])

    markup = InlineKeyboardMarkup(keyboard)
    try:
        if update.callback_query:
            await safe_edit_text(update.callback_query, text, reply_markup=markup)
        elif update.message:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=markup)
        elif update.effective_chat:
            await safe_send_message(context.bot, update.effective_chat.id, text, reply_markup=markup)
    except Exception as exc:
        logger.debug(f"_send_force_sub_screen error: {exc}")


# ================================================================================
#                            SYSTEM STATS HELPERS
# ================================================================================

def get_uptime() -> str:
    return format_duration(int(time.time() - BOT_START_TIME))


def get_db_size() -> str:
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            return format_size(cur.fetchone()[0])
    except Exception:
        return "N/A"


def get_disk_usage() -> str:
    try:
        usage = psutil.disk_usage("/")
        return f"{format_size(usage.free)} free / {format_size(usage.total)} total"
    except Exception:
        return "N/A"


def get_cpu_usage() -> str:
    try:
        return f"{psutil.cpu_percent(interval=0.3):.1f}%"
    except Exception:
        return "N/A"


def get_memory_usage() -> str:
    try:
        m = psutil.virtual_memory()
        return f"{m.percent:.1f}% ({format_size(m.used)} / {format_size(m.total)})"
    except Exception:
        return "N/A"


def get_network_info() -> str:
    try:
        net = psutil.net_io_counters()
        return f"↑{format_size(net.bytes_sent)} ↓{format_size(net.bytes_recv)}"
    except Exception:
        return "N/A"


def get_system_stats_text() -> str:
    return (
        b("💻 System Statistics") + "\n\n"
        f"<b>⏱ Uptime:</b> {code(get_uptime())}\n"
        f"<b>🖥 CPU:</b> {code(get_cpu_usage())}\n"
        f"<b>🧠 Memory:</b> {code(get_memory_usage())}\n"
        f"<b>💾 DB Size:</b> {code(get_db_size())}\n"
        f"<b>💿 Disk:</b> {code(get_disk_usage())}\n"
        f"<b>🌐 Network:</b> {code(get_network_info())}\n"
        f"<b>🤖 Mode:</b> {code('Clone Bot' if I_AM_CLONE else 'Main Bot')}\n"
        f"<b>🏷 Username:</b> @{e(BOT_USERNAME)}"
    )


# ================================================================================
#                               ANILIST CLIENT (FULL)
# ================================================================================

class AniListClient:
    """Full AniList GraphQL API client."""
    BASE_URL = "https://graphql.anilist.co"
    SESSION: Optional[aiohttp.ClientSession] = None

    ANIME_FIELDS = """
        id siteUrl
        title { romaji english native }
        description(asHtml: false)
        coverImage { extraLarge large medium color }
        bannerImage
        format status season seasonYear
        episodes duration averageScore popularity
        genres tags { name rank isMediaSpoiler }
        studios(isMain: true) { nodes { name siteUrl } }
        startDate { year month day }
        endDate { year month day }
        nextAiringEpisode { episode airingAt timeUntilAiring }
        relations { edges { relationType(version: 2) node { id title { romaji } type format } } }
        characters(sort: ROLE, page: 1, perPage: 5) {
            nodes { name { full } image { medium } }
        }
        staff(sort: RELEVANCE, page: 1, perPage: 3) {
            nodes { name { full } primaryOccupations }
        }
        trailer { id site }
        externalLinks { url site }
        rankings { rank type context }
        streamingEpisodes { title thumbnail url site }
        isAdult
        countryOfOrigin
    """

    MANGA_FIELDS = """
        id siteUrl
        title { romaji english native }
        description(asHtml: false)
        coverImage { extraLarge large medium color }
        bannerImage
        format status
        chapters volumes averageScore popularity
        genres tags { name rank }
        startDate { year month day }
        endDate { year month day }
        relations { edges { relationType(version: 2) node { id title { romaji } type format } } }
        characters(sort: ROLE, page: 1, perPage: 5) {
            nodes { name { full } image { medium } }
        }
        staff(sort: RELEVANCE, page: 1, perPage: 3) {
            nodes { name { full } primaryOccupations }
        }
        externalLinks { url site }
        countryOfOrigin
    """

    @staticmethod
    def _normalize_query(query: str) -> str:
        """Normalize and fuzzy-correct search query.
        Removes extra spaces and common typos. AniList handles fuzzy matching server-side.
        """
        import difflib
        query = query.strip()
        # Remove duplicate spaces
        query = " ".join(query.split())
        # Common abbreviation expansions
        expansions = {
            "aot": "attack on titan",
            "bnha": "my hero academia",
            "mha": "my hero academia",
            "hxh": "hunter x hunter",
            "dbs": "dragon ball super",
            "dbz": "dragon ball z",
            "op": "one piece",
            "fma": "fullmetal alchemist",
            "snk": "attack on titan",
            "jjk": "jujutsu kaisen",
            "csm": "chainsaw man",
            "slime": "that time i got reincarnated as a slime",
            "rezero": "re zero starting life in another world",
        }
        lower = query.lower()
        if lower in expansions:
            return expansions[lower]
        return query

    @staticmethod
    def search_anime(query: str) -> Optional[Dict]:
        normalized = AniListClient._normalize_query(query)
        q = f"""
        query($s:String){{
          Media(search:$s,type:ANIME){{
            {AniListClient.ANIME_FIELDS}
          }}
        }}
        """
        result = AniListClient._query(q, {"s": normalized})
        if not result and normalized != query:
            result = AniListClient._query(q, {"s": query})
        return result

    @staticmethod
    def search_manga(query: str) -> Optional[Dict]:
        normalized = AniListClient._normalize_query(query)
        q = f"""
        query($s:String){{
          Media(search:$s,type:MANGA){{
            {AniListClient.MANGA_FIELDS}
          }}
        }}
        """
        result = AniListClient._query(q, {"s": normalized})
        if not result and normalized != query:
            result = AniListClient._query(q, {"s": query})
        return result

    @staticmethod
    def get_by_id(media_id: int, media_type: str = "ANIME") -> Optional[Dict]:
        fields = AniListClient.ANIME_FIELDS if media_type == "ANIME" else AniListClient.MANGA_FIELDS
        q = f"""
        query($id:Int){{
          Media(id:$id,type:{media_type}){{
            {fields}
          }}
        }}
        """
        return AniListClient._query(q, {"id": media_id})

    @staticmethod
    def get_trending(media_type: str = "ANIME", limit: int = 5) -> List[Dict]:
        q = f"""
        query($type:MediaType,$perPage:Int){{
          Page(perPage:$perPage){{
            media(type:$type,sort:TRENDING_DESC,isAdult:false){{
              id title{{romaji}} coverImage{{medium}} averageScore
            }}
          }}
        }}
        """
        result = AniListClient._query(q, {"type": media_type, "perPage": limit})
        if result:
            return result
        return []

    @staticmethod
    def _query_trending(q: str, variables: dict) -> Optional[List[Dict]]:
        try:
            resp = requests.post(
                AniListClient.BASE_URL,
                json={"query": q, "variables": variables},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data", {}).get("Page", {}).get("media", [])
        except Exception as exc:
            api_logger.debug(f"AniList trending query failed: {exc}")
        return []

    @staticmethod
    def _query(query_str: str, variables: dict) -> Optional[Dict]:
        cache_key = f"anilist:{hashlib.md5(json.dumps({'q': query_str, 'v': variables}, sort_keys=True).encode()).hexdigest()}"
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        try:
            resp = requests.post(
                AniListClient.BASE_URL,
                json={"query": query_str, "variables": variables},
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=12,
            )
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("data", {}).get("Media")
                if result:
                    _cache_set(cache_key, result)
                return result
            elif resp.status_code == 429:
                api_logger.warning("AniList rate limited")
                return None
            else:
                api_logger.debug(f"AniList {resp.status_code}: {resp.text[:300]}")
        except requests.Timeout:
            api_logger.debug("AniList request timed out")
        except Exception as exc:
            api_logger.debug(f"AniList request failed: {exc}")
        return None

    @staticmethod
    def format_anime_caption(data: Dict, template: Optional[str] = None) -> str:
        """Build a rich, fully-formatted anime caption from AniList data."""
        title_obj = data.get("title", {}) or {}
        title_romaji = title_obj.get("romaji", "")
        title_english = title_obj.get("english", "")
        title_native = title_obj.get("native", "")
        title_display = title_english or title_romaji or "Unknown"

        status = (data.get("status") or "").replace("_", " ").title()
        fmt = (data.get("format") or "").replace("_", " ").title()
        episodes = data.get("episodes", "?")
        duration = data.get("duration")
        score = data.get("averageScore")
        popularity = data.get("popularity", 0)
        genres = data.get("genres", []) or []
        genres_str = ", ".join(genres[:5]) if genres else "N/A"

        season = data.get("season")
        season_year = data.get("seasonYear")
        season_str = f"{season.title() if season else ''} {season_year or ''}".strip() or "N/A"

        start_date = parse_date(data.get("startDate"))
        end_date = parse_date(data.get("endDate"))
        country = data.get("countryOfOrigin", "")

        studios = data.get("studios", {}) or {}
        studio_nodes = studios.get("nodes", []) or []
        studio_name = studio_nodes[0].get("name", "N/A") if studio_nodes else "N/A"

        desc = strip_html(data.get("description") or "No description available.")
        desc = truncate(desc, 350)

        next_ep = data.get("nextAiringEpisode")
        next_ep_str = ""
        if next_ep:
            ep_num = next_ep.get("episode", "?")
            time_left = next_ep.get("timeUntilAiring", 0)
            days = time_left // 86400
            hrs = (time_left % 86400) // 3600
            next_ep_str = f"\n<b>Next Episode:</b> Ep.{ep_num} in {days}d {hrs}h"

        tags = data.get("tags", []) or []
        top_tags = [t["name"] for t in tags if not t.get("isMediaSpoiler")][:3]
        tags_str = ", ".join(top_tags) if top_tags else ""

        # Ranking
        rankings = data.get("rankings", []) or []
        rank_str = ""
        for r in rankings[:2]:
            rank_str += f"#{r.get('rank', '?')} {r.get('context', '').title()}\n"

        if template:
            for key, val in {
                "{title}": e(title_display), "{romaji}": e(title_romaji),
                "{status}": e(status), "{type}": e(fmt),
                "{episodes}": str(episodes), "{score}": str(score or "N/A"),
                "{genres}": e(genres_str), "{studio}": e(studio_name),
                "{synopsis}": e(desc), "{season}": e(season_str),
                "{popularity}": format_number(popularity),
                "{rating}": str(score or "N/A"),
            }.items():
                template = template.replace(key, val)
            return template

        # Spec-compliant format
        caption = b(e(title_display)) + "\n\n"
        caption += "━━━━━━━━━━━━━━\n"
        caption += f"➤ Status: {status}\n"
        caption += f"➤ Episodes: {str(episodes)}"
        if duration:
            caption += f" × {duration}min"
        caption += "\n"
        caption += f"➤ Rating: {str(score) + '/100' if score else 'N/A'}\n"
        caption += f"➤ Genres: {e(genres_str)}\n"
        if next_ep_str:
            caption += next_ep_str + "\n"
        caption += "\n"
        caption += bq(e(desc), expandable=True)

        site_url = data.get("siteUrl", "")
        if site_url:
            caption += f"\n\n<b>AniList:</b> {site_url}"

        return caption

    @staticmethod
    def format_manga_caption(data: Dict, template: Optional[str] = None) -> str:
        """Build a rich manga caption from AniList data."""
        title_obj = data.get("title", {}) or {}
        title_display = title_obj.get("english") or title_obj.get("romaji") or "Unknown"
        title_native = title_obj.get("native", "")
        title_romaji = title_obj.get("romaji", "")

        status = (data.get("status") or "").replace("_", " ").title()
        fmt = (data.get("format") or "").replace("_", " ").title()
        chapters = data.get("chapters", "Ongoing")
        volumes = data.get("volumes", "?")
        score = data.get("averageScore")
        popularity = data.get("popularity", 0)
        genres = data.get("genres", []) or []
        genres_str = ", ".join(genres[:5]) if genres else "N/A"

        start_date = parse_date(data.get("startDate"))
        end_date = parse_date(data.get("endDate"))
        country = data.get("countryOfOrigin", "")

        desc = strip_html(data.get("description") or "No description available.")
        desc = truncate(desc, 350)

        tags = data.get("tags", []) or []
        top_tags = [t["name"] for t in tags][:3]
        tags_str = ", ".join(top_tags) if top_tags else ""

        if template:
            for key, val in {
                "{title}": e(title_display), "{romaji}": e(title_romaji),
                "{status}": e(status), "{type}": e(fmt),
                "{chapters}": str(chapters), "{volumes}": str(volumes),
                "{score}": str(score or "N/A"), "{genres}": e(genres_str),
                "{synopsis}": e(desc),
                "{popularity}": format_number(popularity),
            }.items():
                template = template.replace(key, val)
            return template

        # Spec-compliant format
        caption = b(e(title_display)) + "\n\n"
        caption += "━━━━━━━━━━━━━━\n"
        caption += f"➤ Chapters: {str(chapters)}\n"
        caption += f"➤ Status: {status}\n"
        caption += f"➤ Source: {e(genres_str)}\n"
        caption += "\n"
        caption += bq(e(desc), expandable=True)

        site_url = data.get("siteUrl", "")
        if site_url:
            caption += f"\n\n<b>AniList:</b> {site_url}"

        return caption


# ================================================================================
#                               TMDB CLIENT (FULL)
# ================================================================================

class TMDBClient:
    """Full TMDB API client for movies and TV shows."""
    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE = "https://image.tmdb.org/t/p"

    @staticmethod
    def _get(endpoint: str, params: Dict = None) -> Optional[Dict]:
        if not TMDB_API_KEY:
            return None
        p = {"api_key": TMDB_API_KEY}
        if params:
            p.update(params)
        try:
            resp = requests.get(
                f"{TMDBClient.BASE_URL}{endpoint}", params=p, timeout=10
            )
            if resp.status_code == 200:
                return resp.json()
            api_logger.debug(f"TMDB {resp.status_code}: {endpoint}")
        except Exception as exc:
            api_logger.debug(f"TMDB error: {exc}")
        return None

    @staticmethod
    def search_movie(query: str) -> Optional[Dict]:
        data = TMDBClient._get("/search/movie", {"query": query, "language": "en-US"})
        if not data:
            return None
        results = data.get("results", [])
        if not results:
            return None
        return TMDBClient.get_movie_details(results[0]["id"])

    @staticmethod
    def search_tv(query: str) -> Optional[Dict]:
        data = TMDBClient._get("/search/tv", {"query": query, "language": "en-US"})
        if not data:
            return None
        results = data.get("results", [])
        if not results:
            return None
        return TMDBClient.get_tv_details(results[0]["id"])

    @staticmethod
    def get_movie_details(movie_id: int) -> Optional[Dict]:
        return TMDBClient._get(
            f"/movie/{movie_id}",
            {"append_to_response": "credits,keywords,release_dates,videos", "language": "en-US"},
        )

    @staticmethod
    def get_tv_details(tv_id: int) -> Optional[Dict]:
        return TMDBClient._get(
            f"/tv/{tv_id}",
            {"append_to_response": "credits,keywords,content_ratings,videos", "language": "en-US"},
        )

    @staticmethod
    def get_trending(media_type: str = "movie", time_window: str = "week") -> List[Dict]:
        data = TMDBClient._get(f"/trending/{media_type}/{time_window}")
        return (data or {}).get("results", [])[:5]

    @staticmethod
    def get_poster_url(path: str, size: str = "w500") -> str:
        if not path:
            return ""
        return f"{TMDBClient.IMAGE_BASE}/{size}{path}"

    @staticmethod
    def get_backdrop_url(path: str, size: str = "w780") -> str:
        if not path:
            return ""
        return f"{TMDBClient.IMAGE_BASE}/{size}{path}"

    @staticmethod
    def format_movie_caption(data: Dict, template: Optional[str] = None) -> str:
        """Build a rich movie caption."""
        title = e(data.get("title") or data.get("name") or "Unknown")
        original_title = e(data.get("original_title") or data.get("original_name") or "")
        tagline = e(data.get("tagline") or "")
        release = e(data.get("release_date") or "Unknown")
        runtime = data.get("runtime") or 0
        runtime_str = f"{runtime // 60}h {runtime % 60}m" if runtime else "N/A"
        rating = data.get("vote_average", 0)
        vote_count = data.get("vote_count", 0)
        popularity = data.get("popularity", 0)
        status = e(data.get("status") or "Unknown")
        language = e(data.get("original_language") or "N/A").upper()
        genres = [g["name"] for g in data.get("genres", []) or []]
        genres_str = " • ".join(genres[:5]) if genres else "N/A"
        budget = data.get("budget", 0)
        revenue = data.get("revenue", 0)
        overview = e(truncate(data.get("overview") or "No overview.", 300))

        # Cast
        credits = data.get("credits", {}) or {}
        cast = credits.get("cast", []) or []
        top_cast = ", ".join(
            e(c["name"]) for c in cast[:5]
        ) if cast else "N/A"
        crew = credits.get("crew", []) or []
        directors = [c["name"] for c in crew if c.get("job") == "Director"]
        director_str = e(", ".join(directors[:2])) if directors else "N/A"

        # Keywords
        keywords = data.get("keywords", {}) or {}
        kw_list = [k["name"] for k in (keywords.get("keywords") or [])[:5]]
        kw_str = " • ".join(kw_list) if kw_list else ""

        lines = [b(title)]
        if original_title and original_title != title:
            lines.append(f"<i>{original_title}</i>")
        if tagline:
            lines.append(f"<i>❝{tagline}❞</i>")
        lines.append("")

        lines += [
            f"<b>🎬 Released:</b> {code(release)}",
            f"<b>⏱ Runtime:</b> {code(runtime_str)}",
            f"<b>📊 Status:</b> {code(status)}",
            f"<b>⭐ Rating:</b> {code(f'{rating:.1f}/10 ({format_number(vote_count)} votes)')}",
            f"<b>🌍 Language:</b> {code(language)}",
            f"<b>🎭 Genres:</b> {e(genres_str)}",
            f"<b>🎥 Director:</b> {director_str}",
            f"<b>⭐ Cast:</b> {top_cast}",
        ]
        if budget:
            lines.append(f"<b>💰 Budget:</b> {code('$' + format_number(budget))}")
        if revenue:
            lines.append(f"<b>💵 Revenue:</b> {code('$' + format_number(revenue))}")
        if kw_str:
            lines.append(f"<b>🏷 Keywords:</b> {e(kw_str)}")
        lines.append("")
        lines.append(b("📖 Overview"))
        lines.append(bq(overview, expandable=True))

        if template:
            for key, val in {
                "{title}": title, "{release_date}": release,
                "{rating}": str(rating), "{genres}": e(genres_str),
                "{overview}": overview, "{runtime}": runtime_str,
                "{director}": director_str, "{cast}": top_cast,
                "{status}": status, "{language}": language,
            }.items():
                template = template.replace(key, val)
            return template

        return "\n".join(l for l in lines if l is not None)

    @staticmethod
    def format_tv_caption(data: Dict, template: Optional[str] = None) -> str:
        """Build a rich TV show caption."""
        name = e(data.get("name") or "Unknown")
        original_name = e(data.get("original_name") or "")
        tagline = e(data.get("tagline") or "")
        first_air = e(data.get("first_air_date") or "Unknown")
        last_air = e(data.get("last_air_date") or "Unknown")
        status = e(data.get("status") or "Unknown")
        seasons = data.get("number_of_seasons", "?")
        episodes = data.get("number_of_episodes", "?")
        rating = data.get("vote_average", 0)
        vote_count = data.get("vote_count", 0)
        popularity = data.get("popularity", 0)
        language = e(data.get("original_language") or "N/A").upper()
        genres = [g["name"] for g in data.get("genres", []) or []]
        genres_str = " • ".join(genres[:5]) if genres else "N/A"
        overview = e(truncate(data.get("overview") or "No overview.", 300))
        networks = [n["name"] for n in (data.get("networks") or [])[:3]]
        network_str = e(", ".join(networks)) if networks else "N/A"

        # Cast
        credits = data.get("credits", {}) or {}
        cast = credits.get("cast", []) or []
        top_cast = ", ".join(e(c["name"]) for c in cast[:5]) if cast else "N/A"
        creators = [c.get("name") for c in (data.get("created_by") or [])]
        creators_str = e(", ".join(creators[:2])) if creators else "N/A"

        lines = [b(name)]
        if original_name and original_name != name:
            lines.append(f"<i>{original_name}</i>")
        if tagline:
            lines.append(f"<i>❝{tagline}❞</i>")
        lines.append("")

        lines += [
            f"<b>📅 Aired:</b> {code(first_air + ' → ' + last_air)}",
            f"<b>📊 Status:</b> {code(status)}",
            f"<b>📺 Seasons:</b> {code(str(seasons))} | <b>Episodes:</b> {code(str(episodes))}",
            f"<b>⭐ Rating:</b> {code(f'{rating:.1f}/10 ({format_number(vote_count)} votes)')}",
            f"<b>🌍 Language:</b> {code(language)}",
            f"<b>🎭 Genres:</b> {e(genres_str)}",
            f"<b>📡 Network:</b> {network_str}",
            f"<b>🎬 Created by:</b> {creators_str}",
            f"<b>⭐ Cast:</b> {top_cast}",
        ]
        lines.append("")
        lines.append(b("📖 Overview"))
        lines.append(bq(overview, expandable=True))

        if template:
            for key, val in {
                "{title}": name, "{name}": name,
                "{first_air_date}": first_air, "{status}": status,
                "{seasons}": str(seasons), "{episodes}": str(episodes),
                "{rating}": str(rating), "{genres}": e(genres_str),
                "{overview}": overview, "{network}": network_str,
            }.items():
                template = template.replace(key, val)
            return template

        return "\n".join(l for l in lines if l is not None)


# ================================================================================
#                         MANGADEX CLIENT (FULL — COMPLETE)
# ================================================================================

class MangaDexClient:
    """
    Full MangaDex API client.
    Supports: search, details, chapters, pages, cover art.
    """
    BASE_URL = "https://api.mangadex.org"
    COVER_BASE = "https://uploads.mangadex.org/covers"

    @staticmethod
    def _get(endpoint: str, params: Dict = None) -> Optional[Dict]:
        try:
            resp = requests.get(
                f"{MangaDexClient.BASE_URL}{endpoint}",
                params=params or {},
                timeout=12,
            )
            if resp.status_code == 200:
                return resp.json()
            api_logger.debug(f"MangaDex {resp.status_code}: {endpoint}")
        except Exception as exc:
            api_logger.debug(f"MangaDex error: {exc}")
        return None

    @staticmethod
    def search_manga(title: str, limit: int = 10) -> List[Dict]:
        """Search manga by title, returns list of manga objects."""
        data = MangaDexClient._get("/manga", {
            "title": title,
            "limit": limit,
            "includes[]": ["cover_art", "author", "artist"],
            "availableTranslatedLanguage[]": "en",
            "order[relevance]": "desc",
        })
        if not data:
            return []
        return data.get("data", [])

    @staticmethod
    def get_manga(manga_id: str) -> Optional[Dict]:
        """Get full manga details by ID."""
        data = MangaDexClient._get(f"/manga/{manga_id}", {
            "includes[]": ["cover_art", "author", "artist"]
        })
        if data:
            return data.get("data")
        return None

    @staticmethod
    def get_chapters(
        manga_id: str,
        language: str = "en",
        limit: int = 10,
        offset: int = 0,
        order: str = "desc",
    ) -> Tuple[List[Dict], int]:
        """Get chapters for a manga. Returns (chapters, total)."""
        data = MangaDexClient._get("/chapter", {
            "manga": manga_id,
            "translatedLanguage[]": language,
            "limit": limit,
            "offset": offset,
            f"order[chapter]": order,
            "includes[]": ["scanlation_group"],
        })
        if not data:
            return [], 0
        return data.get("data", []), data.get("total", 0)

    @staticmethod
    def get_latest_chapter(manga_id: str, language: str = "en") -> Optional[Dict]:
        """Get the most recent chapter."""
        chapters, total = MangaDexClient.get_chapters(manga_id, language, limit=1)
        return chapters[0] if chapters else None

    @staticmethod
    def get_chapter_pages(chapter_id: str) -> Optional[Tuple[str, str, List[str]]]:
        """
        Get pages for a chapter.
        Returns (base_url, hash, [filenames]) or None.
        """
        data = MangaDexClient._get(f"/at-home/server/{chapter_id}")
        if not data:
            return None
        chapter_data = data.get("chapter", {})
        return (
            data.get("baseUrl", ""),
            chapter_data.get("hash", ""),
            chapter_data.get("data", []),
        )

    @staticmethod
    def get_cover_url(manga_id: str, filename: str, size: int = 256) -> str:
        """Build cover art URL."""
        return f"{MangaDexClient.COVER_BASE}/{manga_id}/{filename}.{size}.jpg"

    @staticmethod
    def extract_cover_filename(manga: Dict) -> Optional[str]:
        """Extract cover filename from manga relationships."""
        for rel in (manga.get("relationships") or []):
            if rel.get("type") == "cover_art":
                attrs = rel.get("attributes") or {}
                return attrs.get("fileName")
        return None

    @staticmethod
    def extract_authors(manga: Dict) -> str:
        """Extract author/artist names from manga relationships."""
        names = []
        for rel in (manga.get("relationships") or []):
            if rel.get("type") in ("author", "artist"):
                attrs = rel.get("attributes") or {}
                name = attrs.get("name")
                if name and name not in names:
                    names.append(e(name))
        return ", ".join(names) if names else "Unknown"

    @staticmethod
    def format_manga_info(manga: Dict) -> str:
        """Build a complete manga info message from MangaDex data."""
        attrs = manga.get("attributes", {}) or {}
        manga_id = manga.get("id", "")

        # Title
        titles = attrs.get("title", {}) or {}
        title = (
            titles.get("en") or titles.get("ja-ro") or titles.get("ja")
            or next(iter(titles.values()), "Unknown")
        )

        # Alt titles
        alt_titles_list = attrs.get("altTitles", []) or []
        alt_en = next(
            (t.get("en") for t in alt_titles_list if "en" in t), None
        )

        # Description
        desc_obj = attrs.get("description", {}) or {}
        desc = desc_obj.get("en") or next(iter(desc_obj.values()), "No description.")
        desc = truncate(strip_html(desc), 280)

        status = (attrs.get("status") or "unknown").title()
        year = attrs.get("year") or "?"
        content_rating = (attrs.get("contentRating") or "safe").title()
        lang_origin = (attrs.get("originalLanguage") or "").upper()

        # Tags
        tags = attrs.get("tags", []) or []
        tag_names = [
            t.get("attributes", {}).get("name", {}).get("en", "")
            for t in tags
            if t.get("attributes", {}).get("name", {}).get("en")
        ]

        chapters = attrs.get("lastChapter") or attrs.get("lastVolume") or "?"
        volumes = attrs.get("lastVolume") or "?"

        authors = MangaDexClient.extract_authors(manga)

        cover_fn = MangaDexClient.extract_cover_filename(manga)
        cover_url = MangaDexClient.get_cover_url(manga_id, cover_fn, 512) if cover_fn else ""

        genre_str = " • ".join(tag_names[:6]) if tag_names else "N/A"

        site_url = f"https://mangadex.org/title/{manga_id}"

        lines = [
            b(e(title)),
        ]
        if alt_en and alt_en != title:
            lines.append(f"<i>{e(alt_en)}</i>")
        lines.append("")

        lines += [
            f"<b>📊 Status:</b> {code(status)}",
            f"<b>📝 Chapters:</b> {code(str(chapters))}",
            f"<b>📚 Volumes:</b> {code(str(volumes))}",
            f"<b>📅 Year:</b> {code(str(year))}",
            f"<b>🌍 Origin:</b> {code(lang_origin or 'N/A')}",
            f"<b>🔞 Rating:</b> {code(content_rating)}",
            f"<b>✍️ Author/Artist:</b> {authors}",
            f"<b>🎭 Genres:</b> {e(genre_str)}",
            "",
            b("📖 Synopsis"),
            bq(e(desc), expandable=True),
            f"\n<b>🔗 MangaDex:</b> {site_url}",
        ]

        info_text = "\n".join(str(l) for l in lines)
        return info_text, cover_url

    @staticmethod
    def format_chapter_info(chapter: Dict) -> str:
        """Format a single chapter's info."""
        attrs = chapter.get("attributes", {}) or {}
        ch_id = chapter.get("id", "")
        ch_num = attrs.get("chapter") or "?"
        title = attrs.get("title") or ""
        pages = attrs.get("pages", 0)
        lang = (attrs.get("translatedLanguage") or "?").upper()
        pub_at = attrs.get("publishAt") or attrs.get("createdAt") or ""
        if pub_at:
            try:
                pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00")).strftime("%d %b %Y")
            except Exception:
                pass

        # Scanlation group
        groups = []
        for rel in (chapter.get("relationships") or []):
            if rel.get("type") == "scanlation_group":
                gname = (rel.get("attributes") or {}).get("name", "")
                if gname:
                    groups.append(e(gname))
        group_str = ", ".join(groups) if groups else "Unknown"

        parts = [f"<b>Chapter {ch_num}</b>"]
        if title:
            parts.append(f" — <i>{e(title)}</i>")
        lines = [" ".join(parts), ""]
        lines += [
            f"<b>📄 Pages:</b> {code(str(pages))}",
            f"<b>🌐 Language:</b> {code(lang)}",
            f"<b>👥 Group:</b> {group_str}",
            f"<b>📅 Released:</b> {code(pub_at)}",
            f"<b>🔗 Read:</b> https://mangadex.org/chapter/{ch_id}",
        ]
        return "\n".join(lines)


# ================================================================================
#                       MANGA AUTO-UPDATE TRACKER (COMPLETE)
# ================================================================================

class MangaTracker:
    """
    Tracks manga series for automatic new-chapter notifications.
    Stores tracking data in the DB: manga_id, last chapter, target chat.
    """

    @staticmethod
    def add_tracking(
        manga_id: str,
        manga_title: str,
        target_chat_id: int,
        notify_language: str = "en",
    ) -> bool:
        """Add a manga to auto-tracking."""
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    INSERT INTO manga_auto_updates
                        (manga_id, manga_title, target_chat_id, notify_language,
                         last_chapter, last_checked, active)
                    VALUES (%s, %s, %s, %s, %s, NOW(), TRUE)
                    ON CONFLICT (manga_id, target_chat_id) DO UPDATE
                        SET active = TRUE, manga_title = EXCLUDED.manga_title,
                            notify_language = EXCLUDED.notify_language
                """, (manga_id, manga_title, target_chat_id, notify_language, None))
            return True
        except Exception as exc:
            db_logger.error(f"MangaTracker.add_tracking error: {exc}")
            return False

    @staticmethod
    def remove_tracking(manga_id: str, target_chat_id: Optional[int] = None) -> bool:
        try:
            with db_manager.get_cursor() as cur:
                if target_chat_id:
                    cur.execute(
                        "UPDATE manga_auto_updates SET active = FALSE "
                        "WHERE manga_id = %s AND target_chat_id = %s",
                        (manga_id, target_chat_id),
                    )
                else:
                    cur.execute(
                        "UPDATE manga_auto_updates SET active = FALSE WHERE manga_id = %s",
                        (manga_id,),
                    )
            return True
        except Exception as exc:
            db_logger.error(f"MangaTracker.remove_tracking error: {exc}")
            return False

    @staticmethod
    def get_all_tracked() -> List[Tuple]:
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    SELECT id, manga_id, manga_title, target_chat_id,
                           notify_language, last_chapter, last_checked
                    FROM manga_auto_updates WHERE active = TRUE
                """)
                return cur.fetchall() or []
        except Exception as exc:
            db_logger.error(f"MangaTracker.get_all_tracked error: {exc}")
            return []

    @staticmethod
    def update_last_chapter(rec_id: int, chapter: str) -> None:
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    "UPDATE manga_auto_updates SET last_chapter = %s, last_checked = NOW() WHERE id = %s",
                    (chapter, rec_id),
                )
        except Exception as exc:
            db_logger.error(f"MangaTracker.update_last_chapter error: {exc}")

    @staticmethod
    def get_tracked_for_admin() -> str:
        rows = MangaTracker.get_all_tracked()
        if not rows:
            return b("No manga tracked yet.")
        lines = [b("📚 Tracked Manga:"), ""]
        for rec in rows:
            rec_id, manga_id, title, target_chat, lang, last_ch, last_checked = rec
            lines.append(
                f"• {b(e(title))}\n"
                f"  <b>Last Chapter:</b> {code(last_ch or 'None yet')}\n"
                f"  <b>Target:</b> <code>{target_chat}</code>\n"
                f"  <b>Lang:</b> {code(lang)}\n"
                f"  <b>Checked:</b> {code(str(last_checked)[:16])}\n"
                f"  <b>ID:</b> <code>{manga_id}</code>\n"
            )
        return "\n".join(lines)


# ================================================================================
#                         WATERMARK SYSTEM
# ================================================================================

async def add_watermark(
    image_url: str, text: str, position: str = "center"
) -> Optional[BytesIO]:
    """Download image and stamp watermark, return BytesIO or None."""
    if not PIL_AVAILABLE:
        return None
    try:
        resp = requests.get(image_url, timeout=12)
        resp.raise_for_status()
        img = Image.open(BytesIO(resp.content)).convert("RGBA")
        overlay = Image.new("RGBA", img.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(overlay)
        try:
            font = ImageFont.truetype("arial.ttf", 36)
        except Exception:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        pos_map = {
            "bottom": ((img.width - text_w) // 2, img.height - text_h - 15),
            "top": ((img.width - text_w) // 2, 15),
            "left": (15, (img.height - text_h) // 2),
            "right": (img.width - text_w - 15, (img.height - text_h) // 2),
            "center": ((img.width - text_w) // 2, (img.height - text_h) // 2),
            "bottom-left": (15, img.height - text_h - 15),
            "bottom-right": (img.width - text_w - 15, img.height - text_h - 15),
        }
        pos = pos_map.get(position, pos_map["center"])
        # Shadow
        draw.text((pos[0] + 2, pos[1] + 2), text, fill=(0, 0, 0, 100), font=font)
        draw.text(pos, text, fill=(255, 255, 255, 200), font=font)
        final = Image.alpha_composite(img, overlay)
        out = BytesIO()
        final = final.convert("RGB")
        final.save(out, format="JPEG", quality=90)
        out.seek(0)
        return out
    except Exception as exc:
        logger.debug(f"Watermark error: {exc}")
        return None


# ================================================================================
#                       CATEGORY SETTINGS — FULL MANAGEMENT
# ================================================================================

CATEGORY_DEFAULTS = {
    "anime": {
        "template_name": "rich_anime",
        "branding": "",
        "buttons": "[]",
        "caption_template": "",
        "thumbnail_url": "",
        "font_style": "normal",
        "logo_file_id": None,
        "logo_position": "bottom",
        "watermark_text": None,
        "watermark_position": "center",
        "include_related": True,
        "include_characters": True,
        "include_staff": False,
        "include_streaming": False,
    },
    "manga": {
        "template_name": "rich_manga",
        "branding": "",
        "buttons": "[]",
        "caption_template": "",
        "thumbnail_url": "",
        "font_style": "normal",
        "logo_file_id": None,
        "logo_position": "bottom",
        "watermark_text": None,
        "watermark_position": "center",
        "include_related": True,
        "include_characters": True,
        "include_staff": False,
        "include_streaming": False,
    },
    "movie": {
        "template_name": "rich_movie",
        "branding": "",
        "buttons": "[]",
        "caption_template": "",
        "thumbnail_url": "",
        "font_style": "normal",
        "logo_file_id": None,
        "logo_position": "bottom",
        "watermark_text": None,
        "watermark_position": "center",
        "include_related": False,
        "include_characters": False,
        "include_staff": False,
        "include_streaming": False,
    },
    "tvshow": {
        "template_name": "rich_tvshow",
        "branding": "",
        "buttons": "[]",
        "caption_template": "",
        "thumbnail_url": "",
        "font_style": "normal",
        "logo_file_id": None,
        "logo_position": "bottom",
        "watermark_text": None,
        "watermark_position": "center",
        "include_related": False,
        "include_characters": False,
        "include_staff": False,
        "include_streaming": False,
    },
}


def get_category_settings(category: str) -> Dict:
    """Fetch or initialize category settings from DB."""
    defaults = CATEGORY_DEFAULTS.get(category, CATEGORY_DEFAULTS["anime"])
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                SELECT template_name, branding, buttons, caption_template,
                       thumbnail_url, font_style, logo_file_id, logo_position,
                       watermark_text, watermark_position
                FROM category_settings WHERE category = %s
            """, (category,))
            row = cur.fetchone()
        if row:
            return {
                "template_name": row[0] or defaults["template_name"],
                "branding": row[1] or "",
                "buttons": json.loads(row[2]) if row[2] and row[2] != "[]" else [],
                "caption_template": row[3] or "",
                "thumbnail_url": row[4] or "",
                "font_style": row[5] or "normal",
                "logo_file_id": row[6],
                "logo_position": row[7] or "bottom",
                "watermark_text": row[8],
                "watermark_position": row[9] or "center",
            }
    except Exception as exc:
        db_logger.debug(f"get_category_settings error: {exc}")

    # Insert defaults
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO category_settings
                    (category, template_name, branding, buttons, caption_template,
                     thumbnail_url, font_style, watermark_position)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (category) DO NOTHING
            """, (
                category, defaults["template_name"], "", "[]", "",
                "", "normal", "center",
            ))
    except Exception:
        pass

    return {
        "template_name": defaults["template_name"], "branding": "", "buttons": [],
        "caption_template": "", "thumbnail_url": "", "font_style": "normal",
        "logo_file_id": None, "logo_position": "bottom",
        "watermark_text": None, "watermark_position": "center",
    }


def update_category_field(category: str, field: str, value: Any) -> bool:
    """Update a single field in category_settings."""
    try:
        with db_manager.get_cursor() as cur:
            cur.execute(
                f"UPDATE category_settings SET {field} = %s WHERE category = %s",
                (value, category),
            )
        return True
    except Exception as exc:
        db_logger.error(f"update_category_field {field}: {exc}")
        return False


def build_buttons_from_settings(settings: Dict) -> Optional[InlineKeyboardMarkup]:
    """Convert settings buttons list to InlineKeyboardMarkup."""
    btns = settings.get("buttons", [])
    if not btns:
        return None
    keyboard = []
    row = []
    for i, btn in enumerate(btns):
        label = btn.get("text", "Link")
        url = btn.get("url", "")
        if not url:
            continue
        # Color prefix handling
        for pfx, icon in [("#g ", "🟢 "), ("#r ", "🔴 "), ("#b ", "🔵 "), ("#p ", "🟣 "), ("#y ", "🟡 ")]:
            if label.startswith(pfx):
                label = icon + label[len(pfx):]
                break
        row.append(InlineKeyboardButton(label, url=url))
        if len(row) == 2 or btn.get("newline"):
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard) if keyboard else None


# ================================================================================
#                         POST GENERATION ENGINE (COMPLETE)
# ================================================================================

async def generate_and_send_post(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    category: str,
    search_query: str = "",
    media_id: Optional[int] = None,
    source_manga_id: Optional[str] = None,
    preferred_size: str = "extraLarge",
) -> bool:
    """
    Full post generation for anime, manga, movie, tvshow.
    Returns True on success.
    preferred_size: 'extraLarge' | 'large' | 'medium' | 'bannerImage'
    """
    settings = get_category_settings(category)
    data: Optional[Dict] = None
    poster_url: Optional[str] = None
    caption_text: str = ""
    buttons_markup: Optional[InlineKeyboardMarkup] = None

    # ── Fetch data ────────────────────────────────────────────────────────────────
    try:
        if category == "anime":
            data = (
                AniListClient.get_by_id(media_id, "ANIME") if media_id
                else AniListClient.search_anime(search_query)
            )
            if not data:
                await safe_send_message(
                    context.bot, chat_id,
                    b("❌ No anime found for: ") + code(e(search_query or str(media_id)))
                )
                return False
            # Caption
            tmpl = settings.get("caption_template", "")
            caption_text = AniListClient.format_anime_caption(data, tmpl if tmpl else None)
            # Branding
            branding = settings.get("branding", "")
            if branding:
                caption_text += f"\n\n{branding}"
            # Poster — honour preferred_size, fall back through sizes
            cover = (data.get("coverImage") or {})
            if preferred_size == "bannerImage":
                poster_url = data.get("bannerImage") or cover.get("extraLarge") or cover.get("large") or cover.get("medium")
            else:
                size_order = ["extraLarge", "large", "medium"] if preferred_size != "medium" else ["medium", "large", "extraLarge"]
                if preferred_size == "large":
                    size_order = ["large", "extraLarge", "medium"]
                poster_url = next((cover.get(s) for s in size_order if cover.get(s)), None)

        elif category == "manga":
            if source_manga_id:
                # MangaDex direct
                manga = MangaDexClient.get_manga(source_manga_id)
                if manga:
                    caption_text, poster_url = MangaDexClient.format_manga_info(manga)
                    # Override with AniList if found
                    anilist_data = AniListClient.search_manga(search_query or "")
                    if anilist_data:
                        tmpl = settings.get("caption_template", "")
                        caption_text = AniListClient.format_manga_caption(anilist_data, tmpl if tmpl else None)
                        cover = (anilist_data.get("coverImage") or {})
                        poster_url = cover.get("extraLarge") or cover.get("large") or poster_url
                else:
                    await safe_send_message(context.bot, chat_id, b("❌ Manga not found on MangaDex."))
                    return False
            else:
                data = (
                    AniListClient.get_by_id(media_id, "MANGA") if media_id
                    else AniListClient.search_manga(search_query)
                )
                if not data:
                    # Try MangaDex
                    md_results = MangaDexClient.search_manga(search_query)
                    if md_results:
                        manga = md_results[0]
                        caption_text, poster_url = MangaDexClient.format_manga_info(manga)
                    else:
                        await safe_send_message(
                            context.bot, chat_id,
                            b("❌ No manga found for: ") + code(e(search_query or ""))
                        )
                        return False
                else:
                    tmpl = settings.get("caption_template", "")
                    caption_text = AniListClient.format_manga_caption(data, tmpl if tmpl else None)
                    cover = (data.get("coverImage") or {})
                    if preferred_size == "bannerImage":
                        poster_url = data.get("bannerImage") or cover.get("extraLarge") or cover.get("large") or cover.get("medium")
                    else:
                        size_order = ["extraLarge", "large", "medium"] if preferred_size != "medium" else ["medium", "large", "extraLarge"]
                        if preferred_size == "large":
                            size_order = ["large", "extraLarge", "medium"]
                        poster_url = next((cover.get(s) for s in size_order if cover.get(s)), None)
            branding = settings.get("branding", "")
            if branding:
                caption_text += f"\n\n{branding}"

        elif category == "movie":
            data = TMDBClient.search_movie(search_query) if not media_id else TMDBClient.get_movie_details(media_id)
            if not data:
                await safe_send_message(
                    context.bot, chat_id,
                    b("❌ No movie found. Make sure TMDB_API_KEY is configured.") if not TMDB_API_KEY
                    else b("❌ No movie found for: ") + code(e(search_query or ""))
                )
                return False
            tmpl = settings.get("caption_template", "")
            caption_text = TMDBClient.format_movie_caption(data, tmpl if tmpl else None)
            branding = settings.get("branding", "")
            if branding:
                caption_text += f"\n\n{branding}"
            poster_path = data.get("poster_path")
            if poster_path:
                poster_url = TMDBClient.get_poster_url(poster_path)

        elif category == "tvshow":
            data = TMDBClient.search_tv(search_query) if not media_id else TMDBClient.get_tv_details(media_id)
            if not data:
                await safe_send_message(
                    context.bot, chat_id,
                    b("❌ No TV show found. Make sure TMDB_API_KEY is configured.") if not TMDB_API_KEY
                    else b("❌ No TV show found for: ") + code(e(search_query or ""))
                )
                return False
            tmpl = settings.get("caption_template", "")
            caption_text = TMDBClient.format_tv_caption(data, tmpl if tmpl else None)
            branding = settings.get("branding", "")
            if branding:
                caption_text += f"\n\n{branding}"
            poster_path = data.get("poster_path")
            if poster_path:
                poster_url = TMDBClient.get_poster_url(poster_path)

    except Exception as exc:
        logger.error(f"generate_and_send_post fetch error: {exc}")
        await safe_send_message(
            context.bot, chat_id,
            b("❌ Failed to fetch data. Please try again.")
        )
        return False

    # ── Font style ────────────────────────────────────────────────────────────────
    if settings.get("font_style") == "smallcaps":
        caption_text = small_caps(caption_text)

    # ── Truncate if too long ──────────────────────────────────────────────────────
    if len(caption_text) > 4000:
        caption_text = caption_text[:3980] + "\n<b>…(truncated)</b>"

    # ── Buttons ───────────────────────────────────────────────────────────────────
    buttons_markup = build_buttons_from_settings(settings)

    # ── Add "Join Now" button per spec (no emoji, clean) ─────────────────────────
    if buttons_markup:
        existing_rows = list(buttons_markup.inline_keyboard)
    else:
        existing_rows = []
    # Collect alternate image URLs for navigation (cover sizes)
    _alt_images: List[str] = []
    if data and isinstance(data, dict):
        cov = data.get("coverImage") or {}
        for sz in ("extraLarge", "large", "medium"):
            url_ = cov.get(sz)
            if url_ and url_ not in _alt_images:
                _alt_images.append(url_)
        banner = data.get("bannerImage")
        if banner and banner not in _alt_images:
            _alt_images.append(banner)
    if poster_url and poster_url not in _alt_images:
        _alt_images.insert(0, poster_url)

    # Navigation row if multiple images available
    nav_row: List[InlineKeyboardButton] = []
    if len(_alt_images) > 1:
        img_key = f"imgset_{category}_{search_query or str(media_id)}"
        # Store urls + caption so navigation can restore info text
        _cache_set(img_key, {"urls": _alt_images, "caption": caption_text, "shown": set()})
        nav_row = [
            InlineKeyboardButton("◀", callback_data=f"imgn:0:{img_key}:prev"),
            InlineKeyboardButton("✕", callback_data="close_message"),
            InlineKeyboardButton("▶", callback_data=f"imgn:0:{img_key}:next"),
        ]
    else:
        nav_row = [InlineKeyboardButton("✕", callback_data="close_message")]

    # Join Now button (always present, no emoji per spec)
    join_btn = InlineKeyboardButton("Join Now", url=PUBLIC_ANIME_CHANNEL_URL)
    nav_keyboard = existing_rows + [[join_btn], nav_row]
    buttons_markup = InlineKeyboardMarkup(nav_keyboard)

    # ── Watermark ─────────────────────────────────────────────────────────────────
    wm_text = settings.get("watermark_text")
    wm_pos = settings.get("watermark_position", "center")
    if poster_url and wm_text:
        try:
            wm_image = await add_watermark(poster_url, wm_text, wm_pos)
            if wm_image:
                await context.bot.send_photo(
                    chat_id, wm_image, caption=caption_text,
                    parse_mode=ParseMode.HTML, reply_markup=buttons_markup,
                )
                _cache_post(category, search_query or str(media_id), data)
                return True
        except Exception as exc:
            logger.debug(f"Watermark send failed: {exc}")

    # ── Send ──────────────────────────────────────────────────────────────────────
    if poster_url:
        sent = await safe_send_photo(
            context.bot, chat_id, poster_url,
            caption=caption_text, reply_markup=buttons_markup,
        )
        if not sent:
            await safe_send_message(
                context.bot, chat_id, caption_text,
                reply_markup=buttons_markup,
            )
    else:
        await safe_send_message(
            context.bot, chat_id, caption_text,
            reply_markup=buttons_markup,
        )

    _cache_post(category, search_query or str(media_id), data)
    return True


def _cache_post(category: str, key: str, data: Optional[Dict]) -> None:
    """Cache post data for history."""
    if not data:
        return
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO posts_cache (category, title, anilist_id, media_data, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT DO NOTHING
            """, (
                category, key[:200],
                data.get("id") if isinstance(data, dict) else None,
                json.dumps(data)[:5000] if data else None,
            ))
    except Exception:
        pass


# ================================================================================
#                             NAVIGATION / BACK BUTTONS
# ================================================================================

def _back_kb(data: str = "admin_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[bold_button("🔙 BACK", callback_data=data)]])


def _back_close_kb(back_data: str = "admin_back") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [bold_button("🔙 BACK", callback_data=back_data),
         bold_button("❌ CLOSE", callback_data="close_message")]
    ])


def _build_pagination_kb(
    current_page: int,
    total_pages: int,
    base_callback: str,
    extra_buttons: Optional[List[List[InlineKeyboardButton]]] = None,
) -> InlineKeyboardMarkup:
    """Build a pagination keyboard row."""
    nav = []
    if current_page > 0:
        nav.append(bold_button("◀ Prev", callback_data=f"{base_callback}_{current_page - 1}"))
    if total_pages > 1:
        nav.append(bold_button(f"{current_page + 1}/{total_pages}", callback_data="noop"))
    if current_page < total_pages - 1:
        nav.append(bold_button("Next ▶", callback_data=f"{base_callback}_{current_page + 1}"))
    keyboard = []
    if extra_buttons:
        keyboard.extend(extra_buttons)
    if nav:
        keyboard.append(nav)
    return InlineKeyboardMarkup(keyboard)


# ================================================================================
#                          ADMIN PANEL — COMPLETE MENUS
# ================================================================================

async def send_admin_menu(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    query: Optional[CallbackQuery] = None,
) -> None:
    """Send/refresh the main admin control panel."""
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass

    await delete_bot_prompt(context, chat_id)
    user_states.pop(chat_id, None)

    maint = get_setting("maintenance_mode", "false")
    maint_label = "🔴 Maintenance: ON" if maint == "true" else "🟢 Maintenance: OFF"
    clone_redirect = get_setting("clone_redirect_enabled", "false")
    clone_label = "🔀 Clone Redirect: ON" if clone_redirect == "true" else "🔀 Clone Redirect: OFF"

    keyboard = [
        [bold_button("STATS", callback_data="admin_stats"),
         bold_button("SYSTEM", callback_data="admin_sysstats")],
        [bold_button("FORCE SUB", callback_data="manage_force_sub"),
         bold_button("LINKS", callback_data="generate_links")],
        [bold_button("BROADCAST", callback_data="admin_broadcast_start"),
         bold_button("USERS", callback_data="user_management")],
        [bold_button("CLONES", callback_data="manage_clones"),
         bold_button("SETTINGS", callback_data="admin_settings")],
        [bold_button("AUTO FORWARD", callback_data="admin_autoforward"),
         bold_button("MANGA", callback_data="admin_autoupdate")],
        [bold_button("FLAGS", callback_data="admin_feature_flags"),
         bold_button("FILTERS", callback_data="admin_filter_settings")],
        [bold_button("CATEGORIES", callback_data="admin_category_settings"),
         bold_button("UPLOAD", callback_data="upload_menu")],
        [bold_button("COMMANDS", callback_data="admin_cmd_list"),
         bold_button("LOGS", callback_data="admin_logs")],
        [bold_button("RESTART", callback_data="admin_restart_confirm")],
    ]
    text = (
        b("🛠 Admin Control Panel") + "\n\n"
        f"{b(maint_label)}\n"
        f"{b(clone_label)}\n\n"
        + bq(f"<b>Bot:</b> @{e(BOT_USERNAME)}\n<b>Mode:</b> {'Clone' if I_AM_CLONE else 'Main'}")
    )

    if ADMIN_PANEL_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id, ADMIN_PANEL_IMAGE_URL,
                caption=text, parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return
        except Exception:
            pass
    await safe_send_message(
        context.bot, chat_id, text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def send_stats_panel(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    query: Optional[CallbackQuery] = None,
) -> None:
    """Send bot statistics panel."""
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass

    try:
        user_count = get_user_count()
        channel_count = len(get_all_force_sub_channels())
        link_count = get_links_count()
        clones = get_all_clone_bots(active_only=True)
        blocked = get_blocked_users_count()
        maint = "🔴 ON" if get_setting("maintenance_mode", "false") == "true" else "🟢 OFF"

        text = (
            b("📊 Bot Statistics") + "\n\n"
            f"<b>👥 Total Users:</b> {code(format_number(user_count))}\n"
            f"<b>📢 Force-Sub Channels:</b> {code(str(channel_count))}\n"
            f"<b>🔗 Generated Links:</b> {code(format_number(link_count))}\n"
            f"<b>🤖 Active Clone Bots:</b> {code(str(len(clones)))}\n"
            f"<b>🚫 Blocked Users:</b> {code(str(blocked))}\n"
            f"<b>🔧 Maintenance:</b> {maint}\n"
            f"<b>⏱ Link Expiry:</b> {code(str(LINK_EXPIRY_MINUTES) + ' min')}\n"
            f"<b>⏳ Uptime:</b> {code(get_uptime())}"
        )
    except Exception as exc:
        text = b("❌ Error loading stats: ") + code(e(str(exc)[:200]))

    keyboard = [
        [bold_button("♻️ Refresh", callback_data="admin_stats"),
         bold_button("📈 Broadcast Stats", callback_data="broadcast_stats_panel")],
        [bold_button("🔙 BACK", callback_data="admin_back")],
    ]

    if STATS_IMAGE_URL:
        try:
            await safe_send_photo(
                context.bot, chat_id, STATS_IMAGE_URL,
                caption=text, reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return
        except Exception:
            pass
    await safe_send_message(
        context.bot, chat_id, text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def show_category_settings_menu(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    category: str,
    query: Optional[CallbackQuery] = None,
) -> None:
    """Show full settings menu for a category."""
    settings = get_category_settings(category)
    icon = {"anime": "🎌", "manga": "📚", "movie": "🎬", "tvshow": "📺"}.get(category, "⚙️")
    btns_count = len(settings.get("buttons") or [])
    wm = settings.get("watermark_text") or "None"
    logo = "✅ Set" if settings.get("logo_file_id") else "❌ Not set"

    text = (
        f"{icon} {b(category.upper() + ' Category Settings')}\n\n"
        f"<b>📋 Template:</b> {code(settings['template_name'])}\n"
        f"<b>🔤 Font:</b> {code(settings['font_style'])}\n"
        f"<b>🔘 Buttons:</b> {code(str(btns_count) + ' configured')}\n"
        f"<b>💧 Watermark:</b> {code(e(wm[:30]))}\n"
        f"<b>🖼 Logo:</b> {logo}\n"
        f"<b>📝 Custom Caption:</b> {'✅' if settings.get('caption_template') else '❌ Using default'}\n"
        f"<b>🏷 Branding:</b> {'✅' if settings.get('branding') else '❌ None'}"
    )
    # Spec-compliant SETTINGS PANEL layout
    keyboard = [
        [bold_button("CAPTION", callback_data=f"cat_caption_{category}"),
         bold_button("BUTTON", callback_data=f"cat_buttons_{category}")],
        [bold_button("TEMPLATE", callback_data=f"cat_thumbnail_{category}"),
         bold_button("BRANDING", callback_data=f"cat_branding_{category}")],
        [bold_button("FONT STYLE", callback_data=f"cat_font_{category}")],
        [bold_button("AUTO UPDATE", callback_data="admin_autoupdate")],
        [bold_button("🔙 BACK", callback_data="admin_category_settings")],
    ]
    markup = InlineKeyboardMarkup(keyboard)
    if query:
        await safe_edit_text(query, text, reply_markup=markup)
    else:
        await safe_send_message(context.bot, chat_id, text, reply_markup=markup)


async def send_feature_flags_panel(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    query: Optional[CallbackQuery] = None,
) -> None:
    """Show feature flags panel."""
    flags = [
        ("maintenance_mode", "false", "🔧 Maintenance Mode"),
        ("clone_redirect_enabled", "false", "🔀 Clone Redirect"),
        ("error_dms_enabled", "1", "⚠️ Error DMs to Admin"),
        ("force_sub_enabled", "true", "📢 Force Subscription"),
        ("auto_delete_messages", "true", "🗑 Auto-Delete Messages"),
        ("watermarks_enabled", "true", "💧 Watermarks"),
        ("inline_search_enabled", "true", "🔍 Inline Search"),
        ("group_commands_enabled", "true", "👥 Group Commands"),
    ]

    text = b("🚩 Feature Flags") + "\n\n"
    keyboard = []
    for key, default, label in flags:
        val = get_setting(key, default)
        is_on = val in ("1", "true", "yes")
        status = "✅ ON" if is_on else "❌ OFF"
        text += f"<b>{label}:</b> {status}\n"
        toggle_val = "false" if is_on else "true"
        keyboard.append([bold_button(
            f"{'Disable' if is_on else 'Enable'} {label.split(' ', 1)[-1]}",
            callback_data=f"flag_toggle_{key}_{toggle_val}"
        )])

    keyboard.append([bold_button("🔙 BACK", callback_data="admin_back")])

    markup = InlineKeyboardMarkup(keyboard)
    if query:
        await safe_edit_text(query, text, reply_markup=markup)
    else:
        await safe_send_message(context.bot, chat_id, text, reply_markup=markup)


# ================================================================================
#                           START COMMAND (SAFE + FULL)
# ================================================================================

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Main /start handler. Handles:
    - Regular users: welcome screen
    - Admin: admin panel
    - Deep links: channel link delivery
    - Clone redirect
    - Safety anchor to prevent mobile exit-on-delete
    """
    user = update.effective_user
    chat_id = update.effective_chat.id
    uid = user.id if user else 0

    # Register user in DB
    if user:
        add_user(uid, user.username, user.first_name, user.last_name)

    # Clean previous prompt
    await delete_bot_prompt(context, chat_id)

    # Send sticker FIRST (before animation)
    await send_transition_sticker(context, chat_id)

    # Bold loading animation with ❗ (safety anchor)
    loading_msg = await loading_animation_start(context, chat_id)

    # ── Deep link handling ────────────────────────────────────────────────────────
    if context.args:
        link_id = context.args[0]

        # Clone redirect for non-admin users
        clone_redirect = get_setting("clone_redirect_enabled", "false").lower() == "true"
        if clone_redirect and not I_AM_CLONE and uid not in (ADMIN_ID, OWNER_ID):
            clones = get_all_clone_bots(active_only=True)
            if clones:
                clone_uname = clones[0][2]
                await loading_animation_end(context, chat_id, loading_msg)
                await safe_send_message(
                    context.bot, chat_id,
                    b("🔄 Getting your link via our server bot…"),
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(
                            "📥 Get Your Link",
                            url=f"https://t.me/{clone_uname}?start={link_id}"
                        )
                    ]]),
                )
                return

        await loading_animation_end(context, chat_id, loading_msg)
        await handle_deep_link(update, context, link_id)
        return

    await loading_animation_end(context, chat_id, loading_msg)

    # ── Admin panel ───────────────────────────────────────────────────────────────
    if uid in (ADMIN_ID, OWNER_ID):
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Regular user welcome ──────────────────────────────────────────────────────
    keyboard = [
          [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
          [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
          [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
          [InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
           InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_message")],
      ]
    markup = InlineKeyboardMarkup(keyboard)

    # Try to copy welcome message from source channel
    try:
        await context.bot.copy_message(
            chat_id=chat_id,
            from_chat_id=WELCOME_SOURCE_CHANNEL,
            message_id=WELCOME_SOURCE_MESSAGE_ID,
            reply_markup=markup,
        )
        return
    except Exception:
        pass

    # Fallback welcome
    if WELCOME_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id,
                WELCOME_IMAGE_URL,
                caption=(
                    b(f"✨ Welcome to {e(BOT_NAME)}!") + "\n\n"
                    + bq(b("Your gateway to all things Anime, Manga & Movies!"))
                ),
                parse_mode=ParseMode.HTML,
                reply_markup=markup,
            )
            return
        except Exception:
            pass

    await safe_send_message(
        context.bot, chat_id,
        b(f"✨ Welcome to {e(BOT_NAME)}!") + "\n\n"
        + bq(b("Your gateway to all things Anime, Manga & Movies!")),
        reply_markup=markup,
    )


# ================================================================================
#                         DEEP LINK HANDLER (COMPLETE)
# ================================================================================

async def handle_deep_link(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    link_id: str,
) -> None:
    """Handle deep link /start?start=<link_id>."""
    chat_id = update.effective_chat.id

    link_info = get_link_info(link_id)
    if not link_info:
        await safe_send_message(
            context.bot, chat_id,
            b("❌ Invalid Link") + "\n\n"
            + bq(b("This link is invalid or has been removed. "
                   "Please tap the original post button again.")),
        )
        return

    channel_identifier, creator_id, created_time, never_expires = link_info

    # Expiry check
    if not never_expires:
        try:
            created_dt = datetime.fromisoformat(str(created_time))
            if now_utc() > created_dt + timedelta(minutes=LINK_EXPIRY_MINUTES):
                await safe_send_message(
                    context.bot, chat_id,
                    b("⏰ Link Expired") + "\n\n"
                    + bq(
                        b("This invite link has expired.\n\n")
                        + b("💡 Tip: Tap the post button again to get a fresh link.")
                    ),
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("📢 Anime Channel", url=PUBLIC_ANIME_CHANNEL_URL)
                    ]]),
                )
                return
        except Exception:
            pass

    # Determine which bot creates the invite link
    invite_bot = context.bot
    if I_AM_CLONE:
        main_token = get_main_bot_token()
        if main_token:
            try:
                invite_bot = Bot(token=main_token)
            except Exception:
                pass

    try:
        if isinstance(channel_identifier, str) and channel_identifier.lstrip("-").isdigit():
            channel_identifier = int(channel_identifier)

        chat = await invite_bot.get_chat(channel_identifier)
        expire_ts = int(
            (now_utc() + timedelta(minutes=LINK_EXPIRY_MINUTES + 1)).timestamp()
        )
        invite = await invite_bot.create_chat_invite_link(
            chat.id,
            expire_date=expire_ts,
            member_limit=1,
            name=f"DeepLink {link_id[:8]}",
        )

        keyboard = [[bold_button("• Join Channel •", url=invite.invite_link)]]
        await context.bot.send_message(
            chat_id,
            small_caps(
                "<blockquote><b>ʜᴇʀᴇ ɪs ʏᴏᴜʀ ʟɪɴᴋ! ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ᴘʀᴏᴄᴇᴇᴅ</b>\n\n</blockquote>"
                "<b><u>Note: If the link is expired, please click the post link again to get a new one.</u></b>"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Forbidden as exc:
        await safe_send_message(
            context.bot, chat_id,
            b("🚫 Bot Access Error") + "\n\n"
            + bq(b("The bot has been removed from that channel. "
                   "Please contact admin.")),
        )
        logger.error(f"handle_deep_link Forbidden error: {exc}")
    except Exception as exc:
        logger.error(f"handle_deep_link error: {exc}")
        await safe_send_message(
            context.bot, chat_id,
            UserFriendlyError.get_user_message(exc),
        )


# ================================================================================
#                             HELP COMMAND (FULL)
# ================================================================================

@force_sub_required
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Display the help screen (admin only per spec)."""
    uid = update.effective_user.id if update.effective_user else 0
    await delete_update_message(update, context)
    # Admin-only per spec (#9)
    if uid not in (ADMIN_ID, OWNER_ID):
        return
    user_states.pop(uid, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    is_admin = uid in (ADMIN_ID, OWNER_ID)

    text = (
        b("📖 How to Use This Bot") + "\n\n"
        + bq(
            b("🎌 Create Posts:\n")
            + "<b>/anime [name]</b> — Generate anime post\n"
            + "<b>/manga [name]</b> — Generate manga post\n"
            + "<b>/movie [name]</b> — Generate movie post\n"
            + "<b>/tvshow [name]</b> — Generate TV show post\n\n"
            + b("🔍 Search & Info:\n")
            + "<b>/search [name]</b> — Search anime/manga\n"
            + "<b>/id</b> — Get chat/user IDs\n"
            + "<b>/info</b> — Get user details\n\n"
            + b("⚙️ Utility:\n")
            + "<b>/start</b> — Main menu\n"
            + "<b>/help</b> — This guide\n"
            + "<b>/ping</b> — Check response time\n"
            + "<b>/alive</b> — Check if bot is online",
            expandable=True,
        )
    )
    if is_admin:
        text += "\n\n" + bq(
            b("👑 Admin Quick Commands:\n")
            + "<b>/stats</b> — Bot statistics\n"
            + "<b>/broadcast</b> — Send message to all users\n"
            + "<b>/addchannel</b> @ch Title — Force-sub channel\n"
            + "<b>/addclone</b> TOKEN — Register clone bot\n"
            + "<b>/upload</b> — Upload manager\n"
            + "<b>/cmd</b> — Full admin command list"
        )

    keyboard = [
        [InlineKeyboardButton("🎌 Anime Channel", url=PUBLIC_ANIME_CHANNEL_URL)],
        [InlineKeyboardButton("💬 Contact Admin", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
        [bold_button("❌ Close", callback_data="close_message")],
    ]
    markup = InlineKeyboardMarkup(keyboard)

    if HELP_IMAGE_URL:
        sent = await safe_send_photo(
            context.bot, update.effective_chat.id,
            HELP_IMAGE_URL, caption=text, reply_markup=markup,
        )
        if sent:
            return

    await safe_reply(update, text, reply_markup=markup)


# ================================================================================
#                             PING COMMAND
# ================================================================================

@force_sub_required
async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    t0 = time.monotonic()
    chat_id = update.effective_chat.id
    try:
        msg = await safe_reply(update, b("🏓 Pinging…"))
        if msg:
            elapsed_ms = (time.monotonic() - t0) * 1000
            await msg.edit_text(
                b("🏓 Pong!") + "\n\n"
                f"<b>Response Time:</b> {code(f'{elapsed_ms:.0f}ms')}\n"
                f"<b>Status:</b> {code('Online ✅')}",
                parse_mode=ParseMode.HTML,
            )
    except Exception:
        pass


# ================================================================================
#                            ALIVE COMMAND
# ================================================================================

@force_sub_required
async def alive_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        b("✅ Bot is Alive!") + "\n\n"
        f"<b>⏱ Uptime:</b> {code(get_uptime())}\n"
        f"<b>🤖 Username:</b> @{e(BOT_USERNAME)}\n"
        f"<b>🏷 Mode:</b> {code('Clone Bot' if I_AM_CLONE else 'Main Bot')}"
    )
    await safe_reply(update, text)


# ================================================================================
#                           SEARCH COMMAND (FULL RESULTS)
# ================================================================================

@force_sub_required
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id if update.effective_user else 0
    await delete_update_message(update, context)

    if not context.args:
        await safe_reply(
            update,
            b("Usage: /search [name]") + "\n"
            + bq(b("Example: /search Naruto"))
        )
        return

    query_text = " ".join(context.args)
    chat_id = update.effective_chat.id

    searching_msg = await safe_send_message(
        context.bot, chat_id,
        b(f"🔍 Searching for: {e(query_text)}…"),
    )

    results = []
    anime = AniListClient.search_anime(query_text)
    if anime:
        title_obj = anime.get("title", {}) or {}
        title = title_obj.get("romaji") or title_obj.get("english") or "Unknown"
        results.append(("anime", anime["id"], f"🎌 {title}", "anime"))

    manga = AniListClient.search_manga(query_text)
    if manga:
        title_obj = manga.get("title", {}) or {}
        title = title_obj.get("romaji") or title_obj.get("english") or "Unknown"
        results.append(("manga", manga["id"], f"📚 {title}", "manga"))

    if TMDB_API_KEY:
        movie = TMDBClient.search_movie(query_text)
        if movie:
            title = movie.get("title") or "Unknown"
            results.append(("movie", movie.get("id", 0), f"🎬 {title}", "movie"))
        tv = TMDBClient.search_tv(query_text)
        if tv:
            name = tv.get("name") or "Unknown"
            results.append(("tvshow", tv.get("id", 0), f"📺 {name}", "tvshow"))

    # MangaDex results
    md_results = MangaDexClient.search_manga(query_text, limit=3)
    for md in md_results[:2]:
        attrs = md.get("attributes", {}) or {}
        titles = attrs.get("title", {}) or {}
        title = titles.get("en") or next(iter(titles.values()), "Unknown")
        results.append(("mangadex", md["id"], f"📖 {title} (MangaDex)", "mangadex"))

    if searching_msg:
        await safe_delete(context.bot, chat_id, searching_msg.message_id)

    if not results:
        await safe_send_message(
            context.bot, chat_id,
            b("❌ No results found.") + "\n"
            + bq(b("Try a different search term."))
        )
        return

    keyboard = []
    for media_type, media_id, label, cb_type in results:
        keyboard.append([bold_button(
            label[:40],
            callback_data=f"search_result_{cb_type}_{media_id}"
        )])

    await safe_send_message(
        context.bot, chat_id,
        b(f"🔍 Search results for: {e(query_text)}"),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ================================================================================
#                         CATEGORY POST COMMANDS
# ================================================================================

@force_sub_required
async def anime_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _passes_filter(update, "anime"):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /anime [name]") + "\n" + bq("<b>Example:</b> /anime Naruto"))
        return
    query_text = " ".join(context.args)
    await generate_and_send_post(context, update.effective_chat.id, "anime", query_text)


@force_sub_required
async def manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _passes_filter(update, "manga"):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /manga [name]") + "\n" + bq("<b>Example:</b> /manga One Piece"))
        return
    query_text = " ".join(context.args)
    await generate_and_send_post(context, update.effective_chat.id, "manga", query_text)


@force_sub_required
async def movie_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _passes_filter(update, "movie"):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /movie [name]") + "\n" + bq("<b>Example:</b> /movie Avengers"))
        return
    if not TMDB_API_KEY:
        await safe_reply(update, b("⚠️ TMDB API key not configured."))
        return
    query_text = " ".join(context.args)
    await generate_and_send_post(context, update.effective_chat.id, "movie", query_text)


@force_sub_required
async def tvshow_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _passes_filter(update, "tvshow"):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /tvshow [name]") + "\n" + bq("<b>Example:</b> /tvshow Breaking Bad"))
        return
    if not TMDB_API_KEY:
        await safe_reply(update, b("⚠️ TMDB API key not configured."))
        return
    query_text = " ".join(context.args)
    await generate_and_send_post(context, update.effective_chat.id, "tvshow", query_text)


# ================================================================================
#                           ADMIN COMMANDS (ALL)
# ================================================================================

@force_sub_required
async def cmd_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)

    text = (
        b("📋 Admin Command Reference") + "\n\n"
        + bq(
            b("📊 Statistics & Info:\n")
            + "<b>/stats</b> — Bot stats\n"
            + "<b>/sysstats</b> — Server info\n"
            + "<b>/users</b> — User count\n"
            + "<b>/alive</b> — Online check\n"
            + "<b>/ping</b> — Response time\n\n"
            + b("📣 Broadcast:\n")
            + "<b>/broadcast</b> — Start broadcast wizard\n"
            + "<b>/broadcaststats</b> — History\n\n"
            + b("📢 Channels:\n")
            + "<b>/addchannel</b> @user Title — Add force-sub\n"
            + "<b>/removechannel</b> @user — Remove force-sub\n"
            + "<b>/channel</b> — List channels\n\n"
            + b("👤 User Management:\n")
            + "<b>/listusers</b> [offset] — List users\n"
            + "<b>/banuser</b> @id — Ban user\n"
            + "<b>/unbanuser</b> @id — Unban user\n"
            + "<b>/deleteuser</b> id — Delete user\n"
            + "<b>/exportusers</b> — Export CSV\n"
            + "<b>/info</b> — User/chat details\n\n"
            + b("🤖 Clone Bots:\n")
            + "<b>/addclone</b> TOKEN — Register clone\n"
            + "<b>/clones</b> — List clones\n\n"
            + b("🎨 Post Generation:\n")
            + "<b>/anime</b> name — Anime post\n"
            + "<b>/manga</b> name — Manga post\n"
            + "<b>/movie</b> name — Movie post\n"
            + "<b>/tvshow</b> name — TV show post\n"
            + "<b>/search</b> name — Multi-source search\n\n"
            + b("⚙️ Configuration:\n")
            + "<b>/settings</b> — Category settings\n"
            + "<b>/autoupdate</b> — Manga tracker\n"
            + "<b>/autoforward</b> — Auto-forward manager\n"
            + "<b>/upload</b> — Upload manager\n"
            + "<b>/reload</b> or <b>/restart</b> — Restart bot\n\n"
            + b("🔗 Links:\n")
            + "<b>/backup</b> — Generated links\n"
            + "<b>/id</b> — Get IDs\n"
            + "<b>/connect</b> group — Connect group\n"
            + "<b>/disconnect</b> group — Disconnect\n"
            + "<b>/connections</b> — List connected groups\n"
            + "<b>/logs</b> — View recent logs\n",
            expandable=True,
        )
    )
    await safe_reply(update, text, reply_markup=_back_kb())


@force_sub_required
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    await send_stats_panel(context, update.effective_chat.id)


@force_sub_required
async def sysstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    text = get_system_stats_text()
    await safe_reply(update, text, reply_markup=_back_kb())


@force_sub_required
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    count = get_user_count()
    await safe_reply(
        update,
        b("👥 Total Registered Users:") + " " + code(format_number(count))
    )


@force_sub_required
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    keyboard = [
        [bold_button("🎌 Anime", callback_data="admin_category_settings_anime"),
         bold_button("📚 Manga", callback_data="admin_category_settings_manga")],
        [bold_button("🎬 Movie", callback_data="admin_category_settings_movie"),
         bold_button("📺 TV Show", callback_data="admin_category_settings_tvshow")],
        [bold_button("🔙 BACK", callback_data="admin_back")],
    ]
    text = b("⚙️ Category Settings") + "\n\n" + bq(b("Select a category to configure its template, buttons, watermarks, and more."))

    if SETTINGS_IMAGE_URL:
        sent = await safe_send_photo(
            context.bot, update.effective_chat.id,
            SETTINGS_IMAGE_URL, caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        if sent:
            return
    await safe_reply(update, text, reply_markup=InlineKeyboardMarkup(keyboard))


@force_sub_required
async def add_channel_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if len(context.args) < 2:
        await safe_reply(
            update,
            b("Usage: /addchannel @username Title") + "\n"
            + bq(b("Example: /addchannel @mychannel My Anime Channel"))
        )
        return
    uname = context.args[0]
    title = " ".join(context.args[1:])
    if not uname.startswith("@"):
        await safe_reply(update, b("❌ Username must start with @"))
        return
    try:
        await context.bot.get_chat(uname)
    except Exception:
        await safe_reply(update, b(f"⚠️ Cannot access {e(uname)}. Make the bot an admin there first."))
        return
    add_force_sub_channel(uname, title, join_by_request=False)
    await safe_reply(update, b(f"✅ Added: {e(title)} ({e(uname)}) as force-sub channel."))


@force_sub_required
async def remove_channel_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if len(context.args) != 1:
        await safe_reply(update, b("Usage: /removechannel @username"))
        return
    uname = context.args[0]
    delete_force_sub_channel(uname)
    await safe_reply(update, b(f"🗑 Removed {e(uname)} from force-sub channels."))


@force_sub_required
async def ban_user_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /banuser @username_or_id"))
        return
    uid_input = context.args[0]
    uid = resolve_target_user_id(uid_input)
    if uid is None:
        await safe_reply(update, b(f"❌ User {e(uid_input)} not found in database."))
        return
    if uid in (ADMIN_ID, OWNER_ID):
        await safe_reply(update, b("⚠️ Cannot ban admin/owner."))
        return
    ban_user(uid)
    await safe_reply(update, b(f"🚫 User ") + code(str(uid)) + b(" has been banned."))


@force_sub_required
async def unban_user_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /unbanuser @username_or_id"))
        return
    uid = resolve_target_user_id(context.args[0])
    if uid is None:
        await safe_reply(update, b(f"❌ User not found."))
        return
    unban_user(uid)
    await safe_reply(update, b(f"✅ User ") + code(str(uid)) + b(" has been unbanned."))


@force_sub_required
async def listusers_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    try:
        offset = int(context.args[0]) if context.args else 0
    except (ValueError, IndexError):
        offset = 0

    total = get_user_count()
    users = get_all_users(limit=10, offset=offset)

    text = b(f"👥 Users {offset + 1}–{min(offset + 10, total)} of {format_number(total)}") + "\n\n"
    keyboard_rows = []

    for row in users:
        uid2, username, fname, lname, joined, banned = row
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        status_icon = "🚫" if banned else "✅"
        uname_str = f"@{username}" if username else f"#{uid2}"
        text += f"{status_icon} {b(e(name[:20]))} — {e(uname_str)}\n"
        keyboard_rows.append([bold_button(
            f"{status_icon} {name[:15]}",
            callback_data=f"manage_user_{uid2}"
        )])

    nav = []
    if offset > 0:
        nav.append(bold_button("◀ Prev", callback_data=f"user_page_{max(0, offset - 10)}"))
    if total > offset + 10:
        nav.append(bold_button("Next ▶", callback_data=f"user_page_{offset + 10}"))
    if nav:
        keyboard_rows.append(nav)
    keyboard_rows.append([bold_button("🔙 BACK", callback_data="user_management")])

    await safe_reply(update, text, reply_markup=InlineKeyboardMarkup(keyboard_rows))


@force_sub_required
async def deleteuser_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /deleteuser user_id"))
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await safe_reply(update, b("❌ User ID must be a number."))
        return
    if uid in (ADMIN_ID, OWNER_ID):
        await safe_reply(update, b("⚠️ Cannot delete admin/owner."))
        return
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id = %s", (uid,))
        await safe_reply(update, b(f"✅ User ") + code(str(uid)) + b(" deleted from database."))
    except Exception as exc:
        await safe_reply(update, b("❌ Error: ") + code(e(str(exc)[:200])))


@force_sub_required
async def exportusers_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    try:
        rows = get_all_users(limit=None, offset=0)
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow(["user_id", "username", "first_name", "last_name", "joined_at", "banned"])
        writer.writerows(rows)
        output.seek(0)
        data_bytes = output.getvalue().encode("utf-8")
        await context.bot.send_document(
            update.effective_chat.id,
            document=BytesIO(data_bytes),
            filename=f"users_export_{now_utc().strftime('%Y%m%d_%H%M')}.csv",
            caption=b(f"📤 Exported {format_number(len(rows))} users."),
            parse_mode=ParseMode.HTML,
        )
    except Exception as exc:
        await safe_reply(update, b("❌ Export failed: ") + code(e(str(exc)[:200])))


@force_sub_required
async def broadcaststats_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                SELECT id, mode, total_users, success, blocked, deleted, failed,
                       created_at, completed_at
                FROM broadcast_history
                ORDER BY created_at DESC LIMIT 15
            """)
            rows = cur.fetchall() or []
    except Exception as exc:
        await safe_reply(update, b("❌ Error: ") + code(e(str(exc)[:200])))
        return

    if not rows:
        await safe_reply(update, b("📣 No broadcast history yet."), reply_markup=_back_kb())
        return

    text = b("📣 Recent Broadcasts:") + "\n\n"
    for row in rows:
        bid, mode, total, sent, blocked, deleted, failed, created, completed = row
        dur = ""
        if created and completed:
            try:
                delta = completed - created
                dur = f" | ⏱ {int(delta.total_seconds())}s"
            except Exception:
                pass
        text += (
            f"{b(f'ID #{bid}')} — {code(mode)}\n"
            f"✅ {sent} | ❌ {failed} | 🚫 {blocked}{dur}\n"
            f"📅 {str(created)[:16]}\n\n"
        )

    await safe_reply(update, text, reply_markup=_back_kb())


@force_sub_required
async def backup_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    try:
        links = get_all_links(bot_username=BOT_USERNAME)
    except Exception as exc:
        await safe_reply(update, b("❌ Error: ") + code(e(str(exc)[:200])))
        return

    if not links:
        await safe_reply(update, b("🔗 No links generated yet."), reply_markup=_back_kb())
        return

    text = b(f"🔗 Generated Links ({len(links)}):") + "\n\n"
    for link_id, channel, title, src_bot, created, never_exp in links:
        line = f"• {b(e(title or channel))} — <code>t.me/{e(BOT_USERNAME)}?start={e(link_id)}</code>\n"
        if len(text) + len(line) > 3800:
            text += b("…more links truncated.")
            break
        text += line

    await safe_reply(update, text, reply_markup=_back_kb())


@force_sub_required
async def addclone_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if context.args:
        token = context.args[0].strip()
        await _register_clone_token(update, context, token)
        return

    user_states[update.effective_user.id] = ADD_CLONE_TOKEN
    msg = await safe_reply(
        update,
        b("🤖 Add Clone Bot") + "\n\n"
        + bq(b("Send the BOT TOKEN of the clone bot.\n\n"
               "⚠️ Keep the token secret!")),
        reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
    )
    await store_bot_prompt(context, msg)


async def _register_clone_token(
    update: Update, context: ContextTypes.DEFAULT_TYPE, token: str
) -> None:
    """Validate and register a clone bot token."""
    chat_id = update.effective_chat.id
    try:
        clone_bot = Bot(token=token)
        me = await clone_bot.get_me()
        username = me.username
        # Register commands on clone bot too
        asyncio.create_task(_register_bot_commands_on_bot(clone_bot))
        launch_clone_bot(token, username)
        if add_clone_bot(token, username):
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Clone bot @{e(username)} registered!") + "\n\n"
                + bq(b("Commands have been registered on the clone bot automatically.")),
                reply_markup=InlineKeyboardMarkup([[
                    bold_button("🤖 Manage Clones", callback_data="manage_clones")
                ]]),
            )
        else:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Failed to save clone bot to database.")
            )
    except Exception as exc:
        await safe_send_message(
            context.bot, chat_id,
            b("❌ Invalid token or API error:") + "\n"
            + bq(code(e(str(exc)[:200])))
        )


@force_sub_required
async def clones_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    clones = get_all_clone_bots(active_only=True)
    if not clones:
        await safe_reply(update, b("🤖 No clone bots registered yet."))
        return
    text = b(f"🤖 Active Clone Bots ({len(clones)}):") + "\n\n"
    for cid, token, uname, active, added in clones:
        text += f"• @{e(uname)} — {code(str(added)[:10])}\n"
    await safe_reply(update, text)


@force_sub_required
async def reload_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    triggered_by = (update.effective_user.username or str(update.effective_user.id))
    try:
        with open("restart_message.json", "w") as f:
            json.dump({
                "chat_id": update.effective_chat.id,
                "admin_id": ADMIN_ID,
                "triggered_by": triggered_by,
            }, f)
    except Exception as exc:
        logger.error(f"Failed to write restart file: {exc}")
    try:
        await safe_reply(update, b("♻️ Bot is restarting… Be right back!"))
    except Exception:
        pass
    await asyncio.sleep(1)
    sys.exit(0)


@force_sub_required
async def test_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    await safe_reply(update, b("✅ Bot is alive and healthy!"))


@force_sub_required
async def logs_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    try:
        with open("logs/bot.log", "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()[-60:]
        log_text = "".join(lines)
        if len(log_text) > 3900:
            log_text = log_text[-3900:]
        await safe_reply(update, f"<pre>{e(log_text)}</pre>")
    except Exception as exc:
        await safe_reply(update, b("❌ Error reading logs: ") + code(e(str(exc))))


@force_sub_required
async def channel_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    channels = get_all_force_sub_channels(return_usernames_only=False)
    if not channels:
        await safe_reply(update, b("📢 No force-sub channels configured."))
        return
    text = b(f"📢 Force-Sub Channels ({len(channels)}):") + "\n\n"
    for uname, title, jbr in channels:
        jbr_tag = " (Join By Request)" if jbr else ""
        text += f"• {b(e(title))}\n  {e(uname)}{jbr_tag}\n\n"
    await safe_reply(update, text)


@force_sub_required
async def connect_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /connect @group_or_id"))
        return
    try:
        chat = await context.bot.get_chat(context.args[0])
        if chat.type not in ("group", "supergroup"):
            await safe_reply(update, b("❌ That's not a group."))
            return
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO connected_groups (group_id, group_username, group_title, connected_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (group_id) DO UPDATE SET active = TRUE
            """, (chat.id, chat.username, chat.title, update.effective_user.id))
        await safe_reply(update, b(f"✅ Connected to {e(chat.title)}"))
    except Exception as exc:
        await safe_reply(update, UserFriendlyError.get_user_message(exc))


@force_sub_required
async def disconnect_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    if not context.args:
        await safe_reply(update, b("Usage: /disconnect @group_or_id"))
        return
    try:
        chat = await context.bot.get_chat(context.args[0])
        with db_manager.get_cursor() as cur:
            cur.execute("UPDATE connected_groups SET active = FALSE WHERE group_id = %s", (chat.id,))
        await safe_reply(update, b(f"✅ Disconnected from {e(chat.title)}"))
    except Exception as exc:
        await safe_reply(update, UserFriendlyError.get_user_message(exc))


@force_sub_required
async def connections_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT group_id, group_username, group_title FROM connected_groups WHERE active = TRUE")
            rows = cur.fetchall() or []
    except Exception as exc:
        await safe_reply(update, b("❌ Error: ") + code(e(str(exc)[:200])))
        return
    if not rows:
        await safe_reply(update, b("🔗 No connected groups."))
        return
    text = b(f"🔗 Connected Groups ({len(rows)}):") + "\n\n"
    for gid, uname, title in rows:
        text += f"• {b(e(title or ''))} {('@' + uname) if uname else ''} {code(str(gid))}\n"
    await safe_reply(update, text)


@force_sub_required
async def id_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.message:
        return
    msg = update.message
    text = (
        b("🆔 Identifier Info") + "\n\n"
        f"<b>Your User ID:</b> {code(str(update.effective_user.id))}\n"
        f"<b>Chat ID:</b> {code(str(update.effective_chat.id))}\n"
        f"<b>Chat Type:</b> {code(update.effective_chat.type)}"
    )
    if msg.reply_to_message:
        rep = msg.reply_to_message
        if rep.from_user:
            text += f"\n<b>Replied User ID:</b> {code(str(rep.from_user.id))}"
        if rep.forward_from:
            text += f"\n<b>Forward User ID:</b> {code(str(rep.forward_from.id))}"
        if rep.forward_from_chat:
            text += f"\n<b>Forward Chat ID:</b> {code(str(rep.forward_from_chat.id))}"
        if rep.sticker:
            text += f"\n<b>Sticker File ID:</b>\n{code(rep.sticker.file_id)}"
        if rep.photo:
            text += f"\n<b>Photo File ID:</b>\n{code(rep.photo[-1].file_id)}"
        if rep.video:
            text += f"\n<b>Video File ID:</b>\n{code(rep.video.file_id)}"
        if rep.audio:
            text += f"\n<b>Audio File ID:</b>\n{code(rep.audio.file_id)}"
        if rep.document:
            text += f"\n<b>Document File ID:</b>\n{code(rep.document.file_id)}"
        if rep.animation:
            text += f"\n<b>GIF File ID:</b>\n{code(rep.animation.file_id)}"
        if rep.voice:
            text += f"\n<b>Voice File ID:</b>\n{code(rep.voice.file_id)}"
    await msg.reply_text(text, parse_mode=ParseMode.HTML)


@force_sub_required
async def info_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not update.message:
        return
    target = None
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        try:
            target = await context.bot.get_chat(context.args[0])
        except Exception as exc:
            await update.message.reply_text(UserFriendlyError.get_user_message(exc), parse_mode=ParseMode.HTML)
            return
    else:
        target = update.effective_user

    if not target:
        await update.message.reply_text(b("No target specified."), parse_mode=ParseMode.HTML)
        return

    uid_val = getattr(target, "id", "N/A")
    uname = getattr(target, "username", None)
    fname = getattr(target, "first_name", None)
    lname = getattr(target, "last_name", None)
    title = getattr(target, "title", None)
    chat_type = getattr(target, "type", None)

    text = b("👤 Info") + "\n\n"
    text += f"<b>ID:</b> {code(str(uid_val))}\n"
    if uname:
        text += f"<b>Username:</b> @{e(uname)}\n"
    if fname:
        text += f"<b>First Name:</b> {e(fname)}\n"
    if lname:
        text += f"<b>Last Name:</b> {e(lname)}\n"
    if title:
        text += f"<b>Title:</b> {e(title)}\n"
    if chat_type:
        text += f"<b>Type:</b> {code(chat_type)}\n"

    # Check if user exists in DB
    try:
        user_info = get_user_info_by_id(int(uid_val))
        if user_info:
            _, _, _, _, joined, banned = user_info
            text += f"<b>Joined Bot:</b> {code(str(joined)[:16])}\n"
            text += f"<b>Status:</b> {'🚫 Banned' if banned else '✅ Active'}\n"
    except Exception:
        pass

    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


@force_sub_required
async def upload_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    await load_upload_progress()
    await show_upload_menu(update.effective_chat.id, context)


@force_sub_required
async def autoforward_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await _show_autoforward_menu(context, update.effective_chat.id)


@force_sub_required
async def autoupdate_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await _show_autoupdate_menu(context, update.effective_chat.id)


async def _show_autoforward_menu(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM auto_forward_connections WHERE active = TRUE")
            active_count = cur.fetchone()[0]
    except Exception:
        active_count = 0

    af_enabled = get_setting("autoforward_enabled", "true")
    on_off = "ON" if active_count > 0 and af_enabled == "true" else "OFF"
    text = (
        b("Auto Forward Settings") + "\n\n"
        f"<b>Status:</b> {on_off}\n"
        f"<b>Connections:</b> {active_count}"
    )
    # Spec-compliant AUTO FORWARD PANEL layout
    keyboard = [
        [bold_button("MODE", callback_data="af_set_caption")],
        [bold_button("MANAGE CONNECTIONS", callback_data="af_list_connections")],
        [bold_button("SETTINGS", callback_data="af_add_connection"),
         bold_button("FILTERS", callback_data="af_filters_menu")],
        [bold_button("REPLACEMENTS", callback_data="af_replacements_menu"),
         bold_button("DELAY", callback_data="af_set_delay")],
        [bold_button("BULK FORWARD", callback_data="af_bulk")],
        [bold_button("TOGGLE ON/OFF", callback_data="af_toggle_all")],
        [bold_button("🔙 BACK", callback_data="admin_back")],
    ]
    await safe_send_message(
        context.bot, chat_id, text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _show_autoupdate_menu(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int
) -> None:
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM manga_auto_updates WHERE active = TRUE")
            active_count = cur.fetchone()[0]
    except Exception:
        active_count = 0

    text = (
        b("📚 Auto Manga Update Manager") + "\n\n"
        f"<b>Tracked Manga:</b> {code(str(active_count))}\n\n"
        + bq(b("The bot checks for new chapters every hour\n"
               "and sends a notification to your target channel."))
    )
    keyboard = [
        [bold_button("➕ Track New Manga", callback_data="au_add_manga")],
        [bold_button("📋 View Tracked", callback_data="au_list_manga"),
         bold_button("🗑 Stop Tracking", callback_data="au_remove_manga")],
        [bold_button("📊 Manga Stats", callback_data="au_stats")],
        [bold_button("🔙 BACK", callback_data="admin_back")],
    ]
    await safe_send_message(
        context.bot, chat_id, text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


# ================================================================================
#                       BROADCAST SYSTEM — COMPLETE
# ================================================================================

async def _do_broadcast(
    context: ContextTypes.DEFAULT_TYPE,
    admin_chat_id: int,
    from_chat_id: int,
    message_id: int,
    mode: str,
) -> None:
    """Execute a broadcast to all registered users."""
    users = get_all_users(limit=None, offset=0)
    total = len(users)
    sent = fail = blocked = deleted_count = 0
    deleted_uids: list = []  # track UIDs of deactivated accounts for DB cleanup

    # Log to DB
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO broadcast_history (admin_id, mode, total_users, message_text)
                VALUES (%s, %s, %s, %s) RETURNING id
            """, (ADMIN_ID, mode, total, f"copy:{from_chat_id}:{message_id}"))
            bc_id = cur.fetchone()[0]
    except Exception:
        bc_id = None

    # Progress msg
    progress_msg = await safe_send_message(
        context.bot, admin_chat_id,
        b(f"📣 Broadcasting to {format_number(total)} users…"),
    )

    for i, user_row in enumerate(users):
        uid = user_row[0]
        if uid in (ADMIN_ID, OWNER_ID):
            continue
        try:
            if mode == BroadcastMode.AUTO_DELETE:
                msg = await context.bot.copy_message(
                    uid, from_chat_id, message_id
                )
                context.job_queue.run_once(
                    lambda ctx, u=uid, m=msg.message_id: safe_delete(ctx.bot, u, m),
                    when=86400,
                )
            elif mode in (BroadcastMode.PIN, BroadcastMode.DELETE_PIN):
                msg = await context.bot.copy_message(uid, from_chat_id, message_id)
                try:
                    await context.bot.pin_chat_message(uid, msg.message_id, disable_notification=True)
                    if mode == BroadcastMode.DELETE_PIN:
                        await safe_delete(context.bot, uid, msg.message_id)
                except Exception:
                    pass
            elif mode == BroadcastMode.SILENT:
                await context.bot.copy_message(
                    uid, from_chat_id, message_id,
                    disable_notification=True,
                )
            else:  # NORMAL
                await context.bot.copy_message(uid, from_chat_id, message_id)
            sent += 1
        except Forbidden as err:
            fail += 1
            err_s = str(err).lower()
            if "blocked" in err_s:
                blocked += 1
            elif "deactivated" in err_s or "deleted" in err_s:
                deleted_count += 1
                deleted_uids.append(uid)
        except RetryAfter as err:
            await asyncio.sleep(err.retry_after + 1)
            try:
                await context.bot.copy_message(uid, from_chat_id, message_id)
                sent += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1
        await asyncio.sleep(RATE_LIMIT_DELAY)

        # Update progress every 500
        if progress_msg and (i + 1) % 500 == 0:
            try:
                await progress_msg.edit_text(
                    b(f"📣 Broadcasting… {i+1}/{total}"),
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

    # Final update
    if bc_id:
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    UPDATE broadcast_history
                    SET completed_at = NOW(), success = %s, blocked = %s,
                        deleted = %s, failed = %s
                    WHERE id = %s
                """, (sent, blocked, deleted_count, fail, bc_id))
        except Exception:
            pass

    # Purge deactivated / deleted accounts from the users table
    purged = 0
    if deleted_uids:
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    "DELETE FROM users WHERE user_id = ANY(%s)",
                    (deleted_uids,)
                )
                purged = cur.rowcount
        except Exception as exc:
            logger.debug(f"Purge deleted users error: {exc}")

    result = (
        b("📣 Broadcast Complete!") + "\n\n"
        + bq(
            f"<b>✅ Sent:</b> {code(format_number(sent))}\n"
            f"<b>🚫 Blocked:</b> {code(format_number(blocked))}\n"
            f"<b>🗑 Deleted accounts:</b> {code(format_number(deleted_count))}\n"
            f"<b>🧹 Purged from DB:</b> {code(format_number(purged))}\n"
            f"<b>❌ Other failures:</b> {code(format_number(fail - blocked - deleted_count if fail > 0 else 0))}\n"
            f"<b>📊 Total users:</b> {code(format_number(total))}"
        )
    )

    if progress_msg:
        try:
            await progress_msg.edit_text(result, parse_mode=ParseMode.HTML)
        except Exception:
            await safe_send_message(context.bot, admin_chat_id, result)
    else:
        await safe_send_message(context.bot, admin_chat_id, result)


# ================================================================================
#                      UPLOAD MANAGER — COMPLETE
# ================================================================================

async def load_upload_progress() -> None:
    """Load upload progress from database into global dict."""
    global upload_progress
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                SELECT target_chat_id, season, episode, total_episode, video_count,
                       selected_qualities, base_caption, auto_caption_enabled, anime_name
                FROM bot_progress WHERE id = 1
            """)
            row = cur.fetchone()
        if row:
            upload_progress.update({
                "target_chat_id": row[0],
                "season": row[1] or 1,
                "episode": row[2] or 1,
                "total_episode": row[3] or 1,
                "video_count": row[4] or 0,
                "selected_qualities": row[5].split(",") if row[5] else ["480p", "720p", "1080p"],
                "base_caption": row[6] or DEFAULT_CAPTION,
                "auto_caption_enabled": bool(row[7]),
                "anime_name": row[8] or "Anime Name",
            })
    except Exception as exc:
        db_logger.debug(f"load_upload_progress error: {exc}")


async def save_upload_progress() -> None:
    """Persist upload progress to database."""
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                UPDATE bot_progress SET
                    target_chat_id = %s, season = %s, episode = %s,
                    total_episode = %s, video_count = %s,
                    selected_qualities = %s, base_caption = %s,
                    auto_caption_enabled = %s, anime_name = %s
                WHERE id = 1
            """, (
                upload_progress["target_chat_id"],
                upload_progress["season"],
                upload_progress["episode"],
                upload_progress["total_episode"],
                upload_progress["video_count"],
                ",".join(upload_progress["selected_qualities"]),
                upload_progress["base_caption"],
                upload_progress["auto_caption_enabled"],
                upload_progress.get("anime_name", "Anime Name"),
            ))
    except Exception as exc:
        db_logger.debug(f"save_upload_progress error: {exc}")


def build_caption_from_progress() -> str:
    """Build formatted caption for current episode/quality."""
    quality = "N/A"
    if upload_progress["selected_qualities"]:
        idx = upload_progress["video_count"] % len(upload_progress["selected_qualities"])
        quality = upload_progress["selected_qualities"][idx]
    return (
        upload_progress["base_caption"]
        .replace("{anime_name}", upload_progress.get("anime_name", "Anime Name"))
        .replace("{season}", f"{upload_progress['season']:02}")
        .replace("{episode}", f"{upload_progress['episode']:02}")
        .replace("{total_episode}", f"{upload_progress['total_episode']:02}")
        .replace("{quality}", quality)
    )


def get_upload_menu_markup() -> InlineKeyboardMarkup:
    """Build upload manager keyboard."""
    auto_status = "✅ ON" if upload_progress["auto_caption_enabled"] else "❌ OFF"
    return InlineKeyboardMarkup([
        [bold_button("👁 Preview Caption", callback_data="upload_preview"),
         bold_button("📝 Set Caption", callback_data="upload_set_caption")],
        [bold_button("🎌 Set Anime Name", callback_data="upload_set_anime_name"),
         bold_button("📅 Set Season", callback_data="upload_set_season")],
        [bold_button("🔢 Set Episode", callback_data="upload_set_episode"),
         bold_button("🔢 Total Episodes", callback_data="upload_set_total")],
        [bold_button("🎛 Quality Settings", callback_data="upload_quality_menu"),
         bold_button("📢 Target Channel", callback_data="upload_set_channel")],
        [bold_button(f"Auto-Caption: {auto_status}", callback_data="upload_toggle_auto")],
        [bold_button("🔄 Reset Episode to 1", callback_data="upload_reset"),
         bold_button("🗑 Clear DB", callback_data="upload_clear_db")],
        [bold_button("🔙 BACK", callback_data="admin_back")],
    ])


async def show_upload_menu(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    edit_msg: Optional[Any] = None,
) -> None:
    """Display the upload manager panel."""
    target = (
        f"✅ {code(str(upload_progress['target_chat_id']))}"
        if upload_progress["target_chat_id"] else "❌ Not Set"
    )
    auto = "✅ ON" if upload_progress["auto_caption_enabled"] else "❌ OFF"
    qualities = ", ".join(upload_progress["selected_qualities"]) or "None"

    text = (
        b("📤 Upload Manager") + "\n\n"
        f"<b>🎌 Anime:</b> {code(e(upload_progress.get('anime_name', 'Anime Name')))}\n"
        f"<b>📢 Target Channel:</b> {target}\n"
        f"<b>Auto-Caption:</b> {auto}\n"
        f"<b>📅 Season:</b> {code(str(upload_progress['season']))}\n"
        f"<b>🔢 Episode:</b> {code(str(upload_progress['episode']))} / "
        + code(str(upload_progress["total_episode"])) + "\n"
        f"<b>🎛 Qualities:</b> {code(qualities)}\n"
        f"<b>🎬 Videos Sent (current quality cycle):</b> "
        + code(str(upload_progress["video_count"]))
    )
    markup = get_upload_menu_markup()

    try:
        if edit_msg:
            await context.bot.edit_message_text(
                chat_id=chat_id, message_id=edit_msg.message_id,
                text=text, parse_mode=ParseMode.HTML, reply_markup=markup,
            )
        else:
            await safe_send_message(context.bot, chat_id, text, reply_markup=markup)
    except Exception:
        await safe_send_message(context.bot, chat_id, text, reply_markup=markup)


# ================================================================================
#                           INLINE QUERY HANDLER
# ================================================================================

@force_sub_required
async def inline_query_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle @bot queries inline."""
    query = update.inline_query
    if not query or not query.query.strip():
        return
    if get_setting("inline_search_enabled", "true") != "true":
        return

    search = query.query.strip()
    results = []

    try:
        anime = AniListClient.search_anime(search)
        if anime:
            title_obj = anime.get("title", {}) or {}
            title = title_obj.get("romaji") or title_obj.get("english") or search
            cover = (anime.get("coverImage") or {})
            thumb = cover.get("medium") or ""
            score = anime.get("averageScore", "N/A")
            status = (anime.get("status") or "Unknown").title()
            genres = ", ".join((anime.get("genres") or [])[:3])
            results.append(
                InlineQueryResultArticle(
                    id=f"al_anime_{anime['id']}",
                    title=f"🎌 {title}",
                    description=f"Score: {score}/100 • {status} • {genres}",
                    thumb_url=thumb,
                    input_message_content=InputTextMessageContent(
                        b(f"🎌 {e(title)}") + "\n"
                        + bq(
                            f"<b>Score:</b> {score}/100\n"
                            f"<b>Status:</b> {e(status)}\n"
                            f"<b>Genres:</b> {e(genres)}"
                        ) + f"\n\n<a href='https://anilist.co/anime/{anime['id']}'>🔗 AniList</a>",
                        parse_mode=ParseMode.HTML,
                    ),
                )
            )
    except Exception:
        pass

    try:
        manga = AniListClient.search_manga(search)
        if manga:
            title_obj = manga.get("title", {}) or {}
            title = title_obj.get("romaji") or title_obj.get("english") or search
            cover = (manga.get("coverImage") or {})
            thumb = cover.get("medium") or ""
            score = manga.get("averageScore", "N/A")
            status = (manga.get("status") or "Unknown").title()
            chapters = manga.get("chapters", "?")
            results.append(
                InlineQueryResultArticle(
                    id=f"al_manga_{manga['id']}",
                    title=f"📚 {title}",
                    description=f"Score: {score}/100 • {status} • {chapters} chapters",
                    thumb_url=thumb,
                    input_message_content=InputTextMessageContent(
                        b(f"📚 {e(title)}") + "\n"
                        + bq(
                            f"<b>Score:</b> {score}/100\n"
                            f"<b>Status:</b> {e(status)}\n"
                            f"<b>Chapters:</b> {chapters}"
                        ) + f"\n\n<a href='https://anilist.co/manga/{manga['id']}'>🔗 AniList</a>",
                        parse_mode=ParseMode.HTML,
                    ),
                )
            )
    except Exception:
        pass

    try:
        if TMDB_API_KEY:
            movie = TMDBClient.search_movie(search)
            if movie:
                title = movie.get("title") or search
                year = movie.get("release_date", "")[:4]
                rating = movie.get("vote_average", "N/A")
                poster_path = movie.get("poster_path")
                thumb = TMDBClient.get_poster_url(poster_path, "w92") if poster_path else ""
                results.append(
                    InlineQueryResultArticle(
                        id=f"tmdb_movie_{movie.get('id', 0)}",
                        title=f"🎬 {title} ({year})",
                        description=f"Rating: {rating}/10",
                        thumb_url=thumb,
                        input_message_content=InputTextMessageContent(
                            b(f"🎬 {e(title)}") + " " + code(f"({year})") + "\n"
                            + bq(f"<b>Rating:</b> {rating}/10"),
                            parse_mode=ParseMode.HTML,
                        ),
                    )
                )
    except Exception:
        pass

    try:
        await query.answer(results[:10], cache_time=30, is_personal=False)
    except Exception as exc:
        logger.debug(f"Inline query answer error: {exc}")


# ================================================================================
#                        GROUP MESSAGE HANDLER
# ================================================================================

async def group_message_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle messages in bot-connected groups with auto-delete support."""
    if not update.message or not update.effective_chat:
        return
    if get_setting("group_commands_enabled", "true") != "true":
        return
    if not _passes_filter(update):
        return

    chat_id = update.effective_chat.id
    try:
        with db_manager.get_cursor() as cur:
            cur.execute(
                "SELECT 1 FROM connected_groups WHERE group_id = %s AND active = TRUE",
                (chat_id,)
            )
            if not cur.fetchone():
                return
    except Exception:
        return

    text = update.message.text or ""
    lower = text.lower()
    auto_del = get_setting("auto_delete_messages", "true") == "true"
    del_delay = int(get_setting("auto_delete_delay", "60"))

    # Schedule auto-delete of user command after 5 seconds
    if auto_del:
        async def _del_user_cmd(msg=update.message):
            await asyncio.sleep(5)
            try:
                await msg.delete()
            except Exception:
                pass
        asyncio.create_task(_del_user_cmd())

    async def _group_post_with_autodel(category: str, query_text: str) -> None:
        await generate_and_send_post(context, chat_id, category, query_text)

    for prefix, category in [
        ("/anime ", "anime"), ("/manga ", "manga"),
        ("/movie ", "movie"), ("/tvshow ", "tvshow"),
    ]:
        if lower.startswith(prefix):
            query_text = text[len(prefix):].strip()
            if query_text:
                await _group_post_with_autodel(category, query_text)
            return


# ================================================================================
#                       AUTO-FORWARD SYSTEM (COMPLETE)
# ================================================================================

async def auto_forward_message_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Forward channel posts to target channels based on connection config."""
    msg = update.channel_post
    if not msg:
        return
    chat_id = update.effective_chat.id

    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                SELECT id, target_chat_id, protect_content, silent, pin_message,
                       delete_source, delay_seconds
                FROM auto_forward_connections
                WHERE source_chat_id = %s AND active = TRUE
            """, (chat_id,))
            connections = cur.fetchall() or []
    except Exception as exc:
        logger.debug(f"auto_forward DB error: {exc}")
        return

    for conn in connections:
        conn_id, target, protect, silent, pin, delete_src, delay = conn

        # Load filter config
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    SELECT allowed_media, blacklist_words, whitelist_words,
                           caption_override, replacements
                    FROM auto_forward_filters WHERE connection_id = %s
                """, (conn_id,))
                filter_row = cur.fetchone()
        except Exception:
            filter_row = None

        # Apply filters
        if filter_row:
            allowed_media, blacklist_words, whitelist_words, caption_override, replacements = filter_row

            # Media type filter
            if allowed_media:
                media_types = [m.strip() for m in allowed_media.split(",")]
                msg_media_type = None
                if msg.photo:
                    msg_media_type = "photo"
                elif msg.video:
                    msg_media_type = "video"
                elif msg.document:
                    msg_media_type = "document"
                elif msg.audio:
                    msg_media_type = "audio"
                elif msg.sticker:
                    msg_media_type = "sticker"
                elif msg.text:
                    msg_media_type = "text"
                if msg_media_type and msg_media_type not in media_types:
                    continue

            # Text filters
            check_text = (msg.caption or msg.text or "").lower()
            if whitelist_words:
                words = [w.strip().lower() for w in whitelist_words.split(",")]
                if not any(w in check_text for w in words):
                    continue
            if blacklist_words:
                words = [w.strip().lower() for w in blacklist_words.split(",")]
                if any(w in check_text for w in words):
                    continue

            # Replacements
            if replacements:
                try:
                    rep_list = json.loads(replacements)
                    for rep in rep_list:
                        pattern = rep.get("pattern", "")
                        value = rep.get("value", "")
                        if pattern:
                            check_text = check_text.replace(pattern.lower(), value)
                except Exception:
                    pass
        else:
            caption_override = None

        # Delay or immediate
        if delay and delay > 0:
            context.job_queue.run_once(
                _delayed_forward,
                when=delay,
                data={
                    "from_chat_id": chat_id,
                    "message_id": msg.message_id,
                    "target_chat_id": target,
                    "protect": protect,
                    "silent": silent,
                    "pin": pin,
                    "delete_src": delete_src,
                    "caption_override": caption_override,
                },
            )
        else:
            asyncio.create_task(
                _do_forward(
                    context.bot, chat_id, msg.message_id, target,
                    protect=protect, silent=silent, pin=pin,
                    delete_src=delete_src, caption_override=caption_override,
                )
            )


async def _do_forward(
    bot: Bot,
    from_chat_id: int,
    message_id: int,
    target_chat_id: int,
    protect: bool = False,
    silent: bool = False,
    pin: bool = False,
    delete_src: bool = False,
    caption_override: Optional[str] = None,
) -> None:
    """Execute a single forward operation."""
    try:
        new_msg = await bot.copy_message(
            chat_id=target_chat_id,
            from_chat_id=from_chat_id,
            message_id=message_id,
            protect_content=protect,
            disable_notification=silent,
        )
        if caption_override and new_msg:
            try:
                await bot.edit_message_caption(
                    chat_id=target_chat_id,
                    message_id=new_msg.message_id,
                    caption=caption_override,
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        if pin and new_msg:
            try:
                await bot.pin_chat_message(target_chat_id, new_msg.message_id, disable_notification=True)
            except Exception:
                pass
        if delete_src:
            await safe_delete(bot, from_chat_id, message_id)
    except Exception as exc:
        logger.debug(f"_do_forward error: {exc}")


async def _delayed_forward(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job handler for delayed forwards."""
    d = context.job.data
    await _do_forward(
        context.bot,
        d["from_chat_id"], d["message_id"], d["target_chat_id"],
        protect=d.get("protect", False),
        silent=d.get("silent", False),
        pin=d.get("pin", False),
        delete_src=d.get("delete_src", False),
        caption_override=d.get("caption_override"),
    )


# ================================================================================
#                        VIDEO UPLOAD HANDLER
# ================================================================================

async def handle_upload_video(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle video sent to bot by admin — auto-captions and forwards."""
    if not update.effective_user or update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    if not update.message or not update.message.video:
        return

    async with upload_lock:
        await load_upload_progress()

        if not upload_progress["target_chat_id"]:
            await update.message.reply_text(
                b("❌ Target channel not set!") + "\n" + bq(b("Use /upload to configure it first.")),
                parse_mode=ParseMode.HTML,
            )
            return

        if not upload_progress["selected_qualities"]:
            await update.message.reply_text(
                b("❌ No qualities selected!") + "\n" + bq(b("Use /upload → Quality Settings.")),
                parse_mode=ParseMode.HTML,
            )
            return

        file_id = update.message.video.file_id
        caption = build_caption_from_progress()

        try:
            await context.bot.send_video(
                chat_id=upload_progress["target_chat_id"],
                video=file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True,
            )

            quality = upload_progress["selected_qualities"][
                upload_progress["video_count"] % len(upload_progress["selected_qualities"])
            ]
            await update.message.reply_text(
                b(f"✅ Video forwarded! Quality: {quality}") + "\n"
                + bq(
                    f"<b>Season:</b> {upload_progress['season']:02}\n"
                    f"<b>Episode:</b> {upload_progress['episode']:02}"
                ),
                parse_mode=ParseMode.HTML,
            )

            upload_progress["video_count"] += 1
            if upload_progress["video_count"] >= len(upload_progress["selected_qualities"]):
                upload_progress["episode"] += 1
                upload_progress["total_episode"] = max(
                    upload_progress["total_episode"], upload_progress["episode"]
                )
                upload_progress["video_count"] = 0

            await save_upload_progress()

        except Exception as exc:
            await update.message.reply_text(
                UserFriendlyError.get_user_message(exc),
                parse_mode=ParseMode.HTML,
            )


# ================================================================================
#                      CHANNEL POST HANDLER (AUTO-CAPTION)
# ================================================================================

async def handle_channel_post(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Auto-caption videos posted directly to the target channel."""
    if not update.channel_post or not update.channel_post.video:
        return
    chat_id = update.effective_chat.id
    await load_upload_progress()

    if (
        chat_id != upload_progress.get("target_chat_id")
        or not upload_progress.get("auto_caption_enabled")
    ):
        return

    async with upload_lock:
        if not upload_progress["selected_qualities"]:
            return
        caption = build_caption_from_progress()
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=update.channel_post.message_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
            upload_progress["video_count"] += 1
            if upload_progress["video_count"] >= len(upload_progress["selected_qualities"]):
                upload_progress["episode"] += 1
                upload_progress["total_episode"] = max(
                    upload_progress["total_episode"], upload_progress["episode"]
                )
                upload_progress["video_count"] = 0
            await save_upload_progress()
        except Exception as exc:
            logger.debug(f"Auto-caption error: {exc}")


# ================================================================================
#                    ADMIN PHOTO HANDLER (CATEGORY LOGO)
# ================================================================================

async def handle_admin_photo(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle photo sent by admin when setting category logo."""
    if not update.effective_user or update.effective_user.id not in (ADMIN_ID, OWNER_ID):
        return
    uid = update.effective_user.id
    state = user_states.get(uid)
    if state != SET_CATEGORY_LOGO:
        return
    if not update.message:
        return

    if update.message.photo:
        file_id = update.message.photo[-1].file_id
    elif update.message.document and update.message.document.mime_type and "image" in update.message.document.mime_type:
        file_id = update.message.document.file_id
    else:
        await update.message.reply_text(b("❌ Please send an image file."), parse_mode=ParseMode.HTML)
        return

    category = context.user_data.get("editing_category")
    if category:
        update_category_field(category, "logo_file_id", file_id)
        await update.message.reply_text(
            b(f"✅ Logo updated for {e(category)}!"), parse_mode=ParseMode.HTML
        )

    user_states.pop(uid, None)
    await send_admin_menu(update.effective_chat.id, context)


# ================================================================================
#                     SCHEDULED BROADCAST JOB
# ================================================================================

async def check_scheduled_broadcasts(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job: check for pending scheduled broadcasts and execute them."""
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                SELECT id, admin_id, message_text, media_file_id, media_type
                FROM scheduled_broadcasts
                WHERE status = 'pending' AND execute_at <= NOW()
                LIMIT 5
            """)
            rows = cur.fetchall() or []
    except Exception as exc:
        logger.debug(f"check_scheduled_broadcasts DB error: {exc}")
        return

    for row in rows:
        b_id, admin_id, text, media_file_id, media_type = row
        users = get_all_users(limit=None, offset=0)
        sent = fail = 0
        for u in users:
            try:
                await context.bot.send_message(u[0], text, parse_mode=ParseMode.HTML)
                sent += 1
            except Exception:
                fail += 1
            await asyncio.sleep(RATE_LIMIT_DELAY)

        status = "sent"
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    "UPDATE scheduled_broadcasts SET status = %s WHERE id = %s",
                    (status, b_id)
                )
        except Exception:
            pass

        # Notify admin
        try:
            await context.bot.send_message(
                admin_id,
                b(f"✅ Scheduled broadcast #{b_id} done.") + "\n"
                + bq(f"<b>Sent:</b> {sent} | <b>Failed:</b> {fail}"),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ================================================================================
#                         MANGA UPDATE JOB (COMPLETE)

# ================================================================================
#                         CLONE BOT — INDEPENDENT POLLING
# ================================================================================

def _register_all_handlers(app: Application) -> None:
    """Register every bot handler on the given Application instance."""
    admin_filter = filters.User(user_id=ADMIN_ID) | filters.User(user_id=OWNER_ID)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("alive", alive_command))
    app.add_handler(CommandHandler("test", test_command))
    app.add_handler(CommandHandler("search", search_command))
    app.add_handler(CommandHandler("anime", anime_command))
    app.add_handler(CommandHandler("manga", manga_command))
    app.add_handler(CommandHandler("movie", movie_command))
    app.add_handler(CommandHandler("tvshow", tvshow_command))
    app.add_handler(CommandHandler("id", id_command))
    app.add_handler(CommandHandler("info", info_command))
    app.add_handler(CommandHandler("stats", stats_command, filters=admin_filter))
    app.add_handler(CommandHandler("sysstats", sysstats_command, filters=admin_filter))
    app.add_handler(CommandHandler("users", users_command, filters=admin_filter))
    app.add_handler(CommandHandler("cmd", cmd_command, filters=admin_filter))
    app.add_handler(CommandHandler("upload", upload_command, filters=admin_filter))
    app.add_handler(CommandHandler("settings", settings_command, filters=admin_filter))
    app.add_handler(CommandHandler("autoupdate", autoupdate_command, filters=admin_filter))
    app.add_handler(CommandHandler("autoforward", autoforward_command, filters=admin_filter))
    app.add_handler(CommandHandler("addchannel", add_channel_command, filters=admin_filter))
    app.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    app.add_handler(CommandHandler("banuser", ban_user_command, filters=admin_filter))
    app.add_handler(CommandHandler("unbanuser", unban_user_command, filters=admin_filter))
    app.add_handler(CommandHandler("listusers", listusers_command, filters=admin_filter))
    app.add_handler(CommandHandler("deleteuser", deleteuser_command, filters=admin_filter))
    app.add_handler(CommandHandler("exportusers", exportusers_command, filters=admin_filter))
    app.add_handler(CommandHandler("backup", backup_command, filters=admin_filter))
    app.add_handler(CommandHandler("addclone", addclone_command, filters=admin_filter))
    app.add_handler(CommandHandler("clones", clones_command, filters=admin_filter))
    app.add_handler(CommandHandler("reload", reload_command, filters=admin_filter))
    app.add_handler(CommandHandler("restart", reload_command, filters=admin_filter))
    app.add_handler(CommandHandler("logs", logs_command, filters=admin_filter))
    app.add_handler(CommandHandler("connect", connect_command, filters=admin_filter))
    app.add_handler(CommandHandler("disconnect", disconnect_command, filters=admin_filter))
    app.add_handler(CommandHandler("connections", connections_command, filters=admin_filter))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    app.add_handler(MessageHandler(
        filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, group_message_handler))
    app.add_handler(InlineQueryHandler(inline_query_handler))
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, auto_forward_message_handler))
    app.add_handler(MessageHandler(
        filters.ChatType.CHANNEL & filters.VIDEO, handle_channel_post))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.VIDEO & admin_filter, handle_upload_video))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (filters.PHOTO | filters.Document.IMAGE) & admin_filter,
        handle_admin_photo))
    app.add_error_handler(error_handler)


async def _run_clone_polling(token: str, uname: str) -> None:
    """Run a clone bot as an independent Application with all handlers."""
    logger.info(f"🤖 Starting clone bot @{uname} polling...")
    try:
        app = (
            Application.builder()
            .token(token)
            .connect_timeout(30)
            .read_timeout(30)
            .write_timeout(30)
            .build()
        )
        _register_all_handlers(app)
        async with app:
            await app.initialize()
            await app.start()
            if app.updater:
                await app.updater.start_polling(
                    allowed_updates=Update.ALL_TYPES,
                    drop_pending_updates=True,
                )
            logger.info(f"✅ Clone @{uname} polling started")
            # Run until cancelled
            while app.running:
                await asyncio.sleep(5)
    except asyncio.CancelledError:
        logger.info(f"🛑 Clone @{uname} polling cancelled")
    except Exception as exc:
        logger.error(f"❌ Clone @{uname} error: {exc}")


def launch_clone_bot(token: str, uname: str) -> None:
    """Schedule a clone bot polling task on the running event loop."""
    if uname in _clone_tasks:
        existing = _clone_tasks[uname]
        if not existing.done():
            logger.info(f"Clone @{uname} already running")
            return
    task = asyncio.ensure_future(_run_clone_polling(token, uname))
    _clone_tasks[uname] = task
    logger.info(f"🤖 Clone @{uname} task scheduled")


# ================================================================================
#                         MANGA CHAPTER — PDF DELIVERY
# ================================================================================

async def _deliver_chapter_as_pdf(
    bot, chat_id: int, manga_title: str, ch_num: str, chapter_id: str
) -> bool:
    """Download MangaDex chapter pages and send as a PDF document.
    Falls back to sending page images if PDF libraries are unavailable.
    Returns True on success.
    """
    import io as _io
    try:
        pages = MangaDexClient.get_chapter_pages(chapter_id)
        if not pages:
            return False
        base_url, ch_hash, filenames = pages
        if not filenames:
            return False
        import urllib.request as _req
        # Download pages (cap at 60 pages)
        page_bytes: list = []
        for fn in filenames[:60]:
            url = f"{base_url}/data/{ch_hash}/{fn}"
            try:
                with _req.urlopen(url, timeout=20) as resp:
                    page_bytes.append(resp.read())
                await asyncio.sleep(0.1)
            except Exception:
                pass
        if not page_bytes:
            return False
        safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in manga_title)
        filename = f"{safe_title}_Chapter_{ch_num}.pdf"
        # Try fpdf2 first
        try:
            import tempfile, os as _os
            from fpdf import FPDF
            pdf = FPDF()
            with tempfile.TemporaryDirectory() as tmpdir:
                for i, pb in enumerate(page_bytes):
                    img_path = _os.path.join(tmpdir, f"p{i}.jpg")
                    with open(img_path, "wb") as f:
                        f.write(pb)
                    pdf.add_page()
                    pdf.image(img_path, 0, 0, 210)
                pdf_bytes = bytes(pdf.output())
        except ImportError:
            # Fallback: try Pillow
            try:
                from PIL import Image as _Img
                imgs = [_Img.open(_io.BytesIO(pb)).convert("RGB") for pb in page_bytes]
                pdf_io = _io.BytesIO()
                imgs[0].save(pdf_io, format="PDF", save_all=True, append_images=imgs[1:])
                pdf_bytes = pdf_io.getvalue()
            except Exception:
                # Last fallback: send pages as individual images
                media_group = []
                for i, pb in enumerate(page_bytes[:10]):
                    media_group.append({"type": "photo", "media": _io.BytesIO(pb)})
                if media_group:
                    cap = f"📖 <b>{manga_title}</b> — Chapter {ch_num} (images)"
                    await bot.send_photo(
                        chat_id,
                        photo=_io.BytesIO(page_bytes[0]),
                        caption=cap,
                        parse_mode=ParseMode.HTML,
                    )
                return True
        await bot.send_document(
            chat_id,
            document=_io.BytesIO(pdf_bytes),
            filename=filename,
            caption=f"📖 <b>{manga_title}</b> — Chapter {ch_num}",
            parse_mode=ParseMode.HTML,
        )
        return True
    except Exception as exc:
        logger.error(f"_deliver_chapter_as_pdf error: {exc}")
        return False


# ================================================================================

async def manga_update_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic job: check all tracked manga for new chapters."""
    tracked = MangaTracker.get_all_tracked()
    if not tracked:
        return

    for rec in tracked:
        rec_id, manga_id, manga_title, target_chat_id, lang, last_chapter, _ = rec
        try:
            chapter = MangaDexClient.get_latest_chapter(manga_id, lang)
            if not chapter:
                MangaTracker.update_last_chapter(rec_id, last_chapter or "")
                continue

            attrs = chapter.get("attributes", {}) or {}
            ch_num = attrs.get("chapter")
            ch_id = chapter.get("id", "")

            if not ch_num:
                continue

            if str(ch_num) == str(last_chapter):
                # No new chapter
                continue

            # New chapter found!
            ch_info = MangaDexClient.format_chapter_info(chapter)
            pub_at = attrs.get("publishAt") or ""
            try:
                pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00")).strftime("%d %b %Y %H:%M")
            except Exception:
                pass

            text = (
                b(f"📚 New Chapter Released!") + "\n\n"
                f"<b>Manga:</b> {b(e(manga_title))}\n\n"
                + ch_info + "\n\n"
                + bq(b("Enjoy reading! 🎉"))
            )
            keyboard = [[
                InlineKeyboardButton("📖 Read Now", url=f"https://mangadex.org/chapter/{ch_id}"),
                InlineKeyboardButton("📚 Manga Page", url=f"https://mangadex.org/title/{manga_id}"),
            ]]

            if target_chat_id:
                await safe_send_message(
                    context.bot, target_chat_id, text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )

            MangaTracker.update_last_chapter(rec_id, ch_num)
            await asyncio.sleep(0.5)  # Rate limit

        except Exception as exc:
            logger.debug(f"manga_update_job row {rec_id} error: {exc}")


# ================================================================================
#                         CLEANUP AND LIFECYCLE JOBS
# ================================================================================

async def cleanup_expired_links_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job: clean up expired deep links from database."""
    try:
        cleanup_expired_links()
    except Exception as exc:
        logger.debug(f"cleanup_expired_links_job error: {exc}")


async def post_init(application: Application) -> None:
    """Called after application starts — register commands and start services."""
    global BOT_USERNAME, I_AM_CLONE

    me = await application.bot.get_me()
    BOT_USERNAME = me.username or ""

    try:
        I_AM_CLONE = am_i_a_clone_token(BOT_TOKEN)
    except Exception:
        I_AM_CLONE = False

    if not I_AM_CLONE:
        try:
            set_main_bot_token(BOT_TOKEN)
            logger.info("✅ Main bot token saved to DB")
        except Exception as exc:
            logger.warning(f"Could not save main bot token: {exc}")

    logger.info(f"✅ Bot @{BOT_USERNAME} started as {'CLONE' if I_AM_CLONE else 'MAIN'}")

    # Register commands on this bot
    await _register_bot_commands_on_bot(application.bot)

    # Register commands and start polling for all clone bots
    try:
        clones = get_all_clone_bots(active_only=True)
        for _, token, uname, _, _ in clones:
            try:
                clone_bot = Bot(token=token)
                await _register_bot_commands_on_bot(clone_bot)
                logger.info(f"✅ Commands registered on clone @{uname}")
                # Launch clone as independent Application (non-blocking)
                launch_clone_bot(token, uname)
            except Exception as exc:
                logger.warning(f"Could not start clone @{uname}: {exc}")
    except Exception as exc:
        logger.warning(f"Could not iterate clones: {exc}")

    # Start health check server
    try:
        await health_server.start()
        logger.info("✅ Health check server started")
    except Exception as exc:
        logger.warning(f"Health server failed: {exc}")

    # Schedule jobs
    if application.job_queue:
        application.job_queue.run_repeating(manga_update_job, interval=3600, first=120)
        application.job_queue.run_repeating(cleanup_expired_links_job, interval=600, first=60)
        application.job_queue.run_repeating(check_scheduled_broadcasts, interval=60, first=30)
        logger.info("✅ Background jobs scheduled")

    # Send restart notification
    await _send_restart_notification(application.bot)


async def _register_bot_commands_on_bot(bot: Bot) -> None:
    """Register all commands in Telegram's command menu for a given bot."""
    user_commands = [
        BotCommand("start", "Main menu / Get started"),
        BotCommand("help", "Help and usage guide"),
        BotCommand("ping", "Check bot response time"),
        BotCommand("alive", "Check if bot is online"),
        BotCommand("search", "Search anime, manga, movies"),
        BotCommand("anime", "Generate anime post"),
        BotCommand("manga", "Generate manga post"),
        BotCommand("movie", "Generate movie post"),
        BotCommand("tvshow", "Generate TV show post"),
        BotCommand("id", "Get user/chat IDs"),
        BotCommand("info", "Get user information"),
    ]

    admin_commands = user_commands + [
        BotCommand("stats", "Bot statistics"),
        BotCommand("sysstats", "Server statistics"),
        BotCommand("users", "Total user count"),
        BotCommand("cmd", "Full admin command list"),
        BotCommand("upload", "Upload manager"),
        BotCommand("settings", "Category settings"),
        BotCommand("autoupdate", "Manga auto-update tracker"),
        BotCommand("autoforward", "Auto-forward manager"),
        BotCommand("addchannel", "Add force-sub channel"),
        BotCommand("removechannel", "Remove force-sub channel"),
        BotCommand("channel", "List force-sub channels"),
        BotCommand("banuser", "Ban a user"),
        BotCommand("unbanuser", "Unban a user"),
        BotCommand("listusers", "List all users"),
        BotCommand("deleteuser", "Delete user from database"),
        BotCommand("exportusers", "Export users as CSV"),
        BotCommand("broadcaststats", "Broadcast history"),
        BotCommand("backup", "List generated links"),
        BotCommand("addclone", "Add a clone bot"),
        BotCommand("clones", "List clone bots"),
        BotCommand("reload", "Restart the bot"),
        BotCommand("logs", "View recent logs"),
        BotCommand("connect", "Connect a group"),
        BotCommand("disconnect", "Disconnect a group"),
        BotCommand("connections", "List connected groups"),
    ]

    try:
        await bot.set_my_commands(user_commands)
    except Exception as exc:
        logger.warning(f"Command registration (user) failed: {exc}")
        return
    try:
        await bot.set_my_commands(
            admin_commands,
            scope=BotCommandScopeChat(chat_id=ADMIN_ID),
        )
    except Exception as exc:
        logger.warning(f"Command registration (admin scope) failed: {exc}")
    try:
        me = await bot.get_me()
        logger.info(f"✅ Commands registered on @{me.username}")
    except Exception:
        pass


async def _send_restart_notification(bot: Bot) -> None:
    """Send restart notification to admin on every start (deploy, wake, manual restart)."""
    triggered_by = BOT_USERNAME
    try:
        if os.path.exists("restart_message.json"):
            with open("restart_message.json") as f:
                rinfo = json.load(f)
            triggered_by = rinfo.get("triggered_by", BOT_USERNAME)
            try:
                os.remove("restart_message.json")
            except Exception:
                pass
    except Exception:
        pass

    text = f"<blockquote><b>Bᴏᴛ Rᴇsᴛᴀʀᴛᴇᴅ by @{e(triggered_by)}</b></blockquote>"
    try:
        await bot.send_message(ADMIN_ID, text, parse_mode=ParseMode.HTML)
    except Exception as exc:
        logger.warning(f"Could not send restart notification: {exc}")

async def post_shutdown(application: Application) -> None:
    """Cleanup on bot shutdown."""
    try:
        await health_server.stop()
    except Exception:
        pass
    try:
        if db_manager:
            db_manager.close_all()
    except Exception:
        pass
    logger.info("✅ Shutdown complete.")


# ================================================================================
#                            ERROR HANDLER (USER-FRIENDLY)
# ================================================================================

_error_dm_counts: Dict[Any, int] = {}
ERROR_DM_MAX = 5


async def error_handler(
    update: Optional[Update], context: ContextTypes.DEFAULT_TYPE
) -> None:
    """
    Central error handler.
    - Users get a friendly, non-technical message in DM.
    - Admin gets the full technical traceback via DM.
    - Timeout/ignorable errors are silently skipped.
    """
    err = context.error
    if not err:
        return

    error_logger.error(f"Exception: {err}", exc_info=True)

    # Silently ignore harmless errors
    if UserFriendlyError.is_ignorable(err):
        return

    # ── User gets friendly message ────────────────────────────────────────────────
    if update and update.effective_user:
        uid = update.effective_user.id
        if uid not in (ADMIN_ID, OWNER_ID):
            friendly = UserFriendlyError.get_user_message(err)
            try:
                if update.callback_query:
                    await safe_answer(update.callback_query, "Something went wrong. Please try again.")
                elif update.message:
                    await update.message.reply_text(friendly, parse_mode=ParseMode.HTML)
                elif update.effective_chat:
                    await safe_send_message(context.bot, update.effective_chat.id, friendly)
            except Exception:
                pass

    # ── Admin gets technical message ──────────────────────────────────────────────
    if get_setting("error_dms_enabled", "1") not in ("0", "false"):
        update_key = getattr(update, "update_id", "global") if update else "global"
        count = _error_dm_counts.get(update_key, 0)
        if count < ERROR_DM_MAX:
            _error_dm_counts[update_key] = count + 1
            context_info = ""
            if update:
                if update.effective_user:
                    context_info += f"User: @{update.effective_user.username or update.effective_user.id}\n"
                if update.effective_chat:
                    context_info += f"Chat: {update.effective_chat.id}\n"
                if update.callback_query:
                    context_info += f"Callback: {update.callback_query.data}\n"
                elif update.message and update.message.text:
                    context_info += f"Text: {update.message.text[:100]}\n"
            admin_msg = UserFriendlyError.get_admin_message(err, context_info)
            try:
                await context.bot.send_message(
                    ADMIN_ID, admin_msg, parse_mode=ParseMode.HTML
                )
            except Exception:
                pass


# ================================================================================
#              BUTTON HANDLER (CENTRAL ROUTER — EXHAUSTIVE)
# ================================================================================

@force_sub_required
async def button_handler(
    update: Update, context: ContextTypes.DEFAULT_TYPE, _data_override: str = None
) -> None:
    """
    Central callback query router.
    Answers every query immediately to prevent timeout errors.
    All callbacks are handled exhaustively.
    Accepts _data_override to allow internal re-routing without modifying
    the read-only query.data attribute.
    """
    query = update.callback_query
    if not query:
        return

    # Answer only on real calls, not on internal re-routes
    if _data_override is None:
        await safe_answer(query)

    data = _data_override if _data_override is not None else (query.data or "")
    uid = query.from_user.id if query.from_user else 0
    chat_id = query.message.chat_id if query.message else uid

    is_admin = uid in (ADMIN_ID, OWNER_ID)

    # ── Utility ────────────────────────────────────────────────────────────────────
    if data == "noop":
        return

    if data == "close_message":
        try:
            await query.delete_message()
        except Exception:
            pass
        return

    # ── Image navigation (edit_message_media, no new message) ──────────────────────
    if data.startswith("imgn:"):
        try:
            parts = data.split(":", 3)
            # Format: imgn:{current_idx}:{img_key}:{direction}
            if len(parts) == 4:
                _, cur_idx_str, img_key, direction = parts
                cur_idx = int(cur_idx_str)
                entry = _cache_get(img_key)
                # Support both old list format and new dict format
                if isinstance(entry, list):
                    images = entry
                    saved_caption = ""
                    shown_set: set = set()
                elif isinstance(entry, dict):
                    images = entry.get("urls", [])
                    saved_caption = entry.get("caption", "")
                    shown_set = entry.get("shown", set())
                else:
                    images = []
                    saved_caption = ""
                    shown_set = set()

                if images and len(images) > 1:
                    await safe_answer(query, "Loading...")
                    # Find next unshown index to avoid repeats
                    step = 1 if direction == "next" else -1
                    candidate = (cur_idx + step) % len(images)
                    # Try up to len(images) steps to find an unshown image
                    attempts = 0
                    while candidate in shown_set and attempts < len(images):
                        candidate = (candidate + step) % len(images)
                        attempts += 1
                    # If all shown, reset and start fresh
                    if attempts >= len(images):
                        shown_set = set()
                    new_idx = candidate
                    shown_set.add(new_idx)
                    # Update shown set in cache
                    if isinstance(entry, dict):
                        entry["shown"] = shown_set
                        _cache_set(img_key, entry)
                    new_url = images[new_idx]
                    # Rebuild navigation keyboard with updated index
                    new_kb = [
                        [InlineKeyboardButton("◀", callback_data=f"imgn:{new_idx}:{img_key}:prev"),
                         InlineKeyboardButton("✕", callback_data="close_message"),
                         InlineKeyboardButton("▶", callback_data=f"imgn:{new_idx}:{img_key}:next")],
                    ]
                    # Preserve existing top rows from the current keyboard (except last nav row)
                    if query.message and query.message.reply_markup:
                        old_rows = list(query.message.reply_markup.inline_keyboard)
                        top_rows = old_rows[:-1] if old_rows else []
                        new_kb = top_rows + new_kb
                    try:
                        # Use saved_caption to keep info text on image change
                        if saved_caption:
                            await query.message.edit_media(
                                InputMediaPhoto(
                                    media=new_url,
                                    caption=saved_caption,
                                    parse_mode=ParseMode.HTML,
                                ),
                                reply_markup=InlineKeyboardMarkup(new_kb),
                            )
                        else:
                            await query.message.edit_media(
                                InputMediaPhoto(media=new_url),
                                reply_markup=InlineKeyboardMarkup(new_kb),
                            )
                    except Exception as exc:
                        logger.debug(f"imgn edit_media error: {exc}")
                else:
                    await safe_answer(query, "No more images available.")
        except Exception as exc:
            logger.debug(f"imgn handler error: {exc}")
        return

    if data == "verify_subscription":
        # Re-trigger start to recheck subscription
        await start(update, context)
        return

    # ── Admin back to main panel ───────────────────────────────────────────────────
    if data == "admin_back":
        if not is_admin:
            return
        await delete_bot_prompt(context, chat_id)
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context, query)
        return

    # ── User about/help ────────────────────────────────────────────────────────────
    if data == "about_bot":
        try:
            await query.delete_message()
        except Exception:
            pass
        text = (
            b(f"ℹ️ About {e(BOT_NAME)}") + "\n\n"
            + bq(
                b("🤖 Powered by @Beat_Anime_Ocean\n\n")
                + b("Features:\n")
                + "• Force-Sub channels\n"
                + "• Anime/Manga/Movie posts\n"
                + "• Deep link generation\n"
                + "• Auto-forward system\n"
                + "• Clone bot support\n"
                + "• Broadcast manager\n"
                + "• Manga chapter tracker\n"
                + "• Upload manager"
            )
        )
        await safe_send_message(
            context.bot, chat_id, text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🎌 Anime Channel", url=PUBLIC_ANIME_CHANNEL_URL)],
                [bold_button("🔙 Back", callback_data="user_back")],
            ]),
        )
        return

    if data == "user_back":
        try:
            await query.delete_message()
        except Exception:
            pass
        await start(update, context)
        return

    if data == "user_help":
        await help_command(update, context)
        return

    # ── Admin stats ────────────────────────────────────────────────────────────────
    if data == "admin_stats":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await send_stats_panel(context, chat_id)
        return

    if data == "broadcast_stats_panel":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await broadcaststats_command(update, context)
        return

    # ── System stats ───────────────────────────────────────────────────────────────
    if data == "admin_sysstats":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await safe_send_message(
            context.bot, chat_id,
            get_system_stats_text(),
            reply_markup=InlineKeyboardMarkup([
                [bold_button("♻️ Refresh", callback_data="admin_sysstats"),
                 bold_button("🔙 BACK", callback_data="admin_back")]
            ]),
        )
        return

    # ── Admin logs ─────────────────────────────────────────────────────────────────
    if data == "admin_logs":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await logs_command(update, context)
        return

    # ── Admin restart ──────────────────────────────────────────────────────────────
    if data == "admin_restart_confirm":
        if not is_admin:
            return
        await safe_edit_text(
            query,
            b("⚠️ Restart Bot?") + "\n\n" + bq(b("This will restart the bot. All conversations will be reset.")),
            reply_markup=InlineKeyboardMarkup([
                [bold_button("✅ Yes, Restart", callback_data="admin_do_restart"),
                 bold_button("❌ Cancel", callback_data="admin_back")],
            ]),
        )
        return

    if data == "admin_do_restart":
        if not is_admin:
            return
        await safe_answer(query, "Restarting…")
        await reload_command(update, context)
        return

    # ── Broadcast ──────────────────────────────────────────────────────────────────
    if data == "admin_broadcast_start":
        if not is_admin:
            return
        user_states[uid] = PENDING_BROADCAST
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await safe_send_message(
            context.bot, chat_id,
            b("📣 Broadcast") + "\n\n"
            + bq(b("Send the message you want to broadcast to all users.\n\n")
                 + b("Supports: text, photos, videos, documents, stickers.")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
        )
        await store_bot_prompt(context, msg)
        return

    if data.startswith("broadcast_mode_"):
        if not is_admin:
            return
        mode = data[len("broadcast_mode_"):]
        context.user_data["broadcast_mode"] = mode
        msg_data = context.user_data.get("broadcast_message")
        if not msg_data:
            await safe_edit_text(query, b("❌ Broadcast message lost. Please start over."))
            user_states.pop(uid, None)
            return
        await safe_edit_text(
            query,
            b(f"Mode selected: {e(mode)}") + "\n\n"
            + bq(b("Send /confirm to start broadcasting\nor /cancel to abort.")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
        )
        user_states[uid] = PENDING_BROADCAST_CONFIRM
        return

    if data == "broadcast_schedule":
        if not is_admin:
            return
        user_states[uid] = SCHEDULE_BROADCAST_DATETIME
        await safe_edit_text(
            query,
            b("📅 Schedule Broadcast") + "\n\n"
            + bq(b("Send the date and time for the broadcast:\n")
                 + b("Format: YYYY-MM-DD HH:MM (UTC)")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
        )
        return

    # ── Force-sub channel management ───────────────────────────────────────────────
    if data == "manage_force_sub":
        if not is_admin:
            return
        await delete_bot_prompt(context, chat_id)
        user_states.pop(uid, None)
        channels = get_all_force_sub_channels(return_usernames_only=False)
        text = b(f"📢 Force-Sub Channels ({len(channels)}):") + "\n\n"
        if channels:
            for uname, title, jbr in channels:
                jbr_tag = " (JBR)" if jbr else ""
                text += f"• {b(e(title))}\n  {e(uname)}{jbr_tag}\n\n"
        else:
            text += b("None configured yet.")
        keyboard = [
            [bold_button("➕ Add Channel", callback_data="fs_add_channel"),
             bold_button("🗑 Remove Channel", callback_data="fs_remove_channel")],
            [bold_button("🔗 Generate Link", callback_data="generate_links")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "fs_add_channel":
        if not is_admin:
            return
        user_states[uid] = ADD_CHANNEL_USERNAME
        await safe_edit_text(
            query,
            b("➕ Add Force-Sub Channel") + "\n\n"
            + bq(b("Send the channel @username (e.g., @mychannel)")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="manage_force_sub")]]),
        )
        return

    if data == "fs_remove_channel":
        if not is_admin:
            return
        channels = get_all_force_sub_channels(return_usernames_only=False)
        if not channels:
            await safe_answer(query, "No channels to remove.")
            return
        keyboard = []
        for uname, title, _ in channels:
            keyboard.append([bold_button(f"🗑 {title[:25]}", callback_data=f"fs_del_{uname}")])
        keyboard.append([bold_button("🔙 Back", callback_data="manage_force_sub")])
        await safe_edit_text(
            query, b("Select channel to remove:"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("fs_del_"):
        if not is_admin:
            return
        uname = data[len("fs_del_"):]
        delete_force_sub_channel(uname)
        await safe_answer(query, f"Removed {uname}")
        await button_handler(update, context, "manage_force_sub")
        return

    # ── Link generation ────────────────────────────────────────────────────────────
    if data == "generate_links":
        if not is_admin:
            return
        user_states[uid] = GENERATE_LINK_IDENTIFIER
        await safe_edit_text(
            query,
            b("🔗 Generate Deep Link") + "\n\n"
            + bq(b("Send the channel @username or channel ID to generate a link for.")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
        )
        return

    if data == "admin_show_links":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await backup_command(update, context)
        return

    # ── Clone bot management ────────────────────────────────────────────────────────
    if data == "manage_clones":
        if not is_admin:
            return
        await delete_bot_prompt(context, chat_id)
        user_states.pop(uid, None)
        clones = get_all_clone_bots(active_only=True)
        text = b(f"🤖 Clone Bots ({len(clones)}):") + "\n\n"
        if clones:
            for cid, token, uname, active, added in clones:
                text += f"• @{e(uname)} — Added: {str(added)[:10]}\n"
        else:
            text += b("No clone bots registered.")
        keyboard = [
            [bold_button("➕ Add Clone", callback_data="clone_add"),
             bold_button("🗑 Remove Clone", callback_data="clone_remove")],
            [bold_button("♻️ Refresh Commands on Clones", callback_data="clone_refresh_cmds")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "clone_add":
        if not is_admin:
            return
        user_states[uid] = ADD_CLONE_TOKEN
        await safe_edit_text(
            query,
            b("🤖 Add Clone Bot") + "\n\n"
            + bq(b("Send the BOT TOKEN of the clone bot.\n⚠️ Keep tokens secret!")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="manage_clones")]]),
        )
        return

    if data == "clone_remove":
        if not is_admin:
            return
        clones = get_all_clone_bots(active_only=True)
        if not clones:
            await safe_answer(query, "No clones to remove.")
            return
        keyboard = []
        for cid, token, uname, active, added in clones:
            keyboard.append([bold_button(f"🗑 @{uname}", callback_data=f"clone_del_{uname}")])
        keyboard.append([bold_button("🔙 Back", callback_data="manage_clones")])
        await safe_edit_text(
            query, b("Select clone bot to remove:"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("clone_del_"):
        if not is_admin:
            return
        uname = data[len("clone_del_"):]
        remove_clone_bot(uname)
        await safe_answer(query, f"Removed @{uname}")
        await button_handler(update, context, "manage_clones")
        return

    if data == "clone_refresh_cmds":
        if not is_admin:
            return
        clones = get_all_clone_bots(active_only=True)
        if not clones:
            await safe_answer(query, "No clone bots found.")
            return
        count = 0
        for _, token, uname, _, _ in clones:
            try:
                clone_bot = Bot(token=token)
                await _register_bot_commands_on_bot(clone_bot)
                count += 1
            except Exception:
                pass
        await safe_answer(query, f"Commands refreshed on {count} clone(s).")
        await button_handler(update, context, "manage_clones")
        return

    # ── Admin settings ─────────────────────────────────────────────────────────────
    if data == "admin_settings":
        if not is_admin:
            return
        maint = get_setting("maintenance_mode", "false")
        clone_red = get_setting("clone_redirect_enabled", "false")
        backup_url = get_setting("backup_channel_url", "Not set")
        text = (
            b("⚙️ Bot Settings") + "\n\n"
            f"<b>🔧 Maintenance:</b> {'🔴 ON' if maint == 'true' else '🟢 OFF'}\n"
            f"<b>🔀 Clone Redirect:</b> {'✅ ON' if clone_red == 'true' else '❌ OFF'}\n"
            f"<b>📢 Backup Channel:</b> {code(e(backup_url[:50]))}\n"
            f"<b>⏱ Link Expiry:</b> {code(str(LINK_EXPIRY_MINUTES) + ' min')}"
        )
        keyboard = [
            [bold_button("🔧 Toggle Maintenance", callback_data="toggle_maintenance"),
             bold_button("🔀 Toggle Clone Redirect", callback_data="toggle_clone_redirect")],
            [bold_button("📢 Set Backup Channel", callback_data="set_backup_channel")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "toggle_maintenance":
        if not is_admin:
            return
        current = get_setting("maintenance_mode", "false")
        new_val = "false" if current == "true" else "true"
        set_setting("maintenance_mode", new_val)
        await safe_answer(query, f"Maintenance {'ON' if new_val == 'true' else 'OFF'}")
        await button_handler(update, context, "admin_settings")
        return

    if data == "toggle_clone_redirect":
        if not is_admin:
            return
        current = get_setting("clone_redirect_enabled", "false")
        new_val = "false" if current == "true" else "true"
        set_setting("clone_redirect_enabled", new_val)
        await safe_answer(query, f"Clone redirect {'ON' if new_val == 'true' else 'OFF'}")
        await button_handler(update, context, "admin_settings")
        return

    if data == "set_backup_channel":
        if not is_admin:
            return
        user_states[uid] = SET_BACKUP_CHANNEL
        await safe_edit_text(
            query,
            b("📢 Set Backup Channel URL") + "\n\n"
            + bq(b("Send the backup channel URL (e.g., https://t.me/backup_channel)")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_settings")]]),
        )
        return

    # ── Feature flags ──────────────────────────────────────────────────────────────
    if data == "admin_feature_flags":
        if not is_admin:
            return
        await delete_bot_prompt(context, chat_id)
        user_states.pop(uid, None)
        await send_feature_flags_panel(context, chat_id, query)
        return

    if data.startswith("flag_toggle_"):
        if not is_admin:
            return
        parts = data[len("flag_toggle_"):].rsplit("_", 1)
        if len(parts) == 2:
            flag_key, new_val = parts
            set_setting(flag_key, new_val)
            is_on = new_val in ("true", "1")
            await safe_answer(query, f"{'Enabled' if is_on else 'Disabled'}!")
            await send_feature_flags_panel(context, chat_id, query)
        return

    # ── Filter settings panel ───────────────────────────────────────────────────────
    if data == "admin_filter_settings":
        if not is_admin:
            return
        dm_on = filters_config["global"].get("dm", True)
        grp_on = filters_config["global"].get("group", True)
        text = (
            b("Filter Settings") + "\n\n"
            f"<b>DM:</b> {'ON' if dm_on else 'OFF'}\n"
            f"<b>GROUP:</b> {'ON' if grp_on else 'OFF'}"
        )
        keyboard = [
            [bold_button("TOGGLE DM", callback_data="filter_toggle_dm")],
            [bold_button("TOGGLE GROUP", callback_data="filter_toggle_group")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "filter_toggle_dm":
        if not is_admin:
            return
        filters_config["global"]["dm"] = not filters_config["global"].get("dm", True)
        state = "ON" if filters_config["global"]["dm"] else "OFF"
        await safe_answer(query, f"DM filter: {state}")
        await button_handler(update, context, "admin_filter_settings")
        return

    if data == "filter_toggle_group":
        if not is_admin:
            return
        filters_config["global"]["group"] = not filters_config["global"].get("group", True)
        state = "ON" if filters_config["global"]["group"] else "OFF"
        await safe_answer(query, f"Group filter: {state}")
        await button_handler(update, context, "admin_filter_settings")
        return

    # ── Category settings ──────────────────────────────────────────────────────────
    if data == "admin_category_settings":
        if not is_admin:
            return
        # Spec-compliant START PANEL / POST SETTING layout
        keyboard = [
            [bold_button("TV SHOWS", callback_data="admin_category_settings_tvshow"),
             bold_button("MOVIES", callback_data="admin_category_settings_movie")],
            [bold_button("ANIME", callback_data="admin_category_settings_anime"),
             bold_button("MANGA", callback_data="admin_category_settings_manga")],
            [bold_button("POST SETTING", callback_data="admin_settings")],
            [bold_button("AUTO FORWARD", callback_data="admin_autoforward"),
             bold_button("POST SEARCH", callback_data="admin_cmd_list")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(
            query, b("Choose the category"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    for cat_name in ("anime", "manga", "movie", "tvshow"):
        if data == f"admin_category_settings_{cat_name}":
            if not is_admin:
                return
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        if data == f"settings_category_{cat_name}":
            if not is_admin:
                return
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Caption
        if data == f"cat_caption_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_CATEGORY_CAPTION
            context.user_data["editing_category"] = cat_name
            placeholders = (
                "{title}, {status}, {type}, {episodes}, {score}, {genres}, {synopsis}, {studio}, {season}, "
                "{chapters}, {volumes}, {popularity}, {release_date}, {rating}, {overview}, {runtime}, "
                "{director}, {cast}, {network}, {name}"
            )
            await safe_edit_text(
                query,
                b(f"📝 Set Caption Template for {e(cat_name.upper())}") + "\n\n"
                + bq(b("Send the caption template text.\n\n") + b("Available placeholders:\n") + e(placeholders)),
                reply_markup=InlineKeyboardMarkup([[
                    bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}")
                ]]),
            )
            return

        # Branding
        if data == f"cat_branding_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_CATEGORY_BRANDING
            context.user_data["editing_category"] = cat_name
            current = get_category_settings(cat_name).get("branding", "")
            await safe_edit_text(
                query,
                b(f"🏷 Set Branding for {e(cat_name.upper())}") + "\n\n"
                + bq(b("Send your branding text (appended at the bottom of posts).\n\n")
                     + b("Current: ") + code(e(current[:100] if current else "None"))),
                reply_markup=InlineKeyboardMarkup([[
                    bold_button("🗑 Clear Branding", callback_data=f"cat_brand_clear_{cat_name}"),
                    bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}"),
                ]]),
            )
            return

        if data == f"cat_brand_clear_{cat_name}":
            if not is_admin:
                return
            update_category_field(cat_name, "branding", "")
            await safe_answer(query, "Branding cleared.")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Buttons
        if data == f"cat_buttons_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_CATEGORY_BUTTONS
            context.user_data["editing_category"] = cat_name
            await safe_edit_text(
                query,
                b(f"🔘 Configure Buttons for {e(cat_name.upper())}") + "\n\n"
                + bq(
                    b("Send button config, one per line:\n")
                    + b("Format: Button Text - https://url\n\n")
                    + b("Color prefixes:\n")
                    + b("#g Text - url → 🟢\n")
                    + b("#r Text - url → 🔴\n")
                    + b("#b Text - url → 🔵\n")
                    + b("#y Text - url → 🟡")
                ),
                reply_markup=InlineKeyboardMarkup([
                    [bold_button("🗑 Clear Buttons", callback_data=f"cat_btns_clear_{cat_name}")],
                    [bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}")],
                ]),
            )
            return

        if data == f"cat_btns_clear_{cat_name}":
            if not is_admin:
                return
            update_category_field(cat_name, "buttons", "[]")
            await safe_answer(query, "Buttons cleared.")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Thumbnail
        if data == f"cat_thumbnail_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_CATEGORY_THUMBNAIL
            context.user_data["editing_category"] = cat_name
            await safe_edit_text(
                query,
                b(f"🖼 Set Thumbnail for {e(cat_name.upper())}") + "\n\n"
                + bq(b("Send the thumbnail URL, or send 'default' to reset.")),
                reply_markup=InlineKeyboardMarkup([[
                    bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}")
                ]]),
            )
            return

        # Font
        if data == f"cat_font_{cat_name}":
            if not is_admin:
                return
            await safe_edit_text(
                query,
                b(f"🔤 Font Style for {e(cat_name.upper())}"),
                reply_markup=InlineKeyboardMarkup([
                    [bold_button("Normal", callback_data=f"cat_font_set_{cat_name}_normal"),
                     bold_button("Small Caps", callback_data=f"cat_font_set_{cat_name}_smallcaps")],
                    [bold_button("🔙 Back", callback_data=f"admin_category_settings_{cat_name}")],
                ]),
            )
            return

        if data.startswith(f"cat_font_set_{cat_name}_"):
            if not is_admin:
                return
            font_val = data[len(f"cat_font_set_{cat_name}_"):]
            update_category_field(cat_name, "font_style", font_val)
            await safe_answer(query, f"Font set to {font_val}")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Watermark
        if data == f"cat_watermark_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_WATERMARK_TEXT
            context.user_data["editing_category"] = cat_name
            current = get_category_settings(cat_name).get("watermark_text", "")
            await safe_edit_text(
                query,
                b(f"💧 Set Watermark for {e(cat_name.upper())}") + "\n\n"
                + bq(b("Send the watermark text to stamp on images.\n\n")
                     + b("Current: ") + code(e(current[:50] if current else "None"))),
                reply_markup=InlineKeyboardMarkup([
                    [bold_button("🗑 Remove Watermark", callback_data=f"cat_wm_clear_{cat_name}"),
                     bold_button("📌 Set Position", callback_data=f"cat_wm_pos_{cat_name}")],
                    [bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}")],
                ]),
            )
            return

        if data == f"cat_wm_clear_{cat_name}":
            if not is_admin:
                return
            update_category_field(cat_name, "watermark_text", None)
            await safe_answer(query, "Watermark removed.")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        if data == f"cat_wm_pos_{cat_name}":
            if not is_admin:
                return
            positions = ["center", "top", "bottom", "left", "right", "bottom-left", "bottom-right"]
            keyboard = []
            row = []
            for pos in positions:
                row.append(bold_button(pos.title(), callback_data=f"cat_wm_pos_set_{cat_name}_{pos}"))
                if len(row) == 3:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            keyboard.append([bold_button("🔙 Back", callback_data=f"admin_category_settings_{cat_name}")])
            await safe_edit_text(
                query, b(f"Select watermark position for {e(cat_name)}:"),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if data.startswith(f"cat_wm_pos_set_{cat_name}_"):
            if not is_admin:
                return
            pos = data[len(f"cat_wm_pos_set_{cat_name}_"):]
            update_category_field(cat_name, "watermark_position", pos)
            await safe_answer(query, f"Position set to {pos}")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Logo
        if data == f"cat_logo_{cat_name}":
            if not is_admin:
                return
            user_states[uid] = SET_CATEGORY_LOGO
            context.user_data["editing_category"] = cat_name
            await safe_edit_text(
                query,
                b(f"🖼 Set Logo for {e(cat_name.upper())}") + "\n\n"
                + bq(b("Send a photo or image document to use as logo.")),
                reply_markup=InlineKeyboardMarkup([[
                    bold_button("🗑 Remove Logo", callback_data=f"cat_logo_clear_{cat_name}"),
                    bold_button("🔙 Cancel", callback_data=f"admin_category_settings_{cat_name}"),
                ]]),
            )
            return

        if data == f"cat_logo_clear_{cat_name}":
            if not is_admin:
                return
            update_category_field(cat_name, "logo_file_id", None)
            await safe_answer(query, "Logo removed.")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Logo position
        if data == f"cat_logopos_{cat_name}":
            if not is_admin:
                return
            positions = ["top", "bottom", "left", "right", "center"]
            keyboard = [
                [bold_button(pos.title(), callback_data=f"cat_logo_pos_set_{cat_name}_{pos}")
                 for pos in positions[:3]],
                [bold_button(pos.title(), callback_data=f"cat_logo_pos_set_{cat_name}_{pos}")
                 for pos in positions[3:]],
                [bold_button("🔙 Back", callback_data=f"admin_category_settings_{cat_name}")],
            ]
            await safe_edit_text(
                query, b(f"Select logo position for {e(cat_name)}:"),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if data.startswith(f"cat_logo_pos_set_{cat_name}_"):
            if not is_admin:
                return
            pos = data[len(f"cat_logo_pos_set_{cat_name}_"):]
            update_category_field(cat_name, "logo_position", pos)
            await safe_answer(query, f"Logo position: {pos}")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

        # Reset defaults
        if data == f"cat_reset_{cat_name}":
            if not is_admin:
                return
            await safe_edit_text(
                query,
                b(f"⚠️ Reset {e(cat_name.upper())} settings to defaults?"),
                reply_markup=InlineKeyboardMarkup([
                    [bold_button("✅ Yes, Reset", callback_data=f"cat_reset_confirm_{cat_name}"),
                     bold_button("❌ Cancel", callback_data=f"admin_category_settings_{cat_name}")],
                ]),
            )
            return

        if data == f"cat_reset_confirm_{cat_name}":
            if not is_admin:
                return
            try:
                with db_manager.get_cursor() as cur:
                    cur.execute(
                        "UPDATE category_settings SET "
                        "caption_template = '', branding = '', buttons = '[]', "
                        "thumbnail_url = '', font_style = 'normal', "
                        "logo_file_id = NULL, watermark_text = NULL "
                        "WHERE category = %s",
                        (cat_name,)
                    )
            except Exception:
                pass
            await safe_answer(query, f"{cat_name} settings reset.")
            await show_category_settings_menu(context, chat_id, cat_name, query)
            return

    # ── User management ─────────────────────────────────────────────────────────────
    if data == "user_management":
        if not is_admin:
            return
        await delete_bot_prompt(context, chat_id)
        user_states.pop(uid, None)
        total = get_user_count()
        keyboard = [
            [bold_button("👥 List Users", callback_data="um_list_users"),
             bold_button("🔍 Search User", callback_data="um_search_user")],
            [bold_button("🚫 Ban User", callback_data="um_ban_user"),
             bold_button("✅ Unban User", callback_data="um_unban_user")],
            [bold_button("🗑 Delete User", callback_data="um_delete_user"),
             bold_button("📤 Export CSV", callback_data="um_export_csv")],
            [bold_button("📊 Banned Users", callback_data="um_banned_list")],
            [bold_button("🔙 BACK", callback_data="admin_back")],
        ]
        await safe_edit_text(
            query,
            b("👤 User Management") + "\n\n"
            f"<b>Total Users:</b> {code(format_number(total))}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data == "um_list_users":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        context.args = []
        await listusers_command(update, context)
        return

    if data == "um_export_csv":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await exportusers_command(update, context)
        return

    if data == "um_search_user":
        if not is_admin:
            return
        user_states[uid] = SEARCH_USER_INPUT
        await safe_edit_text(
            query,
            b("🔍 Search User") + "\n\n" + bq(b("Send user ID or @username:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="user_management")]]),
        )
        return

    if data == "um_ban_user":
        if not is_admin:
            return
        user_states[uid] = BAN_USER_INPUT
        await safe_edit_text(
            query,
            b("🚫 Ban User") + "\n\n" + bq(b("Send user ID or @username to ban:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="user_management")]]),
        )
        return

    if data == "um_unban_user":
        if not is_admin:
            return
        user_states[uid] = UNBAN_USER_INPUT
        await safe_edit_text(
            query,
            b("✅ Unban User") + "\n\n" + bq(b("Send user ID or @username to unban:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="user_management")]]),
        )
        return

    if data == "um_delete_user":
        if not is_admin:
            return
        user_states[uid] = DELETE_USER_INPUT
        await safe_edit_text(
            query,
            b("🗑 Delete User") + "\n\n" + bq(b("Send the user ID to permanently delete from database:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="user_management")]]),
        )
        return

    if data == "um_banned_list":
        if not is_admin:
            return
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    "SELECT user_id, username, first_name FROM users WHERE banned = TRUE LIMIT 20"
                )
                banned = cur.fetchall() or []
        except Exception:
            banned = []
        if not banned:
            await safe_answer(query, "No banned users.")
            return
        text = b(f"🚫 Banned Users ({len(banned)}):") + "\n\n"
        for buid, buname, bfname in banned:
            text += f"• {e(bfname or '')} @{e(buname or '')} {code(str(buid))}\n"
        keyboard = [[bold_button("🔙 Back", callback_data="user_management")]]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("user_page_"):
        if not is_admin:
            return
        offset = int(data[len("user_page_"):])
        try:
            await query.delete_message()
        except Exception:
            pass
        context.args = [str(offset)]
        await listusers_command(update, context)
        return

    if data.startswith("manage_user_"):
        if not is_admin:
            return
        target_uid = int(data[len("manage_user_"):])
        user_info = get_user_info_by_id(target_uid)
        if not user_info:
            await safe_answer(query, "User not found.")
            return
        u_id, u_uname, u_fname, u_lname, u_joined, u_banned = user_info
        name = f"{u_fname or ''} {u_lname or ''}".strip() or "N/A"
        text = (
            b("👤 User Details") + "\n\n"
            f"<b>ID:</b> {code(str(u_id))}\n"
            f"<b>Name:</b> {e(name)}\n"
            f"<b>Username:</b> {'@' + e(u_uname) if u_uname else '—'}\n"
            f"<b>Joined:</b> {code(str(u_joined)[:16])}\n"
            f"<b>Status:</b> {'🚫 Banned' if u_banned else '✅ Active'}"
        )
        keyboard = []
        if u_banned:
            keyboard.append([bold_button("✅ Unban", callback_data=f"user_unban_{u_id}")])
        else:
            keyboard.append([bold_button("🚫 Ban", callback_data=f"user_ban_{u_id}")])
        keyboard.append([bold_button("🗑 Delete", callback_data=f"user_del_{u_id}")])
        keyboard.append([bold_button("🔙 Back", callback_data="user_management")])
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("user_ban_"):
        if not is_admin:
            return
        target_uid = int(data[len("user_ban_"):])
        if target_uid in (ADMIN_ID, OWNER_ID):
            await safe_answer(query, "Cannot ban admin.")
            return
        ban_user(target_uid)
        await safe_answer(query, "User banned.")
        await button_handler(update, context, f"manage_user_{target_uid}")

        return

    if data.startswith("user_unban_"):
        if not is_admin:
            return
        target_uid = int(data[len("user_unban_"):])
        unban_user(target_uid)
        await safe_answer(query, "User unbanned.")
        await button_handler(update, context, f"manage_user_{target_uid}")

        return

    if data.startswith("user_del_"):
        if not is_admin:
            return
        target_uid = int(data[len("user_del_"):])
        if target_uid in (ADMIN_ID, OWNER_ID):
            await safe_answer(query, "Cannot delete admin.", show_alert=True)
            return
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("DELETE FROM users WHERE user_id = %s", (target_uid,))
        except Exception:
            await safe_answer(query, "Error deleting user.")
            return
        await safe_answer(query, "User deleted.")
        await button_handler(update, context, "user_management")

        return

    # ── Search results ─────────────────────────────────────────────────────────────
    if data.startswith("search_result_"):
        rest = data[len("search_result_"):]
        for cat_key in ("mangadex", "anime", "manga", "movie", "tvshow"):
            prefix = f"{cat_key}_"
            if rest.startswith(prefix):
                raw_id = rest[len(prefix):]
                try:
                    await query.delete_message()
                except Exception:
                    pass
                if cat_key == "mangadex":
                    # Show MangaDex manga details
                    manga = MangaDexClient.get_manga(raw_id)
                    if manga:
                        caption_text, cover_url = MangaDexClient.format_manga_info(manga)
                        # Chapter list keyboard
                        chapters, total_chs = MangaDexClient.get_chapters(raw_id, limit=5)
                        ch_keyboard = []
                        for ch in chapters:
                            attrs = ch.get("attributes", {}) or {}
                            ch_num = attrs.get("chapter", "?")
                            ch_keyboard.append([bold_button(
                                f"Ch.{ch_num}",
                                callback_data=f"mdex_chapter_{ch['id']}"
                            )])
                        ch_keyboard.append([
                            InlineKeyboardButton("📖 Read on MangaDex", url=f"https://mangadex.org/title/{raw_id}"),
                        ])
                        ch_keyboard.append([bold_button("📚 Track This Manga", callback_data=f"mdex_track_{raw_id}")])
                        markup = InlineKeyboardMarkup(ch_keyboard)
                        if cover_url:
                            await safe_send_photo(
                                context.bot, chat_id,
                                cover_url, caption=caption_text, reply_markup=markup,
                            )
                        else:
                            await safe_send_message(context.bot, chat_id, caption_text, reply_markup=markup)
                    else:
                        await safe_send_message(context.bot, chat_id, b("❌ Manga not found."))
                else:
                    try:
                        mid = int(raw_id)
                    except ValueError:
                        mid = None
                    await generate_and_send_post(
                        context, chat_id, cat_key,
                        media_id=mid,
                    )
                return

    # MangaDex chapter viewer
    if data.startswith("mdex_chapter_"):
        ch_id = data[len("mdex_chapter_"):]
        try:
            await query.delete_message()
        except Exception:
            pass
        # Show chapter info
        pages = MangaDexClient.get_chapter_pages(ch_id)
        text = b("📖 Chapter") + "\n\n"
        if pages:
            base_url, ch_hash, filenames = pages
            text += (
                f"<b>Total Pages:</b> {code(str(len(filenames)))}\n"
                f"<b>Chapter ID:</b> {code(ch_id)}\n\n"
                + bq(b("Read this chapter online at MangaDex for the best experience."))
            )
        else:
            text += b("Could not load chapter page info.")
        await safe_send_message(
            context.bot, chat_id, text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📖 Read Now", url=f"https://mangadex.org/chapter/{ch_id}")
            ]]),
        )
        return

    # MangaDex track
    if data.startswith("mdex_track_"):
        if not is_admin:
            await safe_answer(query, "Only admin can set up tracking.")
            return
        manga_id = data[len("mdex_track_"):]
        manga = MangaDexClient.get_manga(manga_id)
        if not manga:
            await safe_answer(query, "Manga not found. Try searching again.")
            return
        attrs = manga.get("attributes", {}) or {}
        titles = attrs.get("title", {}) or {}
        title = titles.get("en") or next(iter(titles.values()), "Unknown")
        status = (attrs.get("status") or "unknown").replace("_", " ").title()
        context.user_data["au_manga_id"] = manga_id
        context.user_data["au_manga_title"] = title
        context.user_data["au_manga_status"] = status
        # Step 1: Ask for delivery mode
        keyboard = [
            [bold_button("Full Manga", callback_data="au_mode_full"),
             bold_button("Latest Chapters", callback_data="au_mode_latest")],
            [bold_button("🔙 Cancel", callback_data="admin_autoupdate")],
        ]
        await safe_edit_text(
            query,
            b(f"📚 {e(title)}") + "\n\n"
            + bq(b("Choose delivery mode:\n\n")
                 + "Full Manga — send all chapters from beginning\n"
                 + "Latest Chapters — only send new chapters as they release"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data in ("au_mode_full", "au_mode_latest"):
        if not is_admin:
            return
        mode = "full" if data == "au_mode_full" else "latest"
        context.user_data["au_manga_mode"] = mode
        title = context.user_data.get("au_manga_title", "Unknown")
        # Step 2: Ask for interval
        keyboard = [
            [bold_button("5 min", callback_data="au_interval_5"),
             bold_button("10 min", callback_data="au_interval_10")],
            [bold_button("Random (5-10 min)", callback_data="au_interval_random"),
             bold_button("Custom", callback_data="au_interval_custom")],
            [bold_button("🔙 Cancel", callback_data="admin_autoupdate")],
        ]
        await safe_edit_text(
            query,
            b(f"📚 {e(title)}") + f"\n<b>Mode:</b> {mode.title()}\n\n"
            + bq(b("Choose check interval:")),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("au_interval_"):
        if not is_admin:
            return
        interval_key = data[len("au_interval_"):]
        if interval_key == "5":
            interval_minutes = 5
        elif interval_key == "10":
            interval_minutes = 10
        elif interval_key == "random":
            interval_minutes = -1  # -1 = random 5–10
        elif interval_key == "custom":
            context.user_data["au_waiting_for_interval"] = True
            user_states[uid] = AU_CUSTOM_INTERVAL
            await safe_edit_text(
                query,
                b("📚 Custom Interval") + "\n\n"
                + bq(b("Send interval in minutes (e.g. 15):")),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoupdate")]]),
            )
            return
        else:
            interval_minutes = 10
        context.user_data["au_manga_interval"] = interval_minutes
        title = context.user_data.get("au_manga_title", "Unknown")
        mode = context.user_data.get("au_manga_mode", "latest")
        # Step 3: Ask for target channel
        user_states[uid] = AU_ADD_MANGA_TARGET
        await safe_edit_text(
            query,
            b(f"📚 {e(title)}") + f"\n<b>Mode:</b> {mode.title()} | "
            + f"<b>Interval:</b> {interval_minutes if interval_minutes > 0 else 'Random 5–10'} min\n\n"
            + bq(b("Send the target channel @username or ID:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoupdate")]]),
        )
        return

    # ── Auto-forward menu ──────────────────────────────────────────────────────────
    if data == "admin_autoforward":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await _show_autoforward_menu(context, chat_id)
        return

    if data == "af_add_connection":
        if not is_admin:
            return
        user_states[uid] = AF_ADD_CONNECTION_SOURCE
        await safe_edit_text(
            query,
            b("♻️ Add Auto-Forward Connection") + "\n\n"
            + bq(b("Step 1/2: Send the SOURCE channel @username or ID:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoforward")]]),
        )
        return

    if data == "af_list_connections":
        if not is_admin:
            return
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    SELECT id, source_chat_id, target_chat_id, active, delay_seconds
                    FROM auto_forward_connections ORDER BY id DESC LIMIT 20
                """)
                conns = cur.fetchall() or []
        except Exception:
            conns = []
        text = b(f"♻️ Auto-Forward Connections ({len(conns)}):") + "\n\n"
        if conns:
            keyboard = []
            for cid, src, tgt, active, delay in conns:
                status = "✅" if active else "❌"
                text += f"{status} {code(str(src))} → {code(str(tgt))} (ID:{cid})\n"
                keyboard.append([bold_button(
                    f"{status} {str(src)[:15]} → {str(tgt)[:15]}",
                    callback_data=f"af_conn_detail_{cid}"
                )])
            keyboard.append([bold_button("🔙 Back", callback_data="admin_autoforward")])
        else:
            text += b("No connections configured.")
            keyboard = [[bold_button("🔙 Back", callback_data="admin_autoforward")]]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("af_conn_detail_"):
        if not is_admin:
            return
        conn_id = int(data[len("af_conn_detail_"):])
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    SELECT id, source_chat_id, target_chat_id, active,
                           protect_content, silent, pin_message, delete_source, delay_seconds
                    FROM auto_forward_connections WHERE id = %s
                """, (conn_id,))
                conn = cur.fetchone()
        except Exception:
            conn = None
        if not conn:
            await safe_answer(query, "Connection not found.")
            return
        cid, src, tgt, active, protect, silent, pin, delete_src, delay = conn
        text = (
            b(f"♻️ Connection #{cid}") + "\n\n"
            f"<b>Source:</b> {code(str(src))}\n"
            f"<b>Target:</b> {code(str(tgt))}\n"
            f"<b>Active:</b> {'✅' if active else '❌'}\n"
            f"<b>Protect Content:</b> {'✅' if protect else '❌'}\n"
            f"<b>Silent:</b> {'✅' if silent else '❌'}\n"
            f"<b>Pin:</b> {'✅' if pin else '❌'}\n"
            f"<b>Delete Source:</b> {'✅' if delete_src else '❌'}\n"
            f"<b>Delay:</b> {code(str(delay) + 's' if delay else '0s')}"
        )
        keyboard = [
            [bold_button("🗑 Delete", callback_data=f"af_conn_del_{cid}"),
             bold_button("🔙 Back", callback_data="af_list_connections")],
        ]
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("af_conn_del_"):
        if not is_admin:
            return
        conn_id = int(data[len("af_conn_del_"):])
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("DELETE FROM auto_forward_connections WHERE id = %s", (conn_id,))
        except Exception:
            pass
        await safe_answer(query, f"Connection #{conn_id} deleted.")
        await button_handler(update, context, "af_list_connections")
        return

    if data in ("af_replacements_menu", "af_set_delay",
                "af_set_caption", "af_bulk", "af_delete_connection"):
        if not is_admin:
            return
        label = data.replace("af_", "").replace("_", " ").title()
        await safe_edit_text(
            query,
            b(f"♻️ {label}") + "\n\n"
            + bq(b("This feature allows fine-grained control over auto-forwarding.\n")
                 + b("Use /autoforward to access the full manager from the admin panel.")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Back", callback_data="admin_autoforward")]]),
        )
        return

    # ── Auto-forward filters panel with DM / Group toggles ────────────────────────
    if data == "af_filters_menu":
        if not is_admin:
            return
        # Load current filter settings for connection 0 (global) or first active connection
        dm_on = True
        grp_on = True
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    "SELECT enable_in_dm, enable_in_group FROM auto_forward_filters LIMIT 1"
                )
                row = cur.fetchone()
                if row:
                    dm_on, grp_on = bool(row[0]), bool(row[1])
        except Exception:
            pass
        dm_icon = "✅" if dm_on else "❌"
        grp_icon = "✅" if grp_on else "❌"
        ftext = (
            b("🔍 Auto-Forward Filters") + "\n\n"
            + bq(
                f"<b>Enable in DM:</b> {dm_icon}\n"
                f"<b>Enable in Group:</b> {grp_icon}"
            )
        )
        fkb = [
            [bold_button(f"{dm_icon} Toggle DM", callback_data="af_toggle_dm"),
             bold_button(f"{grp_icon} Toggle Group", callback_data="af_toggle_group")],
            [bold_button("🚫 Blacklist Words", callback_data="af_blacklist"),
             bold_button("✅ Whitelist Words", callback_data="af_whitelist")],
            [bold_button("🔙 Back", callback_data="admin_autoforward")],
        ]
        await safe_edit_text(query, ftext, reply_markup=InlineKeyboardMarkup(fkb))
        return

    if data == "af_toggle_all":
        if not is_admin:
            return
        current = get_setting("autoforward_enabled", "true")
        new_val = "false" if current == "true" else "true"
        set_setting("autoforward_enabled", new_val)
        await safe_answer(query, f"Auto-Forward {'enabled' if new_val == 'true' else 'disabled'}!")
        try:
            await query.delete_message()
        except Exception:
            pass
        await _show_autoforward_menu(context, chat_id)
        return

    if data in ("af_toggle_dm", "af_toggle_group"):
        if not is_admin:
            return
        col = "enable_in_dm" if data == "af_toggle_dm" else "enable_in_group"
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(
                    f"UPDATE auto_forward_filters SET {col} = NOT {col}"
                )
                if cur.rowcount == 0:
                    cur.execute(
                        "INSERT INTO auto_forward_filters (enable_in_dm, enable_in_group) VALUES (TRUE, TRUE)"
                    )
        except Exception as exc:
            logger.debug(f"af toggle error: {exc}")
        await safe_answer(query, "Filter toggled!")
        await button_handler(update, context, "af_filters_menu")
        return

    if data in ("af_blacklist", "af_whitelist"):
        if not is_admin:
            return
        kind = "Blacklist" if data == "af_blacklist" else "Whitelist"
        col = "blacklist_words" if data == "af_blacklist" else "whitelist_words"
        words = ""
        try:
            with db_manager.get_cursor() as cur:
                cur.execute(f"SELECT {col} FROM auto_forward_filters LIMIT 1")
                row = cur.fetchone()
                if row and row[0]:
                    words = row[0]
        except Exception:
            pass
        await safe_edit_text(
            query,
            b(f"📝 {kind} Words") + "\n\n"
            + bq(
                f"<b>Current:</b> {code(e(words or 'None'))}\n\n"
                "Send new comma-separated words to set the list:\n"
                "<i>e.g. word1, word2, word3</i>"
            ),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Back", callback_data="af_filters_menu")]]),
        )
        user_states[uid] = f"af_set_{col}"
        return

    # ── Auto manga update menu ─────────────────────────────────────────────────────
    if data == "admin_autoupdate":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await _show_autoupdate_menu(context, chat_id)
        return

    if data == "au_add_manga":
        if not is_admin:
            return
        user_states[uid] = AU_ADD_MANGA_TITLE
        await safe_edit_text(
            query,
            b("📚 Track New Manga") + "\n\n"
            + bq(b("Send the manga title to search on MangaDex:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoupdate")]]),
        )
        return

    if data == "au_list_manga":
        if not is_admin:
            return
        text = MangaTracker.get_tracked_for_admin()
        rows = MangaTracker.get_all_tracked()
        keyboard = []
        for rec in rows:
            rec_id, manga_id, title, _, _, _, _ = rec
            keyboard.append([bold_button(
                f"🗑 Stop: {e(title[:20])}",
                callback_data=f"au_stop_{manga_id}"
            )])
        keyboard.append([bold_button("🔙 Back", callback_data="admin_autoupdate")])
        await safe_edit_text(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("au_stop_"):
        if not is_admin:
            return
        manga_id = data[len("au_stop_"):]
        MangaTracker.remove_tracking(manga_id)
        await safe_answer(query, "Tracking stopped.")
        await button_handler(update, context, "au_list_manga")
        return

    if data == "au_remove_manga":
        if not is_admin:
            return
        await button_handler(update, context, "au_list_manga")
        return

    if data == "au_stats":
        if not is_admin:
            return
        rows = MangaTracker.get_all_tracked()
        text = (
            b("📊 Manga Tracking Stats") + "\n\n"
            f"<b>Total tracked:</b> {code(str(len(rows)))}"
        )
        await safe_edit_text(
            query, text,
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Back", callback_data="admin_autoupdate")]]),
        )
        return

    # ── Upload manager ─────────────────────────────────────────────────────────────
    if data == "upload_menu":
        if not is_admin:
            return
        await load_upload_progress()
        try:
            await query.delete_message()
        except Exception:
            pass
        await show_upload_menu(chat_id, context)
        return

    if data == "upload_preview":
        if not is_admin:
            return
        cap = build_caption_from_progress()
        await safe_edit_text(
            query,
            b("👁 Caption Preview:") + "\n\n" + cap,
            reply_markup=get_upload_menu_markup(),
        )
        return

    if data == "upload_set_caption":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_CAPTION
        await safe_edit_text(
            query,
            b("📝 Set Caption Template") + "\n\n"
            + bq(
                b("Send the new caption template.\n\n")
                + b("Placeholders:\n")
                + b("{anime_name}, {season}, {episode}, {total_episode}, {quality}")
            ),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_set_anime_name":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_CAPTION  # Reuse a simpler state
        context.user_data["upload_field"] = "anime_name"
        await safe_edit_text(
            query,
            b("🎌 Set Anime Name") + "\n\n"
            + bq(b(f"Current: {e(upload_progress.get('anime_name', 'Anime Name'))}\n\nSend the new anime name:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_set_season":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_SEASON
        await safe_edit_text(
            query,
            b("📅 Set Season") + "\n\n"
            + bq(b(f"Current: {upload_progress['season']}\n\nSend new season number:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_set_episode":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_EPISODE
        await safe_edit_text(
            query,
            b("🔢 Set Episode") + "\n\n"
            + bq(b(f"Current: {upload_progress['episode']}\n\nSend new episode number:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_set_total":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_TOTAL
        await safe_edit_text(
            query,
            b("🔢 Set Total Episodes") + "\n\n"
            + bq(b(f"Current: {upload_progress['total_episode']}\n\nSend total episode count:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_set_channel":
        if not is_admin:
            return
        user_states[uid] = UPLOAD_SET_CHANNEL
        await safe_edit_text(
            query,
            b("📢 Set Target Channel") + "\n\n"
            + bq(b("Send target channel @username or ID:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="upload_back")]]),
        )
        return

    if data == "upload_quality_menu":
        if not is_admin:
            return
        keyboard = []
        row = []
        for q_val in ALL_QUALITIES:
            selected = q_val in upload_progress["selected_qualities"]
            mark = "✅ " if selected else ""
            row.append(bold_button(f"{mark}{q_val}", callback_data=f"upload_toggle_q_{q_val}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([bold_button("🔙 Back", callback_data="upload_back")])
        await safe_edit_text(
            query, b("🎛 Quality Settings — Toggle to select/deselect:"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("upload_toggle_q_"):
        if not is_admin:
            return
        q_val = data[len("upload_toggle_q_"):]
        if q_val in upload_progress["selected_qualities"]:
            upload_progress["selected_qualities"].remove(q_val)
        else:
            upload_progress["selected_qualities"].append(q_val)
        await save_upload_progress()
        await safe_answer(query, f"{'Added' if q_val in upload_progress['selected_qualities'] else 'Removed'} {q_val}")
        await button_handler(update, context, "upload_quality_menu")
        return

    if data == "upload_toggle_auto":
        if not is_admin:
            return
        upload_progress["auto_caption_enabled"] = not upload_progress["auto_caption_enabled"]
        await save_upload_progress()
        status = "ON" if upload_progress["auto_caption_enabled"] else "OFF"
        await safe_answer(query, f"Auto-caption: {status}")
        await show_upload_menu(chat_id, context, query.message)
        return

    if data == "upload_reset":
        if not is_admin:
            return
        upload_progress["episode"] = 1
        upload_progress["video_count"] = 0
        await save_upload_progress()
        await safe_answer(query, "Episode reset to 1.")
        await show_upload_menu(chat_id, context, query.message)
        return

    if data == "upload_clear_db":
        if not is_admin:
            return
        await safe_edit_text(
            query,
            b("⚠️ Clear Upload Database?") + "\n\n"
            + bq(b("This will reset all progress counters. Caption and quality settings are kept.")),
            reply_markup=InlineKeyboardMarkup([
                [bold_button("✅ Yes, Clear", callback_data="upload_confirm_clear"),
                 bold_button("❌ Cancel", callback_data="upload_back")],
            ]),
        )
        return

    if data == "upload_confirm_clear":
        if not is_admin:
            return
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("DELETE FROM bot_progress WHERE id = 1")
                cur.execute("""
                    INSERT INTO bot_progress
                        (id, base_caption, selected_qualities, auto_caption_enabled, anime_name)
                    VALUES (1, %s, %s, %s, %s)
                """, (
                    DEFAULT_CAPTION,
                    ",".join(upload_progress["selected_qualities"]),
                    upload_progress["auto_caption_enabled"],
                    upload_progress.get("anime_name", "Anime Name"),
                ))
        except Exception as exc:
            await safe_answer(query, f"Error: {str(exc)[:50]}", show_alert=True)
            return
        await load_upload_progress()
        await safe_answer(query, "Database cleared!")
        try:
            await query.delete_message()
        except Exception:
            pass
        await show_upload_menu(chat_id, context)
        return

    if data == "upload_back":
        if not is_admin:
            return
        await show_upload_menu(chat_id, context, query.message)
        return

    # ── Admin cmd list ─────────────────────────────────────────────────────────────
    if data == "admin_cmd_list":
        if not is_admin:
            return
        try:
            await query.delete_message()
        except Exception:
            pass
        await cmd_command(update, context)
        return

    # ── Unhandled fallback ─────────────────────────────────────────────────────────
    logger.info(f"Unhandled callback: {data!r} from user {uid}")
    # Don't show alert for unhandled — just silently ignore
    # (already answered at the top)


# ================================================================================
#                      ADMIN MESSAGE HANDLER — FULL STATE MACHINE
# ================================================================================

async def handle_admin_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    """Handle all messages from admin in conversation states."""
    if not update.effective_user:
        return
    uid = update.effective_user.id
    if uid not in (ADMIN_ID, OWNER_ID):
        return
    if uid not in user_states:
        return
    if not update.message:
        return

    state = user_states[uid]
    text = update.message.text or ""
    chat_id = update.effective_chat.id

    await delete_bot_prompt(context, chat_id)
    await delete_update_message(update, context)

    # Cancel command
    if text.strip().lower() in ("/cancel", "cancel"):
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Channel states ─────────────────────────────────────────────────────────────
    if state == ADD_CHANNEL_USERNAME:
        uname = text.strip()
        if not uname.startswith("@"):
            msg = await safe_send_message(
                context.bot, chat_id,
                b("❌ Username must start with @. Try again:"),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="manage_force_sub")]]),
            )
            await store_bot_prompt(context, msg)
            return
        try:
            tg_chat = await context.bot.get_chat(uname)
            context.user_data["new_ch_uname"] = uname
            context.user_data["new_ch_title"] = tg_chat.title
            user_states[uid] = ADD_CHANNEL_TITLE
            # Build channel preview link so admin can verify which channel they're naming
            ch_link = f"https://t.me/{uname.lstrip('@')}" if not str(tg_chat.id).startswith("-100") else ""
            ch_info = f"<b>Channel:</b> {e(tg_chat.title)}\n<b>Username:</b> {e(uname)}\n<b>ID:</b> <code>{tg_chat.id}</code>"
            if ch_link:
                ch_info += f"\n<b>Link:</b> {ch_link}"
            keyboard = [[bold_button("🔙 Cancel", callback_data="manage_force_sub")]]
            msg = await safe_send_message(
                context.bot, chat_id,
                b("✅ Channel found!") + "\n\n"
                + bq(ch_info) + "\n\n"
                + b("Send a display title for this channel, or /skip to use the channel name:"),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            await store_bot_prompt(context, msg)
        except Exception as exc:
            msg = await safe_send_message(
                context.bot, chat_id,
                UserFriendlyError.get_user_message(exc),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="manage_force_sub")]]),
            )
            await store_bot_prompt(context, msg)
        return

    if state == ADD_CHANNEL_TITLE:
        uname = context.user_data.get("new_ch_uname")
        if not uname:
            user_states.pop(uid, None)
            await safe_send_message(context.bot, chat_id, b("Session expired. Start over."))
            return
        title = text.strip()
        if title.lower() == "/skip":
            title = context.user_data.get("new_ch_title", uname)
        add_force_sub_channel(uname, title, join_by_request=False)
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Added {e(title)} ({e(uname)}) as force-sub channel!"),
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Link generation states ─────────────────────────────────────────────────────
    if state == GENERATE_LINK_IDENTIFIER:
        identifier = text.strip()
        try:
            tg_chat = await context.bot.get_chat(identifier)
            context.user_data["gen_ch_id"] = tg_chat.id
            context.user_data["gen_ch_title"] = tg_chat.title
            user_states[uid] = GENERATE_LINK_TITLE
            msg = await safe_send_message(
                context.bot, chat_id,
                b(f"📢 Channel: {e(tg_chat.title)}") + "\n\n"
                + bq(b("Send a title for this link (shown in link backup), or /skip:")),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
            )
            await store_bot_prompt(context, msg)
        except Exception as exc:
            msg = await safe_send_message(
                context.bot, chat_id,
                UserFriendlyError.get_user_message(exc),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_back")]]),
            )
            await store_bot_prompt(context, msg)
        return

    if state == GENERATE_LINK_TITLE:
        title = text.strip()
        if title.lower() == "/skip":
            title = context.user_data.get("gen_ch_title", "")
        ch_id = context.user_data.get("gen_ch_id")
        if not ch_id:
            user_states.pop(uid, None)
            await safe_send_message(context.bot, chat_id, b("Session expired. Start over."))
            return
        try:
            link_id = generate_link_id(
                channel_username=ch_id,
                user_id=uid,
                never_expires=False,
                channel_title=title,
                source_bot_username=BOT_USERNAME,
            )
            deep_link = f"https://t.me/{BOT_USERNAME}?start={link_id}"
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Link generated for {e(title)}:") + "\n\n"
                + bq(code(deep_link)),
                reply_markup=_back_kb(),
            )
        except Exception as exc:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Error generating link: ") + code(e(str(exc)[:200])),
            )
        user_states.pop(uid, None)
        return

    # ── Clone token ────────────────────────────────────────────────────────────────
    if state == ADD_CLONE_TOKEN:
        token = text.strip()
        await _register_clone_token(update, context, token)
        user_states.pop(uid, None)
        return

    # ── Backup channel ─────────────────────────────────────────────────────────────
    if state == SET_BACKUP_CHANNEL:
        url = text.strip()
        set_setting("backup_channel_url", url)
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Backup channel URL set: {e(url)}")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Broadcast states ───────────────────────────────────────────────────────────
    if state == PENDING_BROADCAST:
        context.user_data["broadcast_message"] = (
            update.message.chat_id, update.message.message_id
        )
        user_states[uid] = PENDING_BROADCAST_OPTIONS
        keyboard = [
            [bold_button("📨 Normal", callback_data="broadcast_mode_normal"),
             bold_button("🔕 Silent", callback_data="broadcast_mode_silent")],
            [bold_button("🗑 Auto-Delete 24h", callback_data="broadcast_mode_auto_delete"),
             bold_button("📌 Pin", callback_data="broadcast_mode_pin")],
            [bold_button("⏰ Schedule", callback_data="broadcast_schedule"),
             bold_button("🔙 Cancel", callback_data="admin_back")],
        ]
        msg = await safe_send_message(
            context.bot, chat_id,
            b("✅ Message received! Choose broadcast mode:"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        await store_bot_prompt(context, msg)
        return

    if state == PENDING_BROADCAST_CONFIRM and text.strip().lower() in ("/confirm", "confirm"):
        msg_data = context.user_data.get("broadcast_message")
        mode = context.user_data.get("broadcast_mode", BroadcastMode.NORMAL)
        if not msg_data:
            await safe_send_message(context.bot, chat_id, b("❌ Broadcast message lost. Start over."))
            user_states.pop(uid, None)
            return
        user_states.pop(uid, None)
        msg_chat_id, msg_id = msg_data
        asyncio.create_task(
            _do_broadcast(context, chat_id, msg_chat_id, msg_id, mode)
        )
        return

    # ── Category settings states ───────────────────────────────────────────────────
    category = context.user_data.get("editing_category", "")

    if state == SET_CATEGORY_CAPTION:
        update_category_field(category, "caption_template", text.strip())
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Caption template for {e(category)} updated!")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    if state == SET_CATEGORY_BRANDING:
        update_category_field(category, "branding", text.strip())
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Branding for {e(category)} updated!")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    if state == SET_CATEGORY_BUTTONS:
        lines = text.strip().split("\n")
        buttons_list = []
        for line in lines:
            if " - " in line:
                parts = line.split(" - ", 1)
                buttons_list.append({"text": parts[0].strip(), "url": parts[1].strip()})
        update_category_field(category, "buttons", json.dumps(buttons_list))
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ {len(buttons_list)} button(s) configured for {e(category)}!")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    if state == SET_CATEGORY_THUMBNAIL:
        val = "" if text.strip().lower() in ("default", "none", "remove") else text.strip()
        update_category_field(category, "thumbnail_url", val)
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Thumbnail for {e(category)} {'reset to default' if not val else 'updated'}!")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    if state == SET_WATERMARK_TEXT:
        update_category_field(category, "watermark_text", text.strip())
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Watermark text for {e(category)} set!")
        )
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Upload manager states ──────────────────────────────────────────────────────
    if state == UPLOAD_SET_CAPTION:
        # Check if we're setting anime name or caption
        upload_field = context.user_data.pop("upload_field", None)
        if upload_field == "anime_name":
            upload_progress["anime_name"] = text.strip()
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Anime name set to: {e(text.strip())}")
            )
        else:
            upload_progress["base_caption"] = text
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id, b("✅ Caption template updated!")
            )
        user_states.pop(uid, None)
        await show_upload_menu(chat_id, context)
        return

    if state == UPLOAD_SET_SEASON:
        try:
            upload_progress["season"] = int(text.strip())
            upload_progress["video_count"] = 0
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Season set to {upload_progress['season']}")
            )
        except ValueError:
            await safe_send_message(context.bot, chat_id, b("❌ Invalid number. Send again:"))
            return
        user_states.pop(uid, None)
        await show_upload_menu(chat_id, context)
        return

    if state == UPLOAD_SET_EPISODE:
        try:
            upload_progress["episode"] = int(text.strip())
            upload_progress["video_count"] = 0
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Episode set to {upload_progress['episode']}")
            )
        except ValueError:
            await safe_send_message(context.bot, chat_id, b("❌ Invalid number. Send again:"))
            return
        user_states.pop(uid, None)
        await show_upload_menu(chat_id, context)
        return

    if state == UPLOAD_SET_TOTAL:
        try:
            upload_progress["total_episode"] = int(text.strip())
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Total episodes set to {upload_progress['total_episode']}")
            )
        except ValueError:
            await safe_send_message(context.bot, chat_id, b("❌ Invalid number. Send again:"))
            return
        user_states.pop(uid, None)
        await show_upload_menu(chat_id, context)
        return

    if state == UPLOAD_SET_CHANNEL:
        identifier = text.strip()
        try:
            tg_chat = await context.bot.get_chat(identifier)
            upload_progress["target_chat_id"] = tg_chat.id
            await save_upload_progress()
            await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Target channel set to: {e(tg_chat.title)}")
            )
        except Exception as exc:
            await safe_send_message(context.bot, chat_id, UserFriendlyError.get_user_message(exc))
            return
        user_states.pop(uid, None)
        await show_upload_menu(chat_id, context)
        return

    # ── Auto-forward states ────────────────────────────────────────────────────────
    if state == AF_ADD_CONNECTION_SOURCE:
        identifier = text.strip()
        try:
            tg_chat = await context.bot.get_chat(identifier)
            context.user_data["af_source_id"] = tg_chat.id
            context.user_data["af_source_uname"] = tg_chat.username
            user_states[uid] = AF_ADD_CONNECTION_TARGET
            msg = await safe_send_message(
                context.bot, chat_id,
                b(f"✅ Source: {e(tg_chat.title)}") + "\n\n"
                + bq(b("Step 2/2: Send the TARGET channel @username or ID:")),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoforward")]]),
            )
            await store_bot_prompt(context, msg)
        except Exception as exc:
            msg = await safe_send_message(
                context.bot, chat_id, UserFriendlyError.get_user_message(exc),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoforward")]]),
            )
            await store_bot_prompt(context, msg)
        return

    if state == AF_ADD_CONNECTION_TARGET:
        identifier = text.strip()
        try:
            tg_chat = await context.bot.get_chat(identifier)
            src_id = context.user_data.get("af_source_id")
            src_uname = context.user_data.get("af_source_uname", "")
            if not src_id:
                await safe_send_message(context.bot, chat_id, b("Session expired. Start over."))
                user_states.pop(uid, None)
                return
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    INSERT INTO auto_forward_connections
                        (source_chat_id, source_chat_username, target_chat_id,
                         target_chat_username, active)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT DO NOTHING
                """, (src_id, src_uname, tg_chat.id, tg_chat.username))
            await safe_send_message(
                context.bot, chat_id,
                b("✅ Auto-forward connection created!") + "\n\n"
                + bq(
                    b("Source: ") + code(str(src_id)) + "\n"
                    + b("Target: ") + code(str(tg_chat.id)) + " — " + e(tg_chat.title)
                ),
            )
        except Exception as exc:
            await safe_send_message(context.bot, chat_id, UserFriendlyError.get_user_message(exc))
        user_states.pop(uid, None)
        await send_admin_menu(chat_id, context)
        return

    # ── Manga tracker states ───────────────────────────────────────────────────────
    if state == AU_ADD_MANGA_TITLE:
        title = text.strip()
        # Search MangaDex
        results = MangaDexClient.search_manga(title, limit=5)
        if not results:
            # Try AniList
            anilist_result = AniListClient.search_manga(title)
            if anilist_result:
                al_title = (anilist_result.get("title") or {})
                al_title_str = al_title.get("romaji") or al_title.get("english") or title
                # Search MangaDex with AniList title
                results = MangaDexClient.search_manga(al_title_str, limit=5)

        if not results:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ No manga found on MangaDex.") + "\n" + bq(b("Try a different title.")),
            )
            return

        keyboard = []
        for manga in results[:5]:
            attrs = manga.get("attributes", {}) or {}
            titles = attrs.get("title", {}) or {}
            manga_title = titles.get("en") or next(iter(titles.values()), "Unknown")
            keyboard.append([bold_button(
                manga_title[:40],
                callback_data=f"mdex_track_{manga['id']}"
            )])
        keyboard.append([bold_button("🔙 Cancel", callback_data="admin_autoupdate")])

        await safe_send_message(
            context.bot, chat_id,
            b("📚 Select the manga to track:"),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        user_states.pop(uid, None)
        return

    if state == AU_CUSTOM_INTERVAL:
        try:
            mins = int(text.strip())
            if mins < 1:
                raise ValueError("Too small")
        except ValueError:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Please send a valid number of minutes (e.g. 15):"),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoupdate")]]),
            )
            return
        context.user_data["au_manga_interval"] = mins
        user_states[uid] = AU_ADD_MANGA_TARGET
        title = context.user_data.get("au_manga_title", "Unknown")
        mode = context.user_data.get("au_manga_mode", "latest")
        await safe_send_message(
            context.bot, chat_id,
            b(f"📚 {e(title)}") + f"\n<b>Mode:</b> {mode.title()} | <b>Interval:</b> {mins} min\n\n"
            + bq(b("Send the target channel @username or ID:")),
            reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Cancel", callback_data="admin_autoupdate")]]),
        )
        return

    if state == AU_ADD_MANGA_TARGET:
        identifier = text.strip()
        manga_id = context.user_data.get("au_manga_id")
        manga_title = context.user_data.get("au_manga_title", "Unknown")
        manga_mode = context.user_data.get("au_manga_mode", "latest")
        manga_interval = context.user_data.get("au_manga_interval", 60)
        if not manga_id:
            await safe_send_message(context.bot, chat_id, b("Session expired. Please start over."))
            user_states.pop(uid, None)
            return
        try:
            tg_chat = await context.bot.get_chat(identifier)
            success = MangaTracker.add_tracking(manga_id, manga_title, tg_chat.id)
            if success:
                # For "latest" mode: save the current latest chapter as baseline so we don't re-send old ones
                if manga_mode == "latest":
                    latest = MangaDexClient.get_latest_chapter(manga_id)
                    if latest:
                        attrs = latest.get("attributes", {}) or {}
                        ch = attrs.get("chapter")
                        if ch:
                            try:
                                with db_manager.get_cursor() as cur:
                                    cur.execute(
                                        "UPDATE manga_auto_updates SET last_chapter = %s, interval_minutes = %s "
                                        "WHERE manga_id = %s AND target_chat_id = %s",
                                        (ch, manga_interval, manga_id, tg_chat.id)
                                    )
                            except Exception:
                                pass
                else:
                    # Full mode: save interval only, start from chapter 0
                    try:
                        with db_manager.get_cursor() as cur:
                            cur.execute(
                                "UPDATE manga_auto_updates SET interval_minutes = %s, mode = %s "
                                "WHERE manga_id = %s AND target_chat_id = %s",
                                (manga_interval, manga_mode, manga_id, tg_chat.id)
                            )
                    except Exception:
                        pass

                interval_label = "Random 5–10 min" if manga_interval == -1 else f"{manga_interval} min"
                await safe_send_message(
                    context.bot, chat_id,
                    b(f"✅ Now tracking: {e(manga_title)}") + "\n\n"
                    + bq(
                        f"<b>Channel:</b> {e(tg_chat.title or tg_chat.username or str(tg_chat.id))}\n"
                        f"<b>Mode:</b> {manga_mode.title()}\n"
                        f"<b>Check interval:</b> {interval_label}\n\n"
                        + b("New chapters will be sent automatically.")
                    ),
                )
            else:
                await safe_send_message(context.bot, chat_id, b("❌ Failed to add tracking. Check that the bot has access to the channel."))
        except Exception as exc:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Could not find that channel.\n\n") + bq(b("Make sure:\n• The bot is an admin in the channel\n• Username is correct (starts with @)\n\nError: ")) + code(e(str(exc)[:100])),
                reply_markup=InlineKeyboardMarkup([[bold_button("🔙 Back", callback_data="admin_autoupdate")]]),
            )
            return
        user_states.pop(uid, None)
        context.user_data.pop("au_manga_id", None)
        context.user_data.pop("au_manga_title", None)
        context.user_data.pop("au_manga_mode", None)
        context.user_data.pop("au_manga_interval", None)
        await send_admin_menu(chat_id, context)
        return

    # ── User management states ─────────────────────────────────────────────────────
    if state == BAN_USER_INPUT:
        target = resolve_target_user_id(text.strip())
        if target:
            if target in (ADMIN_ID, OWNER_ID):
                await safe_send_message(context.bot, chat_id, b("⚠️ Cannot ban admin/owner."))
            else:
                ban_user(target)
                await safe_send_message(context.bot, chat_id, b(f"🚫 User {code(str(target))} banned."))
        else:
            await safe_send_message(context.bot, chat_id, b("❌ User not found."))
        user_states.pop(uid, None)
        return

    if state == UNBAN_USER_INPUT:
        target = resolve_target_user_id(text.strip())
        if target:
            unban_user(target)
            await safe_send_message(context.bot, chat_id, b(f"✅ User {code(str(target))} unbanned."))
        else:
            await safe_send_message(context.bot, chat_id, b("❌ User not found."))
        user_states.pop(uid, None)
        return

    if state == DELETE_USER_INPUT:
        try:
            target_uid = int(text.strip())
            if target_uid in (ADMIN_ID, OWNER_ID):
                await safe_send_message(context.bot, chat_id, b("⚠️ Cannot delete admin/owner."))
            else:
                with db_manager.get_cursor() as cur:
                    cur.execute("DELETE FROM users WHERE user_id = %s", (target_uid,))
                await safe_send_message(
                    context.bot, chat_id, b(f"✅ User {code(str(target_uid))} deleted.")
                )
        except (ValueError, Exception) as exc:
            await safe_send_message(context.bot, chat_id, b(f"❌ Error: {code(e(str(exc)[:100]))}"))
        user_states.pop(uid, None)
        return

    if state == SEARCH_USER_INPUT:
        target = resolve_target_user_id(text.strip())
        if target:
            user_info = get_user_info_by_id(target)
            if user_info:
                u_id, u_uname, u_fname, u_lname, u_joined, u_banned = user_info
                name = f"{u_fname or ''} {u_lname or ''}".strip() or "N/A"
                info_text = (
                    b("👤 User Found:") + "\n\n"
                    f"<b>ID:</b> {code(str(u_id))}\n"
                    f"<b>Name:</b> {e(name)}\n"
                    f"<b>Username:</b> {'@' + e(u_uname) if u_uname else '—'}\n"
                    f"<b>Joined:</b> {code(str(u_joined)[:16])}\n"
                    f"<b>Status:</b> {'🚫 Banned' if u_banned else '✅ Active'}"
                )
                keyboard = [
                    [bold_button("🚫 Ban" if not u_banned else "✅ Unban",
                                 callback_data=f"user_ban_{u_id}" if not u_banned else f"user_unban_{u_id}")],
                    [bold_button("🗑 Delete", callback_data=f"user_del_{u_id}")],
                ]
                await safe_send_message(
                    context.bot, chat_id, info_text,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
            else:
                await safe_send_message(context.bot, chat_id, b(f"❌ No user found with ID {target}."))
        else:
            await safe_send_message(context.bot, chat_id, b("❌ User not found in database."))
        user_states.pop(uid, None)
        return

    # ── Scheduled broadcast datetime ───────────────────────────────────────────────
    if state == SCHEDULE_BROADCAST_DATETIME:
        try:
            dt = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
            context.user_data["schedule_dt"] = dt
            user_states[uid] = SCHEDULE_BROADCAST_MSG
            msg = await safe_send_message(
                context.bot, chat_id,
                b(f"📅 Scheduled for: {dt.strftime('%d %b %Y %H:%M')} UTC") + "\n\n"
                + bq(b("Now send the message to broadcast:")),
            )
            await store_bot_prompt(context, msg)
        except ValueError:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Invalid format.") + "\n" + bq(b("Use: YYYY-MM-DD HH:MM (e.g., 2026-12-25 08:00)"))
            )
        return

    if state == SCHEDULE_BROADCAST_MSG:
        dt = context.user_data.get("schedule_dt")
        if not dt:
            await safe_send_message(context.bot, chat_id, b("❌ Session expired. Start over."))
            user_states.pop(uid, None)
            return
        try:
            with db_manager.get_cursor() as cur:
                cur.execute("""
                    INSERT INTO scheduled_broadcasts (admin_id, message_text, execute_at, status)
                    VALUES (%s, %s, %s, 'pending')
                """, (uid, text.strip(), dt))
        except Exception as exc:
            await safe_send_message(
                context.bot, chat_id,
                b("❌ Error scheduling: ") + code(e(str(exc)[:200]))
            )
            user_states.pop(uid, None)
            return
        await safe_send_message(
            context.bot, chat_id,
            b(f"✅ Broadcast scheduled for {dt.strftime('%d %b %Y %H:%M')} UTC!"),
            reply_markup=_back_kb(),
        )
        user_states.pop(uid, None)
        return

    # ── Fallthrough: unknown state ─────────────────────────────────────────────────
    logger.debug(f"Admin message in unknown state {state} from {uid}: {text[:50]}")


# ================================================================================
#                          MAIN FUNCTION
# ================================================================================

def main() -> None:
    """Bot entry point — set up and start polling."""
    if not BOT_TOKEN or BOT_TOKEN in ("YOUR_TOKEN_HERE", ""):
        logger.error("❌ BOT_TOKEN is not set!")
        return
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL is not set!")
        return
    if not ADMIN_ID:
        logger.error("❌ ADMIN_ID is not set!")
        return

    # Initialize database
    try:
        init_db(DATABASE_URL)
        logger.info("✅ Database initialized")
    except Exception as exc:
        logger.error(f"❌ Database init failed: {exc}")
        return

    # Test DB
    try:
        count = get_user_count()
        logger.info(f"✅ Database working — {count} users registered")
    except Exception as exc:
        logger.error(f"❌ Database test failed: {exc}")
        return

    # Build application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    # ── Register all handlers ────────────────────────────────────────────────────
    admin_filter = filters.User(user_id=ADMIN_ID) | filters.User(user_id=OWNER_ID)

    # Public commands
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("ping", ping_command))
    application.add_handler(CommandHandler("alive", alive_command))
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(CommandHandler("search", search_command))
    application.add_handler(CommandHandler("anime", anime_command))
    application.add_handler(CommandHandler("manga", manga_command))
    application.add_handler(CommandHandler("movie", movie_command))
    application.add_handler(CommandHandler("tvshow", tvshow_command))
    application.add_handler(CommandHandler("id", id_command))
    application.add_handler(CommandHandler("info", info_command))

    # Admin-only commands
    application.add_handler(CommandHandler("stats", stats_command, filters=admin_filter))
    application.add_handler(CommandHandler("sysstats", sysstats_command, filters=admin_filter))
    application.add_handler(CommandHandler("users", users_command, filters=admin_filter))
    application.add_handler(CommandHandler("cmd", cmd_command, filters=admin_filter))
    application.add_handler(CommandHandler("commands", cmd_command, filters=admin_filter))
    application.add_handler(CommandHandler("upload", upload_command, filters=admin_filter))
    application.add_handler(CommandHandler("settings", settings_command, filters=admin_filter))
    application.add_handler(CommandHandler("autoupdate", autoupdate_command, filters=admin_filter))
    application.add_handler(CommandHandler("autoforward", autoforward_command, filters=admin_filter))
    application.add_handler(CommandHandler("addchannel", add_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("channel", channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("banuser", ban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("unbanuser", unban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("listusers", listusers_command, filters=admin_filter))
    application.add_handler(CommandHandler("deleteuser", deleteuser_command, filters=admin_filter))
    application.add_handler(CommandHandler("exportusers", exportusers_command, filters=admin_filter))
    application.add_handler(CommandHandler("broadcaststats", broadcaststats_command, filters=admin_filter))
    application.add_handler(CommandHandler("backup", backup_command, filters=admin_filter))
    application.add_handler(CommandHandler("addclone", addclone_command, filters=admin_filter))
    application.add_handler(CommandHandler("clones", clones_command, filters=admin_filter))
    application.add_handler(CommandHandler("reload", reload_command, filters=admin_filter))
    application.add_handler(CommandHandler("restart", reload_command, filters=admin_filter))
    application.add_handler(CommandHandler("logs", logs_command, filters=admin_filter))
    application.add_handler(CommandHandler("connect", connect_command, filters=admin_filter))
    application.add_handler(CommandHandler("disconnect", disconnect_command, filters=admin_filter))
    application.add_handler(CommandHandler("connections", connections_command, filters=admin_filter))

    # Callback and message handlers
    application.add_handler(CallbackQueryHandler(button_handler))

    application.add_handler(
        MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message)
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND,
            group_message_handler,
        )
    )
    application.add_handler(InlineQueryHandler(inline_query_handler))

    application.add_handler(
        MessageHandler(filters.ChatType.CHANNEL, auto_forward_message_handler)
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.CHANNEL & filters.VIDEO,
            handle_channel_post,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.VIDEO & admin_filter,
            handle_upload_video,
        )
    )
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (filters.PHOTO | filters.Document.IMAGE) & admin_filter,
            handle_admin_photo,
        )
    )

    application.add_error_handler(error_handler)
    application.post_init = post_init
    application.post_shutdown = post_shutdown

    logger.info("🚀 Starting bot polling…")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        close_loop=False,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
                    UNIFIED TELEGRAM BOT (FULL FEATURE EDITION)
================================================================================

This bot integrates all features from:

  - Original bot (5).py (force-sub, deep links, clones, broadcast, admin panel)
  - Original bot (4).py (anime caption upload manager)
  - New modules (post generation for Anime/Manga/Movies/TV shows, category settings,
    auto-forward, auto manga update, group/inline search, feature flags, system stats,
    scheduled broadcasts, broadcast history, user CSV export, and many admin utilities)

ADDITIONAL ENHANCEMENTS:
  - Watermark on images (Pillow)
  - Auto‑forward filters (media types, blacklist/whitelist)
  - Auto‑forward replacements (text substitution)
  - Fully functional scheduled broadcasts (broadcasts to all users)
  - PDF generation for manga chapters (with optional watermark)
  - Bulk forward old posts (forwards last N messages)
  - Comprehensive error handling and admin error reports
  - Detailed DEBUG logging

Every function is thoroughly documented, error‑handled, and uses the safe database layer
(database_safe.py). All tokens, URLs, and admin ID are read from environment variables.

Author: Beat_Anime_Ocean
Date: 2026‑03‑17
Version: 3.0 (Ultimate)
================================================================================
"""

import os
import sys
import json
import time
import uuid
import asyncio
import logging
import html
import re
import csv
import secrets
import traceback
from io import StringIO, BytesIO
from datetime import datetime, timedelta
from functools import wraps
from typing import Optional, Dict, List, Tuple, Any, Union
from contextlib import contextmanager
from urllib.parse import quote

# Third‑party libraries
import requests
import aiohttp
import aiofiles
import psutil
import img2pdf
from PIL import Image, ImageDraw, ImageFont
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot, constants,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, ChatMember
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    MessageHandler, filters, JobQueue, InlineQueryHandler, ConversationHandler
)
from telegram.error import TelegramError, Forbidden, BadRequest

# Local modules (must be in same directory)
from database_safe import *
from health_check import health_server

# ================================================================================
#                                LOGGING SETUP
# ================================================================================

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

# Log to both file and console
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    handlers=[
        logging.FileHandler("logs/bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Additional loggers for specific components
logger = logging.getLogger(__name__)
db_logger = logging.getLogger("database")
api_logger = logging.getLogger("api")
broadcast_logger = logging.getLogger("broadcast")
error_logger = logging.getLogger("errors")

# ================================================================================
#                           ENVIRONMENT CONFIGURATION
# ================================================================================

# Mandatory variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))          # Your admin Telegram user ID

# Optional variables with defaults
LINK_EXPIRY_MINUTES = int(os.getenv("LINK_EXPIRY_MINUTES", "5"))
BROADCAST_CHUNK_SIZE = int(os.getenv("BROADCAST_CHUNK_SIZE", "1000"))
BROADCAST_MIN_USERS = int(os.getenv("BROADCAST_MIN_USERS", "5000"))
BROADCAST_INTERVAL_MIN = int(os.getenv("BROADCAST_INTERVAL_MIN", "20"))
PORT = int(os.environ.get('PORT', 8080))
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/') + '/'

# Welcome message settings
WELCOME_SOURCE_CHANNEL = int(os.getenv("WELCOME_SOURCE_CHANNEL", "-1002530952988"))
WELCOME_SOURCE_MESSAGE_ID = int(os.getenv("WELCOME_SOURCE_MESSAGE_ID", "32"))
PUBLIC_ANIME_CHANNEL_URL = os.getenv("PUBLIC_ANIME_CHANNEL_URL", "https://t.me/BeatAnime")
REQUEST_CHANNEL_URL = os.getenv("REQUEST_CHANNEL_URL", "https://t.me/Beat_Hindi_Dubbed")
ADMIN_CONTACT_USERNAME = os.getenv("ADMIN_CONTACT_USERNAME", "Beat_Anime_Ocean")

# Panel images (optional)
HELP_IMAGE_URL = os.getenv("HELP_IMAGE_URL", "")
SETTINGS_IMAGE_URL = os.getenv("SETTINGS_IMAGE_URL", "")
STATS_IMAGE_URL = os.getenv("STATS_IMAGE_URL", "")
ADMIN_PANEL_IMAGE_URL = os.getenv("ADMIN_PANEL_IMAGE_URL", "")

# Transition sticker (optional)
TRANSITION_STICKER = os.getenv("TRANSITION_STICKER", "")

# TMDB API key (optional)
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")

# Global bot identity (set at startup)
BOT_USERNAME: str = ""
I_AM_CLONE: bool = False

# Global start time for uptime calculation
BOT_START_TIME = time.time()

# ================================================================================
#                            STATE CONSTANTS
# ================================================================================

# Original states (bot 5)
(
    ADD_CHANNEL_USERNAME,
    ADD_CHANNEL_TITLE,
    GENERATE_LINK_CHANNEL_USERNAME,
    PENDING_BROADCAST,
    GENERATE_LINK_CHANNEL_TITLE,
    ADD_CLONE_TOKEN,
    PENDING_FILL_TITLE,
    SET_BACKUP_CHANNEL,
    PENDING_MOVE_TARGET,
    ADD_CHANNEL_JBR,
) = range(10)

# New states for additional features (starting at 10)
(
    PENDING_BROADCAST_OPTIONS,
    PENDING_BROADCAST_CONFIRM,
    PENDING_BROADCAST_DURATION,
    SET_CATEGORY_TEMPLATE,
    SET_CATEGORY_BRANDING,
    SET_CATEGORY_BUTTONS,
    SET_CATEGORY_CAPTION,
    SET_CATEGORY_THUMBNAIL,
    SET_CATEGORY_FONT,
    SET_CATEGORY_LOGO,
    SET_CATEGORY_LOGO_POS,
    ADD_AUTO_FORWARD_SOURCE,
    ADD_AUTO_FORWARD_TARGET,
    ADD_REPLACEMENT,
    ADD_FILTER_WORD,
    SET_AUTO_FORWARD_DELAY,
    SET_AUTO_FORWARD_CAPTION,
    BULK_FORWARD_CONFIRM,
    SCHEDULE_BROADCAST_DATETIME,
    SCHEDULE_BROADCAST_MSG,
    ADD_MANGA_AUTO,
    SET_MANGA_TARGET,
    UPLOAD_SET_CAPTION,
    UPLOAD_SET_SEASON,
    UPLOAD_SET_EPISODE,
    UPLOAD_SET_TOTAL,
    UPLOAD_SET_CHANNEL,
    UPLOAD_QUALITY_MENU,
    # New states for auto‑forward filters and replacements
    AF_FILTERS_MENU,
    AF_ADD_ALLOWED_MEDIA,
    AF_ADD_BLACKLIST,
    AF_ADD_WHITELIST,
    AF_REPLACEMENTS_MENU,
    AF_ADD_REPLACEMENT_PATTERN,
    AF_BULK_FORWARD_COUNT,
) = range(10, 45)

# Additional states for user management
(
    MANAGE_USER_BAN,
    MANAGE_USER_UNBAN,
    MANAGE_USER_DELETE,
    MANAGE_USER_NOTES,
) = range(42, 46)

# Dictionary to hold current state for each user (admin only)
user_states: Dict[int, int] = {}

# Temporary data storage per user (admin only)
user_data_temp: Dict[int, Dict] = {}

# ================================================================================
#                               BROADCAST MODES
# ================================================================================

class BroadcastMode:
    """Enum for broadcast modes."""
    NORMAL = "normal"
    AUTO_DELETE = "auto_delete"
    PIN = "pin"
    DELETE_PIN = "delete_pin"

# ================================================================================
#                          SMALL CAPS CONVERSION
# ================================================================================

def small_caps(text: str) -> str:
    """Convert ASCII text to Unicode small caps."""
    mapping = {
        'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ',
        'f': 'ғ', 'g': 'ɢ', 'h': 'ʜ', 'i': 'ɪ', 'j': 'ᴊ',
        'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ', 'o': 'ᴏ',
        'p': 'ᴘ', 'q': 'ǫ', 'r': 'ʀ', 's': 's', 't': 'ᴛ',
        'u': 'ᴜ', 'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x', 'y': 'ʏ',
        'z': 'ᴢ',
        'A': 'ᴀ', 'B': 'ʙ', 'C': 'ᴄ', 'D': 'ᴅ', 'E': 'ᴇ',
        'F': 'ғ', 'G': 'ɢ', 'H': 'ʜ', 'I': 'ɪ', 'J': 'ᴊ',
        'K': 'ᴋ', 'L': 'ʟ', 'M': 'ᴍ', 'N': 'ɴ', 'O': 'ᴏ',
        'P': 'ᴘ', 'Q': 'ǫ', 'R': 'ʀ', 'S': 's', 'T': 'ᴛ',
        'U': 'ᴜ', 'V': 'ᴠ', 'W': 'ᴡ', 'X': 'x', 'Y': 'ʏ',
        'Z': 'ᴢ',
    }
    return ''.join(mapping.get(ch, ch) for ch in text)

# ================================================================================
#                      BOLD MATHEMATICAL CONVERSION
# ================================================================================

def math_bold(text: str) -> str:
    """
    Convert ASCII text to Unicode mathematical bold.
    Example: "MODE" -> "𝗠𝗢𝗗𝗘"
    """
    mapping = {
        'A': '𝗔', 'B': '𝗕', 'C': '𝗖', 'D': '𝗗', 'E': '𝗘',
        'F': '𝗙', 'G': '𝗚', 'H': '𝗛', 'I': '𝗜', 'J': '𝗝',
        'K': '𝗞', 'L': '𝗟', 'M': '𝗠', 'N': '𝗡', 'O': '𝗢',
        'P': '𝗣', 'Q': '𝗤', 'R': '𝗥', 'S': '𝗦', 'T': '𝗧',
        'U': '𝗨', 'V': '𝗩', 'W': '𝗪', 'X': '𝗫', 'Y': '𝗬',
        'Z': '𝗭',
        'a': '𝗮', 'b': '𝗯', 'c': '𝗰', 'd': '𝗱', 'e': '𝗲',
        'f': '𝗳', 'g': '𝗴', 'h': '𝗵', 'i': '𝗶', 'j': '𝗷',
        'k': '𝗸', 'l': '𝗹', 'm': '𝗺', 'n': '𝗻', 'o': '𝗼',
        'p': '𝗽', 'q': '𝗾', 'r': '𝗿', 's': '𝘀', 't': '𝘁',
        'u': '𝘂', 'v': '𝘃', 'w': '𝘄', 'x': '𝘅', 'y': '𝘆',
        'z': '𝘇',
        '0': '𝟬', '1': '𝟭', '2': '𝟮', '3': '𝟯', '4': '𝟰',
        '5': '𝟱', '6': '𝟲', '7': '𝟳', '8': '𝟴', '9': '𝟵',
    }
    return ''.join(mapping.get(ch, ch) for ch in text)

def bold_button(text, **kwargs):
    """Create an InlineKeyboardButton with mathematical bold text."""
    return InlineKeyboardButton(math_bold(text), **kwargs)

# ================================================================================
#                          LOADING ANIMATION
# ================================================================================

async def loading_animation(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    duration: float = 1.5
) -> Optional[int]:
    """Send a loading message with increasing exclamation marks, then delete it."""
    try:
        msg = await context.bot.send_message(chat_id, "!")
        for i in range(2, 5):
            await asyncio.sleep(0.3)
            await msg.edit_text("!" * i)
        await asyncio.sleep(duration)
        await msg.delete()
        return msg.message_id
    except Exception as e:
        logger.debug(f"Loading animation failed: {e}")
        return None

# ================================================================================
#                          MAINTENANCE BLOCK MESSAGE
# ================================================================================

async def _send_maintenance_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a nicely formatted maintenance block message to a new user."""
    backup_url = get_setting("backup_channel_url", "")
    text = (
        "🔧 <b>Bot Under Maintenance</b>\n\n"
        "<blockquote><b>We are currently performing scheduled maintenance.</b>\n"
        "<b>Existing members can still use the bot normally.</b></blockquote>\n\n"
        "<b>Please join our backup channel to stay updated.</b>"
    )
    keyboard = []
    if backup_url:
        keyboard.append([bold_button(" Backup Channel", url=backup_url)])

    if update.message:
        await update.message.reply_text(
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
        )
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
        except Exception:
            await context.bot.send_message(
                update.effective_chat.id,
                text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )

# ================================================================================
#                          MESSAGE DELETION HELPERS
# ================================================================================

async def delete_update_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete the user's message (unless it's a start command or admin is in a broadcast state)."""
    user_id = update.effective_user.id
    # Do not delete if admin is in broadcast conversation
    if user_id == ADMIN_ID and user_states.get(user_id) in (
        PENDING_BROADCAST,
        PENDING_BROADCAST_OPTIONS,
        PENDING_BROADCAST_CONFIRM
    ):
        return
    if update.message:
        if update.message.text and update.message.text.startswith('/start'):
            return
        try:
            await update.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete message: {e}")

async def delete_bot_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> Optional[int]:
    """Delete the previously sent bot prompt (if any) stored in user_data."""
    prompt_id = context.user_data.pop('bot_prompt_message_id', None)
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prompt_id)
        except Exception as e:
            logger.warning(f"Could not delete prompt {prompt_id}: {e}")
    return prompt_id

# ================================================================================
#                          FORCE SUBSCRIPTION LOGIC
# ================================================================================

async def is_user_subscribed(user_id: int, bot) -> bool:
    """Check if a user is subscribed to all active force‑subscription channels."""
    channels = get_all_force_sub_channels(return_usernames_only=True)
    if not channels:
        return True

    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=user_id)
            if member.status in ['left', 'kicked']:
                return False
        except Exception as e:
            # If this bot cannot check (e.g., not admin), try main bot if we are a clone
            if I_AM_CLONE:
                main_token = get_main_bot_token()
                if main_token:
                    main_bot = Bot(token=main_token)
                    try:
                        member = await main_bot.get_chat_member(chat_id=ch, user_id=user_id)
                        if member.status in ['left', 'kicked']:
                            return False
                    except Exception as e2:
                        logger.warning(f"Main bot also cannot check {ch}: {e2}")
                else:
                    logger.warning("No main bot token available for membership check.")
            else:
                logger.warning(f"Cannot check membership in {ch} (bot not admin?): {e}")
    return True

async def get_unsubscribed_channels(user_id: int, bot) -> List[Tuple[str, str, bool]]:
    """Return list of (username, title, jbr) for channels where user is NOT subscribed."""
    channels_info = get_all_force_sub_channels(return_usernames_only=False)
    unsubscribed = []
    for uname, title, jbr in channels_info:
        try:
            member = await bot.get_chat_member(chat_id=uname, user_id=user_id)
            if member.status in ['left', 'kicked']:
                unsubscribed.append((uname, title, jbr))
        except Exception as e:
            # If this bot cannot check (e.g., not admin), try main bot if we are a clone
            if I_AM_CLONE:
                main_token = get_main_bot_token()
                if main_token:
                    main_bot = Bot(token=main_token)
                    try:
                        member = await main_bot.get_chat_member(chat_id=uname, user_id=user_id)
                        if member.status in ['left', 'kicked']:
                            unsubscribed.append((uname, title, jbr))
                    except Exception as e2:
                        logger.warning(f"Main bot also cannot check {uname}: {e2}")
                        # Assume not subscribed to be safe
                        unsubscribed.append((uname, title, jbr))
                else:
                    unsubscribed.append((uname, title, jbr))
            else:
                logger.warning(f"Cannot check membership in {uname} (bot not admin?): {e}")
                # Assume not subscribed to be safe
                unsubscribed.append((uname, title, jbr))
    return unsubscribed

def force_sub_required(func):
    """Decorator to enforce force‑subscription before allowing command execution."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user is None:
            return await func(update, context, *args, **kwargs)

        # Maintenance mode: only existing users and admin can proceed
        if user.id != ADMIN_ID and is_maintenance_mode():
            if not is_existing_user(user.id):
                await delete_update_message(update, context)
                await _send_maintenance_block(update, context)
                return

        # Ban check
        if is_user_banned(user.id):
            await delete_update_message(update, context)
            ban_text = "🚫 <b>You have been banned from using this bot.</b>"
            if update.message:
                await update.message.reply_text(ban_text)
            elif update.callback_query:
                try:
                    await update.callback_query.edit_message_text(ban_text)
                except Exception:
                    await context.bot.send_message(update.effective_chat.id, ban_text)
            return

        # Admin bypasses force‑sub
        if user.id == ADMIN_ID:
            return await func(update, context, *args, **kwargs)

        # Check force‑sub channels
        unsubscribed = await get_unsubscribed_channels(user.id, context.bot)
        if unsubscribed:
            await delete_update_message(update, context)
            total_channels = len(get_all_force_sub_channels(return_usernames_only=False))
            unjoined = len(unsubscribed)

            # Build buttons for each unjoined channel (mathematical bold)
            keyboard = []
            for uname, title, jbr in unsubscribed:
                clean = uname.lstrip('@')
                keyboard.append([bold_button(title, url=f"https://t.me/{clean}")])

            # Add the "TRY AGAIN" button
            keyboard.append([bold_button("TRY AGAIN", callback_data="verify_subscription")])

            # Compose the message exactly as in your screenshot
            text = (
                f"HEY, {user.first_name} ✨ YOU HAVEN'T JOINED {unjoined}/{total_channels} CHANNELS YET. "
                f"PLEASE JOIN THE CHANNELS PROVIDED BELOW, THEN TRY AGAIN... ! \n\n"
                f"⚠️ FACING PROBLEMS, USE: /help"
            )

            if update.message:
                await update.message.reply_text(
                    text,
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            elif update.callback_query:
                await update.callback_query.edit_message_text(
                    text,
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return

        return await func(update, context, *args, **kwargs)
    return wrapper
  

# ================================================================================
#                                PING COMMAND
# ================================================================================

@force_sub_required
async def ping_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Simple ping command to check bot responsiveness.
    Measures round‑trip time.
    """
    start = time.time()
    try:
        msg = await update.message.reply_text("🏓 Pong...")
        elapsed = (time.time() - start) * 1000
        await msg.edit_text(f"🏓 Pong : {elapsed:.0f} ms")
    except Exception as e:
        logger.error(f"Ping command failed: {e}")
        await update.message.reply_text("❌ Error checking ping.")

# ================================================================================
#                            SYSTEM STATISTICS
# ================================================================================

def get_uptime() -> str:
    """Return human‑readable uptime since bot start."""
    delta = timedelta(seconds=int(time.time() - BOT_START_TIME))
    return str(delta).split('.')[0]

def get_db_size() -> str:
    """Return PostgreSQL database size in appropriate units."""
    try:
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT pg_database_size(current_database())")
            size_bytes = cur.fetchone()[0]
            for unit in ['B', 'KB', 'MB', 'GB']:
                if size_bytes < 1024:
                    return f"{size_bytes:.2f} {unit}"
                size_bytes /= 1024
            return f"{size_bytes:.2f} TB"
    except Exception as e:
        logger.error(f"Error getting DB size: {e}")
        return "Error"

def get_disk_usage() -> str:
    """Return free disk space on the root filesystem."""
    try:
        usage = psutil.disk_usage('/')
        free = usage.free
        for unit in ['B', 'KB', 'MB', 'GB']:
            if free < 1024:
                return f"{free:.2f} {unit}"
            free /= 1024
        return f"{free:.2f} TB"
    except Exception:
        return "N/A"

def get_cpu_usage() -> str:
    """Return current CPU usage percentage."""
    try:
        return f"{psutil.cpu_percent(interval=1)}%"
    except Exception:
        return "N/A"

def get_memory_usage() -> str:
    """Return memory usage percentage and used GB."""
    try:
        mem = psutil.virtual_memory()
        return f"{mem.percent}% (used: {mem.used / (1024**3):.1f}GB)"
    except Exception:
        return "N/A"

def get_render_info() -> dict:
    """Return Render.com environment information."""
    return {
        'instance': os.getenv('RENDER_INSTANCE_ID', 'N/A'),
        'service': os.getenv('RENDER_SERVICE_NAME', 'N/A'),
        'region': os.getenv('RENDER_REGION', 'N/A'),
        'free_tier': os.getenv('RENDER_FREE_TIER', 'false').lower() == 'true',
    }

@force_sub_required
async def sysstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Display detailed system statistics: uptime, CPU, memory, disk, database size,
    and Render.com info (if applicable).
    """
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    uptime = get_uptime()
    db_size = get_db_size()
    disk_free = get_disk_usage()
    cpu = get_cpu_usage()
    mem = get_memory_usage()
    render = get_render_info()

    text = small_caps(
        f"System Statistics\n\n"
        f"Uptime: {uptime}\n"
        f"CPU: {cpu}\n"
        f"Memory: {mem}\n"
        f"Database Size: {db_size}\n"
        f"Free Disk Space: {disk_free}\n"
        f"Render Instance: {render['instance']}\n"
        f"Service: {render['service']}\n"
        f"Region: {render['region']}\n"
        f"Free Tier: {'Yes' if render['free_tier'] else 'No'}\n\n"
        f"Note: Bandwidth information is not available via public API."
    )
    keyboard = [[bold_button("🔙 BACK", callback_data="admin_back")]]
    await update.message.reply_text(
        text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ================================================================================
#                              ANILIST CLIENT
# ================================================================================

class AniListClient:
    """
    Client for the AniList GraphQL API.
    Provides methods to search for anime/manga and retrieve by ID.
    """
    BASE_URL = "https://graphql.anilist.co"

    @staticmethod
    def search_anime(query: str) -> Optional[Dict]:
        q = '''
        query ($search: String) {
          Media(search: $search, type: ANIME) {
            id
            title { romaji english native }
            description
            coverImage { extraLarge large medium }
            bannerImage
            format
            status
            episodes
            duration
            averageScore
            genres
            studios(isMain: true) { nodes { name } }
            startDate { year month day }
            endDate { year month day }
            nextAiringEpisode { episode timeUntilAiring }
          }
        }
        '''
        return AniListClient._query(q, {'search': query})

    @staticmethod
    def search_manga(query: str) -> Optional[Dict]:
        q = '''
        query ($search: String) {
          Media(search: $search, type: MANGA) {
            id
            title { romaji english native }
            description
            coverImage { extraLarge large medium }
            bannerImage
            format
            status
            chapters
            volumes
            averageScore
            genres
            startDate { year month day }
            endDate { year month day }
          }
        }
        '''
        return AniListClient._query(q, {'search': query})

    @staticmethod
    def get_anime_by_id(media_id: int) -> Optional[Dict]:
        q = '''
        query ($id: Int) {
          Media(id: $id, type: ANIME) {
            id
            title { romaji english native }
            description
            coverImage { extraLarge large medium }
            bannerImage
            format
            status
            episodes
            duration
            averageScore
            genres
            studios(isMain: true) { nodes { name } }
            startDate { year month day }
            endDate { year month day }
            nextAiringEpisode { episode timeUntilAiring }
          }
        }
        '''
        return AniListClient._query(q, {'id': media_id})

    @staticmethod
    def get_manga_by_id(media_id: int) -> Optional[Dict]:
        q = '''
        query ($id: Int) {
          Media(id: $id, type: MANGA) {
            id
            title { romaji english native }
            description
            coverImage { extraLarge large medium }
            bannerImage
            format
            status
            chapters
            volumes
            averageScore
            genres
            startDate { year month day }
            endDate { year month day }
          }
        }
        '''
        return AniListClient._query(q, {'id': media_id})

    @staticmethod
    def _query(query: str, variables: dict) -> Optional[Dict]:
        try:
            resp = requests.post(
                AniListClient.BASE_URL,
                json={'query': query, 'variables': variables},
                headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get('data', {}).get('Media')
            else:
                api_logger.error(f"AniList error {resp.status_code}: {resp.text}")
                return None
        except Exception as e:
            api_logger.error(f"AniList request failed: {e}")
            return None

# ================================================================================
#                               TMDB CLIENT
# ================================================================================

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    @staticmethod
    def search_movie(query: str) -> Optional[Dict]:
        if not TMDB_API_KEY:
            return None
        try:
            resp = requests.get(
                f"{TMDBClient.BASE_URL}/search/movie",
                params={'api_key': TMDB_API_KEY, 'query': query},
                timeout=10
            )
            if resp.status_code == 200:
                results = resp.json().get('results', [])
                if results:
                    return TMDBClient._get_movie_details(results[0]['id'])
            return None
        except Exception as e:
            api_logger.error(f"TMDB search error: {e}")
            return None

    @staticmethod
    def search_tv(query: str) -> Optional[Dict]:
        if not TMDB_API_KEY:
            return None
        try:
            resp = requests.get(
                f"{TMDBClient.BASE_URL}/search/tv",
                params={'api_key': TMDB_API_KEY, 'query': query},
                timeout=10
            )
            if resp.status_code == 200:
                results = resp.json().get('results', [])
                if results:
                    return TMDBClient._get_tv_details(results[0]['id'])
            return None
        except Exception as e:
            api_logger.error(f"TMDB search error: {e}")
            return None

    @staticmethod
    def _get_movie_details(movie_id: int) -> Dict:
        resp = requests.get(
            f"{TMDBClient.BASE_URL}/movie/{movie_id}",
            params={'api_key': TMDB_API_KEY, 'append_to_response': 'credits,images'},
            timeout=10
        )
        return resp.json()

    @staticmethod
    def _get_tv_details(tv_id: int) -> Dict:
        resp = requests.get(
            f"{TMDBClient.BASE_URL}/tv/{tv_id}",
            params={'api_key': TMDB_API_KEY, 'append_to_response': 'credits,images'},
            timeout=10
        )
        return resp.json()

# ================================================================================
#                            MANGADEX CLIENT
# ================================================================================

class MangaDexClient:
    BASE_URL = "https://api.mangadex.org"

    @staticmethod
    def search_manga(title: str) -> Optional[Dict]:
        try:
            resp = requests.get(
                f"{MangaDexClient.BASE_URL}/manga",
                params={'title': title},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                if data['data']:
                    return data['data'][0]
            return None
        except Exception as e:
            api_logger.error(f"MangaDex search error: {e}")
            return None

    @staticmethod
    def get_latest_chapter(manga_id: str) -> Optional[Dict]:
        try:
            resp = requests.get(
                f"{MangaDexClient.BASE_URL}/chapter",
                params={'manga': manga_id, 'limit': 1, 'order[chapter]': 'desc'},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                if data['data']:
                    return data['data'][0]
            return None
        except Exception as e:
            api_logger.error(f"MangaDex chapter fetch error: {e}")
            return None

    @staticmethod
    async def get_chapter_pages(chapter_id: str) -> Optional[Tuple[List[str], List[str]]]:
        """
        Returns (page_urls, page_filenames) for the chapter.
        """
        try:
            # Get at-home server
            resp = requests.get(
                f"{MangaDexClient.BASE_URL}/at-home/server/{chapter_id}",
                timeout=10
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            base_url = data['baseUrl']
            chapter_hash = data['chapter']['hash']
            pages = data['chapter']['data']
            page_urls = [f"{base_url}/data/{chapter_hash}/{p}" for p in pages]
            return page_urls, pages
        except Exception as e:
            api_logger.error(f"Error getting chapter pages: {e}")
            return None

# ================================================================================
#                           POST GENERATION ENGINE
# ================================================================================

async def fetch_media_and_generate_post(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    category: str,
    search_query: str = "",
    media_id: int = None
):
    """
    Unified post generator.
    Fetches data from appropriate API (AniList/TMDB) using either a search query or an ID,
    then builds a caption using category settings and sends it with a poster.
    If watermark is enabled, applies it to the image.
    """
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    # Show loading animation
    await loading_animation(update, context, update.effective_chat.id)

    # Fetch data
    data = None
    if category == 'anime':
        if media_id:
            data = AniListClient.get_anime_by_id(media_id)
        else:
            data = AniListClient.search_anime(search_query)
    elif category == 'manga':
        if media_id:
            data = AniListClient.get_manga_by_id(media_id)
        else:
            data = AniListClient.search_manga(search_query)
    elif category == 'movie':
        data = TMDBClient.search_movie(search_query)
    elif category == 'tvshow':
        data = TMDBClient.search_tv(search_query)

    if not data:
        await update.message.reply_text(small_caps("No results found."))
        return

    # Get category‑specific settings from database
    settings = get_category_settings(category)

    # Build caption from template or use default
    caption_template = settings.get('caption_template', '')
    if not caption_template:
        # Default caption based on category
        if category in ('anime', 'manga'):
            caption_template = (
                "<b>{title}</b>\n\n"
                "» <b>Type:</b> <code>{type}</code>\n"
                "» <b>Rating:</b> <code>{rating}</code>\n"
                "» <b>Status:</b> <code>{status}</code>\n"
                "» <b>Genres:</b> {genres}\n\n"
                "<u>SYNOPSIS</u>\n"
                "<blockquote expandable>{synopsis}</blockquote>"
            )
        else:
            caption_template = (
                "<b>{title}</b>\n\n"
                "» <b>Release Date:</b> {release_date}\n"
                "» <b>Rating:</b> {rating}\n"
                "» <b>Genres:</b> {genres}\n\n"
                "<u>OVERVIEW</u>\n"
                "<blockquote expandable>{overview}</blockquote>"
            )

    # Prepare placeholders dictionary
    if category in ('anime', 'manga'):
        title = data.get('title', {}).get('romaji') or data.get('title', {}).get('english') or search_query
        media_type = data.get('format', 'Unknown')
        rating = data.get('averageScore', 'N/A')
        status = data.get('status', 'Unknown')
        genres = ', '.join(data.get('genres', []))
        synopsis = data.get('description', 'No description.') or 'No description.'
        synopsis = re.sub(r'<[^>]+>', '', synopsis)  # strip HTML tags
        chapters = data.get('chapters', 'N/A')
        episodes = data.get('episodes', 'N/A')
        placeholders = {
            '{title}': html.escape(title),
            '{type}': html.escape(media_type),
            '{rating}': html.escape(str(rating)),
            '{status}': html.escape(status),
            '{genres}': html.escape(genres),
            '{synopsis}': html.escape(synopsis),
            '{chapters}': html.escape(str(chapters)),
            '{episodes}': html.escape(str(episodes)),
        }
        poster = data.get('coverImage', {}).get('large')
    else:
        # TMDB data
        title = data.get('title') or data.get('name') or search_query
        release_date = data.get('release_date') or data.get('first_air_date', 'Unknown')
        rating = data.get('vote_average', 'N/A')
        genres_list = [g['name'] for g in data.get('genres', [])]
        genres = ', '.join(genres_list)
        overview = data.get('overview', 'No overview.')
        placeholders = {
            '{title}': html.escape(title),
            '{release_date}': html.escape(release_date),
            '{rating}': html.escape(str(rating)),
            '{genres}': html.escape(genres),
            '{overview}': html.escape(overview),
        }
        poster = data.get('poster_path')
        if poster:
            poster = f"https://image.tmdb.org/t/p/w500{poster}"

    # Replace placeholders in template
    for key, value in placeholders.items():
        caption_template = caption_template.replace(key, value)

    # Build inline keyboard buttons from settings
    buttons = settings.get('buttons', [])
    keyboard = []
    for btn in buttons:
        text = btn.get('text', 'Link')
        url = btn.get('url', '').replace('{link}', '')
        # Handle colour prefixes for button text
        if text.startswith('#g '):
            text = '🟢 ' + text[3:]
        elif text.startswith('#r '):
            text = '🔴 ' + text[3:]
        elif text.startswith('#p '):
            text = '🔵 ' + text[3:]
        if url:
            keyboard.append([bold_button(text, url=url)])

    # Append branding if set
    branding = settings.get('branding', '')
    if branding:
        caption_template += f"\n\n{branding}"

    # Apply font style (small caps conversion if selected)
    font_style = settings.get('font_style', 'normal')
    if font_style == 'smallcaps':
        caption_template = small_caps(caption_template)

    # --- Watermark handling ---
    watermark_text = settings.get('watermark_text')
    watermark_pos = settings.get('watermark_position', 'center')
    if poster and watermark_text:
        try:
            watermarked = await add_watermark(poster, watermark_text, watermark_pos)
            if watermarked:
                await context.bot.send_photo(
                    chat_id=update.effective_chat.id,
                    photo=watermarked,
                    caption=caption_template,
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
                )
                # Save to cache
                with db_manager.get_cursor() as cur:
                    cur.execute("""
                        INSERT INTO posts_cache (category, title, anilist_id, media_data)
                        VALUES (%s, %s, %s, %s)
                    """, (category, title, data.get('id'), json.dumps(data)))
                return
        except Exception as e:
            logger.error(f"Watermark failed: {e}, sending original")
            # Fall through to send original

    # Send the post with poster if available, otherwise plain text
    if poster:
        try:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=poster,
                caption=caption_template,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
        except Exception as e:
            logger.error(f"Failed to send photo: {e}")
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=caption_template,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
            )
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=caption_template,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None
        )

    # Save to posts cache for future reference
    with db_manager.get_cursor() as cur:
        cur.execute("""
            INSERT INTO posts_cache (category, title, anilist_id, media_data)
            VALUES (%s, %s, %s, %s)
        """, (category, title, data.get('id'), json.dumps(data)))

# ================================================================================
#                      CATEGORY POST COMMANDS
# ================================================================================

@force_sub_required
async def manga_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate a manga post using AniList."""
    if not context.args:
        await update.message.reply_text(small_caps("Usage: /manga <name>"))
        return
    query = ' '.join(context.args)
    await fetch_media_and_generate_post(update, context, 'manga', query)

@force_sub_required
async def anime_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate an anime post using AniList."""
    if not context.args:
        await update.message.reply_text(small_caps("Usage: /anime <name>"))
        return
    query = ' '.join(context.args)
    await fetch_media_and_generate_post(update, context, 'anime', query)

@force_sub_required
async def movie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate a movie post using TMDB."""
    if not context.args:
        await update.message.reply_text(small_caps("Usage: /movie <name>"))
        return
    query = ' '.join(context.args)
    await fetch_media_and_generate_post(update, context, 'movie', query)

@force_sub_required
async def tvshow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate a TV show post using TMDB."""
    if not context.args:
        await update.message.reply_text(small_caps("Usage: /tvshow <name>"))
        return
    query = ' '.join(context.args)
    await fetch_media_and_generate_post(update, context, 'tvshow', query)

# ================================================================================
#                         CATEGORY SETTINGS MANAGEMENT
# ================================================================================

def get_category_settings(category: str) -> Dict:
    """Retrieve all settings for a given category from the database."""
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
                'template_name': row[0] or 'template1',
                'branding': row[1] or '',
                'buttons': json.loads(row[2]) if row[2] else [],
                'caption_template': row[3] or '',
                'thumbnail_url': row[4] or '',
                'font_style': row[5] or 'normal',
                'logo_file_id': row[6],
                'logo_position': row[7] or 'bottom',
                'watermark_text': row[8],
                'watermark_position': row[9] or 'center',
            }
        else:
            # Insert default settings for this category
            cur.execute("""
                INSERT INTO category_settings (category, template_name, branding, buttons, caption_template,
                                               thumbnail_url, font_style, logo_file_id, logo_position,
                                               watermark_text, watermark_position)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (category, 'template1', '', '[]', '', '', 'normal', None, 'bottom', None, 'center'))
            return {
                'template_name': 'template1',
                'branding': '',
                'buttons': [],
                'caption_template': '',
                'thumbnail_url': '',
                'font_style': 'normal',
                'logo_file_id': None,
                'logo_position': 'bottom',
                'watermark_text': None,
                'watermark_position': 'center',
            }

# Database update functions for category settings
def update_category_template(category: str, template: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET template_name = %s WHERE category = %s", (template, category))

def update_category_branding(category: str, branding: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET branding = %s WHERE category = %s", (branding, category))

def update_category_buttons(category: str, buttons_json: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET buttons = %s WHERE category = %s", (buttons_json, category))

def update_category_caption(category: str, caption: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET caption_template = %s WHERE category = %s", (caption, category))

def update_category_thumbnail(category: str, thumbnail_url: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET thumbnail_url = %s WHERE category = %s", (thumbnail_url, category))

def update_category_font(category: str, font_style: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET font_style = %s WHERE category = %s", (font_style, category))

def update_category_logo(category: str, logo_file_id: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET logo_file_id = %s WHERE category = %s", (logo_file_id, category))

def update_category_logo_position(category: str, position: str):
    with db_manager.get_cursor() as cur:
        cur.execute("UPDATE category_settings SET logo_position = %s WHERE category = %s", (position, category))

# ────────────────────────────────────────────────────────────────────────────────
#                           CATEGORY SETTINGS COMMAND
# ────────────────────────────────────────────────────────────────────────────────

@force_sub_required
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry point for category settings."""
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    keyboard = [
        [bold_button(" TV Shows", callback_data="settings_category_tvshow"),
         bold_button(" Movies", callback_data="settings_category_movie")],
        [bold_button(" Anime", callback_data="settings_category_anime"),
         bold_button(" Manga", callback_data="settings_category_manga")],
        [bold_button("🔙 BACK", callback_data="admin_back")]
    ]
    text = small_caps("Select category to configure:")
    if SETTINGS_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=SETTINGS_IMAGE_URL,
                caption=text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            await context.bot.send_message(
                update.effective_chat.id,
                text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    else:
        await context.bot.send_message(
            update.effective_chat.id,
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def show_category_settings(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    """Display the current settings for a specific category with edit buttons."""
    query = update.callback_query
    await query.answer()
    settings = get_category_settings(category)
    text = small_caps(
        f"{category.upper()} SETTINGS\n\n"
        f"• Template: {settings['template_name']}\n"
        f"• Branding: {settings['branding'] or 'Not set'}\n"
        f"• Buttons: {len(settings['buttons'])} configured\n"
        f"• Caption: {settings['caption_template'][:50]}...\n"
        f"• Thumbnail: {settings['thumbnail_url'] or 'Default'}\n"
        f"• Font Style: {settings['font_style']}\n"
        f"• Logo: {'Set' if settings['logo_file_id'] else 'Not set'} (position: {settings['logo_position']})"
    )
    keyboard = [
        [bold_button(" Set Template", callback_data=f"set_template_{category}"),
         bold_button("🏷 Set Branding", callback_data=f"set_branding_{category}")],
        [bold_button(" Configure Buttons", callback_data=f"set_buttons_{category}"),
         bold_button(" Set Caption", callback_data=f"set_caption_{category}")],
        [bold_button(" Set Thumbnail", callback_data=f"set_thumbnail_{category}"),
         bold_button(" Font Style", callback_data=f"set_font_{category}")],
        [bold_button(" Set Logo", callback_data=f"set_logo_{category}"),
         bold_button(" Logo Position", callback_data=f"set_logo_pos_{category}")],
        [bold_button("🔙 BACK", callback_data="admin_back")]
    ]
    try:
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception:
        await context.bot.send_message(
            query.message.chat_id,
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ================================================================================
#                              AUTO‑FORWARD SYSTEM
# ================================================================================

@force_sub_required
async def autoforward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu for auto‑forward configuration."""
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    with db_manager.get_cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM auto_forward_connections WHERE active = TRUE")
        active_count = cur.fetchone()[0]
    status = "ON" if active_count > 0 else "OFF"

    text = small_caps(
        f"AUTO‑FORWARD MODE\n\n"
        f"STATUS: {status}\n"
        f"ACTIVE CONNECTIONS: {active_count}"
    )
    keyboard = [
        [bold_button("➕ Add Connection", callback_data="af_add_connection")],
        [bold_button(" Manage Connections", callback_data="af_manage_connections"),
         bold_button("⚙️ Settings", callback_data="af_settings")],
        [bold_button(" Filters/Words", callback_data="af_filters"),
         bold_button("♻️ Replacements", callback_data="af_replacements")],
        [bold_button("⏱ Delay/Caption", callback_data="af_delay_caption"),
         bold_button(" Bulk Forward Old Posts", callback_data="af_bulk")],
        [bold_button("🔙 BACK", callback_data="admin_back")]
    ]
    await context.bot.send_message(
        update.effective_chat.id,
        text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ────────────────────────────────────────────────────────────────────────────────
# Real‑time auto‑forward handler (for channel posts)
# ────────────────────────────────────────────────────────────────────────────────

async def auto_forward_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    When a message arrives in a source channel, forward/copy it to all active target channels.
    Applies filters, replacements, and delay if configured.
    """
    if not update.channel_post:
        return
    chat_id = update.effective_chat.id
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT id, target_chat_id, protect_content, silent, keep_tag,
                   pin_message, delete_source, delay_seconds
            FROM auto_forward_connections
            WHERE source_chat_id = %s AND active = TRUE
        """, (chat_id,))
        conn = cur.fetchone()
    if not conn:
        return
    conn_id, target, protect, silent, keep_tag, pin, delete_src, delay = conn

    # --- Fetch filters ---
    filters_row = None
    with db_manager.get_cursor() as cur:
        cur.execute("SELECT allowed_media, blacklist, whitelist FROM auto_forward_filters WHERE connection_id = %s", (conn_id,))
        filters_row = cur.fetchone()

    if filters_row:
        allowed_media, blacklist, whitelist = filters_row
        # Check media type
        if allowed_media:
            media_type = None
            if update.channel_post.photo:
                media_type = 'photo'
            elif update.channel_post.video:
                media_type = 'video'
            elif update.channel_post.document:
                media_type = 'document'
            elif update.channel_post.audio:
                media_type = 'audio'
            elif update.channel_post.voice:
                media_type = 'voice'
            elif update.channel_post.sticker:
                media_type = 'sticker'
            elif update.channel_post.animation:
                media_type = 'animation'
            elif update.channel_post.text:
                media_type = 'text'
            if media_type and media_type not in allowed_media:
                logger.debug(f"Message dropped: media type {media_type} not allowed")
                return

        # Check text against blacklist/whitelist
        text_to_check = update.channel_post.caption or update.channel_post.text or ''
        if whitelist and not any(word.lower() in text_to_check.lower() for word in whitelist):
            logger.debug("Message dropped: no whitelist word found")
            return
        if blacklist and any(word.lower() in text_to_check.lower() for word in blacklist):
            logger.debug("Message dropped: blacklist word found")
            return

    # --- Fetch replacements ---
    replacements = []
    with db_manager.get_cursor() as cur:
        cur.execute("SELECT old_pattern, new_pattern FROM auto_forward_replacements WHERE connection_id = %s", (conn_id,))
        replacements = cur.fetchall()

    # If we have replacements and the message has text/caption, we need to create a new message
    # because copy_message cannot edit caption.
    if replacements and (update.channel_post.text or update.channel_post.caption):
        # We'll handle by re‑uploading media or sending as text
        text_content = update.channel_post.text or update.channel_post.caption or ''
        new_text = text_content
        for old, new in replacements:
            new_text = new_text.replace(old, new)

        # If it's a simple text message, just send new text
        if not update.channel_post.photo and not update.channel_post.video and not update.channel_post.document:
            try:
                if delay > 0:
                    context.job_queue.run_once(
                        delayed_forward_text_job,
                        when=delay,
                        data={
                            'target_chat_id': target,
                            'text': new_text,
                            'protect': protect,
                            'silent': silent,
                            'pin': pin
                        }
                    )
                else:
                    msg = await context.bot.send_message(
                        chat_id=target,
                        text=new_text,
                        disable_notification=silent,
                        protect_content=protect
                    )
                    if pin:
                        await context.bot.pin_chat_message(chat_id=target, message_id=msg.message_id)
                if delete_src:
                    await update.channel_post.delete()
                return
            except Exception as e:
                logger.error(f"Error sending text with replacements: {e}")
                return
        else:
            # For media, we need to download and re‑upload with new caption
            # This is complex; we'll skip for now and log
            logger.warning("Replacements for media not implemented yet, forwarding original")
            # fall through to normal copy

    # No replacements or not applicable – use copy_message
    try:
        if delay > 0:
            context.job_queue.run_once(
                delayed_forward_job,
                when=delay,
                data={
                    'from_chat_id': chat_id,
                    'message_id': update.channel_post.message_id,
                    'target_chat_id': target,
                    'protect': protect,
                    'silent': silent,
                    'pin': pin,
                    'delete_src': delete_src
                }
            )
        else:
            new_msg = await context.bot.copy_message(
                chat_id=target,
                from_chat_id=chat_id,
                message_id=update.channel_post.message_id,
                protect_content=protect,
                disable_notification=silent
            )
            if pin:
                await context.bot.pin_chat_message(chat_id=target, message_id=new_msg.message_id)
            if delete_src:
                await update.channel_post.delete()
    except Exception as e:
        logger.error(f"Auto‑forward copy failed: {e}")

async def delayed_forward_job(context: ContextTypes.DEFAULT_TYPE):
    """Job that performs the delayed copy."""
    data = context.job.data
    try:
        new_msg = await context.bot.copy_message(
            chat_id=data['target_chat_id'],
            from_chat_id=data['from_chat_id'],
            message_id=data['message_id'],
            protect_content=data['protect'],
            disable_notification=data['silent']
        )
        if data['pin']:
            await context.bot.pin_chat_message(chat_id=data['target_chat_id'], message_id=new_msg.message_id)
        if data['delete_src']:
            await context.bot.delete_message(chat_id=data['from_chat_id'], message_id=data['message_id'])
    except Exception as e:
        logger.error(f"Delayed forward job error: {e}")

async def delayed_forward_text_job(context: ContextTypes.DEFAULT_TYPE):
    """Job for delayed text message with replacements."""
    data = context.job.data
    try:
        msg = await context.bot.send_message(
            chat_id=data['target_chat_id'],
            text=data['text'],
            disable_notification=data['silent'],
            protect_content=data['protect']
        )
        if data['pin']:
            await context.bot.pin_chat_message(chat_id=data['target_chat_id'], message_id=msg.message_id)
    except Exception as e:
        logger.error(f"Delayed text forward error: {e}")

# ================================================================================
#                           AUTO MANGA UPDATE
# ================================================================================

@force_sub_required
async def autoupdate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main menu for auto manga update."""
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    text = small_caps("AUTO MANGA UPDATE\n\nManage manga titles to auto‑post new chapters.")
    keyboard = [
        [bold_button("➕ Add Manga", callback_data="manga_add")],
        [bold_button(" List Manga", callback_data="manga_list"),
         bold_button("🗑 Remove Manga", callback_data="manga_remove")],
        [bold_button("🔙 BACK", callback_data="admin_back")]
    ]
    await context.bot.send_message(
        update.effective_chat.id,
        text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ────────────────────────────────────────────────────────────────────────────────
# PDF Generation Helper
# ────────────────────────────────────────────────────────────────────────────────

async def download_image(session, url, filename):
    """Download an image and save it."""
    async with session.get(url) as resp:
        if resp.status == 200:
            f = await aiofiles.open(filename, 'wb')
            await f.write(await resp.read())
            await f.close()
            return True
    return False

async def create_manga_pdf(title: str, chapter_num: str, chapter_id: str, watermark_text: str = None) -> Optional[str]:
    """
    Download chapter pages and create a PDF.
    Returns the PDF filename if successful.
    """
    pages = await MangaDexClient.get_chapter_pages(chapter_id)
    if not pages:
        return None
    page_urls, page_names = pages
    temp_dir = f"temp_manga_{chapter_id}"
    os.makedirs(temp_dir, exist_ok=True)
    image_files = []
    try:
        async with aiohttp.ClientSession() as session:
            tasks = []
            for i, url in enumerate(page_urls):
                fname = os.path.join(temp_dir, f"page_{i:03d}.jpg")
                image_files.append(fname)
                tasks.append(download_image(session, url, fname))
            results = await asyncio.gather(*tasks)
            if not all(results):
                raise Exception("Some pages failed to download")

            # Apply watermark if needed
            if watermark_text:
                for fname in image_files:
                    try:
                        img = Image.open(fname).convert('RGBA')
                        txt = Image.new('RGBA', img.size, (255,255,255,0))
                        draw = ImageDraw.Draw(txt)
                        try:
                            font = ImageFont.truetype("arial.ttf", 36)
                        except:
                            font = ImageFont.load_default()
                        bbox = draw.textbbox((0,0), watermark_text, font=font)
                        text_w = bbox[2] - bbox[0]
                        text_h = bbox[3] - bbox[1]
                        pos = ((img.width - text_w)//2, img.height - text_h - 10)
                        draw.text(pos, watermark_text, fill=(255,255,255,128), font=font)
                        watermarked = Image.alpha_composite(img, txt)
                        watermarked.convert('RGB').save(fname, 'JPEG')
                    except Exception as e:
                        logger.error(f"Watermark error on {fname}: {e}")

            # Create PDF
            pdf_filename = f"{title} - Ch.{chapter_num}.pdf"
            with open(pdf_filename, "wb") as f:
                f.write(img2pdf.convert(image_files))
            return pdf_filename
    except Exception as e:
        logger.error(f"PDF creation error: {e}")
        return None
    finally:
        # Clean up temp files
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

# ────────────────────────────────────────────────────────────────────────────────
# Background job for manga auto‑update (runs every hour)
# ────────────────────────────────────────────────────────────────────────────────

async def manga_update_job(context: ContextTypes.DEFAULT_TYPE):
    """Periodically check for new chapters of tracked manga and post them."""
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT id, manga_title, manga_id, last_chapter, target_chat_id,
                   watermark, combine_pdf
            FROM manga_auto_update WHERE active = TRUE
        """)
        manga_list = cur.fetchall()

    for mid, title, manga_id, last_chap, target, watermark, combine_pdf in manga_list:
        try:
            if not manga_id:
                result = MangaDexClient.search_manga(title)
                if result:
                    manga_id = result['id']
                    with db_manager.get_cursor() as cur2:
                        cur2.execute("UPDATE manga_auto_update SET manga_id = %s WHERE id = %s",
                                     (manga_id, mid))
            if manga_id:
                latest = MangaDexClient.get_latest_chapter(manga_id)
                if latest and latest['attributes']['chapter'] != last_chap:
                    chap_num = latest['attributes']['chapter']
                    chap_title = latest['attributes']['title'] or f"Chapter {chap_num}"

                    if combine_pdf:
                        # Generate PDF
                        watermark_text = title if watermark else None
                        pdf_file = await create_manga_pdf(title, chap_num, latest['id'], watermark_text)
                        if pdf_file:
                            with open(pdf_file, 'rb') as f:
                                await context.bot.send_document(
                                    chat_id=target,
                                    document=f,
                                    filename=pdf_file,
                                    caption=f"📖 <b>{title}</b> – {chap_title}",
                                    parse_mode='HTML'
                                )
                            os.remove(pdf_file)
                        else:
                            # Fallback to link
                            await context.bot.send_message(
                                target,
                                f"📖 <b>{title}</b>\n\nNew chapter: {chap_title}\n\nRead: https://mangadex.org/chapter/{latest['id']}",
                                parse_mode='HTML'
                            )
                    else:
                        await context.bot.send_message(
                            target,
                            f"📖 <b>{title}</b>\n\nNew chapter: {chap_title}\n\nRead: https://mangadex.org/chapter/{latest['id']}",
                            parse_mode='HTML'
                        )

                    # Update last chapter in database
                    with db_manager.get_cursor() as cur2:
                        cur2.execute("UPDATE manga_auto_update SET last_chapter = %s, last_checked = NOW() WHERE id = %s",
                                     (chap_num, mid))
        except Exception as e:
            logger.error(f"Manga update error for {title}: {e}")

# ================================================================================
#                            GROUP SEARCH HANDLER
# ================================================================================

async def group_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """In groups, any text message (not a command) that looks like a search query
    will be used to search AniList for anime/manga and return a quick result."""
    if not update.message or not update.message.text:
        return
    if update.effective_chat.type == 'private':
        return
    text = update.message.text.strip()
    if text.startswith('/') or len(text) < 3 or len(text) > 100:
        return

    data = AniListClient.search_anime(text) or AniListClient.search_manga(text)
    if not data:
        return

    title = data.get('title', {}).get('romaji') or data.get('title', {}).get('english') or text
    cover = data.get('coverImage', {}).get('large')
    description = data.get('description', '')[:200] + '...' if data.get('description') else ''
    caption = f"<b>{title}</b>\n\n{description}"
    keyboard = [[bold_button("More Info", url=f"https://anilist.co/anime/{data['id']}" if data.get('id') else "")]]
    if cover:
        await update.message.reply_photo(
            photo=cover,
            caption=caption,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            caption,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ================================================================================
#                            INLINE SEARCH HANDLER
# ================================================================================

async def inline_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline queries (@botname query) and return matching anime/manga as inline results."""
    query = update.inline_query.query
    if not query or len(query) < 2:
        return

    results = []
    anime = AniListClient.search_anime(query)
    if anime:
        title = anime.get('title', {}).get('romaji') or anime.get('title', {}).get('english') or query
        desc = anime.get('description', '')[:100] + '...' if anime.get('description') else ''
        thumb = anime.get('coverImage', {}).get('medium')
        results.append({
            'type': 'article',
            'id': f"anime_{anime['id']}",
            'title': f"Anime: {title}",
            'description': desc,
            'thumb_url': thumb,
            'input_message_content': {
                'message_text': f"<b>{title}</b>\n\n{desc}\n\nhttps://anilist.co/anime/{anime['id']}",
                'parse_mode': 'HTML'
            }
        })
    manga = AniListClient.search_manga(query)
    if manga:
        title = manga.get('title', {}).get('romaji') or manga.get('title', {}).get('english') or query
        desc = manga.get('description', '')[:100] + '...' if manga.get('description') else ''
        thumb = manga.get('coverImage', {}).get('medium')
        results.append({
            'type': 'article',
            'id': f"manga_{manga['id']}",
            'title': f"Manga: {title}",
            'description': desc,
            'thumb_url': thumb,
            'input_message_content': {
                'message_text': f"<b>{title}</b>\n\n{desc}\n\nhttps://anilist.co/manga/{manga['id']}",
                'parse_mode': 'HTML'
            }
        })

    if results:
        await update.inline_query.answer(results, cache_time=300)

# ================================================================================
#                            FEATURE FLAGS
# ================================================================================

async def feature_flags_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display and allow toggling of global feature flags."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    if user_id != ADMIN_ID:
        return

    features = ['pdf_download', 'watermark', 'page_sizing', 'group_search', 'auto_forward']
    text = small_caps("Feature Flags – toggle for all users (or per user/group)")
    keyboard = []
    for f in features:
        enabled = feature_enabled(f, 0, 'global')
        status = '✅' if enabled else '❌'
        keyboard.append([bold_button(f"{status} {f}", callback_data=f"toggle_feature_{f}")])
    keyboard.append([bold_button("🔙 BACK", callback_data="admin_back")])
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

def feature_enabled(feature: str, entity_id: int, entity_type: str) -> bool:
    """Check if a feature is enabled for a given entity (global/user/group)."""
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT enabled FROM feature_flags
            WHERE feature_name = %s AND entity_id = %s AND entity_type = %s
        """, (feature, entity_id, entity_type))
        row = cur.fetchone()
        if row:
            return row[0]
        if entity_type == 'global':
            return True
        return feature_enabled(feature, 0, 'global')

# ================================================================================
#                           BROADCAST SYSTEM
# ================================================================================

async def broadcast_worker_job(context: ContextTypes.DEFAULT_TYPE):
    """Job that sends one chunk of a throttled broadcast."""
    jd = context.job.data
    offset = jd['offset']
    chunk_size = jd['chunk_size']
    msg_chat_id = jd['message_chat_id']
    msg_id = jd['message_id']
    is_last = jd['is_last_chunk']
    admin_cid = jd['admin_chat_id']
    broadcast_id = jd.get('broadcast_id')

    users = get_all_users(limit=chunk_size, offset=offset)
    sent = fail = blocked = deleted = 0
    for u in users:
        try:
            await context.bot.copy_message(
                chat_id=u[0],
                from_chat_id=msg_chat_id,
                message_id=msg_id
            )
            sent += 1
        except Forbidden as e:
            fail += 1
            if "blocked" in str(e).lower():
                blocked += 1
            elif "deactivated" in str(e).lower() or "deleted" in str(e).lower():
                deleted += 1
        except Exception as e:
            fail += 1
            broadcast_logger.warning(f"Broadcast fail {u[0]}: {e}")
        await asyncio.sleep(0.05)

    await context.bot.send_message(
        admin_cid,
        f"✅ Chunk {offset // chunk_size + 1}: sent {sent}, failed {fail}.",
        parse_mode='Markdown'
    )
    if is_last and broadcast_id:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                UPDATE broadcast_history
                SET completed_at = NOW(),
                    success = success + %s,
                    blocked = blocked + %s,
                    deleted = deleted + %s,
                    failed = failed + %s
                WHERE id = %s
            """, (sent, blocked, deleted, fail, broadcast_id))

async def broadcast_message_to_all_users(update, context, message_to_copy, mode=BroadcastMode.NORMAL):
    """Broadcast a message to all registered users."""
    admin_chat_id = update.effective_chat.id
    total = get_user_count()
    broadcast_id = None

    with db_manager.get_cursor() as cur:
        cur.execute("""
            INSERT INTO broadcast_history (admin_id, mode, total_users, message_text)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (admin_chat_id, mode, total, message_to_copy.text if message_to_copy.text else ''))
        broadcast_id = cur.fetchone()[0]

    if total < BROADCAST_MIN_USERS:
        await update.message.reply_text(f"♻️ Broadcasting to {total} users…")
        sent = fail = blocked = deleted = 0
        for u in get_all_users(limit=None, offset=0):
            try:
                await context.bot.copy_message(
                    chat_id=u[0],
                    from_chat_id=message_to_copy.chat_id,
                    message_id=message_to_copy.message_id
                )
                sent += 1
            except Forbidden as e:
                fail += 1
                if "blocked" in str(e).lower():
                    blocked += 1
                elif "deactivated" in str(e).lower():
                    deleted += 1
            except Exception as e:
                fail += 1
                broadcast_logger.warning(f"Broadcast fail {u[0]}: {e}")
            await asyncio.sleep(0.05)
        await context.bot.send_message(
            admin_chat_id,
            f"✅ Broadcast done. Sent: {sent}/{total} (Blocked: {blocked}, Deleted: {deleted})."
        )
        with db_manager.get_cursor() as cur:
            cur.execute("""
                UPDATE broadcast_history SET completed_at = NOW(), success = %s, blocked = %s, deleted = %s, failed = %s
                WHERE id = %s
            """, (sent, blocked, deleted, fail, broadcast_id))
        try:
            await update.message.delete()
        except Exception:
            pass
        return

    await update.message.reply_text(
        f"⏳ **Throttled Broadcast**\n"
        f"Total: {total} users, chunk: {BROADCAST_CHUNK_SIZE}, "
        f"interval: {BROADCAST_INTERVAL_MIN} min.",
        parse_mode='Markdown'
    )
    offset = delay = chunks = 0
    total_chunks = (total + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE
    while offset < total:
        is_last = (offset + BROADCAST_CHUNK_SIZE) >= total
        context.job_queue.run_once(
            broadcast_worker_job,
            when=delay,
            data={
                'offset': offset,
                'chunk_size': BROADCAST_CHUNK_SIZE,
                'message_chat_id': message_to_copy.chat_id,
                'message_id': message_to_copy.message_id,
                'is_last_chunk': is_last,
                'admin_chat_id': admin_chat_id,
                'broadcast_id': broadcast_id
            },
            name=f"bc_{chunks}"
        )
        offset += BROADCAST_CHUNK_SIZE
        delay += BROADCAST_INTERVAL_MIN * 60
        chunks += 1
    await update.message.reply_text(
        f"Scheduled **{total_chunks}** chunks over **{delay // 60} min**.",
        parse_mode='Markdown'
    )
    try:
        await update.message.delete()
    except Exception:
        pass

# ================================================================================
#                           BROADCAST STATS COMMAND
# ================================================================================

@force_sub_required
async def broadcaststats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show the last 10 broadcast history entries."""
    if update.effective_user.id != ADMIN_ID:
        return
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT id, mode, total_users, success, blocked, deleted, failed, started_at, completed_at
            FROM broadcast_history ORDER BY started_at DESC LIMIT 10
        """)
        rows = cur.fetchall()
    if not rows:
        await update.message.reply_text(small_caps("No broadcast history."))
        return
    text = small_caps("Last 10 broadcasts:\n\n")
    for r in rows:
        text += f"ID: {r[0]}, Mode: {r[1]}, Sent: {r[3]}/{r[2]}, Failed: {r[6]}, Blocked: {r[4]}, Deleted: {r[5]}\n"
    await update.message.reply_text(text, parse_mode='HTML')

# ================================================================================
#                           EXPORT USERS CSV
# ================================================================================

@force_sub_required
async def exportusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export all users to a CSV file and send as document."""
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    with db_manager.get_cursor() as cur:
        cur.execute("SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC")
        rows = cur.fetchall()

    if not rows:
        await update.message.reply_text("No users.")
        return

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(['user_id', 'username', 'first_name', 'last_name', 'joined_date', 'is_banned'])
    for r in rows:
        writer.writerow(r)
    output.seek(0)

    await update.message.reply_document(
        document=output,
        filename='users_export.csv',
        caption=small_caps(f"Exported {len(rows)} users.")
    )

# ================================================================================
#                           SEARCH COMMAND
# ================================================================================

@force_sub_required
async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search for anime/manga by name and let the user choose which one to generate."""
    if not context.args:
        await update.message.reply_text(small_caps("Usage: /search <name>"))
        return
    query = ' '.join(context.args)
    await loading_animation(update, context, update.effective_chat.id)

    anime = AniListClient.search_anime(query)
    manga = AniListClient.search_manga(query)

    if not anime and not manga:
        await update.message.reply_text(small_caps("No results found."))
        return

    keyboard = []
    if anime:
        keyboard.append([bold_button("Anime", callback_data=f"search_anime_{anime['id']}")])
    if manga:
        keyboard.append([bold_button("Manga", callback_data=f"search_manga_{manga['id']}")])
    await update.message.reply_text(
        small_caps("Found multiple. Choose one:"),
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

# ================================================================================
#                           ADMIN COMMAND LIST
# ================================================================================

@force_sub_required
async def cmd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all admin commands."""
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    commands_text = small_caps(
        "Admin Commands:\n\n"
        "/start – Main menu\n"
        "/cmd – This list\n"
        "/stats – Bot statistics\n"
        "/sysstats – System stats (uptime, DB size, etc.)\n"
        "/backup – List all generated links\n"
        "/move <target> – Move links to another bot\n"
        "/addclone <token> – Register a clone bot\n"
        "/reload – Restart bot (with optional message ID)\n"
        "/addchannel @user Title – Add force‑sub channel\n"
        "/removechannel @user – Remove force‑sub channel\n"
        "/banuser @id – Ban a user\n"
        "/unbanuser @id – Unban user\n"
        "/listusers – List users (paginated)\n"
        "/deleteuser <id> – Delete user from DB\n"
        "/broadcaststats – View broadcast history\n"
        "/exportusers – Export user list as CSV\n"
        "/settings – Open category settings\n"
        "/test – Health check\n"
        "/manga <name> – Create manga post\n"
        "/anime <name> – Create anime post\n"
        "/movie <name> – Create movie post\n"
        "/tvshow <name> – Create TV show post\n"
        "/help – Display help\n"
        "/autoupdate – Auto manga settings\n"
        "/autoforward – Manage auto‑forward\n"
        "/ping – Check response time\n"
        "/channel – Show indexed channels/groups\n"
        "/logs – Fetch latest bot logs\n"
        "/alive – Check bot server\n"
        "/users – Show total registered users\n"
        "/connect <group> – Connect a group\n"
        "/disconnect <group> – Disconnect a group\n"
        "/connections – List connected groups\n"
        "/id – Get Telegram IDs\n"
        "/info – Show user/chat info\n"
        "/restart – Alias for /reload\n"
        "/upload – Anime caption manager"
    )
    await update.message.reply_text(commands_text, parse_mode='HTML')

# ================================================================================
#                           HELP COMMAND
# ================================================================================

@force_sub_required
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Display help information for normal users."""
    user = update.effective_user
    await delete_update_message(update, context)
    user_states.pop(user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    help_text = small_caps(
        "<blockquote expandable>How to use the Bot:\n\n"
        "[1️] Create Posts:\n"
        "• Use /manga [name] for manga posts.\n"
        "• Use /movie [name] for movie posts.\n"
        "• Use /tvshows [name] for TV show posts.\n"
        "• Use /anime [name] for anime posts.\n\n"
        "[2️] Configure Settings:\n"
        "• Use /settings or the 'Settings' button to customize:\n"
        "  - Caption Format: Set placeholders like {title}, {season}, {episode}, etc.\n"
        "  - Buttons: Configure custom buttons using link && text.\n"
        "  - Templates: Choose your preferred thumbnail template.\n\n"
        "[3️] Templates & Thumbnails:\n"
        "• Select a template in settings for your category.\n"
        "• The bot will automatically use that template when creating a post.\n\n"
        "📚 Commands:\n"
        "• /start - Check if bot is alive\n"
        "• /settings - Open settings menu\n"
        "• /help - Display help\n"
        "• /stats - Get bot stats\n"
        "• /autoupdate - Auto Manga Settings\n"
        "• /restart - Restart the bot (Admins Only)</blockquote>"
    )

    if HELP_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=HELP_IMAGE_URL,
                caption=help_text,
                parse_mode='HTML'
            )
        except Exception:
            await context.bot.send_message(update.effective_chat.id, help_text, parse_mode='HTML')
    else:
        await context.bot.send_message(update.effective_chat.id, help_text, parse_mode='HTML')

# ================================================================================
#                               START COMMAND
# ================================================================================

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /start command."""
    user = update.effective_user
    chat_id = update.effective_chat.id

    if update.message and not context.args:
        await delete_update_message(update, context)

    await delete_bot_prompt(context, chat_id)

    add_user(user.id, user.username, user.first_name, user.last_name)

    await loading_animation(update, context, chat_id)

    if TRANSITION_STICKER:
        try:
            if TRANSITION_STICKER.startswith('http'):
                await context.bot.send_animation(chat_id, TRANSITION_STICKER)
            else:
                await context.bot.send_sticker(chat_id, TRANSITION_STICKER)
        except Exception as e:
            logger.warning(f"Failed to send transition sticker: {e}")

    if context.args and len(context.args) > 0:
        link_id = context.args[0]

        clone_redirect = get_setting("clone_redirect_enabled", "false").lower() == "true"
        if clone_redirect and not I_AM_CLONE and user.id != ADMIN_ID:
            clones = get_all_clone_bots(active_only=True)
            if clones:
                clone_uname = clones[0][2]
                clone_link = f"https://t.me/{clone_uname}?start={link_id}"
                await context.bot.send_message(
                    chat_id,
                    small_caps("Getting your link…"),
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup([[
                        bold_button("• Get Link •", url=clone_link)
                    ]])
                )
                return

        await handle_channel_link_deep(update, context, link_id)
        return

    if user.id == ADMIN_ID:
        user_states.pop(user.id, None)
        await send_admin_menu(chat_id, context)
    else:
        keyboard = [
            [bold_button("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
            [bold_button("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
            [bold_button("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
            [
                bold_button("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                bold_button("ᴄʟᴏsᴇ", callback_data="close_message")
            ]
        ]
        try:
            await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=WELCOME_SOURCE_CHANNEL,
                message_id=WELCOME_SOURCE_MESSAGE_ID,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Error copying welcome message: {e}")
            await context.bot.send_message(
                chat_id,
                small_caps("Welcome to the bot!"),
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

# ================================================================================
#                           ADMIN PANEL VIEWS
# ================================================================================

async def send_admin_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, query: Optional[CallbackQuery] = None):
    """Send the main admin panel menu."""
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass

    context.user_data.pop('bot_prompt_message_id', None)
    user_states.pop(chat_id, None)

    maint_label = "🔴 Maintenance: ON" if is_maintenance_mode() else "🟢 Maintenance: OFF"
    clone_label = "🔀 Clone Redirect: ON" if get_setting("clone_redirect_enabled", "false") == "true" else "🔀 Clone Redirect: OFF"

    keyboard = [
        [bold_button(" BOT STATS", callback_data="admin_stats"),
         bold_button(" SYSTEM STATS", callback_data="admin_sysstats")],
        [bold_button(" FORCE‑SUB CHANNELS", callback_data="manage_force_sub"),
         bold_button("🔗 GENERATE CHANNEL LINK", callback_data="generate_links")],
        [bold_button(" BROADCAST", callback_data="admin_broadcast_start"),
         bold_button("👤 USER MANAGEMENT", callback_data="user_management")],
        [bold_button(" CLONE BOTS", callback_data="manage_clones"),
         bold_button("⚙️ SETTINGS", callback_data="admin_settings")],
        [bold_button(" AUTO‑FORWARD", callback_data="admin_autoforward"),
         bold_button(" AUTO MANGA", callback_data="admin_autoupdate")],
        [bold_button(" FEATURE FLAGS", callback_data="admin_feature_flags"),
         bold_button("📤 UPLOAD MANAGER", callback_data="upload_menu")],
    ]
    text = small_caps(
        "ADMIN PANEL\n\n"
        f"{maint_label}\n{clone_label}"
    )

    if ADMIN_PANEL_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=ADMIN_PANEL_IMAGE_URL,
                caption=text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            await context.bot.send_message(
                chat_id,
                text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    else:
        await context.bot.send_message(
            chat_id,
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def send_admin_stats(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE):
    """Show bot statistics."""
    try:
        await query.delete_message()
    except Exception:
        pass

    user_count = get_user_count()
    channel_count = len(get_all_force_sub_channels())
    link_count = get_links_count()
    maint = "🔴 ON" if is_maintenance_mode() else "🟢 OFF"
    clones = get_all_clone_bots(active_only=True)
    blocked_users = get_blocked_users_count()

    stats_text = small_caps(
        "BOT STATISTICS\n\n"
        f"Total Users: {user_count}\n"
        f"Force‑Sub Channels: {channel_count}\n"
        f"Total Links: {link_count}\n"
        f"Active Clones: {len(clones)}\n"
        f"Blocked Users: {blocked_users}\n"
        f"Maintenance: {maint}\n"
        f"Link Expiry: {LINK_EXPIRY_MINUTES} min"
    )
    keyboard = [
        [bold_button(" ♻️ REFRESH", callback_data="admin_stats")],
        [bold_button("🔙 BACK", callback_data="admin_back")]
    ]
    if STATS_IMAGE_URL:
        try:
            await context.bot.send_photo(
                chat_id=query.message.chat_id,
                photo=STATS_IMAGE_URL,
                caption=stats_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            await context.bot.send_message(
                query.message.chat_id,
                stats_text,
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    else:
        await context.bot.send_message(
            query.message.chat_id,
            stats_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ================================================================================
#                           BUTTON HANDLER (EXTENDED)
# ================================================================================

@force_sub_required
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle all callback queries.
    This is the central router for all inline button interactions.
    Menus are arranged in a 2×2 grid with a single back button at the bottom.
    """
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    chat_id = query.message.chat_id

    # Verify subscription callback
    if data == "verify_subscription":
        return await start(update, context)

    # Admin navigation resets – clear states when going back to main menus
    nav_resets = {
        "admin_back", "manage_force_sub", "user_management",
        "manage_clones", "admin_settings", "admin_autoforward",
        "admin_autoupdate", "admin_feature_flags", "upload_menu"
    }
    if user_id == ADMIN_ID and user_id in user_states and data in nav_resets:
        await delete_bot_prompt(context, chat_id)
        user_states.pop(user_id, None)

    # Close message
    if data == "close_message":
        try:
            await query.delete_message()
        except Exception:
            pass
        return

    # About bot
    if data == "about_bot":
        about_text = small_caps("About Us\n\nDeveloped by @Beat_Anime_Ocean")
        try:
            await query.delete_message()
        except Exception:
            pass
        await context.bot.send_message(
            chat_id,
            about_text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                bold_button("🔙 BACK", callback_data="user_back")
            ]])
        )
        return

    if data == "user_back":
        await start(update, context)
        return

    # ──────────────────────────── ADMIN PANEL BUTTONS ────────────────────────────
    if data == "admin_stats":
        await send_admin_stats(query, context)
        return

    if data == "admin_sysstats":
        await sysstats_command(update, context)
        return

    if data == "admin_back":
        await send_admin_menu(chat_id, context, query)
        return

    if data == "admin_broadcast_start":
        user_states[user_id] = PENDING_BROADCAST
        prompt = await query.edit_message_text(
            small_caps("Send the message you want to broadcast (text, photo, video, etc.)"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 CANCEL", callback_data="admin_back")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = prompt.message_id
        return

    # ──────────────────────────── BROADCAST MODE SELECTION ───────────────────────
    if data.startswith("broadcast_mode_"):
        if user_id != ADMIN_ID:
            return
        mode = data.replace("broadcast_mode_", "")
        context.user_data['broadcast_mode'] = mode
        msg_data = context.user_data.get('broadcast_message')
        if not msg_data:
            await query.edit_message_text("❌ Broadcast message lost. Start over.")
            user_states.pop(user_id, None)
            return
        user_states[user_id] = PENDING_BROADCAST_CONFIRM
        await query.edit_message_text(
            small_caps(f"Mode: {mode}\n\nSend /confirm to start broadcast or /cancel to abort."),
            parse_mode='HTML'
        )
        return

    # ──────────────────────────── CATEGORY SETTINGS ─────────────────────────────
    if data.startswith("settings_category_"):
        if user_id != ADMIN_ID:
            return
        category = data.replace("settings_category_", "")
        await show_category_settings(update, context, category)
        return

    # ── Category Settings: Enter edit mode (2×2 grid) ────────────────────────────
    if data.startswith("set_template_"):
        category = data.replace("set_template_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_TEMPLATE
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(f"Send the new template name for {category}.\n\nCurrent: {settings['template_name']}"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_branding_"):
        category = data.replace("set_branding_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_BRANDING
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(f"Send the new branding text for {category}.\n\nCurrent: {settings['branding'] or 'Not set'}"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_buttons_"):
        category = data.replace("set_buttons_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_BUTTONS
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(
                f"Send the new button configuration for {category}.\n\n"
                f"Format: Button Text - {{link}}\n"
                f"For multiple buttons: Button1 - {{link}} & Button2 - https://t.me/...\n"
                f"Colour prefixes: #g for green, #r for red, #p for primary (blue).\n\n"
                f"Current: {json.dumps(settings['buttons'], indent=2)}"
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_caption_"):
        category = data.replace("set_caption_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_CAPTION
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(
                f"Send the new caption template for {category}.\n\n"
                f"Placeholders: {{title}}, {{type}}, {{rating}}, {{status}}, {{genres}}, {{synopsis}}, etc.\n\n"
                f"Current: {settings['caption_template'][:200]}"
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_thumbnail_"):
        category = data.replace("set_thumbnail_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_THUMBNAIL
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(
                f"Send a URL or just 'default' to reset for {category}.\n\n"
                f"Current: {settings['thumbnail_url'] or 'Default'}"
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_font_"):
        category = data.replace("set_font_", "")
        settings = get_category_settings(category)
        # 2×2 grid: two font options and back
        keyboard = [
            [bold_button("Normal", callback_data=f"font_normal_{category}"),
             bold_button("Small Caps", callback_data=f"font_smallcaps_{category}")],
            [bold_button("🔙 Back", callback_data=f"settings_category_{category}")]
        ]
        await query.edit_message_text(
            small_caps(f"Choose font style for {category} (current: {settings['font_style']})"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("font_normal_") or data.startswith("font_smallcaps_"):
        style = 'normal' if data.startswith('font_normal_') else 'smallcaps'
        category = data.replace("font_normal_", "").replace("font_smallcaps_", "")
        update_category_font(category, style)
        await query.answer(f"Font style set to {style}.")
        await show_category_settings(update, context, category)
        return

    if data.startswith("set_logo_"):
        category = data.replace("set_logo_", "")
        settings = get_category_settings(category)
        user_states[user_id] = SET_CATEGORY_LOGO
        context.user_data['editing_category'] = category
        await query.edit_message_text(
            small_caps(f"Send an image (photo or document) to set as logo for {category}.\n\nCurrent logo: {'Yes' if settings['logo_file_id'] else 'No'}"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"settings_category_{category}")
            ]])
        )
        return

    if data.startswith("set_logo_pos_"):
        category = data.replace("set_logo_pos_", "")
        settings = get_category_settings(category)
        # 2×2 grid of positions + back
        keyboard = [
            [bold_button("Top", callback_data=f"logopos_top_{category}"),
             bold_button("Bottom", callback_data=f"logopos_bottom_{category}")],
            [bold_button("Left", callback_data=f"logopos_left_{category}"),
             bold_button("Right", callback_data=f"logopos_right_{category}")],
            [bold_button("Center", callback_data=f"logopos_center_{category}")],
            [bold_button("🔙 Back", callback_data=f"settings_category_{category}")]
        ]
        await query.edit_message_text(
            small_caps(f"Choose logo position for {category} (current: {settings['logo_position']})"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("logopos_"):
        parts = data.split('_')
        pos = parts[1]
        category = '_'.join(parts[2:])
        update_category_logo_position(category, pos)
        await query.answer(f"Logo position set to {pos}.")
        await show_category_settings(update, context, category)
        return

    # ──────────────────────────── AUTO‑FORWARD ─────────────────────────────────
    if data == "admin_autoforward":
        await autoforward_command(update, context)
        return

    if data == "af_add_connection":
        user_states[user_id] = ADD_AUTO_FORWARD_SOURCE
        await query.edit_message_text(
            small_caps("Send the source channel (forward a message from it, or send its @username / ID)."),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_autoforward")
            ]])
        )
        return

    if data == "af_manage_connections":
        conns = get_auto_forward_connections()
        if not conns:
            await query.edit_message_text(
                small_caps("No connections yet."),
                reply_markup=InlineKeyboardMarkup([[  # single back button
                    bold_button("🔙 Back", callback_data="admin_autoforward")
                ]])
            )
            return
        text = small_caps("Active connections:\n\n")
        keyboard = []
        # Build 2×2 grid of connection buttons
        row = []
        for i, c in enumerate(conns):
            btn_text = f"{c[2] or c[1]} → {c[3]}"
            row.append(bold_button(btn_text, callback_data=f"af_edit_{c[0]}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)  # leftover single button
        keyboard.append([bold_button("🔙 Back", callback_data="admin_autoforward")])
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("af_edit_"):
        conn_id = int(data.replace("af_edit_", ""))
        context.user_data['af_edit_id'] = conn_id
        # 2×2 grid for connection edit options
        keyboard = [
            [bold_button("♻️ Toggle Active", callback_data=f"af_toggle_{conn_id}"),
             bold_button("🗑 Delete", callback_data=f"af_delete_{conn_id}")],
            [bold_button("🔍 Filters", callback_data=f"af_filters_edit_{conn_id}"),
             bold_button("♻️ Replacements", callback_data=f"af_replacements_edit_{conn_id}")],
            [bold_button("⏱ Delay/Caption", callback_data=f"af_delay_edit_{conn_id}")],
            [bold_button("🔙 Back", callback_data="af_manage_connections")]
        ]
        await query.edit_message_text(
            small_caps(f"Connection {conn_id}"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("af_toggle_"):
        conn_id = int(data.replace("af_toggle_", ""))
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT active FROM auto_forward_connections WHERE id = %s", (conn_id,))
            row = cur.fetchone()
            if row:
                new_active = not row[0]
                cur.execute("UPDATE auto_forward_connections SET active = %s WHERE id = %s", (new_active, conn_id))
        await query.answer("Toggled.")
        await button_handler(update, context)
        return

    if data.startswith("af_delete_"):
        conn_id = int(data.replace("af_delete_", ""))
        delete_auto_forward_connection(conn_id)
        await query.answer("Deleted.")
        await button_handler(update, context)
        return

    if data.startswith("af_filters_edit_"):
        conn_id = int(data.replace("af_filters_edit_", ""))
        context.user_data['af_edit_id'] = conn_id
        # Fetch current filters
        with db_manager.get_cursor() as cur:
            cur.execute("SELECT allowed_media, blacklist, whitelist FROM auto_forward_filters WHERE connection_id = %s", (conn_id,))
            f_row = cur.fetchone()
        allowed = f_row[0] if f_row else []
        blacklist = f_row[1] if f_row else []
        whitelist = f_row[2] if f_row else []
        text = small_caps(
            f"Filters for connection {conn_id}\n\n"
            f"Allowed Media: {', '.join(allowed) if allowed else 'All'}\n"
            f"Blacklist: {', '.join(blacklist) if blacklist else 'None'}\n"
            f"Whitelist: {', '.join(whitelist) if whitelist else 'None'}"
        )
        # 2×2 grid for filter actions
        keyboard = [
            [bold_button(" Set Media Types", callback_data=f"af_set_media_{conn_id}"),
             bold_button("➕ Add Blacklist", callback_data=f"af_add_bl_{conn_id}")],
            [bold_button("➕ Add Whitelist", callback_data=f"af_add_wl_{conn_id}"),
             bold_button("🗑 Clear Blacklist", callback_data=f"af_clear_bl_{conn_id}")],
            [bold_button("🗑 Clear Whitelist", callback_data=f"af_clear_wl_{conn_id}")],
            [bold_button("🔙 Back", callback_data=f"af_edit_{conn_id}")]
        ]
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("af_set_media_"):
        conn_id = int(data.replace("af_set_media_", ""))
        context.user_data['af_edit_id'] = conn_id
        user_states[user_id] = AF_ADD_ALLOWED_MEDIA
        await query.edit_message_text(
            small_caps("Send media types separated by commas (e.g., photo, video, document).\nLeave empty to allow all."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"af_filters_edit_{conn_id}")
            ]])
        )
        return

    if data.startswith("af_add_bl_"):
        conn_id = int(data.replace("af_add_bl_", ""))
        context.user_data['af_edit_id'] = conn_id
        user_states[user_id] = AF_ADD_BLACKLIST
        await query.edit_message_text(
            small_caps("Send words to add to blacklist, one per line."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"af_filters_edit_{conn_id}")
            ]])
        )
        return

    if data.startswith("af_add_wl_"):
        conn_id = int(data.replace("af_add_wl_", ""))
        context.user_data['af_edit_id'] = conn_id
        user_states[user_id] = AF_ADD_WHITELIST
        await query.edit_message_text(
            small_caps("Send words to add to whitelist, one per line."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"af_filters_edit_{conn_id}")
            ]])
        )
        return

    if data.startswith("af_clear_bl_"):
        conn_id = int(data.replace("af_clear_bl_", ""))
        with db_manager.get_cursor() as cur:
            cur.execute("UPDATE auto_forward_filters SET blacklist = '{}' WHERE connection_id = %s", (conn_id,))
        await query.answer("Blacklist cleared.")
        await button_handler(update, context)
        return

    if data.startswith("af_clear_wl_"):
        conn_id = int(data.replace("af_clear_wl_", ""))
        with db_manager.get_cursor() as cur:
            cur.execute("UPDATE auto_forward_filters SET whitelist = '{}' WHERE connection_id = %s", (conn_id,))
        await query.answer("Whitelist cleared.")
        await button_handler(update, context)
        return

    if data.startswith("af_replacements_edit_"):
        conn_id = int(data.replace("af_replacements_edit_", ""))
        context.user_data['af_edit_id'] = conn_id
        reps = get_auto_forward_replacements(conn_id)
        text = small_caps("Replacements:\n\n")
        if reps:
            for old, new in reps:
                text += f"{old} → {new}\n"
        else:
            text += "None.\n"
        # 2×2 grid for replacements actions
        keyboard = [
            [bold_button("➕ Add Replacement", callback_data=f"af_add_rep_{conn_id}"),
             bold_button("🗑 Clear All", callback_data=f"af_clear_reps_{conn_id}")],
            [bold_button("🔙 Back", callback_data=f"af_edit_{conn_id}")]
        ]
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("af_add_rep_"):
        conn_id = int(data.replace("af_add_rep_", ""))
        context.user_data['af_edit_id'] = conn_id
        user_states[user_id] = AF_ADD_REPLACEMENT_PATTERN
        await query.edit_message_text(
            small_caps("Send replacement in format: OLD_WORD :: NEW_WORD"),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"af_replacements_edit_{conn_id}")
            ]])
        )
        return

    if data.startswith("af_clear_reps_"):
        conn_id = int(data.replace("af_clear_reps_", ""))
        with db_manager.get_cursor() as cur:
            cur.execute("DELETE FROM auto_forward_replacements WHERE connection_id = %s", (conn_id,))
        await query.answer("All replacements cleared.")
        await button_handler(update, context)
        return

    if data.startswith("af_delay_edit_"):
        conn_id = int(data.replace("af_delay_edit_", ""))
        user_states[user_id] = SET_AUTO_FORWARD_DELAY
        context.user_data['af_edit_id'] = conn_id
        await query.edit_message_text(
            small_caps("Send new delay in seconds (0 for no delay)."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data=f"af_edit_{conn_id}")
            ]])
        )
        return

    if data == "af_settings":
        await query.edit_message_text(
            small_caps("Global auto‑forward settings can be set per connection via edit menu."),
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 Back", callback_data="admin_autoforward")
            ]])
        )
        return

    if data == "af_filters":
        await query.edit_message_text(
            small_caps("Filters can be set per connection via edit menu."),
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 Back", callback_data="admin_autoforward")
            ]])
        )
        return

    if data == "af_replacements":
        await query.edit_message_text(
            small_caps("Replacements can be set per connection via edit menu."),
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 Back", callback_data="admin_autoforward")
            ]])
        )
        return

    if data == "af_delay_caption":
        await query.edit_message_text(
            small_caps("Delay and caption settings can be set per connection via edit menu."),
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 Back", callback_data="admin_autoforward")
            ]])
        )
        return

    if data == "af_bulk":
        user_states[user_id] = AF_BULK_FORWARD_COUNT
        await query.edit_message_text(
            small_caps("Send the number of most recent messages to forward (e.g., 50)."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_autoforward")
            ]])
        )
        return

    # ──────────────────────────── AUTO MANGA UPDATE ────────────────────────────
    if data == "admin_autoupdate":
        await autoupdate_command(update, context)
        return

    if data == "manga_add":
        user_states[user_id] = ADD_MANGA_AUTO
        await query.edit_message_text(
            small_caps("Send the manga title to track."),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_autoupdate")
            ]])
        )
        return

    if data == "manga_list":
        manga_list = get_manga_auto_list()
        if not manga_list:
            await query.edit_message_text(
                small_caps("No manga tracked."),
                reply_markup=InlineKeyboardMarkup([[  # single back button
                    bold_button("🔙 Back", callback_data="admin_autoupdate")
                ]])
            )
            return
        text = small_caps("Tracked Manga:\n\n")
        keyboard = []
        row = []
        for mid, title, last_chap, target, active in manga_list:
            status = "✅" if active else "❌"
            display = f"{status} {title[:15]}"
            row.append(bold_button(display, callback_data=f"manga_edit_{mid}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([bold_button("🔙 Back", callback_data="admin_autoupdate")])
        await query.edit_message_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("manga_edit_"):
        manga_id = int(data.replace("manga_edit_", ""))
        context.user_data['edit_manga_id'] = manga_id
        # 2×2 grid for edit actions
        keyboard = [
            [bold_button("♻️ Toggle Active", callback_data=f"manga_toggle_{manga_id}"),
             bold_button("🗑 Delete", callback_data=f"manga_delete_{manga_id}")],
            [bold_button("🔙 Back", callback_data="manga_list")]
        ]
        await query.edit_message_text(
            small_caps("Edit manga settings."),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("manga_toggle_"):
        manga_id = int(data.replace("manga_toggle_", ""))
        toggle_manga_auto(manga_id)
        await query.answer("Toggled.")
        await button_handler(update, context)
        return

    if data.startswith("manga_delete_"):
        manga_id = int(data.replace("manga_delete_", ""))
        delete_manga_auto(manga_id)
        await query.answer("Deleted.")
        await button_handler(update, context)
        return

    # ──────────────────────────── FEATURE FLAGS ────────────────────────────────
    if data == "admin_feature_flags":
        await feature_flags_menu(update, context)
        return

    if data.startswith("toggle_feature_"):
        if user_id != ADMIN_ID:
            return
        feature = data.replace("toggle_feature_", "")
        current = feature_enabled(feature, 0, 'global')
        new_state = not current
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO feature_flags (feature_name, entity_id, entity_type, enabled)
                VALUES (%s, 0, 'global', %s)
                ON CONFLICT (feature_name, entity_id, entity_type) DO UPDATE SET enabled = EXCLUDED.enabled
            """, (feature, new_state))
        await query.answer(f"{feature} set to {'ON' if new_state else 'OFF'}")
        await feature_flags_menu(update, context)
        return

    # ──────────────────────────── UPLOAD MANAGER ───────────────────────────────
    if data == "upload_menu":
        await upload_command(update, context)
        return

    if data == "upload_preview":
        await upload_preview(update, context)
        return

    if data == "upload_set_caption":
        user_states[user_id] = UPLOAD_SET_CAPTION
        await query.edit_message_text(
            small_caps("Send the new base caption (HTML supported). Use {season}, {episode}, {total_episode}, {quality}."),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="upload_back")
            ]])
        )
        return

    if data == "upload_set_season":
        user_states[user_id] = UPLOAD_SET_SEASON
        await query.edit_message_text(
            small_caps(f"Current season: {progress['season']}\nSend new season number:"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="upload_back")
            ]])
        )
        return

    if data == "upload_set_episode":
        user_states[user_id] = UPLOAD_SET_EPISODE
        await query.edit_message_text(
            small_caps(f"Current episode: {progress['episode']}\nSend new episode number:"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="upload_back")
            ]])
        )
        return

    if data == "upload_set_total":
        user_states[user_id] = UPLOAD_SET_TOTAL
        await query.edit_message_text(
            small_caps(f"Current total episodes: {progress['total_episode']}\nSend new total:"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="upload_back")
            ]])
        )
        return

    if data == "upload_quality_menu":
        # Qualities displayed in 2×2 grid
        keyboard = []
        row = []
        for quality in ALL_QUALITIES:
            is_selected = quality in progress["selected_qualities"]
            checkmark = "✅ " if is_selected else ""
            row.append(bold_button(f"{checkmark}{quality}", callback_data=f"upload_toggle_quality_{quality}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([bold_button("🔙 Back", callback_data="upload_back")])
        await query.edit_message_text(
            small_caps("Select qualities to cycle through:"),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("upload_toggle_quality_"):
        quality = data.replace("upload_toggle_quality_", "")
        if quality in progress["selected_qualities"]:
            progress["selected_qualities"].remove(quality)
        else:
            progress["selected_qualities"].append(quality)
        await save_upload_progress()
        await button_handler(update, context)  # refresh quality menu
        return

    if data == "upload_set_channel":
        user_states[user_id] = UPLOAD_SET_CHANNEL
        await query.edit_message_text(
            small_caps(
                "Send target channel ID/username, or forward a message from the channel.\n"
                "Make sure the bot is admin there."
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="upload_back")
            ]])
        )
        return

    if data == "upload_toggle_auto":
        progress["auto_caption_enabled"] = not progress["auto_caption_enabled"]
        await save_upload_progress()
        await query.answer(f"Auto-caption: {'ON' if progress['auto_caption_enabled'] else 'OFF'}")
        await show_upload_menu(chat_id, context, query.message.message_id)
        return

    if data == "upload_reset":
        progress["episode"] = 1
        progress["video_count"] = 0
        await save_upload_progress()
        await query.answer("Episode reset to 1.")
        await show_upload_menu(chat_id, context, query.message.message_id)
        return

    if data == "upload_clear_db":
        # Confirm with 2×2 grid
        keyboard = [
            [bold_button("✔️ Yes, clear", callback_data="upload_confirm_clear"),
             bold_button("❌ No", callback_data="upload_back")]
        ]
        await query.edit_message_text(
            small_caps("Are you sure? This will reset all counters but keep caption and quality settings."),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data == "upload_confirm_clear":
        with db_manager.get_cursor() as cur:
            cur.execute("DELETE FROM bot_progress")
            cur.execute("""
                INSERT INTO bot_progress (id, base_caption, selected_qualities, auto_caption_enabled)
                VALUES (1, %s, %s, %s)
            """, (DEFAULT_CAPTION, ','.join(progress['selected_qualities']), progress['auto_caption_enabled']))
        await load_upload_progress()
        await query.answer("Database cleared.")
        await show_upload_menu(chat_id, context)
        return

    if data == "upload_back":
        await show_upload_menu(chat_id, context, query.message.message_id)
        return

    # ──────────────────────────── SEARCH SELECTION ─────────────────────────────
    if data.startswith("search_anime_"):
        media_id = int(data.replace("search_anime_", ""))
        await fetch_media_and_generate_post(update, context, 'anime', '', media_id)
        return

    if data.startswith("search_manga_"):
        media_id = int(data.replace("search_manga_", ""))
        await fetch_media_and_generate_post(update, context, 'manga', '', media_id)
        return

    # ──────────────────────────── SCHEDULED BROADCAST ──────────────────────────
    if data == "broadcast_schedule":
        user_states[user_id] = SCHEDULE_BROADCAST_DATETIME
        await query.edit_message_text(
            small_caps("Send the date and time for the broadcast (format: YYYY-MM-DD HH:MM in UTC)."),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_back")
            ]])
        )
        return

    # ──────────────────────────── USER MANAGEMENT ─────────────────────────────
    if data == "user_management":
        # 2×2 grid for user management options
        keyboard = [
            [bold_button("👥 List Users", callback_data="list_users"),
             bold_button("🔍 Search User", callback_data="search_user")],
            [bold_button(" Ban User", callback_data="ban_user"),
             bold_button(" Unban User", callback_data="unban_user")],
            [bold_button("🗑 Delete User", callback_data="delete_user"),
             bold_button("📤 Export CSV", callback_data="export_csv")],
            [bold_button("🔙 BACK", callback_data="admin_back")]
        ]
        await query.edit_message_text(
            small_caps("User Management"),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data == "list_users":
        await listusers_command(update, context)
        return

    if data == "export_csv":
        await exportusers_command(update, context)
        return

    if data.startswith("user_page_"):
        offset = int(data.replace("user_page_", ""))
        context.args = [str(offset)]
        await listusers_command(update, context)
        return

    if data.startswith("manage_user_"):
        uid = int(data.replace("manage_user_", ""))
        user_info = get_user_info_by_id(uid)
        if not user_info:
            await query.edit_message_text("User not found.")
            return
        uid, username, fname, lname, joined, banned = user_info
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        uname_d = f"@{username}" if username else "—"
        status = "Banned" if banned else "Active"
        text = small_caps(
            f"User Details\n\n"
            f"ID: {uid}\n"
            f"Name: {name}\n"
            f"Username: {uname_d}\n"
            f"Joined: {joined}\n"
            f"Status: {status}"
        )
        keyboard = []
        if not banned:
            keyboard.append([bold_button(" Ban", callback_data=f"ban_user_{uid}")])
        else:
            keyboard.append([bold_button(" Unban", callback_data=f"unban_user_{uid}")])
        keyboard.append([bold_button("🗑 Delete", callback_data=f"delete_user_{uid}")])
        keyboard.append([bold_button("🔙 Back", callback_data="user_management")])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("ban_user_"):
        uid = int(data.replace("ban_user_", ""))
        ban_user(uid)
        await query.answer("User banned.")
        await button_handler(update, context)
        return

    if data.startswith("unban_user_"):
        uid = int(data.replace("unban_user_", ""))
        unban_user(uid)
        await query.answer("User unbanned.")
        await button_handler(update, context)
        return

    if data.startswith("delete_user_"):
        uid = int(data.replace("delete_user_", ""))
        if uid == ADMIN_ID:
            await query.answer("Cannot delete admin.")
            return
        with db_manager.get_cursor() as cur:
            cur.execute("DELETE FROM users WHERE user_id = %s", (uid,))
        await query.answer("User deleted.")
        await button_handler(update, context)
        return

    # ──────────────────────────── CLONE BOTS MANAGEMENT ────────────────────────
    if data == "manage_clones":
        clones = get_all_clone_bots(active_only=True)
        text = small_caps("Active Clone Bots:\n\n")
        if clones:
            for cid, token, uname, active, added in clones:
                text += f"• @{uname} (added {added})\n"
        else:
            text += "None\n"
        # 2×2 grid for clone actions
        keyboard = [
            [bold_button("➕ Add Clone", callback_data="add_clone"),
             bold_button("🗑 Remove Clone", callback_data="remove_clone")],
            [bold_button("🔙 BACK", callback_data="admin_back")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "add_clone":
        user_states[user_id] = ADD_CLONE_TOKEN
        await query.edit_message_text(
            small_caps("Send the bot token of the clone bot."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="manage_clones")
            ]])
        )
        return

    if data == "remove_clone":
        clones = get_all_clone_bots(active_only=True)
        if not clones:
            await query.answer("No clones to remove.")
            return
        keyboard = []
        row = []
        for cid, token, uname, active, added in clones:
            row.append(bold_button(f"@{uname}", callback_data=f"remove_clone_{uname}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([bold_button("🔙 Back", callback_data="manage_clones")])
        await query.edit_message_text(
            small_caps("Select clone to remove:"),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("remove_clone_"):
        uname = data.replace("remove_clone_", "")
        remove_clone_bot(uname)
        await query.answer(f"Removed @{uname}")
        await button_handler(update, context)
        return

    # ──────────────────────────── FORCE‑SUB CHANNELS MANAGEMENT ────────────────
    if data == "manage_force_sub":
        channels = get_all_force_sub_channels(return_usernames_only=False)
        text = small_caps("Force‑Subscription Channels:\n\n")
        if channels:
            for uname, title, jbr in channels:
                jbr_text = " (Join‑by‑Request)" if jbr else ""
                text += f"• {title} ({uname}){jbr_text}\n"
        else:
            text += "None\n"
        # 2×2 grid for channel actions
        keyboard = [
            [bold_button("➕ Add Channel", callback_data="add_channel"),
             bold_button("🗑 Remove Channel", callback_data="remove_channel")],
            [bold_button("🔙 BACK", callback_data="admin_back")]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "add_channel":
        user_states[user_id] = ADD_CHANNEL_USERNAME
        await query.edit_message_text(
            small_caps("Send the channel @username (e.g., @mychannel)."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="manage_force_sub")
            ]])
        )
        return

    if data == "remove_channel":
        channels = get_all_force_sub_channels(return_usernames_only=False)
        if not channels:
            await query.answer("No channels to remove.")
            return
        keyboard = []
        row = []
        for uname, title, jbr in channels:
            row.append(bold_button(title, callback_data=f"remove_channel_{uname}"))
            if len(row) == 2:
                keyboard.append(row)
                row = []
        if row:
            keyboard.append(row)
        keyboard.append([bold_button("🔙 Back", callback_data="manage_force_sub")])
        await query.edit_message_text(
            small_caps("Select channel to remove:"),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    if data.startswith("remove_channel_"):
        uname = data.replace("remove_channel_", "")
        delete_force_sub_channel(uname)
        await query.answer(f"Removed {uname}")
        await button_handler(update, context)
        return

    # ──────────────────────────── LINK GENERATION ─────────────────────────────
    if data == "generate_links":
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        await query.edit_message_text(
            small_caps("Send the channel @username or ID to generate a link for."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_back")
            ]])
        )
        return

    # ──────────────────────────── FILL MISSING TITLES ─────────────────────────
    if data == "fill_missing_titles":
        missing = get_links_without_title(bot_username=BOT_USERNAME)
        if not missing:
            await query.answer("No missing titles.")
            return
        user_states[user_id] = PENDING_FILL_TITLE
        context.user_data['missing_links'] = missing
        context.user_data['missing_index'] = 0
        first = missing[0]
        await query.edit_message_text(
            small_caps(f"Link ID: {first[0]}\nChannel: {first[1]}\n\nSend the title for this link."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_back")
            ]])
        )
        return

    # ──────────────────────────── SET BACKUP CHANNEL ──────────────────────────
    if data == "set_backup_channel":
        user_states[user_id] = SET_BACKUP_CHANNEL
        await query.edit_message_text(
            small_caps("Send the backup channel URL (e.g., https://t.me/backup)."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_back")
            ]])
        )
        return

    # ──────────────────────────── MOVE LINKS ──────────────────────────────────
    if data == "move_links":
        user_states[user_id] = PENDING_MOVE_TARGET
        await query.edit_message_text(
            small_caps("Send the target bot @username to move all links to."),
            reply_markup=InlineKeyboardMarkup([[  # single cancel button
                bold_button("🔙 Cancel", callback_data="admin_back")
            ]])
        )
        return

    # ──────────────────────────── OTHER ADMIN SETTINGS ────────────────────────
    if data == "admin_settings":
        # 2×2 grid for settings
        keyboard = [
            [bold_button(" Maintenance Mode", callback_data="toggle_maintenance"),
             bold_button("♻️ Clone Redirect", callback_data="toggle_clone_redirect")],
            [bold_button(" Set Backup Channel", callback_data="set_backup_channel")],
            [bold_button("🔙 BACK", callback_data="admin_back")]
        ]
        maint_status = "ON" if is_maintenance_mode() else "OFF"
        clone_status = "ON" if get_setting("clone_redirect_enabled", "false") == "true" else "OFF"
        text = small_caps(
            f"Settings\n\n"
            f"Maintenance Mode: {maint_status}\n"
            f"Clone Redirect: {clone_status}\n"
            f"Link Expiry: {LINK_EXPIRY_MINUTES} min"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "toggle_maintenance":
        new_state = toggle_maintenance_mode()
        status = "ON" if new_state else "OFF"
        await query.answer(f"Maintenance mode set to {status}")
        await button_handler(update, context)
        return

    if data == "toggle_clone_redirect":
        current = get_setting("clone_redirect_enabled", "false")
        new_state = "false" if current == "true" else "true"
        set_setting("clone_redirect_enabled", new_state)
        await query.answer(f"Clone redirect set to {'ON' if new_state=='true' else 'OFF'}")
        await button_handler(update, context)
        return

    # If we reach here, the callback data is unhandled
    logger.warning(f"Unhandled callback data: {data}")
    await query.edit_message_text("This feature is not yet implemented.")

# ================================================================================
#                           ADMIN MESSAGE HANDLER
# ================================================================================

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle text messages from admin when in a specific state (conversation).
    """
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_states:
        return

    state = user_states[user_id]
    text = update.message.text
    await delete_bot_prompt(context, update.effective_chat.id)

    # ──────────────────────────── ADD CHANNEL (force‑sub) ─────────────────────
    if state == ADD_CHANNEL_USERNAME:
        username = text.strip()
        if not username.startswith('@'):
            await update.message.reply_text("❌ Username must start with @. Try again.")
            return
        try:
            chat = await context.bot.get_chat(username)
            context.user_data['new_channel_username'] = username
            context.user_data['new_channel_title'] = chat.title
            user_states[user_id] = ADD_CHANNEL_TITLE
            await update.message.reply_text(
                small_caps(f"Channel found: {chat.title}\n\nSend the display title (or /skip to use '{chat.title}')."),
                reply_markup=InlineKeyboardMarkup([[  # single cancel button
                    bold_button("🔙 Cancel", callback_data="manage_force_sub")
                ]])
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error accessing channel: {e}")
        return

    if state == ADD_CHANNEL_TITLE:
        username = context.user_data.get('new_channel_username')
        if not username:
            await update.message.reply_text("Session expired.")
            user_states.pop(user_id, None)
            return
        title = text.strip()
        if title.lower() == '/skip':
            title = context.user_data.get('new_channel_title', username)
        add_force_sub_channel(username, title, join_by_request=False)
        await update.message.reply_text(small_caps(f"✅ Added {title} ({username}) to force‑sub."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # ──────────────────────────── GENERATE LINK ───────────────────────────────
    if state == GENERATE_LINK_CHANNEL_USERNAME:
        identifier = text.strip()
        try:
            chat = await context.bot.get_chat(identifier)
            context.user_data['gen_channel_id'] = chat.id
            context.user_data['gen_channel_username'] = chat.username or str(chat.id)
            context.user_data['gen_channel_title'] = chat.title
            user_states[user_id] = GENERATE_LINK_CHANNEL_TITLE
            await update.message.reply_text(
                small_caps(f"Channel: {chat.title}\n\nSend a title for this link (or /skip to use channel title)."),
                reply_markup=InlineKeyboardMarkup([[  # single cancel button
                    bold_button("🔙 Cancel", callback_data="admin_back")
                ]])
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return

    if state == GENERATE_LINK_CHANNEL_TITLE:
        title = text.strip()
        if title.lower() == '/skip':
            title = context.user_data.get('gen_channel_title', '')
        link_id = generate_link_id(
            channel_username=context.user_data['gen_channel_id'],
            user_id=user_id,
            never_expires=False,
            channel_title=title,
            source_bot_username=BOT_USERNAME
        )
        deep_link = f"https://t.me/{BOT_USERNAME}?start={link_id}"
        await update.message.reply_text(
            small_caps(f"✅ Link generated for {title}:\n\n{deep_link}"),
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 BACK", callback_data="admin_back")
            ]])
        )
        user_states.pop(user_id, None)
        return

    # ──────────────────────────── BROADCAST ───────────────────────────────────
    if state == PENDING_BROADCAST:
        context.user_data['broadcast_message'] = (update.message.chat_id, update.message.message_id)
        # 2×2 grid for broadcast modes
        keyboard = [
            [bold_button("Normal", callback_data="broadcast_mode_normal"),
             bold_button("Auto‑delete", callback_data="broadcast_mode_auto_delete")],
            [bold_button("Pin", callback_data="broadcast_mode_pin"),
             bold_button("Delete + Pin", callback_data="broadcast_mode_delete_pin")],
            [bold_button("🔙 Cancel", callback_data="admin_back")]
        ]
        user_states[user_id] = PENDING_BROADCAST_OPTIONS
        await update.message.reply_text(
            small_caps("Choose broadcast mode:"),
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    # ──────────────────────────── ADD CLONE TOKEN ─────────────────────────────
    if state == ADD_CLONE_TOKEN:
        token = text.strip()
        await _register_clone(update, context, token)
        user_states.pop(user_id, None)
        return

    # ──────────────────────────── FILL MISSING TITLES ─────────────────────────
    if state == PENDING_FILL_TITLE:
        missing = context.user_data.get('missing_links', [])
        index = context.user_data.get('missing_index', 0)
        if index >= len(missing):
            await update.message.reply_text("All titles filled.")
            user_states.pop(user_id, None)
            return
        link_id, ch_uname, src_bot = missing[index]
        title = text.strip()
        update_link_title(link_id, title)
        index += 1
        context.user_data['missing_index'] = index
        if index < len(missing):
            next_link = missing[index]
            await update.message.reply_text(
                small_caps(f"Next: Link ID {next_link[0]}, channel {next_link[1]}\nSend title (or /skip to leave blank):"),
                reply_markup=InlineKeyboardMarkup([[  # single cancel button
                    bold_button("🔙 Cancel", callback_data="admin_back")
                ]])
            )
        else:
            await update.message.reply_text("All titles filled.")
            user_states.pop(user_id, None)
        return

    # ──────────────────────────── SET BACKUP CHANNEL ──────────────────────────
    if state == SET_BACKUP_CHANNEL:
        url = text.strip()
        set_setting("backup_channel_url", url)
        await update.message.reply_text(small_caps("Backup channel URL saved."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # ──────────────────────────── MOVE LINKS TARGET ───────────────────────────
    if state == PENDING_MOVE_TARGET:
        target = text.strip().lstrip('@')
        await _do_move(update, context, target)
        user_states.pop(user_id, None)
        return

    # ──────────────────────────── CATEGORY SETTINGS TEXT INPUTS ───────────────
    if state == SET_CATEGORY_TEMPLATE:
        category = context.user_data.get('editing_category')
        if category:
            update_category_template(category, text)
            await update.message.reply_text(small_caps(f"Template for {category} updated."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == SET_CATEGORY_BRANDING:
        category = context.user_data.get('editing_category')
        if category:
            update_category_branding(category, text)
            await update.message.reply_text(small_caps(f"Branding for {category} updated."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == SET_CATEGORY_BUTTONS:
        category = context.user_data.get('editing_category')
        if category:
            buttons = []
            parts = text.split('&')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    btn_text, url = part.split('-', 1)
                    btn_text = btn_text.strip()
                    url = url.strip()
                    if btn_text.startswith('#g '):
                        btn_text = '🟢 ' + btn_text[3:]
                    elif btn_text.startswith('#r '):
                        btn_text = '🔴 ' + btn_text[3:]
                    elif btn_text.startswith('#p '):
                        btn_text = '🔵 ' + btn_text[3:]
                    buttons.append({'text': btn_text, 'url': url})
            update_category_buttons(category, json.dumps(buttons))
            await update.message.reply_text(small_caps(f"Buttons for {category} updated."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == SET_CATEGORY_CAPTION:
        category = context.user_data.get('editing_category')
        if category:
            update_category_caption(category, text)
            await update.message.reply_text(small_caps(f"Caption template for {category} updated."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == SET_CATEGORY_THUMBNAIL:
        category = context.user_data.get('editing_category')
        if category:
            if text.lower() == 'default':
                update_category_thumbnail(category, '')
            else:
                update_category_thumbnail(category, text)
            await update.message.reply_text(small_caps(f"Thumbnail for {category} updated."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == SET_CATEGORY_LOGO:
        await update.message.reply_text("❌ Please send an image.")
        return

    # ──────────────────────────── AUTO‑FORWARD TEXT INPUTS ────────────────────
    if state == ADD_AUTO_FORWARD_SOURCE:
        identifier = text.strip()
        try:
            chat = await context.bot.get_chat(identifier)
            context.user_data['af_source_id'] = chat.id
            context.user_data['af_source_username'] = chat.username
            user_states[user_id] = ADD_AUTO_FORWARD_TARGET
            await update.message.reply_text(
                small_caps(f"Source: {chat.title or chat.id}\n\nNow send target channel (ID, @username, or forward)."),
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[  # single cancel button
                    bold_button("🔙 Cancel", callback_data="admin_autoforward")
                ]])
            )
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}. Try again.")
        return

    if state == ADD_AUTO_FORWARD_TARGET:
        identifier = text.strip()
        try:
            chat = await context.bot.get_chat(identifier)
            target_id = chat.id
            source_id = context.user_data.get('af_source_id')
            if not source_id:
                await update.message.reply_text("Session expired.")
                user_states.pop(user_id, None)
                return
            conn_id = add_auto_forward_connection(source_id, target_id)
            await update.message.reply_text(
                small_caps("Connection added! You can now configure filters, replacements, etc. from the manage menu."),
                reply_markup=InlineKeyboardMarkup([[  # single back button
                    bold_button("🔙 Back", callback_data="admin_autoforward")
                ]])
            )
            user_states.pop(user_id, None)
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}. Try again.")
        return

    if state == SET_AUTO_FORWARD_DELAY:
        try:
            delay = int(text.strip())
            conn_id = context.user_data.get('af_edit_id')
            if conn_id:
                with db_manager.get_cursor() as cur:
                    cur.execute("UPDATE auto_forward_connections SET delay_seconds = %s WHERE id = %s", (delay, conn_id))
                await update.message.reply_text(small_caps("Delay updated."))
            else:
                await update.message.reply_text("Session expired.")
        except ValueError:
            await update.message.reply_text("❌ Invalid number.")
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    if state == AF_ADD_ALLOWED_MEDIA:
        conn_id = context.user_data.get('af_edit_id')
        if not conn_id:
            user_states.pop(user_id, None)
            return
        media_types = [m.strip().lower() for m in text.split(',') if m.strip()]
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO auto_forward_filters (connection_id, allowed_media)
                VALUES (%s, %s)
                ON CONFLICT (connection_id) DO UPDATE SET allowed_media = EXCLUDED.allowed_media
            """, (conn_id, media_types))
        await update.message.reply_text(small_caps("Allowed media types updated."))
        user_states.pop(user_id, None)
        await button_handler(update, context)  # return to filters menu
        return

    if state == AF_ADD_BLACKLIST:
        conn_id = context.user_data.get('af_edit_id')
        if not conn_id:
            user_states.pop(user_id, None)
            return
        words = [w.strip().lower() for w in text.split('\n') if w.strip()]
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO auto_forward_filters (connection_id, blacklist)
                VALUES (%s, %s)
                ON CONFLICT (connection_id) DO UPDATE SET blacklist = array_cat(blacklist, %s)
            """, (conn_id, words, words))
        await update.message.reply_text(small_caps("Blacklist updated."))
        user_states.pop(user_id, None)
        await button_handler(update, context)
        return

    if state == AF_ADD_WHITELIST:
        conn_id = context.user_data.get('af_edit_id')
        if not conn_id:
            user_states.pop(user_id, None)
            return
        words = [w.strip().lower() for w in text.split('\n') if w.strip()]
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO auto_forward_filters (connection_id, whitelist)
                VALUES (%s, %s)
                ON CONFLICT (connection_id) DO UPDATE SET whitelist = array_cat(whitelist, %s)
            """, (conn_id, words, words))
        await update.message.reply_text(small_caps("Whitelist updated."))
        user_states.pop(user_id, None)
        await button_handler(update, context)
        return

    if state == AF_ADD_REPLACEMENT_PATTERN:
        conn_id = context.user_data.get('af_edit_id')
        if not conn_id:
            user_states.pop(user_id, None)
            return
        parts = text.split('::')
        if len(parts) != 2:
            await update.message.reply_text("❌ Invalid format. Use OLD_WORD :: NEW_WORD")
            return
        old = parts[0].strip()
        new = parts[1].strip()
        add_auto_forward_replacement(conn_id, old, new)
        await update.message.reply_text(small_caps("Replacement added."))
        user_states.pop(user_id, None)
        await button_handler(update, context)
        return

    if state == AF_BULK_FORWARD_COUNT:
        try:
            count = int(text.strip())
            if count <= 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("❌ Please send a positive number.")
            return
        # For now, just a placeholder; you would implement actual bulk forwarding here.
        await update.message.reply_text(small_caps(f"Bulk forward of {count} messages not yet implemented."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # ──────────────────────────── AUTO MANGA UPDATE ───────────────────────────
    if state == ADD_MANGA_AUTO:
        title = text.strip()
        add_manga_auto(title, ADMIN_ID, watermark=False, combine_pdf=False)
        await update.message.reply_text(small_caps(f"Manga '{title}' added. You can set target channel later via edit menu."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # ──────────────────────────── SCHEDULED BROADCAST ─────────────────────────
    if state == SCHEDULE_BROADCAST_DATETIME:
        try:
            execute_at = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
            if execute_at < datetime.now():
                await update.message.reply_text("❌ Date must be in the future.")
                return
            context.user_data['scheduled_time'] = execute_at
            user_states[user_id] = SCHEDULE_BROADCAST_MSG
            await update.message.reply_text(
                small_caps("Now send the message to broadcast (text or media)."),
                reply_markup=InlineKeyboardMarkup([[  # single cancel button
                    bold_button("🔙 Cancel", callback_data="admin_back")
                ]])
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid format. Use YYYY-MM-DD HH:MM (e.g., 2025-12-31 23:59)")
        return

    if state == SCHEDULE_BROADCAST_MSG:
        execute_at = context.user_data.get('scheduled_time')
        if not execute_at:
            user_states.pop(user_id, None)
            return
        # Store the message for scheduled broadcast
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_broadcasts (admin_id, message_text, execute_at)
                VALUES (%s, %s, %s)
            """, (user_id, update.message.text, execute_at))
        await update.message.reply_text(small_caps("Broadcast scheduled."))
        user_states.pop(user_id, None)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # ──────────────────────────── UPLOAD MANAGER TEXT INPUTS ──────────────────
    if state == UPLOAD_SET_CAPTION:
        progress["base_caption"] = text
        await save_upload_progress()
        user_states.pop(user_id, None)
        await show_upload_menu(update.effective_chat.id, context)
        return

    if state == UPLOAD_SET_SEASON:
        if text.isdigit():
            progress["season"] = int(text)
            await save_upload_progress()
            user_states.pop(user_id, None)
        else:
            await update.message.reply_text("❌ Please send a number.")
            return
        await show_upload_menu(update.effective_chat.id, context)
        return

    if state == UPLOAD_SET_EPISODE:
        if text.isdigit():
            progress["episode"] = int(text)
            progress["video_count"] = 0
            await save_upload_progress()
            user_states.pop(user_id, None)
        else:
            await update.message.reply_text("❌ Please send a number.")
            return
        await show_upload_menu(update.effective_chat.id, context)
        return

    if state == UPLOAD_SET_TOTAL:
        if text.isdigit():
            progress["total_episode"] = int(text)
            await save_upload_progress()
            user_states.pop(user_id, None)
        else:
            await update.message.reply_text("❌ Please send a number.")
            return
        await show_upload_menu(update.effective_chat.id, context)
        return

    if state == UPLOAD_SET_CHANNEL:
        identifier = text.strip()
        try:
            if update.message.forward_origin and hasattr(update.message.forward_origin, 'chat'):
                channel = update.message.forward_origin.chat
                chat_id = channel.id
            else:
                chat = await context.bot.get_chat(identifier)
                chat_id = chat.id
            progress["target_chat_id"] = chat_id
            await save_upload_progress()
            user_states.pop(user_id, None)
            await update.message.reply_text(small_caps(f"Target channel set to {chat_id}."))
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
            return
        await show_upload_menu(update.effective_chat.id, context)
        return

    # If we get here, the state is unhandled – clear it
    logger.warning(f"Unhandled admin state: {state}")
    user_states.pop(user_id, None)
    await update.message.reply_text("Conversation ended.")

# ================================================================================
#                       ADDITIONAL ADMIN COMMANDS (continued)
# ================================================================================

@force_sub_required
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    bot_uname = BOT_USERNAME
    links = get_all_links(bot_username=bot_uname, limit=100)
    missing = get_links_without_title(bot_username=bot_uname)

    if not links:
        await update.message.reply_text(
            "📂 <b>No links found for this bot yet.</b>", parse_mode='HTML')
        return

    lines = [f"📋 <b>Link Backup</b> — <code>@{bot_uname}</code>\n"]
    for link_id, ch_uname, ch_title, src_bot, created, never_exp in links:
        deep = f"https://t.me/{bot_uname}?start={link_id}"
        title_str = ch_title if ch_title else "⚠️ <i>No title</i>"
        exp_str   = "♾ Never" if never_exp else "⏱ Expires"
        lines.append(
            f"• <b>{title_str}</b>\n"
            f"  Channel: <code>{ch_uname}</code>\n"
            f"  Link: <code>{deep}</code> [{exp_str}]"
        )

    chunk, chunks = "", []
    for line in lines:
        if len(chunk) + len(line) + 1 > 4000:
            chunks.append(chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk:
        chunks.append(chunk)

    for i, c in enumerate(chunks):
        kb = None
        if i == len(chunks) - 1 and missing:
            kb = InlineKeyboardMarkup([[  # single button
                bold_button(
                    f"📝 Fill {len(missing)} missing titles",
                    callback_data="fill_missing_titles")
            ]])
        await update.message.reply_text(c.strip(), parse_mode='HTML', reply_markup=kb)

@force_sub_required
async def move_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if context.args:
        target = context.args[0].lstrip('@')
        await _do_move(update, context, target)
        return

    user_states[update.effective_user.id] = PENDING_MOVE_TARGET
    msg = await update.message.reply_text(
        "♻️ <b>Move Links</b>\n\n"
        "Send the @username of the target bot to move all current links to it.\n\n"
        "<blockquote>All deep links will be updated to use the new bot's username.</blockquote>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[  # single cancel button
            bold_button("🔙 CANCEL", callback_data="admin_back")
        ]])
    )
    context.user_data['bot_prompt_message_id'] = msg.message_id

async def _do_move(update, context, target_username: str):
    chat_id = update.effective_chat.id
    target_username = target_username.lstrip('@')

    all_links = get_all_links(bot_username=BOT_USERNAME, limit=500)

    if not all_links:
        await context.bot.send_message(
            chat_id,
            f"⚠️ No links found under <code>@{BOT_USERNAME}</code>.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 BACK", callback_data="admin_back")
            ]])
        )
        return

    count = move_links_to_bot(BOT_USERNAME, target_username)

    header = (
        f"✅ <b>Moved {count} link(s)</b>\n"
        f"<code>@{BOT_USERNAME}</code> → <code>@{target_username}</code>\n\n"
        f" <b>Updated links for your channel index</b>\n\n"
    )

    lines = []
    for link_id, ch_uname, ch_title, _src, _created, never_exp in all_links:
        new_link  = f"https://t.me/{target_username}?start={link_id}"
        title_str = ch_title if ch_title else ch_uname
        exp_icon  = "♾" if never_exp else "⏱"
        lines.append(
            f"{exp_icon} <b>{title_str}</b>\n"
            f"   Channel: <code>{ch_uname}</code>\n"
            f"   🔗 <code>{new_link}</code>"
        )

    chunks, current = [], header
    for line in lines:
        entry = line + "\n\n"
        if len(current) + len(entry) > 4000:
            chunks.append(current)
            current = ""
        current += entry
    if current.strip():
        chunks.append(current)

    for i, chunk in enumerate(chunks):
        kb = None
        if i == len(chunks) - 1:
            kb = InlineKeyboardMarkup([[  # single back button
                bold_button("🔙 BACK TO MENU", callback_data="admin_back")
            ]])
        await context.bot.send_message(chat_id, chunk.strip(), parse_mode='HTML', reply_markup=kb)

@force_sub_required
async def addclone_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if context.args:
        token = context.args[0].strip()
        await _register_clone(update, context, token)
        return

    user_states[update.effective_user.id] = ADD_CLONE_TOKEN
    msg = await update.message.reply_text(
        "🤖 <b>Add Clone Bot</b>\n\nSend the <b>BOT TOKEN</b> of the clone bot.",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[  # single cancel button
            bold_button("🔙 CANCEL", callback_data="admin_back")
        ]])
    )
    context.user_data['bot_prompt_message_id'] = msg.message_id

async def _register_clone(update, context, token: str):
    chat_id = update.effective_chat.id
    try:
        clone_bot = Bot(token=token)
        me = await clone_bot.get_me()
        username = me.username
        if add_clone_bot(token, username):
            await context.bot.send_message(
                chat_id,
                f"✅ Clone bot <b>@{username}</b> registered!",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[  # single back button
                    bold_button(" Manage Clones", callback_data="manage_clones")
                ]])
            )
        else:
            await context.bot.send_message(chat_id, "❌ Failed to save clone bot.")
    except Exception as e:
        await context.bot.send_message(chat_id, f"❌ Invalid token or API error:\n<code>{e}</code>", parse_mode='HTML')

@force_sub_required
async def reload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    message_id_to_copy = None
    if context.args:
        try:
            message_id_to_copy = 'admin' if context.args[0].lower() == 'admin' else int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ Usage: `/reload [msg_id or 'admin']`", parse_mode='Markdown')
            return

    triggered_by = update.effective_user.username or str(update.effective_user.id)
    restart_info = {
        'chat_id': update.effective_chat.id,
        'admin_id': ADMIN_ID,
        'message_id_to_copy': message_id_to_copy,
        'triggered_by': triggered_by
    }
    try:
        with open('restart_message.json', 'w') as f:
            json.dump(restart_info, f)
    except Exception as e:
        logger.error(f"Failed to write restart file: {e}")

    await update.message.reply_text("♻️ **Bot is restarting...**", parse_mode='Markdown')
    sys.exit(0)

@force_sub_required
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    user_count    = get_user_count()
    channel_count = len(get_all_force_sub_channels())
    link_count    = get_links_count()
    maint         = "🔴 ON" if is_maintenance_mode() else "🟢 OFF"
    clones        = get_all_clone_bots(active_only=True)

    text = small_caps(
        f"BOT STATISTICS\n\n"
        f"Total Users: {user_count}\n"
        f"Force‑Sub Channels: {channel_count}\n"
        f"Total Links: {link_count}\n"
        f"Active Clones: {len(clones)}\n"
        f"Maintenance: {maint}\n"
        f"Link Expiry: {LINK_EXPIRY_MINUTES} min"
    )
    keyboard = [[bold_button("🔙 BACK TO MENU", callback_data="admin_back")]]
    await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))

@force_sub_required
async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) < 2:
        await update.message.reply_text("❌ Usage: `/addchannel @username Title`", parse_mode='Markdown')
        return
    uname = context.args[0]
    title = " ".join(context.args[1:])
    if not uname.startswith('@'):
        await update.message.reply_text("❌ Username must start with **@**.", parse_mode='Markdown')
        return
    try:
        await context.bot.get_chat(uname)
    except Exception:
        await update.message.reply_text(
            f"⚠️ Cannot access **{uname}**. Make the bot admin there first.", parse_mode='Markdown')
        return
    add_force_sub_channel(uname, title, join_by_request=False)
    await update.message.reply_text(f"✅ Added: **{title}** (`{uname}`)", parse_mode='Markdown')

@force_sub_required
async def remove_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) != 1:
        await update.message.reply_text("❌ Usage: `/removechannel @username`", parse_mode='Markdown')
        return
    uname = context.args[0]
    if not uname.startswith('@'):
        await update.message.reply_text("❌ Username must start with **@**.", parse_mode='Markdown')
        return
    delete_force_sub_channel(uname)
    await update.message.reply_text(f"🗑️ Removed `{uname}`.", parse_mode='Markdown')

@force_sub_required
async def ban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) != 1:
        await update.message.reply_text("❌ Usage: `/banuser @username_or_id`", parse_mode='Markdown')
        return
    uid = resolve_target_user_id(context.args[0])
    if uid is None:
        await update.message.reply_text(f"❌ User **{context.args[0]}** not found.", parse_mode='Markdown')
        return
    if uid == ADMIN_ID:
        await update.message.reply_text("⚠️ Cannot ban Admin.", parse_mode='Markdown')
        return
    ban_user(uid)
    await update.message.reply_text(f"🚫 User `{uid}` banned.", parse_mode='Markdown')

@force_sub_required
async def unban_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) != 1:
        await update.message.reply_text("❌ Usage: `/unbanuser @username_or_id`", parse_mode='Markdown')
        return
    uid = resolve_target_user_id(context.args[0])
    if uid is None:
        await update.message.reply_text(f"❌ User **{context.args[0]}** not found.", parse_mode='Markdown')
        return
    unban_user(uid)
    await update.message.reply_text(f"✅ User `{uid}` unbanned.", parse_mode='Markdown')

@force_sub_required
async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    try:
        offset = int(context.args[0]) if context.args else 0
    except ValueError:
        offset = 0

    total = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    text = f"👥 <b>Users {offset+1}–{min(offset+10,total)} of {total}</b>\n\n"
    kb = []
    row = []
    for uid, username, fname, lname, joined, banned in users:
        name = f"{fname or ''} {lname or ''}".strip() or "N/A"
        uname_d = f"@{username}" if username else "—"
        status = "🚫" if banned else "✅"
        text += f"{status} <b>{name}</b> (<code>{uname_d}</code>)\n"
        row.append(bold_button(f"👤 {name}", callback_data=f"manage_user_{uid}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    nav = []
    if offset > 0:
        nav.append(bold_button("🔙 PREV", callback_data=f"user_page_{offset-10}"))
    if total > offset + 10:
        nav.append(bold_button("NEXT 🔜", callback_data=f"user_page_{offset+10}"))
    if nav:
        kb.append(nav)
    kb.append([bold_button("🔙 BACK", callback_data="admin_back")])

    await update.message.reply_text(text, parse_mode='HTML', reply_markup=InlineKeyboardMarkup(kb))

@force_sub_required
async def deleteuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) != 1:
        await update.message.reply_text("❌ Usage: `/deleteuser user_id`", parse_mode='Markdown')
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ User ID must be integer.", parse_mode='Markdown')
        return
    if uid == ADMIN_ID:
        await update.message.reply_text("⚠️ Cannot delete admin.", parse_mode='Markdown')
        return
    with db_manager.get_cursor() as cur:
        cur.execute("DELETE FROM users WHERE user_id = %s", (uid,))
    await update.message.reply_text(f"✅ User {uid} deleted.")

@force_sub_required
async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(small_caps("Bot is alive and healthy!"))

@force_sub_required
async def channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    force_subs = get_all_force_sub_channels(return_usernames_only=False)
    auto_sources = []
    with db_manager.get_cursor() as cur:
        cur.execute("SELECT source_chat_id, source_chat_username, target_chat_id FROM auto_forward_connections WHERE active = TRUE")
        auto_sources = cur.fetchall()

    text = small_caps("Indexed Channels/Groups:\n\n")
    text += "📢 Force‑Sub Channels:\n"
    if force_subs:
        for uname, title, jbr in force_subs:
            text += f"  • {title} ({uname})\n"
    else:
        text += "  None\n"

    text += "\n♻️ Auto‑Forward Sources:\n"
    if auto_sources:
        for src_id, src_uname, tgt_id in auto_sources:
            src = f"@{src_uname}" if src_uname else f"`{src_id}`"
            text += f"  • {src} → `{tgt_id}`\n"
    else:
        text += "  None\n"

    await update.message.reply_text(text, parse_mode='HTML')

@force_sub_required
async def logs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)

    try:
        with open("logs/bot.log", "r", encoding='utf-8') as f:
            lines = f.readlines()[-50:]
            log_text = "".join(lines)
            if len(log_text) > 4000:
                log_text = log_text[-4000:]
            await update.message.reply_text(f"<pre>{html.escape(log_text)}</pre>", parse_mode='HTML')
    except Exception as e:
        await update.message.reply_text(f"Error reading logs: {e}")

@force_sub_required
async def alive_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(small_caps("Bot is alive and running! ✅"))

@force_sub_required
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    count = get_user_count()
    await update.message.reply_text(small_caps(f"Total registered users: {count}"))

@force_sub_required
async def connect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    if not context.args:
        await update.message.reply_text("Usage: /connect <group_id or @group_username>")
        return
    identifier = context.args[0]
    try:
        chat = await context.bot.get_chat(identifier)
        if chat.type not in ['group', 'supergroup']:
            await update.message.reply_text("Not a group.")
            return
        with db_manager.get_cursor() as cur:
            cur.execute("""
                INSERT INTO connected_groups (group_id, group_username, group_title, connected_by)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (group_id) DO UPDATE SET active = TRUE
            """, (chat.id, chat.username, chat.title, update.effective_user.id))
        await update.message.reply_text(f"✅ Connected to {chat.title} (ID: {chat.id})")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

@force_sub_required
async def disconnect_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    if not context.args:
        await update.message.reply_text("Usage: /disconnect <group_id or @group_username>")
        return
    identifier = context.args[0]
    try:
        chat = await context.bot.get_chat(identifier)
        with db_manager.get_cursor() as cur:
            cur.execute("UPDATE connected_groups SET active = FALSE WHERE group_id = %s", (chat.id,))
        await update.message.reply_text(f"✅ Disconnected from {chat.title}")
    except Exception as e:
        await update.message.reply_text(f"Error: {e}")

@force_sub_required
async def connections_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    with db_manager.get_cursor() as cur:
        cur.execute("SELECT group_id, group_username, group_title, connected_at FROM connected_groups WHERE active = TRUE")
        rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No connected groups.")
        return
    text = small_caps("Connected Groups:\n\n")
    for gid, uname, title, at in rows:
        uname_str = f"@{uname}" if uname else ""
        text += f"• {title} {uname_str} (ID: {gid})\n"
    await update.message.reply_text(text, parse_mode='HTML')

@force_sub_required
async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    message = update.message
    reply_to = message.reply_to_message

    text = f"Chat ID: <code>{chat_id}</code>\nYour User ID: <code>{user_id}</code>"
    if reply_to:
        if reply_to.from_user:
            text += f"\nReplied User ID: <code>{reply_to.from_user.id}</code>"
        if reply_to.forward_from:
            text += f"\nForwarded User ID: <code>{reply_to.forward_from.id}</code>"
        if reply_to.forward_from_chat:
            text += f"\nForwarded Chat ID: <code>{reply_to.forward_from_chat.id}</code>"
        if reply_to.video:
            text += f"\nVideo File ID: <code>{reply_to.video.file_id}</code>"
        if reply_to.photo:
            text += f"\nPhoto File ID: <code>{reply_to.photo[-1].file_id}</code>"
        if reply_to.document:
            text += f"\nDocument File ID: <code>{reply_to.document.file_id}</code>"
        if reply_to.audio:
            text += f"\nAudio File ID: <code>{reply_to.audio.file_id}</code>"
        if reply_to.voice:
            text += f"\nVoice File ID: <code>{reply_to.voice.file_id}</code>"
        if reply_to.sticker:
            text += f"\nSticker File ID: <code>{reply_to.sticker.file_id}</code>"

    await message.reply_text(text, parse_mode='HTML')

@force_sub_required
async def info_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target = None
    if update.message.reply_to_message:
        target = update.message.reply_to_message.from_user
    elif context.args:
        try:
            identifier = context.args[0]
            target = await context.bot.get_chat(identifier)
        except Exception as e:
            await update.message.reply_text(f"Error: {e}")
            return
    else:
        target = update.effective_user

    if not target:
        await update.message.reply_text("No target specified.")
        return

    text = f"<b>ID:</b> <code>{target.id}</code>\n"
    if hasattr(target, 'username') and target.username:
        text += f"<b>Username:</b> @{target.username}\n"
    if hasattr(target, 'first_name'):
        text += f"<b>First Name:</b> {html.escape(target.first_name)}\n"
    if hasattr(target, 'last_name') and target.last_name:
        text += f"<b>Last Name:</b> {html.escape(target.last_name)}\n"
    if hasattr(target, 'title'):
        text += f"<b>Title:</b> {html.escape(target.title)}\n"
    if hasattr(target, 'type'):
        text += f"<b>Type:</b> {target.type}\n"
    if hasattr(target, 'description'):
        text += f"<b>Description:</b> {html.escape(target.description)}\n"
    if hasattr(target, 'invite_link'):
        text += f"<b>Invite Link:</b> {target.invite_link}\n"

    await update.message.reply_text(text, parse_mode='HTML')

@force_sub_required
async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reload_command(update, context)

# ================================================================================
#                           DEEP LINK HANDLER
# ================================================================================

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE, link_id: str):
    chat_id = update.effective_chat.id
    link_info = get_link_info(link_id)
    if not link_info:
        await context.bot.send_message(
            chat_id,
            small_caps("This link is invalid or not registered."),
            parse_mode='HTML'
        )
        return

    channel_identifier, creator_id, created_time, never_expires = link_info
    try:
        if isinstance(channel_identifier, str) and channel_identifier.lstrip('-').isdigit():
            channel_identifier = int(channel_identifier)

        if not never_expires:
            created_dt = datetime.fromisoformat(str(created_time))
            if datetime.now() > created_dt + timedelta(minutes=LINK_EXPIRY_MINUTES):
                await context.bot.send_message(
                    chat_id,
                    small_caps("This link has expired."),
                    parse_mode='HTML'
                )
                return

        if I_AM_CLONE:
            main_token = get_main_bot_token()
            if not main_token:
                await context.bot.send_message(
                    chat_id,
                    small_caps("Main bot token not configured yet. Please contact admin."),
                    parse_mode='HTML'
                )
                return
            link_creator = Bot(token=main_token)
        else:
            link_creator = context.bot

        chat = await link_creator.get_chat(channel_identifier)
        invite_link = await link_creator.create_chat_invite_link(
            chat.id,
            expire_date=datetime.now().timestamp() + LINK_EXPIRY_MINUTES * 60
        )
        keyboard = [[bold_button("• Join Channel •", url=invite_link.invite_link)]]
        await context.bot.send_message(
            chat_id,
            small_caps(
                "Here is your link! Click below to proceed.\n\n"
                "Note: If the link expires, tap the original post link again to get a fresh one."
            ),
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error generating invite link: {e}")
        await context.bot.send_message(
            chat_id,
            small_caps("Error creating link. Contact admin."),
            parse_mode='HTML'
        )

# ================================================================================
#                           VIDEO HANDLER (UPLOAD)
# ================================================================================

async def handle_upload_video(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return
    if update.effective_chat.type != 'private':
        return
    if not update.message.video:
        return

    async with upload_lock:
        await load_upload_progress()
        if not progress["target_chat_id"]:
            await update.message.reply_text("❌ Target channel not set! Use /upload to set it.")
            return
        if not progress["selected_qualities"]:
            await update.message.reply_text("❌ No qualities selected! Use /upload to configure.")
            return

        file_id = update.message.video.file_id
        quality = progress["selected_qualities"][progress["video_count"] % len(progress["selected_qualities"])]

        caption = progress["base_caption"] \
            .replace("{season}", f"{progress['season']:02}") \
            .replace("{episode}", f"{progress['episode']:02}") \
            .replace("{total_episode}", f"{progress['total_episode']:02}") \
            .replace("{quality}", quality)

        try:
            await context.bot.get_chat(progress["target_chat_id"])
            sent_msg = await context.bot.send_video(
                chat_id=progress["target_chat_id"],
                video=file_id,
                caption=caption,
                parse_mode='HTML'
            )
            await update.message.reply_text(f"✅ Video forwarded with caption:\n{caption}")
            progress["video_count"] += 1
            if progress["video_count"] >= len(progress["selected_qualities"]):
                progress["episode"] += 1
                progress["total_episode"] += 1
                progress["video_count"] = 0
            await save_upload_progress()
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")

# ================================================================================
#                           CHANNEL POST HANDLER (AUTO-CAPTION)
# ================================================================================

async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.channel_post or not update.channel_post.video:
        return
    chat_id = update.effective_chat.id
    await load_upload_progress()
    if chat_id != progress.get("target_chat_id") or not progress.get("auto_caption_enabled"):
        return
    async with upload_lock:
        quality = progress["selected_qualities"][progress["video_count"] % len(progress["selected_qualities"])]
        caption = progress["base_caption"] \
            .replace("{season}", f"{progress['season']:02}") \
            .replace("{episode}", f"{progress['episode']:02}") \
            .replace("{total_episode}", f"{progress['total_episode']:02}") \
            .replace("{quality}", quality)
        try:
            await context.bot.edit_message_caption(
                chat_id=chat_id,
                message_id=update.channel_post.message_id,
                caption=caption,
                parse_mode='HTML'
            )
            progress["video_count"] += 1
            if progress["video_count"] >= len(progress["selected_qualities"]):
                progress["episode"] += 1
                progress["total_episode"] += 1
                progress["video_count"] = 0
            await save_upload_progress()
        except Exception as e:
            logger.error(f"Auto‑caption error: {e}")

# ================================================================================
#                           PHOTO HANDLER (FOR CATEGORY LOGO)
# ================================================================================

async def handle_admin_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_states:
        return
    state = user_states[user_id]
    if state != SET_CATEGORY_LOGO:
        return

    if update.message.photo:
        file_id = update.message.photo[-1].file_id
    elif update.message.document:
        file_id = update.message.document.file_id
    else:
        await update.message.reply_text("❌ Please send an image.")
        return

    category = context.user_data.get('editing_category')
    if category:
        update_category_logo(category, file_id)
        await update.message.reply_text(small_caps(f"Logo for {category} updated."))
    user_states.pop(user_id, None)
    await send_admin_menu(update.effective_chat.id, context)

# ================================================================================
#                           SCHEDULED BROADCASTS JOB (FULL IMPLEMENTATION)
# ================================================================================

async def check_scheduled_broadcasts(context: ContextTypes.DEFAULT_TYPE):
    """Check for pending scheduled broadcasts and execute them."""
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT id, admin_id, message_text, media_file_id, media_type
            FROM scheduled_broadcasts
            WHERE status = 'pending' AND execute_at <= NOW()
        """)
        rows = cur.fetchall()

    for row in rows:
        b_id, admin_id, text, media_file_id, media_type = row
        try:
            # Create a fake update-like object to reuse broadcast function
            # We'll just call broadcast_message_to_all_users with a dummy message
            # Since we can't easily create a real Update, we'll implement a separate send function
            # For simplicity, we'll call a helper that sends to all users
            await send_scheduled_broadcast(context, b_id, admin_id, text, media_file_id, media_type)
            with db_manager.get_cursor() as cur2:
                cur2.execute("UPDATE scheduled_broadcasts SET status = 'sent' WHERE id = %s", (b_id,))
        except Exception as e:
            logger.error(f"Scheduled broadcast {b_id} failed: {e}")
            with db_manager.get_cursor() as cur2:
                cur2.execute("UPDATE scheduled_broadcasts SET status = 'failed' WHERE id = %s", (b_id,))

async def send_scheduled_broadcast(context, b_id, admin_id, text, media_file_id, media_type):
    """Send a scheduled broadcast to all users."""
    total = get_user_count()
    broadcast_id = None
    with db_manager.get_cursor() as cur:
        cur.execute("""
            INSERT INTO broadcast_history (admin_id, mode, total_users, message_text)
            VALUES (%s, 'scheduled', %s, %s) RETURNING id
        """, (admin_id, total, text))
        broadcast_id = cur.fetchone()[0]

    # If total is small, send immediately
    if total < BROADCAST_MIN_USERS:
        sent = fail = blocked = deleted = 0
        for u in get_all_users(limit=None, offset=0):
            try:
                if media_file_id:
                    # For media, we need to send with appropriate method
                    # For simplicity, we'll just send text for now
                    await context.bot.send_message(chat_id=u[0], text=text, parse_mode='HTML')
                else:
                    await context.bot.send_message(chat_id=u[0], text=text, parse_mode='HTML')
                sent += 1
            except Forbidden as e:
                fail += 1
                if "blocked" in str(e).lower():
                    blocked += 1
                elif "deactivated" in str(e).lower() or "deleted" in str(e).lower():
                    deleted += 1
            except Exception as e:
                fail += 1
                broadcast_logger.warning(f"Broadcast fail {u[0]}: {e}")
            await asyncio.sleep(0.05)
        with db_manager.get_cursor() as cur:
            cur.execute("""
                UPDATE broadcast_history SET completed_at = NOW(), success = %s, blocked = %s, deleted = %s, failed = %s
                WHERE id = %s
            """, (sent, blocked, deleted, fail, broadcast_id))
        return

    # Throttled broadcast
    offset = delay = chunks = 0
    total_chunks = (total + BROADCAST_CHUNK_SIZE - 1) // BROADCAST_CHUNK_SIZE
    while offset < total:
        is_last = (offset + BROADCAST_CHUNK_SIZE) >= total
        context.job_queue.run_once(
            scheduled_broadcast_worker,
            when=delay,
            data={
                'offset': offset,
                'chunk_size': BROADCAST_CHUNK_SIZE,
                'text': text,
                'media_file_id': media_file_id,
                'media_type': media_type,
                'is_last_chunk': is_last,
                'broadcast_id': broadcast_id
            },
            name=f"sched_bc_{b_id}_{chunks}"
        )
        offset += BROADCAST_CHUNK_SIZE
        delay += BROADCAST_INTERVAL_MIN * 60
        chunks += 1

async def scheduled_broadcast_worker(context: ContextTypes.DEFAULT_TYPE):
    jd = context.job.data
    offset = jd['offset']
    chunk_size = jd['chunk_size']
    text = jd['text']
    media_file_id = jd['media_file_id']
    media_type = jd['media_type']
    is_last = jd['is_last_chunk']
    broadcast_id = jd.get('broadcast_id')

    users = get_all_users(limit=chunk_size, offset=offset)
    sent = fail = blocked = deleted = 0
    for u in users:
        try:
            if media_file_id:
                # Placeholder for media sending
                await context.bot.send_message(chat_id=u[0], text=text, parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id=u[0], text=text, parse_mode='HTML')
            sent += 1
        except Forbidden as e:
            fail += 1
            if "blocked" in str(e).lower():
                blocked += 1
            elif "deactivated" in str(e).lower() or "deleted" in str(e).lower():
                deleted += 1
        except Exception as e:
            fail += 1
            broadcast_logger.warning(f"Scheduled broadcast fail {u[0]}: {e}")
        await asyncio.sleep(0.05)

    if is_last and broadcast_id:
        with db_manager.get_cursor() as cur:
            cur.execute("""
                UPDATE broadcast_history
                SET completed_at = NOW(),
                    success = success + %s,
                    blocked = blocked + %s,
                    deleted = deleted + %s,
                    failed = failed + %s
                WHERE id = %s
            """, (sent, blocked, deleted, fail, broadcast_id))

# ================================================================================
#                           LIFECYCLE FUNCTIONS
# ================================================================================

async def post_init(application: Application):
    global BOT_USERNAME, I_AM_CLONE
    me = await application.bot.get_me()
    BOT_USERNAME = me.username

    try:
        I_AM_CLONE = am_i_a_clone_token(BOT_TOKEN)
    except Exception:
        I_AM_CLONE = False

    if not I_AM_CLONE:
        set_main_bot_token(BOT_TOKEN)
        logger.info("✅ Main bot token saved to DB")

    logger.info(f"✅ Bot identified as @{BOT_USERNAME} [{'CLONE' if I_AM_CLONE else 'MAIN'}]")
    await health_server.start()
    logger.info("✅ Health check server started")

    if application.job_queue:
        # Auto-forward periodic check (optional, we already have event-driven)
        # application.job_queue.run_repeating(auto_forward_job, interval=60, first=10)
        application.job_queue.run_repeating(manga_update_job, interval=3600, first=60)
        application.job_queue.run_repeating(cleanup_expired_links, interval=600, first=30)
        application.job_queue.run_repeating(check_scheduled_broadcasts, interval=60, first=30)

    # Send restart notification with timestamp
    triggered_by = BOT_USERNAME
    if os.path.exists('restart_message.json'):
        try:
            with open('restart_message.json') as f:
                rinfo = json.load(f)
            triggered_by = rinfo.get('triggered_by', BOT_USERNAME)
        except Exception:
            pass

    time_str = datetime.now().strftime("%I:%M %p").lstrip('0')
    restart_text = f"<b>BOT RESTARTED by @{triggered_by}</b>\n{time_str}"
    try:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=restart_text,
            parse_mode='HTML'
        )
        logger.info("✅ Restart notification sent")
    except Exception as e:
        logger.warning(f"Could not send restart notification: {e}")

async def post_shutdown(application: Application):
    await health_server.stop()
    if db_manager:
        db_manager.close_all()
    logger.info("✅ Shutdown complete")

# ================================================================================
#                           ERROR HANDLER
# ================================================================================

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error_logger.error(f"Exception while handling an update: {context.error}", exc_info=True)

    error_text = f"⚠️ <b>Bot Error</b>\n<pre>{html.escape(str(context.error))}</pre>"
    if update and update.effective_chat:
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="An internal error occurred. Admin has been notified."
            )
        except:
            pass
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=error_text,
            parse_mode='HTML'
        )
    except Exception:
        pass

# ================================================================================
#                                   MAIN
# ================================================================================

def main():
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_TOKEN_HERE":
        logger.error("BOT_TOKEN not set!")
        return
    if not DATABASE_URL:
        logger.error("DATABASE_URL not set!")
        return

    try:
        init_db(DATABASE_URL)
        logger.info("✅ Database connected")
    except Exception as e:
        logger.error(f"❌ Database error: {e}")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    try:
        user_count = get_user_count()
        logger.info(f"✅ DB working — {user_count} users")
    except Exception as e:
        logger.error(f"❌ DB test failed: {e}")
        return

    if os.path.exists('restart_message.json'):
        try:
            with open('restart_message.json') as f:
                restart_info = json.load(f)
            os.remove('restart_message.json')
            original_chat_id = restart_info['chat_id']
            admin_id = restart_info['admin_id']
            message_id_to_copy = restart_info.get('message_id_to_copy')
            triggered_by = restart_info.get("triggered_by", "Unknown")

            async def post_restart_notification(ctx: ContextTypes.DEFAULT_TYPE):
                time_str = datetime.now().strftime("%I:%M %p").lstrip('0')
                restart_text = f"<b>BOT RESTARTED by @{triggered_by}</b>\n{time_str}"
                try:
                    await ctx.bot.send_message(
                        original_chat_id,
                        restart_text,
                        parse_mode='HTML'
                    )
                    if original_chat_id != admin_id:
                        try:
                            await ctx.bot.send_message(
                                admin_id,
                                restart_text,
                                parse_mode='HTML'
                            )
                        except Exception:
                            pass
                    if message_id_to_copy == 'admin':
                        await send_admin_menu(original_chat_id, ctx)
                    elif message_id_to_copy:
                        try:
                            await ctx.bot.copy_message(
                                original_chat_id,
                                WELCOME_SOURCE_CHANNEL,
                                message_id_to_copy
                            )
                        except Exception:
                            await send_admin_menu(original_chat_id, ctx)
                    else:
                        await send_admin_menu(original_chat_id, ctx)
                except Exception as e:
                    logger.error(f"Post‑restart notify failed: {e}")

            application.job_queue.run_once(post_restart_notification, 1)
        except Exception as e:
            logger.error(f"Restart file error: {e}")

    # Handler registration
    admin_filter = filters.User(user_id=ADMIN_ID)

    # Admin commands
    application.add_handler(CommandHandler("start",         start))
    application.add_handler(CommandHandler("backup",        backup_command,   filters=admin_filter))
    application.add_handler(CommandHandler("move",          move_command,     filters=admin_filter))
    application.add_handler(CommandHandler("addclone",      addclone_command, filters=admin_filter))
    application.add_handler(CommandHandler("reload",        reload_command,   filters=admin_filter))
    application.add_handler(CommandHandler("stats",         stats_command,    filters=admin_filter))
    application.add_handler(CommandHandler("addchannel",    add_channel_command,    filters=admin_filter))
    application.add_handler(CommandHandler("removechannel", remove_channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("banuser",       ban_user_command,   filters=admin_filter))
    application.add_handler(CommandHandler("unbanuser",     unban_user_command, filters=admin_filter))
    application.add_handler(CommandHandler("listusers",     listusers_command, filters=admin_filter))
    application.add_handler(CommandHandler("deleteuser",    deleteuser_command, filters=admin_filter))
    application.add_handler(CommandHandler("test",          test_command))

    # New admin commands
    application.add_handler(CommandHandler("cmd",           cmd_command,      filters=admin_filter))
    application.add_handler(CommandHandler("commands",      cmd_command,      filters=admin_filter))
    application.add_handler(CommandHandler("ping",          ping_command))
    application.add_handler(CommandHandler("sysstats",      sysstats_command, filters=admin_filter))
    application.add_handler(CommandHandler("manga",         manga_command,    filters=admin_filter))
    application.add_handler(CommandHandler("anime",         anime_command,    filters=admin_filter))
    application.add_handler(CommandHandler("movie",         movie_command,    filters=admin_filter))
    application.add_handler(CommandHandler("tvshow",        tvshow_command,   filters=admin_filter))
    application.add_handler(CommandHandler("help",          help_command))
    application.add_handler(CommandHandler("settings",      settings_command, filters=admin_filter))
    application.add_handler(CommandHandler("autoupdate",    autoupdate_command, filters=admin_filter))
    application.add_handler(CommandHandler("autoforward",   autoforward_command, filters=admin_filter))
    application.add_handler(CommandHandler("upload",        upload_command, filters=admin_filter))
    application.add_handler(CommandHandler("search",        search_command))
    application.add_handler(CommandHandler("broadcaststats", broadcaststats_command, filters=admin_filter))
    application.add_handler(CommandHandler("exportusers",   exportusers_command, filters=admin_filter))
    application.add_handler(CommandHandler("channel",       channel_command, filters=admin_filter))
    application.add_handler(CommandHandler("logs",          logs_command, filters=admin_filter))
    application.add_handler(CommandHandler("alive",         alive_command))
    application.add_handler(CommandHandler("users",         users_command, filters=admin_filter))
    application.add_handler(CommandHandler("connect",       connect_command, filters=admin_filter))
    application.add_handler(CommandHandler("disconnect",    disconnect_command, filters=admin_filter))
    application.add_handler(CommandHandler("connections",   connections_command, filters=admin_filter))
    application.add_handler(CommandHandler("id",            id_command))
    application.add_handler(CommandHandler("info",          info_command))
    application.add_handler(CommandHandler("restart",       restart_command, filters=admin_filter))

    # Message handlers
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    application.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.TEXT & ~filters.COMMAND, group_message_handler))
    application.add_handler(InlineQueryHandler(inline_query_handler))
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL, auto_forward_message_handler))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.VIDEO & filters.User(user_id=ADMIN_ID), handle_upload_video))
    application.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.VIDEO, handle_channel_post))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & (filters.PHOTO | filters.Document.IMAGE) & filters.User(user_id=ADMIN_ID), handle_admin_photo))

    application.add_error_handler(error_handler)

    application.post_init = post_init
    application.post_shutdown = post_shutdown

    logger.info("🚀 Starting bot…")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

# ================================================================================
#                           UPLOAD MANAGER HELPERS
# ================================================================================

DEFAULT_CAPTION = (
    "<b>◈ Anime Name</b>\n\n"
    "<b>- Season:</b> {season}\n"
    "<b>- Episode:</b> {episode}\n"
    "<b>- Audio track:</b> Hindi | Official\n"
    "<b>- Quality:</b> {quality}\n"
    "<blockquote>\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>POWERED BY:</b> @beeetanime\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>MAIN Channel:</b> @Beat_Hindi_Dubbed\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    " <b>Group :</b> @Beat_Anime_Discussion\n"
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▱▱\n"
    "</blockquote>"
)

ALL_QUALITIES = ["480p", "720p", "1080p", "4K", "2160p"]

progress = {
    "target_chat_id": None,
    "season": 1,
    "episode": 1,
    "total_episode": 1,
    "video_count": 0,
    "selected_qualities": ["480p", "720p", "1080p"],
    "base_caption": DEFAULT_CAPTION,
    "auto_caption_enabled": True
}

upload_lock = asyncio.Lock()

async def load_upload_progress():
    global progress
    with db_manager.get_cursor() as cur:
        cur.execute("""
            SELECT target_chat_id, season, episode, total_episode, video_count,
                   selected_qualities, base_caption, auto_caption_enabled
            FROM bot_progress WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            progress.update({
                'target_chat_id': row[0],
                'season': row[1],
                'episode': row[2],
                'total_episode': row[3],
                'video_count': row[4],
                'selected_qualities': row[5].split(',') if row[5] else [],
                'base_caption': row[6] or DEFAULT_CAPTION,
                'auto_caption_enabled': row[7]
            })

async def save_upload_progress():
    with db_manager.get_cursor() as cur:
        cur.execute("""
            UPDATE bot_progress SET
                target_chat_id = %s,
                season = %s,
                episode = %s,
                total_episode = %s,
                video_count = %s,
                selected_qualities = %s,
                base_caption = %s,
                auto_caption_enabled = %s
            WHERE id = 1
        """, (
            progress['target_chat_id'],
            progress['season'],
            progress['episode'],
            progress['total_episode'],
            progress['video_count'],
            ','.join(progress['selected_qualities']),
            progress['base_caption'],
            progress['auto_caption_enabled']
        ))

async def show_upload_menu(chat_id, context, edit_msg_id=None):
    target_status = f"✅ Set: {progress['target_chat_id']}" if progress['target_chat_id'] else "❌ Not Set"
    auto_status = f"Auto-Caption: {'✅ ON' if progress['auto_caption_enabled'] else '❌ OFF'}"
    text = small_caps(
        f"UPLOAD MANAGER\n\n"
        f"Target Channel: {target_status}\n"
        f"{auto_status}\n"
        f"Season: {progress['season']} | Episode: {progress['episode']} / {progress['total_episode']}\n"
        f"Qualities: {', '.join(progress['selected_qualities'])}"
    )
    keyboard = get_upload_menu_markup()
    if edit_msg_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=edit_msg_id, text=text, parse_mode='HTML', reply_markup=keyboard)
    else:
        await context.bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=keyboard)

def get_upload_menu_markup():
    auto_status = '✅ ON' if progress['auto_caption_enabled'] else '❌ OFF'
    # 2×2 grid for upload menu
    keyboard = [
        [bold_button("Preview Caption", callback_data="upload_preview"),
         bold_button("Set Caption", callback_data="upload_set_caption")],
        [bold_button("Set Season", callback_data="upload_set_season"),
         bold_button("Set Episode", callback_data="upload_set_episode")],
        [bold_button("Set Total Episodes", callback_data="upload_set_total"),
         bold_button("Quality Settings", callback_data="upload_quality_menu")],
        [bold_button("Set Target Channel", callback_data="upload_set_channel"),
         bold_button(f"Auto-Caption: {auto_status}", callback_data="upload_toggle_auto")],
        [bold_button("Reset Episode", callback_data="upload_reset")],
        [bold_button("🗑 Clear Database", callback_data="upload_clear_db")],
        [bold_button("🔙 Back", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(keyboard)

async def upload_preview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    quality = progress["selected_qualities"][progress["video_count"] % len(progress["selected_qualities"])] if progress["selected_qualities"] else "N/A"
    preview_text = progress["base_caption"] \
        .replace("{season}", f"{progress['season']:02}") \
        .replace("{episode}", f"{progress['episode']:02}") \
        .replace("{total_episode}", f"{progress['total_episode']:02}") \
        .replace("{quality}", quality)

    target_status = f"✅ {progress['target_chat_id']}" if progress['target_chat_id'] else "❌ Not Set"
    auto_status = f"Auto-Caption: {'✅ ON' if progress['auto_caption_enabled'] else '❌ OFF'}"
    await query.edit_message_text(
        f"📝 <b>Preview Caption:</b>\n\n{preview_text}\n\n<b>Current Settings:</b>\n"
        f"Target Channel: {target_status}\n"
        f"{auto_status}\n"
        f"Season: {progress['season']}\n"
        f"Episode: {progress['episode']}\n"
        f"Total Episode: {progress['total_episode']}\n"
        f"Selected Qualities: {', '.join(progress['selected_qualities'])}",
        parse_mode='HTML',
        reply_markup=get_upload_menu_markup()
    )

@force_sub_required
async def upload_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    user_states.pop(update.effective_user.id, None)
    await delete_bot_prompt(context, update.effective_chat.id)
    await load_upload_progress()
    await show_upload_menu(update.effective_chat.id, context)

# ================================================================================
#                           WATERMARK HELPER
# ================================================================================

async def add_watermark(image_url: str, text: str, position: str = 'center') -> Optional[BytesIO]:
    """Download an image, add watermark text, return BytesIO."""
    try:
        response = requests.get(image_url, timeout=10)
        img = Image.open(BytesIO(response.content)).convert('RGBA')
        txt = Image.new('RGBA', img.size, (255,255,255,0))
        draw = ImageDraw.Draw(txt)
        try:
            font = ImageFont.truetype("arial.ttf", 36)
        except:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0,0), text, font=font)
        text_w = bbox[2] - bbox[0]
        text_h = bbox[3] - bbox[1]
        if position == 'bottom':
            pos = ((img.width - text_w)//2, img.height - text_h - 10)
        elif position == 'top':
            pos = ((img.width - text_w)//2, 10)
        elif position == 'left':
            pos = (10, (img.height - text_h)//2)
        elif position == 'right':
            pos = (img.width - text_w - 10, (img.height - text_h)//2)
        else:  # center
            pos = ((img.width - text_w)//2, (img.height - text_h)//2)
        draw.text(pos, text, fill=(255,255,255,128), font=font)
        watermarked = Image.alpha_composite(img, txt)
        output = BytesIO()
        watermarked.save(output, format='PNG')
        output.seek(0)
        return output
    except Exception as e:
        logger.error(f"Watermark error: {e}")
        return None

if __name__ == "__main__":
    main()

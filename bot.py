import os
import logging
import sys
import json
import asyncio
from datetime import datetime, timedelta
from functools import wraps

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Application, CommandHandler, CallbackQueryHandler,
                           ContextTypes, MessageHandler, filters)
from telegram import Bot

from database_safe import *
from health_check import health_server

# ─────────────────────────────────── LOGGING ─────────────────────────────────

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────── CONFIG ──────────────────────────────────

BOT_TOKEN    = os.getenv("BOT_TOKEN", "YOUR_TOKEN_HERE")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_ID     = 829342319
LINK_EXPIRY_MINUTES = 5

BROADCAST_CHUNK_SIZE  = 1000
BROADCAST_MIN_USERS   = 5000
BROADCAST_INTERVAL_MIN = 20

PORT        = int(os.environ.get('PORT', 8080))
WEBHOOK_URL = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/') + '/'

WELCOME_SOURCE_CHANNEL  = -1002530952988
WELCOME_SOURCE_MESSAGE_ID = 32
PUBLIC_ANIME_CHANNEL_URL  = "https://t.me/BeatAnime"
REQUEST_CHANNEL_URL       = "https://t.me/Beat_Hindi_Dubbed"
ADMIN_CONTACT_USERNAME    = "Beat_Anime_Ocean"

# Current bot username (resolved at startup)
BOT_USERNAME: str = ""

# ─────────────────────────────────── STATES ──────────────────────────────────

(ADD_CHANNEL_USERNAME,       # 0
 ADD_CHANNEL_TITLE,          # 1
 GENERATE_LINK_CHANNEL_USERNAME,  # 2
 PENDING_BROADCAST,          # 3
 GENERATE_LINK_CHANNEL_TITLE,     # 4  ← new
 ADD_CLONE_TOKEN,            # 5  ← new
 PENDING_FILL_TITLE,         # 6  ← new: filling missing link titles one-by-one
 SET_BACKUP_CHANNEL,         # 7  ← new
 PENDING_MOVE_TARGET,        # 8  ← new
 ADD_CHANNEL_JBR             # 9  ← new: after title, ask join-by-request?
 ) = range(10)

user_states: dict = {}

# ─────────────────────────── MAINTENANCE HELPER ──────────────────────────────

async def _send_maintenance_block(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send maintenance message to a blocked new user."""
    backup_url = get_setting("backup_channel_url", "")
    text = (
        "🔧 <b>Bot Under Maintenance</b>\n\n"
        "<blockquote><b>We are currently performing scheduled maintenance.\n"
        "Existing members can still use the bot normally.</b></blockquote>\n\n"
        "<b>Please join our backup channel to stay updated.</b>"
    )
    keyboard = []
    if backup_url:
        keyboard.append([InlineKeyboardButton(" Backup Channel", url=backup_url)])

    if update.message:
        await update.message.reply_text(text, parse_mode='HTML',
                                        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)
        except Exception:
            await context.bot.send_message(
                update.effective_chat.id, text, parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None)

# ─────────────────────────── MESSAGE DELETE HELPERS ──────────────────────────

async def delete_update_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id == ADMIN_ID and user_states.get(user_id) == PENDING_BROADCAST:
        return
    if update.message:
        if update.message.text and update.message.text.startswith('/start'):
            return
        try:
            await update.message.delete()
        except Exception as e:
            logger.warning(f"Could not delete message: {e}")


async def delete_bot_prompt(context: ContextTypes.DEFAULT_TYPE, chat_id):
    prompt_id = context.user_data.pop('bot_prompt_message_id', None)
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=prompt_id)
        except Exception as e:
            logger.warning(f"Could not delete prompt {prompt_id}: {e}")
    return prompt_id

# ─────────────────────────── FORCE SUB LOGIC ─────────────────────────────────

async def is_user_subscribed(user_id: int, bot) -> bool:
    channels = get_all_force_sub_channels(return_usernames_only=True)
    if not channels:
        return True
    for ch in channels:
        try:
            member = await bot.get_chat_member(chat_id=ch, user_id=user_id)
            if member.status in ['left', 'kicked']:
                return False
        except Exception as e:
            logger.error(f"Error checking {ch} for {user_id}: {e}")
            return False
    return True


def force_sub_required(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user = update.effective_user
        if user is None:
            return await func(update, context, *args, **kwargs)

        # ── Maintenance check (skip admin + existing users) ───────────────
        if user.id != ADMIN_ID and is_maintenance_mode():
            if not is_existing_user(user.id):
                await delete_update_message(update, context)
                await _send_maintenance_block(update, context)
                return

        # ── Ban check ─────────────────────────────────────────────────────
        if is_user_banned(user.id):
            await delete_update_message(update, context)
            ban_text = "🚫 You have been banned from using this bot."
            if update.message:
                await update.message.reply_text(ban_text)
            elif update.callback_query:
                try:
                    await update.callback_query.edit_message_text(ban_text)
                except Exception:
                    await context.bot.send_message(update.effective_chat.id, ban_text)
            return

        # ── Admin bypasses force-sub ──────────────────────────────────────
        if user.id == ADMIN_ID:
            return await func(update, context, *args, **kwargs)

        channels_info = get_all_force_sub_channels(return_usernames_only=False)
        if not channels_info:
            return await func(update, context, *args, **kwargs)

        subscribed = await is_user_subscribed(user.id, context.bot)
        if not subscribed:
            await delete_update_message(update, context)
            keyboard = []
            lines = []
            for uname, title, jbr in channels_info:
                clean = uname.lstrip('@')
                if jbr:
                    btn_label = f" Request to Join — {title}"
                else:
                    btn_label = f" {title}"
                keyboard.append([InlineKeyboardButton(btn_label, url=f"https://t.me/{clean}")])
                lines.append(f"• <b>{title}</b> (<code>{uname}</code>)")

            keyboard.append([InlineKeyboardButton("🔄 I've Joined — Verify", callback_data="verify_subscription")])
            channels_text = "\n".join(lines)
            text = (
                "<b>Join our channels to use this bot:</b>\n\n"
                f"{channels_text}\n\n"
                "<blockquote><b>After joining all channels, tap Verify to continue.</b></blockquote>"
            )
            if update.message:
                await update.message.reply_text(text, parse_mode='HTML',
                                                reply_markup=InlineKeyboardMarkup(keyboard))
            elif update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode='HTML',
                                                              reply_markup=InlineKeyboardMarkup(keyboard))
            return

        return await func(update, context, *args, **kwargs)
    return wrapper

# ─────────────────────────────────── ADMIN COMMANDS ──────────────────────────

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

    restart_info = {
        'chat_id': update.effective_chat.id,
        'admin_id': ADMIN_ID,
        'message_id_to_copy': message_id_to_copy
    }
    try:
        with open('restart_message.json', 'w') as f:
            json.dump(restart_info, f)
    except Exception as e:
        logger.error(f"Failed to write restart file: {e}")

    await update.message.reply_text("🔄 **Bot is restarting...**", parse_mode='Markdown')
    sys.exit(0)


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


async def add_channel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await delete_update_message(update, context)
    await delete_bot_prompt(context, update.effective_chat.id)

    if len(context.args) < 2:
        await update.message.reply_text(
            "❌ Usage: `/addchannel @username Title`", parse_mode='Markdown')
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

    text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: <b>{user_count}</b>\n"
        f"📢 Force-Sub Channels: <b>{channel_count}</b>\n"
        f"🔗 Total Links: <b>{link_count}</b>\n"
        f"🤖 Active Clones: <b>{len(clones)}</b>\n"
        f"🔧 Maintenance: <b>{maint}</b>\n"
        f"⏱ Link Expiry: <b>{LINK_EXPIRY_MINUTES} min</b>"
    )
    keyboard = [[InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]]
    await update.message.reply_text(text, parse_mode='HTML',
                                    reply_markup=InlineKeyboardMarkup(keyboard))


# ─── /backup — list all links with channel names ─────────────────────────────

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
            "📂 No links found for this bot yet.", parse_mode='HTML')
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

    # Split into chunks of ~4096 chars
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
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    f"📝 Fill {len(missing)} missing titles",
                    callback_data="fill_missing_titles")
            ]])
        await update.message.reply_text(c.strip(), parse_mode='HTML', reply_markup=kb)


# ─── /move — reassign links to another bot ───────────────────────────────────

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
        "🔀 <b>Move Links</b>\n\n"
        "Send the <b>@username</b> of the target bot to move all current links to it.\n\n"
        "<blockquote>All deep links will be updated to use the new bot's username. "
        "Share this command carefully — links in posts will need manual updating.</blockquote>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
        ]])
    )
    context.user_data['bot_prompt_message_id'] = msg.message_id


async def _do_move(update, context, target_username: str):
    chat_id = update.effective_chat.id
    target_username = target_username.lstrip('@')

    # Fetch all links BEFORE moving (still under old username)
    all_links = get_all_links(bot_username=BOT_USERNAME, limit=500)

    if not all_links:
        await context.bot.send_message(
            chat_id,
            f"⚠️ No links found under <code>@{BOT_USERNAME}</code>.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK", callback_data="admin_back")
            ]])
        )
        return

    # Update DB — reassign source_bot_username
    count = move_links_to_bot(BOT_USERNAME, target_username)

    # Build the formatted list with NEW bot username
    # Split into Telegram-safe chunks (≤4096 chars)
    header = (
        f"✅ <b>Moved {count} link(s)</b>\n"
        f"<code>@{BOT_USERNAME}</code> → <code>@{target_username}</code>\n\n"
        f"📋 <b>Updated links for your channel index</b>\n"
        f"<blockquote>Copy each link below and replace the old ones in your posts.</blockquote>\n\n"
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

    # Send header + paginated link blocks
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
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")
            ]])
        await context.bot.send_message(chat_id, chunk.strip(), parse_mode='HTML',
                                       reply_markup=kb)


# ─── /addclone — register a clone bot ────────────────────────────────────────

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
        "🤖 <b>Add Clone Bot</b>\n\nSend the <b>BOT TOKEN</b> of the clone bot.\n\n"
        "<blockquote>The clone bot must be running the same bot code connected "
        "to this same database. Its links will be managed here.</blockquote>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
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
                f"✅ Clone bot <b>@{username}</b> registered!\n\n"
                "<blockquote>Deploy the same bot code with this token and the same DATABASE_URL. "
                "Links generated on the clone will use its username.</blockquote>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🤖 Manage Clones", callback_data="manage_clones")
                ]])
            )
        else:
            await context.bot.send_message(chat_id, "❌ Failed to save clone bot.", parse_mode='HTML')
    except Exception as e:
        await context.bot.send_message(
            chat_id, f"❌ Invalid token or API error:\n<code>{e}</code>", parse_mode='HTML')


# ─────────────────────────────────── START ───────────────────────────────────

@force_sub_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if update.message:
        await delete_update_message(update, context)
    if update.callback_query and update.callback_query.message:
        try:
            await update.callback_query.message.delete()
        except Exception:
            pass

    add_user(user.id, user.username, user.first_name, user.last_name)

    if context.args and len(context.args) > 0:
        link_id = context.args[0]

        # ── Clone redirect ────────────────────────────────────────────────
        # If redirect is ON, send the SAME link_id but with the clone bot's username.
        # The clone shares the same DB so it handles the link identically.
        clone_redirect = get_setting("clone_redirect_enabled", "false").lower() == "true"
        if clone_redirect and user.id != ADMIN_ID:
            clones = get_all_clone_bots(active_only=True)
            if clones:
                # Use the first (primary) active clone
                _, _, clone_uname, _, _ = clones[0]
                clone_link = f"https://t.me/{clone_uname}?start={link_id}"
                await context.bot.send_message(
                    update.effective_chat.id,
                    "🔄 <b>ɢᴇᴛᴛɪɴɢ ʏᴏᴜʀ ʟɪɴᴋ…</b>\n\n"
                    "<blockquote><b>Tap below to access your channel link via our partner bot.<b></blockquote>",
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("• ɢᴇᴛ ʟɪɴᴋ •", url=clone_link)
                    ]])
                )
                return

        await handle_channel_link_deep(update, context, link_id)
        return

    if user.id == ADMIN_ID:
        await delete_bot_prompt(context, update.effective_chat.id)
        user_states.pop(user.id, None)
        await send_admin_menu(update.effective_chat.id, context)
    else:
        keyboard = [
            [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
            [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
            [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
            [
                InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                InlineKeyboardButton("ᴄʟᴏsᴇ", callback_data="close_message")
            ]
        ]
        try:
            await context.bot.copy_message(
                chat_id=update.effective_chat.id,
                from_chat_id=WELCOME_SOURCE_CHANNEL,
                message_id=WELCOME_SOURCE_MESSAGE_ID,
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            logger.error(f"Error copying welcome: {e}")
            await context.bot.send_message(
                update.effective_chat.id,
                "👋 <b>Welcome!</b>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard)
            )


# ─────────────────────────────── ADMIN MESSAGE HANDLER ───────────────────────

@force_sub_required
async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID or user_id not in user_states:
        return

    state = user_states[user_id]
    text  = update.message.text
    await delete_bot_prompt(context, update.effective_chat.id)

    # ── Broadcast ────────────────────────────────────────────────────────────
    if state == PENDING_BROADCAST:
        user_states.pop(user_id, None)
        await broadcast_message_to_all_users(update, context, update.message)
        await send_admin_menu(update.effective_chat.id, context)
        return

    # Non-text states that need text
    if text is None and state not in []:
        await delete_update_message(update, context)
        msg = await update.message.reply_text("❌ Send text only.", parse_mode='HTML')
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    # ── Add force-sub channel: step 1 — username ─────────────────────────────
    if state == ADD_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        if not text.startswith('@'):
            msg = await update.message.reply_text("❌ Include @ in username.", parse_mode='HTML')
            context.user_data['bot_prompt_message_id'] = msg.message_id
            return
        context.user_data['channel_username'] = text
        user_states[user_id] = ADD_CHANNEL_TITLE
        msg = await update.message.reply_text(
            "📝 Now send the <b>channel title</b>:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id

    # ── Add force-sub channel: step 2 — title ────────────────────────────────
    elif state == ADD_CHANNEL_TITLE:
        await delete_update_message(update, context)
        context.user_data['channel_title'] = text
        user_states[user_id] = ADD_CHANNEL_JBR
        msg = await update.message.reply_text(
            "🔐 Is this a <b>private channel</b> that uses join requests?",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📨 Yes – Request to Join", callback_data="channel_jbr_yes")],
                [InlineKeyboardButton("✅ No – Public / Direct Join", callback_data="channel_jbr_no")]
            ])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id

    # ── Generate link: step 1 — channel identifier ───────────────────────────
    elif state == GENERATE_LINK_CHANNEL_USERNAME:
        await delete_update_message(update, context)
        identifier = text.strip()
        if not (identifier.startswith('@') or identifier.startswith('-100')
                or identifier.lstrip('-').isdigit()):
            msg = await update.message.reply_text(
                "❌ Use @username or channel ID (-100...)", parse_mode='HTML')
            context.user_data['bot_prompt_message_id'] = msg.message_id
            return
        try:
            chat = await context.bot.get_chat(identifier)
        except Exception:
            await update.message.reply_text(
                "❌ Cannot access channel. Make bot admin there.", parse_mode='HTML')
            return
        context.user_data['generating_link_channel'] = str(identifier)
        context.user_data['generating_link_channel_title_hint'] = chat.title or identifier
        user_states[user_id] = GENERATE_LINK_CHANNEL_TITLE
        msg = await update.message.reply_text(
            f"✅ Channel found: <b>{chat.title}</b>\n\n"
            "📝 Now send a <b>label/title</b> for this link "
            "(used in /backup and link management):",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id

    # ── Generate link: step 2 — channel title ────────────────────────────────
    elif state == GENERATE_LINK_CHANNEL_TITLE:
        await delete_update_message(update, context)
        channel_identifier = context.user_data.pop('generating_link_channel', None)
        channel_title      = text
        user_states.pop(user_id, None)

        if not channel_identifier:
            await update.message.reply_text("❌ Session expired. Try again.")
            return

        link_id = generate_link_id(
            str(channel_identifier), user_id,
            never_expires=True,
            channel_title=channel_title,
            source_bot_username=BOT_USERNAME
        )
        botname  = context.bot.username
        deep_link = f"https://t.me/{botname}?start={link_id}"

        await update.message.reply_text(
            f"🔗 <b>Link Generated</b>\n\n"
            f"📌 Label: <b>{channel_title}</b>\n"
            f"📢 Channel: <code>{channel_identifier}</code>\n\n"
            f"<code>{deep_link}</code>\n\n"
            "<blockquote>This link persists across redeployments. "
            "Use /backup to list all links.</blockquote>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")
            ]])
        )

    # ── Add clone token ───────────────────────────────────────────────────────
    elif state == ADD_CLONE_TOKEN:
        await delete_update_message(update, context)
        user_states.pop(user_id, None)
        token = text.strip()
        await _register_clone(update, context, token)

    # ── Fill missing title (one-by-one) ──────────────────────────────────────
    elif state == PENDING_FILL_TITLE:
        await delete_update_message(update, context)
        queue      = context.user_data.get('fill_titles_queue', [])
        current_id = context.user_data.pop('current_fill_link_id', None)
        if current_id:
            update_link_title(current_id, text)
        if not queue:
            user_states.pop(user_id, None)
            await update.message.reply_text(
                "✅ <b>All titles filled!</b>\nUse /backup to review.",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📋 /backup", callback_data="cmd_backup")
                ]])
            )
            return
        next_link = queue.pop(0)
        context.user_data['fill_titles_queue']    = queue
        context.user_data['current_fill_link_id'] = next_link[0]
        remaining = len(queue) + 1
        msg = await update.message.reply_text(
            f"📝 <b>{remaining} remaining</b>\n\n"
            f"Channel: <code>{next_link[1]}</code>\n"
            "Send the title for this link:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⏭ Skip", callback_data="fill_title_skip"),
                InlineKeyboardButton("🛑 Stop", callback_data="admin_back")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id

    # ── Set backup channel URL ────────────────────────────────────────────────
    elif state == SET_BACKUP_CHANNEL:
        await delete_update_message(update, context)
        user_states.pop(user_id, None)
        url = text.strip()
        if not url.startswith("https://"):
            msg = await update.message.reply_text(
                "❌ Send a valid https:// URL for the backup channel.", parse_mode='HTML')
            context.user_data['bot_prompt_message_id'] = msg.message_id
            return
        set_setting("backup_channel_url", url)
        await update.message.reply_text(
            f"✅ Backup channel URL saved:\n<code>{url}</code>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK", callback_data="admin_settings")
            ]])
        )

    # ── Move links: enter target bot username ─────────────────────────────────
    elif state == PENDING_MOVE_TARGET:
        await delete_update_message(update, context)
        user_states.pop(user_id, None)
        target = text.strip().lstrip('@')
        await _do_move(update, context, target)


# ─────────────────────────────── BUTTON HANDLER ──────────────────────────────

@force_sub_required
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query   = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data    = query.data

    if data == "verify_subscription":
        return await start(update, context)

    # Cancel pending states on nav callbacks
    nav_resets = {"admin_back", "manage_force_sub", "user_management",
                  "manage_clones", "admin_settings"}
    if user_id == ADMIN_ID and user_id in user_states and data in nav_resets:
        await delete_bot_prompt(context, query.message.chat_id)
        user_states.pop(user_id, None)

    # ── close ─────────────────────────────────────────────────────────────────
    if data == "close_message":
        try:
            await query.delete_message()
        except Exception:
            pass
        return

    # ── verify + about + user-back ────────────────────────────────────────────
    if data == "about_bot":
        about_text = (
            "<b>About Us</b>\n\n"
            "<blockquote><b>Developed by @Beat_Anime_Ocean</b></blockquote>"
        )
        try:
            await query.delete_message()
        except Exception:
            pass
        await context.bot.send_message(
            query.message.chat_id, about_text, parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK", callback_data="user_back")
            ]])
        )
        return

    # ── channel join-by-request answer ───────────────────────────────────────
    if data in ("channel_jbr_yes", "channel_jbr_no"):
        if user_id != ADMIN_ID:
            return
        jbr     = (data == "channel_jbr_yes")
        uname   = context.user_data.pop('channel_username', None)
        title   = context.user_data.pop('channel_title', None)
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat_id)
        if uname and title:
            add_force_sub_channel(uname, title, join_by_request=jbr)
            jbr_label = "📨 Request to Join" if jbr else "✅ Direct Join"
            try:
                await query.edit_message_text(
                    f"✅ Channel added:\n<b>{title}</b> (<code>{uname}</code>)\n"
                    f"Mode: {jbr_label}",
                    parse_mode='HTML',
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton(" Manage Channels", callback_data="manage_force_sub")
                    ]])
                )
            except Exception:
                pass
        return

    # ── fill missing titles ───────────────────────────────────────────────────
    if data == "fill_missing_titles":
        if user_id != ADMIN_ID:
            return
        missing = get_links_without_title(bot_username=BOT_USERNAME)
        if not missing:
            await query.answer("No missing titles!", show_alert=True)
            return
        first = missing.pop(0)
        context.user_data['fill_titles_queue']    = list(missing)
        context.user_data['current_fill_link_id'] = first[0]
        user_states[user_id] = PENDING_FILL_TITLE
        try:
            await query.edit_message_text(
                f"📝 <b>Fill {len(missing)+1} missing titles</b>\n\n"
                f"Channel: <code>{first[1]}</code>\nSend the title for this link:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⏭ Skip", callback_data="fill_title_skip"),
                    InlineKeyboardButton("🛑 Stop", callback_data="admin_back")
                ]])
            )
        except Exception:
            pass
        return

    if data == "fill_title_skip":
        if user_id != ADMIN_ID:
            return
        queue = context.user_data.get('fill_titles_queue', [])
        if not queue:
            user_states.pop(user_id, None)
            await query.edit_message_text("✅ Done filling titles.", parse_mode='HTML')
            return
        next_link = queue.pop(0)
        context.user_data['fill_titles_queue']    = queue
        context.user_data['current_fill_link_id'] = next_link[0]
        try:
            await query.edit_message_text(
                f"📝 <b>{len(queue)+1} remaining</b>\n\n"
                f"Channel: <code>{next_link[1]}</code>\nSend the title for this link:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⏭ Skip", callback_data="fill_title_skip"),
                    InlineKeyboardButton("🛑 Stop", callback_data="admin_back")
                ]])
            )
        except Exception:
            pass
        return

    # ── cmd shortcuts ────────────────────────────────────────────────────────
    if data == "cmd_backup":
        if user_id != ADMIN_ID:
            return
        class FakeUpdate:
            effective_user = query.from_user
            effective_chat = query.message.chat
            message        = None
            callback_query = query
        await backup_command(FakeUpdate(), context)
        return

    # ── admin nav ─────────────────────────────────────────────────────────────
    if data in ("admin_back", "user_back", "channels_back"):
        if user_id == ADMIN_ID:
            await send_admin_menu(query.message.chat_id, context, query)
        else:
            keyboard = [
                [InlineKeyboardButton("ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=PUBLIC_ANIME_CHANNEL_URL)],
                [InlineKeyboardButton("ᴄᴏɴᴛᴀᴄᴛ ᴀᴅᴍɪɴ", url=f"https://t.me/{ADMIN_CONTACT_USERNAME}")],
                [InlineKeyboardButton("ʀᴇǫᴜᴇsᴛ ᴀɴɪᴍᴇ ᴄʜᴀɴɴᴇʟ", url=REQUEST_CHANNEL_URL)],
                [InlineKeyboardButton("ᴀʙᴏᴜᴛ ᴍᴇ", callback_data="about_bot"),
                 InlineKeyboardButton("ᴄʟᴏsᴇ",  callback_data="close_message")]
            ]
            try:
                await query.delete_message()
            except Exception:
                pass
            try:
                await context.bot.copy_message(
                    chat_id=query.message.chat_id,
                    from_chat_id=WELCOME_SOURCE_CHANNEL,
                    message_id=WELCOME_SOURCE_MESSAGE_ID,
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                logger.error(f"Error copying back: {e}")
                await context.bot.send_message(
                    query.message.chat_id, "🏠 <b>Main Menu</b>",
                    parse_mode='HTML', reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # ── admin_stats ───────────────────────────────────────────────────────────
    if data == "admin_stats":
        if user_id != ADMIN_ID:
            return
        await send_admin_stats(query, context)
        return

    # ── admin_settings panel ──────────────────────────────────────────────────
    if data == "admin_settings":
        if user_id != ADMIN_ID:
            return
        await show_admin_settings(query, context)
        return

    if data == "toggle_maintenance":
        if user_id != ADMIN_ID:
            return
        new_state = toggle_maintenance_mode()
        state_txt = "🔴 ON" if new_state else "🟢 OFF"
        await query.answer(f"Maintenance mode: {state_txt}", show_alert=True)
        await show_admin_settings(query, context)
        return

    if data == "set_backup_channel":
        if user_id != ADMIN_ID:
            return
        user_states[user_id] = SET_BACKUP_CHANNEL
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await context.bot.send_message(
            query.message.chat_id,
            "📢 Send the <b>https:// URL</b> of your backup channel:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="admin_settings")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    if data == "toggle_clone_redirect":
        if user_id != ADMIN_ID:
            return
        current = get_setting("clone_redirect_enabled", "false").lower() == "true"
        set_setting("clone_redirect_enabled", "false" if current else "true")
        new_val = "🔴 ON" if not current else "🟢 OFF"
        await query.answer(f"Clone redirect: {new_val}", show_alert=True)
        await show_admin_settings(query, context)
        return

    # ── manage_clones ─────────────────────────────────────────────────────────
    if data == "manage_clones":
        if user_id != ADMIN_ID:
            return
        await show_clone_management(query, context)
        return

    if data == "add_clone_start":
        if user_id != ADMIN_ID:
            return
        user_states[user_id] = ADD_CLONE_TOKEN
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await context.bot.send_message(
            query.message.chat_id,
            "🤖 Send the <b>BOT TOKEN</b> of the clone bot:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="manage_clones")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    if data.startswith("remove_clone_"):
        if user_id != ADMIN_ID:
            return
        clone_uname = data[len("remove_clone_"):]
        remove_clone_bot(clone_uname)
        await query.answer(f"@{clone_uname} removed.", show_alert=True)
        await show_clone_management(query, context)
        return

    # ── user management ───────────────────────────────────────────────────────
    if data == "user_management":
        if user_id != ADMIN_ID:
            return
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat_id)
        await send_user_management(query, context, offset=0)
        return

    if data.startswith("user_page_"):
        if user_id != ADMIN_ID:
            return
        try:
            offset = int(data[10:])
        except Exception:
            offset = 0
        await send_user_management(query, context, offset=offset)
        return

    if data.startswith("manage_user_"):
        if user_id != ADMIN_ID:
            return
        user_states.pop(user_id, None)
        await delete_bot_prompt(context, query.message.chat_id)
        try:
            target = int(data[12:])
            await send_single_user_management(query, context, target)
        except ValueError:
            await query.answer("Invalid ID.", show_alert=True)
        return

    if data.startswith("toggle_ban_"):
        if user_id != ADMIN_ID:
            return
        try:
            parts          = data.split('_')
            target_uid     = int(parts[2].lstrip('f'))
            target_status  = int(parts[3].lstrip('f'))
            if target_uid == ADMIN_ID:
                await query.answer("Cannot ban self!", show_alert=True)
                return
            if target_status == 1:
                ban_user(target_uid)
                action = "banned"
            else:
                unban_user(target_uid)
                action = "unbanned"
            await send_single_user_management(query, context, target_uid)
            await query.answer(f"User {target_uid} {action}.", show_alert=True)
        except Exception as e:
            logger.error(f"ban toggle error: {e}")
            await query.answer("Error.", show_alert=True)
        return

    # ── broadcast ─────────────────────────────────────────────────────────────
    if data == "admin_broadcast_start":
        if user_id != ADMIN_ID:
            return
        user_states[user_id] = PENDING_BROADCAST
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await context.bot.send_message(
            query.message.chat_id,
            "📣 Send the message to broadcast (text, photo, video, etc.):",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    # ── manage force-sub ──────────────────────────────────────────────────────
    if data == "manage_force_sub":
        if user_id != ADMIN_ID:
            return
        await show_force_sub_management(query, context)
        return

    if data == "add_channel_start":
        if user_id != ADMIN_ID:
            return
        user_states[user_id] = ADD_CHANNEL_USERNAME
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await context.bot.send_message(
            query.message.chat_id,
            "📢 Send the <b>@username</b> of the channel to add:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="manage_force_sub")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    if data.startswith("channel_"):
        if user_id != ADMIN_ID:
            return
        await show_channel_details(query, context, data[8:])
        return

    if data == "delete_channel_prompt":
        if user_id != ADMIN_ID:
            return
        channels = get_all_force_sub_channels()
        if not channels:
            await query.answer("No channels!", show_alert=True)
            return
        keyboard = [[InlineKeyboardButton(t, callback_data=f"delete_{u.lstrip('@')}")]
                    for u, t, _ in channels]
        keyboard.append([InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")])
        await query.edit_message_text("🗑️ Select channel to delete:",
                                      parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("delete_"):
        if user_id != ADMIN_ID:
            return
        clean = data[7:]
        uname = '@' + clean
        info  = get_force_sub_channel_info(uname)
        if info:
            keyboard = [
                [InlineKeyboardButton("✅ YES DELETE", callback_data=f"confirm_delete_{clean}")],
                [InlineKeyboardButton("❌ CANCEL",     callback_data=f"channel_{clean}")]
            ]
            await query.edit_message_text(
                f"🗑️ Confirm delete <b>{info[1]}</b> ({info[0]})?",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("confirm_delete_"):
        if user_id != ADMIN_ID:
            return
        clean = data[15:]
        delete_force_sub_channel('@' + clean)
        await query.edit_message_text(
            f"✅ Channel @{clean} removed.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("📢 Manage Channels", callback_data="manage_force_sub")
            ]])
        )
        return

    # ── generate links ────────────────────────────────────────────────────────
    if data == "generate_links":
        if user_id != ADMIN_ID:
            return
        user_states[user_id] = GENERATE_LINK_CHANNEL_USERNAME
        try:
            await query.delete_message()
        except Exception:
            pass
        msg = await context.bot.send_message(
            query.message.chat_id,
            "🔗 Send channel <b>@username</b> or <b>ID</b> to generate a deep link:",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
            ]])
        )
        context.user_data['bot_prompt_message_id'] = msg.message_id
        return

    if data.startswith("genlink_"):
        if user_id != ADMIN_ID:
            return
        clean = data[8:]
        uname = '@' + clean
        context.user_data['generating_link_channel'] = uname
        user_states[user_id] = GENERATE_LINK_CHANNEL_TITLE
        try:
            await query.edit_message_text(
                f"📝 Send a <b>label/title</b> for the link to <code>{uname}</code>:",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 CANCEL", callback_data="admin_back")
                ]])
            )
        except Exception:
            pass
        return

# ─────────────────────────── DEEP LINK HANDLER ───────────────────────────────

async def handle_channel_link_deep(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   link_id: str):
    link_info = get_link_info(link_id)
    if not link_info:
        await update.message.reply_text("❌ This link is invalid or not registered.", parse_mode='HTML')
        return

    channel_identifier, creator_id, created_time, never_expires = link_info
    try:
        if isinstance(channel_identifier, str) and channel_identifier.lstrip('-').isdigit():
            channel_identifier = int(channel_identifier)

        if not never_expires:
            created_dt = datetime.fromisoformat(str(created_time))
            if datetime.now() > created_dt + timedelta(minutes=LINK_EXPIRY_MINUTES):
                await update.message.reply_text("❌ This link has expired.", parse_mode='HTML')
                return

        chat        = await context.bot.get_chat(channel_identifier)
        invite_link = await context.bot.create_chat_invite_link(
            chat.id,
            expire_date=datetime.now().timestamp() + LINK_EXPIRY_MINUTES * 60
        )
        keyboard = [[InlineKeyboardButton("• 𝗝𝗢𝗜𝗡 𝗖𝗛𝗔𝗡𝗡𝗘𝗟 •", url=invite_link.invite_link)]]
        await update.message.reply_text(
            "<b>ʜᴇʀᴇ ɪs ʏᴏᴜʀ ʟɪɴᴋ! ᴄʟɪᴄᴋ ʙᴇʟᴏᴡ ᴛᴏ ᴘʀᴏᴄᴇᴇᴅ</b>\n\n"
            "<blockquote><u>If the link expires, tap the original post link again to get a fresh one.</u></blockquote>",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error generating invite link: {e}")
        await update.message.reply_text("❌ Error creating link. Contact admin.", parse_mode='HTML')

# ─────────────────────────── ADMIN PANEL VIEWS ───────────────────────────────

async def send_admin_menu(chat_id, context, query=None):
    if query:
        try:
            await query.delete_message()
        except Exception:
            pass
    context.user_data.pop('bot_prompt_message_id', None)
    user_states.pop(chat_id, None)

    maint_label  = "🔴 Maintenance: ON" if is_maintenance_mode() else "🟢 Maintenance: OFF"
    clone_label  = "🔀 Clone Redirect: ON" if get_setting("clone_redirect_enabled","false")=="true" else "🔀 Clone Redirect: OFF"

    keyboard = [
        [InlineKeyboardButton("📊 BOT STATS",               callback_data="admin_stats")],
        [InlineKeyboardButton("📢 FORCE-SUB CHANNELS",       callback_data="manage_force_sub")],
        [InlineKeyboardButton("🔗 GENERATE CHANNEL LINK",    callback_data="generate_links")],
        [InlineKeyboardButton("📣 BROADCAST",                callback_data="admin_broadcast_start")],
        [InlineKeyboardButton("👤 USER MANAGEMENT",          callback_data="user_management")],
        [InlineKeyboardButton("🤖 CLONE BOTS",               callback_data="manage_clones")],
        [InlineKeyboardButton("⚙️ SETTINGS",                 callback_data="admin_settings")],
    ]
    text = (
        "👨‍💼 <b>ADMIN PANEL</b>\n\n"
        f"<blockquote>{maint_label}\n{clone_label}</blockquote>"
    )
    await context.bot.send_message(chat_id, text, parse_mode='HTML',
                                   reply_markup=InlineKeyboardMarkup(keyboard))


async def send_admin_stats(query, context):
    try:
        await query.delete_message()
    except Exception:
        pass
    user_count    = get_user_count()
    channel_count = len(get_all_force_sub_channels())
    link_count    = get_links_count()
    maint         = "🔴 ON" if is_maintenance_mode() else "🟢 OFF"
    clones        = get_all_clone_bots(active_only=True)
    stats_text = (
        "📊 <b>BOT STATISTICS</b>\n\n"
        f"👤 Total Users: <b>{user_count}</b>\n"
        f"📢 Force-Sub Channels: <b>{channel_count}</b>\n"
        f"🔗 Total Links: <b>{link_count}</b>\n"
        f"🤖 Active Clones: <b>{len(clones)}</b>\n"
        f"🔧 Maintenance: <b>{maint}</b>\n"
        f"⏱ Link Expiry: <b>{LINK_EXPIRY_MINUTES} min</b>"
    )
    keyboard = [
        [InlineKeyboardButton("🔄 REFRESH", callback_data="admin_stats")],
        [InlineKeyboardButton("🔙 BACK",    callback_data="admin_back")]
    ]
    await context.bot.send_message(query.message.chat_id, stats_text, parse_mode='HTML',
                                   reply_markup=InlineKeyboardMarkup(keyboard))


async def show_admin_settings(query, context):
    maint   = is_maintenance_mode()
    clone_r = get_setting("clone_redirect_enabled", "false").lower() == "true"
    backup  = get_setting("backup_channel_url", "Not set")
    text = (
        "⚙️ <b>BOT SETTINGS</b>\n\n"
        f"🔧 Maintenance Mode: <b>{'🔴 ON' if maint else '🟢 OFF'}</b>\n"
        "<blockquote>When ON, new users (not yet in DB) see a maintenance "
        "message and cannot use the bot.</blockquote>\n\n"
        f"🔀 Clone Redirect: <b>{'🔴 ON' if clone_r else '🟢 OFF'}</b>\n"
        "<blockquote>When ON, deep link clicks show clone bot buttons "
        "instead of a direct invite link.</blockquote>\n\n"
        f"📢 Backup Channel: <code>{backup}</code>"
    )
    keyboard = [
        [InlineKeyboardButton(
            f"{'🔴 Disable' if maint else '🟢 Enable'} Maintenance",
            callback_data="toggle_maintenance")],
        [InlineKeyboardButton("📢 Set Backup Channel URL", callback_data="set_backup_channel")],
        [InlineKeyboardButton(
            f"{'🔴 Disable' if clone_r else '🟢 Enable'} Clone Redirect",
            callback_data="toggle_clone_redirect")],
        [InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")]
    ]
    try:
        await query.edit_message_text(text, parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        await context.bot.send_message(query.message.chat_id, text, parse_mode='HTML',
                                       reply_markup=InlineKeyboardMarkup(keyboard))


async def show_clone_management(query, context):
    clones = get_all_clone_bots()
    text   = "🤖 <b>CLONE BOT MANAGEMENT</b>\n\n"
    if not clones:
        text += "No clone bots registered yet.\n"
    else:
        for _, _, uname, active, added in clones:
            status = "✅" if active else "❌"
            text  += f"{status} <b>@{uname}</b> — added {str(added)[:10]}\n"
    keyboard = [
        [InlineKeyboardButton("➕ ADD CLONE BOT", callback_data="add_clone_start")],
    ]
    for _, _, uname, active, _ in clones:
        if active:
            keyboard.append([InlineKeyboardButton(
                f"🗑 Remove @{uname}", callback_data=f"remove_clone_{uname}")])
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])
    try:
        await query.edit_message_text(text, parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        await context.bot.send_message(query.message.chat_id, text, parse_mode='HTML',
                                       reply_markup=InlineKeyboardMarkup(keyboard))


async def show_force_sub_management(query, context):
    channels  = get_all_force_sub_channels(return_usernames_only=False)
    text      = "📢 <b>FORCE-SUBSCRIPTION CHANNELS</b>\n\n"
    if not channels:
        text += "No channels configured."
    else:
        for uname, title, jbr in channels:
            icon  = "📨" if jbr else "✅"
            text += f"{icon} <b>{title}</b> (<code>{uname}</code>)\n"

    keyboard = [[InlineKeyboardButton("➕ ADD CHANNEL", callback_data="add_channel_start")]]
    if channels:
        btns    = [InlineKeyboardButton(t, callback_data=f"channel_{u.lstrip('@')}")
                   for u, t, _ in channels]
        grouped = [btns[i:i+2] for i in range(0, len(btns), 2)]
        keyboard.extend(grouped)
        keyboard.append([InlineKeyboardButton("🗑️ DELETE CHANNEL", callback_data="delete_channel_prompt")])
    keyboard.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])

    try:
        await query.edit_message_text(text, parse_mode='HTML',
                                      reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        await context.bot.send_message(query.message.chat_id, text, parse_mode='HTML',
                                       reply_markup=InlineKeyboardMarkup(keyboard))


async def show_channel_details(query, context, channel_username_clean: str):
    uname = '@' + channel_username_clean
    info  = get_force_sub_channel_info(uname)
    if not info:
        await query.edit_message_text(
            "❌ Channel not found.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK", callback_data="manage_force_sub")
            ]])
        )
        return
    uname_r, title, jbr = info
    jbr_txt = "📨 Request to Join (private)" if jbr else "✅ Direct Join (public)"
    details = (
        f"📢 <b>CHANNEL DETAILS</b>\n\n"
        f"<b>Title:</b> {title}\n"
        f"<b>Username:</b> <code>{uname_r}</code>\n"
        f"<b>Join Mode:</b> {jbr_txt}"
    )
    keyboard = [
        [InlineKeyboardButton("🔗 GENERATE LINK", callback_data=f"genlink_{channel_username_clean}")],
        [InlineKeyboardButton("🗑️ DELETE",        callback_data=f"delete_{channel_username_clean}")],
        [InlineKeyboardButton("🔙 BACK",           callback_data="manage_force_sub")]
    ]
    await query.edit_message_text(details, parse_mode='HTML',
                                  reply_markup=InlineKeyboardMarkup(keyboard))


async def send_single_user_management(query, context, target_user_id: int):
    info = get_user_info_by_id(target_user_id)
    if not info:
        await query.edit_message_text(
            f"❌ User `{target_user_id}` not found.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 BACK", callback_data="user_management")
            ]])
        )
        return
    uid, username, fname, lname, joined, is_banned = info
    uname_d = f"@{username}" if username else "N/A"
    name    = f"{fname or ''} {lname or ''}".strip() or "N/A"
    status  = "🚫 <b>BANNED</b>" if is_banned else "✅ <b>Active</b>"
    text    = (
        f"👤 <b>USER DETAILS</b>\n\n"
        f"Name: {name}\n"
        f"ID: <code>{uid}</code>\n"
        f"Username: <code>{uname_d}</code>\n"
        f"Joined: {joined}\n"
        f"Status: {status}"
    )
    action_btn = "✅ UNBAN" if is_banned else "🚫 BAN"
    action_val = 1 - int(is_banned)
    keyboard   = [
        [InlineKeyboardButton(action_btn, callback_data=f"toggle_ban_f{uid}_f{action_val}")],
        [InlineKeyboardButton("🔙 BACK",  callback_data="user_management")]
    ]
    await query.edit_message_text(text, parse_mode='HTML',
                                  reply_markup=InlineKeyboardMarkup(keyboard))


async def send_user_management(query, context, offset: int = 0):
    if query.from_user.id != ADMIN_ID:
        await query.answer("Unauthorized", show_alert=True)
        return
    total = get_user_count()
    users = get_all_users(limit=10, offset=offset)
    text  = f"👤 <b>USER MANAGEMENT</b>\n{offset+1}–{min(offset+10,total)} of {total}\n\n"
    kb    = []
    for uid, username, fname, lname, joined, is_banned in users:
        uname_d = f"@{username}" if username else f"ID:{uid}"
        name    = f"{fname or ''} {lname or ''}".strip() or "N/A"
        icon    = '🚫' if is_banned else '✅'
        text   += f"{icon} <b>{name}</b> (<code>{uname_d}</code>)\n"
        kb.append([InlineKeyboardButton(f"👤 {name}", callback_data=f"manage_user_{uid}")])

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️ PREV", callback_data=f"user_page_{offset-10}"))
    if total > offset + 10:
        nav.append(InlineKeyboardButton("NEXT ➡️", callback_data=f"user_page_{offset+10}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton("🔙 BACK TO MENU", callback_data="admin_back")])

    await query.edit_message_text(text, parse_mode='HTML',
                                  reply_markup=InlineKeyboardMarkup(kb))

# ─────────────────────────── BROADCAST ───────────────────────────────────────

async def broadcast_worker_job(context: ContextTypes.DEFAULT_TYPE):
    jd          = context.job.data
    offset      = jd['offset']
    chunk_size  = jd['chunk_size']
    msg_chat_id = jd['message_chat_id']
    msg_id      = jd['message_id']
    is_last     = jd['is_last_chunk']
    admin_cid   = jd['admin_chat_id']

    users      = get_all_users(limit=chunk_size, offset=offset)
    sent = fail = 0
    for u in users:
        try:
            await context.bot.copy_message(chat_id=u[0], from_chat_id=msg_chat_id,
                                           message_id=msg_id)
            sent += 1
        except Exception as e:
            logger.warning(f"Broadcast fail {u[0]}: {e}")
            fail += 1
        await asyncio.sleep(0.05)

    await context.bot.send_message(
        admin_cid,
        f"✅ Chunk {offset // chunk_size + 1}: sent {sent}, failed {fail}.",
        parse_mode='Markdown'
    )
    if is_last:
        await context.bot.send_message(admin_cid, "🎉 **BROADCAST COMPLETE!**",
                                       parse_mode='Markdown')


async def broadcast_message_to_all_users(update, context, message_to_copy):
    admin_chat_id = update.effective_chat.id
    total         = get_user_count()
    if total < BROADCAST_MIN_USERS:
        await update.message.reply_text(f"🔄 Broadcasting to {total} users…")
        sent = 0
        for u in get_all_users(limit=None, offset=0):
            try:
                await context.bot.copy_message(
                    chat_id=u[0],
                    from_chat_id=message_to_copy.chat_id,
                    message_id=message_to_copy.message_id
                )
                sent += 1
            except Exception:
                pass
            await asyncio.sleep(0.05)
        await context.bot.send_message(admin_chat_id,
                                       f"✅ Broadcast done. Sent: {sent}/{total}.",
                                       parse_mode='Markdown')
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
        is_last  = (offset + BROADCAST_CHUNK_SIZE) >= total
        context.job_queue.run_once(
            broadcast_worker_job, when=delay,
            data={
                'offset': offset, 'chunk_size': BROADCAST_CHUNK_SIZE,
                'message_chat_id': message_to_copy.chat_id,
                'message_id': message_to_copy.message_id,
                'is_last_chunk': is_last, 'admin_chat_id': admin_chat_id
            },
            name=f"bc_{chunks}"
        )
        offset += BROADCAST_CHUNK_SIZE
        delay  += BROADCAST_INTERVAL_MIN * 60
        chunks += 1
    await update.message.reply_text(
        f"Scheduled **{total_chunks}** chunks over **{delay//60} min**.",
        parse_mode='Markdown'
    )
    try:
        await update.message.delete()
    except Exception:
        pass

# ─────────────────────────── ERROR + CLEANUP ─────────────────────────────────

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Exception: {context.error}")


async def cleanup_task(context: ContextTypes.DEFAULT_TYPE):
    cleanup_expired_links()

# ─────────────────────────── LIFECYCLE ───────────────────────────────────────

async def post_init(application):
    global BOT_USERNAME
    me           = await application.bot.get_me()
    BOT_USERNAME = me.username
    logger.info(f"✅ Bot identified as @{BOT_USERNAME}")
    await health_server.start()
    logger.info("✅ Health check server started")


async def post_shutdown(application):
    await health_server.stop()
    db_manager.close_all()
    logger.info("✅ Shutdown complete")

# ─────────────────────────── MAIN ────────────────────────────────────────────

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

    # ── Post-restart notification ─────────────────────────────────────────────
    if os.path.exists('restart_message.json'):
        try:
            with open('restart_message.json') as f:
                restart_info = json.load(f)
            os.remove('restart_message.json')
            original_chat_id    = restart_info['chat_id']
            admin_id            = restart_info['admin_id']
            message_id_to_copy  = restart_info.get('message_id_to_copy')

            async def post_restart_notification(ctx: ContextTypes.DEFAULT_TYPE):
                try:
                    await ctx.bot.send_message(
                        original_chat_id,
                        "✅ **Bot reloaded!**",
                        parse_mode='Markdown'
                    )
                    if message_id_to_copy == 'admin':
                        await send_admin_menu(original_chat_id, ctx)
                    elif message_id_to_copy:
                        try:
                            await ctx.bot.copy_message(
                                original_chat_id, WELCOME_SOURCE_CHANNEL, message_id_to_copy)
                        except Exception:
                            await send_admin_menu(original_chat_id, ctx)
                    else:
                        await send_admin_menu(original_chat_id, ctx)
                except Exception as e:
                    logger.error(f"Post-restart notify failed: {e}")

            application.job_queue.run_once(post_restart_notification, 1)
        except Exception as e:
            logger.error(f"Restart file error: {e}")

    # ── Register handlers ─────────────────────────────────────────────────────
    admin_filter = filters.User(user_id=ADMIN_ID)

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

    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(admin_filter & ~filters.COMMAND, handle_admin_message))
    application.add_error_handler(error_handler)

    if application.job_queue:
        application.job_queue.run_repeating(cleanup_task, interval=600, first=10)

    application.post_init     = post_init
    application.post_shutdown = post_shutdown

    logger.info("🚀 Starting bot…")
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()

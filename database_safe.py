#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Safe database handler with connection pooling and full schema for the unified bot.
Supports: maintenance mode, clone bots, channel‑titled links, category settings,
auto‑forward, auto manga update, scheduled broadcasts, feature flags, upload progress,
connected groups, and broadcast history.
"""

import psycopg2
from psycopg2 import pool
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
import secrets
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    _instance = None
    _connection_pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance

    def initialize(self, database_url, min_conn=1, max_conn=5):
        try:
            self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                min_conn, max_conn,
                database_url,
                sslmode='require',
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info(f"✅ Database pool initialized ({min_conn}-{max_conn} connections)")
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            raise

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                self._connection_pool.putconn(conn)

    def get_cursor(self):
        """Convenience method to get a cursor from a connection."""
        return self.get_connection().__enter__().cursor()

    def close_all(self):
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Database pool closed")


db_manager = DatabaseManager()


# ─────────────────────────────────── INIT ────────────────────────────────────

def init_db(database_url):
    db_manager.initialize(database_url, min_conn=1, max_conn=5)
    _migrate_new_tables()
    logger.info("✅ Database ready")


def _migrate_new_tables():
    """Idempotent: creates all required tables and adds missing columns."""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()

        # ── Core tables ─────────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_date TIMESTAMP DEFAULT NOW(),
                is_banned BOOLEAN DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS force_sub_channels (
                channel_username TEXT PRIMARY KEY,
                channel_title TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                join_by_request BOOLEAN DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS generated_links (
                link_id TEXT PRIMARY KEY,
                channel_username TEXT NOT NULL,
                user_id BIGINT,
                created_time TIMESTAMP DEFAULT NOW(),
                never_expires BOOLEAN DEFAULT FALSE,
                channel_title TEXT,
                source_bot_username TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clone_bots (
                id SERIAL PRIMARY KEY,
                bot_token TEXT UNIQUE NOT NULL,
                bot_username TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                added_date TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Category settings ───────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS category_settings (
                category TEXT PRIMARY KEY,
                template_name TEXT,
                branding TEXT,
                buttons TEXT,  -- JSON array
                caption_template TEXT,
                thumbnail_url TEXT,
                font_style TEXT DEFAULT 'normal',
                logo_file_id TEXT,
                logo_position TEXT DEFAULT 'bottom',
                watermark_text TEXT,
                watermark_position TEXT DEFAULT 'center'
            )
        """)

        # ── Auto‑forward tables ─────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_connections (
                id SERIAL PRIMARY KEY,
                source_chat_id BIGINT NOT NULL,
                source_chat_username TEXT,
                target_chat_id BIGINT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                delay_seconds INT DEFAULT 0,
                protect_content BOOLEAN DEFAULT FALSE,
                silent BOOLEAN DEFAULT FALSE,
                keep_tag BOOLEAN DEFAULT FALSE,
                pin_message BOOLEAN DEFAULT FALSE,
                delete_source BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_filters (
                id SERIAL PRIMARY KEY,
                connection_id INT REFERENCES auto_forward_connections(id) ON DELETE CASCADE,
                allowed_media TEXT[] DEFAULT '{}',
                blacklist TEXT[] DEFAULT '{}',
                whitelist TEXT[] DEFAULT '{}'
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_replacements (
                id SERIAL PRIMARY KEY,
                connection_id INT REFERENCES auto_forward_connections(id) ON DELETE CASCADE,
                old_pattern TEXT NOT NULL,
                new_pattern TEXT NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_state (
                connection_id INT PRIMARY KEY REFERENCES auto_forward_connections(id) ON DELETE CASCADE,
                last_message_id BIGINT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Auto manga update ───────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS manga_auto_update (
                id SERIAL PRIMARY KEY,
                manga_title TEXT NOT NULL,
                manga_id TEXT,
                last_chapter TEXT,
                target_chat_id BIGINT,
                watermark BOOLEAN DEFAULT FALSE,
                combine_pdf BOOLEAN DEFAULT FALSE,
                active BOOLEAN DEFAULT TRUE,
                last_checked TIMESTAMP DEFAULT NOW(),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Scheduled broadcasts ────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                message_text TEXT,
                media_file_id TEXT,
                media_type TEXT,  -- 'text', 'photo', 'video', etc.
                execute_at TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'pending',  -- 'pending', 'sent', 'failed'
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Broadcast history ───────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_history (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                mode TEXT,
                total_users INT,
                success INT DEFAULT 0,
                blocked INT DEFAULT 0,
                deleted INT DEFAULT 0,
                failed INT DEFAULT 0,
                message_text TEXT,
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP
            )
        """)

        # ── Feature flags ───────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS feature_flags (
                feature_name TEXT,
                entity_id BIGINT,
                entity_type TEXT,  -- 'global', 'user', 'group'
                enabled BOOLEAN DEFAULT TRUE,
                PRIMARY KEY (feature_name, entity_id, entity_type)
            )
        """)

        # ── Upload progress (bot 4) ─────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_progress (
                id INTEGER PRIMARY KEY,
                target_chat_id BIGINT,
                season INTEGER DEFAULT 1,
                episode INTEGER DEFAULT 1,
                total_episode INTEGER DEFAULT 1,
                video_count INTEGER DEFAULT 0,
                selected_qualities TEXT DEFAULT '480p,720p,1080p',
                base_caption TEXT,
                auto_caption_enabled BOOLEAN DEFAULT TRUE
            )
        """)
        # Insert default row if not exists
        cur.execute("INSERT INTO bot_progress (id, base_caption) VALUES (1, '') ON CONFLICT (id) DO NOTHING")

        # ── Connected groups ─────────────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS connected_groups (
                group_id BIGINT PRIMARY KEY,
                group_username TEXT,
                group_title TEXT,
                connected_by BIGINT,
                connected_at TIMESTAMP DEFAULT NOW(),
                active BOOLEAN DEFAULT TRUE
            )
        """)

        # ── Posts cache (optional) ──────────────────────────────────────────
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts_cache (
                id SERIAL PRIMARY KEY,
                category TEXT,
                title TEXT,
                anilist_id INT,
                media_data JSONB,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # ── Add missing columns to existing tables (idempotent) ─────────────
        # generated_links
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE generated_links ADD COLUMN channel_title TEXT;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
        """)
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE generated_links ADD COLUMN source_bot_username TEXT;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
        """)

        # force_sub_channels
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE force_sub_channels ADD COLUMN join_by_request BOOLEAN DEFAULT FALSE;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
        """)

        # users
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE users ADD COLUMN is_banned BOOLEAN DEFAULT FALSE;
            EXCEPTION WHEN duplicate_column THEN NULL; END $$;
        """)

    logger.info("✅ DB migration complete")


# ─────────────────────────────────── SETTINGS ────────────────────────────────

def get_setting(key: str, default=None):
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT value FROM bot_settings WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else default
    except Exception:
        return default


def set_setting(key: str, value: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bot_settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (key, value))


def is_maintenance_mode() -> bool:
    return get_setting("maintenance_mode", "false").lower() == "true"


def toggle_maintenance_mode() -> bool:
    """Toggle maintenance mode. Returns the NEW state (True = ON)."""
    new_state = not is_maintenance_mode()
    set_setting("maintenance_mode", "true" if new_state else "false")
    return new_state


# ─────────────────────────────────── USERS ───────────────────────────────────

def get_user_id_by_username(username: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        clean = username.lstrip('@').lower()
        cur.execute("SELECT user_id FROM users WHERE LOWER(username) = %s", (clean,))
        row = cur.fetchone()
        return row[0] if row else None


def resolve_target_user_id(target_arg: str):
    if target_arg.startswith('@'):
        return get_user_id_by_username(target_arg)
    try:
        return int(target_arg)
    except ValueError:
        return None


def is_existing_user(user_id: int) -> bool:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM users WHERE user_id = %s", (user_id,))
        return cur.fetchone() is not None


def ban_user(user_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_banned = TRUE WHERE user_id = %s", (user_id,))


def unban_user(user_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_banned = FALSE WHERE user_id = %s", (user_id,))


def is_user_banned(user_id: int) -> bool:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT is_banned FROM users WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        return row[0] if row else False


def add_user(user_id, username, first_name, last_name):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        clean_username = username.lstrip('@') if username else None
        cur.execute("""
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
                SET username   = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name  = EXCLUDED.last_name
        """, (user_id, clean_username, first_name, last_name))


def get_user_count() -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]


def get_blocked_users_count() -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users WHERE is_banned = TRUE")
        return cur.fetchone()[0]


def get_all_users(limit=None, offset=0):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if limit is None:
            cur.execute("""
                SELECT user_id, username, first_name, last_name, joined_date, is_banned
                FROM users ORDER BY joined_date DESC
            """)
        else:
            cur.execute("""
                SELECT user_id, username, first_name, last_name, joined_date, is_banned
                FROM users ORDER BY joined_date DESC LIMIT %s OFFSET %s
            """, (limit, offset))
        return cur.fetchall()


def get_user_info_by_id(user_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, username, first_name, last_name, joined_date, is_banned
            FROM users WHERE user_id = %s
        """, (user_id,))
        return cur.fetchone()


# ────────────────────────────── FORCE SUB CHANNELS ───────────────────────────

def add_force_sub_channel(channel_username: str, channel_title: str,
                          join_by_request: bool = False) -> bool:
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE force_sub_channels
                   SET is_active = TRUE, channel_title = %s, join_by_request = %s
                 WHERE channel_username = %s
            """, (channel_title, join_by_request, channel_username))
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO force_sub_channels
                        (channel_username, channel_title, is_active, join_by_request)
                    VALUES (%s, %s, TRUE, %s)
                """, (channel_username, channel_title, join_by_request))
        return True
    except Exception as e:
        logger.error(f"DB Error adding channel: {e}")
        return False


def get_all_force_sub_channels(return_usernames_only=False):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if return_usernames_only:
            cur.execute("""
                SELECT channel_username FROM force_sub_channels
                WHERE is_active = TRUE ORDER BY channel_title
            """)
            return [r[0] for r in cur.fetchall()]
        else:
            cur.execute("""
                SELECT channel_username, channel_title,
                       COALESCE(join_by_request, FALSE) AS join_by_request
                FROM force_sub_channels
                WHERE is_active = TRUE ORDER BY channel_title
            """)
            return cur.fetchall()


def get_force_sub_channel_info(channel_username: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT channel_username, channel_title,
                   COALESCE(join_by_request, FALSE)
            FROM force_sub_channels
            WHERE channel_username = %s AND is_active = TRUE
        """, (channel_username,))
        return cur.fetchone()


def delete_force_sub_channel(channel_username: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE force_sub_channels SET is_active = FALSE
            WHERE channel_username = %s
        """, (channel_username,))


# ────────────────────────────── GENERATED LINKS ──────────────────────────────

def generate_link_id(channel_username: str, user_id: int, never_expires: bool = False,
                     channel_title: str = None, source_bot_username: str = None) -> str:
    link_id = secrets.token_urlsafe(16)
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO generated_links
                (link_id, channel_username, user_id, never_expires,
                 channel_title, source_bot_username)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (link_id) DO UPDATE SET channel_username = EXCLUDED.channel_username
        """, (link_id, channel_username, user_id, never_expires,
              channel_title, source_bot_username))
    return link_id


def get_link_info(link_id: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT channel_username, user_id, created_time, never_expires
            FROM generated_links WHERE link_id = %s
        """, (link_id,))
        return cur.fetchone()


def get_all_links(bot_username: str = None, limit: int = 50, offset: int = 0):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if bot_username:
            cur.execute("""
                SELECT link_id, channel_username, channel_title,
                       source_bot_username, created_time, never_expires
                FROM generated_links
                WHERE source_bot_username = %s
                ORDER BY created_time DESC LIMIT %s OFFSET %s
            """, (bot_username, limit, offset))
        else:
            cur.execute("""
                SELECT link_id, channel_username, channel_title,
                       source_bot_username, created_time, never_expires
                FROM generated_links
                ORDER BY created_time DESC LIMIT %s OFFSET %s
            """, (limit, offset))
        return cur.fetchall()


def get_links_without_title(bot_username: str = None):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if bot_username:
            cur.execute("""
                SELECT link_id, channel_username, source_bot_username
                FROM generated_links
                WHERE (channel_title IS NULL OR channel_title = '')
                  AND source_bot_username = %s
                ORDER BY created_time DESC
            """, (bot_username,))
        else:
            cur.execute("""
                SELECT link_id, channel_username, source_bot_username
                FROM generated_links
                WHERE channel_title IS NULL OR channel_title = ''
                ORDER BY created_time DESC
            """)
        return cur.fetchall()


def update_link_title(link_id: str, channel_title: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE generated_links SET channel_title = %s WHERE link_id = %s
        """, (channel_title, link_id))


def move_links_to_bot(from_bot_username: str, to_bot_username: str) -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE generated_links
               SET source_bot_username = %s
             WHERE source_bot_username = %s
        """, (to_bot_username, from_bot_username))
        return cur.rowcount


def get_links_count(bot_username: str = None) -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if bot_username:
            cur.execute("""
                SELECT COUNT(*) FROM generated_links WHERE source_bot_username = %s
            """, (bot_username,))
        else:
            cur.execute("SELECT COUNT(*) FROM generated_links")
        return cur.fetchone()[0]


def cleanup_expired_links():
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cutoff = datetime.now() - timedelta(days=7)
        cur.execute("""
            DELETE FROM generated_links
            WHERE created_time < %s AND never_expires = FALSE
        """, (cutoff,))
        deleted = cur.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} expired links")


# ─────────────────────────────── CLONE BOTS ──────────────────────────────────

def add_clone_bot(bot_token: str, bot_username: str) -> bool:
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO clone_bots (bot_token, bot_username, is_active)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (bot_token) DO UPDATE
                    SET bot_username = EXCLUDED.bot_username, is_active = TRUE
            """, (bot_token, bot_username))
        return True
    except Exception as e:
        logger.error(f"Error adding clone bot: {e}")
        return False


def get_all_clone_bots(active_only: bool = False):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if active_only:
            cur.execute("""
                SELECT id, bot_token, bot_username, is_active, added_date
                FROM clone_bots WHERE is_active = TRUE ORDER BY added_date
            """)
        else:
            cur.execute("""
                SELECT id, bot_token, bot_username, is_active, added_date
                FROM clone_bots ORDER BY added_date
            """)
        return cur.fetchall()


def remove_clone_bot(bot_username: str) -> bool:
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                UPDATE clone_bots SET is_active = FALSE
                WHERE LOWER(bot_username) = LOWER(%s)
            """, (bot_username.lstrip('@'),))
        return True
    except Exception as e:
        logger.error(f"Error removing clone: {e}")
        return False


def get_main_bot_token() -> str:
    """Returns the stored main bot token so clones can use it for invite links."""
    return get_setting("main_bot_token", "")


def set_main_bot_token(token: str):
    set_setting("main_bot_token", token)


def am_i_a_clone_token(bot_token: str) -> bool:
    """Returns True if this bot_token is registered as an active clone."""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM clone_bots WHERE bot_token = %s AND is_active = TRUE",
                (bot_token,)
            )
            return cur.fetchone() is not None
    except Exception:
        return False


def get_clone_bot_by_username(bot_username: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, bot_token, bot_username, is_active
            FROM clone_bots
            WHERE LOWER(bot_username) = LOWER(%s)
        """, (bot_username.lstrip('@'),))
        return cur.fetchone()


# ─────────────────────────────── CATEGORY SETTINGS ───────────────────────────

def get_category_settings(category: str) -> dict:
    """Retrieve settings for a given category. (Duplicate of bot.py version, kept for completeness)"""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
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
            # Insert default
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


def update_category_template(category: str, template: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET template_name = %s WHERE category = %s", (template, category))


def update_category_branding(category: str, branding: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET branding = %s WHERE category = %s", (branding, category))


def update_category_buttons(category: str, buttons_json: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET buttons = %s WHERE category = %s", (buttons_json, category))


def update_category_caption(category: str, caption: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET caption_template = %s WHERE category = %s", (caption, category))


def update_category_thumbnail(category: str, thumbnail_url: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET thumbnail_url = %s WHERE category = %s", (thumbnail_url, category))


def update_category_font(category: str, font_style: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET font_style = %s WHERE category = %s", (font_style, category))


def update_category_logo(category: str, logo_file_id: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET logo_file_id = %s WHERE category = %s", (logo_file_id, category))


def update_category_logo_position(category: str, position: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE category_settings SET logo_position = %s WHERE category = %s", (position, category))


# ─────────────────────────────── AUTO‑FORWARD ────────────────────────────────

def add_auto_forward_connection(source_chat_id, target_chat_id, **kwargs):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_forward_connections
                (source_chat_id, target_chat_id, delay_seconds, protect_content, silent, keep_tag, pin_message, delete_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (source_chat_id, target_chat_id,
              kwargs.get('delay', 0), kwargs.get('protect', False),
              kwargs.get('silent', False), kwargs.get('keep_tag', False),
              kwargs.get('pin', False), kwargs.get('delete_src', False)))
        return cur.fetchone()[0]


def get_auto_forward_connections(active_only=True):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if active_only:
            cur.execute("SELECT * FROM auto_forward_connections WHERE active = TRUE ORDER BY created_at DESC")
        else:
            cur.execute("SELECT * FROM auto_forward_connections ORDER BY created_at DESC")
        return cur.fetchall()


def delete_auto_forward_connection(conn_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_forward_connections WHERE id = %s", (conn_id,))


def toggle_auto_forward_connection(conn_id, active):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE auto_forward_connections SET active = %s WHERE id = %s", (active, conn_id))


def add_auto_forward_filter(conn_id, allowed_media=None, blacklist=None, whitelist=None):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_forward_filters (connection_id, allowed_media, blacklist, whitelist)
            VALUES (%s, %s, %s, %s)
        """, (conn_id, allowed_media or [], blacklist or [], whitelist or []))


def update_auto_forward_filter(conn_id, allowed_media=None, blacklist=None, whitelist=None):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE auto_forward_filters SET allowed_media = %s, blacklist = %s, whitelist = %s
            WHERE connection_id = %s
        """, (allowed_media or [], blacklist or [], whitelist or [], conn_id))


def add_auto_forward_replacement(conn_id, old, new):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_forward_replacements (connection_id, old_pattern, new_pattern)
            VALUES (%s, %s, %s)
        """, (conn_id, old, new))


def get_auto_forward_replacements(conn_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT old_pattern, new_pattern FROM auto_forward_replacements WHERE connection_id = %s", (conn_id,))
        return cur.fetchall()


def delete_auto_forward_replacement(conn_id, old):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_forward_replacements WHERE connection_id = %s AND old_pattern = %s", (conn_id, old))


def set_auto_forward_last_message(conn_id, msg_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_forward_state (connection_id, last_message_id) VALUES (%s, %s)
            ON CONFLICT (connection_id) DO UPDATE SET last_message_id = EXCLUDED.last_message_id, updated_at = NOW()
        """, (conn_id, msg_id))


def get_auto_forward_last_message(conn_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT last_message_id FROM auto_forward_state WHERE connection_id = %s", (conn_id,))
        row = cur.fetchone()
        return row[0] if row else 0


# ─────────────────────────────── AUTO MANGA UPDATE ───────────────────────────

def add_manga_auto(title, target_chat_id, watermark=False, combine_pdf=False):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO manga_auto_update (manga_title, target_chat_id, watermark, combine_pdf, active)
            VALUES (%s, %s, %s, %s, TRUE) RETURNING id
        """, (title, target_chat_id, watermark, combine_pdf))
        return cur.fetchone()[0]


def get_manga_auto_list():
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, manga_title, last_chapter, target_chat_id, active
            FROM manga_auto_update ORDER BY id
        """)
        return cur.fetchall()


def delete_manga_auto(manga_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM manga_auto_update WHERE id = %s", (manga_id,))


def toggle_manga_auto(manga_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE manga_auto_update SET active = NOT active WHERE id = %s", (manga_id,))


# ─────────────────────────────── SCHEDULED BROADCASTS ────────────────────────

def add_scheduled_broadcast(admin_id, message_text, execute_at, media_file_id=None, media_type=None):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scheduled_broadcasts (admin_id, message_text, media_file_id, media_type, execute_at)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
        """, (admin_id, message_text, media_file_id, media_type, execute_at))
        return cur.fetchone()[0]


def get_pending_scheduled_broadcasts():
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, admin_id, message_text, media_file_id, media_type
            FROM scheduled_broadcasts
            WHERE status = 'pending' AND execute_at <= NOW()
        """)
        return cur.fetchall()


def mark_scheduled_broadcast_sent(b_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE scheduled_broadcasts SET status = 'sent' WHERE id = %s", (b_id,))


def mark_scheduled_broadcast_failed(b_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE scheduled_broadcasts SET status = 'failed' WHERE id = %s", (b_id,))


# ─────────────────────────────── FEATURE FLAGS ───────────────────────────────

def set_feature_flag(feature: str, entity_id: int, entity_type: str, enabled: bool):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO feature_flags (feature_name, entity_id, entity_type, enabled)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (feature_name, entity_id, entity_type) DO UPDATE SET enabled = EXCLUDED.enabled
        """, (feature, entity_id, entity_type, enabled))


def get_feature_flag(feature: str, entity_id: int, entity_type: str) -> bool:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT enabled FROM feature_flags
            WHERE feature_name = %s AND entity_id = %s AND entity_type = %s
        """, (feature, entity_id, entity_type))
        row = cur.fetchone()
        if row:
            return row[0]
        # Default: enabled for global
        if entity_type == 'global':
            return True
        # Fallback to global
        return get_feature_flag(feature, 0, 'global')


# ─────────────────────────────── UPLOAD PROGRESS ─────────────────────────────

def load_upload_progress():
    """Return a dict with the current upload progress."""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT target_chat_id, season, episode, total_episode, video_count,
                   selected_qualities, base_caption, auto_caption_enabled
            FROM bot_progress WHERE id = 1
        """)
        row = cur.fetchone()
        if row:
            return {
                'target_chat_id': row[0],
                'season': row[1],
                'episode': row[2],
                'total_episode': row[3],
                'video_count': row[4],
                'selected_qualities': row[5].split(',') if row[5] else [],
                'base_caption': row[6] or '',
                'auto_caption_enabled': row[7]
            }
        else:
            # Insert default
            cur.execute("""
                INSERT INTO bot_progress (id, base_caption, auto_caption_enabled)
                VALUES (1, '', TRUE)
            """)
            return {
                'target_chat_id': None,
                'season': 1,
                'episode': 1,
                'total_episode': 1,
                'video_count': 0,
                'selected_qualities': ['480p', '720p', '1080p'],
                'base_caption': '',
                'auto_caption_enabled': True
            }


def save_upload_progress(progress):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
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


# ─────────────────────────────── CONNECTED GROUPS ────────────────────────────

def add_connected_group(group_id, group_username, group_title, connected_by):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO connected_groups (group_id, group_username, group_title, connected_by)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (group_id) DO UPDATE SET active = TRUE
        """, (group_id, group_username, group_title, connected_by))


def remove_connected_group(group_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE connected_groups SET active = FALSE WHERE group_id = %s", (group_id,))


def get_connected_groups(active_only=True):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if active_only:
            cur.execute("SELECT group_id, group_username, group_title, connected_at FROM connected_groups WHERE active = TRUE")
        else:
            cur.execute("SELECT group_id, group_username, group_title, connected_at FROM connected_groups")
        return cur.fetchall()


# ─────────────────────────────── BROADCAST HISTORY ───────────────────────────

def add_broadcast_history(admin_id, mode, total_users, message_text):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO broadcast_history (admin_id, mode, total_users, message_text)
            VALUES (%s, %s, %s, %s) RETURNING id
        """, (admin_id, mode, total_users, message_text))
        return cur.fetchone()[0]


def update_broadcast_history(b_id, success, blocked, deleted, failed):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE broadcast_history
            SET completed_at = NOW(),
                success = %s,
                blocked = %s,
                deleted = %s,
                failed = %s
            WHERE id = %s
        """, (success, blocked, deleted, failed, b_id))


# ─────────────────────────────── POSTS CACHE ─────────────────────────────────

def cache_post(category, title, anilist_id, media_data):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO posts_cache (category, title, anilist_id, media_data)
            VALUES (%s, %s, %s, %s)
        """, (category, title, anilist_id, json.dumps(media_data)))


def get_cached_post(anilist_id):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT category, title, media_data FROM posts_cache WHERE anilist_id = %s ORDER BY created_at DESC LIMIT 1", (anilist_id,))
        row = cur.fetchone()
        if row:
            return {'category': row[0], 'title': row[1], 'media_data': json.loads(row[2])}
        return None

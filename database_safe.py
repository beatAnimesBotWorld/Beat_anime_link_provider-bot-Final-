"""
Safe database handler with connection pooling
Supports: maintenance mode, clone bots, channel-titled links, settings,
          category settings, auto-forward, manga auto-update, feature flags.
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

    def close_all(self):
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Database pool closed")


db_manager = DatabaseManager()


# ─────────────────────────────────── INIT ────────────────────────────────────

def init_db(database_url):
    db_manager.initialize(database_url, min_conn=1, max_conn=5)
    _create_tables()
    logger.info("✅ Database ready")


def _create_tables():
    """Idempotent table creation – all tables in one place."""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()

        # Original tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_banned BOOLEAN DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS force_sub_channels (
                channel_username TEXT PRIMARY KEY,
                channel_title TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                join_by_request BOOLEAN DEFAULT FALSE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS generated_links (
                link_id TEXT PRIMARY KEY,
                channel_username TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                never_expires BOOLEAN DEFAULT FALSE,
                channel_title TEXT,
                source_bot_username TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clone_bots (
                id SERIAL PRIMARY KEY,
                bot_token TEXT UNIQUE NOT NULL,
                bot_username TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # New tables for post generation
        cur.execute("""
            CREATE TABLE IF NOT EXISTS category_settings (
                category TEXT PRIMARY KEY,
                template_name TEXT DEFAULT 'template1',
                branding TEXT,
                buttons JSONB DEFAULT '[]',
                caption_template TEXT,
                thumbnail_url TEXT,
                font_style TEXT DEFAULT 'normal',
                logo_file_id TEXT,
                logo_position TEXT DEFAULT 'bottom',
                watermark_text TEXT,
                watermark_position TEXT DEFAULT 'bottom-right'
            )
        """)

        # Auto‑forward connections
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_filters (
                id SERIAL PRIMARY KEY,
                connection_id INT REFERENCES auto_forward_connections(id) ON DELETE CASCADE,
                allowed_media TEXT[] DEFAULT ARRAY['all'],
                blacklist TEXT[],
                whitelist TEXT[]
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

        # Scheduled broadcasts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                message_text TEXT,
                media_file_id TEXT,
                media_type TEXT,
                execute_at TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS broadcast_history (
                id SERIAL PRIMARY KEY,
                admin_id BIGINT NOT NULL,
                mode VARCHAR(20) NOT NULL,
                total_users INT NOT NULL,
                success INT NOT NULL,
                blocked INT NOT NULL DEFAULT 0,
                deleted INT NOT NULL DEFAULT 0,
                failed INT NOT NULL DEFAULT 0,
                message_text TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_status (
                user_id BIGINT PRIMARY KEY,
                is_blocked BOOLEAN DEFAULT FALSE,
                is_deleted BOOLEAN DEFAULT FALSE,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Manga auto‑update
        cur.execute("""
            CREATE TABLE IF NOT EXISTS manga_auto_update (
                id SERIAL PRIMARY KEY,
                manga_title TEXT NOT NULL,
                manga_id TEXT,
                source_api TEXT DEFAULT 'mangadex',
                last_chapter TEXT,
                last_checked TIMESTAMP,
                target_chat_id BIGINT NOT NULL,
                active BOOLEAN DEFAULT TRUE,
                watermark BOOLEAN DEFAULT FALSE,
                combine_pdf BOOLEAN DEFAULT FALSE
            )
        """)

        # Feature flags (for per‑user/group permissions)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS feature_flags (
                id SERIAL PRIMARY KEY,
                feature_name TEXT NOT NULL,
                entity_id BIGINT NOT NULL,          -- user_id or group_id
                entity_type TEXT NOT NULL,           -- 'user' or 'group'
                enabled BOOLEAN DEFAULT TRUE,
                UNIQUE(feature_name, entity_id, entity_type)
            )
        """)

        # Posts cache (optional)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS posts_cache (
                id SERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                anilist_id INT,
                media_data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                message_id BIGINT,
                chat_id BIGINT
            )
        """)

        # Auto‑forward state (last forwarded message id)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS auto_forward_state (
                connection_id INT REFERENCES auto_forward_connections(id) ON DELETE CASCADE,
                last_message_id BIGINT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (connection_id)
            )
        """)

        logger.info("✅ All tables created/verified")


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


def delete_user_by_id(user_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
        cur.execute("DELETE FROM user_status WHERE user_id = %s", (user_id,))


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
            cur.execute("SELECT COUNT(*) FROM generated_links WHERE source_bot_username = %s", (bot_username,))
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
    return get_setting("main_bot_token", "")


def set_main_bot_token(token: str):
    set_setting("main_bot_token", token)


def am_i_a_clone_token(bot_token: str) -> bool:
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
                'template_name': row[0],
                'branding': row[1],
                'buttons': json.loads(row[2]) if row[2] else [],
                'caption_template': row[3],
                'thumbnail_url': row[4],
                'font_style': row[5] or 'normal',
                'logo_file_id': row[6],
                'logo_position': row[7] or 'bottom',
                'watermark_text': row[8],
                'watermark_position': row[9] or 'bottom-right'
            }
        else:
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
                'watermark_position': 'bottom-right'
            }


def save_category_settings(category: str, settings: dict):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO category_settings
                (category, template_name, branding, buttons, caption_template,
                 thumbnail_url, font_style, logo_file_id, logo_position,
                 watermark_text, watermark_position)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (category) DO UPDATE SET
                template_name = EXCLUDED.template_name,
                branding = EXCLUDED.branding,
                buttons = EXCLUDED.buttons,
                caption_template = EXCLUDED.caption_template,
                thumbnail_url = EXCLUDED.thumbnail_url,
                font_style = EXCLUDED.font_style,
                logo_file_id = EXCLUDED.logo_file_id,
                logo_position = EXCLUDED.logo_position,
                watermark_text = EXCLUDED.watermark_text,
                watermark_position = EXCLUDED.watermark_position
        """, (
            category,
            settings.get('template_name', 'template1'),
            settings.get('branding', ''),
            json.dumps(settings.get('buttons', [])),
            settings.get('caption_template', ''),
            settings.get('thumbnail_url', ''),
            settings.get('font_style', 'normal'),
            settings.get('logo_file_id'),
            settings.get('logo_position', 'bottom'),
            settings.get('watermark_text'),
            settings.get('watermark_position', 'bottom-right')
        ))


# ─────────────────────────────── BROADCAST HISTORY ───────────────────────────

def create_broadcast_record(admin_id: int, mode: str, total_users: int) -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO broadcast_history (admin_id, mode, total_users)
            VALUES (%s, %s, %s) RETURNING id
        """, (admin_id, mode, total_users))
        return cur.fetchone()[0]


def update_broadcast_stats(broadcast_id: int, success: int, failed: int, blocked: int, deleted: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE broadcast_history
            SET success = %s, failed = %s, blocked = %s, deleted = %s, completed_at = NOW()
            WHERE id = %s
        """, (success, failed, blocked, deleted, broadcast_id))


def get_broadcast_history(limit: int = 10):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, admin_id, mode, total_users, success, blocked, deleted, failed, started_at, completed_at
            FROM broadcast_history
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                'id': r[0], 'admin_id': r[1], 'mode': r[2], 'total_users': r[3],
                'success': r[4], 'blocked': r[5], 'deleted': r[6], 'failed': r[7],
                'started_at': r[8], 'completed_at': r[9]
            } for r in rows
        ]


# ─────────────────────────────── USER STATUS ─────────────────────────────────

def mark_user_blocked(user_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_status (user_id, is_blocked, last_checked)
            VALUES (%s, TRUE, NOW())
            ON CONFLICT (user_id) DO UPDATE SET is_blocked = TRUE, last_checked = NOW()
        """, (user_id,))


def mark_user_deleted(user_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO user_status (user_id, is_deleted, last_checked)
            VALUES (%s, TRUE, NOW())
            ON CONFLICT (user_id) DO UPDATE SET is_deleted = TRUE, last_checked = NOW()
        """, (user_id,))


def get_blocked_users_count() -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM user_status WHERE is_blocked = TRUE")
        return cur.fetchone()[0]


# ─────────────────────────────── FEATURE FLAGS ───────────────────────────────

def feature_enabled(feature: str, entity_id: int, entity_type: str = 'user') -> bool:
    """Check if a feature is enabled for a given user/group."""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT enabled FROM feature_flags
            WHERE feature_name = %s AND entity_id = %s AND entity_type = %s
        """, (feature, entity_id, entity_type))
        row = cur.fetchone()
        if row:
            return row[0]
        # If not set, default to True (or you could return False)
        return True


def set_feature(feature: str, entity_id: int, entity_type: str, enabled: bool):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO feature_flags (feature_name, entity_id, entity_type, enabled)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (feature_name, entity_id, entity_type)
            DO UPDATE SET enabled = EXCLUDED.enabled
        """, (feature, entity_id, entity_type, enabled))


# ─────────────────────────────── AUTO-FORWARD ────────────────────────────────

def add_auto_forward_connection(source_chat_id: int, target_chat_id: int,
                                source_username: str = None, delay: int = 0,
                                protect: bool = False, silent: bool = False,
                                keep_tag: bool = False, pin: bool = False,
                                delete_src: bool = False) -> int:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO auto_forward_connections
                (source_chat_id, source_chat_username, target_chat_id,
                 delay_seconds, protect_content, silent, keep_tag,
                 pin_message, delete_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (source_chat_id, source_username, target_chat_id,
              delay, protect, silent, keep_tag, pin, delete_src))
        return cur.fetchone()[0]


def get_auto_forward_connections(active_only: bool = True):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        if active_only:
            cur.execute("SELECT * FROM auto_forward_connections WHERE active = TRUE")
        else:
            cur.execute("SELECT * FROM auto_forward_connections")
        return cur.fetchall()


def delete_auto_forward_connection(conn_id: int):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM auto_forward_connections WHERE id = %s", (conn_id,))

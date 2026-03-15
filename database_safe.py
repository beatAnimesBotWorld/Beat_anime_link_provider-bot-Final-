"""
Safe database handler with connection pooling
Supports: maintenance mode, clone bots, channel-titled links, settings
"""
import psycopg2
from psycopg2 import pool
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
import secrets

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
    _migrate_new_tables()
    logger.info("✅ Database ready")


def _migrate_new_tables():
    """Idempotent: creates new tables and safely adds missing columns."""
    with db_manager.get_connection() as conn:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS clone_bots (
                id           SERIAL PRIMARY KEY,
                bot_token    TEXT UNIQUE NOT NULL,
                bot_username TEXT,
                is_active    BOOLEAN   DEFAULT TRUE,
                added_date   TIMESTAMP DEFAULT NOW()
            )
        """)

        # Add new columns to generated_links
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

        # Add join_by_request flag to force_sub_channels
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE force_sub_channels ADD COLUMN join_by_request BOOLEAN DEFAULT FALSE;
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

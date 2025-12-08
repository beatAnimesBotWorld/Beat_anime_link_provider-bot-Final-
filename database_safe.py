"""
Safe database handler with connection pooling
NO CHANGES TO DATABASE SCHEMA - Only optimizes connections
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
    
    def initialize(self, database_url, min_conn=1, max_conn=3):
        """Initialize connection pool - NO DATABASE CHANGES"""
        try:
            self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                min_conn,
                max_conn,
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
        """Context manager for database connections"""
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
        """Close all connections in pool"""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("Database pool closed")

# Global instance
db_manager = DatabaseManager()

# Initialize database connection pool
def init_db(database_url):
    db_manager.initialize(database_url, min_conn=1, max_conn=3)
    logger.info("✅ Database connection pool ready")

# All your database functions using connection pool
def get_user_id_by_username(username):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        clean_username = username.lstrip('@').lower()
        cursor.execute('SELECT user_id FROM users WHERE LOWER(username) = %s', (clean_username,))
        result = cursor.fetchone()
        return result[0] if result else None

def ban_user(user_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = TRUE WHERE user_id = %s', (user_id,))

def unban_user(user_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE users SET is_banned = FALSE WHERE user_id = %s', (user_id,))

def is_user_banned(user_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT is_banned FROM users WHERE user_id = %s', (user_id,))
        result = cursor.fetchone()
        return result[0] if result else False

def add_user(user_id, username, first_name, last_name):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        clean_username = username.lstrip('@') if username else None
        cursor.execute('''
            INSERT INTO users (user_id, username, first_name, last_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) 
            DO UPDATE SET username = EXCLUDED.username, 
                          first_name = EXCLUDED.first_name, 
                          last_name = EXCLUDED.last_name
        ''', (user_id, clean_username, first_name, last_name))

def get_user_count():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM users')
        return cursor.fetchone()[0]

def get_all_users(limit=None, offset=0):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        if limit is None:
            cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC')
        else:
            cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users ORDER BY joined_date DESC LIMIT %s OFFSET %s', (limit, offset))
        return cursor.fetchall()

def get_user_info_by_id(user_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT user_id, username, first_name, last_name, joined_date, is_banned FROM users WHERE user_id = %s', (user_id,))
        return cursor.fetchone()

def add_force_sub_channel(channel_username, channel_title):
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE force_sub_channels SET is_active = TRUE, channel_title = %s WHERE channel_username = %s', (channel_title, channel_username))
            if cursor.rowcount == 0:
                cursor.execute('''
                    INSERT INTO force_sub_channels (channel_username, channel_title, is_active)
                    VALUES (%s, %s, TRUE)
                ''', (channel_username, channel_title))
        return True
    except Exception as e:
        logger.error(f"DB Error adding channel: {e}")
        return False

def get_all_force_sub_channels(return_usernames_only=False):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        if return_usernames_only:
            cursor.execute('SELECT channel_username FROM force_sub_channels WHERE is_active = TRUE ORDER BY channel_title')
            return [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE is_active = TRUE ORDER BY channel_title')
            return cursor.fetchall()

def get_force_sub_channel_info(channel_username):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT channel_username, channel_title FROM force_sub_channels WHERE channel_username = %s AND is_active = TRUE', (channel_username,))
        return cursor.fetchone()

def delete_force_sub_channel(channel_username):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('UPDATE force_sub_channels SET is_active = FALSE WHERE channel_username = %s', (channel_username,))

def generate_link_id(channel_username, user_id, never_expires=False):
    link_id = secrets.token_urlsafe(16)
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO generated_links (link_id, channel_username, user_id, never_expires)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (link_id) DO UPDATE SET channel_username = EXCLUDED.channel_username
        ''', (link_id, channel_username, user_id, never_expires))
    return link_id

def get_link_info(link_id):
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT channel_username, user_id, created_time, never_expires
            FROM generated_links WHERE link_id = %s
        ''', (link_id,))
        return cursor.fetchone()

def cleanup_expired_links():
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cutoff = datetime.now() - timedelta(days=7)
        cursor.execute('DELETE FROM generated_links WHERE created_time < %s AND never_expires = FALSE', (cutoff,))
        deleted = cursor.rowcount
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} expired links")

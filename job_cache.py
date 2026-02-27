import sqlite3
import hashlib
import time
import zlib

DB_FILE = "job_cache.db"
TTL_DAYS = 15   # auto expire jobs after 7 days


def _hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


def init_cache():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            url_hash TEXT PRIMARY KEY,
            job_url TEXT,
            content BLOB,
            created_at INTEGER
        )
    """)

    conn.commit()
    conn.close()


def save_job(url, content):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    compressed = zlib.compress(content.encode())

    cursor.execute("""
        INSERT OR REPLACE INTO jobs (url_hash, job_url, content, created_at)
        VALUES (?, ?, ?, ?)
    """, (_hash_url(url), url, compressed, int(time.time())))

    conn.commit()
    conn.close()


def get_job(url):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT content, created_at FROM jobs WHERE url_hash=?
    """, (_hash_url(url),))

    row = cursor.fetchone()

    conn.close()

    if not row:
        return None

    content, created_at = row

    # TTL check
    if time.time() - created_at > TTL_DAYS * 86400:
        delete_job(url)
        return None

    try:
        return zlib.decompress(content).decode("utf-8")
    except (zlib.error, UnicodeDecodeError):
        delete_job(url)
        return None


def delete_job(url):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM jobs WHERE url_hash=?", (_hash_url(url),))

    conn.commit()
    conn.close()
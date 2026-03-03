# db/cache.py — AI cache and job description cache helpers

import time
import hashlib
from datetime import datetime, timedelta

from db.connection import get_conn


# ─────────────────────────────────────────
# AI CACHE
# ─────────────────────────────────────────

def get_ai_cache(cache_key):
    """Return cached AI content if exists and not expired. Returns dict or None."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM ai_cache
        WHERE cache_key = ?
        AND expires_at > CURRENT_TIMESTAMP
    """, (cache_key,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "subject_initial":   row["subject_initial"],
        "subject_followup1": row["subject_followup1"],
        "subject_followup2": row["subject_followup2"],
        "intro":     row["intro"],
        "followup1": row["followup1"],
        "followup2": row["followup2"],
    }


def save_ai_cache(cache_key, company, job_title, data, ttl_days=21):
    """Save AI generated content to cache with expiry."""
    conn = get_conn()
    c = conn.cursor()
    expires_at = (datetime.now() + timedelta(days=ttl_days)).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO ai_cache (
            cache_key, company, job_title,
            subject_initial, subject_followup1, subject_followup2,
            intro, followup1, followup2,
            expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            subject_initial   = excluded.subject_initial,
            subject_followup1 = excluded.subject_followup1,
            subject_followup2 = excluded.subject_followup2,
            intro             = excluded.intro,
            followup1         = excluded.followup1,
            followup2         = excluded.followup2,
            expires_at        = excluded.expires_at,
            created_at        = CURRENT_TIMESTAMP
    """, (
        cache_key, company, job_title,
        data.get("subject_initial"),
        data.get("subject_followup1"),
        data.get("subject_followup2"),
        data.get("intro"),
        data.get("followup1"),
        data.get("followup2"),
        expires_at,
    ))
    conn.commit()
    conn.close()


def get_applications_missing_ai_cache():
    """Return active applications with no valid ai_cache entry."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT a.id, a.company, a.job_url, a.job_title, a.applied_date
        FROM applications a
        WHERE a.status = 'active'
        AND NOT EXISTS (
            SELECT 1 FROM ai_cache ac
            WHERE ac.company = a.company
            AND COALESCE(ac.job_title, '') = COALESCE(a.job_title, '')
            AND ac.expires_at > CURRENT_TIMESTAMP
        )
        ORDER BY a.applied_date DESC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


# ─────────────────────────────────────────
# JOB CACHE
# ─────────────────────────────────────────

def _hash_url(url):
    return hashlib.sha256(url.encode()).hexdigest()


def save_job(url, content):
    """Compress and save job description. Replaces existing entry."""
    import zlib
    conn = get_conn()
    c = conn.cursor()
    compressed = zlib.compress(content.encode())
    c.execute("""
        INSERT OR REPLACE INTO jobs (url_hash, job_url, content, created_at)
        VALUES (?, ?, ?, ?)
    """, (_hash_url(url), url, compressed, int(time.time())))
    conn.commit()
    conn.close()


def get_job(url):
    """Return decompressed job description or None if missing/expired."""
    import zlib
    from config import RETENTION_JOB_CACHE
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT content, created_at FROM jobs WHERE url_hash = ?
    """, (_hash_url(url),))
    row = c.fetchone()
    conn.close()

    if not row:
        return None

    content, created_at = row["content"], row["created_at"]

    if time.time() - created_at > RETENTION_JOB_CACHE * 86400:
        delete_job(url)
        return None

    try:
        return zlib.decompress(content).decode("utf-8")
    except (zlib.error, UnicodeDecodeError):
        delete_job(url)
        return None


def delete_job(url):
    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM jobs WHERE url_hash = ?", (_hash_url(url),))
    conn.commit()
    conn.close()


def init_job_cache():
    """Alias for init_db — ensures jobs table exists."""
    from db.schema import init_db
    init_db()
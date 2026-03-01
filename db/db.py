"""
db/db.py — Single SQLite database for the recruiter outreach pipeline.

Tables:
    applications          — jobs you applied to
    recruiters            — company-level recruiter contacts
    application_recruiters — many-to-many: links recruiters to applications
    outreach              — emails scheduled/sent per recruiter+application pair
    careershift_quota     — tracks daily CareerShift profile view limit
    ai_cache              — stores AI generated email content per company+job
"""

import sqlite3
import os
import time
import hashlib
from datetime import datetime, timedelta

DAILY_LIMITS = {
    "gemini-2.5-flash-lite": 20,
    "gemini-2.5-flash": 20
}

DB_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "recruiter_pipeline.db")


def get_conn():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def _cleanup_expired_ai_cache(c):
    """Delete expired AI cache rows. Called at startup via init_db."""
    c.execute("DELETE FROM ai_cache WHERE expires_at <= CURRENT_TIMESTAMP")


def _cleanup_expired_jobs(c):
    """Delete expired job cache rows. Called at startup via init_db."""
    c.execute("DELETE FROM jobs WHERE created_at <= ?",
              (int(time.time()) - 21 * 86400,))


def _cleanup_old_model_usage(c):
    """Delete model usage records older than 21 days."""
    cutoff = (datetime.now() - timedelta(days=21)).strftime("%Y-%m-%d")
    c.execute("DELETE FROM model_usage WHERE date < ?", (cutoff,))


def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            company      TEXT NOT NULL,
            job_url      TEXT NOT NULL UNIQUE,
            job_title    TEXT,
            applied_date DATE DEFAULT (DATE('now')),
            status       TEXT DEFAULT 'active',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS recruiters (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            company          TEXT NOT NULL,
            name             TEXT,
            position         TEXT,
            email            TEXT UNIQUE,
            confidence       TEXT,
            recruiter_status TEXT DEFAULT 'active',
            verified_at      TIMESTAMP,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS application_recruiters (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            application_id INTEGER NOT NULL REFERENCES applications(id),
            recruiter_id   INTEGER NOT NULL REFERENCES recruiters(id),
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(application_id, recruiter_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS outreach (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            recruiter_id   INTEGER NOT NULL REFERENCES recruiters(id),
            application_id INTEGER NOT NULL REFERENCES applications(id),
            stage          TEXT DEFAULT 'initial',
            status         TEXT DEFAULT 'pending',
            replied        INTEGER DEFAULT 0,
            scheduled_for  DATE,
            sent_at        TIMESTAMP,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS careershift_quota (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        DATE NOT NULL UNIQUE DEFAULT (DATE('now')),
            total_limit INTEGER DEFAULT 50,
            used        INTEGER DEFAULT 0,
            remaining   INTEGER DEFAULT 50
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_cache (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key         TEXT NOT NULL UNIQUE,
            company           TEXT NOT NULL,
            job_title         TEXT NOT NULL,
            subject_initial   TEXT,
            subject_followup1 TEXT,
            subject_followup2 TEXT,
            intro             TEXT,
            followup1         TEXT,
            followup2         TEXT,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at        TIMESTAMP NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            url_hash   TEXT PRIMARY KEY,
            job_url    TEXT,
            content    BLOB,
            created_at INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS model_usage (
            model TEXT,
            date  TEXT,
            count INTEGER,
            PRIMARY KEY (model, date)
        )
    """)

    _cleanup_expired_ai_cache(c)
    _cleanup_expired_jobs(c)
    _cleanup_old_model_usage(c)
    conn.commit()
    conn.close()
    print("[OK] Database initialized: data/recruiter_pipeline.db")


# ─────────────────────────────────────────
# APPLICATION HELPERS
# ─────────────────────────────────────────

def add_application(company, job_url, job_title=None, applied_date=None):
    """Insert a new application. Returns new id or existing id if duplicate URL."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO applications (company, job_url, job_title, applied_date)
            VALUES (?, ?, ?, ?)
        """, (company, job_url, job_title,
              applied_date or datetime.now().strftime("%Y-%m-%d")))
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        c.execute("SELECT id FROM applications WHERE job_url = ?", (job_url,))
        row = c.fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def get_all_active_applications():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM applications
        WHERE status = 'active'
        ORDER BY applied_date ASC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_application_by_id(application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM applications WHERE id = ?", (application_id,))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None


# ─────────────────────────────────────────
# RECRUITER HELPERS
# ─────────────────────────────────────────

def get_recruiters_by_company(company):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM recruiters
        WHERE company = ? AND recruiter_status = 'active'
    """, (company,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def add_recruiter(company, name, position, email, confidence):
    """Insert recruiter at company level. Returns new id or existing id."""
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT INTO recruiters (company, name, position, email, confidence, verified_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (company, name, position, email, confidence))
        conn.commit()
        return c.lastrowid
    except sqlite3.IntegrityError:
        c.execute("SELECT id FROM recruiters WHERE email = ?", (email,))
        row = c.fetchone()
        return row["id"] if row else None
    finally:
        conn.close()


def update_recruiter(recruiter_id, name=None, position=None,
                     confidence=None, recruiter_status=None):
    conn = get_conn()
    c = conn.cursor()
    fields = []
    values = []
    if name is not None:
        fields.append("name = ?"); values.append(name)
    if position is not None:
        fields.append("position = ?"); values.append(position)
    if confidence is not None:
        fields.append("confidence = ?"); values.append(confidence)
    if recruiter_status is not None:
        fields.append("recruiter_status = ?"); values.append(recruiter_status)
    fields.append("verified_at = CURRENT_TIMESTAMP")
    values.append(recruiter_id)
    c.execute(f"UPDATE recruiters SET {', '.join(fields)} WHERE id = ?", values)
    conn.commit()
    conn.close()


def recruiter_email_exists(email):
    """Returns recruiter id if exists, None otherwise."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id FROM recruiters WHERE email = ?", (email,))
    row = c.fetchone()
    conn.close()
    return row["id"] if row else None


# ─────────────────────────────────────────
# APPLICATION_RECRUITERS HELPERS
# ─────────────────────────────────────────

def link_recruiter_to_application(application_id, recruiter_id):
    conn = get_conn()
    c = conn.cursor()
    try:
        c.execute("""
            INSERT OR IGNORE INTO application_recruiters (application_id, recruiter_id)
            VALUES (?, ?)
        """, (application_id, recruiter_id))
        conn.commit()
    finally:
        conn.close()


def get_recruiters_for_application(application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT r.* FROM recruiters r
        INNER JOIN application_recruiters ar ON ar.recruiter_id = r.id
        WHERE ar.application_id = ? AND r.recruiter_status = 'active'
    """, (application_id,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_unique_companies_needing_scraping(min_recruiters=2):
    """Return unique company names with fewer than min_recruiters linked."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT a.id, a.company, COUNT(ar.recruiter_id) as recruiter_count
        FROM applications a
        LEFT JOIN application_recruiters ar ON ar.application_id = a.id
        LEFT JOIN recruiters r ON r.id = ar.recruiter_id AND r.recruiter_status = 'active'
        WHERE a.status = 'active'
        GROUP BY a.id, a.company
        HAVING recruiter_count < ?
        ORDER BY a.applied_date ASC
    """, (min_recruiters,))
    rows = c.fetchall()
    conn.close()

    seen = set()
    unique = []
    for row in rows:
        if row["company"] not in seen:
            seen.add(row["company"])
            unique.append(row["company"])
    return unique


# ─────────────────────────────────────────
# OUTREACH HELPERS
# ─────────────────────────────────────────

def schedule_outreach(recruiter_id, application_id, stage, scheduled_for):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO outreach (recruiter_id, application_id, stage, scheduled_for)
        VALUES (?, ?, ?, ?)
    """, (recruiter_id, application_id, stage, scheduled_for))
    conn.commit()
    oid = c.lastrowid
    conn.close()
    return oid


def schedule_next_outreach(recruiter_id, application_id):
    """
    After an email is sent, schedule the next stage if recruiter hasn't replied.
    Stage flow: initial → followup1 → followup2 → done
    Returns the new outreach id or None if sequence is complete.
    """
    conn = get_conn()
    c = conn.cursor()

    # Get the last sent stage — use id DESC to avoid same-second timestamp ties
    c.execute("""
        SELECT stage FROM outreach
        WHERE recruiter_id = ? AND application_id = ?
        AND status = 'sent'
        ORDER BY id DESC LIMIT 1
    """, (recruiter_id, application_id))
    row = c.fetchone()
    conn.close()

    if not row:
        return None

    last_stage = row["stage"]
    next_stage_map = {
        "initial":   "followup1",
        "followup1": "followup2",
        "followup2": None,          # sequence complete
    }
    next_stage = next_stage_map.get(last_stage)

    if not next_stage:
        print(f"   [OK] Outreach sequence complete for recruiter_id={recruiter_id}")
        return None

    from config import SEND_INTERVAL_DAYS
    scheduled_for = (datetime.now() + timedelta(days=SEND_INTERVAL_DAYS)).strftime("%Y-%m-%d")
    oid = schedule_outreach(recruiter_id, application_id, next_stage, scheduled_for)
    print(f"   [INFO] Scheduled {next_stage} for {scheduled_for}")
    return oid


def get_pending_outreach():
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT o.*, r.name, r.email, r.company, a.job_url, a.job_title
        FROM outreach o
        JOIN recruiters r ON r.id = o.recruiter_id
        JOIN applications a ON a.id = o.application_id
        WHERE o.status = 'pending'
        AND o.scheduled_for <= DATE('now')
        AND o.replied = 0
        AND r.recruiter_status = 'active'
        ORDER BY o.scheduled_for ASC
    """)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def mark_outreach_sent(outreach_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        UPDATE outreach
        SET status = 'sent', sent_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (outreach_id,))
    conn.commit()
    conn.close()


def mark_outreach_failed(outreach_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("UPDATE outreach SET status = 'failed' WHERE id = ?", (outreach_id,))
    conn.commit()
    conn.close()


def has_pending_or_sent_outreach(recruiter_id, application_id):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        SELECT id FROM outreach
        WHERE recruiter_id = ? AND application_id = ?
        AND status IN ('pending', 'sent')
    """, (recruiter_id, application_id))
    row = c.fetchone()
    conn.close()
    return row is not None


# ─────────────────────────────────────────
# CAREERSHIFT QUOTA HELPERS
# ─────────────────────────────────────────

def get_today_quota():
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("SELECT * FROM careershift_quota WHERE date = ?", (today,))
    row = c.fetchone()
    if not row:
        c.execute("""
            INSERT INTO careershift_quota (date, total_limit, used, remaining)
            VALUES (?, 50, 0, 50)
        """, (today,))
        conn.commit()
        c.execute("SELECT * FROM careershift_quota WHERE date = ?", (today,))
        row = c.fetchone()
    result = dict(row)
    conn.close()
    return result


def increment_quota_used(count=1):
    conn = get_conn()
    c = conn.cursor()
    today = datetime.now().strftime("%Y-%m-%d")
    c.execute("""
        UPDATE careershift_quota
        SET used = used + ?, remaining = remaining - ?
        WHERE date = ?
    """, (count, count, today))
    conn.commit()
    conn.close()


def get_remaining_quota():
    quota = get_today_quota()
    return max(0, quota["remaining"])


# ─────────────────────────────────────────
# AI CACHE HELPERS
# ─────────────────────────────────────────

def get_ai_cache(cache_key):
    """Return cached AI content if it exists and hasn't expired. Returns dict or None."""
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


# ─────────────────────────────────────────
# JOB CACHE HELPERS
# ─────────────────────────────────────────

def _hash_url(url):
    return hashlib.md5(url.encode()).hexdigest()


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

    if time.time() - created_at > 21 * 86400:
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
    init_db()


# ─────────────────────────────────────────
# QUOTA MANAGER HELPERS
# ─────────────────────────────────────────

def _get_today():
    return datetime.now().strftime("%Y-%m-%d")


def can_call(model):
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    c.execute(
        "SELECT count FROM model_usage WHERE model=? AND date=?",
        (model, today)
    )
    row = c.fetchone()
    current = row["count"] if row else 0
    limit = DAILY_LIMITS.get(model, 0)
    conn.close()
    return current < limit


def increment_usage(model):
    conn = get_conn()
    c = conn.cursor()
    today = _get_today()
    c.execute("""
        INSERT INTO model_usage (model, date, count)
        VALUES (?, ?, 1)
        ON CONFLICT(model, date)
        DO UPDATE SET count = count + 1
    """, (model, today))
    conn.commit()
    conn.close()


def all_models_exhausted():
    for model in DAILY_LIMITS:
        if can_call(model):
            return False
    return True


if __name__ == "__main__":
    init_db()
"""
scripts/add_user.py — Admin script to onboard a new pipeline user.

Usage:
    python scripts/add_user.py --name "Fiancée" --email fiancee@gmail.com
    python scripts/add_user.py --name "Fiancée" --email fiancee@gmail.com \\
                               --resume-path Resume_Fiancee.pdf

Creates the users row, runs deferred backfills if this is user 2
(recruiters.found_by_user_id, careershift_quota.user_id), then prints the
exact .env vars and CareerShift session commands the operator must run before
the new user goes live.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger, init_logging
from db.db import get_conn, init_db

init_logging("add_user")
logger = get_logger(__name__)


def _run_deferred_backfills_for_user2(conn, user_id: int) -> None:
    """
    Run the two backfills that were deferred from init_db() because user 2
    didn't exist yet at migration time:

      recruiters.found_by_user_id → 2
        All recruiter scraping before multi-user used user 2's CareerShift
        account.  Profiles are cached under that account; routing verification
        to user 1's account would burn quota re-visiting cached profiles.

      careershift_quota.user_id → 2
        Existing quota rows tracked user 2's account usage.

    After backfill, NOT NULL is enforced on careershift_quota.user_id.
    """
    c = conn.cursor()

    n = c.execute(
        "UPDATE recruiters SET found_by_user_id = %s WHERE found_by_user_id IS NULL",
        (user_id,)
    ).rowcount
    logger.info("add_user: backfilled recruiters.found_by_user_id=%d — %d row(s)", user_id, n)

    n = c.execute(
        "UPDATE careershift_quota SET user_id = %s WHERE user_id IS NULL",
        (user_id,)
    ).rowcount
    logger.info("add_user: backfilled careershift_quota.user_id=%d — %d row(s)", user_id, n)

    c.execute("ALTER TABLE careershift_quota ALTER COLUMN user_id SET NOT NULL")
    logger.info("add_user: careershift_quota.user_id is now NOT NULL")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Add a new user to the recruiter pipeline."
    )
    parser.add_argument("--name",        required=True,
                        help="Display name shown in alert email subjects (e.g. 'Fiancée')")
    parser.add_argument("--email",       required=True,
                        help="Notification email — quota alerts are delivered here")
    parser.add_argument("--resume-path", default="Resume.pdf",
                        help="Resume filename at repo root (default: Resume.pdf)")
    args = parser.parse_args()

    init_db()
    conn = get_conn()

    try:
        c = conn.cursor()

        existing = c.execute(
            "SELECT id, name FROM users WHERE email = %s", (args.email,)
        ).fetchone()
        if existing:
            print(f"\n[ERROR] Email {args.email!r} already belongs to "
                  f"user id={existing['id']} ({existing['name']!r}).")
            logger.error("add_user: duplicate email %r — already exists as id=%d",
                         args.email, existing["id"])
            return 1

        row = c.execute("""
            INSERT INTO users (email, name, resume_path)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (args.email, args.name, args.resume_path)).fetchone()
        new_id = row["id"]
        logger.info("add_user: created user id=%d name=%r email=%r resume_path=%r",
                    new_id, args.name, args.email, args.resume_path)

        if new_id == 2:
            _run_deferred_backfills_for_user2(conn, new_id)

        conn.commit()

    except Exception as exc:
        conn.rollback()
        logger.exception("add_user: transaction rolled back: %s", exc)
        print(f"\n[ERROR] {exc}")
        return 1
    finally:
        conn.close()

    uid = new_id
    print(f"\nUser created (id={uid}, name={args.name!r}). Add these to .env:\n")
    print(f"  GMAIL_USER_{uid}_EMAIL={args.email}")
    print(f"  GMAIL_USER_{uid}_APP_PASS=<gmail-app-password>")
    print(f"  GEMINI_API_KEY_USER_{uid}=<gemini-api-key>")
    print(f"  CAREERSHIFT_USER_{uid}_EMAIL=<careershift-login-email>")
    print(f"  CAREERSHIFT_USER_{uid}_PASS=<careershift-password>")

    if uid == 2:
        print(f"\nExisting session file migration (run on SERVER):\n")
        print(f"  mv data/careershift_session.json data/careershift_session_{uid}.json")

    print(f"\nCareerShift session setup (run on LOCAL machine, then SCP to server):\n")
    print(f"  python careershift/auth_njit.py --user-id {uid}")
    print(f"  scp data/careershift_session_{uid}.json opc@<server>:/home/opc/mail/data/")

    print(f"\nSession renewal every ~30 days:\n")
    print(f"  python careershift/auth_njit.py --user-id {uid}")
    print(f"  scp data/careershift_session_{uid}.json opc@<server>:/home/opc/mail/data/")

    return 0


if __name__ == "__main__":
    sys.exit(main())

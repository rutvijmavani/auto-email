"""
scripts/pd_redirect_review.py — decide on cross-domain redirects held by the employer-name gate.

  python -m scripts.pd_redirect_review list
  python -m scripts.pd_redirect_review approve <fein> [new_domain]
  python -m scripts.pd_redirect_review reject  <fein> [new_domain]

Pending pairs come from the live resolvers and the backfill (db/pd_redirect_review.py) and are emailed weekly
by `pd_candidates --notify`. approve: the pair is accepted the next time the employer's domain is resolved.
reject: nothing is stored for it and it is never queued again. Without new_domain, every pending pair of the
employer is decided.
"""
import argparse
import sys

from db.connection import get_conn
from db.pd_redirect_review import APPROVED, REJECTED, decide
from logger import get_logger, init_logging

log = get_logger(__name__)


def list_pending(conn) -> int:
    rows = conn.execute(
        "SELECT employer_fein, employer_name, old_domain, new_domain, hint, source, notified_at "
        "FROM pd_redirect_review WHERE status = 'pending' ORDER BY first_seen_at, employer_fein").fetchall()
    for r in rows:
        print(f"{r['employer_fein']}  {(r['employer_name'] or '')[:40]:40}  {r['old_domain']} -> {r['new_domain']}"
              f"  [{r['hint']}, {r['source']}{'' if r['notified_at'] else ', not yet emailed'}]")
    print(f"{len(rows)} pending")
    return 0


def main(argv=None) -> int:
    init_logging("pd_redirect_review")
    parser = argparse.ArgumentParser(description="Review redirects held by the employer-name gate")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="show pending pairs")
    for name in ("approve", "reject"):
        p = sub.add_parser(name)
        p.add_argument("fein")
        p.add_argument("new_domain", nargs="?")
    args = parser.parse_args(argv)

    conn = get_conn()
    try:
        if args.cmd == "list":
            return list_pending(conn)
        status = APPROVED if args.cmd == "approve" else REJECTED
        n = decide(conn, args.fein, status, args.new_domain)
        conn.commit()
        log.info("pd_redirect_review: %s %d pending pair(s) for fein=%s%s", status, n, args.fein,
                 f" new_domain={args.new_domain}" if args.new_domain else "")
        print(f"{status}: {n} pair(s)")
        return 0 if n else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())

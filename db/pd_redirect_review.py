# db/pd_redirect_review.py — review queue for cross-domain redirects that failed the employer-name gate.
#
# Backs pd_redirect_review (db/schema.py); gate logic lives in jobs/pd_name_gate.py. All functions run on the
# caller's connection and never commit (the caller owns the transaction). No retention by design: approved and
# rejected rows are decisions the live resolvers read, pending rows wait for a human.

PENDING, APPROVED, REJECTED = "pending", "approved", "rejected"


def get_status(conn, fein: str, new_domain: str) -> "str | None":
    """Decision on 'this employer may land on new_domain', or None if never queued.

    Keyed on (fein, new_domain), NOT the old domain: the backfill's old domain is the stored public_domain while
    a live resolver's is the assigned email domain, and a human approval of "X may use new.com" covers both.
    Several rows can exist (one per old domain); approved beats rejected beats pending.
    """
    row = conn.execute(
        "SELECT status FROM pd_redirect_review WHERE employer_fein = ? AND new_domain = ? "
        "ORDER BY CASE status WHEN 'approved' THEN 0 WHEN 'rejected' THEN 1 ELSE 2 END LIMIT 1",
        (fein, new_domain)).fetchone()
    return row["status"] if row else None


def fetch_statuses(conn, feins: list, batch: int = 500) -> dict:
    """Bulk get_status: {(fein, new_domain): status} for the given employers (same precedence)."""
    rank = {APPROVED: 0, REJECTED: 1, PENDING: 2}
    out: dict = {}
    for i in range(0, len(feins), batch):
        for r in conn.execute(
                "SELECT employer_fein, new_domain, status FROM pd_redirect_review WHERE employer_fein = ANY(?)",
                (feins[i:i + batch],)).fetchall():
            key = (r["employer_fein"], r["new_domain"])
            if key not in out or rank[r["status"]] < rank[out[key]]:
                out[key] = r["status"]
    return out


def queue_pair(conn, fein: str, old_domain: str, new_domain: str, new_host: "str | None",
               employer_name: "str | None", hint: str, source: str) -> bool:
    """Queue a failed pair as pending. A pair that is already queued only has last_seen_at refreshed (its status,
    notified_at and first_seen_at are never reset, so a decided pair is not re-opened and not re-emailed).
    Returns True if a NEW row was inserted."""
    inserted = conn.execute(
        """
        INSERT INTO pd_redirect_review
            (employer_fein, old_domain, new_domain, new_host, employer_name, hint, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (employer_fein, old_domain, new_domain) DO NOTHING
        """, (fein, old_domain, new_domain, new_host, employer_name, hint, source)).rowcount > 0
    if not inserted:
        conn.execute(
            "UPDATE pd_redirect_review SET last_seen_at = NOW() "
            "WHERE employer_fein = ? AND old_domain = ? AND new_domain = ?", (fein, old_domain, new_domain))
    return inserted


def unnotified(conn) -> list:
    """Pending rows not yet emailed, oldest first."""
    return conn.execute(
        "SELECT employer_fein, employer_name, old_domain, new_domain, hint, source, first_seen_at "
        "FROM pd_redirect_review WHERE status = 'pending' AND notified_at IS NULL "
        "ORDER BY first_seen_at, employer_fein").fetchall()


def pending_count(conn) -> int:
    return conn.execute("SELECT count(*) AS n FROM pd_redirect_review WHERE status = 'pending'").fetchone()["n"]


def mark_notified(conn, keys: list) -> int:
    """Stamp notified_at for [(fein, old, new), ...] after the email was sent."""
    n = 0
    for fein, old, new in keys:
        n += conn.execute(
            "UPDATE pd_redirect_review SET notified_at = NOW() "
            "WHERE employer_fein = ? AND old_domain = ? AND new_domain = ?", (fein, old, new)).rowcount
    return n


def decide(conn, fein: str, status: str, new_domain: "str | None" = None) -> int:
    """Approve/reject the pending pair(s) of one employer (optionally only the one landing on new_domain).
    Returns rows changed."""
    if status not in (APPROVED, REJECTED):
        raise ValueError(f"status must be approved or rejected, got {status!r}")
    sql = ("UPDATE pd_redirect_review SET status = ?, decided_at = NOW() "
           "WHERE employer_fein = ? AND status = 'pending'")
    params = [status, fein]
    if new_domain:
        sql += " AND new_domain = ?"
        params.append(new_domain)
    return conn.execute(sql, tuple(params)).rowcount

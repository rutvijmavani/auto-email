"""
scripts/build_email_patterns.py — Build email_patterns table from lca_contacts

Reads all non-generic contacts from lca_contacts, runs pattern detection per domain,
and upserts results into email_patterns. Safe to re-run — fully idempotent.

Usage:
    python scripts/build_email_patterns.py
    python scripts/build_email_patterns.py --domain infosys.com  # single domain

Algorithm (see docs/email-pattern-inference.md):
  - Normalize name parts: lowercase, strip accents, strip non-alpha
  - Try all 1-3 token combinations with separators (., _, -, none)
  - If no direct match: locate digit sequences, replace with {d}, retry
  - Pattern stored only if probability >= 5% (count / total_unique_personal)
  - Patterns JSONB: [{pattern_id, count, probability, example_local, last_seen, has_digit}]
"""

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import date
from itertools import product

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.connection import get_conn
from logger import get_logger, init_logging

log = get_logger(__name__)

PROBABILITY_FLOOR = 0.05  # patterns below 5% are dropped

SEPARATORS = [".", "_", "-", ""]


# ─────────────────────────────────────────────────────────────────────────────
# Name normalization
# ─────────────────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lowercase, strip accents, keep only a-z0-9."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", ascii_str.lower())


def _build_tokens(first: str, middle: str, last: str) -> dict[str, str]:
    """Return all named token expansions for a given name."""
    fn = _norm(first)
    mn = _norm(middle)
    ln = _norm(last)
    tokens: dict[str, str] = {}
    if fn:
        tokens["fn"]  = fn
        tokens["fi"]  = fn[0] if fn else ""
        for n in range(2, len(fn)):
            tokens[f"fn[:{n}]"] = fn[:n]
    if mn:
        tokens["mn"]  = mn
        tokens["mi"]  = mn[0] if mn else ""
        for n in range(2, len(mn)):
            tokens[f"mn[:{n}]"] = mn[:n]
    if ln:
        tokens["ln"]  = ln
        tokens["li"]  = ln[0] if ln else ""
        for n in range(2, len(ln)):
            tokens[f"ln[:{n}]"] = ln[:n]
    # Remove empty tokens
    return {k: v for k, v in tokens.items() if v}


# ─────────────────────────────────────────────────────────────────────────────
# Pattern detection
# ─────────────────────────────────────────────────────────────────────────────

_DIGIT_RE = re.compile(r"\d+")


def _try_match(local: str, tokens: dict[str, str]) -> str | None:
    """
    Try all 1-3 token combinations with all separators against `local`.
    Returns a pattern_id string like "{fn}.{ln}" on match, None otherwise.
    """
    token_names  = list(tokens.keys())
    token_values = list(tokens.values())

    for n_tokens in range(1, 4):
        for indices in _combinations(len(token_names), n_tokens):
            selected_names  = [token_names[i]  for i in indices]
            selected_values = [token_values[i] for i in indices]
            for sep in SEPARATORS:
                candidate = sep.join(selected_values)
                if candidate == local:
                    pattern_id = sep.join(f"{{{n}}}" for n in selected_names)
                    return pattern_id
    return None


def _combinations(n: int, r: int):
    """Yield all ordered r-length sequences of distinct indices from 0..n-1."""
    from itertools import permutations
    from itertools import combinations as _comb
    for combo in _comb(range(n), r):
        yield from permutations(combo)


def detect_pattern(local: str, first: str, middle: str, last: str) -> tuple[str | None, bool]:
    """
    Return (pattern_id, has_digit).
    pattern_id is None if no template matched.
    has_digit is True whenever digit substitution was required.
    """
    tokens = _build_tokens(first, middle, last)
    if not tokens:
        return None, False

    pid = _try_match(local, tokens)
    if pid is not None:
        return pid, False

    if not _DIGIT_RE.search(local):
        return None, False

    _DIGIT_SENTINEL = "\x00d\x00"
    parts = _DIGIT_RE.sub(_DIGIT_SENTINEL, local).split(_DIGIT_SENTINEL)

    for sep in SEPARATORS:
        pid = _try_match_with_digit_slots(parts, sep, tokens)
        if pid is not None:
            return pid, True

    return None, True


def _try_match_with_digit_slots(parts: list[str], sep: str, tokens: dict[str, str]) -> str | None:
    """
    Match each non-digit segment of `parts` via _try_match, then reassemble.

    parts = local split on digit sequences (e.g. ["jsmith",""] for "jsmith2").
    sep   = candidate separator to strip from edges adjacent to each digit gap.

    Each segment is matched with _try_match, which internally tries all token
    combinations and all separators — so multi-token segments like "jsmith" are
    correctly matched as "{fi}{ln}" with sep="". The sep arg is used only for
    stripping chars that separate the name tokens from the digit in the original
    string (e.g. "john." → "john" when sep=".").

    Separator tracking: left_seps[i] = sep stripped from the trailing edge of
    parts[i] (before the digit gap); right_seps[i] = sep stripped from the
    leading edge of parts[i] (after the prior digit gap). Both are re-inserted
    in the returned pattern_id so it faithfully represents the original layout.

    Examples (tokens = {fi:"j", fn:"john", ln:"smith"}):
      parts=["jsmith",""],       sep="" → "{fi}{ln}{d}"     (jsmith2)
      parts=["j","smith"],       sep="" → "{fi}{d}{ln}"     (j2smith)
      parts=["john.",".smith"],  sep="."→ "{fn}.{d}.{ln}"  (john.2.smith)
      parts=["john","smith"],    sep="" → "{fn}{d}{ln}"     (john2smith)
      parts=["john2","smith3","doe"], sep="" → multi-digit  (handled)
    """
    stripped   = []
    left_seps  = []  # sep stripped from trailing edge of parts[i] (before gap)
    right_seps = []  # sep stripped from leading  edge of parts[i] (after gap)

    for i, part in enumerate(parts):
        s, ls, rs = part, "", ""
        if sep:
            if i > 0 and s.startswith(sep):         # digit gap was to the left
                rs, s = sep, s[len(sep):]
            if i < len(parts) - 1 and s.endswith(sep):  # digit gap is to the right
                ls, s = sep, s[:-len(sep)]
        left_seps.append(ls)
        right_seps.append(rs)
        stripped.append(s)

    segment_pids = []
    for part in stripped:
        if not part:
            segment_pids.append(None)
            continue
        pid = _try_match(part, tokens)
        if pid is None:
            return None
        segment_pids.append(pid)

    if not any(pid is not None for pid in segment_pids):
        return None

    pieces = []
    for i, pid in enumerate(segment_pids):
        if i > 0:
            pieces.append(left_seps[i - 1])
            pieces.append("{d}")
            pieces.append(right_seps[i])
        if pid is not None:
            pieces.append(pid)

    return "".join(pieces) or None


# ─────────────────────────────────────────────────────────────────────────────
# Per-domain pattern building
# ─────────────────────────────────────────────────────────────────────────────

def build_patterns_for_domain(contacts: list[dict]) -> dict:
    """
    Given a list of non-generic lca_contacts rows for one domain,
    return the email_patterns row dict ready for upsert.
    """
    pattern_counts: dict[str, int]         = defaultdict(int)
    pattern_examples: dict[str, str]       = {}
    pattern_last_seen: dict[str, str]      = {}
    pattern_has_digit: dict[str, bool]     = {}
    total_unique_personal                   = len(contacts)

    for row in contacts:
        local = row["email"].split("@")[0].lower()
        first  = row.get("first_name")  or ""
        middle = row.get("middle_name") or ""
        last   = row.get("last_name")   or ""

        if not first and not last:
            continue

        pid, has_digit = detect_pattern(local, first, middle, last)
        if pid is None:
            log.debug("No pattern match: local=%r first=%r middle=%r last=%r", local, first, middle, last)
            continue

        pattern_counts[pid] += 1
        pattern_examples.setdefault(pid, local)
        dec = str(row.get("decision_date") or "")
        if dec > pattern_last_seen.get(pid, ""):
            pattern_last_seen[pid] = dec
        pattern_has_digit[pid] = has_digit

    if total_unique_personal == 0:
        return None

    patterns = []
    for pid, count in pattern_counts.items():
        prob = count / total_unique_personal
        if prob < PROBABILITY_FLOOR:
            continue
        patterns.append({
            "pattern_id":    pid,
            "count":         count,
            "probability":   round(prob, 4),
            "example_local": pattern_examples.get(pid, ""),
            "last_seen":     pattern_last_seen.get(pid, ""),
            "has_digit":     pattern_has_digit.get(pid, False),
        })

    patterns.sort(key=lambda p: -p["probability"])

    return {
        "patterns":              patterns,
        "total_unique_personal": total_unique_personal,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def run(domain_filter: str | None = None) -> None:
    conn = get_conn()
    try:
        query = """
            SELECT email, domain, first_name, middle_name, last_name, decision_date
            FROM lca_contacts
            WHERE is_generic = FALSE
              AND first_name IS NOT NULL
              AND last_name  IS NOT NULL
        """
        params = []
        if domain_filter:
            query += " AND domain = %s"
            params.append(domain_filter)
        query += " ORDER BY domain, decision_date DESC NULLS LAST"

        rows = conn.execute(query, params).fetchall()
        log.info("Loaded %d personal contacts from lca_contacts", len(rows))

        by_domain: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_domain[row["domain"]].append(dict(row))

        upserted = skipped = 0
        for domain, contacts in by_domain.items():
            result = build_patterns_for_domain(contacts)
            if result is None or not result["patterns"]:
                log.debug("No patterns derived for domain %s (%d contacts)", domain, len(contacts))
                skipped += 1
                continue

            conn.execute("""
                INSERT INTO email_patterns (domain, patterns, total_unique_personal, updated_at)
                VALUES (%s, %s::jsonb, %s, NOW())
                ON CONFLICT (domain) DO UPDATE SET
                    patterns              = EXCLUDED.patterns,
                    total_unique_personal = EXCLUDED.total_unique_personal,
                    updated_at            = NOW()
            """, (domain, json.dumps(result["patterns"]), result["total_unique_personal"]))
            upserted += 1

        conn.commit()
        log.info(
            "email_patterns: %d domains upserted, %d skipped (no patterns above %.0f%% floor)",
            upserted, skipped, PROBABILITY_FLOOR * 100,
        )
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Build email_patterns from lca_contacts")
    parser.add_argument("--domain", help="Process only this domain (for testing/backfill)")
    args = parser.parse_args()
    run(domain_filter=args.domain)


if __name__ == "__main__":
    init_logging("build_email_patterns")
    main()

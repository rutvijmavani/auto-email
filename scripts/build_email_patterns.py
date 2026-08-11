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

    # Direct match (no digits)
    pid = _try_match(local, tokens)
    if pid is not None:
        return pid, False

    # Digit substitution: replace each digit sequence with placeholder, try again
    digit_spans = [(m.start(), m.end()) for m in _DIGIT_RE.finditer(local)]
    if not digit_spans:
        return None, False

    # Build a version with all digit sequences replaced by a literal sentinel
    _DIGIT_SENTINEL = "\x00d\x00"
    scrubbed = _DIGIT_RE.sub(_DIGIT_SENTINEL, local)
    parts = scrubbed.split(_DIGIT_SENTINEL)

    # Try matching with {d} injected between each pair of parts
    for sep in SEPARATORS:
        rejoined = f"{{d}}{sep}".join(parts) if sep else "{d}".join(parts)
        # Now check if joining token values with sep gives us each part
        pid = _try_match_with_digit_slots(parts, sep, tokens)
        if pid is not None:
            return pid, True

    return None, True  # had digits but unrecognized


def _try_match_with_digit_slots(parts: list[str], sep: str, tokens: dict[str, str]) -> str | None:
    """
    Try to assign name tokens to fill the non-digit slots in `parts`.
    parts = local split on digit sequences, e.g. ["j", "smith"] for "j2smith"
    """
    if len(parts) == 1:
        # Digit at start or end: try matching the non-digit part
        stripped = parts[0]
        pid = _try_match(stripped, tokens)
        if pid:
            # Determine digit position from original: leading vs trailing
            return f"{{d}}{pid}" if not stripped else f"{pid}{{d}}"
        return None

    # Multiple parts — each part must be a token value (or empty for leading/trailing digit)
    token_names  = list(tokens.keys())
    token_values = list(tokens.values())

    n_slots = len(parts)  # number of non-digit segments
    # Try assigning one token per non-empty slot
    non_empty_parts = [(i, p) for i, p in enumerate(parts) if p]
    if not non_empty_parts:
        return None

    from itertools import permutations
    for selected in permutations(range(len(token_names)), len(non_empty_parts)):
        match = True
        for (slot_idx, part), tok_idx in zip(non_empty_parts, selected):
            if token_values[tok_idx] != part:
                match = False
                break
        if match:
            # Reconstruct pattern_id with {d} placeholders between segments
            result_parts = []
            sel_iter = iter(selected)
            for i, part in enumerate(parts):
                if part:
                    tok_idx = next(sel_iter)
                    result_parts.append(f"{{{token_names[tok_idx]}}}")
                else:
                    result_parts.append("{d}")
                if i < len(parts) - 1:
                    result_parts.append(f"{{d}}" if not part else sep if sep else "")
            # Simpler: build from non_empty_parts assignments with {d} between
            pieces = []
            assigned = {slot_idx: token_names[tok_idx]
                        for (slot_idx, _), tok_idx in zip(non_empty_parts, selected)}
            for i, part in enumerate(parts):
                if i > 0:
                    pieces.append("{d}")
                    if sep:
                        pieces.append(sep)
                if part:
                    pieces.append(f"{{{assigned[i]}}}")
            return "".join(pieces) if pieces else None
    return None


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

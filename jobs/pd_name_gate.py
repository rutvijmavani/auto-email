"""
jobs/pd_name_gate.py — employer-name sanity check for cross-domain redirects.

Pure functions, no network and no DB. A company's own redirect (old.com -> new.com) is normally
authoritative, but a handful land on an unrelated company (blink.app -> loftware.com for Kimberly-Clark).
The gate asks: does the employer name look like the domain we would STORE? If not, nothing is stored and the
(fein, old, new) pair goes to pd_redirect_review for a human decision (scripts/pd_redirect_review.py).

Strict rule: the name must match the NEW domain, or the old and new domains must contain each other
(same brand, other suffix: svitinc.net -> svitinc.com). A name that only matches the OLD domain is NOT enough: that is exactly
what a rebrand/acquisition (faurecia.com -> forvia.com) and a wrong redirect (stakaha.com -> largourugs.com)
both look like, so both go to review. Shared by scripts/pd_backfill_from_scan.py and the live resolvers.
"""
import re

from config import PD_NAME_GATE_MIN_LABEL, PD_NAME_GATE_MIN_TOKEN

# Words that say "a company" but nothing about which one; never evidence of a match.
NAME_STOP_WORDS = frozenset({
    "inc", "llc", "llp", "ltd", "corp", "corporation", "company", "group", "holdings", "holding",
    "systems", "solutions", "technologies", "technology", "services", "service", "health", "healthcare",
    "international", "global", "america", "americas", "north", "united", "states", "partners",
    "associates", "consulting", "enterprises", "limited",
})

HINT_ACQUISITION_LIKE = "acquisition-like"   # name matches the OLD domain only (rebrand / acquisition / wrong redirect)
HINT_NO_NAME_MATCH = "no-name-match"         # name matches neither domain


def _squash(domain: str) -> str:
    """'group.bnpparibas' -> 'groupbnpparibas' (dots/hyphens dropped so multi-label names still match)."""
    return re.sub(r"[^a-z0-9]", "", (domain or "").lower())


def _brand(domain: str) -> str:
    """First label of a registrable root ('cmegroup.com' -> 'cmegroup')."""
    return _squash((domain or "").split(".")[0])


def _name_tokens(employer_name: str) -> list:
    return [t for t in re.findall(r"[a-z0-9]+", (employer_name or "").lower())
            if len(t) >= PD_NAME_GATE_MIN_TOKEN and t not in NAME_STOP_WORDS]


def _matches(tokens: list, squashed: str) -> bool:
    return any(t in squashed or (len(squashed) >= PD_NAME_GATE_MIN_TOKEN and squashed in t) for t in tokens)


def check_redirect(employer_name: str, old_domain: str, new_domain: str) -> "tuple[bool, str]":
    """-> (ok, hint). ok=True: safe to store new_domain. hint is '' when ok, else a HINT_* tag.

    Both domains are registrable roots (jobs.public_domain._root output).
    """
    old_s, new_s = _squash(old_domain), _squash(new_domain)
    tokens = _name_tokens(employer_name)
    if _matches(tokens, new_s):
        return True, ""
    # Same brand, different suffix/prefix (svitinc.net -> svitinc.com, cme.com -> cmegroup.com, corp.x.com -> x.com)
    old_l, new_l = _brand(old_domain), _brand(new_domain)
    short, long_ = sorted((old_l, new_l), key=len)
    if len(short) >= PD_NAME_GATE_MIN_LABEL and short in long_:
        return True, ""
    return False, HINT_ACQUISITION_LIKE if _matches(tokens, old_s) else HINT_NO_NAME_MATCH

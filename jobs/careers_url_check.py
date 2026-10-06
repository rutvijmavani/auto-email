"""
jobs/careers_url_check.py — ownership checks for a careers_url before it is stored.

Pure functions, no network and no DB.

Two checks, for two different failure modes:

1. blocked_reason(url, employer_name) — every phase (3/4/6/7, enrichment + discovery + relay).
   A careers page is never on a bot-challenge vendor (perfdrive.com ...), a job aggregator (myvisajobs.com ...),
   or a mail/CDN vendor (google.com, cloudflare.com ...) — UNLESS the employer is that vendor (Google LLC,
   Cloudflare Inc, Microsoft Corp are real H-1B sponsors, so vendor roots are only blocked when the name does not
   match the root).

2. phase4_anchor_check(url, anchor_root, employer_name, ats_roots) — Phase 4 (Brave) only.
   Phases 3/6/7 start from the company's own domain, so what they land on is anchored by construction. A search
   engine result is not: it is whatever ranks for "{name} careers". With a verified public_domain as the anchor, a
   Brave URL is stored only if it is on the pd root, a same-brand root (corp.x.com -> x.com, x.net -> x.com), or a
   known ATS host whose slug/subdomain/path carries the employer name or the pd brand. Anything else is rejected.
"""
from urllib.parse import urlparse

from config import PD_NAME_GATE_MIN_LABEL
from jobs.pd_name_gate import NAME_STOP_WORDS, _brand, _name_tokens, _squash
from jobs.public_domain import GENERIC_ROOTS, _CHALLENGE_DOMAINS, _root

# Mail / CDN / hosting vendors beyond jobs.public_domain.GENERIC_ROOTS. Blocked as a careers host unless the
# employer name matches the root. Issue 3 (vendor-domain public_domain values) reuses VENDOR_ROOTS.
_EXTRA_VENDOR_ROOTS = frozenset({
    "google.com", "googlemail.com", "aol.com", "icloud.com", "live.com", "msn.com",
    "comcast.net", "att.net", "verizon.net", "sbcglobal.net",
    "cloudflaressl.com", "business.site",
})
VENDOR_ROOTS = frozenset(GENERIC_ROOTS) | _EXTRA_VENDOR_ROOTS

# Job boards / aggregators: never a company's own careers page, whatever the employer is called.
AGGREGATOR_ROOTS = frozenset({
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com", "dice.com",
    "hired.com", "wellfound.com", "builtin.com",
    "myvisajobs.com", "theladders.com", "zippia.com", "builtinnyc.com", "optnation.com",
    "iitjobs.com", "flexjobs.com", "instahyre.com", "dejobs.org", "diversityworking.com",
})

ALWAYS_BLOCKED_ROOTS = AGGREGATOR_ROOTS | _CHALLENGE_DOMAINS

REASON_AGGREGATOR = "aggregator-host"
REASON_CHALLENGE = "challenge-vendor-host"
REASON_VENDOR = "vendor-host"
REASON_OFF_DOMAIN = "off-domain"
REASON_ATS_NO_NAME = "ats-no-name-match"
REASON_NO_ROOT = "no-registrable-domain"


def _name_owns_root(employer_name: str, root: str) -> bool:
    """True when the employer name IS the root's brand ('Google LLC' / google.com, 'Cloudflare Inc' / cloudflare.com).
    Whole-name comparison, not any-token: 'Cloud Tek Data' must not pass for cloudflare.com."""
    import re
    words = [w for w in re.findall(r"[a-z0-9]+", (employer_name or "").lower()) if w not in NAME_STOP_WORDS]
    joined = "".join(words)
    brand = _brand(root)
    if min(len(joined), len(brand)) < PD_NAME_GATE_MIN_LABEL + 1:
        return False
    return brand in joined or joined in brand


def blocked_reason(url: str, employer_name: str) -> str:
    """'' when the URL's host may be a careers page for this employer, else a REASON_* tag."""
    root = _root(url)
    if not root:
        return REASON_NO_ROOT
    if root in _CHALLENGE_DOMAINS:
        return REASON_CHALLENGE
    if root in AGGREGATOR_ROOTS:
        return REASON_AGGREGATOR
    if root in VENDOR_ROOTS and not _name_owns_root(employer_name, root):
        return REASON_VENDOR
    return ""


def _same_brand(a_root: str, b_root: str) -> bool:
    short, long_ = sorted((_brand(a_root), _brand(b_root)), key=len)
    return len(short) >= PD_NAME_GATE_MIN_LABEL and short in long_


def _tenant_matches(employer_name: str, brand: str, tenant_text: str) -> bool:
    """True when a whole host label / path segment of an ATS URL IS the employer identifier.

    tenant_text is the ATS subdomain + path ('acme.wd5.', '/acmecorp/jobs'). Segments split on '.' and '/'
    only; hyphens/underscores stay inside a segment and are squashed away, so 'acme-other' becomes 'acmeother'
    and does NOT equal 'acme', while 'acme-corp' / 'acmecorp' equals the full name 'Acme Corp'."""
    import re
    words = re.findall(r"[a-z0-9]+", (employer_name or "").lower())
    identifiers = {t for t in _name_tokens(employer_name)}
    identifiers.add("".join(words))
    identifiers.add("".join(w for w in words if w not in NAME_STOP_WORDS))
    if len(brand) >= PD_NAME_GATE_MIN_LABEL + 1:
        identifiers.add(brand)
    identifiers.discard("")
    return any(_squash(seg) in identifiers for seg in re.split(r"[./]", tenant_text.lower()) if seg)


def phase4_anchor_check(url: str, anchor_root: str, employer_name: str, ats_roots) -> "tuple[bool, str]":
    """-> (ok, reason). anchor_root is the stored public_domain root; callers must not call this without one."""
    reason = blocked_reason(url, employer_name)
    if reason:
        return False, reason
    root = _root(url)
    if root == anchor_root or _same_brand(anchor_root, root):
        return True, ""
    if root in ats_roots:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        sub = host[:-len(root)] if host.endswith(root) else host
        if _tenant_matches(employer_name, _brand(anchor_root), sub + parsed.path):
            return True, ""
        return False, REASON_ATS_NO_NAME
    return False, REASON_OFF_DOMAIN

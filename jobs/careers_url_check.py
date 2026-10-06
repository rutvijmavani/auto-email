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

# Pure hosting/infrastructure roots whose brand is an ordinary word ('business'): no employer owns them, and
# stop-word stripping would otherwise make "Business Solutions Inc" look like the owner of business.site.
_NEVER_OWNED_ROOTS = frozenset({"business.site", "cloudflaressl.com"})

# Job boards / aggregators: never a company's own careers page, whatever the employer is called.
AGGREGATOR_ROOTS = frozenset({
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "monster.com", "careerbuilder.com", "simplyhired.com", "dice.com",
    "hired.com", "wellfound.com", "builtin.com",
    "myvisajobs.com", "theladders.com", "zippia.com", "builtinnyc.com", "optnation.com",
    "iitjobs.com", "flexjobs.com", "instahyre.com", "dejobs.org", "diversityworking.com",
})

ALWAYS_BLOCKED_ROOTS = AGGREGATOR_ROOTS | _CHALLENGE_DOMAINS

# Host labels that are part of the ATS provider's own hostname layout, never the employer's tenant.
_ATS_FIXED_LABELS = frozenset({"www", "boards", "job-boards", "boards-api", "api", "jobs", "careers", "apply", "hire"})

REASON_AGGREGATOR = "aggregator-host"
REASON_CHALLENGE = "challenge-vendor-host"
REASON_VENDOR = "vendor-host"
REASON_OFF_DOMAIN = "off-domain"
REASON_ATS_NO_NAME = "ats-no-name-match"
REASON_NO_ROOT = "no-registrable-domain"


def _name_owns_root(employer_name: str, root: str) -> bool:
    """True when the employer name IS the root's brand ('Google LLC' / google.com, 'Cloudflare Inc' / cloudflare.com).
    Whole-name equality after dropping legal/generic suffix words, not containment: 'Cloud Tek Data' must not pass
    for cloudflare.com and 'Business Solutions Inc' must not pass for business.site ('Google Public Sector' is a
    subsidiary on its own domain, not Google LLC)."""
    import re
    if root in _NEVER_OWNED_ROOTS:
        return False
    words = [w for w in re.findall(r"[a-z0-9]+", (employer_name or "").lower()) if w not in NAME_STOP_WORDS]
    joined = "".join(words)
    return bool(joined) and joined == _brand(root)


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


def ats_derived_from_careers_source(ats_source, careers_source) -> bool:
    """True when the ATS platform/slug was read off the same page as careers_url (so dropping a blocked
    careers_url must drop the ATS too): same phase, or Phase 5 fingerprinting a Phase 4 Brave result. An ATS
    found independently (company_ats cache, Phase 7 BFS under a different careers source) is kept."""
    return bool(ats_source) and (ats_source == careers_source
                                 or (careers_source == "phase4" and ats_source == "phase5"))


def _same_brand(a_root: str, b_root: str) -> bool:
    short, long_ = sorted((_brand(a_root), _brand(b_root)), key=len)
    return len(short) >= PD_NAME_GATE_MIN_LABEL and short in long_


def _tenant_matches(employer_name: str, brand: str, tenant_text: str) -> bool:
    """True when a whole host label / path segment of an ATS URL IS the employer identifier.

    tenant_text is the ATS subdomain + path ('acme.wd5.', '/acmecorp/jobs'). Segments split on '.' and '/'
    only; hyphens/underscores stay inside a segment and are squashed away, so 'acme-other' becomes 'acmeother'
    and does NOT equal 'acme', while 'acme-corp' / 'acmecorp' equals the full name 'Acme Corp'.
    Fixed provider host labels (boards, job-boards, www ...) are not tenants and never count as a match, so
    'Boards Inc' does not match boards.greenhouse.io/zenith."""
    import re
    host_part, _, path_part = tenant_text.lower().partition("/")
    # Tenant-bearing positions only: first non-provider host label (acme.wd5.myworkdayjobs.com) and first path
    # segment (boards.greenhouse.io/acme, jobs.lever.co/acme/<job>). Later path segments are job slugs / filters,
    # so boards.greenhouse.io/zenith/jobs/acme is Zenith's board, not Acme's.
    labels = [l for l in host_part.split(".") if l and l not in _ATS_FIXED_LABELS]
    segments = [s for s in path_part.split("/") if s]
    candidates = labels[:1] + segments[:1]
    words = re.findall(r"[a-z0-9]+", (employer_name or "").lower())
    identifiers = {t for t in _name_tokens(employer_name)}
    identifiers.add("".join(words))
    identifiers.add("".join(w for w in words if w not in NAME_STOP_WORDS))
    if len(brand) >= PD_NAME_GATE_MIN_LABEL + 1:
        identifiers.add(brand)
    identifiers.discard("")
    return any(_squash(c) in identifiers for c in candidates)


# Phase 4 confidence ranks (lower is better): the employer's own domain beats a same-brand domain beats an
# ATS tenant page.
RANK_PD_ROOT, RANK_SAME_BRAND, RANK_ATS_TENANT = 0, 1, 2


def phase4_rank(url: str, anchor_root: str, employer_name: str, ats_roots) -> "tuple[int | None, str]":
    """-> (rank, reason). rank is None (with a REASON_* tag) when the URL is rejected."""
    reason = blocked_reason(url, employer_name)
    if reason:
        return None, reason
    root = _root(url)
    if root == anchor_root:
        return RANK_PD_ROOT, ""
    if _same_brand(anchor_root, root):
        return RANK_SAME_BRAND, ""
    if root in ats_roots:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        sub = host[:-len(root)] if host.endswith(root) else host
        if _tenant_matches(employer_name, _brand(anchor_root), sub + parsed.path):
            return RANK_ATS_TENANT, ""
        return None, REASON_ATS_NO_NAME
    return None, REASON_OFF_DOMAIN


def phase4_anchor_check(url: str, anchor_root: str, employer_name: str, ats_roots) -> "tuple[bool, str]":
    """-> (ok, reason). anchor_root is the stored public_domain root; callers must not call this without one."""
    rank, reason = phase4_rank(url, anchor_root, employer_name, ats_roots)
    return rank is not None, reason

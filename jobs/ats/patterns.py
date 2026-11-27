# jobs/ats/patterns.py — ATS URL patterns and validation helpers
#
# Each URL pattern extracts platform + slug from a career page URL.
# Used by career_page.py, ats_verifier.py, ats_sitemap.py to identify ATS.
#
# Site-search queries (formerly ATS_SITE_SEARCHES) now live in registry.py
# under each platform's "site_search" key — single source of truth.

import re
import json

# ─────────────────────────────────────────
# URL PATTERNS
# Each entry: (compiled_regex, platform, slug_extractor_fn)
# ─────────────────────────────────────────

# Hosts that are Workday aggregator / portal / marketing sites, never a tenant. Only matters for
# the bare form ({tenant}.myworkdayjobs.com); the wdN form was never ambiguous. Shared by
# patterns.py, career_detector.py and build_ats_slug_list.py.
WORKDAY_NON_TENANT = frozenset({
    "jobs", "apply", "www", "careers", "secure", "community", "wd", "impl", "myworkday",
})

# What may follow a whole URL path segment: the end, the next URL part, what closes a link in text or
# markup, or sentence punctuation. Anything else ("fmr+test") means the segment goes on.
URL_SEGMENT_END = r"""(?=$|[/?#&\s"'<>\\)\]}|`]|[.,;:!](?![a-z0-9]))"""

_SITEMAP_MARKER = r"(?:sitemap[^\s]*\.xml|feed\.xml)"   # what makes a URL a sitemap / XML feed

# The path of a Workday link: it ends at the query, the fragment, or what closes a link in text or
# markup (a Markdown ")" included).
_WORKDAY_PATH = r"""([^?&#\s"'<>\\)\]}|`]*)"""
_WORKDAY_LOCALE = re.compile(r'^[a-z]{2}[-_][A-Z]{2}$')


def _workday_site(path):
    """The career site of a Workday link's path: the first segment that is not a locale, without
    trailing sentence punctuation; "" when the path names none."""
    parts = (p.rstrip(".,;:!") for p in (path or "").split("/"))
    return next((p for p in parts if p and not _WORKDAY_LOCALE.match(p)), "")


def _make_patterns():
    patterns = []


    # Greenhouse embed — any path under /embed/ that carries ?for={slug}
    # Covers all known variants:
    #   /embed/job_board?for=stripe            (job board widget)
    #   /embed/job_app?for=stripe&token=…      (apply form iframe — Stripe pattern)
    #   /embed/job_board/js?for=stripe&b=1     (JS script loader)
    patterns.append((
        re.compile(
            r"(?:boards|job-boards)\.greenhouse\.io/embed/[^?#\s]+\?(?:[^#\s]*&)?for=([^&\s\"'<>]+)",
            re.IGNORECASE
        ),
        "greenhouse",
        lambda m: m.group(1).lower(),
    ))
    # Greenhouse — boards.greenhouse.io/{slug}/jobs  (slug is first path segment,
    # not "embed" — the embed pattern above must run first so embed/ URLs don't
    # fall through here and return "embed" as the slug).
    # Also handles EU region: boards.eu.greenhouse.io/{slug}
    patterns.append((
        re.compile(
            r"(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?!embed/)([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "greenhouse",
        lambda m: m.group(1).lower().rstrip("/"),
    ))

    # Lever — jobs.lever.co/{slug}
    # Also: hire.lever.co/{slug}
    patterns.append((
        re.compile(
            r"(?:jobs|hire)\.lever\.co/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "lever",
        lambda m: m.group(1).lower().rstrip("/"),
    ))

    # Ashby — jobs.ashbyhq.com/{slug}
    patterns.append((
        re.compile(
            r"jobs\.ashbyhq\.com/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "ashby",
        lambda m: m.group(1).lower().rstrip("/"),
    ))

    # SmartRecruiters — oneclick-ui job-detail URLs:
    #   jobs.smartrecruiters.com/oneclick-ui/company/{slug}/publication/{id}
    # Must come before the generic pattern below (same domain, longer path).
    patterns.append((
        re.compile(
            r"jobs\.smartrecruiters\.com/oneclick-ui/company/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "smartrecruiters",
        lambda m: m.group(1).lower().rstrip("/"),
    ))

    # SmartRecruiters — jobs.smartrecruiters.com/{slug}  (standard listing page)
    patterns.append((
        re.compile(
            r"jobs\.smartrecruiters\.com/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "smartrecruiters",
        lambda m: m.group(1).lower().rstrip("/"),
    ))

    # Workday — {slug}.{wd}.myworkdayjobs.com/{path}
    # Excludes: jobs.myworkdayjobs.com (aggregator)
    #           apply.myworkdayjobs.com (application portal)
    # Note: URLs often have locale prefix before the career site name
    #   e.g. /en-US/NVIDIAExternalCareerSite/job/...
    #        /NVIDIAExternalCareerSite/job/...
    # We always want the career site name, not the locale prefix.
    def _workday_slug(m):
        tenant = m.group(1).lower()
        wd     = (m.group(2) or "").lower()   # "" = bare host ({tenant}.myworkdayjobs.com)
        if not wd and tenant in WORKDAY_NON_TENANT:
            return None
        return json.dumps({
            "slug": tenant,
            "wd":   wd,
            # bare host or locale-only path: never use the locale as the site
            "path": _workday_site(m.group(3)) or "careers",
        })

    def _workday_root_only(m):
        """True when the match names no career site (bare host or locale-only path)."""
        return not _workday_site(m.group(3))

    _workday_slug.root_only = _workday_root_only

    patterns.append((
        re.compile(
            # (?<![a-z0-9.-]) keeps "a.b.myworkdayjobs.com" from reading "b" as a bare tenant.
            r"(?<![a-z0-9.-])([a-z0-9-]+)\.(?:(wd\d+)\.)?myworkdayjobs\.com(?![a-z0-9_-])(?!\.[a-z0-9])(?::\d+)?"
            r"(?:/" + _WORKDAY_PATH + r")?",
            re.IGNORECASE
        ),
        "workday",
        _workday_slug,
    ))

    # Workday (alternate domain) — {wd}.myworkdaysite.com/recruiting/{tenant}/{career_site}
    # e.g. wd1.myworkdaysite.com/recruiting/fmr/FidelityCareers/...
    # API: wd1.myworkdaysite.com/wday/cxs/{tenant}/{career_site}/jobs
    def _workdaysite_slug(m):
        wd     = m.group(1).lower()
        tenant = m.group(2).lower()
        return json.dumps({
            "slug": tenant,
            "wd":   wd,
            # nothing or only a locale: never use the locale as the site
            "path": _workday_site(m.group(3)) or "careers",
            "site": "myworkdaysite",  # flag different base URL
        })

    def _workdaysite_root_only(m):
        """True when the match names no career site (nothing or only a locale after the tenant)."""
        return not _workday_site(m.group(3))

    _workdaysite_slug.root_only = _workdaysite_root_only

    patterns.append((
        re.compile(
            # (?<![a-z0-9.-]): wd1 must start the hostname ("team.wd1.myworkdaysite.com" is not it)
            r"(?<![a-z0-9.-])(wd\d+)\.myworkdaysite\.com/"
            r"(?:[a-z]{2}[-_][A-Z]{2}/)?"   # optional locale: en-US, fr-FR, pt_BR
            r"recruiting/([a-z0-9-]+)" + URL_SEGMENT_END +   # the whole tenant segment
            r"(?:/" + _WORKDAY_PATH + r")?",   # the site part is optional: the URL may end at the tenant
            re.IGNORECASE
        ),
        "workday",
        _workdaysite_slug,
    ))

    # Oracle HCM — {slug}.fa.oraclecloud.com/hcmUI/.../sites/{site_id}
    # Also handles regional and OCS variants:
    #   hdpc.fa.us2.oraclecloud.com           (Goldman Sachs — regional)
    #   jpmc.fa.oraclecloud.com               (JPMorgan — no region)
    #   fa-extu-saasfaprod1.fa.ocs.oraclecloud.com  (Akamai — OCS cluster)
    #   fa-evmr-saasfaprod1.fa.ocs.oraclecloud.com  (Nokia  — OCS cluster)
    # Group layout:
    #   1 = slug
    #   2 = "ocs." when present (None otherwise) — stored as bool flag
    #   3 = region (us2, eu1, ap1 …) or None
    #   4 = site_id
    patterns.append((
        re.compile(
            r"([a-z0-9][a-z0-9\-]*[a-z0-9])\.fa\.(ocs\.)?(?:(us\d+|eu\d+|ap\d+)\.)?"
            r"oraclecloud\.com(?::\d+)?/hcmUI/[^?#]*?/sites/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "oracle_hcm",
        lambda m: json.dumps({
            "slug":   m.group(1).lower(),
            "ocs":    m.group(2) is not None,   # True for .fa.ocs.oraclecloud.com
            "region": m.group(3).lower() if m.group(3) else "",
            "site":   m.group(4).rstrip("/"),
        }),
    ))

    # Oracle HCM JS fingerprint — {slug}.fa.{region}.oraclecloud.com/hcmUI/
    # (no /sites/ in URL — fires on <script src="..."> asset tags from custom-domain
    # career pages such as careers.americanexpress.com)
    #
    # These pages embed the Oracle HCM JS as:
    #   <script src="https://egug.fa.us2.oraclecloud.com:443/hcmUI/CandExpStatic/js/ce-custom.js">
    # The :443 port and absence of /sites/ meant the primary pattern above never fired.
    # (?::\d+)? handles the explicit port; site="" triggers auto-discovery in fetch_jobs().
    #
    # MUST come AFTER the primary pattern so URLs that do contain /sites/ are handled
    # by the richer pattern (site extracted directly) and don't fall through here.
    patterns.append((
        re.compile(
            r"([a-z0-9][a-z0-9\-]*[a-z0-9])\.fa\.(ocs\.)?(?:(us\d+|eu\d+|ap\d+)\.)?"
            r"oraclecloud\.com(?::\d+)?/hcmUI/",
            re.IGNORECASE
        ),
        "oracle_hcm",
        lambda m: json.dumps({
            "slug":   m.group(1).lower(),
            "ocs":    m.group(2) is not None,
            "region": m.group(3).lower() if m.group(3) else "",
            "site":   "",   # auto-discovered by oracle_hcm.fetch_jobs()
        }),
    ))

    # iCIMS — careers-{slug}.icims.com/jobs  or  {slug}.icims.com/jobs
    # The full subdomain is stored as the slug (including any careers-/career-
    # prefix) because it is used verbatim when building iCIMS API URLs.
    # e.g. careers-schwab.icims.com → slug "careers-schwab"
    #      careers-charter.icims.com → slug "careers-charter"
    patterns.append((
        re.compile(
            r"((?:careers-)?[a-z0-9][a-z0-9\-]*[a-z0-9]|[a-z0-9]+)"
            r"\.icims\.com(?:/jobs|$)",
            re.IGNORECASE
        ),
        "icims",
        lambda m: m.group(1).lower(),
    ))
    
    # Jobvite — jobs.jobvite.com/{slug}/job/{id}
    # Also: jobs.jobvite.com/{slug}/jobs (listing)
    patterns.append((
        re.compile(r"jobs\.jobvite\.com/([^/?&#\s]+)/", re.IGNORECASE),
        "jobvite",
        lambda m: m.group(1).lower(),
    ))

    # SAP SuccessFactors — {company}.jobs2web.com
    patterns.append((
        re.compile(
            r"([a-z0-9]+)\.jobs2web\.com",
            re.IGNORECASE
        ),
        "successfactors",
        lambda m: m.group(1).lower(),
    ))

    # SAP SuccessFactors — career{dc}.successfactors.{region}/career(s)?company=...
    # Must come before the generic subdomain pattern below — same domain but the
    # subdomain is a datacenter ID (career4, career12) not the company slug.
    # The company slug is in the ?company= query param.
    patterns.append((
        re.compile(
            r"career(\d+)\.successfactors\.(com|eu)/(careers?)\?.*company=([^&\s]+)",
            re.IGNORECASE,
        ),
        "successfactors",
        lambda m: json.dumps({
            "slug":   m.group(4),
            "dc":     m.group(1),
            "region": m.group(2),
            # Only store "path" when non-default (/careers) — mirrors detect() behaviour
            **({} if m.group(3).lower() == "career" else {"path": f"/{m.group(3)}"}),
        }),
    ))

    # SAP SuccessFactors — {company}.successfactors.com/careers
    # Generic pattern: subdomain IS the company slug (not a datacenter ID).
    patterns.append((
        re.compile(
            r"([a-z0-9]+)\.successfactors\.com/careers",
            re.IGNORECASE
        ),
        "successfactors",
        lambda m: m.group(1).lower(),
    ))

    # Avature hosted — {slug}.avature.net
    patterns.append((
        re.compile(r"([a-z0-9]+)\.avature\.net/", re.IGNORECASE),
        "avature",
        lambda m: json.dumps({"base": f"https://{m.group(1)}.avature.net", "path": "careers"}),
    ))

    # Avature custom domains
    for _domain, _path in [("jobs.ea.com", "en_US/careers")]:
        patterns.append((
            re.compile(re.escape(_domain), re.IGNORECASE),
            "avature",
            lambda m, d=_domain, p=_path: json.dumps({"base": f"https://{d}", "path": p}),
        ))

    # ── Phenom People (custom domains) ──────────────────────────────────────────
    # careers.chewy.com/us/en/job/{id}/{slug}
    # jobs.ebayinc.com/us/en/job/{id}/{slug}
    # Pattern: domain/path/job/{id}
    # Slug_info stored as JSON with base, path, sitemap
    for _phenom_domain, _phenom_path in [
        ("careers.chewy.com",  "us/en"),
        ("jobs.ebayinc.com",   "us/en"),
    ]:
        patterns.append((
            re.compile(re.escape(_phenom_domain), re.IGNORECASE),
            "phenom",
            # Use default args to capture loop variables
            lambda m, d=_phenom_domain, p=_phenom_path: json.dumps({
                "base":    f"https://{d}",
                "path":    p,
                "sitemap": f"{p}/sitemap.xml",
            }),
        ))

    # ── TalentBrew / Radancy ─────────────────────────────────────────────────────
    # jobs.intuit.com/job/{city}/{slug}/{tenant_id}/{job_id}
    # jobs.disneycareers.com/job/{city}/{slug}/{tenant_id}/{job_id}
    # Tenant ID extracted from URL (3rd-to-last numeric segment)
    for _tb_domain, _tb_tenant in [
        ("jobs.intuit.com",         "27595"),
        ("jobs.disneycareers.com",  "391"),
    ]:
        patterns.append((
            re.compile(re.escape(_tb_domain), re.IGNORECASE),
            "talentbrew",
            lambda m, d=_tb_domain, t=_tb_tenant: json.dumps({
                "base":      f"https://{d}",
                "tenant_id": t,
            }),
        ))

    # ── Google Careers XML feed ──────────────────────────────────────────────────
    # google.com/about/careers/applications/jobs/feed.xml
    patterns.append((
        re.compile(r"google\.com/about/careers/applications/jobs/feed\.xml", re.IGNORECASE),
        "google",
        lambda m: "{}",
    ))

    # Taleo — {company}.taleo.net/careersection/...
    # portal_id auto-discovered during first fetch_jobs() call
    patterns.append((
        re.compile(
            r"([a-z0-9][a-z0-9\-]*?)\.taleo\.net/careersection/([^/?&#\s]+)/",
            re.IGNORECASE
        ),
        "taleo",
        lambda m: json.dumps({
            "company": m.group(1).lower(),
            "portal_id": "",   # auto-discovered on first fetch
            "section":  m.group(2).lower(),
        }),
    ))

    # ── Apple Jobs ──────────────────────────────────────────────────────────────
    # jobs.apple.com/sitemap/sitemap-jobs-en-us.xml
    # jobs.apple.com/en-us/details/{id}/{slug}
    patterns.append((
        re.compile(r"jobs\.apple\.com/", re.IGNORECASE),
        "apple",
        lambda m: "{}",
    ))

    # Eightfold.ai — {slug}.eightfold.ai/careers
    patterns.append((
        re.compile(r"([a-z0-9][a-z0-9\-]*)\.eightfold\.ai/", re.IGNORECASE),
        "eightfold",
        lambda m: json.dumps({
            "slug":   m.group(1).lower(),
            "domain": "",   # filled manually or via career page scan
        }),
    ))

    # Jibe / iCIMS Jibe — {domain}/api/jobs or app.jibecdn.com in HTML
    # slug = careers domain e.g. "careers.rivian.com"
    patterns.append((
        re.compile(r"(careers\.[a-z0-9\-]+\.[a-z]+)/(?:api/jobs|careers-home)", re.IGNORECASE),
        "jibe",
        lambda m: m.group(1).lower(),
    ))

    # ── ADP WorkforceNow — myjobs.adp.com/{slug}/cx/ ────────────────────────────
    # Career page / listing API: myjobs.adp.com/{slug}/cx/job-listing
    # Job detail page:           myjobs.adp.com/{slug}/cx/job-details?reqId=…
    # The {slug} is the company identifier (e.g. "apply", "scacareers").
    patterns.append((
        re.compile(
            r"myjobs\.adp\.com/([a-z0-9][a-z0-9\-]*)/cx/",
            re.IGNORECASE
        ),
        "adp",
        lambda m: m.group(1).lower(),
    ))

    # ── Generic sitemap / XML feed ───────────────────────────────────────────────
    # Matches when user provides any sitemap.xml or feed.xml URL directly
    # Stores full URL as slug_info for sitemap.py
    # NOTE: This pattern is intentionally broad — only matched when URL
    # explicitly contains sitemap.xml or feed.xml
    patterns.append((
        re.compile(r"(https?://[^\s]+" + _SITEMAP_MARKER + r")", re.IGNORECASE),
        "sitemap",
        lambda m: json.dumps({"url": m.group(1)}),
    ))

    return patterns


ATS_URL_PATTERNS = _make_patterns()


# ─────────────────────────────────────────
# KNOWN COMPANY ALIASES
# Some companies use different names on ATS
# ─────────────────────────────────────────

COMPANY_ALIASES = {
    # Maps display name → list of slug variants
    # RULE: only include full normalized names, no abbreviations
    # Abbreviations like 'wf', 'ms', 'gs' cause false positives
    # with unrelated companies (e.g. 'ms' matches Microsoft AND Morgan Stanley)
    # Truth comes from API verification, not slug string matching
    "Meta":                          ["meta", "facebook"],
    "X":                             ["x", "twitter"],
    "Alphabet":                      ["alphabet", "google"],
    "Google":                        ["google", "alphabet"],
    "Waymo":                         ["waymo", "alphabet"],
    "Block":                         ["block", "squareup", "square"],
    "Docusign":                      ["docusign"],
    "T-Mobile":                      ["tmobile", "t-mobile"],
    "AT&T":                          ["att", "at&t"],
    "NXP USA":                       ["nxpusa", "nxp"],
    "SAP America":                   ["sapamerica", "sap"],
    "Sirius XM":                     ["siriusxm", "sirius"],
    "JPMorgan Chase":                ["jpmc", "jpmorganchase", "jpmorgan"],
    "Goldman Sachs":                 ["goldmansachs", "goldman"],
    "Bank of America":               ["bankofamerica", "bofa"],
    "General Motors":                ["generalmotors", "gm"],
    "Ford Motor Company":            ["fordmotor", "ford"],
    "Samsung Electronics America":   ["samsungelectronicsamerica", "samsung"],
    "Sony Interactive Entertainment":["sonyinteractiveentertainment", "sony", "sie"],
    "Electronic Arts":               ["electronicarts", "ea"],
    "Palo Alto Networks":            ["paloaltonetworks", "paloalto"],
    "Cadence Design Systems":        ["cadencedesign", "cadence"],
    "KLA Corporation":               ["klacorporation", "kla"],
    "Lam Research":                  ["lamresearch", "lam"],
    "Marvell Semiconductor":         ["marvellsemiconductor", "marvell"],
    "Micron Technology":             ["microntechnology", "micron"],
    "Western Digital":               ["westerndigital"],
    "Applied Materials":             ["appliedmaterials", "amat"],
    "Texas Instruments":             ["texasinstruments"],
    "Analog Devices":                ["analogdevices"],
    "Charter Communications":        ["chartercommunications", "charter"],
    "Cox Automotive":                ["coxautomotive", "cox"],
    "Elevance Health":               ["elevancehealth", "elevance", "anthem"],
    "Gilead Sciences":               ["gileadsciences", "gilead"],
    "Charles Schwab":                ["charlesschwab", "schwab"],
    "Morgan Stanley":                ["morganstanley"],
    "Deutsche Bank":                 ["deutschebank"],
    "State Street":                  ["statestreet"],
    "Wells Fargo":                   ["wellsfargo"],
    "Fidelity":                      ["fidelity"],
    "Citibank":                      ["citibank", "citi"],
    "American Express":              ["americanexpress", "amex"],
    "Capital One":                   ["capitalone"],
    "ServiceNow":                    ["servicenow"],
    "Intuit":                        ["intuit"],
    "Doordash":                      ["doordash"],
    "Wayfair":                       ["wayfair"],
    "Fortinet":                      ["fortinet"],
    "Nutanix":                       ["nutanix"],
    "Splunk":                        ["splunk"],
    "Informatica":                   ["informatica"],
    "Akamai Technologies":           ["akamai"],
    "NetApp":                        ["netapp"],
    "Juniper Networks":              ["junipernetworks", "juniper"],
    "Synopsys":                      ["synopsys"],
    "Xilinx":                        ["xilinx"],
    "Starbucks":                     ["starbucks"],
    "Caterpillar":                   ["caterpillar"],
    "Honeywell":                     ["honeywell"],
    "Siemens":                       ["siemens"],
    "Nokia":                         ["nokia"],
    "Ericsson":                      ["ericsson"],
    "Bosch":                         ["bosch"],
    "Genentech":                     ["genentech"],
    "Visa":                          ["visa"],
    "VMware":                        ["vmware"],
    "Optum":                         ["optum"],
    "Lucid":                         ["lucid"],
    "MathWorks":                     ["mathworks"],
    "ByteDance":                     ["bytedance"],
    "TikTok":                        ["tiktok"],
    "Cruise":                        ["cruise", "getcruise"],
    "Citrix":                        ["citrix"],
}


def get_slug_keywords(company):
    """
    Get all valid slug keywords for a company.
    Includes aliases for companies with different ATS names.
    Returns list of lowercase strings.
    """
    import re as _re
    from jobs.ats_detector import _get_keywords

    # Base keywords from company name
    base = _get_keywords(company)

    # Known aliases
    aliases = COMPANY_ALIASES.get(company, [])

    # Combine + deduplicate
    all_kw = list(dict.fromkeys(base + aliases))
    return all_kw


def match_ats_pattern(url, own_url=False):
    """
    Try all ATS URL patterns against a URL.
    Returns {platform, slug} or None.

    own_url=True: `url` is one pasted link, not free text. Only a match that begins in its own
    scheme/host counts, so a link inside its path or query cannot decide the platform. User
    information ("user@") is dropped first: it is not the host. A "/url?q=" redirect is followed
    only from a Google origin; a pasted one from any other origin matches nothing. A sitemap is
    recognised from the link's own path, and keeps its query.
    """
    if not url:
        return None

    if own_url:
        redirect = _redirect_parts(url)
        if redirect and not _is_google_origin(redirect[0]):
            return None

    # Decode Google redirect URLs
    url = _decode_google_redirect(url)

    start_limit = len(url)   # matches must start before this index
    if own_url:
        from urllib.parse import urlsplit
        try:
            netloc = urlsplit(url).netloc
        except ValueError:
            return None
        at = url.find(netloc)
        if at < 0:   # urlsplit removed characters (tab/newline) from the host: not a clean pasted URL
            return None
        host = netloc.rsplit("@", 1)[-1]
        url = url[:at] + host + url[at + len(netloc):]
        start_limit = at + len(host)

    root_only = None   # a workday match that names no career site: kept while an explicit site is looked for
    for pattern, platform, extractor in ATS_URL_PATTERNS:
        # Every match is tried: an earlier one may be rejected (e.g. jobs.myworkdayjobs.com)
        # while a later one in the same text is a real tenant.
        for m in pattern.finditer(url):
            if m.start() >= start_limit:
                break
            try:
                slug = extractor(m)
                bare = bool(slug) and getattr(extractor, "root_only", lambda _m: False)(m)
            except Exception:
                continue
            if own_url and platform == "sitemap":
                # its match always begins at the scheme, so the start limit proves nothing
                if not re.search(_SITEMAP_MARKER, urlsplit(url).path, re.IGNORECASE):
                    continue
                slug = json.dumps({"url": url.split("#", 1)[0]})   # the query may select the feed
            if slug and bare:
                if root_only is None:
                    root_only = {"platform": platform, "slug": slug}
                continue
            if slug:
                return {"platform": platform, "slug": slug}

    return root_only


def validate_slug_for_company(slug, company):
    """
    Validate that extracted slug belongs to expected company.
    At least one company keyword must appear in the slug text.

    Handles:
      - Simple string slugs: "stripe"
      - JSON slugs: {"slug":"capitalone","wd":"wd12","path":"Capital_One"}
    """
    keywords = get_slug_keywords(company)

    # Extract searchable text from slug
    slug_text = _slug_to_text(slug)

    # Check each keyword — at least one must match
    for kw in keywords:
        # Check 1: word boundary match
        # Prevents "block" matching "hrblock"
        pattern = r'(?<![a-z0-9])' + re.escape(kw) + r'(?![a-z0-9])'
        if re.search(pattern, slug_text, re.IGNORECASE):
            return True

        # Check 2: compound slug match
        # Handles: "capitalone" contains "capital"
        #          "Capital_One" → ["capital", "one"]
        #          "capitalOne"  → ["capital", "one"]
        parts = _split_compound(slug_text)
        if any(kw.lower() == p.lower() for p in parts):
            return True

        # Check 3: prefix match for all-lowercase compound slugs
        # "capitalone" starts with "capital" → match ✓
        # "hrblock" does NOT start with "block" → no match ✓
        slug_lower = slug_text.lower()
        if slug_lower.startswith(kw.lower()):
            return True

        # Check 4: substring match within path/career site name
        # "WellsFargoJobs" contains "wells" and "fargo" ✓
        # "wellsfargo" contains "wells" ✓
        # Guard: keyword must not appear as a suffix of another word
        # e.g. "block" in "hrblock" → rejected (suffix match)
        # e.g. "wells" in "wellsfargo" → accepted (prefix match)
        if len(kw) >= 4:
            kw_lower   = kw.lower()
            text_lower = slug_text.lower()
            idx = text_lower.find(kw_lower)
            while idx != -1:
                # Check char before — must not be a letter (no suffix match)
                before_ok = (idx == 0 or not text_lower[idx - 1].isalpha())
                if before_ok:
                    return True
                idx = text_lower.find(kw_lower, idx + 1)

    return False


def _split_compound(slug_text):
    """
    Split compound slug into parts for matching.
    "capitalone"  → ["capitalone", "capital", "one"]
    "Capital_One" → ["Capital", "One", "capital", "one"]
    "jpmc"        → ["jpmc"]
    """
    parts = set()
    parts.add(slug_text.lower())

    # Split on underscores, hyphens, dots
    for sep in ["_", "-", "."]:
        if sep in slug_text:
            for p in slug_text.split(sep):
                if p:
                    parts.add(p.lower())

    # Split camelCase: "capitalOne" → ["capital", "One"]
    camel_parts = re.findall(r'[A-Z]?[a-z0-9]+|[A-Z]+(?=[A-Z]|$)', slug_text)
    for p in camel_parts:
        parts.add(p.lower())

    return list(parts)


def _slug_to_text(slug):
    """Convert slug (string or JSON) to searchable text."""
    if not slug:
        return ""
    try:
        data = json.loads(slug)
        # Combine all string values
        return " ".join(
            str(v) for v in data.values()
            if isinstance(v, str)
        ).lower()
    except (json.JSONDecodeError, TypeError):
        return str(slug).lower()


# ─────────────────────────────────────────
# COMPANY NAME MATCHING
# Moved here from base.py so all ATS validation logic lives in one module.
# base.py re-exports this for backward compatibility with existing importers.
# ─────────────────────────────────────────

def validate_company_match(response_text, expected_company):
    """
    Check that an API response or URL text belongs to the expected company.

    Strips common legal suffixes (Inc, Corp, LLC…) and checks that at least
    one significant keyword from the company name appears in the response,
    using word-boundary matching to avoid false positives.

    Returns True when the match is likely correct (or when there is
    insufficient signal to make a determination).
    """
    if not response_text or not expected_company:
        return True
    expected = expected_company.lower().strip()
    response = response_text.lower()
    stop_words = {
        "inc", "corp", "llc", "ltd", "co", "the",
        "and", "jobs", "careers", "group",
    }
    words = [
        w for w in expected.split()
        if len(w) > 3 and w not in stop_words
    ]
    if not words:
        return True
    for word in words[:2]:
        pattern = r'(?<![a-z0-9])' + re.escape(word) + r'(?![a-z0-9])'
        if re.search(pattern, response):
            return True
    return False


_GOOGLE_HOST = re.compile(r'(?:www\.)?google\.(?:com?\.[a-z]{2}|com|[a-z]{2})\.?', re.IGNORECASE)


def _redirect_parts(url):
    """(parsed URL, "q" value) when `url` has the "/url?q=" redirect shape, else None."""
    from urllib.parse import urlparse, parse_qs
    try:
        parsed = urlparse(url)
    except ValueError:
        # Scraped HTML can contain "http://[..." — urlparse rejects the bracketed
        # netlocs; treat as "not a redirect" and let the ATS regexes decide.
        return None
    if parsed.path != "/url":
        return None
    q = parse_qs(parsed.query).get("q")
    return (parsed, q[0]) if q else None


def _is_google_origin(parsed):
    """A Google host, or a relative link (as scraped from a Google results page)."""
    if not parsed.scheme and not parsed.netloc:
        return True
    return parsed.scheme in ("", "http", "https") and bool(_GOOGLE_HOST.fullmatch(parsed.hostname or ""))


def _decode_google_redirect(url):
    """
    Handle Google redirect URLs:
    /url?q=https://boards.greenhouse.io/stripe/jobs&sa=...
    → https://boards.greenhouse.io/stripe/jobs
    A redirect shape on any other host is returned unchanged.
    """
    if not url:
        return url
    from urllib.parse import unquote
    redirect = _redirect_parts(url)
    if redirect and _is_google_origin(redirect[0]):
        return unquote(redirect[1])
    return url
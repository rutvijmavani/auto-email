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

    # SmartRecruiters — jobs.smartrecruiters.com/{slug}
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
    _LOCALE_PATTERN = re.compile(r'^[a-z]{2}[-_][A-Z]{2}$')

    def _workday_slug(m):
        raw_path = m.group(3).rstrip("/")
        parts    = [p for p in raw_path.split("/") if p]
        # Skip locale prefix (en-US, fr-FR, zh-CN, etc.)
        path = next(
            (p for p in parts if not _LOCALE_PATTERN.match(p)),
            parts[0] if parts else "careers"
        )
        return json.dumps({
            "slug": m.group(1).lower(),
            "wd":   m.group(2).lower(),
            "path": path or "careers",
        })

    patterns.append((
        re.compile(
            r"([a-z0-9]+)\.(wd\d+)\.myworkdayjobs\.com/([^?&#\s]*)",
            re.IGNORECASE
        ),
        "workday",
        _workday_slug,
    ))

    # Workday (alternate domain) — {wd}.myworkdaysite.com/recruiting/{tenant}/{career_site}
    # e.g. wd1.myworkdaysite.com/recruiting/fmr/FidelityCareers/...
    # API: wd1.myworkdaysite.com/wday/cxs/{tenant}/{career_site}/jobs
    _LOCALE_PATTERN2 = re.compile(r'^[a-z]{2}[-_][A-Z]{2}$')

    def _workdaysite_slug(m):
        wd     = m.group(1).lower()
        tenant = m.group(2).lower()
        parts  = [p for p in m.group(3).strip("/").split("/") if p]
        path   = next(
            (p for p in parts if not _LOCALE_PATTERN2.match(p)),
            parts[0] if parts else "careers"
        )
        return json.dumps({
            "slug": tenant,
            "wd":   wd,
            "path": path,
            "site": "myworkdaysite",  # flag different base URL
        })

    patterns.append((
        re.compile(
            r"(wd\d+)\.myworkdaysite\.com/"
            r"(?:[a-z]{2}[-_][A-Z]{2}/)?"   # optional locale: en-US, fr-FR, pt_BR
            r"recruiting/([a-z0-9]+)/([^?&#\s]*)",
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

    # SAP SuccessFactors — {company}.successfactors.com/careers
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

    # SAP SuccessFactors — career{dc}.successfactors.{region}/career?company=...
    # Also handles /careers path (SAP uses this)
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
        re.compile(r"(https?://[^\s]+(?:sitemap[^\s]*\.xml|feed\.xml))", re.IGNORECASE),
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


def match_ats_pattern(url):
    """
    Try all ATS URL patterns against a URL.
    Returns {platform, slug} or None.
    """
    if not url:
        return None

    # Decode Google redirect URLs
    url = _decode_google_redirect(url)

    for pattern, platform, extractor in ATS_URL_PATTERNS:
        m = pattern.search(url)
        if m:
            try:
                slug = extractor(m)
                if slug:
                    return {"platform": platform, "slug": slug}
            except Exception:
                continue

    return None


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


def _decode_google_redirect(url):
    """
    Handle Google redirect URLs:
    /url?q=https://boards.greenhouse.io/stripe/jobs&sa=...
    → https://boards.greenhouse.io/stripe/jobs
    """
    if not url:
        return url
    from urllib.parse import urlparse, parse_qs, unquote
    parsed = urlparse(url)
    if parsed.path == "/url":
        qs = parse_qs(parsed.query)
        if "q" in qs:
            return unquote(qs["q"][0])
    return url
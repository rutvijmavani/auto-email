# jobs/ats/patterns.py — ATS URL patterns for Google search detection
#
# Each pattern extracts platform + slug from a career page URL.
# Used by google_detector.py to identify ATS from Google search results.

import re
import json

# ─────────────────────────────────────────
# ATS SITE SEARCH QUERIES
# Order matters — try most common/reliable first
# Stop early on first high-confidence match
# ─────────────────────────────────────────

ATS_SITE_SEARCHES = [
    # platform          site: filter
    ("greenhouse",      "site:boards.greenhouse.io"),
    ("greenhouse",      "site:job-boards.greenhouse.io"),  # newer subdomain
    ("lever",           "site:jobs.lever.co"),
    ("ashby",           "site:jobs.ashbyhq.com"),
    ("smartrecruiters", "site:jobs.smartrecruiters.com"),
    ("workday",         "site:myworkdayjobs.com"),
    ("oracle_hcm",      "site:oraclecloud.com"),
    ("icims",           "site:icims.com careers"),  # careers-*.icims.com
    ("successfactors",  "site:successfactors.com"),
    ("successfactors",  "site:jobs2web.com"),
]

# ─────────────────────────────────────────
# URL PATTERNS
# Each entry: (compiled_regex, platform, slug_extractor_fn)
# ─────────────────────────────────────────

def _make_patterns():
    patterns = []


    # Greenhouse embed — job-boards.greenhouse.io/embed/job_board?for=Databricks
    patterns.append((
        re.compile(
            r"(?:boards|job-boards)\.greenhouse\.io/embed/job_board\?(?:.*&)?for=([^&\s]+)",
            re.IGNORECASE
        ),
        "greenhouse",
        lambda m: m.group(1).lower(),
    ))
    # Greenhouse — boards.greenhouse.io/{slug}/jobs
    # Also: job-boards.greenhouse.io/{slug}/jobs
    patterns.append((
        re.compile(
            r"(?:boards|job-boards)\.greenhouse\.io/([^/?&#\s]+)",
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
    # Also handles regional variants:
    #   hdpc.fa.us2.oraclecloud.com  (Goldman Sachs)
    #   jpmc.fa.oraclecloud.com      (JPMorgan — no region)
    patterns.append((
        re.compile(
            r"([a-z0-9][a-z0-9\-]*[a-z0-9])\.fa\.(?:ocs\.)?(?:(us\d+|eu\d+|ap\d+)\.)?"
            r"oraclecloud\.com/hcmUI/[^?#]*?/sites/([^/?&#\s]+)",
            re.IGNORECASE
        ),
        "oracle_hcm",
        lambda m: json.dumps({
            "slug":   m.group(1).lower(),
            "region": m.group(2).lower() if m.group(2) else "",
            "site":   m.group(3).rstrip("/"),
        }),
    ))

    # iCIMS — careers-{slug}.icims.com/jobs
    # Also: {slug}.icims.com/jobs
    # Note: "careers-" prefix stripped from slug
    patterns.append((
        re.compile(
            r"(?:careers-)?([a-z0-9][a-z0-9\-]*[a-z0-9]|[a-z0-9]+)"
            r"\.icims\.com(?:/jobs|$)",
            re.IGNORECASE
        ),
        "icims",
        lambda m: m.group(1).lower(),
    ))

    # Jobvite — jobs.jobvite.com/{slug}/job/{id}
    patterns.append((
        re.compile(r"jobs\.jobvite\.com/([^/?&#\s]+)/job/", re.IGNORECASE),
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
# jobs/ats/registry.py — Single source of truth for all ATS platforms.
#
# Adding a new ATS = add ONE entry here + create the module.
# No other shared file (job_monitor, ats_detector, pipeline) needs touching.
#
# Each entry defines:
#   module          Python module that implements the platform protocol
#   slug_type       "string" | "json"  — how the slug is stored in DB
#   detect_phases   Phases that can find this platform during ATS detection
#                   "sitemap"     → Phase 1 (ats_sitemap.py)
#                   "api"         → Phase 2 (ats_verifier.py name probe)
#                   "career_page" → Phase 3 (career_page.py HTML scan)
#   site_search     Google/Bing site: query used in career-page scan;
#                   None = platform not findable via search
#   listing_filter  "full"       → filter_jobs()  (title + location at listing)
#                   "title_only" → filter_jobs_title_only() (location deferred
#                                  to detail fetch — listing location absent/vague)
#   has_detail      True  → module exposes fetch_job_detail(job) -> dict
#                   False → all data available at listing stage
#   country_source  Documentation of where country data comes from:
#                   "text"       → plain-text location, use is_us_location()
#                   "alpha2"     → ISO alpha-2 code; module exposes
#                                  get_country_code(job) -> str
#                   "descriptor" → full country name embedded in location
#                                  string by the detail normaliser;
#                                  is_us_location() works fine

from jobs.ats import (
    greenhouse, lever, ashby, smartrecruiters, workday,
    oracle_hcm, icims, jobvite, avature, phenom,
    talentbrew, sitemap, successfactors, google,
    taleo, eightfold, jibe, custom_career, adp,
)

ATS_REGISTRY = {

    # ── Group A: listing-complete (title + location filter at listing stage) ──

    "greenhouse": {
        "module":          greenhouse,
        "slug_type":       "string",
        "detect_phases":   ["api"],
        "site_search":     "site:boards.greenhouse.io OR site:job-boards.greenhouse.io",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "text",
    },

    "lever": {
        "module":          lever,
        "slug_type":       "string",
        "detect_phases":   ["api"],
        "site_search":     "site:jobs.lever.co",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "text",
    },

    "ashby": {
        "module":          ashby,
        "slug_type":       "string",
        "detect_phases":   ["api"],
        "site_search":     "site:jobs.ashbyhq.com",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "text",
    },

    "oracle_hcm": {
        # PrimaryLocationCountry gives ISO alpha-2 on every listing ("US","TR","IN" …)
        # → listing-level alpha-2 gate in job_monitor drops non-US before filter_jobs().
        # listing_filter="full" still runs after the alpha-2 gate for title + text location.
        # NOTE: description is ShortDescriptionStr (teaser) — full description TBD.
        "module":          oracle_hcm,
        "slug_type":       "json",
        "detect_phases":   ["career_page"],
        "site_search":     "site:oraclecloud.com/hcmUI",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "alpha2",
    },

    "successfactors": {
        "module":          successfactors,
        "slug_type":       "json",
        "detect_phases":   ["api"],
        "site_search":     "site:successfactors.com OR site:jobs2web.com",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "text",
    },

    "google": {
        "module":          google,
        "slug_type":       "json",
        "detect_phases":   ["sitemap"],
        "site_search":     None,
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "text",
    },

    "jibe": {
        # country_code field in API response gives ISO alpha-2 ("US", "IN", "GB" …)
        # on every listing → listing-level alpha-2 gate drops non-US before filter_jobs().
        "module":          jibe,
        "slug_type":       "string",
        "detect_phases":   ["career_page"],
        "site_search":     None,
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "alpha2",
    },

    # ── Group B: detail-required, structured country code ────────────────────
    # listing_filter="full" here means location is reliable at listing level;
    # get_country_code() provides a belt-and-suspenders check after detail.

    "smartrecruiters": {
        # fullLocation in listing has complete country name → filter_jobs() works.
        # Detail fetch is for description only (does not change location).
        # get_country_code() uses the alpha-2 code stored at listing time as a
        # secondary gate to catch "Remote but country=IN" edge cases.
        "module":          smartrecruiters,
        "slug_type":       "string",
        "detect_phases":   ["api"],
        "site_search":     "site:jobs.smartrecruiters.com",
        "listing_filter":  "full",
        "has_detail":      True,
        "country_source":  "alpha2",
    },

    "icims": {
        # Listing location is always empty — must defer to detail.
        # JSON-LD addressCountry gives ISO alpha-2; module stores it as
        # _country_code and exposes get_country_code().
        "module":          icims,
        "slug_type":       "string",
        "detect_phases":   ["career_page"],
        "site_search":     "site:icims.com careers",
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "alpha2",
    },

    # ── Group C: detail-required, text-based country detection ───────────────

    "workday": {
        # locationsText ("2 Locations", "London") too vague for listing filter.
        # Detail provides jobRequisitionLocation.country.alpha2Code ("US"/"IN")
        # stored as _country_code — used as Tier 1 gate in job_monitor.
        # Falls back to is_us_location() on descriptor-embedded location string
        # for tenants that omit alpha2Code.
        "module":          workday,
        "slug_type":       "json",
        "detect_phases":   ["sitemap", "career_page"],
        "site_search":     "site:myworkdayjobs.com OR site:myworkdaysite.com",
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "alpha2",
    },

    "jobvite": {
        "module":          jobvite,
        "slug_type":       "string",
        "detect_phases":   ["career_page"],
        "site_search":     "site:jobs.jobvite.com",
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "text",
    },

    "avature": {
        "module":          avature,
        "slug_type":       "json",
        "detect_phases":   ["career_page"],
        "site_search":     None,
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "text",
    },

    "phenom": {
        # JSON-LD addressCountry gives full country name embedded in location
        # string → is_us_location() handles via Signal 4.
        "module":          phenom,
        "slug_type":       "json",
        "detect_phases":   ["api"],
        "site_search":     None,
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "descriptor",
    },

    "talentbrew": {
        "module":          talentbrew,
        "slug_type":       "json",
        "detect_phases":   ["career_page"],
        "site_search":     None,
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "descriptor",
    },

    "sitemap": {
        "module":          sitemap,
        "slug_type":       "json",
        "detect_phases":   ["sitemap"],
        "site_search":     None,
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "descriptor",
    },

    "taleo": {
        "module":          taleo,
        "slug_type":       "json",
        "detect_phases":   ["career_page"],
        "site_search":     None,
        "listing_filter":  "title_only",
        "has_detail":      True,
        "country_source":  "text",
    },

    "eightfold": {
        # standardizedLocations entries follow "City, State, CountryCode" format.
        # _extract_country_code() parses the trailing alpha-2 → _country_code.
        # listing-level alpha-2 gate drops non-US before any detail fetch.
        # "IN" (India) would otherwise conflict with Indiana in Signal 3 text scan.
        # Detail fetch is for description only (location/country set at listing).
        "module":          eightfold,
        "slug_type":       "json",
        "detect_phases":   ["career_page"],
        "site_search":     None,
        "listing_filter":  "full",
        "has_detail":      True,
        "country_source":  "alpha2",
    },

    "adp": {
        # Full description + location available at listing level — no detail fetch.
        # Location: "Charlotte, North Carolina, USA" / "Mississauga, Ontario, CAN".
        # country.codeValue is alpha-3 ("USA", "CAN") — get_country_code() converts
        # to alpha-2 ("US", "CA") for the listing-level country gate.
        "module":          adp,
        "slug_type":       "string",
        "detect_phases":   ["career_page", "api"],
        "site_search":     "site:myjobs.adp.com",
        "listing_filter":  "full",
        "has_detail":      False,
        "country_source":  "alpha2",
    },

    # ── Custom: catch-all for companies with curl-captured ATS configs ────────

    "custom": {
        "module":          custom_career,
        "slug_type":       "json",
        "detect_phases":   [],        # no auto-detection; curl must be captured
        "site_search":     None,
        "listing_filter":  "title_only",   # be conservative; varies per config
        "has_detail":      True,           # conditional on slug config
        "country_source":  "text",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Convenience accessors (used by job_monitor, ats_detector, pipeline, etc.)
# ─────────────────────────────────────────────────────────────────────────────

def get_module(platform: str):
    """Return the ATS module for platform, or None if unknown."""
    entry = ATS_REGISTRY.get(platform)
    return entry["module"] if entry else None


def get_config(platform: str) -> dict:
    """Return the full registry entry for platform, or {} if unknown."""
    return ATS_REGISTRY.get(platform, {})


def all_platforms() -> list:
    """Return list of all supported platform names."""
    return list(ATS_REGISTRY.keys())


def is_supported(platform: str) -> bool:
    """Return True if platform is in the registry."""
    return platform in ATS_REGISTRY

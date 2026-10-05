"""
jobs/pd_classify.py — Rule 1 vendor-signature classifier for public-domain probes.

Pure functions, no network and no DB: given the final HTTP response of a domain probe,
decide whether the domain is a real site, a parked / "no site here" placeholder, or a
bot-protection challenge. Ported 1:1 from data/parked_domain_scan_v4.py (the scan that
verified every signature by fetching live sample domains) so scan and production agree.

Rule 1 (explicit vendor signatures) is the ONLY thing that decides a verdict. Rule 2
(tiny-body heuristic) and Rule 3 (shared body hash) live in scripts/pd_candidates.py as
candidate generators that propose new signatures; a signature is added here only after
sample fetches confirm it.

Response dict contract (what classify() reads):
    status       int | None   final HTTP status, None = no response
    headers      dict         final response headers
    body         str          bounded body text
    final_url    str          final URL after redirects
    cookies      set[str]     cookie names seen on the chain
    error_type   str          set when status is None
    www_conflict str          platform-template guard, set by the fetch orchestration:
                              "" = www agrees / doesn't exist (template confirmed),
                              "www_differs" = www serves something else, "unchecked" = not yet checked
Verdicts: ok | parked | challenge | blocked | inconclusive | error.
"""
import hashlib
import re

from config import PD_SMALL_BODY_BYTES

WORKER_STATUSES = (403, 429, 503)          # statuses the CF Worker / relay tiers retry
INTERIM_STATUSES = (202,)                  # 2xx that is never "real" — CDN/WAF interim
PARKING_COOKIE_KEYS = frozenset({"lander_type", "traffic_target", "caf_ipaddr"})   # GoDaddy parkweb family
BLUEHOST_SUSPENDED_PATH = "/cgi-sys/suspendedpage.cgi"   # point-in-time: re-check near decision time
CPANEL_DEFAULT_PATH = "/cgi-sys/defaultwebpage.cgi"      # cPanel "no site here" page

# Headers that exist ONLY on a block/challenge response.
CHALLENGE_HEADERS_ALWAYS = frozenset({"sg-captcha", "x-px-access-denied"})
# Vendor-presence headers are stamped on EVERY response behind the vendor (scan v4: 280/288 x-iinfo
# rows and 92/285 x-sucuri-id rows were full 200 pages) — a challenge only when the response is not a full page.
VENDOR_PRESENCE_HEADERS = frozenset({"x-iinfo", "x-sucuri-id"})
WAF_ACTION_CHALLENGE = frozenset({"challenge", "captcha"})   # x-amzn-waf-action values

CHALLENGE_BODY_RE = re.compile(
    r"/\.well-known/sgcaptcha/"                            # SiteGround meta-refresh stub
    r"|<title>\s*just a moment\.{0,3}\s*</title>"          # Cloudflare managed challenge
    r"|checking your browser before accessing"             # DDoS-Guard / CF IUAM
    r"|<title>\s*checking your browser\.{0,3}\s*</title>"  # "Checking your browser... Javascript required"
    r"|<title>\s*security verification\s*</title>"         # shared 401 interstitial template
    r"|<title>\s*client challenge\s*</title>"              # JS-challenge interstitial, 200 + 3,036 bytes
    r"|sucuri_cloudproxy_js",                              # Sucuri JS interstitial
    re.IGNORECASE,
)
# Web-server "no site configured here" pages: the host answers but no company site is mounted on it.
UNCONFIGURED_BODY_RE = re.compile(
    r"this is the default server vhost"                    # nginx/Plesk default vhost
    r"|domain name is either not yet po",                  # "This site's domain name is either not yet pointed..."
    re.IGNORECASE,
)
# GoDaddy parkweb first-hop stub: the CF Worker never follows JS and returns no cookies, so the
# cookie-key rule can't fire on a Worker response; the stub body itself is the vendor signature.
PARKING_STUB_RE = re.compile(r'window\.location\.href\s*=\s*["\']/lander["\']', re.IGNORECASE)

# Hosting-platform "this domain has no site" templates. A bare-domain hit is only SOFT evidence (scan v4:
# 6 of 53 re-fetched domains showed one on the bare host yet served a real site on www), so the verdict
# stands only when www agrees or doesn't exist — the fetch orchestration sets res["www_conflict"].
PLATFORM_NO_SITE_RES = {
    "wix":          re.compile(r"<title>\s*connectyourdomain error", re.IGNORECASE),
    "squarespace":  re.compile(r"<title>\s*squarespace - (domain not claimed|website expired)", re.IGNORECASE),
    "zoho":         re.compile(r"<title>\s*zoho\s*</title>", re.IGNORECASE),
    "azure_webapp": re.compile(r"<title>\s*microsoft azure web app - error 404", re.IGNORECASE),
    "pantheon":     re.compile(r"<title>\s*(404 - unknown site|530 - site is frozen)", re.IGNORECASE),
    "vercel":       re.compile(r"<title>\s*(404: not_found|deployment paused)", re.IGNORECASE),
    "site_not_configured": re.compile(r"<title>\s*site not configured", re.IGNORECASE),
    "google_sites": re.compile(r"<title>\s*error 404 \(not found\)!!1", re.IGNORECASE),   # needs server: ghs
    "dreamhost":    re.compile(r"<title>\s*site not found\s*&middot;\s*dreamhost", re.IGNORECASE),   # answers 200
    "turbify":      re.compile(r"<title>\s*under construction\s*</title>.{0,1500}turbifycdn\.com",
                               re.IGNORECASE | re.DOTALL),   # Yahoo/Turbify hosting placeholder, answers 200
    # Identical 1,963-byte Apache file with the same Last-Modified on 9 unrelated domains + cert mismatch:
    # a hosting-provider default page. Vendor unnamed, matched by exact template (title + CSS marker).
    "shared_coming_soon": re.compile(r"<title>\s*coming soon\s*</title>.{0,1200}margin-top:\s*177px",
                                     re.IGNORECASE | re.DOTALL),
    "netlify":      re.compile(r"<title>\s*site not found\s*</title><style>:root\{--colorRgbFace", re.IGNORECASE),
    "cloudflare_1001": re.compile(r"<title>\s*dns resolution error\b", re.IGNORECASE),   # zone on CF, origin has no DNS
}
PLATFORM_STATUSES = (404, 409, 410, 530)
PLATFORM_200_NAMES = frozenset({"dreamhost", "turbify", "shared_coming_soon"})   # templates that answer 200
PLATFORM_REASON_PREFIX = "platform_no_site:"

# 429/503 = server trouble, not a deliberate refusal: a final 429/503 stores nothing (retried later).
# Only 403 means "real" (a live server answering for the domain).
RETRY_LATER_STATUSES = frozenset({429, 503})

# Registrable domains a company's own redirect never legitimately lands on. A cross-domain landing here
# stores nothing; anything else the owner redirects to is accepted (mmm.com -> 3m.com).
JUNK_LANDING_ROOTS = frozenset({
    "godaddy.com", "lucky.gives", "yahoo.com", "sharpschool.net", "amazonaws.com", "microsoftonline.com",
    "printersetupzone.com", "hugedomains.com", "dan.com", "afternic.com", "sedo.com", "sedoparking.com",
    "parkingcrew.net", "bodis.com",
})

_EXT_REF_RE = re.compile(r'<script[^>]*\bsrc=|<link[^>]*\brel=["\']?stylesheet', re.IGNORECASE)
_TITLE_RE = re.compile(r'<title[^>]*>(.*?)</title>', re.IGNORECASE | re.DOTALL)


def _lower_headers(headers) -> dict:
    return {str(k).lower(): v for k, v in (headers or {}).items()}


def _is_full_page(status, lh: dict, body: str) -> bool:
    """A 2xx response that is plainly a real page: oversized (body dropped) or not a tiny stub."""
    if status is None or not 200 <= status < 300:
        return False
    return "x-scan-too-large" in lh or len(body) >= PD_SMALL_BODY_BYTES


def challenge_reason(headers, body: str, final_root: str, status=None, challenge_domains=frozenset()) -> str:
    """Reason string if this response is a bot-protection challenge, else ''.

    final_root / challenge_domains: caller passes the registrable domain of the final URL and the
    vendor-domain set (jobs.public_domain._CHALLENGE_DOMAINS) so this module stays dependency-free.
    """
    lh = _lower_headers(headers)
    hit = CHALLENGE_HEADERS_ALWAYS & lh.keys()
    if hit:
        return f"header:{sorted(hit)[0]}"
    presence = VENDOR_PRESENCE_HEADERS & lh.keys()
    if presence and not _is_full_page(status, lh, body):
        return f"header:{sorted(presence)[0]}"
    if (lh.get("cf-mitigated", "") or "").strip().lower() == "challenge":
        return "cf_mitigated"
    if (lh.get("x-amzn-waf-action", "") or "").strip().lower() in WAF_ACTION_CHALLENGE:
        return "aws_waf"
    if final_root and final_root in challenge_domains:
        return "vendor_domain"
    if len(body) < PD_SMALL_BODY_BYTES * 8 and CHALLENGE_BODY_RE.search(body):
        return "body_signature"
    return ""


def platform_no_site(res: dict) -> str:
    """Name of the hosting platform whose 'no site here' template this response is, else ''."""
    status, body = res.get("status"), res.get("body") or ""
    if len(body) >= PD_SMALL_BODY_BYTES * 8 or not (status in PLATFORM_STATUSES or status == 200):
        return ""
    server = _lower_headers(res.get("headers")).get("server", "").lower()
    for name, pattern in PLATFORM_NO_SITE_RES.items():
        if status == 200 and name not in PLATFORM_200_NAMES:
            continue
        if pattern.search(body) and (name != "google_sites" or server == "ghs"):
            return name
    return ""


def classify(res: dict, final_root: str = "", challenge_domains=frozenset()) -> "tuple[str, str]":
    """-> (verdict, reason). verdict in parked | challenge | blocked | ok | inconclusive | error."""
    status = res.get("status")
    if status is None:
        return "error", res.get("error_type") or "no_response"
    body = res.get("body") or ""
    final_path = _path_of(res.get("final_url") or "")
    if BLUEHOST_SUSPENDED_PATH in final_path:
        return "parked", "bluehost_suspended"
    if PARKING_COOKIE_KEYS <= set(res.get("cookies") or ()):
        return "parked", "godaddy_parkweb"
    if len(body) < PD_SMALL_BODY_BYTES and PARKING_STUB_RE.search(body):
        return "parked", "godaddy_lander_stub"
    if CPANEL_DEFAULT_PATH in final_path:
        return "parked", "cpanel_default_page"
    if len(body) < PD_SMALL_BODY_BYTES * 8 and UNCONFIGURED_BODY_RE.search(body):
        return "parked", "unconfigured_default_vhost"
    platform = platform_no_site(res)
    if platform:
        # soft evidence until the orchestration has checked www (www_conflict "" = confirmed)
        conflict = res.get("www_conflict", "unchecked")
        if conflict == "":
            return "parked", PLATFORM_REASON_PREFIX + platform
        return "inconclusive", PLATFORM_REASON_PREFIX + platform + ":" + conflict
    why = challenge_reason(res.get("headers"), body, final_root, status, challenge_domains)
    if why:
        return "challenge", why
    if status in INTERIM_STATUSES:
        return "challenge", "interim_202_unrecognized"
    if status in WORKER_STATUSES:
        return "blocked", str(status)
    if 200 <= status < 300:
        return "ok", ""
    return "inconclusive", str(status)


def apply_outcome_rules(verdict: str, reason: str, host_root: str, final_root: str) -> "tuple[str, str, bool]":
    """Post-classification policy: retry-later mapping and the junk-landing deny list.

    -> (verdict, reason, cross_domain). A final 429/503 becomes inconclusive:retry_later (store nothing,
    retried by the existing retry gate); 403 stays blocked (= real, user decision 2026-10-04). An ok verdict
    that landed on a different registrable domain is flagged cross_domain, and a junk landing root stores nothing.
    """
    if verdict == "blocked" and reason.isdigit() and int(reason) in RETRY_LATER_STATUSES:
        verdict, reason = "inconclusive", f"retry_later:{reason}"
    cross = False
    if verdict == "ok" and final_root and final_root != host_root:
        cross = True
        if final_root in JUNK_LANDING_ROOTS:
            verdict, reason = "inconclusive", f"junk_landing:{final_root}"
    return verdict, reason, cross


def describe(res: dict, snippet_chars: int) -> dict:
    """Content fingerprint stored per probe (the Rule 2/3 mining inputs)."""
    body = res.get("body") or ""
    m = _TITLE_RE.search(body)
    return {
        "body_len": len(body),
        "ext_refs": len(_EXT_REF_RE.findall(body)),
        "body_hash": hashlib.sha256(body.strip().encode("utf-8", "ignore")).hexdigest(),
        "title": re.sub(r"\s+", " ", m.group(1)).strip()[:150] if m else "",
        "snippet": re.sub(r"\s+", " ", body).strip()[:snippet_chars],
    }


def _path_of(url: str) -> str:
    from urllib.parse import urlparse
    return urlparse(url).path or ""

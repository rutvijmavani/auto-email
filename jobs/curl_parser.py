r"""
jobs/curl_parser.py — Parse curl commands into replayable request configs.

Handles both listing and detail curls copied from Chrome DevTools.
Supports Windows ^ escaping and Linux/Mac \ escaping.

Public API:
    curl_to_slug_info(curl_string, career_page_url=None)
        Parse listing curl → slug_info dict

    parse_detail_curl(curl_string, listing_slug_info)
        Parse detail curl → detail config dict
        Auto-detects job_id location in URL
        Auto-detects GraphQL body structure

    extract_job_id_from_url(job_url, detail_config)
        Extract job_id from a listing href at runtime

    build_detail_url(detail_config, job)
        Build detail request URL by substituting job_id

    build_graphql_body(graphql_config, lsd, rev)
        Rebuild Meta-style GraphQL POST body with fresh tokens
"""

import re
import json
import shlex
from urllib.parse import urlparse, parse_qs, parse_qsl, urlunparse, quote_plus, unquote_plus
from jobs.utils import SKIP_HEADERS


# ─────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────

KNOWN_ID_PARAMS = {
    "id", "job_id", "jobid", "position_id", "positionid",
    "requisitionid", "req_id", "reqid", "jobno", "job_no",
    "contest_no", "contestno", "posting_id", "postingid",
    "jid", "j_id", "pid", "position",
}

DETAIL_NOISE_PARAMS = {
    "uclick_id", "uclick", "clickid", "click_id",
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "ref", "referrer", "source", "src",
    "_ga", "_gl", "gclid", "fbclid", "msclkid",
}

# GraphQL params that rotate per session — strip before storing
# Params that change every single request — never store, never reuse
GRAPHQL_NOISE_PARAMS = {
    "__req",        # request counter — increments per call
    "__s",          # session fingerprint — changes per session
    "__hsi",        # session ID — changes per session
    "s_ppvl", "s_ppv",  # analytics noise
}

# Params that change per deployment or session but are stable within
# a session — store from curl, reuse as-is, refresh when re-capturing
GRAPHQL_SEMI_STABLE_PARAMS = {
    "__dyn", "__csr", "__hsdp", "__hblp", "__sjsp",
    "__comet_req", "__hs", "__ccg", "__crn", "__jssesw",
    "dpr",
}

# SKIP_HEADERS imported from jobs.utils
from jobs.utils import SKIP_HEADERS


# ─────────────────────────────────────────
# PUBLIC API — LISTING CURL
# ─────────────────────────────────────────

def curl_to_slug_info(curl_string, career_page_url=None):
    """
    Parse a listing curl command into slug_info dict.

    career_page_url is stored so custom_career.py can refresh
    session cookies dynamically on every run. When provided,
    live cookies from the curl are discarded — they expire anyway.

    Returns dict with keys:
        url, method, params, headers, cookies, body,
        career_page_url, graphql_config (if GraphQL POST)
    """
    normalized = _normalize_curl(curl_string)
    tokens     = _tokenize(normalized)
    result     = _extract(tokens)

    result["career_page_url"] = career_page_url or None

    # Detect GraphQL and extract stable config
    if _is_graphql(result):
        result["graphql_config"] = _extract_graphql_config(result)
        result["body"]           = _clean_graphql_body(result.get("body", ""))

    # When career_page_url is present, fresh cookies are acquired dynamically
    # before every fetch, so live curl cookies are normally discarded.
    # Preserve them under _fallback_cookies so _build_legacy_session can use
    # them if the dynamic session warm-up fails (e.g. career page timeout).
    if career_page_url and result.get("cookies"):
        result["_fallback_cookies"] = result["cookies"]
        result["cookies"] = {}

    return result


# ─────────────────────────────────────────
# PUBLIC API — DETAIL CURL
# ─────────────────────────────────────────

def parse_detail_curl(curl_string, listing_slug_info):
    """
    Parse a detail curl and return detail config dict
    to be stored as slug_info["detail"].

    Automatically detects:
      - job_id location (path / query param / POST body)
      - URL template with {job_id} placeholder
      - GraphQL structure if POST to /graphql
      - Headers that differ from listing (stored as extras)

    Args:
        curl_string        -- detail curl from DevTools
        listing_slug_info  -- existing listing slug_info
                             (used to diff headers)

    Returns dict for slug_info["detail"].
    """
    normalized = _normalize_curl(curl_string)
    tokens     = _tokenize(normalized)
    parsed     = _extract(tokens)

    url     = parsed["url"]
    method  = parsed["method"]
    params  = parsed["params"]
    headers = parsed["headers"]
    body    = parsed.get("body")

    # Detect job_id + build URL template
    job_id, id_pattern, id_location = _detect_job_id_in_url(
        url, params, body
    )

    url_template, static_params, stored_body = _build_template(
        url, params, body, job_id, id_location,
        parsed.get("id_param_name")
    )

    # Find which query param holds the id (for query-param style)
    id_param = None
    if id_location == "query" and job_id:
        id_param = next(
            (k for k, v in params.items() if v == job_id), None
        )
    elif id_location == "body" and job_id and body:
        id_param = _find_id_param_in_body(body, job_id)

    # Only store headers that differ from listing headers
    listing_headers = listing_slug_info.get("headers", {})
    extra_headers = {
        k: v for k, v in headers.items()
        if k not in listing_headers or listing_headers[k] != v
    }

    # GraphQL detail
    graphql_config = None
    if _is_graphql(parsed):
        graphql_config = _extract_graphql_config(parsed)
        # Pass stored_body (not raw body) so the {job_id} placeholder written
        # by _build_template survives the noise-param cleanup.
        stored_body    = _clean_graphql_body(stored_body)

    detail = {
        "url_template": url_template,
        "method":       method,
        "headers":      extra_headers,
        "params":       static_params,
        "id_location":  id_location,
        "id_param":     id_param,
        "id_pattern":   id_pattern,
        "detected_id":  job_id,
        "listing_url_template": url_template,
        # Detected on first run, then cached:
        "format":       None,
        "object_path":  None,
        "field_map":    None,
    }

    if graphql_config:
        detail["graphql"] = graphql_config

    if stored_body:
        detail["body"] = stored_body

    return detail


def _build_template(url, params, body, job_id, id_location, id_param_name):
    """Build URL template and static params by removing job_id."""
    static_params = dict(params)
    stored_body   = body

    if not job_id:
        return url, static_params, stored_body

    if id_location == "path":
        # Replace job_id in URL path with placeholder
        url_template = url.replace(str(job_id), "{job_id}", 1)
        return url_template, static_params, stored_body

    elif id_location == "query":
        # Remove id param from static params — substituted at runtime
        id_param = next(
            (k for k, v in params.items() if v == str(job_id)), None
        )
        if id_param:
            static_params = {
                k: v for k, v in static_params.items()
                if k != id_param
            }
        return url, static_params, stored_body

    elif id_location == "body" and body:
        stored_body = body.replace(str(job_id), "{job_id}", 1)
        return url, static_params, stored_body

    return url, static_params, stored_body


# ─────────────────────────────────────────
# PUBLIC API — RUNTIME HELPERS
# ─────────────────────────────────────────

def extract_job_id_from_url(job_url, detail_config):
    """
    Extract job_id from a listing job_url using the stored pattern.
    Called at runtime to build detail request URLs.

    Returns job_id string or None.
    """
    if not job_url or not detail_config:
        return None

    pattern_name = detail_config.get("id_pattern")
    id_param     = detail_config.get("id_param")
    id_location  = detail_config.get("id_location")
    detected_id  = str(detail_config.get("detected_id", "") or "")

    parsed       = urlparse(job_url)
    path         = parsed.path
    query_params = {k: v[0] for k, v in
                    parse_qs(parsed.query, keep_blank_values=True).items()}

    # Query param style
    if id_location == "query" and id_param:
        return query_params.get(id_param)

    # Any known id query param
    for k, v in query_params.items():
        if k.lower().replace("_", "").replace("-", "") in {
            p.replace("_", "").replace("-", "") for p in KNOWN_ID_PARAMS
        }:
            return v

    # Path patterns
    if pattern_name == "last_segment":
        segments = [s for s in path.split("/") if s]
        if segments:
            last = segments[-1].split("?")[0]
            if re.match(r'^\d{4,}$', last):
                return last
            if detected_id and len(last) >= len(detected_id) * 0.5:
                return last

    elif pattern_name == "before_slug":
        m = re.search(r'/(\d{5,})/[a-z0-9\-]+', path)
        if m:
            return m.group(1)

    elif pattern_name == "after_double_dash":
        m = re.search(r'--(\d{5,})(?:/|$|\?)', path)
        if m:
            return m.group(1)

    elif pattern_name == "before_dash":
        m = re.search(r'/(\d{15,})-', path)
        if m:
            return m.group(1)

    elif pattern_name == "after_slug":
        m = re.search(r'/[a-z0-9\-]+-(\d{5,})(?:/|\?|$)', path)
        if m:
            return m.group(1)

    elif pattern_name == "uuid":
        m = re.search(
            r'/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
            r'-[0-9a-f]{4}-[0-9a-f]{12})(?:/|$)',
            path, re.I
        )
        if m:
            return m.group(1)

    # Fallback — try all numeric patterns
    nums = re.findall(r'\d{5,}', path)
    if nums:
        if detected_id:
            for n in nums:
                if abs(len(n) - len(detected_id)) <= 2:
                    return n
        return nums[0]

    return None


def build_detail_url(detail_config, job):
    """
    Build the detail request URL/params/body for a specific job.

    Returns (url, params, body) ready for requests.
    """
    template    = detail_config.get("url_template", "")
    id_loc      = detail_config.get("id_location", "path")
    id_param    = detail_config.get("id_param")
    base_params = dict(detail_config.get("params", {}))
    body        = detail_config.get("body")

    # Get job_id — from listing field_map, then URL extraction
    job_id = str(job.get("job_id") or "").strip()
    if not job_id:
        job_id = extract_job_id_from_url(
            job.get("job_url", ""), detail_config
        ) or ""

    if not job_id:
        return template, base_params, body

    if id_loc == "path":
        url = template.replace("{job_id}", job_id)
        return url, base_params, body

    elif id_loc == "query":
        url = template
        if id_param:
            base_params = dict(base_params)
            base_params[id_param] = job_id
        return url, base_params, body

    elif id_loc == "body":
        url = template
        if body:
            new_body = body.replace("{job_id}", job_id)
            try:
                body_data = json.loads(new_body)
                if id_param and id_param in body_data:
                    body_data[id_param] = job_id
                new_body = json.dumps(body_data)
            except (json.JSONDecodeError, TypeError):
                pass
            return url, base_params, new_body
        return url, base_params, body

    # Unknown — substitute in template as fallback
    return template.replace("{job_id}", job_id), base_params, body


# ─────────────────────────────────────────
# PUBLIC API — GRAPHQL
# ─────────────────────────────────────────

def build_graphql_body(graphql_config, lsd, rev):
    """
    Build a fresh GraphQL POST body using tokens extracted from
    the career page. Called on every run so body is never stale.

    Args:
        graphql_config -- stored graphql_config from slug_info
        lsd            -- CSRF token extracted from page HTML
        rev            -- build revision extracted from page JS

    Returns form-encoded POST body string or JSON string (depending on is_json_body).
    """
    import time as _time

    is_json_body = graphql_config.get("is_json_body", False)
    jazoest = compute_jazoest(lsd) if lsd else "22348"

    # Layer 1 — semi-stable params from curl (e.g. __dyn, __csr, __hs)
    # These are session/deployment specific, stored at curl capture time,
    # refreshed when user re-captures curl via --sync-prospective
    fields = {}
    stable = graphql_config.get("stable_params", {})
    fields.update(stable)

    # Layer 2 — always-present structural fields (override stable)
    fields.update({
        "av":           "0",
        "__user":       "0",
        "__a":          "1",
        "fb_api_caller_class":      "RelayModern",
        "fb_api_req_friendly_name": graphql_config.get("friendly_name", ""),
        "server_timestamps":        "true",
    })

    # Layer 3 — dynamic fields rebuilt fresh on every run (override all)
    fields.update({
        "lsd":      lsd or "",
        "jazoest":  jazoest,
        "__rev":    str(rev) if rev else "",
        "__spin_r": str(rev) if rev else "",
        "__spin_b": "trunk",
        "__spin_t": str(int(_time.time())),
    })

    # Layer 4 — query definition (override all)
    variables = graphql_config.get("variables")
    if variables:
        if is_json_body:
            # For JSON body, store variables as object
            fields["variables"] = variables
        else:
            # For form-encoded, store as JSON string
            try:
                fields["variables"] = json.dumps(variables)
            except (TypeError, ValueError):
                pass

    if graphql_config.get("doc_id"):
        fields["doc_id"] = str(graphql_config["doc_id"])

    # Return JSON string for JSON bodies
    if is_json_body:
        return json.dumps(fields)

    # Return form-encoded string for form bodies
    return "&".join(
        f"{quote_plus(k)}={quote_plus(str(v))}"
        for k, v in fields.items()
        if v is not None and str(v) != ""
    )


def compute_jazoest(lsd):
    """Compute Meta's jazoest checksum from lsd CSRF token."""
    return str(sum(ord(c) + 2 for c in (lsd or "")))


# ─────────────────────────────────────────
# INTERNAL — JOB ID DETECTION
# ─────────────────────────────────────────

def _find_id_in_nested_structure(data, depth=0, max_depth=5):
    """
    Recursively search for job_id in nested dicts/lists.
    Returns (value, field_name, location_type) or (None, None, None).
    """
    if depth > max_depth:
        return None, None, None

    # Normalize known ID params for comparison
    normalized_id_params = {p.replace("_", "").replace("-", "") for p in KNOWN_ID_PARAMS}

    if isinstance(data, dict):
        for k, v in data.items():
            norm = k.lower().replace("_", "").replace("-", "")
            if norm in normalized_id_params:
                return str(v), k, "body"
            # Recurse into nested structures
            if isinstance(v, (dict, list)):
                result, field, loc = _find_id_in_nested_structure(v, depth + 1, max_depth)
                if result:
                    return result, field, loc
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                result, field, loc = _find_id_in_nested_structure(item, depth + 1, max_depth)
                if result:
                    return result, field, loc

    return None, None, None


def _detect_job_id_in_url(url, params, body=None):
    """
    Detect job_id in URL path, query params, or POST body.
    Returns (job_id, pattern_name, location).
    """
    parsed = urlparse(url)
    path   = parsed.path

    # Check known query params
    for k, v in params.items():
        norm = k.lower().replace("_", "").replace("-", "")
        if norm in {p.replace("_", "").replace("-", "")
                    for p in KNOWN_ID_PARAMS}:
            return v, "query_param", "query"

    # Path patterns in priority order
    patterns = [
        # Long numeric (15+ digits) before dash (Google style)
        (r'/(\d{15,})-[a-z]',                           "before_dash"),
        # Numeric before slug segment
        (r'/(\d{5,})/[a-z0-9\-]+(?:/|\?|$)',            "before_slug"),
        # After double dash (Tesla)
        (r'--(\d{5,})(?:/|\?|$)',                        "after_double_dash"),
        # After slug with dash separator (Wayfair)
        (r'/[a-z][a-z0-9\-]+-(\d{5,})(?:\?|$|/)',       "after_slug"),
        # Clean numeric last segment — trailing slash OR end OR query (Uber, Siemens, Apple)
        (r'/(\d{5,})(?:/|\?|$)',                         "last_segment"),
        # UUID last segment
        (r'/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}'
         r'-[0-9a-f]{4}-[0-9a-f]{12})(?:/|\?|$)',        "uuid"),
        # Alphanumeric last segment (6+ chars)
        (r'/([A-Za-z0-9_\-]{6,})(?:\?|$)',               "last_segment"),
    ]

    for pattern, pattern_name in patterns:
        m = re.search(pattern, path, re.IGNORECASE)
        if m:
            return m.group(1), pattern_name, "path"

    # Check POST body for id field (including nested structures)
    if body:
        # Try JSON body first
        try:
            data = json.loads(body)
            result, field, loc = _find_id_in_nested_structure(data)
            if result:
                return result, "body_field", loc
        except (json.JSONDecodeError, TypeError):
            pass

        # Fall back to form-encoded body (e.g. Meta GraphQL uses
        # application/x-www-form-urlencoded with a JSON "variables" value)
        try:
            fields = dict(parse_qsl(body, keep_blank_values=False))
            if fields:
                # First: look for direct ID fields in the top-level form params
                norm_fields = {k.lower().replace("_", "").replace("-", ""): v
                               for k, v in fields.items()}
                for param in KNOWN_ID_PARAMS:
                    norm = param.replace("_", "").replace("-", "")
                    if norm in norm_fields:
                        return norm_fields[norm], "body_field", "body"

                # Second: look for values that are JSON strings (e.g. variables)
                for k, v in fields.items():
                    if v and v.strip().startswith(("{", "[")):
                        try:
                            nested = json.loads(v)
                            result, field, loc = _find_id_in_nested_structure(nested)
                            if result:
                                return result, "body_field", "body"
                        except (json.JSONDecodeError, TypeError):
                            pass
        except Exception:
            pass

    return None, None, None


def _find_id_param_in_body(body, job_id):
    """Find which field in a JSON body contains the job_id (including nested)."""
    try:
        data = json.loads(body)

        def search_nested(obj, depth=0, max_depth=5):
            if depth > max_depth:
                return None
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if str(v) == str(job_id):
                        return k
                    if isinstance(v, (dict, list)):
                        result = search_nested(v, depth + 1, max_depth)
                        if result:
                            return result
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, (dict, list)):
                        result = search_nested(item, depth + 1, max_depth)
                        if result:
                            return result
            return None

        return search_nested(data)
    except (json.JSONDecodeError, TypeError):
        pass
    return None


# ─────────────────────────────────────────
# INTERNAL — GRAPHQL
# ─────────────────────────────────────────

def _is_graphql(parsed):
    """Detect if curl is a GraphQL request."""
    url  = parsed.get("url", "")
    body = parsed.get("body", "") or ""
    if "/graphql" in url.lower():
        return True
    if "doc_id" in body or "fb_api_req_friendly_name" in body:
        return True
    if "variables" in body and ("query" in body or "doc_id" in body):
        return True
    return False


def _extract_graphql_config(parsed):
    """
    Extract stable GraphQL config from curl.
    Strips rotating session params.
    """
    body = parsed.get("body", "") or ""
    url  = parsed.get("url", "")

    config = {
        "doc_id":        None,
        "friendly_name": None,
        "variables":     None,
        "endpoint":      url,
        "stable_params": {},
        "is_json_body":  False,
    }

    # Try JSON body first (other GraphQL APIs)
    stripped = body.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                config["doc_id"]        = data.get("doc_id") or data.get("docid")
                config["friendly_name"] = (data.get("operationName") or
                                           data.get("fb_api_req_friendly_name"))
                config["variables"]     = data.get("variables")
                config["is_json_body"]  = True
                config["stable_params"] = {
                    k: v for k, v in data.items()
                    if k not in ("variables", "query", "lsd", "extensions")
                    and not k.startswith("__")
                }
                return config
        except (json.JSONDecodeError, TypeError):
            pass

    # Form-encoded body (Meta style)
    try:
        fields = {}
        for part in body.split("&"):
            if not part or "=" not in part:
                continue
            k, _, v = part.partition("=")
            fields[unquote_plus(k)] = unquote_plus(v)

        # Only proceed if we actually parsed fields
        if not fields:
            return config

        if "doc_id" in fields:
            config["doc_id"] = fields["doc_id"]
        if "fb_api_req_friendly_name" in fields:
            config["friendly_name"] = fields["fb_api_req_friendly_name"]
        if "variables" in fields:
            try:
                config["variables"] = json.loads(fields["variables"])
            except (json.JSONDecodeError, TypeError):
                config["variables"] = fields["variables"]

        # Always-excluded: rotating tokens rebuilt fresh every run
        ALWAYS_EXCLUDE = {
            "lsd", "jazoest", "av", "__user", "__a",
            "server_timestamps", "fb_api_caller_class",
            "fb_api_req_friendly_name", "variables", "doc_id",
            "__spin_r", "__spin_b", "__spin_t",
        }

        stable = {}
        for k, v in fields.items():
            if k in ALWAYS_EXCLUDE:
                continue
            if k in GRAPHQL_NOISE_PARAMS:
                continue
            # Store semi-stable __ params (e.g. __dyn, __csr, __hs)
            # and all regular params — these come from curl and are
            # refreshed when the user re-captures curl
            stable[k] = v

        config["stable_params"] = stable
        return config
    except Exception:
        pass

    return config


def _clean_graphql_body(body):
    """Remove rotating params from GraphQL body before storing."""
    if not body:
        return body
    # JSON bodies must not be split on '&' — return as-is
    stripped = body.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            import json as _json
            _json.loads(stripped)
            return body
        except (ValueError, TypeError):
            pass

    # Only process as form-encoded if it doesn't look like JSON
    try:
        fields = {}
        for part in body.split("&"):
            if not part or "=" not in part:
                continue
            k, _, v = part.partition("=")
            fields[unquote_plus(k)] = unquote_plus(v)

        # If no fields parsed, body wasn't form-encoded — return as-is
        if not fields:
            return body

        clean = {
            k: v for k, v in fields.items()
            if k not in GRAPHQL_NOISE_PARAMS
            and k not in ("lsd", "jazoest", "__spin_t",
                          "__spin_r", "__spin_b", "__rev",
                          "av", "__user", "__a")
        }
        result = "&".join(
            f"{quote_plus(k)}={quote_plus(v)}"
            for k, v in clean.items()
        )
        # quote_plus encodes literal '{' and '}' as %7B/%7D, so the
        # {job_id} placeholder written by _build_template becomes
        # %7Bjob_id%7D — restore it so build_detail_url can substitute.
        result = result.replace("%7Bjob_id%7D", "{job_id}")
        return result
    except Exception:
        return body


# ─────────────────────────────────────────
# INTERNAL — CURL PARSING
# ─────────────────────────────────────────

def _normalize_curl(s):
    """Convert any curl format to clean single-line string."""
    s = s.strip()

    if any(x in s for x in ("^\n", "^ \n", "^\r\n", "^ \r\n")):
        s = re.sub(r'\^ *\r?\n\s*', ' ', s)
        s = s.replace('^^', '\x00')
        s = s.replace('^"', '"')
        s = re.sub(r'\^(.)', r'\1', s)
        s = s.replace('\x00', '^')
    elif re.search(r'\^ {2,}-', s) or re.search(r'\^ {2,}curl', s):
        # Google Form strips newlines — Windows ^ continuation
        # becomes "^   -H" (caret + spaces + flag, no newline)
        s = re.sub(r'\^ +', ' ', s)
        s = s.replace('^^', '\x00')
        s = s.replace('^"', '"')
        s = re.sub(r'\^(.)', r'\1', s)
        s = s.replace('\x00', '^')
    elif "\\\n" in s:
        s = re.sub(r'\\\n\s*', ' ', s)

    return re.sub(r'  +', ' ', s).strip()


def _tokenize(s):
    s = (s.replace('\u2018', "'").replace('\u2019', "'")
          .replace('\u201c', '"').replace('\u201d', '"'))
    try:
        return shlex.split(s)
    except ValueError:
        return _basic_split(s)


def _basic_split(s):
    tokens, current, quote = [], [], None
    for ch in s:
        if ch in ('"', "'") and quote is None:
            quote = ch
        elif ch == quote:
            quote = None
        elif ch == ' ' and quote is None:
            if current:
                tokens.append(''.join(current))
                current = []
        else:
            current.append(ch)
    if current:
        tokens.append(''.join(current))
    return tokens


def _extract(tokens):
    url, method, body = None, None, None
    headers, cookies  = {}, {}

    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if tok.lower() == 'curl':
            i += 1; continue

        if url is None and (
            tok.startswith('http') or
            tok.startswith('"http') or
            tok.startswith("'http")
        ):
            url = tok.strip("'\""); i += 1; continue

        if tok == '--url':
            if i + 1 < len(tokens):
                url = tokens[i + 1].strip("'\""); i += 2
            continue

        if tok in ('-X', '--request'):
            if i + 1 < len(tokens):
                method = tokens[i + 1].upper().strip("'\""); i += 2
            continue

        if tok in ('-H', '--header'):
            if i + 1 < len(tokens):
                _parse_header(tokens[i + 1], headers); i += 2
            continue

        if tok in ('-b', '--cookie'):
            if i + 1 < len(tokens):
                _parse_cookies(tokens[i + 1], cookies); i += 2
            continue

        if tok in ('-d', '--data', '--data-raw',
                   '--data-binary', '--data-urlencode'):
            if i + 1 < len(tokens):
                body = tokens[i + 1]
                if method is None:
                    method = 'POST'
                i += 2
            continue

        if tok.startswith('-'):
            nxt = tokens[i + 1] if i + 1 < len(tokens) else ''
            if nxt and not nxt.startswith('-') and not nxt.startswith('http'):
                i += 2
            else:
                i += 1
            continue

        i += 1

    if not url:
        raise ValueError(
            "Could not extract URL from curl command. "
            "Ensure curl starts with the URL or use --url flag."
        )

    parsed   = urlparse(url)
    params   = {k: v[0] for k, v in
                parse_qs(parsed.query, keep_blank_values=True).items()}
    base_url = urlunparse(parsed._replace(query='', fragment=''))

    if method is None:
        method = 'POST' if body else 'GET'

    if 'cookie' in headers:
        _parse_cookies(headers.pop('cookie'), cookies)

    return {
        "url":     base_url,
        "method":  method,
        "params":  params,
        "headers": headers,
        "cookies": cookies,
        "body":    body,
    }


def _parse_header(s, headers):
    if ':' in s:
        name, _, value = s.partition(':')
        key = name.strip().lower()
        # Drop HTTP/2 pseudo-headers (any :xxx) and hop-by-hop transport
        # headers (host, content-length, etc.) — replaying them causes errors.
        if not key.startswith(':') and key not in SKIP_HEADERS:
            headers[key] = value.strip()


def _parse_cookies(s, cookies):
    for part in s.split(';'):
        part = part.strip()
        if '=' in part:
            name, _, value = part.partition('=')
            cookies[name.strip()] = value.strip()
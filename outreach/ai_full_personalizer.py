# ai_full_personalizer.py

import os
import json
import hashlib
import re
from google import genai
from dotenv import load_dotenv
from db.quota_manager import can_call, increment_usage, all_models_exhausted
from db.quota import within_rpm
from db.db import get_ai_cache, save_ai_cache
import time

load_dotenv()

_client = None


def _get_client():
    """Lazy-initialize Gemini client. Returns None if API key is not set."""
    global _client
    if _client is not None:
        return _client
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("[WARNING] GEMINI_API_KEY not set. AI generation will be skipped.")
        return None
    _client = genai.Client(api_key=api_key)
    return _client

# -----------------------
# CONFIG
# -----------------------

PRIMARY_MODEL = "gemini-2.5-flash-lite"
FALLBACK_MODEL = "gemini-2.5-flash"
FIELD_MAP_MODEL = "gemma-4-31b-it"
CACHE_TTL_DAYS = 21


# -----------------------
# CACHE KEYS
# -----------------------

def _cache_key(company, job_title, job_text):
    """Cache key for full JD-based generation."""
    raw = f"{company}-{job_title}-{job_text}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _fallback_cache_key(company, job_title):
    """Separate cache key for fallback generation (no JD)."""
    raw = f"fallback-{company}-{job_title}"
    return hashlib.sha256(raw.encode()).hexdigest()


# -----------------------
# SHARED AI CALL
# -----------------------

def _call_model(prompt, cache_key, company, job_title):
    """
    Call Gemini with the given prompt.
    Saves result to ai_cache and returns the parsed data dict.
    Returns {} if all models exhausted or both fail.
    """
    if all_models_exhausted():
        print("All model quotas exhausted for today.")
        return {}

    client = _get_client()
    if client is None:
        return {}

    for model in [PRIMARY_MODEL, FALLBACK_MODEL]:
        if not can_call(model):
            if not within_rpm(model):
                print(f"{model} RPM limit hit — waiting 60s...")
                time.sleep(60)
                if not can_call(model):
                    print(f"{model} still unavailable after wait — trying next model.")
                    continue
            else:
                print(f"{model} daily limit reached (local guard).")
                continue

        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt
            )

            increment_usage(model)

            text = response.text.strip()

            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                raise ValueError("No JSON found in response")

            data = json.loads(match.group(0))

            save_ai_cache(cache_key, company, job_title, data, ttl_days=CACHE_TTL_DAYS)

            print(f"Generated using model: {model}")
            return data

        except Exception as e:
            print(f"{model} failed: {e}")
            continue

    print("Both models failed.")
    return {}


# -----------------------
# MAIN GENERATION FUNCTION (with JD)
# -----------------------

def generate_all_content(company, job_title, job_text):
    """
    Generate personalized email content using the full job description.
    Falls back to generate_all_content_without_jd() if job_text is empty.
    """
    if not job_text:
        print(f"[WARNING] No job description available for {company}. Using role-based fallback.")
        return generate_all_content_without_jd(company, job_title)

    key = _cache_key(company, job_title, job_text)

    cached = get_ai_cache(key)
    if cached:
        print("Using cached AI content")
        return cached

    prompt = f"""
You are helping me as a software engineer write a short outreach email based on job description of a particular role at particular company.
Kindly generate 3 different subjects lines for initial mail , followup email 1 and followup email 2 based on job title and store them into subject_initial , subject_followup1 , subject_followup2 respectively. 
Kindly generate 3 different bodies for initial mail , followup email 1 and followup email 2 based on job description of a particular role at particular company and store them into intro , followup1 and followup2 respecively.

Company: {company}
Job Title: {job_title}

Job Description:
{job_text[:4000]}

Candidate Background:
- Backend Software Engineer
- Python & Go
- Microservices
- Kubernetes
- PostgreSQL optimization
- CI/CD pipelines
- Distributed systems

Return STRICT JSON in this format:

{{
  "subject_initial": "...",
  "subject_followup1": "...",
  "subject_followup2": "...",
  "intro": "...",
  "followup1": "...",
  "followup2": "..."
}}

Rules:
- Professional tone
- No emojis
- Each body under 120 words
- Subject lines under 10 words
- Return ONLY valid JSON
- Do not include greeting in subject.
- Write 3 concise professional sentences explaining why I am a strong fit in a body.
"""

    return _call_model(prompt, key, company, job_title)


# -----------------------
# FALLBACK GENERATION (without JD)
# -----------------------

def generate_all_content_without_jd(company, job_title):
    """
    Generate role-specific email content using only company name and job title.
    Used when job description scraping fails.
    Cached separately from JD-based content.
    """
    key = _fallback_cache_key(company, job_title)

    cached = get_ai_cache(key)
    if cached:
        print("Using cached fallback AI content")
        return cached

    prompt = f"""
You are helping a software engineer write a short cold outreach email.
No job description is available, but generate professional and specific outreach
based on the typical requirements and responsibilities for this role at this company.
Research what this company is known for technically and tailor the email accordingly.

Company: {company}
Job Title: {job_title}

Candidate Background:
- Backend Software Engineer
- Python & Go
- Microservices
- Kubernetes
- PostgreSQL optimization
- CI/CD pipelines
- Distributed systems

Generate 3 subject lines and 3 email bodies for:
- Initial outreach email
- First follow-up email
- Second follow-up email

Return STRICT JSON in this format:

{{
  "subject_initial": "...",
  "subject_followup1": "...",
  "subject_followup2": "...",
  "intro": "...",
  "followup1": "...",
  "followup2": "..."
}}

Rules:
- Professional tone
- No emojis
- Mention the company name and role specifically
- Each body under 120 words
- Subject lines under 10 words
- Return ONLY valid JSON
- Do not include greeting in subject.
- Write 3 concise professional sentences explaining why I am a strong fit based on typical requirements for this role.
"""

    print(f"[INFO] Generating fallback AI content for {company} | {job_title} (no JD available)")
    return _call_model(prompt, key, company, job_title)


# -----------------------
# FIELD MAP DETECTION (custom ATS)
# -----------------------

def detect_field_map_with_ai(company, first_job_raw, base_url,
                              full_response=None , sample_job_url=None):
    """
    Use Gemma to identify field mapping from a raw job API response dict,
    and optionally the total job count field from the full response envelope.
    Both results come from a single AI call — one quota hit.

    Returns:
        (field_map, total_field, ai_available)

        field_map:     dict like {"title": "jobTitle", ...} or None
        total_field:   dot-separated path like "data.totalResults" or None
        ai_available:  True if the AI call was attempted and completed
                       (even if it returned nulls for some fields).
                       False if quota exhausted, client unavailable, or
                       an unhandled exception prevented the call.

    Callers use ai_available to decide whether to run pattern fallbacks:
      - ai_available=True  → trust results as-is; null means genuinely absent
      - ai_available=False → run pattern detection for both fields
    """

    if not first_job_raw or not isinstance(first_job_raw, dict):
        return None, None, False

    # ── Quota / availability checks ──────────────────────────────
    if not can_call(FIELD_MAP_MODEL):
        print(f"[INFO] {FIELD_MAP_MODEL} daily limit reached — "
              f"skipping AI field map for {company}")
        return None, None, False

    if not within_rpm(FIELD_MAP_MODEL):
        print(f"[INFO] {FIELD_MAP_MODEL} RPM limit hit — "
              f"waiting 60s for field map ({company})...")
        time.sleep(60)
        if not can_call(FIELD_MAP_MODEL):
            print(f"[INFO] {FIELD_MAP_MODEL} still unavailable — "
                  f"skipping AI field map for {company}")
            return None, None, False

    client = _get_client()
    if client is None:
        return None, None, False

    try:
        print(f"[AI DEBUG] building truncated preview...")
        # ── Build job dict preview ───────────────────────────────────
        truncated = {}
        for k, v in first_job_raw.items():
            if isinstance(v, str) and len(v) > 500:
                truncated[k] = v[:500] + "...[truncated]"
            else:
                truncated[k] = v
        raw_preview = json.dumps(truncated, indent=2)

        sample_url_section = ""
        if sample_job_url and sample_job_url.strip():
            sample_url_section = f"""
        Sample job URL (from clicking a job on the listing page):
        {sample_job_url.strip()}

        Use this URL to identify the correct job_id field. Find which field in the
        raw job dict above appears in this URL path. That field is the job_id.
        Also use this URL to construct the job_url_template by replacing the
        specific ID value with {{job_id}} placeholder.
        """

        # ── Build total-field section (only when full_response provided) ──
        total_section           = ""
        total_field_instruction = ""

        if full_response and isinstance(full_response, dict):
            def _flatten_top(obj, depth=0, prefix=""):
                if depth > 3 or not isinstance(obj, dict):
                    return {}
                result = {}
                for k, v in obj.items():
                    path = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, int):
                        result[path] = v
                    elif isinstance(v, dict):
                        result[path] = v          # include raw dict (protobuf Longs)
                        result.update(_flatten_top(v, depth + 1, path))
                    elif isinstance(v, list):
                        result[path] = f"[list of {len(v)} items]"
                return result

            flat         = dict(list(_flatten_top(full_response).items())[:40])
            flat_preview = json.dumps(flat, indent=2, default=str)

            total_section = f"""
    ---

    PART 2 — Total job count field

    Below is a flattened view of the full API response envelope (field path -> value).
    Identify which field contains the TOTAL number of jobs available across ALL pages
    (not just this page). It may be a plain integer, or a protobuf-style Long like
    {{"low": 1287, "high": 0, "unsigned": false}} where the true count is `low`.

    {flat_preview}

    Add "total_field" to your JSON with the dot-separated path (e.g. "data.totalResults").
    If you cannot identify it with confidence, set it to null.
    """
            total_field_instruction = ', "total_field": "dot.separated.path_or_null"'
        

        # ── Prompt ───────────────────────────────────────────────────
        prompt = f"""You are analyzing a raw job posting dict returned by a company career API.
        Your task is to identify the exact dot-separated path to each standard field's scalar value.

        Company: {company}
        Base URL: {base_url}

        Raw job dict (first job from listing):
        {raw_preview}
        {sample_url_section}

        For each standard field, return the dot-separated path to the actual scalar value.
        Walk into nested objects to find human-readable values, not codes or IDs.

        Standard fields:
        - title: job title string (e.g. "title" or "position.name")
        - job_url: URL/path string to job detail page (null if absent)
        - location_city: city name string (e.g. "city_info.en_name" or "location.city")
        - location_state: state/province/region name string (e.g. "city_info.parent.en_name")
        Return null if state is same value as city or country, or genuinely absent.
        - location_country: country name string (e.g. "city_info.parent.parent.en_name")
        Return null if country is same value as city, or genuinely absent.
        - posted_at: post date string or unix timestamp (prefer creation over update date)
        - job_id: unique external-facing ID string or number (e.g. "code" or "id")
        - description: full job description text if present in listing (null if absent)
        - job_url_template: if job_url is null but job_id exists, and you can infer the
        job detail URL pattern from the base_url or company name, provide the template
        with {{job_id}} placeholder (e.g. "https://lifeattiktok.com/search/{{job_id}}").
        Only include if confident. Otherwise null.

        Rules:
        - Return EXACT dot-separated path to the scalar value, not the container object
        - If city and state resolve to the same string, set location_state to null
        - If city and country resolve to the same string, set location_country to null
        - job_id: prefer alphanumeric external IDs (e.g. "A201789A") over large numeric IDs
        - description: only map if the field contains substantial text (>100 chars typical)
        - Return ONLY valid JSON, no explanation, no markdown backticks
        {total_section}
        {{"title": "path_or_null", "job_url": "path_or_null", "location_city": "path_or_null", "location_state": "path_or_null", "location_country": "path_or_null", "posted_at": "path_or_null", "job_id": "path_or_null", "description": "path_or_null", "job_url_template": "template_or_null"{total_field_instruction}}}"""
        # ── Call model ───────────────────────────────────────────────
        try:
            print(f"[AI DEBUG] calling model...")
            response = client.models.generate_content(
                model=FIELD_MAP_MODEL,
                contents=prompt,
            )
            print(f"[AI DEBUG] response received: {response.text[:200]!r}")
            increment_usage(FIELD_MAP_MODEL)

            text  = response.text.strip()
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if not match:
                raise ValueError("No JSON found in response")

            data = json.loads(match.group(0))

            # ── Validate field map ───────────────────────────────────
            # title must exist in the actual job dict — if not, the AI
            # hallucinated a field name. Discard field_map but keep
            # ai_available=True so pattern fallback is NOT triggered.
            from jobs.ats.custom_career import _walk_path
            TEMPLATE_KEY = "job_url_template"
            TOTAL_KEY    = "total_field"

            field_map    = None
            job_url_template = None

            if data.get("title"):
                title_val = _walk_path(first_job_raw, data["title"])
                if title_val is not None:
                    validated = {}
                    for k, path in data.items():
                        if k in (TOTAL_KEY, TEMPLATE_KEY) or path is None:
                            continue
                        val = _walk_path(first_job_raw, path)
                        if val is not None:
                            validated[k] = path
                        else:
                            logger.debug(
                                "detect_field_map_with_ai: path %r "
                                "for %r not found in job for %r — dropping",
                                path, k, company
                            )
                    if validated.get("title"):
                        field_map = validated

                    # Validate job_url_template has {job_id} placeholder
                    tmpl = data.get(TEMPLATE_KEY)
                    if tmpl and isinstance(tmpl, str) and "{job_id}" in tmpl:
                        job_url_template = tmpl

            # Validate total_field path exists in full_response
            total_field = None
            if full_response and data.get(TOTAL_KEY):
                path  = data[TOTAL_KEY]
                parts = path.lstrip(".").split(".")
                obj   = full_response
                valid = True
                for part in parts:
                    if isinstance(obj, dict) and part in obj:
                        obj = obj[part]
                    else:
                        valid = False
                        break
                if valid:
                    total_field = path

            print(f"[INFO] AI detection for {company} using {FIELD_MAP_MODEL}: "
                f"field_map={field_map} total_field={total_field!r} "
                f"url_template={job_url_template!r}")

            return field_map, total_field, job_url_template, True

        except Exception as e:
            print(f"[AI DEBUG] EXCEPTION: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None, False
    except Exception as e:
        print(f"[AI DEBUG] EXCEPTION at some step: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return None, None, False
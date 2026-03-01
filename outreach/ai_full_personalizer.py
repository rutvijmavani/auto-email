# ai_full_personalizer.py

import os
import json
import hashlib
import re
from google import genai
from dotenv import load_dotenv
from db.quota_manager import can_call, increment_usage, all_models_exhausted
from db.db import get_ai_cache, save_ai_cache

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
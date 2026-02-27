# ai_personalizer.py

import os
import json
import hashlib
from datetime import datetime, timedelta
from google import genai
from dotenv import load_dotenv
import re

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file")

client = genai.Client(api_key=GEMINI_API_KEY)

# -----------------------
# CONFIG
# -----------------------

PRIMARY_MODEL = "gemini-2.5-flash-lite"
FALLBACK_MODEL = "gemini-2.5-flash"
CACHE_DIR = "ai_cache"
CACHE_TTL_DAYS = 15

PRIMARY_QUOTA = 20
FALLBACK_QUOTA = 20

os.makedirs(CACHE_DIR, exist_ok=True)


# -----------------------
# CACHE HELPERS
# -----------------------

def _cache_key(company, job_title, job_text):
    raw = f"{company}-{job_title}-{job_text}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_valid(path):
    if not os.path.exists(path):
        return False

    modified = datetime.fromtimestamp(os.path.getmtime(path))
    return datetime.now() - modified < timedelta(days=CACHE_TTL_DAYS)


# -----------------------
# MAIN GENERATION FUNCTION
# -----------------------

def generate_all_content(company, job_title, job_text):

    if not job_text:
        return {}

    key = _cache_key(company, job_title, job_text)
    cache_path = os.path.join(CACHE_DIR, f"{key}.json")

    # Return cached if valid
    if _cache_valid(cache_path):
        print("Using cached AI content")
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)

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

    for model in [PRIMARY_MODEL, FALLBACK_MODEL]:
        if not PRIMARY_QUOTA and model == PRIMARY_MODEL:
            continue
        
        if not FALLBACK_QUOTA and model == FALLBACK_MODEL:
            return {}

        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt
            )

            if model == PRIMARY_MODEL:
                PRIMARY_QUOTA -= 1
            
            if model == FALLBACK_MODEL:
                FALLBACK_QUOTA -= 1

            text = response.text.strip()

            # Extract JSON block using regex
            match = re.search(r"\{.*\}", text, re.DOTALL)

            if not match:
                raise ValueError("No JSON found in response")

            json_text = match.group(0)

            data = json.loads(json_text)

            # Save to cache
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            print(f"Generated using model: {model}")
            return data

        except Exception as e:
            print(f"{model} failed: {e}")
            continue

    print("Both models failed.")
    return {}
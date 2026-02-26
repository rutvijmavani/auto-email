# ai_personalizer.py

import os
from google import genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file")

client = genai.Client(api_key=GEMINI_API_KEY)


def generate_job_based_intro(company, job_text):

    if not job_text:
        return ""

    prompt = f"""
You are helping me as a software engineer write a short outreach email based on job description of a particular role at particular company.

Company: {company}

Job Description:
{job_text}

Candidate Background:
- Software Engineer
- Backend engineer
- Python & Go
- Microservices
- Kubernetes
- PostgreSQL optimization
- CI/CD pipelines
- Distributed systems

Write 3 concise professional sentences explaining why I am a strong fit.
Do not include email subject.
Directly include 3 statements in the body of the email.
Be confident and specific.
Limit to under 120 words.
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",   # Gemini 2.5 Flash Lite
            contents=prompt
        )

        if hasattr(response, "text") and response.text:
            return str(response.text).strip()
        return ""

    except Exception as e:
        print("Gemini error:", e)
        return ""


def generate_subject(company, job_title):

    prompt = f"""
Generate a professional email subject line for a job outreach email.

Rules:
- Under 10 words
- Include company name
- Include job title
- Professional tone
- No emojis

Company: {company}
Job Title: {job_title}

Return ONLY the subject line.
"""

    try:
        response = client.models.generate_content(
            model="gemini-2.5-flash-lite",
            contents=prompt
        )

        subject = response.text.strip()

        # Safety fallback
        if not subject or len(subject) > 100:
            return f"{job_title} – {company}"

        return subject

    except Exception as e:
        print("Subject generation failed:", e)
        return f"{job_title} – {company}"
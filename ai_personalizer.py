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
Do not include greetings to the recruiter/hiring manager.
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


def generate_followup_content(company, job_title, job_text, stage):

    prompt = f"""
Write a concise professional follow-up email for a {job_title} role at {company}.

Stage: {stage}

Keep it under 120 words.
No emojis.
Professional tone.
Return only the email body.
"""

    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt
    )

    return response.text.strip()


def generate_subject(company, job_title, stage="initial"):

    stage_instruction = {
        "initial": "This is the first outreach email.",
        "followup1": "This is a polite first follow-up email.",
        "followup2": "This is a final follow-up email."
    }.get(stage, "This is a job outreach email.")

    prompt = f"""
Generate a professional email subject line.

Context:
{stage_instruction}

Requirements:
- Under 10 words
- Include company name
- Include job title
- Professional tone
- No emojis
- Do not use Software Engineer in subject if job title is different.
- Only use Software Engineer if job title is empty or Software Engineer.

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

        if not subject or len(subject) > 100:
            raise ValueError("Invalid subject")

        return subject

    except:
        # Smart fallback per stage
        if stage == "followup1":
            return f"Following Up – {job_title} at {company}"
        elif stage == "followup2":
            return f"Final Follow-Up – {job_title} at {company}"
        else:
            return f"{job_title} – {company}"
import json
import re
import time
from llama_cpp import Llama

MODEL_PATH = "/home/opc/models/qwen3-8b/Qwen3-8B-Q4_K_M.gguf"

EMAILS = [
    {
        "name": "Brellium — clear rejection",
        "sender": "no-reply@ashbyhq.com",
        "subject": "Brellium Hiring Team",
        "body": """Hi Rutvij,
Thank you for applying for the Software Engineer role at Brellium. After reviewing your application, we've decided to move forward with other candidates whose experience more closely aligns with what we're looking for at this time.
We appreciate your interest in Brellium and the time you took to apply.
All the best,
Brellium Hiring Team""",
        "expected": {"company": "Brellium", "title": "Software Engineer", "status": "rejected"},
    },
    {
        "name": "Kustomer — acknowledgement (should be irrelevant)",
        "sender": "no-reply@ashbyhq.com",
        "subject": "Thanks for applying to Kustomer!",
        "body": """Hello Rutvij,
Thank you for your interest in Kustomer. We appreciate the time you took to apply for the Software Engineer, Full Stack role and look forward to reviewing your application.
Your resume will be reviewed shortly. If selected for an interview, one of our teammates will reach out to schedule a phone call.
Regards,
The Kustomer Talent Acquisition Team""",
        "expected": {"company": "Kustomer", "title": "Software Engineer, Full Stack", "status": "irrelevant"},
    },
    {
        "name": "Vectra AI — clear rejection",
        "sender": "no-reply@us.greenhouse-mail.io",
        "subject": "Important information about your application to Vectra AI",
        "body": """Hi Rutvij,
Thank you for your interest in Vectra. Unfortunately, we have decided not to proceed with your candidacy for the Software Engineer II opening at this time due to the specific needs of this role.
We wish you luck in your search.
Best regards,
The Vectra Talent Team""",
        "expected": {"company": "Vectra AI", "title": "Software Engineer II", "status": "rejected"},
    },
    {
        "name": "Fannie Mae — acknowledgement (should be irrelevant)",
        "sender": "notification@smartrecruiters.com",
        "subject": "Thank you for applying to Fannie Mae!",
        "body": """Dear Rutvij,
Thank you for submitting your application for the Software Engineer position. We appreciate your interest.
A Recruiter will contact you within a few weeks if your qualifications fit the job's requirements and your application is moved to the next level of review.
Best,
Talent Acquisition — Fannie Mae""",
        "expected": {"company": "Fannie Mae", "title": "Software Engineer", "status": "irrelevant"},
    },
    {
        "name": "Amazon — online assessment invitation",
        "sender": "amazon-recruiting@amazon.com",
        "subject": "Next steps for your Amazon SDE application",
        "body": """Hi Rutvij,
Thank you for your continued interest in Amazon's Software Development Engineer (SDE) opportunities! We are excited to move you forward in the application process. As the next step, we'd like to invite you to complete this online assessment no later than a week from now.
There are 3 types of exercises in the assessment:
1. Coding Challenge – 90 minutes, two coding problems.
2. Work Simulation – 15 minutes.
3. Work Style Surveys – 10 minutes.
We value the people we hire and appreciate your interest in Amazon!""",
        "expected": {"company": "Amazon", "title": "Software Development Engineer", "status": "assessment"},
    },
]

SYSTEM_PROMPT = "You are a job application email classifier. Respond only with valid JSON."

USER_PROMPT = """/no_think
Extract information from this job application email.

Respond ONLY with valid JSON in this exact format:
{{"is_job_email": true/false, "company": "company name or null", "title": "job title or null", "status": "one of: rejected/interview/phone_screen/assessment/offer/irrelevant"}}

Status rules — read carefully:
- irrelevant: Pure acknowledgement emails only — "we received your application", "we will review your resume", "someone MIGHT reach out IF selected". No action required from you now.
- phone_screen: The company is actively scheduling a call RIGHT NOW — includes a calendar link, asks you to pick a time, or says "please reply to schedule".
- interview: Invitation to an interview (phone, video, or in-person) where a time is being arranged.
- assessment: Invitation to complete an online coding test, work simulation, or any timed assessment.
- rejected: Clear rejection — "not moving forward", "other candidates", "not proceeding".
- offer: Job offer extended.

Key distinction: "We MIGHT reach out if selected" = irrelevant. "Please schedule / complete this now" = phone_screen/interview/assessment.

From: {sender}
Subject: {subject}
Body:
{body}"""


def extract_json(raw):
    # Strip <think>...</think> block (present even when /no_think suppresses reasoning)
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    # Pull out the JSON object in case there's surrounding text
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    return match.group(0) if match else cleaned


def run_test(llm, email):
    user_msg = USER_PROMPT.format(
        sender=email["sender"],
        subject=email["subject"],
        body=email["body"],
    )
    t0 = time.time()
    out = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        max_tokens=200,
        temperature=0,
    )
    elapsed = time.time() - t0
    raw = out["choices"][0]["message"]["content"].strip()
    return raw, elapsed


def check_pass(raw, expected):
    json_str = extract_json(raw)
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        return False, f"invalid JSON after stripping think tags: {json_str!r}"

    if parsed.get("status") != expected["status"]:
        return False, f"status: got {parsed.get('status')!r}, want {expected['status']!r}"

    if expected["status"] != "irrelevant":
        got_company = (parsed.get("company") or "").lower()
        want_company = expected["company"].lower()
        # Accept partial match (e.g. "Vectra" for "Vectra AI") since we use fuzzy matching in prod
        if want_company.split()[0] not in got_company and got_company not in want_company:
            return False, f"company: got {parsed.get('company')!r}, want {expected['company']!r}"

    return True, "ok"


def main():
    print("=" * 60)
    print("Loading Qwen3-8B Q4_K_M...")
    t0 = time.time()
    llm = Llama(
        model_path=MODEL_PATH,
        n_ctx=2048,
        n_threads=2,
        chat_format="chatml",
        verbose=False,
    )
    print(f"Loaded in {time.time()-t0:.1f}s")
    print("=" * 60)

    passed = 0
    for i, email in enumerate(EMAILS, 1):
        print(f"\nTest {i}: {email['name']}")
        print(f"Expected: {email['expected']}")
        raw, elapsed = run_test(llm, email)
        json_str = extract_json(raw)
        print(f"Raw:      {raw[:80]}{'...' if len(raw) > 80 else ''}")
        print(f"Parsed:   {json_str}")
        print(f"Time:     {elapsed:.1f}s")

        ok, reason = check_pass(raw, email["expected"])
        print(f"Result:   {'PASS ✓' if ok else f'FAIL ✗  ({reason})'}")
        if ok:
            passed += 1

    print("\n" + "=" * 60)
    print(f"Score: {passed}/{len(EMAILS)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
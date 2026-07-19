import re
import time
from llama_cpp import Llama

MODEL_PATH = "/home/opc/models/qwen3-8b/Qwen3-8B-Q4_K_M.gguf"

EMAILS = [
    {
        "name": "CMT — rejection disguised as 'thanks for applying'",
        "sender": "no-reply@careers.cmtelematics.com",
        "subject": "Thanks for your application - CMT Senior Software Engineer, Full Stack",
        "expected": "yes",
    },
    {
        "name": "Amazon — assessment invite",
        "sender": "amazon-recruiting@amazon.com",
        "subject": "Next steps for your Amazon SDE application",
        "expected": "yes",
    },
    {
        "name": "Vectra AI — rejection (Greenhouse)",
        "sender": "no-reply@us.greenhouse-mail.io",
        "subject": "Important information about your application to Vectra AI",
        "expected": "yes",
    },
    {
        "name": "Kustomer — acknowledgement",
        "sender": "no-reply@ashbyhq.com",
        "subject": "Thanks for applying to Kustomer!",
        "expected": "yes",
    },
    {
        "name": "Fannie Mae — acknowledgement",
        "sender": "notification@smartrecruiters.com",
        "subject": "Thank you for applying to Fannie Mae!",
        "expected": "yes",
    },
    {
        "name": "Brellium — application update (ambiguous without body)",
        "sender": "no-reply@ashbyhq.com",
        "subject": "Brellium Application Update",
        "expected": "not_sure",
    },
    {
        "name": "matchingdonors.com — networking ask",
        "sender": "pr@matchingdonors.com",
        "subject": "Hi Rutvij, A quick favor — know an OPT F-1 student looking for sponsorship?",
        "expected": "no",
    },
    {
        "name": "iHireTechnology — job alert",
        "sender": "jobseekers@email.ihire.com",
        "subject": "Sr VoIP/Avaya Engineer (JC, CLT, JAX)",
        "expected": "no",
    },
    {
        "name": "Boston Children's Hospital — talent community newsletter",
        "sender": "careers@childrens.harvard.edu",
        "subject": "July Talent Community Update | Featured Jobs, Class of 2027, & More",
        "expected": "no",
    },
    {
        "name": "Oracle — career newsletter",
        "sender": "ota@career.oracle.com",
        "subject": "Career insights from Oracle",
        "expected": "no",
    },
    {
        "name": "Under Armour — job alert",
        "sender": "UnderArmour-jobnotification@noreply.jobs2web.com",
        "subject": "New jobs posted from careers.underarmour.com",
        "expected": "no",
    },
    {
        "name": "Indeed — job recommendation",
        "sender": "donotreply@match.indeed.com",
        "subject": "Software Engineer - Database Internals (Remote) @ VillageSQL",
        "expected": "no",
    },
]

SYSTEM_PROMPT = "You are an email classifier for a job application tracker."

USER_PROMPT = """/no_think
You track job applications a user has already submitted.

Look at the sender and subject. Output exactly one word:
  yes      → clearly about a specific job application the user already submitted
             (rejections, interviews, assessments, offers, acknowledgements from companies)
  no       → clearly NOT about a submitted application
             (job alerts, job recommendations, newsletters, career tips, networking emails)
  not_sure → subject is ambiguous and you cannot tell without reading the email body

Examples:
  "Thanks for your application - CMT Software Engineer"  → yes       (company responding to an application)
  "Next steps for your Amazon SDE application"           → yes       (clear application update)
  "Important information about your application to Acme" → yes       (company update)
  "Thanks for applying to Stripe!"                       → yes       (company acknowledgement)
  "Brellium Application Update"                          → not_sure  (could be status update, need body)
  "Sr VoIP Engineer (JC, CLT, JAX)"                     → no        (job listing, not your application)
  "Software Engineer - Database Internals @ VillageSQL"  → no        (job recommendation)
  "New jobs posted from careers.underarmour.com"         → no        (job alert)
  "Career insights from Oracle"                          → no        (newsletter)
  "A quick favor — know someone looking for a job?"      → no        (networking, not your application)

Sender: {sender}
Subject: {subject}

Output:"""


def run(llm, email):
    t0 = time.time()
    out = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT.format(
                sender=email["sender"],
                subject=email["subject"],
            )},
        ],
        max_tokens=10,
        temperature=0,
    )
    elapsed = time.time() - t0
    raw = out["choices"][0]["message"]["content"].strip().lower()
    cleaned = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
    token = cleaned.split()[0].rstrip(".,!") if cleaned else ""
    return token, elapsed


def main():
    print("=" * 60)
    print("Gate test — Qwen3-8B on subject+sender only")
    print("(Same test cases as 1.7B gate — comparing speed & accuracy)")
    print("Loading model...")
    t0 = time.time()
    llm = Llama(
        model_path=MODEL_PATH,
        n_ctx=512,
        n_threads=2,
        chat_format="chatml",
        verbose=False,
    )
    print(f"Loaded in {time.time()-t0:.1f}s")
    print("=" * 60)

    passed = 0
    dangerous = 0
    times = []
    total = len(EMAILS)

    for i, e in enumerate(EMAILS, 1):
        result, elapsed = run(llm, e)
        times.append(elapsed)
        expected = e["expected"]
        ok = result == expected
        is_dangerous = expected in ("yes", "not_sure") and result == "no"

        if ok:
            label = "PASS ✓"
            passed += 1
        elif is_dangerous:
            label = f"FAIL ✗ DANGEROUS  (got {result!r} — real email would be dropped!)"
            dangerous += 1
        else:
            label = f"FAIL ✗  (got {result!r}, want {expected!r})"

        print(f"\nTest {i}/{total}: {e['name']}")
        print(f"  Sender:  {e['sender']}")
        print(f"  Subject: {e['subject']}")
        print(f"  Result:  {result}  ({elapsed:.1f}s)  {label}")

    print("\n" + "=" * 60)
    print(f"Exact match:              {passed}/{total}")
    print(f"Safe (no dangerous drops): {total - dangerous}/{total}")
    print(f"Avg time per email:        {sum(times)/len(times):.1f}s")
    print(f"Min / Max:                 {min(times):.1f}s / {max(times):.1f}s")
    print("=" * 60)
    print()
    print("Compare to 1.7B gate: 12/12 safe, ~3-5s avg")
    print("If 8B scores same accuracy at acceptable speed → drop 1.7B entirely")


if __name__ == "__main__":
    main()
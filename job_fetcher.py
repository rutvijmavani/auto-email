# job_fetcher.py

import requests
from bs4 import BeautifulSoup
from config import JOB_TEXT_LIMIT

def fetch_job_description(url):

    if not url:
        return None

    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Remove script & style
        for script in soup(["script", "style", "noscript"]):
            script.decompose()

        text = soup.get_text(separator="\n")

        # Clean blank lines
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]

        cleaned_text = "\n".join(lines)

        return cleaned_text[:JOB_TEXT_LIMIT]

    except Exception as e:
        print("Job fetch failed:", e)
        return None
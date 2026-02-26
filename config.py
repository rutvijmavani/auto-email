import os
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("GMAIL_EMAIL")
APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

RESUME_PATH = "Resume.pdf"

SEND_INTERVAL_DAYS = 7
MAX_EMAIL_COUNT = 3
JOB_TEXT_LIMIT = 4000
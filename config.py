import os
from dotenv import load_dotenv

load_dotenv()

EMAIL = os.getenv("GMAIL_EMAIL")
APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")

RESUME_PATH = "Resume.pdf"

SEND_INTERVAL_DAYS = 7


# MAX_EMAIL_COUNT = 3
# JOB_TEXT_LIMIT = 4000

SEND_WINDOW_START    = 9     # 9:00 AM
SEND_WINDOW_END      = 11    # 11:00 AM preferred end
GRACE_PERIOD_HOURS   = 1     # hard cutoff at 12:00 PM
SEND_TIMEZONE        = "America/New_York"
SEND_INTERVAL_DAYS   = 7
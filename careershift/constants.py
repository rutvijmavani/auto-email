# careershift/constants.py — Shared constants for CareerShift scraping

import os

SESSION_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "careershift_session.json")
CAREERSHIFT_SEARCH_URL = "https://www.careershift.com/App/Contacts/Search"
CAREERSHIFT_QUOTA_URL  = "https://www.careershift.com/App/Settings/ResetPassword"

MIN_RECRUITERS_PER_COMPANY = 2

HR_SEARCH_TERMS = [
    "Recruiter",
    "Talent Acquisition",
    "Human Resources",
    "People Operations",
    "HR",
]

HR_KEYWORDS_STRONG = [
    "recruiter", "recruiting", "recruitment",
    "talent acquisition", "talent partner",
    "human resources", "hr manager", "hr director",
    "hr business partner", "hrbp", "hr generalist",
    "people operations", "people partner",
    "staffing", "head of people",
    "vp of people", "vp hr", "director of hr",
    "hr specialist", "hr coordinator",
]

HR_KEYWORDS_LOOSE = [
    "people", "hiring", "workforce", "culture",
    "talent", "onboarding", " hr", "human capital",
]

EXCLUDE_KEYWORDS = [
    "chief executive", "ceo", "chief technology", "cto",
    "chief operating", "coo", "chief financial", "cfo",
    "chief marketing", "cmo", "chief information", "cio",
    "chief people", "chief hr", "chief human resources",
    "founder", "co-founder", "president",
    "board member", "board of director",
    "managing partner", "general partner",
    "executive vice president", "evp",
    "senior vice president", "svp",
    "vice president", " vp ",
]

# Tiered verification thresholds (days)
TIER1_DAYS = 30
TIER2_DAYS = 60
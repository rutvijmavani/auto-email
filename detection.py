import logging
logging.basicConfig(level=logging.DEBUG, format="%(message)s")

from jobs.ats.career_detector import detect_company
print(detect_company("wayfair.com"))
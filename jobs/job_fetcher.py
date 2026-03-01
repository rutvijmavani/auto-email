from db.job_cache import init_cache, get_job, save_job
from jobs.job_scraper import JobScraper, detect_portal
import logging

logger = logging.getLogger(__name__)

init_cache()

scraper = JobScraper()


def fetch_job_description(url):

    if not url:
        return None

    cached = get_job(url)
    if cached:
        print("Using cached job description")
        return cached

    try:
        portal = detect_portal(url)
        use_playwright = portal in ("workday", "icims", "taleo", "ashby")

        job = scraper.scrape(url, use_playwright=use_playwright)

        if not job:
            print("Scraper returned None.")
            return None

        job_text = f"""
Job Title: {job.title}

Company: {job.company}

Location: {job.location}

Job Type: {job.job_type}

Department: {job.department}

Salary: {job.salary}

Description:
{job.description}
"""

        if not job.description or len(job.description.strip()) < 200:
            print("Job description too short. Skipping cache save.")
            return None

        save_job(url, job_text)

        return {
            "job_text": job_text,
            "job_title": job.title
        }

    except Exception as e:
        logger.exception("Scraper integration failed for url=%s", url)
        return None
"""
db/job_cache.py — Job description cache.

All data stored in recruiter_pipeline.db (jobs table).
This file is a thin wrapper so job_fetcher.py imports stay clean.
"""

from db.db import save_job, get_job, delete_job, init_job_cache as init_cache

__all__ = ["init_cache", "save_job", "get_job", "delete_job"]
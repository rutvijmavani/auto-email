"""workers/__main__.py — Entry point for: python -m workers.scheduler"""
import sys
from workers.scheduler import run_scheduler

if __name__ == "__main__":
    from logger import init_logging
    init_logging("scheduler")
    skip = "--skip-rebuild" in sys.argv
    run_scheduler(skip_rebuild=skip)

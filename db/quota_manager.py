"""
db/quota_manager.py — Gemini AI model quota manager.

All data stored in recruiter_pipeline.db (model_usage table).
This file is a thin wrapper so ai_full_personalizer.py imports stay clean.
"""

from db.db import can_call, increment_usage, all_models_exhausted

__all__ = ["can_call", "increment_usage", "all_models_exhausted"]
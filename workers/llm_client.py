"""
workers/llm_client.py — Shared LLM client for email classification inference.

Supports two backends selected by EMAIL_LLM_PROVIDER (config.py):
  "gemini" — Google AI API (gemma-4-26b-it by default); no local GPU needed.
  "local"  — llama-server via OpenAI-compatible HTTP API at LLM_BASE_URL.

The Gemini backend uses the same quota system as ai_full_personalizer:
  can_call()        — checks RPD (DB-backed, persists across restarts) + RPM
  increment_usage() — records RPM timestamp + increments daily DB count
  tpm_wait_seconds() / record_tpm() — in-memory sliding window for TPM
All three limits (RPD / RPM / TPM) are defined in db/connection.py.

Clients in both paths are lazy-initialised once per process.
"""

import json
import os
import re
import time
import urllib.request

import openai

from config import (
    EMAIL_LLM_GEMINI_MODEL,
    EMAIL_LLM_PROVIDER,
    LLM_BASE_URL,
    LLM_MODEL,
    LLM_REQUEST_TIMEOUT,
    LLM_SLOTS_URL,
)
from db.quota_manager import can_call, increment_usage, within_rpm, tpm_wait_seconds, record_tpm
from logger import get_logger

logger = get_logger(__name__)

# ── Local (llama-server) backend ───────────────────────────────────────────────

_local_client: "openai.OpenAI | None" = None


def _get_local_client() -> openai.OpenAI:
    global _local_client
    if _local_client is None:
        _local_client = openai.OpenAI(
            base_url=LLM_BASE_URL,
            api_key="none",
            timeout=LLM_REQUEST_TIMEOUT,
        )
    return _local_client


def _poll_until_free() -> None:
    """Poll /slots until no slot is processing, deadline expires, or server is unreachable."""
    deadline = time.monotonic() + LLM_REQUEST_TIMEOUT
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(LLM_SLOTS_URL, timeout=5) as resp:
                slots = json.loads(resp.read())
            if not any(s.get("is_processing") for s in slots):
                return
        except Exception:
            return
        time.sleep(1)


def _call_local(prompt: str) -> str:
    """Send prompt to llama-server via streaming; return response with <think> stripped."""
    stream = None
    try:
        tokens: list[str] = []
        stream = _get_local_client().chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            stream=True,
        )
        for chunk in stream:
            delta = chunk.choices[0].delta.content if chunk.choices else None
            if delta:
                tokens.append(delta)
    except Exception:
        _poll_until_free()
        raise
    finally:
        if stream is not None:
            stream.response.close()

    text = "".join(tokens)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL)
    return text.strip()


# ── Gemini backend ─────────────────────────────────────────────────────────────

_gemini_client: "object | None" = None

_QWEN_PREFIX = re.compile(r"^/(no_think|think)\n")


def _get_gemini_client() -> object:
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            raise RuntimeError(
                "GOOGLE_API_KEY not set — required when EMAIL_LLM_PROVIDER=gemini"
            )
        _gemini_client = genai.Client(api_key=api_key)
        logger.info("Gemini provider initialised — model=%s", EMAIL_LLM_GEMINI_MODEL)
    return _gemini_client


def _call_gemini(prompt: str) -> str:
    """Call Gemini API with RPD+RPM+TPM quota guards. Raises on API errors."""
    from google.genai import types

    clean = _QWEN_PREFIX.sub("", prompt)
    client = _get_gemini_client()

    # RPD + RPM: distinguish daily exhaustion from per-minute throttle
    while not can_call(EMAIL_LLM_GEMINI_MODEL, use_case="email_classif"):
        if within_rpm(EMAIL_LLM_GEMINI_MODEL):
            raise RuntimeError("email_classif daily quota exhausted — requeueing job")
        logger.debug("email quota: RPM limit reached — sleeping 5s")
        time.sleep(5)

    # TPM: wait until token budget has headroom
    estimated = len(clean) // 4 + 50
    wait_s = tpm_wait_seconds(EMAIL_LLM_GEMINI_MODEL, estimated_tokens=estimated)
    if wait_s > 0:
        time.sleep(wait_s)

    response = client.models.generate_content(
        model=EMAIL_LLM_GEMINI_MODEL,
        contents=clean,
        config=types.GenerateContentConfig(temperature=0),
    )
    tokens = getattr(response.usage_metadata, "total_token_count", 0) or 0
    increment_usage(EMAIL_LLM_GEMINI_MODEL, use_case="email_classif")
    record_tpm(EMAIL_LLM_GEMINI_MODEL, tokens or estimated)
    logger.debug("Gemini email response tokens=%d", tokens)

    return (response.text or "").strip()


# ── Public interface ───────────────────────────────────────────────────────────

def call_llm(prompt: str) -> str:
    """
    Send prompt to the configured LLM backend; return the response text.
    Raises on any connection or API error — callers decide retry vs discard.
    """
    if EMAIL_LLM_PROVIDER == "gemini":
        return _call_gemini(prompt)
    if EMAIL_LLM_PROVIDER == "local":
        return _call_local(prompt)
    raise RuntimeError(f"Unsupported EMAIL_LLM_PROVIDER: {EMAIL_LLM_PROVIDER!r}")

"""
workers/llm_client.py — Shared OpenAI-compatible client for llama-server inference.

Creates the client once at module scope (one per process) so workers do not
re-establish the connection pool on every request.
"""

import re

import openai

from config import LLM_BASE_URL, LLM_MAX_TOKENS, LLM_MODEL, LLM_REQUEST_TIMEOUT

_client: "openai.OpenAI | None" = None


def _get_client() -> openai.OpenAI:
    global _client
    if _client is None:
        _client = openai.OpenAI(
            base_url=LLM_BASE_URL,
            api_key="none",
            timeout=LLM_REQUEST_TIMEOUT,
        )
    return _client


def call_llm(prompt: str, *, max_tokens: int = LLM_MAX_TOKENS) -> str:
    """
    Send prompt to llama-server; return response with <think> blocks stripped.
    Raises on any connection or API error — callers decide retry vs discard.
    """
    resp = _get_client().chat.completions.create(
        model=LLM_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=max_tokens,
    )
    text = resp.choices[0].message.content or ""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL)
    return text.strip()

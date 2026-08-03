"""
workers/llm_client.py — Shared OpenAI-compatible client for llama-server inference.

Creates the client once at module scope (one per process) so workers do not
re-establish the connection pool on every request.
"""

import json
import re
import time
import urllib.request

import openai

from config import LLM_BASE_URL, LLM_MAX_TOKENS, LLM_MODEL, LLM_REQUEST_TIMEOUT, LLM_SLOTS_URL

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
            return  # server unreachable — let caller handle
        time.sleep(1)


def call_llm(prompt: str, *, max_tokens: int = LLM_MAX_TOKENS) -> str:
    """
    Send prompt to llama-server via streaming; return response with <think> stripped.
    Streaming keeps the HTTP connection alive during long /think inference so the
    per-chunk read timeout applies instead of a total-response timeout.
    On any stream error, polls /slots until the slot is free before re-raising so
    the caller can safely retry without competing with the in-flight request.
    Raises on any connection or API error — callers decide retry vs discard.
    """
    stream = None
    try:
        tokens: list[str] = []
        stream = _get_client().chat.completions.create(
            model=LLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=max_tokens,
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

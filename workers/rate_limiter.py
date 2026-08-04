"""
workers/rate_limiter.py — Sliding-window RPM + TPM rate limiter.

Shared by h1b_llm_worker (long-running service) and h1b_gemma_batch (one-shot script).
No per-request token cap — the model can use as many tokens as it needs per call.
The limiter pauses before a new request only when the 60s window is full (RPM) or the
accumulated token count has reached the TPM ceiling.
"""

import time
from collections import deque


class DualRateLimiter:
    def __init__(self, rpm: int, tpm: int):
        self._rpm = rpm
        self._tpm = tpm
        self._req_times: deque[float] = deque()
        self._token_log: deque[tuple[float, int]] = deque()

    def _prune(self) -> float:
        now = time.monotonic()
        cutoff = now - 60.0
        while self._req_times and self._req_times[0] < cutoff:
            self._req_times.popleft()
        while self._token_log and self._token_log[0][0] < cutoff:
            self._token_log.popleft()
        return now

    def wait(self) -> None:
        """Block until both RPM and TPM have headroom for one more request."""
        while True:
            now = self._prune()

            if len(self._req_times) >= self._rpm:
                sleep_s = 60.0 - (now - self._req_times[0]) + 0.05
                if sleep_s > 0:
                    time.sleep(sleep_s)
                    continue

            tokens_used = sum(t for _, t in self._token_log)
            if tokens_used >= self._tpm:
                sleep_s = 60.0 - (now - self._token_log[0][0]) + 0.05
                if sleep_s > 0:
                    time.sleep(sleep_s)
                    continue

            break

    def record(self, tokens: int) -> None:
        now = time.monotonic()
        self._req_times.append(now)
        self._token_log.append((now, tokens))

    def status(self) -> str:
        self._prune()
        tokens_used = sum(t for _, t in self._token_log)
        return f"RPM {len(self._req_times)}/{self._rpm}  TPM {tokens_used}/{self._tpm}"

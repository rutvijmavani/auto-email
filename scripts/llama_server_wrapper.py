"""
scripts/llama_server_wrapper.py — systemd-aware process manager for llama-server.

Spawns llama-server as a child process and manages its lifecycle:
  Phase 1 (loading): polls GET /health every LLM_POLL_S seconds until model is ready,
                     then signals systemd via sd_notify("READY=1").
  Phase 2 (liveness): continues polling every LLM_POLL_S seconds; exits if the child
                      dies or health check fails — systemd restarts this wrapper,
                      which restarts llama-server.

All timing and paths come from config.py / .env — no hardcoded values.

Usage (via systemd — see deploy/systemd/llama-server.service):
    python scripts/llama_server_wrapper.py
"""

import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import (
    LLM_CTX_SIZE,
    LLM_HEALTH_URL,
    LLM_MODEL_PATH,
    LLM_POLL_S,
    LLM_SERVER_BIN,
    LLM_STARTUP_TIMEOUT_S,
)
from logger import get_logger, init_logging

log = get_logger(__name__)

try:
    import requests as _requests
except ImportError:
    print("[ERROR] requests is not installed. Run: pip install requests")
    sys.exit(1)


def _sd_notify(msg: str) -> None:
    path = os.environ.get("NOTIFY_SOCKET", "")
    if not path:
        return
    # Linux abstract-namespace sockets are indicated by a leading "@"; convert to NUL prefix
    if path.startswith("@"):
        path = "\0" + path[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.sendto(msg.encode(), path)
    except OSError as exc:
        log.warning("sd_notify failed (NOTIFY_SOCKET=%r): %s", os.environ.get("NOTIFY_SOCKET"), exc)


def _health() -> str:
    """Returns 'ok', 'loading', or 'error'."""
    try:
        resp = _requests.get(LLM_HEALTH_URL, timeout=LLM_POLL_S)
        return resp.json().get("status", "error")
    except Exception:
        return "error"


def _validate() -> None:
    if not LLM_SERVER_BIN:
        log.error("LLM_SERVER_BIN is not set — point it at the llama-server binary in .env")
        sys.exit(1)
    if not LLM_MODEL_PATH:
        log.error("LLM_MODEL_PATH is not set — point it at the .gguf model file in .env")
        sys.exit(1)


def main() -> None:
    _validate()

    cmd = [
        LLM_SERVER_BIN,
        "--model",    LLM_MODEL_PATH,
        "--host",     "127.0.0.1",
        "--port",     "8080",
        "--ctx-size", str(LLM_CTX_SIZE),
        "--parallel", "1",
        "--log-disable",
    ]

    log.info("Starting llama-server: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd)

    # ── Phase 1: wait for model to load ──────────────────────────────────────
    log.info("Waiting for model to load (poll every %ds, timeout %ds)",
             LLM_POLL_S, LLM_STARTUP_TIMEOUT_S)
    deadline = time.monotonic() + LLM_STARTUP_TIMEOUT_S

    while True:
        if proc.poll() is not None:
            log.error("llama-server died during model load (exit %d)", proc.returncode)
            sys.exit(1)

        if time.monotonic() > deadline:
            log.error("llama-server did not become ready within %ds — killing",
                      LLM_STARTUP_TIMEOUT_S)
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            sys.exit(1)

        status = _health()
        if status == "ok":
            log.info("Model ready — signalling systemd READY=1")
            _sd_notify("READY=1\n")
            break

        log.debug("Model still loading (status=%s)", status)
        time.sleep(LLM_POLL_S)

    # ── Phase 2: liveness keepalive ───────────────────────────────────────────
    while True:
        time.sleep(LLM_POLL_S)

        if proc.poll() is not None:
            log.error("llama-server died post-startup (exit %d)", proc.returncode)
            sys.exit(1)

        if _health() != "ok":
            log.error("llama-server health check failed post-startup — killing")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            sys.exit(1)


if __name__ == "__main__":
    init_logging("llama_server_wrapper")
    main()

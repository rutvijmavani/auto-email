"""
scripts/tunnel_manager.py
─────────────────────────
Manages two cloudflared quick tunnels:
  - API tunnel      (port EXTENSION_API_PORT, default 5000) → Gist key "api_base"
  - Frontend tunnel (port STREAMLIT_PORT,     default 8501) → Gist key "frontend_base"

Both URLs are written to the same GitHub Gist so the Chrome extension and the
Streamlit bookmark redirect can always discover the current endpoints.

Gist content (api-config.json):
    {"api_base": "https://xxx.trycloudflare.com",
     "frontend_base": "https://yyy.trycloudflare.com"}

Env vars (read from .env):
    GITHUB_PAT          GitHub personal access token (gist scope only)
    GIST_ID             ID of the GitHub Gist to update
    EXTENSION_API_PORT  Flask port (default 5000)
    STREAMLIT_PORT      Streamlit port (default 8501)

Run via systemd: deploy/systemd/cloudflare-tunnel.service
"""

import json
import logging
import os
import re
import subprocess
import sys
import threading
import time

import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_PAT      = os.environ.get('GITHUB_PAT', '')
GIST_ID         = os.environ.get('GIST_ID', '')
API_PORT        = os.environ.get('EXTENSION_API_PORT', '5000')
FRONTEND_PORT   = os.environ.get('STREAMLIT_PORT', '8501')

GIST_FILENAME = 'api-config.json'
URL_RE = re.compile(r'https://[a-z0-9-]+\.trycloudflare\.com')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# Shared Gist state — both threads write here, protected by a lock
_gist_lock  = threading.Lock()
_gist_cache: dict = {}


def _update_gist(key: str, url: str) -> None:
    """Merge {key: url} into the Gist, preserving all other existing keys."""
    if not GITHUB_PAT or not GIST_ID:
        log.warning('GITHUB_PAT or GIST_ID not set — Gist update skipped')
        return

    with _gist_lock:
        # Read current Gist content so we don't overwrite the other key
        try:
            r = requests.get(
                f'https://api.github.com/gists/{GIST_ID}',
                headers={
                    'Authorization': f'token {GITHUB_PAT}',
                    'Accept': 'application/vnd.github.v3+json',
                },
                timeout=15,
            )
            if r.status_code == 200:
                raw = r.json()['files'].get(GIST_FILENAME, {}).get('content', '{}')
                _gist_cache.update(json.loads(raw))
        except Exception as exc:
            log.warning('Could not read current Gist content: %s', exc)

        _gist_cache[key] = url

        try:
            r = requests.patch(
                f'https://api.github.com/gists/{GIST_ID}',
                headers={
                    'Authorization': f'token {GITHUB_PAT}',
                    'Accept': 'application/vnd.github.v3+json',
                },
                json={'files': {GIST_FILENAME: {'content': json.dumps(_gist_cache)}}},
                timeout=15,
            )
            if r.status_code == 200:
                log.info('Gist updated — %s → %s', key, url)
            else:
                log.error('Gist update failed: HTTP %s — %s', r.status_code, r.text[:200])
        except Exception as exc:
            log.error('Gist update error: %s', exc)


def _health_monitor(url: str, proc: subprocess.Popen, label: str,
                    interval: int = 300, max_failures: int = 3) -> None:
    """
    Periodically check that the tunnel URL resolves in DNS.
    If DNS fails consecutively, Cloudflare has invalidated the quick tunnel URL —
    kill cloudflared so systemd restarts the service and registers a fresh URL.
    """
    time.sleep(60)  # grace period: let tunnel stabilise before first check
    failures = 0
    while proc.poll() is None:
        try:
            requests.head(url, timeout=10, allow_redirects=False)
            if failures:
                log.info('[%s] health check recovered', label)
            failures = 0
        except requests.exceptions.ConnectionError as exc:
            msg = str(exc).lower()
            is_dns = any(s in msg for s in (
                'name or service not known',
                'nodename nor servname',
                'name resolution',
                'nxdomain',
            ))
            if is_dns:
                failures += 1
                log.warning('[%s] DNS lookup failed (%d/%d): %s', label, failures, max_failures, exc)
                if failures >= max_failures:
                    log.error('[%s] tunnel URL dead — killing cloudflared for fresh registration', label)
                    proc.terminate()
                    return
            else:
                # DNS resolved but connection refused/reset — local service issue, not the tunnel
                log.debug('[%s] connection error (not DNS, not restarting): %s', label, exc)
                failures = 0
        except Exception as exc:
            log.debug('[%s] health check non-fatal error: %s', label, exc)
            failures = 0
        time.sleep(interval)


def _run_tunnel(port: str, gist_key: str, label: str) -> None:
    """Spawn a cloudflared quick tunnel for the given port, update Gist when URL appears."""
    cmd = ['cloudflared', 'tunnel', '--url', f'localhost:{port}']
    log.info('[%s] Starting: %s', label, ' '.join(cmd))

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    url_found = False
    for line in proc.stdout:
        line = line.rstrip()
        log.info('[%s] %s', label, line)
        if not url_found:
            m = URL_RE.search(line)
            if m:
                url_found = True
                _update_gist(gist_key, m.group(0))
                threading.Thread(
                    target=_health_monitor,
                    args=(m.group(0), proc, label),
                    daemon=True,
                    name=f'health-{label}',
                ).start()

    ret = proc.wait()
    log.info('[%s] cloudflared exited (code %d)', label, ret)


def main() -> None:
    _stop = threading.Event()

    def _run_and_signal(port: str, gist_key: str, label: str) -> None:
        _run_tunnel(port, gist_key, label)
        log.info('[%s] tunnel thread exiting — signalling stop', label)
        _stop.set()

    api_thread = threading.Thread(
        target=_run_and_signal,
        args=(API_PORT, 'api_base', 'api'),
        daemon=True,
        name='tunnel-api',
    )
    frontend_thread = threading.Thread(
        target=_run_and_signal,
        args=(FRONTEND_PORT, 'frontend_base', 'frontend'),
        daemon=True,
        name='tunnel-frontend',
    )

    api_thread.start()
    frontend_thread.start()

    # Block until either tunnel exits; systemd Restart=always will relaunch both
    _stop.wait()
    log.info('A tunnel thread exited — stopping')
    sys.exit(1)


if __name__ == '__main__':
    main()

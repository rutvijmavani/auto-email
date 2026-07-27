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

    ret = proc.wait()
    log.info('[%s] cloudflared exited (code %d)', label, ret)


def main() -> None:
    api_thread = threading.Thread(
        target=_run_tunnel,
        args=(API_PORT, 'api_base', 'api'),
        daemon=True,
        name='tunnel-api',
    )
    frontend_thread = threading.Thread(
        target=_run_tunnel,
        args=(FRONTEND_PORT, 'frontend_base', 'frontend'),
        daemon=True,
        name='tunnel-frontend',
    )

    api_thread.start()
    frontend_thread.start()

    # Block until both tunnels exit (cloudflared crash → systemd restarts this service)
    api_thread.join()
    frontend_thread.join()

    log.info('Both tunnels exited — stopping')
    sys.exit(1)


if __name__ == '__main__':
    main()

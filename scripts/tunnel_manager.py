"""
scripts/tunnel_manager.py
─────────────────────────
Manages the cloudflared quick tunnel.  Captures the assigned trycloudflare.com
URL from cloudflared's output and pushes the base URL to a GitHub Gist so the
Chrome extension can discover the current endpoint without a static domain.

Env vars (read from .env):
    GITHUB_PAT          GitHub personal access token (gist scope only)
    GIST_ID             ID of the GitHub Gist to update
    EXTENSION_API_PORT  Flask port (default 5000)

Run via systemd: deploy/systemd/cloudflare-tunnel.service
"""

import json
import logging
import os
import re
import subprocess
import sys

import requests
from dotenv import load_dotenv

load_dotenv()

GITHUB_PAT = os.environ.get('GITHUB_PAT', '')
GIST_ID    = os.environ.get('GIST_ID', '')
API_PORT   = os.environ.get('EXTENSION_API_PORT', '5000')

GIST_FILENAME = 'api-config.json'
URL_RE = re.compile(r'https://[a-z0-9-]+\.trycloudflare\.com')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


def _update_gist(base_url: str) -> None:
    if not GITHUB_PAT or not GIST_ID:
        log.warning('GITHUB_PAT or GIST_ID not set — Gist update skipped')
        return
    try:
        r = requests.patch(
            f'https://api.github.com/gists/{GIST_ID}',
            headers={
                'Authorization': f'token {GITHUB_PAT}',
                'Accept': 'application/vnd.github.v3+json',
            },
            json={'files': {GIST_FILENAME: {'content': json.dumps({'api_base': base_url})}}},
            timeout=15,
        )
        if r.status_code == 200:
            log.info('Gist updated → %s', base_url)
        else:
            log.error('Gist update failed: HTTP %s — %s', r.status_code, r.text[:200])
    except Exception as exc:
        log.error('Gist update error: %s', exc)


def main() -> None:
    cmd = ['cloudflared', 'tunnel', '--url', f'localhost:{API_PORT}']
    log.info('Starting: %s', ' '.join(cmd))

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
        log.info('[cf] %s', line)
        if not url_found:
            m = URL_RE.search(line)
            if m:
                url_found = True
                _update_gist(m.group(0))

    ret = proc.wait()
    log.info('cloudflared exited (code %d)', ret)
    sys.exit(ret)


if __name__ == '__main__':
    main()

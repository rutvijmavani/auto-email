"""
scripts/watch_ext.py
────────────────────
Watches chrome-extension/ for file changes and POSTs /dev-bump so the
extension's DEV_MODE polling loop picks it up and calls chrome.runtime.reload().

Usage:
    pip install watchdog requests
    python scripts/watch_ext.py
"""

import time
import sys
import os
import requests
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

API_BASE = os.environ.get('EXTENSION_API_BASE', 'http://localhost:5001')
WATCH_DIR = os.path.join(os.path.dirname(__file__), '..', 'chrome-extension')


class _Handler(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory:
            rel = os.path.relpath(event.src_path, WATCH_DIR)
            print(f'  changed: {rel}', flush=True)
            try:
                requests.post(f'{API_BASE}/dev-bump', timeout=2)
            except Exception as e:
                print(f'  [warn] /dev-bump failed: {e}', flush=True)

    on_created = on_modified


def main():
    watch_dir = os.path.abspath(WATCH_DIR)
    if not os.path.isdir(watch_dir):
        print(f'[watch_ext] directory not found: {watch_dir}')
        sys.exit(1)

    print(f'[watch_ext] watching {watch_dir}')
    print(f'[watch_ext] posting to {API_BASE}/dev-bump on change')
    print('[watch_ext] Ctrl+C to stop\n')

    obs = Observer()
    obs.schedule(_Handler(), path=watch_dir, recursive=True)
    obs.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        obs.stop()
    obs.join()


if __name__ == '__main__':
    main()

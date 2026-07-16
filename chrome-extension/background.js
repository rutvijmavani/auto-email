// chrome-extension/background.js
// Handles icon click → show overlay in the active tab.

importScripts('config.js');

// ── Gist-based API URL discovery ───────────────────────────────────────────
// Fetches the current cloudflare tunnel base URL from a GitHub Gist and caches
// it in chrome.storage.local so content.js can read it synchronously.
// Only active when GIST_CONFIG_URL is set in config.js.

async function refreshApiUrl() {
  if (!GIST_CONFIG_URL) return;
  try {
    const r = await fetch(GIST_CONFIG_URL + '?t=' + Date.now()); // bypass cache
    if (!r.ok) return;
    const { api_base } = await r.json();
    if (api_base) await chrome.storage.local.set({ api_base });
  } catch (_) {}
}

if (GIST_CONFIG_URL) {
  refreshApiUrl();
  chrome.alarms.create('refresh-api-url', { periodInMinutes: 30 });
  chrome.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name === 'refresh-api-url') refreshApiUrl();
  });
}

if (DEV_MODE) {
  // Polls /dev-ping every second; reloads extension when watch_ext.py
  // bumps the version after detecting a file change in chrome-extension/.
  (async function devReload() {
    let lastV = null;
    while (true) {
      try {
        const r = await fetch('http://localhost:5001/dev-ping');
        const { v } = await r.json();
        if (lastV !== null && v !== lastV) { chrome.runtime.reload(); return; }
        lastV = v;
      } catch (_) {}
      await new Promise(r => setTimeout(r, 1000));
    }
  })();
}

chrome.action.onClicked.addListener(async (tab) => {
  try {
    // Happy path: content script already running on an allowlisted page.
    await chrome.tabs.sendMessage(tab.id, { type: 'SHOW_OVERLAY' });
  } catch (_) {
    // Content script not present (non-allowlisted page) — inject on demand.
    try {
      // Set a flag so content.js skips the 4-second delay and shows immediately.
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        func: () => { window.__jobCaptureImmediate = true; },
      });
      await chrome.scripting.insertCSS({
        target: { tabId: tab.id },
        files: ['overlay.css'],
      });
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        files: ['config.js', 'content.js'],
      });
    } catch (err) {
      console.error('[Job Capture] Failed to inject:', err.message);
    }
  }
});

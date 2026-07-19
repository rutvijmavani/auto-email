// chrome-extension/background.js

importScripts('config.js');

// ── Gist-based API URL discovery ───────────────────────────────────────────

async function refreshApiUrl() {
  if (!GIST_CONFIG_URL) return;
  try {
    const r = await fetch(GIST_CONFIG_URL + '?t=' + Date.now());
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
    await chrome.tabs.sendMessage(tab.id, { type: 'SHOW_OVERLAY' });
  } catch (_) {
    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        func: () => { window.__jobCaptureImmediate = true; },
      });
      await chrome.scripting.insertCSS({ target: { tabId: tab.id }, files: ['overlay.css'] });
      await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['config.js', 'content.js'] });
    } catch (err) {
      console.error('[Job Capture] Failed to inject:', err.message);
    }
  }
});

// ── Apply URL capture — persistent tab tracking ────────────────────────────
//
// MV3 service workers are killed when idle and restarted on the next event.
// In-memory Maps are wiped on every restart, so we write-through to
// chrome.storage.session (RAM-backed, session-scoped, survives SW restarts).
// Reads check the in-memory cache first; on cache miss they fall back to
// session storage (handles the SW-was-killed case).

const _mem = { tabs: new Map(), pending: new Map() };  // in-memory caches
const _JOB_TAB_EXPIRY = 5 * 60 * 1000;

// Storage key helpers
const _K = {
  tab:     (id) => `jt_${id}`,
  pending: (id) => `jp_${id}`,
  draft:   (id) => `wd_${id}`,
};

// ── Generic session storage helpers ───────────────────────────────────────

async function _ssGet(key) {
  try { const r = await chrome.storage.session.get(key); return r?.[key] ?? null; }
  catch (_) { return null; }
}
async function _ssSet(key, val) {
  try { await chrome.storage.session.set({ [key]: val }); } catch (_) {}
}
async function _ssDel(key) {
  try { await chrome.storage.session.remove(key); } catch (_) {}
}

// ── Job tab helpers ────────────────────────────────────────────────────────

async function _getJobTab(tabId) {
  let entry = _mem.tabs.get(tabId);
  if (!entry) {
    entry = await _ssGet(_K.tab(tabId));
    if (entry) _mem.tabs.set(tabId, entry);
  }
  if (!entry) return null;
  if (Date.now() - entry.registeredAt > _JOB_TAB_EXPIRY) {
    _delJobTab(tabId);
    return null;
  }
  return entry;
}

function _setJobTab(tabId, data) {
  _mem.tabs.set(tabId, data);
  _ssSet(_K.tab(tabId), data);
}

function _delJobTab(tabId) {
  _mem.tabs.delete(tabId);
  _ssDel(_K.tab(tabId));
}

// ── Pending new-tab helpers ────────────────────────────────────────────────

async function _getOpener(tabId) {
  let v = _mem.pending.get(tabId);
  if (!v) { v = await _ssGet(_K.pending(tabId)); if (v) _mem.pending.set(tabId, v); }
  return v ?? null;
}

function _setPending(tabId, openerTabId) {
  _mem.pending.set(tabId, openerTabId);
  _ssSet(_K.pending(tabId), openerTabId);
}

function _delPending(tabId) {
  _mem.pending.delete(tabId);
  _ssDel(_K.pending(tabId));
}

// ── URL classifiers ────────────────────────────────────────────────────────

function _isCareerUrl(url) {
  if (!url) return false;
  return /\/(careers?|jobs?|openings?|positions?|apply)\b/.test(url) ||
    /\.(greenhouse\.io|lever\.co|ashbyhq\.com|myworkdayjobs\.com|myworkdaysite\.com|icims\.com|successfactors\.com|taleo\.net|eightfold\.ai|jobvite\.com|avature\.net|phenompeople\.com|talentbrew\.com)/.test(url);
}

function _isObviouslyNonJob(url) {
  if (!url) return true;
  return /\b(google|youtube|facebook|instagram|twitter|reddit|amazon|netflix|spotify|wikipedia|gmail|outlook|slack|notion|figma|github|stackoverflow)\.(com|org|net|co\.uk)\b/i.test(url);
}

// ── Injection helpers ──────────────────────────────────────────────────────

async function _injectIntoTab(tabId) {
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      func: () => { window.__jobCaptureImmediate = true; },
    });
    await chrome.scripting.insertCSS({ target: { tabId }, files: ['overlay.css'] });
    await chrome.scripting.executeScript({ target: { tabId }, files: ['config.js', 'content.js'] });
  } catch (_) {}
}

async function _injectWatchlistForm(tabId, sampleUrl, careerPageUrl, draft) {
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      func: (s, c, d) => {
        window.__jobCaptureSampleUrl = s;
        window.__jobCaptureCareerUrl = c;
        window.__jobCaptureDraft     = d;
      },
      args: [sampleUrl, careerPageUrl, draft],
    });
    await chrome.scripting.insertCSS({ target: { tabId }, files: ['overlay.css'] });
    await chrome.scripting.executeScript({ target: { tabId }, files: ['config.js', 'content.js'] });
  } catch (_) {}
}

// ── Message listener ───────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, sender) => {
  const tabId = sender.tab?.id;
  if (!tabId) return;

  if (msg.type === 'REGISTER_JOB_TAB') {
    _setJobTab(tabId, {
      registeredAt: Date.now(),
      mode: msg.mode || 'detail',
      careerPageUrl: msg.careerPageUrl || '',
    });
  }
  if (msg.type === 'SAVE_WATCHLIST_DRAFT')  _ssSet(_K.draft(tabId), msg.draft);
  if (msg.type === 'CLEAR_WATCHLIST_DRAFT') _ssDel(_K.draft(tabId));
});

// ── Tab lifecycle ──────────────────────────────────────────────────────────

chrome.tabs.onRemoved.addListener((tabId) => {
  _delJobTab(tabId);
  _delPending(tabId);
  _ssDel(_K.draft(tabId));
});

chrome.tabs.onCreated.addListener(async (tab) => {
  if (!tab.openerTabId) return;
  const openerEntry = await _getJobTab(tab.openerTabId);
  if (openerEntry) _setPending(tab.id, tab.openerTabId);
});

chrome.tabs.onUpdated.addListener(async (tabId, info, tab) => {
  if (info.status !== 'complete') return;

  // ── New tab opened from a job tab ────────────────────────────────────────
  const openerTabId = await _getOpener(tabId);
  if (openerTabId) {
    _delPending(tabId);
    const openerEntry = await _getJobTab(openerTabId);
    if ((openerEntry?.mode === 'listing' || openerEntry?.watchlistContext) && !_isObviouslyNonJob(tab.url)) {
      // Send apply URL back to fill "Sample Job URL" on the still-open watchlist form
      chrome.tabs.sendMessage(openerTabId, { type: 'FILL_JOB_URL', url: tab.url }).catch(() => {});
    } else {
      _injectIntoTab(tabId);
    }
    return;
  }

  // ── Same-tab navigation on a registered job tab ──────────────────────────
  const entry = await _getJobTab(tabId);
  if (!entry || !tab.url) return;

  if (entry.mode === 'listing') {
    if (_isObviouslyNonJob(tab.url)) {
      _delJobTab(tabId);
    } else {
      // Downgrade mode but mark that a watchlist form is open — used in new-tab
      // apply detection (openerEntry?.watchlistContext check below).
      _setJobTab(tabId, { ...entry, mode: 'detail', watchlistContext: true });
      const draft = await _ssGet(_K.draft(tabId));
      _injectWatchlistForm(tabId, tab.url, entry.careerPageUrl || '', draft);
    }
  } else if (!_isCareerUrl(tab.url)) {
    _delJobTab(tabId);
  } else {
    _injectIntoTab(tabId);
  }
});

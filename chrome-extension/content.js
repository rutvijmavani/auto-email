// chrome-extension/content.js

(function () {
  const OVERLAY_ID = 'job-capture-overlay';
  let _autoTimer = null;
  let _lastUrl = location.href;

  // ── Utilities ──────────────────────────────────────────────────────────────

  function titleCase(str) {
    return (str || '').replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).trim();
  }

  function escHtml(str) {
    return (str || '')
      .replace(/&/g, '&amp;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  // ── Data extraction ────────────────────────────────────────────────────────

  function extractJsonLd() {
    for (const el of document.querySelectorAll('script[type="application/ld+json"]')) {
      try {
        const data = JSON.parse(el.textContent);
        const items = Array.isArray(data) ? data : [data];
        for (const item of items) {
          if (item['@type'] === 'JobPosting') return item;
        }
      } catch (_) {}
    }
    return null;
  }

  function extractTitle() {
    const ld = extractJsonLd();
    if (ld?.title) return ld.title.trim();

    const selectors = [
      'h1.app-title',                                              // Greenhouse
      '.posting-headline h2',                                      // Lever
      'h1[data-ui="job-title"]',                                   // Ashby
      '[data-automation-id="jobPostingHeader"] h2',                // Workday
      '[data-automation-id="jobPostingHeader"] h1',
      '.job-title h1',                                             // SmartRecruiters
      '.job-details-jobs-unified-top-card__job-title h1',          // LinkedIn
      '.jobs-unified-top-card__job-title h1',
      'h1[data-testid="jobsearch-JobInfoHeader-title"]',           // Indeed
      'h1.jobsearch-JobInfoHeader-title',
      'h1',                                                        // generic
    ];

    for (const sel of selectors) {
      const el = document.querySelector(sel);
      const text = el?.textContent?.trim();
      if (text) return text;
    }

    // Strip site suffix from page title as last resort
    return document.title.split(/[|\-–—]/)[0].trim();
  }

  function extractCompany() {
    const { hostname, pathname } = window.location;
    const seg = pathname.split('/').filter(Boolean);

    if (hostname === 'boards.greenhouse.io')        return titleCase(seg[0]);
    if (hostname === 'jobs.lever.co')               return titleCase(seg[0]);
    if (hostname === 'jobs.ashbyhq.com')            return titleCase(seg[0]);
    if (hostname.endsWith('.myworkdayjobs.com'))    return titleCase(hostname.split('.')[0]);
    if (hostname === 'careers.smartrecruiters.com') return titleCase(seg[0]);

    // JSON-LD fallback (LinkedIn, Indeed)
    const ld = extractJsonLd();
    if (ld?.hiringOrganization?.name) return ld.hiringOrganization.name.trim();

    // OpenGraph fallback
    const og = document.querySelector('meta[property="og:site_name"]');
    if (og?.content) return og.content.trim();

    return '';
  }

  // ── Identity detection ─────────────────────────────────────────────────────

  function detectUser(callback) {
    try {
      chrome.identity.getAuthToken({ interactive: false }, (token) => {
        if (chrome.runtime.lastError || !token) {
          reportToServer('warning', 'identity detection failed — OAuth token unavailable', {
            error: chrome.runtime.lastError?.message || 'no token',
          });
          callback(null);
          return;
        }
        fetch('https://www.googleapis.com/oauth2/v1/userinfo', {
          headers: { Authorization: `Bearer ${token}` },
        })
          .then(r => r.json())
          .then(info => {
            const name = (info.name || '').toLowerCase();
            const match = USERS.find(u => u.nameMatch && name.includes(u.nameMatch));
            callback(match || null);
          })
          .catch(() => callback(null));
      });
    } catch (_) {
      callback(null);
    }
  }

  // ── Overlay HTML ───────────────────────────────────────────────────────────

  function buildOverlayHTML(title, company, url, detectedUser) {
    const userOptions = USERS.map(u => {
      const label = u.nameMatch ? titleCase(u.nameMatch) : `User ${u.id}`;
      const sel = detectedUser?.id === u.id ? ' selected' : '';
      return `<option value="${u.id}"${sel}>${escHtml(label)}</option>`;
    }).join('');

    return `
      <div class="jco-header">
        <span class="jco-title">Save Job</span>
        <button class="jco-close" title="Close (Esc)">&#x2715;</button>
      </div>
      <div class="jco-body">
        <div class="jco-field">
          <label>Applying as</label>
          <select name="user_id">${userOptions}</select>
        </div>
        <div class="jco-field">
          <label>Job Title</label>
          <input type="text" name="job_title" value="${escHtml(title)}" placeholder="e.g. Senior Engineer" />
        </div>
        <div class="jco-field">
          <label>Company <span class="jco-req">*</span></label>
          <input type="text" name="company" value="${escHtml(company)}" placeholder="e.g. Stripe" />
        </div>
        <div class="jco-field">
          <label>URL</label>
          <input type="text" name="job_url" value="${escHtml(url)}" readonly class="jco-url" title="${escHtml(url)}" />
        </div>
        <div class="jco-field">
          <label>Status</label>
          <select name="status">
            <option value="active">Active (Applied)</option>
            <option value="prospective">Prospective (Save for later)</option>
          </select>
        </div>
        <div class="jco-banner" style="display:none"></div>
        <button class="jco-submit">Add to Pipeline</button>
      </div>
    `;
  }

  // ── Banner helper ──────────────────────────────────────────────────────────

  function showBanner(overlay, msg, type) {
    const b = overlay.querySelector('.jco-banner');
    b.textContent = msg;
    b.className = `jco-banner jco-banner--${type}`;
    b.style.display = '';
  }

  // ── Remove overlay ─────────────────────────────────────────────────────────

  function removeOverlay() {
    document.getElementById(OVERLAY_ID)?.remove();
  }

  // ── Server-side error reporting ────────────────────────────────────────────

  function reportToServer(level, message, context) {
    try {
      const endpoint = API_ENDPOINT.replace('/add-application', '/log-error');
      const headers  = { 'Content-Type': 'application/json' };
      if (API_KEY) headers['X-API-Key'] = API_KEY;
      fetch(endpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify({ level, message, context }),
      }).catch(() => {});
    } catch (_) {}
  }

  // ── API call ───────────────────────────────────────────────────────────────

  async function postToApi(payload) {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
    try {
      const headers = { 'Content-Type': 'application/json' };
      if (API_KEY) headers['X-API-Key'] = API_KEY;
      const res = await fetch(API_ENDPOINT, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: ctrl.signal,
      });
      clearTimeout(t);
      let data;
      try { data = await res.json(); } catch (_) { data = {}; }
      return { ok: res.ok, data, status: res.status };
    } catch (err) {
      clearTimeout(t);
      return { ok: false, error: err };
    }
  }

  // ── Google Sheets fallback ─────────────────────────────────────────────────

  function postToSheet(payload) {
    return new Promise((resolve) => {
      try {
        chrome.identity.getAuthToken({ interactive: false }, async (token) => {
          if (chrome.runtime.lastError || !token) { resolve(false); return; }
          const now = new Date();
          const ts   = `${now.getMonth() + 1}/${now.getDate()}/${now.getFullYear()}`;
          const date = now.toISOString().split('T')[0];
          const userName = USERS.find(u => u.id === payload.user_id)?.nameMatch || '';
          const row  = [ts, payload.company, payload.job_url, payload.job_title || '', date, titleCase(userName)];
          const url  = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/${encodeURIComponent(SHEET_TAB)}!A1:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
          try {
            const res = await fetch(url, {
              method: 'POST',
              headers: {
                Authorization: `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
              body: JSON.stringify({ values: [row] }),
            });
            resolve(res.ok);
          } catch (_) {
            resolve(false);
          }
        });
      } catch (_) {
        resolve(false);
      }
    });
  }

  // ── Submit handler ─────────────────────────────────────────────────────────

  async function handleSubmit(overlay) {
    const get = (name) => (overlay.querySelector(`[name="${name}"]`)?.value || '').trim();

    const company   = get('company');
    const job_url   = get('job_url');
    const job_title = get('job_title') || null;
    const status    = get('status');
    const user_id   = parseInt(get('user_id'), 10);

    if (!company) {
      overlay.querySelector('[name="company"]').classList.add('jco-error');
      showBanner(overlay, 'Company name is required.', 'error');
      return;
    }

    const btn = overlay.querySelector('.jco-submit');
    btn.disabled = true;
    btn.textContent = 'Saving…';

    const payload = { company, job_url, job_title, status, user_id };
    const result  = await postToApi(payload);

    if (result.ok) {
      if (result.data.created) {
        showBanner(overlay, `Added to pipeline (ID: ${result.data.id})`, 'success');
        setTimeout(removeOverlay, 2000);
      } else {
        showBanner(overlay, `Already in pipeline (ID: ${result.data.id})`, 'duplicate');
        btn.disabled = false;
        btn.textContent = 'Add to Pipeline';
      }
      return;
    }

    // API returned an error response (reachable but failed)
    if (result.status) {
      reportToServer('error', 'API error response', { status: result.status, company, job_url });
    }

    // API unreachable or errored — try Google Sheets fallback
    reportToServer('warning', 'API unreachable — falling back to Sheets', { company, job_url });
    const saved = await postToSheet(payload);
    if (saved) {
      showBanner(overlay, 'Saved to sheet — will sync automatically', 'sheet');
      setTimeout(removeOverlay, 2500);
    } else {
      reportToServer('error', 'both API and Sheets fallback failed', { company, job_url });
      showBanner(overlay, 'Failed to save. Check your connection.', 'error');
      btn.disabled = false;
      btn.textContent = 'Add to Pipeline';
    }
  }

  // ── Show overlay ───────────────────────────────────────────────────────────

  function showOverlay() {
    // Guard: focus existing overlay instead of stacking
    const existing = document.getElementById(OVERLAY_ID);
    if (existing) {
      existing.querySelector('select, input:not([readonly])')?.focus();
      return;
    }

    const title   = extractTitle();
    const company = extractCompany();
    const url     = window.location.href;

    detectUser((detectedUser) => {
      const overlay = document.createElement('div');
      overlay.id = OVERLAY_ID;
      overlay.innerHTML = buildOverlayHTML(title, company, url, detectedUser);
      document.body.appendChild(overlay);

      overlay.querySelector('.jco-close').addEventListener('click', removeOverlay);

      overlay.querySelectorAll('input').forEach(inp => {
        inp.addEventListener('input', () => inp.classList.remove('jco-error'));
      });

      overlay.querySelector('.jco-submit').addEventListener('click', () => handleSubmit(overlay));

      overlay.querySelector('select')?.focus();
    });
  }

  // ── Timer helpers ──────────────────────────────────────────────────────────

  function armTimer() {
    clearTimeout(_autoTimer);
    if (document.visibilityState === 'hidden') return;
    _autoTimer = setTimeout(showOverlay, 4000);
  }

  function cancelTimer() {
    clearTimeout(_autoTimer);
    _autoTimer = null;
  }

  // ── SPA navigation ─────────────────────────────────────────────────────────

  function onUrlChange() {
    if (location.href === _lastUrl) return;
    _lastUrl = location.href;
    cancelTimer();
    removeOverlay();
    armTimer();
  }

  // Intercept pushState (LinkedIn, Indeed SPA navigation)
  const _origPush = history.pushState.bind(history);
  history.pushState = (...args) => { _origPush(...args); onUrlChange(); };
  window.addEventListener('popstate', onUrlChange);

  // ── Visibility change ──────────────────────────────────────────────────────

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'hidden') {
      cancelTimer();
    } else {
      // Re-arm only if not already showing overlay
      if (!document.getElementById(OVERLAY_ID)) armTimer();
    }
  });

  // ── Keyboard ───────────────────────────────────────────────────────────────

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') removeOverlay();
  });

  // ── Message listener (from background.js on manual icon click) ─────────────

  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === 'SHOW_OVERLAY') showOverlay();
  });

  // ── Init ───────────────────────────────────────────────────────────────────

  if (window.__jobCaptureImmediate) {
    showOverlay();
  } else {
    armTimer();
  }

})();

// chrome-extension/content.js

(function () {
  if (window.__jobCaptureLoaded) return;
  window.__jobCaptureLoaded = true;

  const OVERLAY_ID  = 'job-capture-overlay';
  const TOGGLE_ID   = 'job-capture-toggle';
  let _autoTimer = null;
  let _lastUrl = location.href;
  let _draftSyncTimer = null;

  // ── Utilities ──────────────────────────────────────────────────────────────

  function titleCase(str) {
    return (str || '').replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()).trim();
  }

  function escHtml(str) {
    return (str || '')
      .replace(/&/g, '&amp;')
      .replace(/'/g, '&#39;')
      .replace(/"/g, '&quot;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function _extractDomain() {
    const { hostname } = window.location;
    const CAREER_SUBS = ['careers', 'career', 'jobs', 'job', 'talent', 'work', 'apply', 'hiring', 'join', 'opportunities'];
    const parts = hostname.split('.');
    if (parts.length >= 3 && CAREER_SUBS.includes(parts[0])) {
      return parts.slice(1).join('.');
    }
    return hostname;
  }

  // ── Watchlist draft — background-mediated persistence ─────────────────────
  // Sends draft to the background service worker on every form change (debounced).
  // Background stores it in chrome.storage.session (RAM, fast) keyed by tabId,
  // and passes it back via window.__jobCaptureDraft when the apply page loads.
  // This avoids the beforeunload + async-write race that makes local.set unreliable.

  function _syncDraft(overlay) {
    clearTimeout(_draftSyncTimer);
    _draftSyncTimer = setTimeout(() => {
      if (!overlay?.isConnected) return;
      const get = (n) => (overlay.querySelector(`[name="${n}"]`)?.value || '').trim();
      chrome.runtime.sendMessage({
        type: 'SAVE_WATCHLIST_DRAFT',
        draft: { company: get('company'), domain: get('domain'), notes: get('notes'), careerPageUrl: get('career_page') },
      }).catch(() => {});
    }, 250);
  }

  function _clearDraft() {
    clearTimeout(_draftSyncTimer);
    chrome.runtime.sendMessage({ type: 'CLEAR_WATCHLIST_DRAFT' }).catch(() => {});
  }

  function _applyDraft(overlay, draft, sampleUrl) {
    if (draft) {
      const set = (n, v) => { const el = overlay.querySelector(`[name="${n}"]`); if (el && v) el.value = v; };
      set('company', draft.company);
      set('domain', draft.domain);
      set('notes', draft.notes);
      set('career_page', draft.careerPageUrl);
    }
    if (sampleUrl) {
      const inp = overlay.querySelector('[name="job_url"]');
      if (inp) { inp.value = sampleUrl; inp.focus(); }
    }
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

    if (hostname === 'boards.greenhouse.io')          return titleCase(seg[0]);
    if (hostname === 'job-boards.greenhouse.io')      return titleCase(seg[0]);
    if (hostname === 'jobs.lever.co')                 return titleCase(seg[0]);
    if (hostname === 'jobs.ashbyhq.com')              return titleCase(seg[0]);
    if (hostname.endsWith('.myworkdayjobs.com'))      return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.myworkdaysite.com'))      return titleCase(hostname.split('.')[0]);
    if (hostname === 'careers.smartrecruiters.com')   return titleCase(seg[0]);
    if (hostname.endsWith('.icims.com'))              return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.successfactors.com'))     return titleCase(seg[0]);
    if (hostname.endsWith('.taleo.net'))              return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.eightfold.ai'))           return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.avature.net'))            return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.phenompeople.com'))       return titleCase(hostname.split('.')[0]);
    if (hostname.endsWith('.talentbrew.com'))         return titleCase(hostname.split('.')[0]);
    if (hostname === 'jobs.jobvite.com')              return titleCase(seg[0]);
    if (hostname === 'myjobs.adp.com')               return titleCase(seg[1] || seg[0]);

    // JSON-LD fallback (LinkedIn, Indeed)
    const ld = extractJsonLd();
    if (ld?.hiringOrganization?.name) return ld.hiringOrganization.name.trim();

    // OpenGraph fallback
    const og = document.querySelector('meta[property="og:site_name"]');
    if (og?.content) return og.content.trim();

    return '';
  }

  function detectPageMode() {
    const { hostname, pathname } = window.location;

    const ATS_HINTS = [
      'greenhouse.io', 'lever.co', 'ashbyhq.com', 'smartrecruiters.com',
      'myworkdayjobs.com', 'myworkdaysite.com', 'oraclecloud.com', 'icims.com',
      'successfactors.com', 'jobs2web.com', 'jobvite.com', 'taleo.net',
      'eightfold.ai', 'avature.net', 'phenompeople.com', 'talentbrew.com',
      'jibecdn.com', 'myjobs.adp.com',
    ];
    const CAREER_SUBDOMAINS = ['careers', 'career', 'jobs', 'job', 'talent', 'work', 'apply', 'hiring', 'join', 'opportunities'];
    const CAREER_PATHS = ['/careers', '/jobs', '/openings', '/positions', '/open-roles', '/join-us', '/join', '/work-with-us', '/opportunities'];
    const CAREER_TERMS = ['careers', 'open roles', 'openings', "we're hiring", 'join our team', 'join us', 'current openings', 'job openings'];

    let score = 0;
    let hasJobPosting = false;

    // ── JSON-LD ────────────────────────────────────────────────────────────
    for (const el of document.querySelectorAll('script[type="application/ld+json"]')) {
      try {
        const data = JSON.parse(el.textContent);
        const items = Array.isArray(data) ? data : [data];
        for (const item of items) {
          if (item['@type'] === 'JobPosting') { hasJobPosting = true; score += 5; }
        }
      } catch (_) {}
    }

    // ── URL signals ────────────────────────────────────────────────────────
    const subdomain = hostname.split('.')[0].toLowerCase();
    if (CAREER_SUBDOMAINS.includes(subdomain)) score += 3;
    const pathLower = pathname.toLowerCase();
    if (CAREER_PATHS.some(p => pathLower === p || pathLower.startsWith(p + '/'))) score += 2;

    // ── ATS fingerprints in attributes + iframes ───────────────────────────
    const foundAts = new Set();
    const SCAN_ATTRS = ['src', 'href', 'action', 'data-src', 'data-href', 'data-url', 'data-apply-url', 'data-job-url'];
    const attrSelector = SCAN_ATTRS.map(a => '[' + a + ']').join(',');
    atsLoop: for (const el of document.querySelectorAll(attrSelector)) {
      for (const attr of SCAN_ATTRS) {
        const val = el.getAttribute(attr) || '';
        for (const hint of ATS_HINTS) {
          if (!foundAts.has(hint) && val.includes(hint)) {
            foundAts.add(hint);
            score += 4;
          }
        }
        if (foundAts.size === ATS_HINTS.length) break atsLoop;
      }
    }

    // ── Inline script content ──────────────────────────────────────────────
    for (const script of document.querySelectorAll('script:not([src])')) {
      const content = (script.textContent || '').toLowerCase();
      for (const hint of ATS_HINTS) {
        if (!foundAts.has(hint) && content.includes(hint)) {
          foundAts.add(hint);
          score += 4;
        }
      }
      if (!foundAts.has('_sf') && (content.includes('ssocompanyid') || content.includes('j2w.init'))) {
        foundAts.add('_sf');
        score += 4;
      }
    }

    // ── DOM structure ──────────────────────────────────────────────────────
    const jobCards = document.querySelectorAll(
      '[data-job-id],[data-req-id],.job-card,.job-listing,.job-item,.opening,.position,.role-card,[class*="JobCard"],[class*="job-card"],[class*="job-listing"]'
    );
    if (jobCards.length >= 3) score += 3;
    else if (jobCards.length >= 1) score += 1;

    // ── Page title / meta ──────────────────────────────────────────────────
    const titleText = document.title.toLowerCase();
    const metaDesc = (document.querySelector('meta[name="description"]')?.content || '').toLowerCase();
    if (CAREER_TERMS.some(t => titleText.includes(t) || metaDesc.includes(t))) score += 2;

    // ── Apply button (signals detail page) ────────────────────────────────
    const APPLY_TEXTS = ['apply now', 'apply for this role', 'apply for this job', 'quick apply', 'apply today', 'start application', 'apply for job'];
    let hasApplyButton = false;
    for (const el of document.querySelectorAll('a,button')) {
      const text = (el.textContent || '').trim().toLowerCase();
      if (text === 'apply' || APPLY_TEXTS.some(t => text === t || text.startsWith(t))) { hasApplyButton = true; break; }
    }

    if (score < 2) return { mode: 'none', score };
    if (hasJobPosting) return { mode: 'detail', score, hasApplyButton };

    // ── Edge case #2: URL depth for known ATS listing vs detail ────────────
    const isKnownAts = ATS_HINTS.some(h => hostname.includes(h));
    if (isKnownAts) {
      const segs = pathname.split('/').filter(Boolean);
      const lastSeg = segs[segs.length - 1] || '';
      const looksLikeId = /^[0-9]{4,}$/.test(lastSeg) || /^[a-f0-9-]{8,}$/.test(lastSeg) || lastSeg.length > 20;
      if (segs.length >= 3 || (segs.length >= 2 && looksLikeId)) return { mode: 'detail', score, hasApplyButton };
      if (segs.length <= 1 || (segs.length === 2 && !hasApplyButton)) return { mode: 'listing', score };
    }

    if (hasApplyButton) return { mode: 'detail', score, hasApplyButton };
    return { mode: 'listing', score };
  }

  function extractCompanyForListing() {
    const og = document.querySelector('meta[property="og:site_name"]');
    if (og?.content) return og.content.trim();

    const ld = extractJsonLd();
    if (ld?.hiringOrganization?.name) return ld.hiringOrganization.name.trim();

    const { hostname } = window.location;
    const CAREER_SUBS = ['careers', 'career', 'jobs', 'job', 'talent', 'work', 'apply', 'hiring', 'join', 'opportunities'];
    const parts = hostname.split('.');
    if (parts.length >= 3 && CAREER_SUBS.includes(parts[0])) return titleCase(parts[1]);

    const m = document.title.match(/(?:careers?\s+at\s+|jobs?\s+at\s+|work\s+at\s+)([^|\-–—]+)/i);
    if (m) return m[1].trim();

    return titleCase(parts[parts.length >= 2 ? parts.length - 2 : 0]);
  }

  // ── Identity detection ─────────────────────────────────────────────────────

  function detectUser(callback) {
    try {
      chrome.identity.getAuthToken({ interactive: false }, (token) => {
        if (chrome.runtime.lastError || !token) { callback(null); return; }
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
        <button class="jco-switch" type="button">Add company to watchlist instead</button>
      </div>
    `;
  }

  function buildProspectiveHTML(company, careerUrl, domain) {
    return `
      <div class="jco-header">
        <span class="jco-title">Add to Watchlist</span>
        <button class="jco-close" title="Close (Esc)">&#x2715;</button>
      </div>
      <div class="jco-body">
        <div class="jco-badge">Prospective Company</div>
        <div class="jco-field">
          <label>Company <span class="jco-req">*</span></label>
          <input type="text" name="company" value="${escHtml(company)}" placeholder="e.g. Stripe" />
        </div>
        <div class="jco-field">
          <label>Career Page URL</label>
          <input type="text" name="career_page" value="${escHtml(careerUrl)}" readonly class="jco-url" title="${escHtml(careerUrl)}" />
        </div>
        <div class="jco-field">
          <label>Domain</label>
          <input type="text" name="domain" value="${escHtml(domain)}" placeholder="e.g. stripe.com" />
        </div>
        <div class="jco-field">
          <label>Sample Job URL <span class="jco-hint">(optional — helps ATS detection)</span></label>
          <input type="text" name="job_url" value="" placeholder="paste a job listing URL" />
        </div>
        <div class="jco-field">
          <label>Notes <span class="jco-hint">(optional)</span></label>
          <input type="text" name="notes" value="" placeholder="e.g. fast-growing startup" />
        </div>
        <div class="jco-banner" style="display:none"></div>
        <button class="jco-submit">Add to Watchlist</button>
        <button class="jco-switch" type="button">Save a specific job instead</button>
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

  // ── Remove / hide overlay ──────────────────────────────────────────────────

  // ── Toggle button (persistent reopen handle) ──────────────────────────────

  function _ensureToggle() {
    if (document.getElementById(TOGGLE_ID)) return;
    const btn = document.createElement('button');
    btn.id = TOGGLE_ID;
    btn.title = 'Save this job to pipeline';
    btn.textContent = '+';
    btn.addEventListener('click', showOverlay);
    document.body.appendChild(btn);
  }

  function _showToggle() {
    _ensureToggle();
    const t = document.getElementById(TOGGLE_ID);
    if (t) t.style.display = '';
  }

  function _hideToggle() {
    const t = document.getElementById(TOGGLE_ID);
    if (t) t.style.display = 'none';
  }

  // hideOverlay: user explicitly closed — keep DOM so reopening is instant
  // and doesn't need a new detectUser() round-trip.
  function hideOverlay() {
    const el = document.getElementById(OVERLAY_ID);
    if (el) el.style.display = 'none';
    _clearDraft(); // user dismissed — don't restore on next page
    _showToggle();
  }

  // removeOverlay: navigation or post-submit — purge so next showOverlay()
  // builds a fresh form with the new page's job data.
  function removeOverlay() {
    document.getElementById(OVERLAY_ID)?.remove();
    _showToggle();
    // intentionally does NOT clear draft — called during navigation,
    // and we want the draft available on the next page load.
  }

  // ── API URL resolution ─────────────────────────────────────────────────────
  // Returns the base URL (no trailing slash, no path).
  // Priority: Gist-cached cloudflare URL → API_BASE from config.js.

  async function _getApiBase() {
    if (GIST_CONFIG_URL) {
      const { api_base } = await chrome.storage.local.get('api_base');
      if (api_base) return api_base;
    }
    return API_BASE;
  }

  // ── API call ───────────────────────────────────────────────────────────────

  async function postToApi(payload) {
    const base = await _getApiBase();
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
    try {
      const headers = {
        'Content-Type': 'application/json',
      };
      if (API_KEY) headers['X-API-Key'] = API_KEY;
      const res = await fetch(base + '/add-application', {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: ctrl.signal,
      });
      clearTimeout(t);
      return { ok: true, data: await res.json(), status: res.status };
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
          const row  = [ts, payload.company, payload.job_url, payload.job_title || '', date];
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

  function postToProspectiveSheet(payload) {
    return new Promise((resolve) => {
      try {
        chrome.identity.getAuthToken({ interactive: false }, async (token) => {
          if (chrome.runtime.lastError || !token) { resolve(false); return; }
          const now = new Date();
          const ts = `${now.getMonth()+1}/${now.getDate()}/${now.getFullYear()} ` +
                     `${now.getHours()}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`;
          // columns: Timestamp|Company|Job URL|Domain|Career Page URL|XML URL|Listing Curl|Detail Curl|Notes
          const row = [ts, payload.company, payload.job_url || '', payload.domain || '',
                       payload.career_page || '', '', '', '', payload.notes || ''];
          const url = `https://sheets.googleapis.com/v4/spreadsheets/${SHEET_ID}/values/` +
                      `${encodeURIComponent(PROSPECTIVE_SHEET_TAB)}!A1:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
          const ctrl = new AbortController();
          const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
          try {
            const res = await fetch(url, {
              method: 'POST',
              headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
              body: JSON.stringify({ values: [row] }),
              signal: ctrl.signal,
            });
            clearTimeout(t);
            resolve(res.ok);
          } catch (_) { clearTimeout(t); resolve(false); }
        });
      } catch (_) { resolve(false); }
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

    // API unreachable — try Google Sheets fallback
    const saved = await postToSheet(payload);
    if (saved) {
      showBanner(overlay, 'Saved to sheet — will sync automatically', 'sheet');
      setTimeout(removeOverlay, 2500);
    } else {
      showBanner(overlay, 'Failed to save. Check your connection.', 'error');
      btn.disabled = false;
      btn.textContent = 'Add to Pipeline';
    }
  }

  async function handleProspectiveSubmit(overlay) {
    const get = (name) => (overlay.querySelector(`[name="${name}"]`)?.value || '').trim();
    const company = get('company');
    if (!company) {
      overlay.querySelector('[name="company"]').classList.add('jco-error');
      showBanner(overlay, 'Company name is required.', 'error');
      return;
    }
    const btn = overlay.querySelector('.jco-submit');
    btn.disabled = true;
    btn.textContent = 'Saving…';
    const payload = {
      company,
      career_page: get('career_page'),
      domain: get('domain'),
      job_url: get('job_url'),
      notes: get('notes'),
    };
    const saved = await postToProspectiveSheet(payload);
    if (saved) {
      _clearDraft(); // submitted — no need to restore on next page
      showBanner(overlay, 'Added to watchlist — will be scanned automatically', 'success');
      setTimeout(removeOverlay, 2500);
    } else {
      showBanner(overlay, 'Failed to save. Check your connection.', 'error');
      btn.disabled = false;
      btn.textContent = 'Add to Watchlist';
    }
  }

  // ── Show overlay ───────────────────────────────────────────────────────────

  function showOverlay() {
    _hideToggle();
    const existing = document.getElementById(OVERLAY_ID);
    if (existing) {
      existing.style.display = '';
      existing.querySelector('select, input:not([readonly])')?.focus();
      return;
    }

    const { mode, score } = detectPageMode();

    if (mode === 'listing') {
      const company    = extractCompanyForListing();
      const careerUrl  = window.location.href;
      const domain     = _extractDomain();
      const overlay    = document.createElement('div');
      overlay.id       = OVERLAY_ID;
      overlay.innerHTML = buildProspectiveHTML(company, careerUrl, domain);
      document.body.appendChild(overlay);
      _wireWatchlistOverlay(overlay);
      return;
    }

    // mode === 'detail' or 'none'
    _showJobForm(mode === 'none');
  }

  function _showJobForm(lowConfidence = false) {
    const title   = extractTitle();
    const company = extractCompany();
    const url     = window.location.href;
    detectUser((detectedUser) => {
      const overlay    = document.createElement('div');
      overlay.id       = OVERLAY_ID;
      overlay.innerHTML = buildOverlayHTML(title, company, url, detectedUser);
      document.body.appendChild(overlay);
      overlay.querySelector('.jco-close').addEventListener('click', hideOverlay);
      overlay.querySelectorAll('input').forEach(inp =>
        inp.addEventListener('input', () => inp.classList.remove('jco-error'))
      );
      overlay.querySelector('.jco-submit').addEventListener('click', () => handleSubmit(overlay));
      overlay.querySelector('.jco-switch').addEventListener('click', () => _switchToWatchlistForm());
      if (lowConfidence) showBanner(overlay, "Couldn't confirm this is a job page — verify details before saving.", 'duplicate');
      overlay.querySelector('select')?.focus();
    });
  }

  function _switchToJobForm() {
    const el = document.getElementById(OVERLAY_ID);
    if (el) el.remove();
    _showJobForm();
  }

  function _switchToWatchlistForm() {
    const el = document.getElementById(OVERLAY_ID);
    if (el) el.remove();
    const company   = extractCompanyForListing();
    const careerUrl = window.__jobCaptureCareerUrl || window.location.href;
    const domain    = _extractDomain();
    const overlay   = document.createElement('div');
    overlay.id      = OVERLAY_ID;
    overlay.innerHTML = buildProspectiveHTML(company, careerUrl, domain);
    document.body.appendChild(overlay);
    _wireWatchlistOverlay(overlay);
  }

  function _wireWatchlistOverlay(overlay) {
    overlay.querySelector('.jco-close').addEventListener('click', hideOverlay);
    overlay.querySelectorAll('input').forEach(inp =>
      inp.addEventListener('input', () => { inp.classList.remove('jco-error'); _syncDraft(overlay); })
    );
    overlay.querySelector('.jco-submit').addEventListener('click', () => handleProspectiveSubmit(overlay));
    overlay.querySelector('.jco-switch').addEventListener('click', () => _switchToJobForm());
    overlay.querySelector('input:not([readonly])')?.focus();
    _syncDraft(overlay); // save initial state immediately
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

    // If the watchlist form is currently open, capture the new URL into the
    // Sample Job URL field instead of closing — user navigated to a job detail
    // from the listing page in the same tab (SPA navigation).
    const overlay = document.getElementById(OVERLAY_ID);
    const isWatchlistForm = overlay && overlay.querySelector('[name="career_page"]');
    if (isWatchlistForm) {
      _syncDraft(overlay); // flush any pending debounced sync before URL changes
      const jobUrlInput = overlay.querySelector('[name="job_url"]');
      if (jobUrlInput && !jobUrlInput.value) {
        jobUrlInput.value = location.href;
        jobUrlInput.focus();
      }
      return; // keep watchlist overlay open
    }

    removeOverlay();
    armTimer();
  }

  // Intercept pushState/replaceState via a main-world script injection.
  // Isolated-world assignment to history.pushState does not intercept the
  // page's own SPA navigation calls in Chrome MV3.
  (function () {
    const s = document.createElement('script');
    s.textContent = `(function(){['pushState','replaceState'].forEach(function(m){
      var orig=history[m].bind(history);
      history[m]=function(){orig.apply(history,arguments);
        window.postMessage({type:'__jcp_nav'},'*');};});})();`;
    (document.head || document.documentElement).appendChild(s);
    s.remove();
  })();
  window.addEventListener('message', function (e) {
    if (e.source === window && e.data && e.data.type === '__jcp_nav') onUrlChange();
  });
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
    if (e.key === 'Escape') hideOverlay();
  });

  // ── Message listener (from background.js on manual icon click) ─────────────

  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === 'SHOW_OVERLAY') showOverlay();
    if (msg.type === 'FILL_JOB_URL') {
      // URL captured from a new tab opened from this listing page
      const overlay = document.getElementById(OVERLAY_ID);
      if (!overlay) return;
      const input = overlay.querySelector('[name="job_url"]');
      if (input) { input.value = msg.url; input.classList.remove('jco-error'); input.focus(); }
    }
  });

  // ── Init ───────────────────────────────────────────────────────────────────

  _ensureToggle();
  const _initMode = detectPageMode().mode;
  // Skip REGISTER_JOB_TAB when background already injected us to show the watchlist form —
  // background set the correct tab state (mode + careerPageUrl) before injecting; overwriting
  // it here would corrupt the stored careerPageUrl with the apply-page URL.
  if (!window.__jobCaptureSampleUrl) {
    chrome.runtime.sendMessage({ type: 'REGISTER_JOB_TAB', mode: _initMode, careerPageUrl: location.href }).catch(() => {});
  }
  if (window.__jobCaptureSampleUrl) {
    // Full-page same-tab navigation from a listing page.
    // Draft was saved to background (chrome.storage.session) on every form change;
    // background retrieved it and passed it here synchronously before injecting us.
    _switchToWatchlistForm();
    _applyDraft(document.getElementById(OVERLAY_ID), window.__jobCaptureDraft || null, window.__jobCaptureSampleUrl);
  } else if (window.__jobCaptureImmediate) {
    showOverlay();
  } else {
    armTimer();
  }

})();

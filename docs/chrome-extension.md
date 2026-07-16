# Chrome Extension — Job Capture

## Purpose

A Chrome extension that captures job posting data (title, company, JD URL) directly
from your browser and inserts it into the `applications` table via the existing
`add_application()` function. Eliminates manual Google Form entry when saving or
logging a job you applied to.

---

## Architecture

```text
Chrome Extension (laptop)
        │
        │  POST /add-application  (JSON)
        │  http://localhost:5001   ← SSH tunnel (testing)
        │  https://yourdomain.com  ← production
        ▼
Flask API  —  api.py  (Oracle VM, port 5000)
        │
        │  calls
        ▼
db/applications.py → add_application() → PostgreSQL
```

---

## Deployment Phases

### Phase 1 — Testing via SSH tunnel
Run this on your laptop before using the extension:
```bash
ssh -L 5001:localhost:5000 ubuntu@<VM-IP> -N
```
- Flask runs on VM port 5000
- Tunnel forwards your laptop's port 5001 → VM port 5000
- Extension calls `http://localhost:5001` — Chrome treats localhost as secure, no HTTPS needed
- Tunnel drops if terminal closes; run in a background session or tmux

### Phase 2 — Production via domain (Option C)
Once your domain is pointed at the VM and SSL is configured:
- Extension calls `https://yourdomain.com/add-application`
- No tunnel needed; works from any network

---

## Installation — Multiple Laptops

The extension is not published to the Chrome Web Store. It loads as an **unpacked
extension** in Chrome developer mode — a 2-minute setup per laptop:

1. Open `chrome://extensions`
2. Toggle **Developer mode** on (top-right)
3. Click **Load unpacked** → select the `chrome-extension/` folder from the repo
4. Done

**The repo does the heavy lifting.** Since `chrome-extension/` lives inside the
project repo, any laptop with the repo cloned already has the extension files.
`git pull` on the new laptop → Load unpacked. No copying files around.

**No per-device settings to configure.** User identity is detected automatically
from the signed-in Google account display name (see §7). The only one-time setup
is the Google Cloud Console OAuth registration — done once by the developer,
not per user or per device.

**If the manual setup becomes annoying** (3+ laptops regularly): publish as an
unlisted private app on the Chrome Web Store ($5 one-time developer fee, quick review).
It then installs and auto-updates across all Chrome devices on sign-in.

---

## Design Decisions

### 1. UI approach — Injected overlay (not Chrome action popup)

The extension injects a floating panel directly into the job page's DOM rather than
using Chrome's built-in action popup (`popup.html`). 

**Why:** Chrome MV3 restricts programmatic popup opening (`chrome.action.openPopup()`
is unreliable for auto-triggering). An injected overlay works identically for both
auto-trigger and manual-trigger flows, and doesn't require the user to click the
extension icon for it to appear.

The overlay is a fixed-position `<div>` injected at the bottom-right of the page.
It can be dismissed with Escape or a close button.

### 2. Trigger mechanism

**Auto-trigger (known ATS pages):**
- Content script loads only on allowlisted ATS domains (enforced by `manifest.json`)
- Starts a **4-second** focus timer on page load
- If the tab stays focused for 4 seconds → overlay appears pre-filled
- If tab loses focus or user navigates away → timer cancels, nothing happens
- Implementation: `setTimeout` + `document.addEventListener('visibilitychange', ...)`

**Manual trigger (any page, including non-allowlisted career sites):**
- User clicks the extension icon
- `background.js` receives the click via `chrome.action.onClicked`
- Background uses `chrome.scripting.executeScript` to inject `content.js` into
  the active tab on demand
- `content.js` extracts data and immediately injects the overlay (no 4s delay)

### 3. Data extraction — priority order

**Job Title:**
1. JSON-LD `<script type="application/ld+json">` with `@type: "JobPosting"` → `title`
2. ATS-specific CSS selector (lookup table keyed by hostname)
3. `document.title` — almost always contains the job title as the first segment

**Company Name:**
Extracted from URL structure for known ATS platforms:

| ATS | URL pattern | Extracted from |
|-----|-------------|---------------|
| Greenhouse | `boards.greenhouse.io/stripe/jobs/123` | `pathname.split('/')[1]` |
| Lever | `jobs.lever.co/stripe/uuid` | `pathname.split('/')[1]` |
| Ashby | `jobs.ashbyhq.com/stripe/uuid` | `pathname.split('/')[1]` |
| Workday | `stripe.wd1.myworkdayjobs.com` | `hostname.split('.')[0]` |
| SmartRecruiters | `careers.smartrecruiters.com/Stripe/` | `pathname.split('/')[1]` |
| LinkedIn | JSON-LD `hiringOrganization.name` | structured data |
| Indeed | JSON-LD `hiringOrganization.name` | structured data |

Fallback: `<meta property="og:site_name">` content attribute.

All extracted company names go through `.replace(/-/g, ' ')` and title-case
normalization before display in the overlay (user can edit before submitting).

**URL:**
`window.location.href` — always the JD page URL (not the apply URL). Preferred
because it contains the job description and is stable.

### 4. Default application status

The overlay shows a status dropdown. Default is **Active (Applied)** since the most
common use case is logging a job you just applied to. Options:

| Dropdown label | DB value | Use case |
|---------------|----------|---------|
| Active (Applied) | `active` | Just applied — pipeline starts working it |
| Prospective (Save for later) | `prospective` | Interested but haven't applied yet |

### 6. Duplicate URL handling

`add_application()` returns `(app_id, created)` where `created=False` means the
URL already exists. The overlay shows:

- `created=True` → green: **"Added to pipeline (ID: 42)"** → auto-closes in 2s
- `created=False` → yellow: **"Already in pipeline (ID: 42)"** → stays open
- VM unreachable → yellow: **"Saved to sheet — will sync automatically"** → auto-closes in 2s
- Both VM and sheet unreachable → red: **"Failed to save. Check your connection."**
- Missing required field → inline field highlight (red border + label below field)

### 7. User identification — who applied?

The pipeline is multi-user and both users may share the same device. Per-device
or per-profile config doesn't work for a shared laptop.

**Design: auto-detect from Google display name, with manual override.**

The extension calls `chrome.identity.getAuthToken({interactive: false})` silently,
then hits `https://www.googleapis.com/oauth2/v1/userinfo` to get the display name
of whoever is signed into Chrome (e.g. `"Rutvi Mavani"`). It matches that name
against the user list in `config.js` using a case-insensitive substring check.

```js
// config.js
const USERS = [
  { id: 1, nameMatch: "rutvi" },
  { id: 2, nameMatch: "other persons first name" }
];
```

The overlay always shows an "Applying as" dropdown, pre-selected automatically:

```text
Applying as: [ Rutvi ▼ ]   ← auto-detected, correct 99% of the time
```

User can change it from the dropdown if detection picked the wrong person.

**Why display name over email:**
- You use multiple Google accounts — the active account in Chrome may be a work
  account or secondary account with a different email
- Display name ("Rutvi Mavani") stays consistent across all your accounts
- Substring match (`"rutvi"` in `"Rutvi Mavani"`) is robust to name format variations

**Fallback if detection fails** (not signed in, token error, name not in config):
- Dropdown shows with no pre-selection — user picks manually
- No crash, no broken state

**One-time setup required:**
- Register the extension in Google Cloud Console as an OAuth 2.0 client
  (type: Chrome Extension) — free, ~15 minutes, done once by the developer
- Add the client ID to `manifest.json` under `"oauth2"`
- After this: completely automatic on every device, no per-user configuration

**`user_id` is sent directly in the request body** — server needs no mapping logic,
just stores whatever `user_id` the extension sends.

### 8. Authentication

**Testing phase (SSH tunnel):** No auth. The SSH tunnel is the security boundary.

**Production phase (domain):** Single shared API key. Stored in `.env` on the VM
as `EXTENSION_API_KEY` and hardcoded in `chrome-extension/config.js` (which lives
in your private repo — no more exposed than `.env`). Flask checks the
`X-API-Key` header; mismatches return `401`. If `EXTENSION_API_KEY` is not set,
the check is skipped (testing env).

Auth and identity are now separate concerns:
- **Auth** (is this request from our extension?): shared API key
- **Identity** (who is applying?): `user_id` field in the request body, set by the user selector

### 10. Fetch timeout + Google Sheets fallback

`fetch()` to the Flask API has a **10-second timeout** via `AbortController`. On
timeout or network error, the extension falls back to writing the job row directly
to the existing Google Sheet ("Responses" tab, same sheet `form_sync.py` reads).
The existing cron that runs `form_sync.py` picks it up on its next run — no new
cron or tab needed.

**Fallback column format** (matches existing Google Form responses):
```text
Timestamp | Company Name | Job URL | Job Title | Applied Date | User Name
```

`form_sync.py` reads column F (`COL_USER_NAME = 5`), maps the display name to
`user_id` via `_USER_NAME_MAP` (e.g. `{'alice': 1, 'bob': 2}`), and passes it to
`add_application(user_id=...)`. Load the actual mapping from the `USER_NAME_MAP`
environment variable (JSON string) in `.env` — do not commit real names to the
repo. Rows with a blank or unrecognised column F are **skipped** and left in
the sheet for manual review.

The extension needs `https://www.googleapis.com/auth/spreadsheets` scope added to
the OAuth config in `manifest.json` (alongside `userinfo.profile` already there).

### 11. SPA navigation (LinkedIn, Indeed)

LinkedIn Jobs and Indeed are single-page apps — clicking from one job to another
changes the URL via `pushState` but does not reload the page. The content script
stays alive but the 4-second timer already fired and won't re-trigger naturally.

**Fix:** Inject a tiny inline script into the **main world** that wraps
`pushState`/`replaceState` and posts a message back to the content script via
`window.postMessage`. The content script listens for those messages and calls
`onUrlChange()`.

```js
// Inject into main world — isolated-world override does NOT intercept
// the page's own history calls in Chrome MV3.
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
```

**Why not assign directly to `history.pushState`?** In Chrome MV3 content scripts
run in an isolated JavaScript world. Assigning to `history.pushState` in the
isolated world creates a shadow property that doesn't affect the page's own
`window.history` object, so the page's SPA navigation bypasses the hook entirely.
The inline script injection runs in the main world where the page's history lives.

### 12. Overlay close and reopen

**Two removal functions with different semantics:**

- `hideOverlay()` — sets `display:none` on the overlay div. Used by the close
  button (×) and Escape key. Keeps the DOM element alive so reopening is instant.
- `removeOverlay()` — fully removes the element from the DOM. Used on SPA
  navigation and after a successful submit. Forces a fresh form build with new
  page data on the next `showOverlay()` call.

**Reopen:** clicking the extension icon after closing sends a `SHOW_OVERLAY`
message from `background.js` → `showOverlay()` → finds the hidden element →
un-hides it and re-focuses the first field. No new network round-trip or
`detectUser()` call needed.

**Double-inject guard:** if `showOverlay()` is called while the overlay is
already visible (e.g. auto-trigger fires and user also clicks the icon),
it just re-focuses the first field rather than stacking a second overlay:

```js
function showOverlay() {
  const existing = document.getElementById('job-capture-overlay');
  if (existing) {
    existing.style.display = '';        // un-hide if hidden
    existing.querySelector('select, input:not([readonly])')?.focus();
    return;
  }
  // ... build and inject new overlay
}
```

### 13. Endpoint URL — testing vs production

The API endpoint is controlled by a single flag in `config.js`:

```js
const PROD = false;  // ← flip to true when moving to production
const API_URL = PROD
  ? "https://yourdomain.com/add-application"
  : "http://localhost:5001/add-application";
```

Switching environments: edit `PROD`, then click **Reload** on the extension card
in `chrome://extensions`. No other changes needed.

### 14. `api.py` process management on the VM

`api.py` runs as a `systemd` service alongside the main pipeline — always-on,
auto-restarts on crash, survives VM reboots.

**Local / SSH-tunnel testing** — Flask's built-in dev server is fine:
```bash
python api.py          # runs on 0.0.0.0:5000
```

**Production** — the systemd unit (`deploy/systemd/pipeline-api.service`) launches
via **Gunicorn** (a production WSGI server):
```bash
gunicorn -w 2 -b 0.0.0.0:5000 api:app
```
After changing the service file, re-stage and restart manually — `deploy.sh` does
not manage `pipeline-api`:
```bash
sudo bash deploy/install-systemd.sh   # re-stage the updated unit file
bash deploy/deploy.sh                 # pulls code, installs gunicorn, daemon-reload
sudo systemctl restart pipeline-api   # deploy.sh does not restart this service
```

The systemd unit file lives at `/etc/systemd/system/pipeline-api.service`. Starting
it: `sudo systemctl start pipeline-api`. Logs: `journalctl -u pipeline-api -f`.

### 15. CORS

Flask validates two headers together before setting `Access-Control-Allow-Origin`:

| Header | Who sets it | Value |
|---|---|---|
| `Origin` | Browser (cannot be overridden by JS) | `chrome-extension://<real-ext-id>` |
| `X-Extension-Id` | `content.js` via `chrome.runtime.id` | `<same-ext-id>` |

The server only echoes the origin back when `Origin == f"chrome-extension://{X-Extension-Id}"`. If they don't agree (e.g. a webpage tries to fake it), no CORS header is set and the browser blocks the response.

**Why this is dynamic:** the extension always knows its own ID via `chrome.runtime.id` and sends it on every request. The server validates consistency rather than comparing against a hardcoded value — so reinstalling the extension and getting a new ID just works automatically.

**Why `Origin` can't be spoofed by a webpage:** browsers set the `Origin` header from the actual document origin and forbid JavaScript from overriding it. A webpage at `https://evil.com` cannot produce `Origin: chrome-extension://...` no matter what JS it runs.

Implemented via a manual `after_request` hook in `api.py` (no `flask-cors` dependency needed). `X-Extension-Id` is listed in `Access-Control-Allow-Headers` so the preflight OPTIONS request approves it.

### 16. Logging and observability

**Server-side (`api.py`):**
- `init_logging('api')` at startup → writes to `logs/api.log`
- `TimedRotatingFileHandler` rotates at midnight → `api.log.YYYY-MM-DD`
- Rotation backups deleted after **14 days** by `_cleanup_old_logs()` (same rule as `scheduler.log.*`)
- `cleanup_logs_if_due()` called on every incoming request via `@app.before_request` (no-op until 24h elapsed)
- `log_monitor.py` (cron every 15 min) scans `logs/api.log` automatically — no config change needed
- All file output is **JSON format** (one object per line); console output when running interactively stays human-readable

**Client-side (`content.js`):**
- `reportToServer(level, message, context)` — fire-and-forget `POST /log-error` to the API; never throws, failure is silently swallowed so it never affects the user flow
- Called on:
  - OAuth token unavailable → `warning` ("identity detection failed")
  - API call returned non-ok HTTP status → `error` ("API error response")  
  - API unreachable, falling back to Sheets → `warning` ("API unreachable — falling back to Sheets")
  - Both API and Sheets failed → `error` ("both API and Sheets fallback failed")

**`POST /log-error` endpoint (`api.py`):**
```json
POST /log-error
{ "level": "error|warning|info", "message": "...", "context": { ... } }
→ 204 No Content
```
The endpoint logs `[extension] <message> | <context>` at the specified level.
The log monitor then alerts on new `ERROR` or `WARNING` lines within 15 minutes.

### 17. Prospective Company Capture — Design (not yet implemented)

**Goal:** Expand the extension to also bookmark company career pages into the
`prospective_companies` table, not just log job applications.

---

#### Dual-mode overlay

A single overlay with a **tab toggle at the top** instead of two separate overlays:

```text
[ Save Job ● ]  [ Save Company ]
```

The active tab controls which fields are shown and which API endpoint is called.
User can switch at any time before submitting.

---

#### Auto-detecting which tab to default to

The extension detects whether the current page is a **job listing/career page**
or a **specific job description page** from the URL alone, then pre-selects the
appropriate tab:

| URL example | Detection | Default tab |
|---|---|---|
| `boards.greenhouse.io/stripe` | no `/jobs/` segment after slug | Save Company |
| `boards.greenhouse.io/stripe/jobs/123` | has `/jobs/` + numeric ID | Save Job |
| `jobs.lever.co/stripe` | only 1 path segment | Save Company |
| `jobs.lever.co/stripe/uuid-here` | 2+ path segments | Save Job |
| `jobs.ashbyhq.com/stripe` | 1 path segment | Save Company |
| `jobs.ashbyhq.com/stripe/uuid` | 2+ path segments | Save Job |
| `*.myworkdayjobs.com/External` | no `/job/` in path | Save Company |
| `*.myworkdayjobs.com/External/job/123` | has `/job/` in path | Save Job |
| `careers.somecompany.com` | custom page, no JSON-LD `JobPosting` | Save Company |
| `careers.somecompany.com/engineer` | JSON-LD `@type: JobPosting` present | Save Job |

User can always override by clicking the other tab.

---

#### Career page URL pre-fill

- **"Save Company" tab:** Career Page URL is pre-filled with `window.location.href`
  — when the user is on a listing page, the current URL IS the career page URL.
- **"Save Job" tab:** JD URL is `window.location.href` — same as today.
- If user is on a job page but switches to "Save Company": career page URL
  field is editable; for known ATS platforms the listing URL can be derived by
  stripping the job ID segment. User can also paste the correct URL manually.

---

#### Apply form pages — suppress auto-trigger entirely

Apply form pages are not useful to capture. Auto-trigger is suppressed when the
URL matches apply form patterns:

```js
const APPLY_URL_PATTERNS = [
  /\/applications\/new/,
  /\/apply/,
  /\/application\//,
  /[?&]apply=/,
];
```

Manual trigger (icon click) still works on these pages if the user needs it.

---

#### Apply button opens in a new tab — cross-tab URL recovery

When the user clicks Apply and the apply form opens in a **new tab**, that tab
has an apply form URL (e.g. `/applications/new`), not the JD URL. We want to
save the JD URL, not the apply form URL.

**Solution: `background.js` records the opener tab's URL when any new tab is
created.**

Chrome provides `tab.openerTabId` on new tab creation. Background.js stores a
mapping of `newTabId → openerUrl` in `chrome.storage.session`:

```js
// background.js
chrome.tabs.onCreated.addListener((tab) => {
  if (!tab.openerTabId) return;
  chrome.tabs.get(tab.openerTabId, (opener) => {
    if (opener?.url) {
      chrome.storage.session.set({ [`opener_${tab.id}`]: opener.url });
    }
  });
});

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === 'GET_OPENER_URL') {
    const key = `opener_${sender.tab.id}`;
    chrome.storage.session.get(key, (r) => sendResponse({ url: r[key] || null }));
    return true; // keep channel open for async response
  }
});
```

When `content.js` loads on an apply page, it detects the apply URL pattern,
asks background.js for the opener's URL, and uses that as the job URL instead
of `window.location.href`:

```js
async function getJobUrl() {
  if (!isApplyPage()) return window.location.href;
  return new Promise((resolve) => {
    chrome.runtime.sendMessage({ type: 'GET_OPENER_URL' }, (res) => {
      resolve(res?.url || window.location.href);
    });
  });
}
```

**Full flow:**
1. User lands on JD page → auto-trigger fires after 4s → "Save Job" overlay
   with JD URL → user can submit here directly
2. OR user dismisses overlay / ignores it → clicks Apply → new tab opens →
   background.js records `newTabId → JD URL`
3. Apply form tab loads → content.js detects apply page → fetches opener URL
   from background.js → overlay shows "Save Job" with the **JD URL** pre-filled
4. User submits → correct URL saved

**Edge case:** If the opener was a listing page (not a JD page), the job URL
field will show the listing URL. User can edit it or switch to "Save Company"
tab. This is an acceptable rare path.

---

#### User's actual workflow this feature supports

1. Google a company → land on their careers listing page
2. Extension detects listing page → defaults to "Save Company" tab
3. User verifies Company Name + Career Page URL → submits → saved to `prospective_companies`
4. Click an open position → JD page → extension detects job page → defaults to "Save Job" tab
5. Click Apply → new tab (apply form) → extension recovers JD URL via opener tracking →
   "Save Job" overlay pre-filled with JD URL from step 4
6. User submits → saved to `applications`

---

#### New API endpoint needed

```http
POST /add-prospective
{ "company": "Stripe", "career_page_url": "https://stripe.com/jobs" }
→ 201 { "id": 5, "created": true }
→ 200 { "id": 5, "created": false }   (duplicate)
```

Maps to `db/prospective_companies.py` → `add_prospective_company()` (to be written).

---

### 18. Cloudflare Tunnel — public access without a domain

The extension connects to `api.py` on the VM via a cloudflared quick tunnel.
This gives a free `https://` URL with no domain, no firewall changes on OCI,
and no SSH tunnel setup required for other users.

**How it works:**

```text
VM boots
  → pipeline-api.service starts api.py on :5000
  → cloudflare-tunnel.service starts tunnel_manager.py
  → cloudflared assigns https://abc123.trycloudflare.com
  → tunnel_manager.py captures URL from cloudflared output
  → POSTs base URL to GitHub Gist as {"api_base": "https://abc123..."}

Extension starts (any laptop)
  → background.js fetches stable Gist raw URL
  → reads api_base, caches in chrome.storage.local
  → refreshes every 30 min via chrome.alarms

User saves a job
  → content.js._getApiBase() reads api_base from storage
  → calls https://abc123.trycloudflare.com/add-application
  → if Gist not set (GIST_CONFIG_URL = ''), falls back to API_BASE from config.js
```

**One-time VM setup:**

```bash
# 1. Install cloudflared on the VM
wget https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.rpm
sudo rpm -ivh cloudflared-linux-amd64.rpm

# 2. Install the systemd service
sudo cp deploy/systemd/cloudflare-tunnel.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable cloudflare-tunnel
sudo systemctl start cloudflare-tunnel
```

**One-time GitHub Gist setup:**

1. Go to gist.github.com → New gist
2. Filename: `api-config.json`, content: `{"api_base": ""}` → Create **secret** gist
3. Copy the Gist ID from the URL: `gist.github.com/user/{GIST_ID}`
4. Go to github.com → Settings → Developer settings → Personal access tokens
5. Generate a token with only `gist` scope → copy it
6. Add to `.env` on the VM:
   ```dotenv
   GITHUB_PAT=ghp_xxxxxxxxxxxx
   GIST_ID=xxxxxxxxxxxxxxxxxxxx
   ```

**One-time extension setup (per user):**

In `chrome-extension/config.js`, set `GIST_CONFIG_URL` to the raw Gist URL:
```javascript
const GIST_CONFIG_URL = 'https://gist.githubusercontent.com/YOUR_USERNAME/YOUR_GIST_ID/raw/api-config.json';
```
Reload the extension once. After that, URL changes are handled automatically.

**Security:** Set `EXTENSION_API_KEY` in `.env` on the VM. The extension sends
it as `X-API-Key` on every request, which blocks unauthenticated traffic. Note
that the key is bundled in `config.js` and visible to anyone who installs the
extension, so it is not a secret — add server-side rate limiting and
per-user authentication if the endpoint needs stronger protection.

**What happens when the tunnel URL changes (VM restart):**
1. `tunnel_manager.py` detects the new URL from cloudflared output
2. Updates the Gist automatically
3. Next time `background.js` polls (within 30 min), it picks up the new URL
4. Users see at most one failed save during the window; retry works

---

### 19. Development auto-reload

During active extension development, manually clicking the reload button in
`chrome://extensions` after every file save adds friction. The project includes
an auto-reload mechanism gated behind a `DEV_MODE` flag so it never ships.

**How it works:**

```text
You save content.js
  → scripts/watch_ext.py detects the change (watchdog)
  → POSTs /dev-bump to Flask → version counter increments (0 → 1)
  → background.js is polling /dev-ping every second
  → sees version changed → calls chrome.runtime.reload()
  → extension reloads automatically
```

**To enable during development:**

1. In `chrome-extension/config.js`, set `DEV_MODE = true`
2. Reload the extension once manually in `chrome://extensions`
3. In a terminal, run the file watcher:
   ```bash
   pip install watchdog requests   # one-time
   python scripts/watch_ext.py
   ```
4. Now every file save in `chrome-extension/` triggers an automatic reload.
   Refresh the job page tab to pick up the new content script.

**Before every commit:** set `DEV_MODE = false`. A pre-commit hook
(`.git/hooks/pre-commit`) blocks the commit if `DEV_MODE = true` is staged,
so it's enforced automatically.

**Why it's safe to ship with `DEV_MODE = false`:**
The polling block in `background.js` is inside `if (DEV_MODE) { ... }`.
When `false`, the code loads but never executes — zero network overhead.
The `/dev-ping` and `/dev-bump` routes in `api.py` are always present but
harmless; they're behind the SSH tunnel during testing and have no side effects.

**Implementation files:**
- `chrome-extension/config.js` — `DEV_MODE` flag (one source of truth)
- `chrome-extension/background.js` — `importScripts('config.js')` at top,
  polling loop inside `if (DEV_MODE)`
- `api.py` — `/dev-ping` (GET, returns `{v: int}`) and `/dev-bump` (POST, increments)
- `scripts/watch_ext.py` — watchdog observer; `EXTENSION_API_BASE` env var
  overrides the default `http://localhost:5001`
- `.git/hooks/pre-commit` — blocks commit if `DEV_MODE = true` is staged

---

## File Structure

```text
mail/
├── api.py                        ← Flask REST API (runs on VM)
├── scripts/
│   ├── tunnel_manager.py         ← cloudflared process manager + Gist URL updater
│   └── watch_ext.py              ← dev file watcher → triggers extension auto-reload
├── deploy/
│   └── systemd/
│       ├── pipeline-api.service  ← systemd unit for api.py (gunicorn)
│       └── cloudflare-tunnel.service ← systemd unit for tunnel_manager.py
└── chrome-extension/             ← load unpacked in Chrome developer mode
    ├── manifest.json
    ├── config.js                 ← PROD, DEV_MODE, API_BASE, GIST_CONFIG_URL, USERS
    ├── background.js             ← Gist URL fetch, icon click handler, DEV_MODE reload
    ├── content.js                ← extraction + identity + overlay + dynamic API URL
    ├── overlay.css               ← styles for the injected panel
    └── icons/
        ├── icon16.png
        ├── icon48.png
        └── icon128.png
```

---

## Flask API — `api.py`

Single endpoint. Thin wrapper around `add_application()`.

**Request:**
```json
POST /add-application
Content-Type: application/json
X-API-Key: <shared-key>   ← omit during SSH tunnel testing

{
  "company":   "Stripe",
  "job_url":   "https://boards.greenhouse.io/stripe/jobs/123",
  "job_title": "Senior Engineer",
  "status":    "active",
  "user_id":   1
}
```

`user_id` is always sent explicitly in the body — resolved client-side from the
Google display name match. Server stores it as-is, no mapping needed.

**Responses:**
```json
201 Created
{ "id": 42, "created": true }

200 OK  (duplicate)
{ "id": 42, "created": false }

400 Bad Request  (missing required field)
{ "error": "company is required" }

401 Unauthorized  (wrong API key)
{ "error": "unauthorized" }
```

Required fields: `company`, `job_url`. Everything else has defaults
(`job_title=null`, `status="active"`, `user_id=1`).

**`GET /health`** — returns `{"status": "ok", "time": "..."}`. Use to verify the tunnel is live.

**`POST /log-error`** — accepts `{"level": "error|warning|info", "message": "...", "context": {...}}` from the extension and logs it to `logs/api.log`. Picked up by `log_monitor.py` within 15 minutes. Returns `204 No Content`. Requires `X-API-Key` when `EXTENSION_API_KEY` is set in env.

**`GET /dev-ping`** — returns `{"v": int}`. Dev-only: background.js polls this when `DEV_MODE = true` to detect file changes. See §19.

**`POST /dev-bump`** — increments the version counter. Called by `scripts/watch_ext.py` on every file-save in `chrome-extension/`. See §19.

`api.py` lives at the project root and imports from `db.applications`. In production the systemd service uses Gunicorn (see §14).

---

## Chrome Extension

### `manifest.json`

```json
{
  "manifest_version": 3,
  "name": "Job Pipeline Capture",
  "version": "1.0",
  "permissions": ["activeTab", "scripting", "storage", "identity", "alarms"],
  "oauth2": {
    "client_id": "<your-client-id-from-google-cloud-console>",
    "scopes": [
      "https://www.googleapis.com/auth/userinfo.profile",
      "https://www.googleapis.com/auth/spreadsheets"
    ]
  },
  "background": {
    "service_worker": "background.js"
  },
  "content_scripts": [{
    "matches": [
      "*://boards.greenhouse.io/*",
      "*://jobs.lever.co/*",
      "*://jobs.ashbyhq.com/*",
      "*://*.myworkdayjobs.com/*",
      "*://careers.smartrecruiters.com/*",
      "*://www.linkedin.com/jobs/*",
      "*://www.indeed.com/viewjob*"
    ],
    "js": ["config.js", "content.js"],
    "css": ["overlay.css"]
  }],
  "action": {
    "default_title": "Capture this job"
  }
}
```

No `default_popup` — icon click is handled by `background.js` instead.
`config.js` is loaded before `content.js` so the user list and API endpoint
are available as globals.

### `background.js` responsibilities

- `importScripts('config.js')` — loads all config constants into the service worker scope
- **Gist URL discovery** (when `GIST_CONFIG_URL` set): fetches current cloudflare tunnel
  base URL from GitHub Gist on startup and every 30 min via `chrome.alarms`; caches
  result in `chrome.storage.local` as `api_base` for `content.js` to read
- **DEV_MODE auto-reload** (when `DEV_MODE = true`): polls `/dev-ping` every second;
  calls `chrome.runtime.reload()` when version changes (see §19)
- **Icon click** (`chrome.action.onClicked`): injects `config.js` + `content.js` +
  `overlay.css` into the active tab on demand for non-allowlisted pages

### `content.js` responsibilities

- On load (auto-trigger flow): start 4s focus timer; cancel on `visibilitychange`
- On message from background (manual-trigger flow): skip timer, extract immediately
- Extract title, company, URL using the priority chain above
- Inject the overlay `<div>` into `document.body`
- **`_getApiBase()`**: reads `api_base` from `chrome.storage.local` (set by
  background.js Gist fetch) when `GIST_CONFIG_URL` is set; falls back to `API_BASE`
  from `config.js` (SSH tunnel / production domain)
- On form submit: `fetch()` POST to `(await _getApiBase()) + '/add-application'`
- Handle all response states (success, duplicate, error) by updating overlay UI
- On Escape key or close button: hide overlay (keeps DOM; reopen via icon is instant)

### Overlay fields

| Field | Type | Pre-filled | Required |
|-------|------|-----------|---------|
| Applying as | dropdown | auto-detected via Google display name | yes |
| Job Title | text input | extracted from page | no |
| Company | text input | extracted from URL/JSON-LD | yes |
| URL | text input (readonly) | `window.location.href` | yes |
| Status | dropdown | Active (Applied) | yes |

URL is read-only — it's the dedup key in the DB. If the wrong URL was captured,
dismiss and navigate to the correct page before triggering again.

---

## Connecting the Extension to the VM

### Option A — Cloudflare Tunnel (recommended, no domain needed)

The extension discovers the current tunnel URL automatically via GitHub Gist.
See **§18** for full setup. Once configured:
- No per-session setup for any user
- URL changes on VM restart are handled automatically within 30 min
- Other users install the extension and set `GIST_CONFIG_URL` in `config.js` once (§18 extension setup)

### Option B — SSH Tunnel (dev / single-user fallback)

```bash
# Run once before using the extension. Keep terminal open (or use tmux).
ssh -L 5001:localhost:5000 opc@<VM-IP> -N

# Verify tunnel is working:
curl http://localhost:5001/health
# → {"status": "ok"}
```

Set `GIST_CONFIG_URL = ''` in `config.js` to disable Gist discovery.
Extension will call `http://localhost:5001` (the `API_BASE` default).

---

## Implementation Order

1. Google Cloud Console — register OAuth client, get client ID (~15 min, one-time)
2. `api.py` — Flask endpoint + `/health` check + systemd service file
3. `config.js` — user list, `PROD` flag, API endpoint, shared API key, Sheet ID
4. `manifest.json` + `background.js` skeleton (load unpacked, verify icon click fires)
5. `content.js` — extraction logic only (log to console, no overlay yet)
6. SPA navigation — `pushState` intercept + `popstate` listener
7. Identity detection — `getAuthToken` → userinfo → name match → log result
8. Overlay UI — inject panel with all fields pre-filled, overlay guard, no submit yet
9. Fetch + response handling — 10s timeout → Flask API → Google Sheets fallback
10. Auto-trigger — add 4s focus delay on top of working manual flow
11. Test: Greenhouse, Lever, Workday, LinkedIn (SPA), Ashby, Indeed

---

## Effort Estimate

| Component | Estimate |
|-----------|----------|
| Google Cloud Console OAuth setup | 15 min |
| `api.py` + systemd service | 1 h |
| `config.js` + `manifest.json` + `background.js` | 30 min |
| `content.js` extraction logic | 1 h |
| SPA navigation handling | 30 min |
| Identity detection (OAuth → name match) | 30 min |
| Overlay UI + guard | 1 h |
| Fetch + timeout + Sheets fallback + all response states | 1 h |
| Auto-trigger focus delay | 30 min |
| Testing across 5–6 ATS platforms | 1–2 h |
| **Total** | **~7.5 h** |

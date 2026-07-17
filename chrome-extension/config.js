// chrome-extension/config.js
// ─────────────────────────────────────────────────────────────────────────────
// Edit before loading the extension. Flip PROD to true when moving to
// production and fill in User 2's first name (lowercase) in USERS.
// ─────────────────────────────────────────────────────────────────────────────

const PROD = false;

// Flip to true locally to enable auto-reload on file save. Never commit true.
const DEV_MODE = false;

const API_BASE = PROD
  ? 'https://yourdomain.com'
  : 'http://localhost:5001';

const API_ENDPOINT = API_BASE + '/add-application';

// Stable raw URL of the GitHub Gist that holds the current cloudflare tunnel
// base URL.  background.js fetches this on startup and every 30 min, caches
// the result in chrome.storage.local, and content.js reads it for every API
// call.  Set to '' to skip Gist discovery and use API_ENDPOINT directly
// (SSH tunnel mode or when you have a real domain).
const GIST_CONFIG_URL = 'https://gist.githubusercontent.com/rutvijmavani/4f400d820fd0b390c15dd7d6592d8053/raw/api-config.json';

// Shared API key — leave empty for SSH tunnel testing; set in production.
const API_KEY = '7df822f9d051c53fc32fd36b441e29d88afa0bba34493cee6de81633a79733c2';

// Copy the value of GOOGLE_SHEET_ID from your .env file.
const SHEET_ID = 'YOUR_GOOGLE_SHEET_ID';
const SHEET_TAB = 'Responses';

// nameMatch is a lowercase substring of the Google account display name.
// e.g. if your name is "Rutvi Mavani", set nameMatch to "rutvi".
const USERS = [
  { id: 1, nameMatch: 'rutvij' },
  { id: 2, nameMatch: 'disha' },   // ← fill in User 2's first name
];

const FETCH_TIMEOUT_MS = 10000;

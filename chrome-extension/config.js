// chrome-extension/config.js
// ─────────────────────────────────────────────────────────────────────────────
// Edit before loading the extension. Flip PROD to true when moving to
// production and fill in User 2's first name (lowercase) in USERS.
// ─────────────────────────────────────────────────────────────────────────────

const PROD = false;

const API_ENDPOINT = PROD
  ? 'https://yourdomain.com/add-application'
  : 'http://localhost:5001/add-application';

// Shared API key — leave empty for SSH tunnel testing; set in production.
const API_KEY = '';

// Copy the value of GOOGLE_SHEET_ID from your .env file.
const SHEET_ID = '15B3jTyMkb1Af1GLZOn7-ZWiX1hyKBAX43AidZmYyQdg';
const SHEET_TAB = 'Responses';

// nameMatch is a lowercase substring of the Google account display name.
// e.g. if your name is "Rutvi Mavani", set nameMatch to "rutvi".
const USERS = [
  { id: 1, nameMatch: 'rutvij' },
  { id: 2, nameMatch: 'disha' },   // ← fill in User 2's first name
];

const FETCH_TIMEOUT_MS = 10000;

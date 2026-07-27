const GIST_URL = 'https://gist.githubusercontent.com/rutvijmavani/4f400d820fd0b390c15dd7d6592d8053/raw/api-config.json';
const FETCH_TIMEOUT_MS = 5000;

function isValidUrl(url) {
  try {
    return new URL(url).protocol === 'https:';
  } catch (_) { return false; }
}

function openUrl(url) {
  if (!isValidUrl(url)) { showError('Received invalid URL from Gist: ' + url); return; }
  document.getElementById('spinner').style.display = 'none';
  document.getElementById('msg').style.display = 'none';
  const box = document.getElementById('url-box');
  box.style.display = 'block';
  const a = document.createElement('a');
  a.href = url;
  a.textContent = url;
  a.target = '_blank';
  document.getElementById('url-text').appendChild(a);
  document.getElementById('open-btn').onclick = () => window.open(url, '_blank');
  chrome.storage.local.set({ frontend_base: url });
  setTimeout(() => { window.location.href = url; }, 300);
}

function showError(msg) {
  document.getElementById('spinner').style.display = 'none';
  document.getElementById('msg').textContent = msg;
}

function tryStorage(fallbackMsg) {
  chrome.storage.local.get('frontend_base', ({ frontend_base: cached }) => {
    if (cached && isValidUrl(cached)) {
      openUrl(cached);
    } else {
      showError(fallbackMsg);
    }
  });
}

// Single-fire guard — whichever path resolves first wins; all others are no-ops
let settled = false;
function settle(fn) {
  if (settled) return;
  settled = true;
  clearTimeout(safetyTimer);
  clearTimeout(fetchTimer);
  fn();
}

const safetyTimer = setTimeout(() => {
  settle(() => tryStorage('Gist timed out. No cached URL found. Is the cloudflare-tunnel service running?'));
}, 8000);

const controller = new AbortController();
const fetchTimer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

fetch(GIST_URL + '?t=' + Date.now(), { signal: controller.signal })
  .then(r => r.json())
  .then(data => {
    const url = data.frontend_base;
    if (url) {
      settle(() => openUrl(url));
    } else {
      settle(() => tryStorage('frontend_base not in Gist yet. Is the tunnel running on the server?'));
    }
  })
  .catch(() => {
    settle(() => tryStorage('Could not reach Gist. Using cached URL if available.'));
  });

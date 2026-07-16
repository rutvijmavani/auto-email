// chrome-extension/background.js
// Handles icon click → show overlay in the active tab.

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

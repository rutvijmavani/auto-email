# jobs/ats/base.py — Shared ATS client logic

import re
import time
import json
import random
import logging
import requests
from config import JOB_MONITOR_API_TIMEOUT, PLATFORM_DELAYS

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# DELAY HELPERS
# ─────────────────────────────────────────

def platform_delay(platform):
    """
    Apply per-platform delay with random jitter.
    Mimics human browsing patterns.
    Start minimal — increase only if 429s appear.
    """
    cfg   = PLATFORM_DELAYS.get(platform, {"base": 0.3, "jitter": 0.1})
    base  = cfg["base"]
    jitter = cfg["jitter"]
    delay = base + random.uniform(-jitter, jitter)
    delay = max(0.05, delay)  # never less than 50ms
    time.sleep(delay)


def between_companies_delay():
    """
    Delay between companies during --monitor-jobs.
    0.5s ±0.2s — reduces per-IP request concentration.
    """
    from config import MONITOR_BETWEEN_COMPANIES
    cfg   = MONITOR_BETWEEN_COMPANIES
    delay = cfg["base"] + random.uniform(
        -cfg["jitter"], cfg["jitter"]
    )
    time.sleep(max(0.1, delay))


# ─────────────────────────────────────────
# TRACKED REQUEST
# ─────────────────────────────────────────

def fetch_json(url, params=None, retries=2,
               platform=None, track=True):
    """
    Fetch JSON from URL with timeout + retry.
    Records request stats to api_health if platform given.
    Applies per-platform delay if platform given.
    Returns parsed JSON dict/list or None on failure.
    """
    for attempt in range(retries + 1):
        start_ms    = int(time.time() * 1000)
        status_code = 0
        backoff_s   = 0

        try:
            resp = requests.get(
                url,
                params=params,
                timeout=JOB_MONITOR_API_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            status_code  = resp.status_code
            elapsed_ms   = int(time.time() * 1000) - start_ms

            # Record to api_health
            if platform and track:
                _record(platform, status_code, elapsed_ms)

            if status_code == 429:
                backoff_s = 60
                if attempt < retries:
                    logger.warning(
                        "Rate limited on %s — waiting %ds",
                        platform or url, backoff_s
                    )
                    if platform and track:
                        _record(platform, 429, elapsed_ms,
                                backoff_s=backoff_s)
                        _check_rate_limit_alert(platform)
                    time.sleep(backoff_s)
                    continue
                return None

            if status_code == 404:
                return None

            if not resp.ok:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                    continue
                return None

            # Apply platform delay after successful request
            if platform:
                platform_delay(platform)

            return resp.json()

        except requests.exceptions.Timeout:
            elapsed_ms = int(time.time() * 1000) - start_ms
            if platform and track:
                _record(platform, 0, elapsed_ms)
            if attempt < retries:
                time.sleep(5)
                continue
            return None

        except requests.exceptions.ConnectionError:
            elapsed_ms = int(time.time() * 1000) - start_ms
            if platform and track:
                _record(platform, 0, elapsed_ms)
            if attempt < retries:
                time.sleep(5)
                continue
            return None

        except (ValueError, json.JSONDecodeError,
                KeyError, AttributeError):
            return None

    return None


def fetch_html(url, params=None, platform=None, track=True):
    """
    Fetch HTML from URL with tracking.
    Used by iCIMS and career_page.py.
    """
    start_ms    = int(time.time() * 1000)
    status_code = 0

    try:
        resp = requests.get(
            url,
            params=params,
            timeout=JOB_MONITOR_API_TIMEOUT,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        status_code = resp.status_code
        elapsed_ms  = int(time.time() * 1000) - start_ms

        if platform and track:
            _record(platform, status_code, elapsed_ms)

        if status_code == 429:
            if platform:
                _check_rate_limit_alert(platform)
            return None

        if platform:
            platform_delay(platform)

        return resp if resp.ok else None

    except Exception as e:
        elapsed_ms = int(time.time() * 1000) - start_ms
        if platform and track:
            _record(platform, 0, elapsed_ms)
        logger.debug("fetch_html failed for %s: %s", url, e)
        return None


# ─────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────

def _record(platform, status_code, elapsed_ms,
            backoff_s=0):
    """Record request to api_health table. Best-effort."""
    try:
        from db.api_health import record_request
        record_request(platform, status_code,
                       elapsed_ms, backoff_s)
    except Exception:
        pass  # never block on tracking failure


def _check_rate_limit_alert(platform):
    """
    Check if 429 rate exceeds threshold → create alert.
    Critical alert sent immediately if > CRITICAL threshold.
    Warning alert queued for daily digest if > WARNING threshold.
    """
    try:
        from db.api_health import get_run_429_rate
        from db.pipeline_alerts import (
            create_alert, ALERT_RATE_LIMIT,
            CRITICAL, WARNING
        )
        from config import (
            RATE_LIMIT_CRITICAL_THRESHOLD,
            RATE_LIMIT_WARNING_THRESHOLD,
        )

        rate = get_run_429_rate(platform)

        if rate >= RATE_LIMIT_CRITICAL_THRESHOLD:
            alert_id = create_alert(
                alert_type=ALERT_RATE_LIMIT,
                severity=CRITICAL,
                platform=platform,
                value=rate,
                threshold=RATE_LIMIT_CRITICAL_THRESHOLD,
                message=(
                    f"{platform} rate limited: "
                    f"{rate}% of requests returned 429"
                ),
            )
            if alert_id:
                _send_critical_alert(
                    alert_id, platform, rate,
                    RATE_LIMIT_CRITICAL_THRESHOLD
                )

        elif rate >= RATE_LIMIT_WARNING_THRESHOLD:
            create_alert(
                alert_type=ALERT_RATE_LIMIT,
                severity=WARNING,
                platform=platform,
                value=rate,
                threshold=RATE_LIMIT_WARNING_THRESHOLD,
                message=(
                    f"{platform} rate limited: "
                    f"{rate}% of requests returned 429"
                ),
            )

    except Exception as e:
        logger.debug("Alert check failed: %s", e)


def _send_critical_alert(alert_id, platform, rate, threshold):
    """Send immediate critical email alert."""
    try:
        from outreach.report_templates.api_health_report import (
            build_critical_rate_limit_alert
        )
        from db.pipeline_alerts import mark_notified
        build_critical_rate_limit_alert(platform, rate, threshold)
        mark_notified(alert_id)
    except Exception as e:
        logger.error("Failed to send critical alert: %s", e)


# ─────────────────────────────────────────
# EXISTING HELPERS (unchanged)
# ─────────────────────────────────────────

def slugify(company):
    """
    Generate slug variants to try for ATS detection.
    Returns list of slugs in order of likelihood.
    """
    name = company.strip().lower()
    name = re.sub(
        r'\b(inc|corp|llc|ltd|co|company|technologies|'
        r'tech|systems|solutions|services|group|holding|'
        r'holdings)\.?\b', '', name
    )
    name = name.strip().strip('.,')

    v1 = re.sub(r'[^a-z0-9]', '', name)
    v2 = re.sub(r'[^a-z0-9]+', '-', name).strip('-')
    v3 = re.split(r'[^a-z0-9]', name)[0]
    parts = [p for p in re.split(r'[^a-z0-9]+', name) if p]
    v4 = ''.join(parts[:2]) if len(parts) >= 2 else v1

    seen     = set()
    variants = []
    for v in [v1, v2, v3, v4]:
        if v and v not in seen:
            seen.add(v)
            variants.append(v)
    return variants


def validate_company_match(response_text, expected_company):
    """
    Check that API response is for the right company.
    Returns True if likely correct company.
    """
    if not response_text or not expected_company:
        return True
    expected = expected_company.lower().strip()
    response = response_text.lower()
    stop_words = {
        "inc", "corp", "llc", "ltd", "co", "the",
        "and", "jobs", "careers", "group"
    }
    words = [
        w for w in expected.split()
        if len(w) > 3 and w not in stop_words
    ]
    if not words:
        return True
    for word in words[:2]:
        pattern = (r'(?<![a-z0-9])' + re.escape(word)
                   + r'(?![a-z0-9])')
        if re.search(pattern, response):
            return True
    return False
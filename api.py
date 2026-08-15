import base64
import hmac
import ipaddress
import json
import os
import secrets
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin

import requests as _requests
import tldextract as _tldextract
from flask import Flask, request, jsonify, make_response, redirect
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

load_dotenv()

from logger import get_logger, init_logging, cleanup_logs_if_due
from config import (
    DOMAIN_ENRICHMENT_QUEUE,
    ENRICHMENT_HIGH_PRIORITY_SCORE,
    REDIS_EMAIL_PUSH,
    VERIFY_TASK_QUEUE_CAP,
)
from db.applications import add_application
from db.connection import get_conn
from db.gmail_tokens import upsert_token, update_watch
from workers.redis_client import get_redis

_GMAIL_SCOPES       = ["https://www.googleapis.com/auth/gmail.readonly"]
_CLIENT_ID          = os.environ.get("GMAIL_CLIENT_ID", "")
_CLIENT_SECRET      = os.environ.get("GMAIL_CLIENT_SECRET", "")
_REDIRECT_URI       = os.environ.get("GMAIL_OAUTH_REDIRECT_URI", "")  # fallback when Gist unavailable
_PUBSUB_TOPIC       = os.environ.get("GMAIL_PUBSUB_TOPIC", "")
_GIST_CONFIG_URL    = os.environ.get("GIST_CONFIG_URL", "")  # same Gist used by Chrome extension

init_logging('api')
logger = get_logger(__name__)

app = Flask(__name__)

_API_KEY = os.environ.get('EXTENSION_API_KEY', '')


def _cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-API-Key'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    return response


def _cors_preflight():
    return _cors(make_response('', 204))


@app.before_request
def before_request():
    cleanup_logs_if_due()


@app.after_request
def after_request(response):
    return _cors(response)


@app.route('/health', methods=['GET'])
def health():
    logger.debug("health check")
    return jsonify({'status': 'ok', 'time': datetime.utcnow().isoformat()})


@app.route('/log-error', methods=['POST'])
def log_error_endpoint():
    if _API_KEY and not hmac.compare_digest(request.headers.get('X-API-Key', ''), _API_KEY):
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return '', 204

    level   = str(data.get('level', 'error')).lower()
    message = str(data.get('message', ''))[:500]
    context = data.get('context', {})
    if not isinstance(context, dict):
        context = {}

    log_fn = {
        'error':   logger.error,
        'warning': logger.warning,
        'info':    logger.info,
    }.get(level, logger.error)

    log_fn("[extension] %s | %s", message, context)
    return '', 204


@app.route('/add-application', methods=['OPTIONS'])
def add_application_options():
    return _cors(make_response('', 204))


@app.route('/add-application', methods=['POST'])
def add_application_endpoint():
    if _API_KEY and not hmac.compare_digest(request.headers.get('X-API-Key', ''), _API_KEY):
        logger.warning("unauthorized request from origin=%r", request.headers.get('Origin', ''))
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        logger.warning("invalid payload — expected JSON object, got %s", type(data).__name__)
        data = {}

    company   = (data.get('company')   or '').strip()
    job_url   = (data.get('job_url')   or '').strip()
    job_title = (data.get('job_title') or '').strip() or None
    status    = data.get('status', 'active')
    try:
        user_id = int(data.get('user_id', 1))
        if user_id <= 0:
            raise ValueError
    except (ValueError, TypeError):
        logger.warning("rejected request — invalid user_id %r", data.get('user_id'))
        return jsonify({'error': 'user_id must be a positive integer'}), 400

    if not company:
        logger.warning("rejected request — missing company")
        return jsonify({'error': 'company is required'}), 400
    if not job_url:
        logger.warning("rejected request — missing job_url (company=%r)", company)
        return jsonify({'error': 'job_url is required'}), 400
    if status not in ('active', 'prospective'):
        status = 'active'

    try:
        app_id, created = add_application(
            company=company,
            job_url=job_url,
            job_title=job_title,
            status_override=status,
            user_id=user_id,
        )
    except Exception as e:
        logger.error("add_application failed for company=%r user_id=%s: %s", company, user_id, e, exc_info=True)
        return jsonify({'error': 'failed to insert'}), 500

    if app_id is None:
        logger.error("add_application returned None for company=%r user_id=%s", company, user_id)
        return jsonify({'error': 'failed to insert'}), 500

    if created:
        logger.info("added application id=%s company=%r user_id=%s status=%s", app_id, company, user_id, status)
    else:
        logger.info("duplicate application id=%s company=%r user_id=%s", app_id, company, user_id)

    return jsonify({'id': app_id, 'created': created}), 201 if created else 200


# ── Gmail Push Notifications ──────────────────────────────────────────────────

@app.route('/email-push', methods=['POST'])
def email_push():
    """
    Pub/Sub push endpoint. Called by Google Cloud Pub/Sub when a new email
    arrives in a monitored Gmail inbox.

    Pub/Sub delivers a JSON envelope:
        {"message": {"data": "<base64>", "messageId": "..."}, "subscription": "..."}

    The base64-decoded data is:
        {"emailAddress": "user@gmail.com", "historyId": "12345"}

    Authentication: the Pub/Sub push subscription URL must include
    ?token=<EXTENSION_API_KEY> so only Google's authenticated calls are
    accepted. Unauthenticated requests are rejected before any decoding.

    We write this to the Redis queue and return 200 immediately.
    Pub/Sub retries if we return non-200, so Redis write failures return 500.
    """
    if not _API_KEY or not hmac.compare_digest(request.args.get("token", ""), _API_KEY):
        logger.warning("email-push: unauthorized request from %s", request.remote_addr)
        return '', 401

    envelope = request.get_json(silent=True)
    if not envelope or "message" not in envelope:
        logger.warning("email-push: malformed envelope — ignoring")
        return '', 204

    try:
        data = json.loads(base64.b64decode(envelope["message"]["data"]).decode())
        email_address = data["emailAddress"]
        history_id    = str(data["historyId"])
    except Exception as e:
        logger.warning("email-push: failed to decode message: %s", e)
        return '', 204

    payload = json.dumps({"email": email_address, "history_id": history_id})
    try:
        get_redis().lpush(REDIS_EMAIL_PUSH, payload)
    except Exception as e:
        logger.error("email-push: Redis write failed for %s: %s", email_address, e)
        return '', 500  # tell Pub/Sub to retry

    logger.info("email-push queued email=%s history_id=%s", email_address, history_id)
    return '', 200


# ── Gmail OAuth ───────────────────────────────────────────────────────────────

def _get_redirect_uri() -> str:
    """
    Return the OAuth redirect URI for the current tunnel URL.

    Fetches api_base from the GitHub Gist (kept current by tunnel_manager.py)
    and appends /oauth/callback. Falls back to GMAIL_OAUTH_REDIRECT_URI env var
    when the Gist is unreachable or GIST_CONFIG_URL is not set.
    """
    if _GIST_CONFIG_URL:
        try:
            resp = _requests.get(_GIST_CONFIG_URL, timeout=5)
            base = resp.json().get("api_base", "").rstrip("/")
            if base:
                return f"{base}/oauth/callback"
        except Exception as exc:
            logger.warning("oauth: Gist fetch failed, falling back to env var: %s", exc)
    return _REDIRECT_URI


def _make_flow(redirect_uri: str) -> Flow:
    return Flow.from_client_config(
        client_config={
            "web": {
                "client_id":     _CLIENT_ID,
                "client_secret": _CLIENT_SECRET,
                "auth_uri":      "https://accounts.google.com/o/oauth2/auth",
                "token_uri":     "https://oauth2.googleapis.com/token",
            }
        },
        scopes=_GMAIL_SCOPES,
        redirect_uri=redirect_uri,
    )


_OAUTH_NONCE_TTL = 600   # seconds — nonce expires after 10 minutes


@app.route('/oauth/start')
def oauth_start():
    """
    Begin the OAuth flow for a user. Visit this URL once per user to grant
    Gmail read access.

    Query param: user_id (int) — must already exist in the users table.

    Redirects to Google's consent screen. On approval, Google calls /oauth/callback.
    The OAuth state param carries a one-time server-generated nonce (not user_id
    directly) so user_id is never exposed in the redirect URI.
    """
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    redirect_uri = _get_redirect_uri()
    nonce = secrets.token_urlsafe(32)
    try:
        get_redis().set(
            f"oauth:nonce:{nonce}",
            json.dumps({"user_id": user_id, "redirect_uri": redirect_uri}),
            ex=_OAUTH_NONCE_TTL,
        )
    except Exception as e:
        logger.error("oauth/start: failed to store nonce for user_id=%s: %s", user_id, e)
        return jsonify({"error": "internal error"}), 500

    auth_url, _ = _make_flow(redirect_uri).authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",   # force refresh token on every auth
        state=nonce,
    )
    logger.info("oauth/start redirecting user_id=%s to Google consent", user_id)
    return redirect(auth_url)


@app.route('/oauth/callback')
def oauth_callback():
    """
    OAuth callback — Google redirects here after the user grants access.
    Exchanges the auth code for tokens, stores the encrypted refresh token,
    and starts the Gmail watch for this user.

    The state param is a one-time nonce mapped to user_id in Redis. The nonce
    is consumed exactly once — missing, expired, or replayed nonces are rejected.
    """
    nonce = request.args.get("state", "")
    code  = request.args.get("code")
    error = request.args.get("error")

    if error:
        logger.warning("oauth/callback: nonce=%s denied access: %s", nonce, error)
        return jsonify({"error": "access denied", "detail": error}), 400

    if not code or not nonce:
        return jsonify({"error": "missing code or state"}), 400

    # Resolve and consume the nonce — reject if missing, expired, or replayed
    try:
        raw = get_redis().getdel(f"oauth:nonce:{nonce}")
    except Exception as e:
        logger.error("oauth/callback: Redis error resolving nonce: %s", e)
        return jsonify({"error": "internal error"}), 500

    if raw is None:
        logger.warning("oauth/callback: unknown or expired nonce=%s", nonce)
        return jsonify({"error": "invalid or expired state"}), 400

    try:
        data = json.loads(raw)
        user_id      = int(data["user_id"])
        redirect_uri = data.get("redirect_uri") or _REDIRECT_URI
    except (ValueError, TypeError, KeyError):
        logger.error("oauth/callback: corrupt nonce value for nonce=%s", nonce)
        return jsonify({"error": "internal error"}), 500

    try:
        flow = _make_flow(redirect_uri)
        flow.fetch_token(code=code)
        creds = flow.credentials
    except Exception as e:
        logger.error("oauth/callback: token exchange failed for user_id=%s: %s", user_id, e)
        return jsonify({"error": "token exchange failed"}), 500

    # Persist encrypted refresh token
    try:
        gmail = build("gmail", "v1", credentials=creds)
        profile = gmail.users().getProfile(userId="me").execute()
        gmail_email = profile["emailAddress"]
        upsert_token(user_id, gmail_email, creds.refresh_token)
    except Exception as e:
        logger.error("oauth/callback: failed to store token for user_id=%s: %s", user_id, e)
        return jsonify({"error": "failed to store token"}), 500

    # Start Gmail push notifications watch
    try:
        watch = gmail.users().watch(
            userId="me",
            body={"topicName": _PUBSUB_TOPIC, "labelIds": ["INBOX"]},
        ).execute()
        expires_ms  = int(watch["expiration"])
        expires_at  = datetime.fromtimestamp(expires_ms / 1000, tz=timezone.utc).isoformat()
        update_watch(user_id, str(watch["historyId"]), expires_at)
        logger.info(
            "oauth/callback: watch started for user_id=%s email=%s expires=%s",
            user_id, gmail_email, expires_at,
        )
    except Exception as e:
        logger.error("oauth/callback: watch setup failed for user_id=%s: %s", user_id, e)
        return jsonify({"error": "token stored but watch failed — retry /oauth/start"}), 500

    return jsonify({"status": "authorized", "email": gmail_email})


# Bounded executor for background verify/enrich tasks — prevents thread explosion
# under rapid extension requests for the same company.
_VERIFY_EXECUTOR   = ThreadPoolExecutor(max_workers=8)
_INFLIGHT_FEINS    = set()          # FEINs with an active background task
_INFLIGHT_LOCK     = threading.Lock()

_VERIFY_HEAD_TIMEOUT  = 8   # seconds per hop — fast, never block the request
_VERIFY_TOTAL_TIMEOUT = 30  # seconds total across all hops — prevents 10×8s worst case
_VERIFY_GOOD_CODES    = {200, 201, 204, 206, 403}  # 2xx + 403 (bot-blocked pages exist but are valid)
_VERIFY_MAX_REDIRECTS = 10
_VERIFY_STALE_DAYS    = 30  # re-verify after this many days
_PRIVATE_NETS = [
    ipaddress.ip_network(r) for r in (
        "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16",
        "127.0.0.0/8", "169.254.0.0/16", "0.0.0.0/8",
        "100.64.0.0/10",   # CGNAT / shared address space (RFC 6598)
        "::1/128", "fc00::/7", "fe80::/10",   # loopback, ULA, link-local
    )
]


def _is_private_host(host: str) -> bool:
    """Return True when ANY address getaddrinfo returns is private/loopback (fail closed)."""
    try:
        results = socket.getaddrinfo(host, None)
        if not results:
            return True
        for _family, _type, _proto, _canon, sockaddr in results:
            addr = ipaddress.ip_address(sockaddr[0])
            if any(addr in net for net in _PRIVATE_NETS):
                return True
            mapped = getattr(addr, "ipv4_mapped", None)
            if mapped and any(mapped in net for net in _PRIVATE_NETS):
                return True
        return False
    except Exception:
        return True  # treat unresolvable as private (fail closed)


def _host_root(host: str) -> str:
    """Return the PSL-aware registrable domain (e.g. 'acme.co.uk' not 'co.uk')."""
    ext = _tldextract.extract(host)
    return ext.registered_domain or host


def _head_ok(url: str, allowed_root: "str | None" = None) -> bool:
    """
    Return True if url returns a response in _VERIFY_GOOD_CODES (2xx or 403).
    Pre-validates scheme and rejects private/loopback hosts before every hop.
    Follows redirects manually (allow_redirects=False) to validate each hop's
    scheme, resolved addresses, and allowed registrable domain before connecting.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    hostname = parsed.hostname or ""
    if not hostname or _is_private_host(hostname):
        return False
    initial_root = _host_root(hostname)
    allowed = {initial_root}
    if allowed_root:
        allowed.add(_host_root(allowed_root))  # normalize through PSL before comparing
    current_url = url
    _deadline = time.monotonic() + _VERIFY_TOTAL_TIMEOUT
    try:
        for _ in range(_VERIFY_MAX_REDIRECTS):
            if time.monotonic() > _deadline:
                return False
            _hop_timeout = min(_VERIFY_HEAD_TIMEOUT, max(1.0, _deadline - time.monotonic()))
            resp = _requests.head(
                current_url,
                allow_redirects=False,
                timeout=_hop_timeout,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if resp.status_code in _VERIFY_GOOD_CODES:
                return True
            if resp.status_code == 405:
                # Server rejected HEAD — fall back to GET (stream=True to avoid body download)
                try:
                    gr = _requests.get(
                        current_url,
                        allow_redirects=False,
                        stream=True,
                        timeout=_hop_timeout,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    gr.close()
                    if gr.status_code in _VERIFY_GOOD_CODES:
                        return True
                    resp = gr  # treat GET response as HEAD — fall through to redirect handling
                except Exception:
                    return False
            if resp.status_code not in (301, 302, 303, 307, 308):
                return False
            location = resp.headers.get("Location", "")
            if not location:
                return False
            next_url = urljoin(current_url, location)
            next_parsed = urlparse(next_url)
            if next_parsed.scheme not in ("http", "https"):
                return False
            next_host = next_parsed.hostname or ""
            if not next_host or _is_private_host(next_host):
                return False
            next_root = _host_root(next_host)
            if next_root not in allowed:
                logger.warning(
                    "verify-company: redirect to unexpected domain %s (allowed %s)",
                    next_root, allowed,
                )
                return False
            current_url = next_url
        return False  # too many redirects
    except Exception:
        return False


def _trigger_enrichment(fein: str, r=None) -> None:
    """Push fein to enrichment queue at HIGH priority. Fire-and-forget.

    Workers are started on demand by staleness_checker; no systemctl here so
    the web process doesn't require sudo and doesn't repeat the call per request.
    Accepts a pre-created Redis client (r) so the caller can initialise it in
    the request thread rather than inside the thread-pool worker.
    """
    try:
        _r = r if r is not None else get_redis()
        member = json.dumps({"fein": fein, "trigger": "on_demand"})
        _r.zadd(DOMAIN_ENRICHMENT_QUEUE, {member: ENRICHMENT_HIGH_PRIORITY_SCORE}, gt=True)
        logger.info("verify-company: queued high-priority re-enrichment fein=%s", fein)
    except Exception as exc:
        logger.error("verify-company: failed to queue re-enrichment fein=%s: %s", fein, exc)


@app.route('/verify-company', methods=['POST', 'OPTIONS'])
def verify_company():
    """
    On-demand career URL verification. Called by the UI/extension when a user
    visits a company page. Always returns immediately with cached data.
    If the careers_url fails a HEAD check, re-enrichment is triggered silently.

    POST body: {"fein": "123456789", "user_id": 1}   (user_id optional)
    Response:  {"careers_url": "...", "ats_platform": "...", "stale": bool}
    """
    if request.method == 'OPTIONS':
        return _cors_preflight()

    if not _API_KEY:
        return jsonify({'error': 'API key not configured'}), 503
    if not hmac.compare_digest(request.headers.get('X-API-Key', ''), _API_KEY):
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({'error': 'request body must be a JSON object'}), 400
    fein = data.get('fein')
    if not isinstance(fein, str) or not fein.strip():
        return jsonify({'error': 'fein is required'}), 400
    fein = fein.strip()
    if not fein.isdigit() or len(fein) != 9:
        return jsonify({'error': 'fein must be a 9-digit number'}), 400

    conn = get_conn()
    try:
        row = conn.execute("""
            SELECT
                f.employer_fein,
                f.public_domain,
                f.careers_url,
                f.careers_url_verified_at,
                ca.platform AS ats_platform,
                ca.slug     AS ats_slug
            FROM fein_domain_map f
            LEFT JOIN company_ats ca ON ca.employer_fein = f.employer_fein
            WHERE f.employer_fein = %s
            ORDER BY ca.priority DESC NULLS LAST,
                     ca.detected_at DESC NULLS LAST,
                     ca.slug NULLS LAST
            LIMIT 1
        """, (fein,)).fetchone()
    except Exception as exc:
        logger.error("verify-company: DB error for fein=%s: %s", fein, exc)
        return jsonify({'error': 'internal error'}), 500
    finally:
        conn.close()

    if row is None:
        return jsonify({'error': 'company not found'}), 404

    careers_url = row['careers_url']
    # Determine staleness synchronously from DB timestamp; the background HEAD
    # check will trigger re-enrichment if needed but can't update this response.
    _verified_at = row.get('careers_url_verified_at')
    if not careers_url:
        _is_stale = True
    elif _verified_at is None:
        _is_stale = True  # never verified
    else:
        if _verified_at.tzinfo is None:  # guard: psycopg2 returns aware for TIMESTAMPTZ, but be safe
            _verified_at = _verified_at.replace(tzinfo=timezone.utc)
        _age_days = (datetime.now(timezone.utc) - _verified_at).days
        _is_stale = _age_days > _VERIFY_STALE_DAYS
    payload = {
        'careers_url':  careers_url,
        'ats_platform': row['ats_platform'],
        'ats_slug':     row['ats_slug'],
        'stale':        _is_stale,
    }

    # Pre-create Redis client in the request thread so background tasks don't
    # call get_redis() inside the thread-pool worker (avoids thread-safety issues).
    try:
        _r_client = get_redis()
    except Exception:
        _r_client = None

    def _submit(fn, *args):
        """Submit to bounded executor; skip if this FEIN is already in-flight or global cap reached."""
        with _INFLIGHT_LOCK:
            if fein in _INFLIGHT_FEINS:
                return
            if len(_INFLIGHT_FEINS) >= VERIFY_TASK_QUEUE_CAP:
                logger.warning("verify task queue full (%d) — dropping task for fein=%s",
                               VERIFY_TASK_QUEUE_CAP, fein)
                return
            _INFLIGHT_FEINS.add(fein)

        def _wrapped():
            try:
                fn(*args)
            except Exception as _exc:
                logger.error("background task %s failed for fein=%s: %s",
                             fn.__name__, fein, _exc, exc_info=True)
            finally:
                with _INFLIGHT_LOCK:
                    _INFLIGHT_FEINS.discard(fein)

        try:
            _VERIFY_EXECUTOR.submit(_wrapped)
        except Exception as _sub_exc:
            # submit() itself failed (e.g. executor shut down) — release inflight slot
            logger.error("executor.submit failed for fein=%s: %s", fein, _sub_exc)
            with _INFLIGHT_LOCK:
                _INFLIGHT_FEINS.discard(fein)

    # If there's no careers_url, queue for enrichment and return immediately
    if not careers_url:
        _submit(_trigger_enrichment, fein, _r_client)
        return jsonify(payload), 200

    # Fire-and-forget HEAD check — never block the HTTP response
    _allowed_root = row.get("public_domain") or None

    def _background_verify():
        ok = _head_ok(careers_url, allowed_root=_allowed_root)
        if ok:
            # Mark URL as verified so staleness_checker skips it longer
            conn2 = get_conn()
            try:
                conn2.execute(
                    "UPDATE fein_domain_map SET careers_url_verified_at = NOW() WHERE employer_fein = %s",
                    (fein,),
                )
                conn2.commit()
            except Exception as exc:
                logger.warning("verify-company: failed to update verified_at fein=%s: %s", fein, exc)
                try:
                    conn2.rollback()
                except Exception:
                    pass
            finally:
                conn2.close()
        else:
            logger.info(
                "verify-company: HEAD failed for careers_url=%s fein=%s — triggering re-enrichment",
                careers_url, fein,
            )
            _trigger_enrichment(fein, _r_client)

    _submit(_background_verify)
    return jsonify(payload), 200


if __name__ == '__main__':
    port = int(os.environ.get('EXTENSION_API_PORT', 5000))
    logger.info("pipeline-api starting on port %s", port)
    app.run(host='0.0.0.0', port=port)

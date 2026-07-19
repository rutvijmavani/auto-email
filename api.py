import base64
import hmac
import json
import os
from datetime import datetime, timezone

from flask import Flask, request, jsonify, make_response, redirect
from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

load_dotenv()

from logger import get_logger, init_logging, cleanup_logs_if_due
from config import REDIS_EMAIL_PUSH
from db.applications import add_application
from db.gmail_tokens import upsert_token, update_watch
from workers.redis_client import get_redis

_GMAIL_SCOPES       = ["https://www.googleapis.com/auth/gmail.readonly"]
_CLIENT_ID          = os.environ.get("GMAIL_CLIENT_ID", "")
_CLIENT_SECRET      = os.environ.get("GMAIL_CLIENT_SECRET", "")
_REDIRECT_URI       = os.environ.get("GMAIL_OAUTH_REDIRECT_URI", "")
_PUBSUB_TOPIC       = os.environ.get("GMAIL_PUBSUB_TOPIC", "")

init_logging('api')
logger = get_logger(__name__)

app = Flask(__name__)

_API_KEY = os.environ.get('EXTENSION_API_KEY', '')


def _cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-API-Key'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    return response


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

    We write this to the Redis queue and return 200 immediately.
    Pub/Sub retries if we return non-200, so Redis write failures return 500.
    """
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

def _make_flow() -> Flow:
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
        redirect_uri=_REDIRECT_URI,
    )


@app.route('/oauth/start')
def oauth_start():
    """
    Begin the OAuth flow for a user. Visit this URL once per user to grant
    Gmail read access.

    Query param: user_id (int) — must already exist in the users table.

    Redirects to Google's consent screen. On approval, Google calls /oauth/callback.
    """
    user_id = request.args.get("user_id", type=int)
    if not user_id:
        return jsonify({"error": "user_id required"}), 400

    auth_url, _ = _make_flow().authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",   # force refresh token on every auth
        state=str(user_id),
    )
    logger.info("oauth/start redirecting user_id=%s to Google consent", user_id)
    return redirect(auth_url)


@app.route('/oauth/callback')
def oauth_callback():
    """
    OAuth callback — Google redirects here after the user grants access.
    Exchanges the auth code for tokens, stores the encrypted refresh token,
    and starts the Gmail watch for this user.
    """
    code    = request.args.get("code")
    user_id = request.args.get("state", type=int)
    error   = request.args.get("error")

    if error:
        logger.warning("oauth/callback: user_id=%s denied access: %s", user_id, error)
        return jsonify({"error": "access denied", "detail": error}), 400

    if not code or not user_id:
        return jsonify({"error": "missing code or state"}), 400

    try:
        flow = _make_flow()
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


if __name__ == '__main__':
    port = int(os.environ.get('EXTENSION_API_PORT', 5000))
    logger.info("pipeline-api starting on port %s", port)
    app.run(host='0.0.0.0', port=port)

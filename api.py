import hmac
import os
from datetime import datetime

from flask import Flask, request, jsonify, make_response
from dotenv import load_dotenv

load_dotenv()

from logger import get_logger, init_logging, cleanup_logs_if_due
from db.applications import add_application

init_logging('api')
logger = get_logger(__name__)

app = Flask(__name__)

_API_KEY = os.environ.get('EXTENSION_API_KEY', '')


def _cors(response):
    origin = request.headers.get('Origin', '')
    if origin.startswith('chrome-extension://'):
        response.headers['Access-Control-Allow-Origin'] = origin
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


if __name__ == '__main__':
    port = int(os.environ.get('EXTENSION_API_PORT', 5000))
    logger.info("pipeline-api starting on port %s", port)
    app.run(host='0.0.0.0', port=port)

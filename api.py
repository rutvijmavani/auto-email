import os
from datetime import datetime

from flask import Flask, request, jsonify, make_response
from dotenv import load_dotenv

load_dotenv()

from db.applications import add_application

app = Flask(__name__)

_API_KEY = os.environ.get('EXTENSION_API_KEY', '')


def _cors(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, X-API-Key'
    response.headers['Access-Control-Allow-Methods'] = 'POST, GET, OPTIONS'
    return response


@app.after_request
def after_request(response):
    return _cors(response)


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'time': datetime.utcnow().isoformat()})


@app.route('/add-application', methods=['OPTIONS'])
def add_application_options():
    return _cors(make_response('', 204))


@app.route('/add-application', methods=['POST'])
def add_application_endpoint():
    if _API_KEY and request.headers.get('X-API-Key') != _API_KEY:
        return jsonify({'error': 'unauthorized'}), 401

    data = request.get_json(silent=True) or {}

    company  = (data.get('company')   or '').strip()
    job_url  = (data.get('job_url')   or '').strip()
    job_title = (data.get('job_title') or '').strip() or None
    status   = data.get('status', 'active')
    user_id  = int(data.get('user_id', 1))

    if not company:
        return jsonify({'error': 'company is required'}), 400
    if not job_url:
        return jsonify({'error': 'job_url is required'}), 400
    if status not in ('active', 'prospective'):
        status = 'active'

    app_id, created = add_application(
        company=company,
        job_url=job_url,
        job_title=job_title,
        status_override=status,
        user_id=user_id,
    )

    if app_id is None:
        return jsonify({'error': 'failed to insert'}), 500

    return jsonify({'id': app_id, 'created': created}), 201 if created else 200


if __name__ == '__main__':
    port = int(os.environ.get('EXTENSION_API_PORT', 5000))
    app.run(host='0.0.0.0', port=port)

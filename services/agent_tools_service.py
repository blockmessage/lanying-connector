import json
import os

from flask import Blueprint, make_response, request

import lanying_agent_tools
from lanying_async import executor


service = 'agent_tools'
bp = Blueprint(service, __name__)


def _authorized():
    configured = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    return bool(configured and configured == request.headers.get('access-token', ''))


def _body():
    try:
        value = json.loads(request.get_data(as_text=True) or '{}')
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def _actor(data):
    # These fields are overwritten by Butler after it validates the Console
    # access token.  Connector's endpoint is protected by the shared service
    # token and is not exposed as an end-user authentication boundary.
    return {
        'subject_id': str(data.get('actor_subject_id', '')),
        'tenement_id': str(data.get('actor_tenement_id', '')),
        'role': str(data.get('actor_role', '')),
        'im_user_id': str(data.get('im_user_id', '')),
        'client_instance_id': str(data.get('client_instance_id', '')),
    }


def _response(result):
    if result.get('result') == 'ok':
        return make_response({'code': 200, 'data': result.get('data', {})})
    status = 404 if result.get('message') in [
        'tool request not found', 'Skill revision not found',
        'public Skill not found'] else 400
    return make_response({
        'code': status,
        'message': result.get('message', 'request failed'),
        'error_code': result.get('code', '')
    })


@bp.route('/service/agent_tools/public_catalog/notify', methods=['POST'])
def notify_public_catalog():
    if len(request.get_data(cache=True)) > lanying_agent_tools.MAX_NOTIFY_BYTES:
        return make_response({'code': 413, 'message': 'notification is too large'}, 413)
    try:
        body = json.loads(request.get_data(as_text=True) or '{}')
        if not isinstance(body, dict):
            raise ValueError('body must be an object')
    except (TypeError, ValueError, json.JSONDecodeError):
        return make_response({'code': 400, 'message': 'invalid notification'}, 400)
    try:
        result = lanying_agent_tools.enqueue_public_catalog_sync(
            request.remote_addr or '')
    except Exception:
        return make_response({'code': 503, 'message': 'failed to queue catalog sync'}, 503)
    if result.get('code') == 'rate_limited':
        return make_response({'code': 429, 'message': result.get('message')}, 429)
    return make_response({'code': 202, 'data': result.get('data', {})}, 202)


@bp.route('/service/agent_tools/public_catalog', methods=['POST'])
def list_public_catalog():
    denied = _require_auth()
    if denied:
        return denied
    catalog = lanying_agent_tools.get_public_catalog()
    if not catalog:
        return make_response({'code': 503, 'message': 'public Skill catalog is unavailable'}, 503)
    return make_response({'code': 200, 'data': lanying_agent_tools.public_catalog_view(catalog)})


@bp.route('/service/agent_tools/public_catalog/detail', methods=['POST'])
def get_public_skill_detail():
    denied = _require_auth()
    if denied:
        return denied
    return _response(lanying_agent_tools.public_skill_detail(
        str(_body().get('skill_id', ''))))


@bp.route('/service/agent_tools/public_catalog/refresh', methods=['POST'])
def refresh_public_catalog():
    denied = _require_auth()
    if denied:
        return denied
    return _response(lanying_agent_tools.sync_public_catalog())


@bp.route('/service/agent_tools/authorization/sync', methods=['POST'])
def sync_authorization():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    return _response(lanying_agent_tools.sync_authorization_projection(
        str(data.get('app_id', '')), data))


@bp.route('/service/agent_tools/authorization/status', methods=['POST'])
def authorization_status():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    projection = lanying_agent_tools.get_authorization_projection(
        str(data.get('app_id', '')))
    return make_response({'code': 200, 'data': {
        'authorization_revision': -1 if projection is None else int(
            projection.get('authorization_revision', 0))
    }})


def _require_auth():
    if _authorized():
        return None
    return make_response({'code': 401, 'message': 'bad authorization'})


@bp.route('/service/agent_tools/capabilities', methods=['POST'])
def capabilities():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    return _response(lanying_agent_tools.register_capabilities(
        str(data.get('app_id', '')), _actor(data), data))


@bp.route('/service/agent_tools/configure', methods=['POST'])
def configure():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    return _response(lanying_agent_tools.configure_feature(
        str(data.get('app_id', '')), data.get('enabled', False),
        str(data.get('chatbot_id', '*'))))


@bp.route('/service/agent_tools/request', methods=['POST'])
def get_tool_request():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    return _response(lanying_agent_tools.get_request_for_actor(
        str(data.get('app_id', '')), str(data.get('request_id', '')), _actor(data)))


def _resume_async(result):
    if not result.get('resume'):
        return
    request_info = result.get('request')
    tool_result = lanying_agent_tools.tool_result_for_model(result.get('data', {}))
    if not request_info:
        return

    def resume():
        # Imported lazily to avoid a blueprint import cycle.
        import openai_service
        openai_service.resume_client_tool_request(request_info, tool_result)

    request_id = str(request_info.get('request_id', ''))
    app_id = str(request_info.get('app_id', ''))
    lanying_agent_tools.record_resume_status(app_id, request_id, 'queued')
    try:
        executor.submit(resume)
    except Exception as error:
        lanying_agent_tools.record_resume_status(
            app_id, request_id, 'failed', str(error))


@bp.route('/service/agent_tools/decision', methods=['POST'])
def decide_tool_request():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    result = lanying_agent_tools.decide_request(
        str(data.get('app_id', '')), str(data.get('request_id', '')),
        _actor(data), str(data.get('decision', 'reject')),
        data.get('authorization_revision'))
    if result.get('result') == 'ok':
        _resume_async(result)
        return make_response({'code': 200, 'data': result.get('data', {})})
    return _response(result)


@bp.route('/service/agent_tools/result', methods=['POST'])
def submit_tool_result():
    denied = _require_auth()
    if denied:
        return denied
    data = _body()
    result = lanying_agent_tools.submit_local_result(
        str(data.get('app_id', '')), str(data.get('request_id', '')),
        _actor(data), data.get('result', {}))
    if result.get('result') == 'ok':
        _resume_async(result)
        return make_response({'code': 200, 'data': result.get('data', {})})
    return _response(result)

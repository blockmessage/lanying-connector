from flask import Blueprint, request, make_response, send_file, abort
import logging
import os
import json
import lanying_openclaw
import lanying_openclaw_sync_validation
from datetime import date as datetime_date
from datetime import timedelta as datetime_timedelta
service = 'openclaw'
bp = Blueprint(service, __name__)

def html_response(body, status_code=200):
    resp = make_response(body, status_code)
    resp.headers['Content-Type'] = 'text/html; charset=utf-8'
    return resp

# @bp.route("/service/openclaw/check_client_login", methods=["POST"])
# def check_client_login():
#     if not check_openclaw_server_access_token_valid():
#         resp = make_response({'code':401, 'message':'bad authorization'})
#         return resp
#     text = request.get_data(as_text=True)
#     data = json.loads(text)
#     token = str(data['token'])
#     result = lanying_openclaw.check_client_login(token)
#     if result['result'] == 'error':
#         resp = make_response({'code':400, 'message':result['message']})
#     else:
#         resp = make_response({'code':200, 'data':result["data"]})
#     return resp

# @bp.route("/service/openclaw/message", methods=["POST"])
# def message():
#     if not check_openclaw_server_access_token_valid():
#         resp = make_response({'code':401, 'message':'bad authorization'})
#         return resp
#     text = request.get_data(as_text=True)
#     data = json.loads(text)
#     token = str(data['token'])
#     message = dict(data['message'])
#     result = lanying_openclaw.send_lanying_message(token, message)
#     if result['result'] == 'error':
#         resp = make_response({'code':400, 'message':result['message']})
#     else:
#         resp = make_response({'code':200, 'data':result["data"]})
#     return resp

@bp.route("/service/openclaw/check_create_node", methods=["POST"])
def check_create_node():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_openclaw.check_create_node(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/create_node", methods=["POST"])
def create_node():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    name = str(data['name'])
    product_id = str(data['product_id'])
    charge_id = str(data['charge_id'])
    node_id = str(data['node_id'])
    lanying_link = str(data['lanying_link'])
    access_type = str(data.get('access_type', 'friend'))
    show_in_support = data.get('show_in_support')
    access_list = '' # not allow when create
    chatbot_id = '' # not allow when create
    session_map_sync = str(data.get('session_map_sync', 'off') or 'off')
    merge_sub_sessions = str(data.get('merge_sub_sessions', 'off') or 'off')
    setting = lanying_openclaw.NodeSetting(
        app_id=app_id,
        name=name,
        product_id=product_id,
        charge_id=charge_id,
        node_id = node_id,
        lanying_link = lanying_link,
        access_type=access_type,
        access_list=access_list,
        chatbot_id = chatbot_id,
        session_map_sync=session_map_sync,
        merge_sub_sessions=merge_sub_sessions,
        show_in_support=show_in_support
    )
    result = lanying_openclaw.create_node(setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/configure_node", methods=["POST"])
def configure_node():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    node_id = str(data['node_id'])
    name = str(data['name'])
    lanying_link = str(data['lanying_link'])
    access_type = str(data['access_type'])
    access_list = str(data['access_list'])
    show_in_support = data.get('show_in_support')
    chatbot_id = str(data['chatbot_id'])
    session_map_sync = str(data.get('session_map_sync', 'off') or 'off')
    merge_sub_sessions = str(data.get('merge_sub_sessions', 'off') or 'off')
    setting = lanying_openclaw.ConfigureNodeParam(
        name=name,
        lanying_link = lanying_link,
        access_type=access_type,
        access_list=access_list,
        chatbot_id = chatbot_id,
        session_map_sync=session_map_sync,
        merge_sub_sessions=merge_sub_sessions,
        show_in_support=show_in_support
    )
    result = lanying_openclaw.configure_node(app_id, node_id, setting)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/sync_model_config", methods=["POST"])
def sync_model_config():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    node_id = str(data['node_id'])
    result = lanying_openclaw.sync_model_config(app_id, node_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/sync_model_config_and_wait", methods=["POST"])
def sync_model_config_and_wait():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    node_id = str(data['node_id'])
    wait_timeout_ms = data.get('wait_timeout_ms', lanying_openclaw.CONFIG_SYNC_WAIT_TIMEOUT_MS)
    result = lanying_openclaw.sync_model_config_and_wait(app_id, node_id, wait_timeout_ms)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/probe_node", methods=["POST"])
def probe_node():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    node_id = str(data['node_id'])
    wait_timeout_ms = data.get('wait_timeout_ms', lanying_openclaw.PROBE_WAIT_TIMEOUT_MS)
    wait_for_fresh_report = data.get('wait_for_fresh_report', True)
    result = lanying_openclaw.probe_node(app_id, node_id, wait_timeout_ms, wait_for_fresh_report)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/get_node_list", methods=["POST"])
def get_node_list():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_openclaw.get_node_list(app_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/generate_app_manager_login_code", methods=["POST"])
def generate_app_manager_login_code():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    expire_seconds = data.get('expire_seconds', 300)
    result = lanying_openclaw.generate_openclaw_app_manager_login_code(app_id, expire_seconds)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/delete_node", methods=["POST"])
def delete_node():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    node_id = str(data['node_id'])
    result = lanying_openclaw.delete_node(app_id, node_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/run_sync_validation", methods=["POST"])
def run_sync_validation():
    try:
        if not check_access_token_valid():
            return html_response('<h1>bad authorization</h1>', 401)
        text = request.get_data(as_text=True)
        try:
            data = json.loads(text) if text else {}
        except Exception:
            return html_response('<h1>invalid json</h1>', 400)
        app_id = str(data.get('app_id', '') or '')
        node_id = str(data.get('node_id', '') or '')
        scenario = data.get('scenario')
        scenarios = data.get('scenarios')
        result = lanying_openclaw_sync_validation.start(app_id, node_id, scenario, scenarios)
        if result.get('result') != 'ok':
            return html_response(f"<h1>start sync validation failed</h1><p>{result.get('message', '')}</p>", 400)
        task_info = result.get('data', {})
        task = lanying_openclaw_sync_validation.get_task(task_info.get('task_id', ''))
        if not isinstance(task, dict):
            return html_response('<h1>task created but not found</h1>', 500)
        return html_response(lanying_openclaw_sync_validation.build_status_page(task))
    except Exception as err:
        logging.exception("run_sync_validation route failed")
        return html_response(f"<h1>run sync validation crashed</h1><pre>{str(err)}</pre>", 500)

@bp.route("/service/openclaw/sync_validation/<string:task_id>", methods=["GET"])
def sync_validation_report(task_id):
    if not check_access_token_valid():
        return html_response('<h1>bad authorization</h1>', 401)
    task = lanying_openclaw_sync_validation.get_task(task_id)
    if not isinstance(task, dict):
        report_path = lanying_openclaw_sync_validation.get_report_path(task_id)
        if os.path.exists(report_path):
            return send_file(report_path)
        return html_response('<h1>task not found</h1>', 404)
    if os.path.exists(task.get('report_path', '')):
        return send_file(task.get('report_path', ''))
    return html_response(lanying_openclaw_sync_validation.build_status_page(task))

def check_openclaw_server_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = lanying_openclaw.get_access_token()
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

from flask import Blueprint, request, make_response, send_file, abort
import logging
import os
import json
import lanying_openclaw
from datetime import date as datetime_date
from datetime import timedelta as datetime_timedelta
service = 'openclaw'
bp = Blueprint(service, __name__)

@bp.route("/service/openclaw/check_client_login", methods=["POST"])
def check_client_login():
    if not check_openclaw_server_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    token = str(data['token'])
    result = lanying_openclaw.check_client_login(token)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/openclaw/message", methods=["POST"])
def message():
    if not check_openclaw_server_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    token = str(data['token'])
    message = dict(data['message'])
    result = lanying_openclaw.send_lanying_message(token, message)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

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
    setting = lanying_openclaw.NodeSetting(
        app_id=app_id,
        name=name,
        product_id=product_id,
        charge_id=charge_id,
        node_id = node_id
    )
    result = lanying_openclaw.create_node(setting)
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
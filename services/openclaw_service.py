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
    if not check_access_token_valid():
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
    if not check_access_token_valid():
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

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = lanying_openclaw.get_access_token()
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False

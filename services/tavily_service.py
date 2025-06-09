from flask import Blueprint, request, make_response, send_file, Response
import logging
import os
import lanying_config
import requests
import lanying_redis
from datetime import datetime
import time
from urllib.parse import urlencode
from openai_service import check_deduct_message_quota, deduct_message_quota
import json
from lanying_async import executor
import lanying_slack

service = 'tavily'
bp = Blueprint(service, __name__)

@bp.route("/search", methods=["POST"])
@bp.route("/extract", methods=["POST"])
@bp.route("/tavily/search", methods=["POST"])
@bp.route("/tavily/extract", methods=["POST"])
def forward_tavily_search():
    result = do_forward_tavily_search(request)
    if result['result'] == 'error':
        error_message = result.get('message', '')
        error_code = result.get('code', error_message)
        error_info = {
            'detail': {
                'error': f'{error_code}:{error_message}'
            }
        }
        return make_response(error_info, 400)
    else:
        response = result['response']
        return Response(response.content, status=response.status_code, headers=response.headers.items())

def do_forward_tavily_search(request):
    path = request.path
    text = request.get_data(as_text=True)
    request_body = json.loads(text)
    logging.info(f"do_forward_tavily_search receive forward | request_body:{request_body}")
    auth_result = check_authorization(request)
    if auth_result['result'] == 'error':
        logging.info(f"do_forward_tavily_search check_authorization deny, msg={auth_result}")
        return auth_result
    app_id = auth_result['app_id']
    config = auth_result['config']
    deduct_res = check_message_deduct_failed(app_id)
    if deduct_res['result'] == 'error':
        logging.info(f"check_message_deduct_failed deny: app_id={app_id}, msg={deduct_res}")
        return deduct_res
    check_res = check_search_quota_and_token(request)
    if check_res['result'] == 'error':
        logging.info(f"do_forward_tavily_search check_search_quota_and_token deny, msg={check_res}")
        return check_res
    quota = check_res['quota']
    lanying_quota = tavily_quota_to_lanying_quota(quota)
    limit_res = check_deduct_message_quota(app_id, config, lanying_quota)
    if limit_res['result'] == 'error':
        logging.info(f"check_deduct_message_quota deny: app_id={app_id}, msg={limit_res}")
        return limit_res
    api_key_type = limit_res['api_key_type']
    response = None
    response_json = {}
    try:
        api_secret = os.getenv(f"TAVILY_API_SECRET", '')
        logging.info(f"do_forward_tavily_search forward start | app_id:{app_id}, request_body:{request_body}")
        response = forwart_request(app_id, request, api_secret)
        logging.info(f"do_forward_tavily_search forward finish | app_id:{app_id}, status_code: {response.status_code}")
        response_json = response.json()
    except Exception as e:
        logging.info(f"do_forward_tavily_search forward exception | app_id:{app_id}")
        logging.exception(e)
    if response is None:
        logging.info(f"do_forward_tavily_search failed response is None | app_id:{app_id}")
        return {'result': 'error', 'message': 'Lanying internal error', 'code': 'LanyingInternalError'}
    if response.status_code == 200:
        new_quota = calc_success_quota(check_res, response_json, quota)
        new_lanying_quota = tavily_quota_to_lanying_quota(new_quota)
        logging.info(f"do_forward_tavily_search success | app_id:{app_id}, quota:{quota}, new_quota:{new_quota}, new_lanying_quota:{new_lanying_quota}, path:{path}, request_body:{request_body}")
        try:
            executor.submit(add_tavily_quota_used, app_id, new_quota)
        except Exception as e:
            logging.exception(e)
        deduct_message_quota(app_id, config, new_lanying_quota, api_key_type, 'tavily')
    else:
        logging.info(f"do_forward_tavily_search failed | app_id:{app_id}, quota:{quota}, request_body:{request_body}, response_json:{response_json}")
    return {'result':'ok', 'response': response}

def check_authorization(request):
    try:
        authorization = request.headers.get('Authorization')
        if authorization:
            token = str(authorization)
            prefix = 'Bearer '
            if token.startswith(prefix):
                token = token[len(prefix):]
            tokens = token.split("-")
            if len(tokens) == 3:
                app_id = tokens[0]
                config = lanying_config.get_lanying_connector(app_id)
                if config:
                    if token == config.get('access_token', ''):
                        return {'result':'ok', 'app_id':app_id, 'config': config}
    except Exception as e:
        logging.exception(e)
    return {'result':'error', 'message':'Lanying bad authorization', 'code':'LanyingBadAuthorization'}

def forwart_request(app_id, request, api_secret):
    path = request.path
    endpoint = get_tavily_endpoint()
    if path == '/search' or path == '/tavily/search':
        url = f"{endpoint}/search"
    elif path == '/extract' or path == '/tavily/extract':
        url = f"{endpoint}/extract"
    text = request.get_data(as_text=True)
    request_body = json.loads(text)
    headers = dict(request.headers)
    for header_key in ['Remoteip', 'Host', 'X-Forwarded-For', 'Connection', 'User-Agent', 'Ocp-Apim-Subscription-Key']:
        if header_key in headers:
            del headers[header_key]
    logging.info(f"forward tavily request start | app_id:{app_id}, url:{url}, request_body:{request_body}, header_keys: {headers.keys()}")
    headers['Authorization'] = api_secret
    response = requests.post(url, json=request_body, headers=headers)
    logging.info(f"forward tavily request finish | app_id:{app_id}, status_code:{response.status_code}, response_text:{response.text}")
    return response

def get_tavily_endpoint():
    return os.getenv("TAVILY_ENDPOINT", 'https://api.tavily.com')

def check_message_deduct_failed(app_id):
    if lanying_config.get_lanying_connector_deduct_failed(app_id):
        return {'result':'error', 'message': 'Lanying Deduct Failed', 'code': 'LanyingDeductFailed'}
    return {'result':'ok'}

def check_search_quota_and_token(request):
    path = request.path
    text = request.get_data(as_text=True)
    data = json.loads(text)
    quota_per_url = None
    if path == '/search' or path == '/tavily/search':
        search_depth = data.get('search_depth', 'basic')
        if search_depth == 'advanced':
            quota = 2
        elif search_depth == 'basic':
            quota = 1
        else:
            return {'result': 'error', 'message': 'arg search_depth must be basic or advanced', 'code': 'InvalidArguments'}
    elif path == '/extract' or path == '/tavily/extract':
        extract_depth = data.get('extract_depth', 'basic')
        urls = data.get('urls', '')
        if isinstance(urls, str) and urls != '':
            url_cnt = 1
        elif isinstance(urls, list):
            url_cnt = len(urls)
        else:
            return {'result': 'error', 'message': 'arg urls required', 'code': 'InvalidArguments'}
        if extract_depth == 'advanced':
            quota_per_url = 2 / 5
            quota = quota_per_url * url_cnt
        elif extract_depth == 'basic':
            quota_per_url = 1 / 5
            quota = quota_per_url * url_cnt
        else:
            return {'result': 'error', 'message': 'arg extract_depth must be basic or advanced', 'code': 'InvalidArguments'}
    else:
        return {'result':'error', 'message':'Lanying api not support', 'code':'LanyingAPINotSupport'}
    return {'result': 'ok', 'quota': quota, 'quota_per_url': quota_per_url}

def calc_success_quota(check_res, response_json, quota):
    try:
        if 'quota_per_url' in check_res and 'results' in response_json:
            return check_res['quota_per_url'] * len(response_json['results'])
    except Exception as e:
        return quota

def tavily_quota_to_lanying_quota(N):
    return N * 10.67

def add_tavily_quota_used(app_id, quota):
    now = datetime.now()
    everyday_key = f"lanying-connector:tavily_quota_everyday:{now.strftime('%Y-%m-%d')}"
    total_key = 'lanying-connector:tavily_quota_total'
    redis = lanying_redis.get_redis_connection()
    if redis:
        new_total = redis.incrbyfloat(total_key, quota)
        if (new_total // 100) > ((new_total - quota) // 100):
            lanying_slack.async_send_message(f'[Tavily Search]累计用量：{new_total} API credit')
        redis.hincrbyfloat(everyday_key, app_id, quota)

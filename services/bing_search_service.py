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

service = 'bing_search'
bp = Blueprint(service, __name__)

@bp.route("/v7.0/search", methods=["GET"])
@bp.route("/v7.0/news/search", methods=["GET"])
@bp.route("/v7.0/news", methods=["GET"])
@bp.route("/v7.0/news/trendingtopics", methods=["GET"])
@bp.route("/v7.0/images/search", methods=["GET"])
@bp.route("/v7.0/images/trending", methods=["GET"])
@bp.route("/v7.0/videos/search", methods=["GET"])
@bp.route("/v7.0/videos/details", methods=["GET"])
@bp.route("/v7.0/videos/trending", methods=["GET"])
@bp.route("/v7.0/entities", methods=["GET"])
def forward_bing_search():
    result = do_forward_bing_search(request)
    if result['result'] == 'error':
        error_message = result.get('message', '')
        error_code = result.get('code', error_message)
        error_info = {
            '_type': 'ErrorResponse',
            'errors': [
                {
                    '_type': 'Error',
                    'code': error_code,
                    'message': error_message
                }
            ]
        }
        return make_response(error_info, 400)
    else:
        response = result['response']
        response.headers['Content-Encoding'] = 'identity'
        return Response(response.content, status=response.status_code, headers=response.headers.items())

def do_forward_bing_search(request):
    path = request.path
    now_datetime = datetime.now()
    request_args = dict(request.args)
    logging.info(f"do_forward_bing_search receive forward | request_args:{request_args}")
    auth_result = check_authorization(request)
    if auth_result['result'] == 'error':
        logging.info(f"bing_search check_authorization deny, msg={auth_result}")
        return auth_result
    app_id = auth_result['app_id']
    config = auth_result['config']
    deduct_res = check_message_deduct_failed(app_id)
    if deduct_res['result'] == 'error':
        logging.info(f"check_message_deduct_failed deny: app_id={app_id}, msg={deduct_res}")
        return deduct_res
    check_res = check_search_quota_and_token(request)
    if check_res['result'] == 'error':
        logging.info(f"bing_search check_search_quota_and_token deny, msg={check_res}")
        return check_res
    quota = check_res['quota']
    limit_res = check_deduct_message_quota(app_id, config, quota)
    if limit_res['result'] == 'error':
        logging.info(f"check_deduct_message_quota deny: app_id={app_id}, msg={limit_res}")
        return limit_res
    openai_key_type = limit_res['openai_key_type']
    packages = check_res['packages']
    response = None
    package_name = ''
    origin_package_name = ''
    response_json = {}
    for package in packages:
        package_name = package['name']
        origin_package_name = package.get('origin_name', '')
        if package_name == 'Free':
            if is_in_traffic_limit(now_datetime, 1) or is_in_traffic_limit(now_datetime, 86400):
                logging.info(f"do_forward_bing_search in traffic limit | app_id:{app_id}, package_name:{package_name}")
                continue
        try:
            api_secret = os.getenv(f"BING_SEARCH_API_SECRET_{package_name}".upper(), '')
            if api_secret == '':
                continue
            logging.info(f"do_forward_bing_search forward start | app_id:{app_id}, package_name:{package_name}, request_args:{request_args}")
            response = forwart_request(app_id, request, api_secret)
            logging.info(f"do_forward_bing_search forward finish | app_id:{app_id}, package_name:{package_name}, status_code: {response.status_code}")
            need_try_next = False
            response_json = response.json()
            if response.status_code != 200:
                logging.info(f"do_forward_bing_search forward error | app_id:{app_id}, package_name:{package_name}, response_json:{response_json}")
                try:
                    if response_json['errors'][0]['code'] == 'RateLimitExceeded':
                        logging.info(f"do_forward_bing_search forward RateLimitExceeded | app_id:{app_id}, package_name:{package_name}, status_code: {response.status_code}")
                        need_try_next = True
                        if package_name == 'Free':
                            if response.status_code == 403:
                                logging.info(f"do_forward_bing_search set in traffic limit 86400 | app_id:{app_id}, package_name:{package_name}")
                                set_in_traffic_limit(now_datetime, 86400)
                            elif response.status_code == 429:
                                logging.info(f"do_forward_bing_search set in traffic limit 1 | app_id:{app_id}, package_name:{package_name}")
                                set_in_traffic_limit(now_datetime, 1)
                except Exception as e:
                    pass
            if not need_try_next:
                break
        except Exception as e:
            logging.info(f"do_forward_bing_search forward exception | app_id:{app_id}, package_name:{package_name}")
            logging.exception(e)
            break
    if response is None:
        logging.info(f"do_forward_bing_search failed response is None | app_id:{app_id}, package_name:{package_name}")
        return {'result': 'error', 'message': 'Lanying internal error', 'code': 'LanyingInternalError'}
    if response.status_code == 200:
        logging.info(f"do_forward_bing_search success | app_id:{app_id}, quota:{quota}, package_name:{package_name}, origin_package_name:{origin_package_name}, path:{path}, request_args:{request_args}")
        deduct_message_quota(app_id, config, quota, openai_key_type, 'bing_search')
    else:
        logging.info(f"do_forward_bing_search failed | app_id:{app_id}, quota:{quota}, package_name:{package_name}, request_args:{request_args}, response_json:{response_json}")
    return {'result':'ok', 'response': response}

def check_authorization(request):
    try:
        authorization = request.headers.get('Ocp-Apim-Subscription-Key')
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
    endpoint = get_bing_search_endpoint()
    url = f"{endpoint}{request.path}"
    params = dict(request.args)
    headers = dict(request.headers)
    for header_key in ['Remoteip', 'Host', 'X-Forwarded-For', 'Connection', 'User-Agent', 'Accept-Encoding', 'Accept', 'Ocp-Apim-Subscription-Key']:
        if header_key in headers:
            del headers[header_key]
    headers['Ocp-Apim-Subscription-Key'] = api_secret
    logging.info(f"forward bing search request start | app_id:{app_id}, url:{url}, param:{params}, header_keys: {headers.keys()}")
    if request.path == '/v7.0/search' and 'responseFilter' in params and ',' in params['responseFilter']:
        query_string = urlencode(params, safe=',')
        logging.info(f"forward bing search request fix querystring | app_id:{app_id}, url:{url}, param:{params}, query_string: {query_string}")
        response = requests.get(f'{url}?{query_string}', headers=headers)
    else:
        response = requests.get(url, params=params, headers=headers)
    logging.info(f"forward bing search request finish | app_id:{app_id}, status_code:{response.status_code}, response_text:{response.text}")
    return response

def get_bing_search_endpoint():
    return os.getenv("BING_SEARCH_ENDPOINT", 'https://api.bing.microsoft.com')

def check_message_deduct_failed(app_id):
    if lanying_config.get_lanying_connector_deduct_failed(app_id):
        return {'result':'error', 'message': 'Lanying Deduct Failed', 'code': 'LanyingDeductFailed'}
    return {'result':'ok'}

def check_search_quota_and_token(request):
    path = request.path
    if path == '/v7.0/search':
        responseFilter = request.args.get('responseFilter','')
        if responseFilter == '':
            features = ['Webpages', 'News', 'Images', 'Videos', 'Entities']
        else:
            features = []
            for feature in responseFilter.split(','):
                if feature in ['Webpages', 'News', 'Images', 'Videos', 'Entities']:
                    features.append(feature)
            if feature == []:
                features = ['Webpages', 'News', 'Images', 'Videos', 'Entities']
    elif path in ['/v7.0/news/search', '/v7.0/news', '/v7.0/news/trendingtopics']:
        features = ['News']
    elif path in ['/v7.0/images/search', '/v7.0/images/trending']:
        features = ['Images']
    elif path in ['/v7.0/videos/search', '/v7.0/videos/details', '/v7.0/videos/trending']:
        features = ['Videos']
    elif path == '/v7.0/entities':
        features = ['Entities']
    else:
        return {'result':'error', 'message':'Lanying api not support', 'code':'LanyingAPINotSupport'}
    packages = get_all_packages()
    choosed_packages = []
    for package in packages:
        is_good = True
        for feature in features:
            if feature not in package['features']:
                is_good = False
        if is_good:
            choosed_packages.append(package)
    if choosed_packages == []:
        {'result':'error', 'message':'Lanying api not support', 'code':'LanyingAPINotSupport'}
    sorted_packages= sorted(choosed_packages, key=lambda x: x['quota'])
    paid_package = sorted_packages[0]
    free_package = get_free_package()
    quota = paid_package['quota']
    free_package['quota'] = quota
    free_package['origin_name'] = paid_package['name']
    final_packages = [free_package, paid_package]
    return {'result': 'ok', 'packages': final_packages, 'quota': quota}

def get_all_packages():
    return [
        {
            'name': 'S1',
            'features': ['Webpages', 'News', 'Images', 'Videos', 'Entities'],
            'month_limit': -1,
            'quota': 10
        },
        {
            'name': 'S5',
            'features': ['Webpages', 'News'],
            'month_limit': -1,
            'quota': 6
        },
        {
            'name': 'S8',
            'features': ['Webpages', 'News', 'Images', 'Videos'],
            'month_limit': -1,
            'quota': 8
        }
    ]

def get_free_package():
    return {
            'name': 'Free',
            'features': ['Webpages', 'News', 'Images', 'Videos', 'Entities'],
            'month_limit': 1000
    }


def is_in_traffic_limit(now_datetime, seconds):
    key = in_traffic_limit_key(now_datetime, seconds)
    redis = lanying_redis.get_redis_connection()
    result = lanying_redis.redis_get(redis, key)
    if result == '1':
        return True
    else:
        return False

def set_in_traffic_limit(now_datetime, seconds):
    key = in_traffic_limit_key(now_datetime, seconds)
    redis = lanying_redis.get_redis_connection()
    redis.setex(key, seconds, 1)

def in_traffic_limit_key(now_datetime, seconds):
    return f"lanying_connector:bing_search:in_traffic_limit:{now_datetime.strftime('%Y-%m-%d')}:{seconds}"

import xml.etree.ElementTree as ET
import logging
from flask import Blueprint, request, make_response
import lanying_config
import hashlib
import lanying_redis
import requests
import random
import string
import json
import lanying_message

official_account_max_message_size = 600
service = 'wechat_official_account'
bp = Blueprint(service, __name__)

@bp.route("/service/wechat_official_account/app/<string:app_id>/messages", methods=["GET"])
@bp.route("/wechat/official/messages/<string:app_id>", methods=["GET"])
def service_get_messages(app_id):
    logging.info(f"app_id:{app_id}, args:{request.args.to_dict()}")
    if check_token(app_id):
        echostr = request.args.get('echostr')
        resp = make_response(echostr)
        return resp
    return 'bad_signature'

@bp.route("/service/wechat_official_account/app/<string:app_id>/messages", methods=["POST"])
@bp.route("/wechat/official/messages/<string:app_id>", methods=["POST"])
def service_post_messages(app_id):
    xml_data = request.data
    reply = 'failed'
    logging.info(f"app_id:{app_id}, xml_data:{xml_data}, request.args:{request.args.to_dict()}, headers:{request.headers.to_wsgi_list()}")
    if check_token(app_id):
        config = lanying_config.get_service_config(app_id, service)
        if config:
            xml = ET.fromstring(xml_data)
            msg_type = xml.find('MsgType').text
            if msg_type == 'text':
                to_user_name = xml.find('ToUserName').text
                from_user_name = xml.find('FromUserName').text
                create_time = xml.find('CreateTime').text
                content = xml.find('Content').text
                msg_id = xml.find('MsgId').text
                logging.info(f"app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time},msg_type:{msg_type},content:{content},msg_id:{msg_id}")
                logging.info(f"request.headers:{request.headers.items()}")
                user_id = get_or_register_user(app_id, from_user_name)
                if user_id:
                    lanying_message.send_message_async(config, app_id, user_id, config['lanying_user_id'],content)
                    reply = 'success'
                else:
                    logging.info(f"failed to get user_id | app_id:{app_id}, username:{from_user_name}")
            else:
                reply = 'success'
        else:
            logging.info(f"config not found:{app_id}, {service}")
    resp = make_response(reply)
    return resp

def handle_chat_message(config, message):
    checkres = check_message_user_ids(config, message)
    if checkres['result'] == 'error':
        return ''
    app_id = message['appId']
    to_user_id = message['to']['uid']
    logging.info(f"{service} | handle_chat_message do for user_id, app_id={app_id}, to_user_id:{to_user_id}")
    wechat_username = get_wechat_username(app_id, to_user_id)
    if wechat_username:
        send_wechat_message(config, app_id, message, wechat_username)
    return ''

def check_message_user_ids(config, message):
    from_user_id = int(message['from']['uid'])
    to_user_id = int(message['to']['uid'])
    type = message['type']
    my_user_id = config['lanying_user_id']
    if my_user_id != None and from_user_id == my_user_id and to_user_id != my_user_id and type == 'CHAT':
        logging.info(f'lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},result:ok')
        return {'result':'ok'}
    logging.info(f'lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id}, type:{type},result:error:{my_user_id != None} { from_user_id == my_user_id} {to_user_id != my_user_id} { type == "CHAT"}')
    return {'result':'error', 'msg':''}

def send_wechat_message(config, app_id, message, to_username):
    access_token = get_wechat_access_token(config, app_id)
    content = message['content']
    content_list = split_string_by_size(content, official_account_max_message_size)
    for now_content in content_list:
        data = {
            "touser": to_username,
            "msgtype": "text",
            "text": {
                "content": now_content
            }
        }
        logging.info(f"send_wechat_message start | app_id:{app_id}, to_username:{to_username}, content:{now_content}")
        url = f"https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token={access_token}"
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'))
        result = response.json()
        logging.info(f"send_wechat_message finish| app_id:{app_id}, to_username:{to_username}, content:{now_content}, result:{result}")

def check_token(app_id):
    config = lanying_config.get_service_config(app_id, service)
    if config:
        wechat_message_token = config.get('wechat_message_token')
        signature = request.args.get('signature','')
        timestamp = request.args.get('timestamp','')
        nonce = request.args.get('nonce','')
        sign_list = sorted([wechat_message_token,timestamp,nonce])
        mysignature = hashlib.sha1("".join(sign_list).encode("utf-8")).hexdigest()
        logging.info(f"my:{mysignature},got:{signature}")
        return mysignature == signature
    return True

def get_or_register_user(app_id, username):
    redis = lanying_redis.get_redis_connection()
    key = wechat_user_key(app_id, username)
    result = redis.get(key)
    if result:
        user_id = int(result)
        return user_id
    else:
        user_id = register_anonymous_user(app_id, username, "wechat_")
        if user_id:
            im_key = im_user_key(app_id, user_id)
            redis.set(key, user_id)
            redis.set(im_key, username)
        return user_id

def get_wechat_username(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    im_key = im_user_key(app_id, user_id)
    result = redis.get(im_key)
    if result:
        return str(result,'utf-8')
    logging.info(f"get_wechat_username | not found, app_id:{app_id}, user_id:{user_id}")
    return None

def get_wechat_access_token(config, app_id):
    redis = lanying_redis.get_redis_connection()
    key = wechat_access_token_key(app_id)
    result = redis.get(key)
    if result:
        return str(result, 'utf-8')
    wechat_app_id = config['wechat_app_id']
    wechat_app_secret = config['wechat_app_secret']
    if wechat_app_id and wechat_app_secret:
        token_result = get_wechat_access_token_internal(app_id, wechat_app_id, wechat_app_secret)
        if token_result['result'] == 'ok':
            access_token = token_result['access_token']
            expires_in = token_result['expires_in']
            redis.setex(key, expires_in - 120, access_token)
            return access_token
    else:
        logging.info(f"get_wechat_access_token cannot get wechat app_id or secret:app_id:{app_id}")
    return None

def get_wechat_access_token_internal(app_id, wechat_app_id, wechat_app_secret):
    url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={wechat_app_id}&secret={wechat_app_secret}"
    response = requests.get(url)
    data = response.json()
    if 'access_token' in data:
        logging.info(f"get_wechat_access_token_internal success | appid={app_id}")
        return {'result':'ok', 'access_token': data['access_token'], 'expires_in': data['expires_in']}
    else:
        logging.info(f"get_wechat_access_token_internal failed | appid={app_id}")
        return {'result': 'error'}

def wechat_user_key(app_id, username):
    return f"{service}:wechat_user:{app_id}:{username}"

def im_user_key(app_id, user_id):
    return f"{service}:im_user:{app_id}:{user_id}"

def wechat_access_token_key(app_id):
    return f"{service}:wechat_access_token:{app_id}"

def register_anonymous_user(app_id, username, prefix):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    password = get_random_string(32)
    response = requests.post(apiEndpoint + '/user/register/anonymous',
                                headers={'app_id': app_id},
                                json={'username':prefix,
                                        'password': password})
    logging.info(f"register user, app_id={app_id}, username={username}, response={response.content}")
    logging.info(password)
    response_json = json.loads(response.content)
    if response_json['code'] == 200:
        user_id = response_json['data']['user_id']
        logging.info(f"register user, app_id={app_id}, username={username}, user_id={user_id}")
        return user_id
    return None

def get_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def split_string_by_size(input_string, chunk_size):
    return [input_string[i:i+chunk_size] for i in range(0, len(input_string), chunk_size)]

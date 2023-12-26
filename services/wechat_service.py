
from flask import Blueprint, request, make_response
import logging
import time
import json
import lanying_wechat_chatbot
import lanying_chatbot
import os
import lanying_redis
import lanying_config
import requests
import string
import random
import lanying_message

wechat_max_message_size = 3900
service = 'wechat'
bp = Blueprint(service, __name__)
wechat_chatbot_message_token = os.getenv('WECHAT_CHATBOT_MESSAGE_TOKEN')

@bp.route("/wechat/<string:token>/messages", methods=["POST"])
def service_post_messages(token):
    text = request.get_data(as_text=True)
    message = json.loads(text)
    if token == wechat_chatbot_message_token:
        logging.info(f"receive wechat message:{message}")
        wc_id = message.get('wcId', '')
        account = message.get('account', '')
        message_type = message.get('messageType', '')
        data = message.get('data', {})
        if message_type == '60001':
            handle_wechat_chat_message(wc_id, account, data)
    else:
        logging.info(f"receive invalid wechat message:{message}")
    resp = make_response({'code':200, 'data':{'success': True}})
    return resp

@bp.route("/service/wechat/login", methods=["POST"])
def login():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    type = str(data['type'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    proxy = int(data.get('proxy', 0))
    ttuid = str(data.get('ttuid', ''))
    result = lanying_wechat_chatbot.login(app_id, type, wechat_chatbot_id, proxy, ttuid)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/get_login_info", methods=["POST"])
def get_login_info():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    w_id = str(data['w_id'])
    result = lanying_wechat_chatbot.get_login_info(app_id, w_id)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/create_wechat_chatbot", methods=["POST"])
def create_wechat_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    w_id = str(data['w_id'])
    chatbot_id = str(data['chatbot_id'])
    msg_types = list(data['msg_types'])
    non_friend_chat_mode = str(data['non_friend_chat_mode'])
    note = str(data.get('note',''))
    result = lanying_wechat_chatbot.create_wechat_chatbot(app_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/list_wechat_chatbots", methods=["POST"])
def list_wechat_chatbots():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    result = lanying_wechat_chatbot.list_wechat_chatbots(app_id)
    resp = make_response({'code':200, 'data': {'list': result}})
    return resp

@bp.route("/service/wechat/change_status", methods=["POST"])
def change_status():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    status = str(data['status'])
    result = lanying_wechat_chatbot.change_status(app_id, wechat_chatbot_id, status)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

@bp.route("/service/wechat/configure_wechat_chatbot", methods=["POST"])
def configure_wechat_chatbot():
    if not check_access_token_valid():
        resp = make_response({'code':401, 'message':'bad authorization'})
        return resp
    text = request.get_data(as_text=True)
    data = json.loads(text)
    app_id = str(data['app_id'])
    wechat_chatbot_id = str(data.get('wechat_chatbot_id',''))
    w_id = str(data.get('w_id',''))
    chatbot_id = str(data['chatbot_id'])
    msg_types = list(data['msg_types'])
    non_friend_chat_mode = str(data['non_friend_chat_mode'])
    note = str(data.get('note',''))
    result = lanying_wechat_chatbot.configure_wechat_chatbot(app_id, wechat_chatbot_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note)
    if result['result'] == 'error':
        resp = make_response({'code':400, 'message':result['message']})
    else:
        resp = make_response({'code':200, 'data':result["data"]})
    return resp

def handle_wechat_chat_message(wc_id, account, data):
    redis = lanying_redis.get_redis_connection()
    content = data['content']
    from_user = data['fromUser']
    msg_id = data['msgId']
    new_msg_id = data['newMsgId']
    self = data.get('self', False)
    timestamp = data['timestamp']
    to_user = data['toUser']
    wid = data['wId']
    if self:
        logging.info(f"handle_chat_message skip self message | self:{self}, wc_id: {wc_id}, account:{account}, data:{data}")
        return
    message_deduplication = message_deduplication_key(from_user, to_user, msg_id, new_msg_id)
    if redis.get(message_deduplication):
        logging.info(f"handle_chat_message skip for message_deduplication | wc_id: {wc_id}, account:{account}, data:{data}")
        return
    wc_id_info = lanying_wechat_chatbot.get_wc_id_info(wc_id)
    if wc_id_info is None:
        logging.info(f"handle_chat_message wc_id not found: wc_id: {wc_id}, account:{account}, data:{data}")
        return
    app_id = wc_id_info['app_id']
    wechat_chatbot_id = wc_id_info['wechat_chatbot_id']
    wechat_chatbot_info = lanying_wechat_chatbot.get_wechat_chatbot(app_id, wechat_chatbot_id)
    if wechat_chatbot_info is None:
        logging.info(f"handle_chat_message wechat_chatbot_id not found | app_id:{app_id}, wechat_chatbot_id:{wechat_chatbot_id}, wc_id: {wc_id}, account:{account}, data:{data}")
        return
    chatbot_id = wechat_chatbot_info['chatbot_id']
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        logging.info(f"handle_chat_message chatbot_id not found | app_id:{app_id}, wechat_chatbot_id:{wechat_chatbot_id}, wc_id: {wc_id}, account:{account}, data:{data}, chatbot_id:{chatbot_id}")
        return
    to_user_id = chatbot_info['user_id']
    from_user_id = get_or_register_user(app_id, from_user)
    if from_user_id:
        config = lanying_config.get_service_config(app_id, service)
        redis.setex(message_deduplication, 3*86400, "1")
        lanying_message.send_message_async(config, app_id, from_user_id, to_user_id,content)
    else:
        logging.info(f"handle_chat_message user_id not found: {from_user_id}")

def handle_chat_message(config, message):
    checkres = check_message_need_send(config, message)
    if checkres['result'] == 'error':
        return
    wechat_chatbot = checkres['wechat_chatbot']
    app_id = message['appId']
    to_user_id = message['to']['uid']
    logging.info(f"{service} | handle_chat_message do for user_id, app_id={app_id}, to_user_id:{to_user_id}")
    wechat_username = get_wechat_username(app_id, to_user_id)
    if wechat_username:
        w_id = wechat_chatbot['w_id']
        if len(w_id) > 0:
            send_wechat_message(config, app_id, message, wechat_username, w_id)

def check_message_need_send(config, message):
    from_user_id = int(message['from']['uid'])
    to_user_id = int(message['to']['uid'])
    app_id = str(message['app_id'])
    type = message['type']
    chatbot_id = lanying_chatbot.get_user_chatbot_id(app_id, from_user_id)
    if chatbot_id is None:
        return {'result': 'error', 'message': 'chatbot not found'}
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {'result': 'error', 'message': 'chatbot not found'}
    wechat_chatbot_id = chatbot['wechat_chatbot_id']
    wechat_chatbot = lanying_wechat_chatbot.get_wechat_chatbot(app_id,wechat_chatbot_id)
    if wechat_chatbot is None:
        return {'result': 'error', 'message': 'wechat_chatbot not found'}
    my_user_id = chatbot['user_id']
    if my_user_id != None and from_user_id == my_user_id and to_user_id != my_user_id and (type == 'CHAT' or type == 'REPLACE' or type == 'APPEND'):
        ext = message.get('ext', '')
        try:
            json_ext = json.loads(ext)
        except Exception as e:
            json_ext = {}
        try:
            is_stream = (json_ext['ai']['stream'] == True)
            is_finish = (json_ext['ai']['finish'] == True)
        except Exception as e:
            is_stream = False
            is_finish = False
        if is_stream:
            if type == 'REPLACE':
                pass
            elif type == 'CHAT' and is_finish:
                pass
            else:
                logging.info(f"skip chat and stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'msg':''}
        else:
            if type == 'REPLACE' or type == 'APPEND':
                logging.info(f"skip EDIT and not stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'msg':''}
        logging.info(f'check_message_need_send: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},result:ok')
        return {'result':'ok', 'wechat_chatbot': wechat_chatbot}
    logging.info(f'skip other user msg: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id}, type:{type}')
    return {'result':'error', 'msg':''}

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

def wechat_user_key(app_id, username):
    return f"lc_service:{service}:wechat_user:{app_id}:{username}"

def im_user_key(app_id, user_id):
    return f"lc_service:{service}:im_user:{app_id}:{user_id}"

def message_deduplication_key(from_user, to_user, msg_id, new_msg_id):
    return f"lc_service:{service}:message_deduplication:{from_user}:{to_user}:{msg_id}:{new_msg_id}"

def get_wechat_username(app_id, user_id):
    redis = lanying_redis.get_redis_connection()
    im_key = im_user_key(app_id, user_id)
    result = redis.get(im_key)
    if result:
        return str(result,'utf-8')
    logging.info(f"get_wechat_username | not found, app_id:{app_id}, user_id:{user_id}")
    return None

def send_wechat_message(config, app_id, message, to_username, w_id):
    url =  lanying_wechat_chatbot.get_api_server() + "/sendText"
    headers = lanying_wechat_chatbot.get_headers(app_id)
    content = message['content']
    content_list = split_string_by_size(content, wechat_max_message_size)
    for now_content in content_list:
        data = {
            "wId": w_id,
            "wcId": to_username,
            "content": now_content
        }
        logging.info(f"wechat_chatbot send_wechat_message start | app_id:{app_id}, to_username:{to_username}, content:{now_content}")
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
        result = response.json()
        logging.info(f"wechat_chatbot send_wechat_message finish| app_id:{app_id}, to_username:{to_username}, content:{now_content}, result:{result}")

def split_string_by_size(input_string, chunk_size):
    return [input_string[i:i+chunk_size] for i in range(0, len(input_string), chunk_size)]

def check_access_token_valid():
    headerToken = request.headers.get('access-token', "")
    accessToken = os.getenv('LANYING_CONNECTOR_ACCESS_TOKEN')
    if accessToken and accessToken == headerToken:
        return True
    else:
        return False
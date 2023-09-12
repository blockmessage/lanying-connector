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
import os
import time

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
    start_time = time.time()
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
                msg_id = int(xml.find('MsgId').text)
                verify_type = config.get('type', 'verified')
                logging.info(f"got wechat message | app_id:{app_id}, from_user_name:{from_user_name},to_user_name:{to_user_name},create_time:{create_time},msg_type:{msg_type},content:{content},msg_id:{msg_id}, verify_type:{verify_type}")
                user_id = get_or_register_user(app_id, from_user_name)
                if user_id:
                    if verify_type == 'unverified':
                        reply_expire_time = start_time + int(os.getenv("WECHAT_OFFICIAL_ACCOUNT_REPLY_EXPIRE_TIME", "3"))
                        last_msg_id_key = f"wechat_official_account:last_msg_id:{user_id}"
                        key = subscribe_key(user_id, msg_id)
                        redis = lanying_redis.get_redis_connection()
                        keys = [key]
                        if redis.exists(*keys) == 0:
                            redis.hincrby(key, "retry_count", 1)
                            redis.expire(key, 600)
                            last_msg_id_str = lanying_redis.redis_get(redis, last_msg_id_key)
                            if content == "1" and last_msg_id_str:
                                redis.hset(key, 'watch_msg_id', last_msg_id_str)
                                key = subscribe_key(user_id, int(last_msg_id_str))
                                lock_value = redis.hincrby(key, 'lock', 1)
                                reply = wait_reply_msg(key, reply_expire_time, False, lock_value)
                            else:
                                redis.set(last_msg_id_key, msg_id)
                                ext = {
                                    'ai':{
                                        'feedback':{
                                            'wechat_msg_id':msg_id
                                        },
                                        'force_stream': True
                                    }
                                }
                                lanying_message.send_message_async(config, app_id, user_id, config['lanying_user_id'],content, ext)
                                lock_value = redis.hincrby(key, 'lock', 1)
                                reply = wait_reply_msg(key, reply_expire_time, False, lock_value)
                        else:
                            retry_count = redis.hincrby(key, "retry_count", 1)
                            redis.expire(key, 600)
                            watch_msg_id_str = lanying_redis.redis_hget(redis, key, 'watch_msg_id')
                            if watch_msg_id_str:
                                key = subscribe_key(user_id, int(watch_msg_id_str))
                            lock_value = redis.hincrby(key, 'lock', 1)
                            reply = wait_reply_msg(key, reply_expire_time, retry_count >=3, lock_value)
                        if len(reply) > 0:
                            reply = f"""<xml>
                                    <ToUserName><![CDATA[{from_user_name}]]></ToUserName>
                                    <FromUserName><![CDATA[{to_user_name}]]></FromUserName>
                                    <CreateTime>{int(time.time())}</CreateTime>
                                    <MsgType><![CDATA[text]]></MsgType>
                                    <Content><![CDATA[{reply}]]></Content>
                                    </xml>
                                    """
                    else:
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

def subscribe_key(user_id, msg_id):
    return f"wechat_official_account:subscribe:{user_id}:{msg_id}"

def wait_reply_msg(key, expire_time, is_last, lock_value):
    redis = lanying_redis.get_redis_connection()
    now = time.time()
    info = {}
    tip = '...（消息超长，回复1继续接收）'
    while now < expire_time:
        info = lanying_redis.redis_hgetall(redis, key)
        if int(info.get('lock', '0')) != lock_value:
            break
        now = time.time()
        if now > expire_time + 0.2:
            break
        message = info.get('message', '')
        start = int(info.get('start', '0'))
        if int(info.get('finish', '0')) > 0:
            message_len = len(message)
            if message_len > start + official_account_max_message_size:
                send_len = official_account_max_message_size - len(tip)
                redis.hset(key, 'start', start+send_len)
                reply = message[start:start+send_len] + tip
                logging.info(f"reply wechat finish part message | {reply}")
                return reply
            elif message_len > start:
                redis.hset(key, 'start', message_len)
                reply = message[start:message_len]
                logging.info(f"reply wechat finish last part message | {reply}")
                return reply
            else:
                reply = '没有更多消息'
                logging.info(f"reply wechat nomore message | {reply}")
                return reply
        else:
            message_len = len(message)
            if message_len > start + official_account_max_message_size:
                send_len = official_account_max_message_size - len(tip)
                redis.hset(key, 'start', start+send_len)
                reply = message[start:start+send_len] + tip
                logging.info(f"reply wechat unfinish part message | {reply}")
                return reply
        now = time.time()
        time.sleep(min(max(0,expire_time-now), 500))
        now = time.time()
    if is_last:
        message = info.get('message', '')
        message_len = len(message)
        start = int(info.get('start', '0'))
        send_len = min(official_account_max_message_size - len(tip), max(0, message_len - start))
        redis.hset(key, 'start', start+send_len)
        reply = message[start:start+send_len] + tip
        logging.info(f"reply wechat getmore message | {reply}")
        return reply
    else:
        time.sleep(6)
        return 'wait'

def handle_chat_message(config, message):
    checkres = check_message_need_send(config, message)
    if checkres['result'] == 'error':
        return ''
    msg_type = checkres['msg_type']
    json_ext = checkres['json_ext']
    verify_type = checkres['verify_type']
    app_id = message['appId']
    to_user_id = message['to']['uid']
    logging.info(f"{service} | handle_chat_message do for user_id, app_id={app_id}, to_user_id:{to_user_id}, verify_type:{verify_type}")
    if verify_type == 'unverified':
        redis = lanying_redis.get_redis_connection()
        wechat_msg_id = 0
        try:
            wechat_msg_id = int(json_ext['ai']['feedback']['wechat_msg_id'])
        except Exception as e:
            pass
        if wechat_msg_id > 0:
            key = subscribe_key(to_user_id, wechat_msg_id)
            sub_info = lanying_redis.redis_hgetall(redis, key)
            ai_info = json_ext.get('ai',{})
            old_seq = int(sub_info.get('seq', '0'))
            seq = int(ai_info.get('seq', '0'))
            if seq > old_seq:
                message_content = sub_info.get('message','')
                if msg_type == 'CHAT' or msg_type == 'APPEND':
                    message_content += message['content']
                elif msg_type == 'REPLACE':
                    message_content = message['content']
                    message_antispam = lanying_config.get_message_antispam(app_id)
                    if message_content == message_antispam:
                        redis.hset(key, "start", 0)
                redis.hset(key, "message", message_content)
                if ai_info.get('finish', False) == True:
                    redis.hset(key, 'finish', 1)
    else:
        wechat_username = get_wechat_username(app_id, to_user_id)
        if wechat_username:
            send_wechat_message(config, app_id, message, wechat_username)
    return ''

def check_message_need_send(config, message):
    from_user_id = int(message['from']['uid'])
    to_user_id = int(message['to']['uid'])
    type = message['type']
    my_user_id = config['lanying_user_id']
    verify_type = config.get('type', 'verified')
    if my_user_id != None and from_user_id == my_user_id and to_user_id != my_user_id and (type == 'CHAT' or type == 'REPLACE' or type == 'APPEND'):
        ext = message.get('ext', '')
        try:
            json_ext = json.loads(ext)
        except Exception as e:
            json_ext = {}
        try:
            is_stream = (json_ext['ai']['stream'] == True)
        except Exception as e:
            is_stream = False
        if is_stream:
            if verify_type == 'unverified':
                pass
            else:
                if type == 'REPLACE':
                    pass
                else:
                    logging.info(f"skip chat and stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                    return {'result':'error', 'msg':''}
        else:
            if type == 'REPLACE' or type == 'APPEND':
                logging.info(f"skip EDIT and not stream msg:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},ext:{json_ext}")
                return {'result':'error', 'msg':''}
        logging.info(f'check_message_need_send: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id},type:{type},result:ok')
        return {'result':'ok', "is_stream": is_stream, "verify_type":verify_type, "json_ext":json_ext, "msg_type": type}
    logging.info(f'skip other user msg: lanying_user_id:{my_user_id},from_user_id:{from_user_id},to_user_id:{to_user_id}, type:{type}')
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
        server, headers = get_proxy_info()
        url = f"{server}/cgi-bin/message/custom/send?access_token={access_token}"
        response = requests.post(url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'), headers=headers)
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
    server, headers = get_proxy_info()
    url = f"{server}/cgi-bin/token?grant_type=client_credential&appid={wechat_app_id}&secret={wechat_app_secret}"
    response = requests.get(url, headers=headers)
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

def get_proxy_info():
    proxy_server = os.getenv("LANYING_CONNECTOR_WECHAT_PROXY_SERVER", '')
    if len(proxy_server) > 0:
        proxy_key = os.getenv("LANYING_CONNECTOR_WECHAT_PROXY_KEY", '')
        headers = {
            "Authorization": f"Basic {proxy_key}",
        }
        return (proxy_server, headers)
    else:
        server = "https://api.weixin.qq.com"
        headers = {}
        return (server, headers)

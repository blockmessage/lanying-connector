import os
import requests
import time
import lanying_redis
import json
import lanying_chatbot
import logging

def get_authorization(app_id):
    return os.getenv('WECHAT_CHATBOT_AUTHORIZATION', '')

def get_api_server():
    return os.getenv("WECHAT_CHATBOT_API_SERVER", 'https://abc.com')

def get_headers(app_id):
    return {
        'Content-Type': 'application/json',
        'Authorization': get_authorization(app_id)
    }

def login(app_id, type, wechat_chatbot_id, proxy, ttuid):
    wc_id = ''
    if wechat_chatbot_id != "":
        wechat_chatbot_info = get_wechat_chatbot(app_id, wechat_chatbot_id)
        if wechat_chatbot_info is None:
            return {'result':'error', 'message': 'wechat_chatbot_id not found'}
        wc_id = wechat_chatbot_info['wc_id']
    if type not in ["ttuid", "proxy"]:
        return {'result':'error', 'message': 'bad type'}
    if type == 'ttuid':
        if len(ttuid) > 0:
            url = get_api_server() + "/localIPadLogin"
            headers = get_headers(app_id)
            body = {
                'wcId': wc_id,
                'ttuid': ttuid
            }
            response = requests.post(url, headers=headers, json=body)
            logging.info(f"wechat_chatbot login result: body={body}, response: {response.text}")
            return handle_login_response(app_id, wechat_chatbot_id, response)
        else:
            return {'result':'error', 'message': 'bad ttuid'}
    elif type == 'proxy':
        if proxy in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,20]:
            url = get_api_server() + "/iPadLogin"
            headers = get_headers(app_id)
            body = {
                'wcId': wc_id,
                'proxy': proxy
            }
            response = requests.post(url, headers=headers, json=body)
            logging.info(f"wechat_chatbot login result: body={body}, response: {response.text}")
            return handle_login_response(app_id, wechat_chatbot_id, response)
        else:
            return {'result': 'error', 'message': 'bad proxy id '}

def handle_login_response(app_id, wechat_chatbot_id, response):
    result = response.json()
    if result["code"] == "1000":
        w_id = result["data"]["wId"]
        qr_code_url = result["data"]["qrCodeUrl"]
        create_wid_info(app_id, w_id, wechat_chatbot_id, qr_code_url)
        return {'result': "ok",
                "data": {
                    "qr_code_url": qr_code_url,
                    "w_id": w_id
                }}
    else:
        return {'result': 'error', 'message': result['message']}

def create_wid_info(app_id, w_id, wechat_chatbot_id, qr_code_url):
    now = int(time.time())
    redis = lanying_redis.get_redis_connection()
    key = get_wid_info_key(app_id, w_id)
    redis.hmset(key, {
        'app_id': app_id,
        'wechat_chatbot_id': wechat_chatbot_id,
        'qr_code_url': qr_code_url,
        'create_time': now
    })
    redis.expire(key, 86400 * 7)
    redis.hset(get_wid_info_ids_key(), w_id, now)
    redis.hset(get_wid_info_ids_app_key(app_id), w_id, now)

def delete_wid_info(app_id, w_id):
    info_key = get_wid_info_key(app_id, w_id)
    redis = lanying_redis.get_redis_connection()
    redis.delete(info_key)
    redis.hdel(get_wid_info_ids_key(), w_id)
    redis.hdel(get_wid_info_ids_app_key(app_id), w_id)

def set_wid_info_field(app_id, w_id, field, value):
    redis = lanying_redis.get_redis_connection()
    key = get_wid_info_key(app_id, w_id)
    redis.hset(key, field, value)
    redis.expire(key, 86400*7)

def get_wid_info(app_id, w_id):
    redis = lanying_redis.get_redis_connection()
    key = get_wid_info_key(app_id, w_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if 'create_time' in info:
        return info
    return None
    
def get_login_info(app_id, w_id):
    wid_info = get_wid_info(app_id, w_id)
    if wid_info is None:
        return {'result':'error', 'message': 'bad login info'}
    if 'result' in wid_info:
        return {"result": "ok", "data":{"wc_id": wid_info['wc_id'], "w_account": wid_info['w_account']}}
    url = get_api_server() + "/getIPadLoginInfo"
    headers = get_headers(app_id)
    body = {
        'wId': w_id
    }
    response = requests.post(url, headers=headers, json=body,timeout=(20.0, 260.0))
    logging.info(f"wechat_chatbot get_login_info result: body={body}, response: {response.text}")
    result = response.json()
    if result["code"] == "1000":
        wc_id = result["data"]["wcId"]
        w_account = result["data"]["wAccount"]
        wid_info_result = json.dumps(result["data"], ensure_ascii=False)
        set_wid_info_field(app_id, w_id, "wc_id", wc_id)
        set_wid_info_field(app_id, w_id, "w_account", w_account)
        set_wid_info_field(app_id, w_id, "result", wid_info_result)
        return {'result': "ok", "data":{"wc_id":wc_id, "w_account": w_account}}
    else:
        return {'result': 'error', 'message': result['message']}

def create_wechat_chatbot(app_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note):
    now = int(time.time())
    wid_info = get_wid_info(app_id, w_id)
    if wid_info is None:
        return {'result':'error', 'message': 'bad login info'}
    if wid_info["wechat_chatbot_id"] != "":
        return {'result':'error', 'message': 'bad login info'}
    if 'result' not in wid_info:
        return {'result':'error', 'message': 'bad login info'}
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {'result': 'error', 'message': 'bad bind chatbot id'}
    if chatbot_info['wechat_chatbot_id'] != '':
        return {'result': 'error', 'message': 'chatbot id is already bind'}
    wid_info_result = json.loads(wid_info['result'])
    wc_id = wid_info_result["wcId"]
    w_account = wid_info_result["wAccount"]
    wechat_chatbot_id = generate_chatbot_id()
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_chatbot_key(app_id, wechat_chatbot_id), {
        "chatbot_id": chatbot_id,
        "wechat_chatbot_id": wechat_chatbot_id,
        "create_time": now,
        "app_id": app_id,
        "msg_types": json.dumps(msg_types, ensure_ascii=False),
        "non_friend_chat_mode": non_friend_chat_mode,
        "note": note,
        "wc_id": wc_id,
        "w_id": w_id,
        "w_account": w_account,
        "wid_info_result": json.dumps(wid_info_result, ensure_ascii=False),
        "status": "normal"
    })
    redis.rpush(get_chatbot_ids_key(app_id), wechat_chatbot_id)
    update_wc_id_info(wc_id, app_id, wechat_chatbot_id)
    lanying_chatbot.set_chatbot_field(app_id, chatbot_id, "wechat_chatbot_id", wechat_chatbot_id)
    delete_wid_info(app_id, w_id)
    return {'result':'ok', 'data':{'wechat_chatbot_id':wechat_chatbot_id}}

def configure_wechat_chatbot(app_id, wechat_chatbot_id, w_id, chatbot_id, msg_types, non_friend_chat_mode, note):
    wid_info = get_wid_info(app_id, w_id)
    if wid_info:
        if wid_info["wechat_chatbot_id"] != wechat_chatbot_id:
            return {'result':'error', 'message': 'bad login info'}
        if 'result' not in wid_info:
            return {'result':'error', 'message': 'bad login info'}
    wechat_chatbot = get_wechat_chatbot(app_id, wechat_chatbot_id)
    if wechat_chatbot is None:
        return {'result':'error', 'message': 'wechat_chatbot not exist'}
    redis = lanying_redis.get_redis_connection()
    if wid_info:
        wid_info_result = json.loads(wid_info['result'])
        wc_id = wid_info_result["wcId"]
        w_account = wid_info_result["wAccount"]
        redis.hmset(get_chatbot_key(app_id, wechat_chatbot_id), {
            "chatbot_id": chatbot_id,
            "msg_types": json.dumps(msg_types, ensure_ascii=False),
            "non_friend_chat_mode": non_friend_chat_mode,
            "note": note,
            "wc_id": wc_id,
            "w_id": w_id,
            "w_account": w_account,
            "wid_info_result": json.dumps(wid_info_result, ensure_ascii=False),
            "status": "normal"
        })
        update_wc_id_info(wc_id, app_id, wechat_chatbot_id)
        delete_wid_info(app_id, w_id)
    else:
        redis.hmset(get_chatbot_key(app_id, wechat_chatbot_id), {
            "chatbot_id": chatbot_id,
            "msg_types": json.dumps(msg_types, ensure_ascii=False),
            "non_friend_chat_mode": non_friend_chat_mode,
            "note": note
        })
    if wechat_chatbot['chatbot_id'] != chatbot_id:
        lanying_chatbot.set_chatbot_field(app_id, wechat_chatbot['chatbot_id'], "wechat_chatbot_id", '')
        lanying_chatbot.set_chatbot_field(app_id, chatbot_id, "wechat_chatbot_id", wechat_chatbot_id)
    return {'result':'ok', 'data':{'success': True}}

def list_wechat_chatbots(app_id):
    list_key = get_chatbot_ids_key(app_id)
    redis = lanying_redis.get_redis_connection()
    wechat_chatbot_ids = lanying_redis.redis_lrange(redis, list_key, 0, -1)
    dtos = []
    for wechat_chatbot_id in wechat_chatbot_ids:
        info = get_wechat_chatbot(app_id, wechat_chatbot_id)
        if info:
            dto = {}
            for key,value in info.items():
                if key in ["chatbot_id", "wechat_chatbot_id", "non_friend_chat_mode", "note", "wc_id", "w_account", "status", "create_time", "msg_types"]:
                    dto[key] = value
            dtos.append(dto)
    return dtos

def change_status(app_id, wechat_chatbot_id, status):
    wechat_chatbot = get_wechat_chatbot(app_id, wechat_chatbot_id)
    if status not in ["normal", "disabled"]:
        return {'result':'error', 'message': 'status only support normal or disabled'}
    if wechat_chatbot is None:
        return {'result':'error', 'message': 'wechat_chatbot not exist'}
    update_wechat_chatbot_field(app_id, wechat_chatbot_id, "status", status)
    return {'result':'ok', 'data':{'success': True}}

def update_wechat_chatbot_field(app_id, wechat_chatbot_id, field, value):
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_chatbot_key(app_id, wechat_chatbot_id), field, value)

def get_wechat_chatbot(app_id, wechat_chatbot_id):
    key = get_chatbot_key(app_id, wechat_chatbot_id)
    redis = lanying_redis.get_redis_connection()
    result = lanying_redis.redis_hgetall(redis, key)
    if 'create_time' in result:
        dto = {}
        for k,v in result.items():
            if k in ["create_time"]:
                dto[k] = int(v)
            elif k in ["msg_types"]:
                dto[k] = json.loads(v)
            else:
                dto[k] = v
        return dto
    else:
        return None

def update_wc_id_info(wc_id, app_id, wechat_chatbot_id):
    key = get_wc_id_info_key(wc_id)
    redis = lanying_redis.get_redis_connection()
    redis.hmset(key, {
        "app_id": app_id,
        "wechat_chatbot_id": wechat_chatbot_id
    })

def get_wc_id_info(wc_id):
    key = get_wc_id_info_key(wc_id)
    redis = lanying_redis.get_redis_connection()
    info = lanying_redis.redis_hgetall(redis, key)
    if 'app_id' in info:
        return info
    return None

def delete_wc_id_info(wc_id):
    key = get_wc_id_info_key(wc_id)
    redis = lanying_redis.get_redis_connection()
    redis.delete(key)

def clean_old_login(wechat_chatbot_info):
    pass # TODO

def generate_chatbot_id():
    redis = lanying_redis.get_redis_connection()
    return str(redis.incrby("lanying_connector:wechat:chatbot_id_generator", 1))

def get_chatbot_key(app_id, chatbot_id):
    return f"lanying_connector:wechat:chatbot:{app_id}:{chatbot_id}"

def get_chatbot_ids_key(app_id):
    return f"lanying_connector:wechat:chatbot_ids:{app_id}"

def get_wid_info_key(app_id, w_id):
    return f"lanying_connector:wechat:w_id_info:{app_id}:{w_id}"

def get_wid_info_ids_key():
    return "lanying_connector:wechat:w_id_info_ids"

def get_wid_info_ids_app_key(app_id):
    return f"lanying_connector:wechat:w_id_info_ids_by_app:{app_id}"

def get_wc_id_info_key(wc_id):
    return f"lanying_connector:wechat:wc_id_info:{wc_id}"

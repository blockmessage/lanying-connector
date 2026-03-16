import time
import lanying_redis
import logging
import secrets
import os
import requests
import lanying_config
import lanying_chatbot
import lanying_im_api
import lanying_utils
import json

class NodeSetting:
    def __init__(self, app_id, name, product_id, charge_id, node_id, lanying_link):
        self.app_id = app_id
        self.name = name
        self.product_id = product_id
        self.charge_id = charge_id
        self.node_id = node_id
        self.lanying_link = lanying_link

    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'name': self.name,
            'product_id': self.product_id,
            'charge_id': self.charge_id,
            'node_id': self.node_id,
            'lanying_link': self.lanying_link
        }

def check_create_node(app_id):
    now = int(time.time())
    node_id = generate_node_id()
    register_result = register_node_im_user(app_id, node_id)
    if register_result['result'] == 'error':
        return register_result
    username = register_result['data']['username']
    password = register_result['data']['password']
    user_id = register_result['data']['user_id']
    redis = lanying_redis.get_redis_connection()
    key = get_node_prepare_key(app_id, node_id)
    fields = {
        'username': username,
        'password': password,
        'user_id': user_id,
        'create_time': now
    }
    redis.hmset(key, fields)
    redis.expire(key, 120)
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'user_id': user_id
        }
    }

def create_node(node_setting: NodeSetting):
    now = int(time.time())
    app_id = node_setting.app_id
    node_id = node_setting.node_id
    name = node_setting.name
    node_prepare = get_node_prepare(app_id, node_id)
    if node_prepare is None:
        return {
            'result': 'error',
            'message': 'must prepare first'
        }
    delete_node_prepare(app_id, node_id)
    old_node_info = get_node(app_id, node_id)
    if old_node_info is not None:
        return {
            'result': 'error',
            'message': 'old node exist'
        }
    username = node_prepare['username']
    password = node_prepare['password']
    user_id = node_prepare['user_id']
    redis = lanying_redis.get_redis_connection()
    fields = node_setting.to_hmset_fields()
    fields['status'] = 'wait'
    fields['create_time'] = now
    fields['node_id'] = node_id
    fields['username'] = username
    fields['password'] = password
    fields['user_id'] = user_id
    if len(name) == 0:
        fields['name'] = f'OpenClaw-{node_id}'
    token = secrets.token_hex(32)
    fields['token'] = token
    update_token_info(token, app_id, node_id, 'normal')
    logging.info(f"create openclaw node start | app_id:{app_id}, node_info:{fields}")
    redis.hmset(get_node_key(app_id, node_id), fields)
    redis.rpush(get_node_list_key(app_id), node_id)
    node_info = get_node(app_id, node_id)
    return {
        'result': 'ok',
        'data': node_info
    }

def check_node(app_id, node_id):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    return {
        'result': 'ok',
        'data': {
            'node_info': node_info
        }
    }

def get_node_list(app_id):
    redis = lanying_redis.get_redis_connection()
    node_ids = list(reversed(lanying_redis.redis_lrange(redis, get_node_list_key(app_id), 0, -1)))
    node_info_list = []
    for node_id in node_ids:
        node_info = get_node(app_id, node_id)
        if node_info:
            node_info_list.append(node_info)
    return {
        'result': 'ok',
        'data': {
            'list': node_info_list
        }
    }

def delete_node(app_id, node_id):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    redis = lanying_redis.get_redis_connection()
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    redis.delete(get_node_key(app_id, node_id))
    redis.lrem(get_node_list_key(app_id), 1, node_id)
    if chatbot_id is not None:
        unbind_chatbot(app_id, node_id, chatbot_id)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def handle_chat_message(msg):
    from_user_id = msg['from']['uid']
    to_user_id = msg['to']['uid']
    app_id = msg['appId']
    if from_user_id == to_user_id:
        ext = lanying_utils.safe_json_loads(msg['ext'], {})
        if isinstance(ext, dict) and 'openclaw' in ext:
            event = ext['openclaw']
            handle_client_event(event, app_id, from_user_id)

def handle_client_event(event, app_id, user_id):
    logging.info(f"handle client event | event: {event}, app_id: {app_id}, user_id: {user_id}")
    if event['type'] == 'online':
        node_list = get_node_list(app_id)['data']['list']
        for node in node_list:
            if node['user_id'] == user_id:
                node_id = node['node_id']
                if node['status'] == 'wait':
                    logging.info(f"change node status to normal | node_id: {node_id}")
                    update_node_field(app_id, node_id, 'status', 'normal')
                    model_patch_config = get_model_patch_config(app_id)
                    update_node_config(app_id, node_id, model_patch_config)

def get_model_patch_config(app_id, model="openai/gpt-4o-mini"):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        token = config.get('access_token', '')
        if len(token) > 0:
            return {
                "models": {
                    "providers": {
                        "lanying": {
                            "baseUrl": "https://connector.lanyingim.com/v1",
                            "apiKey": token,
                            "api": "openai-completions",
                            "models": [
                                {
                                    "id": model,
                                    "name": model,
                                    "reasoning": False,
                                    "input": ["text"],
                                    "contextWindow": 128000,
                                    "maxTokens": 8192
                                }
                            ]
                        }
                    }
                },
                "agents": {
                    "defaults":{
                        "model": {
                            "primary": f"lanying/{model}"
                        }
                    }
                }
            }
    return None

def update_node_config(app_id, node_id, patch_config):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    user_id = node_info['user_id']
    content = '开始同步模型配置'
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 0
    extra = {
        'ext': {
            'openclaw': {
                    'type': 'config_patch',
                    'raw': json.dumps(patch_config)
                },
        },
        'skip_antispam_prompt': True
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, user_id, user_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        return {
            'result': 'error',
            'message': 'send message failed'
        }
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_id
        }
    }

def register_node_im_user(app_id, node_id):
    username = f'openclaw_{node_id}_{secrets.token_hex(4)}'
    password = secrets.token_hex(32)
    result = lanying_im_api.register(app_id, username, password)
    if result.get('code') == 200:
        return {
            'result': 'ok',
            'data': {
                'username': username,
                'password': password,
                'user_id': str(result.get('data').get('user_id'))
            }
        }
    return {
        'result': 'error',
        'message': 'register user failed'
    }

def update_node_field(app_id, node_id, field, value):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    redis = lanying_redis.get_redis_connection()
    redis.hset(get_node_key(app_id, node_id), field, value)
    return {
        'result': 'ok'
    }

def get_node(app_id, node_id):
    redis = lanying_redis.get_redis_connection()
    key = get_node_key(app_id, node_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time']:
                dto[key] = int(value)
            else:
                dto[key] = value
        if 'wechat_chatbot_id' not in dto:
            dto['wechat_chatbot_id'] = ''
        return dto
    return None

def get_node_prepare(app_id, node_id):
    redis = lanying_redis.get_redis_connection()
    key = get_node_prepare_key(app_id, node_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time']:
                dto[key] = int(value)
            else:
                dto[key] = value
        return dto
    return None

def delete_node_prepare(app_id, node_id):
    redis = lanying_redis.get_redis_connection()
    key = get_node_prepare_key(app_id, node_id)
    redis.delete(key)

def bind_chatbot(app_id, node_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    old_node_id = get_chatbot_node_id(app_id, chatbot_id)
    old_chatbot_id = get_node_chatbot_id(app_id, node_id)
    redis.hset(get_node_chatbot_bind_key(app_id), node_id, chatbot_id)
    redis.hset(get_chatbot_node_bind_key(app_id), chatbot_id, node_id)
    if old_node_id is not None and old_node_id != node_id:
        redis.hdel(get_node_chatbot_bind_key(app_id), old_node_id)
    if old_chatbot_id is not None and old_chatbot_id != chatbot_id:
        redis.hdel(get_chatbot_node_bind_key(app_id), old_chatbot_id)

def unbind_chatbot(app_id, node_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    redis.hdel(get_node_chatbot_bind_key(app_id), node_id)
    redis.hdel(get_chatbot_node_bind_key(app_id), chatbot_id)

def check_client_login(token):
    token_info = get_token_info(token)
    if token_info is None:
        return {
            'result': 'error',
            'message': 'token not exist'
        }
    if token_info['status'] != 'normal':
        return {
            'result': 'error',
            'message': 'bad token status'
        }
    app_id = token_info['app_id']
    node_id = token_info['node_id']
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    if node_info['status'] == 'wait':
        update_node_field(app_id, node_id, 'status', 'normal')
    return {
        'result': 'ok',
        'data': {
            'app_id': app_id,
            'node_id': node_id
        }
    }

def send_lanying_message(token, message):
    token_info = get_token_info(token)
    if token_info is None:
        return {
            'result': 'error',
            'message': 'token not exist'
        }
    if token_info['status'] != 'normal':
        return {
            'result': 'error',
            'message': 'bad token status'
        }
    app_id = token_info['app_id']
    node_id = token_info['node_id']
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None:
        return {
            'result': 'error',
            'message': 'chatbot not bind'
        }
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {
            'result': 'error',
            'message': 'chatbot not found'
        }
    if message['chatType'] != "direct":
        return {
            'result': 'error',
            'message': 'chatType not support'
        }
    if message['contentType'] != "text":
        return {
            'result': 'error',
            'message': 'contentType not support'
        }
    to_user_id = message['to']
    content = message['content']
    chatbot_user_id = chatbot_info['user_id']
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 0
    extra = {
        'msg_config': {
            'ai': {
                    'role': 'ai'
                }
        }
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, chatbot_user_id, to_user_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        return {
            'result': 'error',
            'message': 'send message failed'
        }
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_id
        }
    }

def get_token_info(token):
    redis = lanying_redis.get_redis_connection()
    key = get_token_key(token)
    info = lanying_redis.redis_hgetall(redis, key)
    if "node_id" in info:
        return info
    return None

def get_chatbot_node_info(app_id, chatbot_id):
    node_id = get_chatbot_node_id(app_id, chatbot_id)
    if node_id is None:
        return None
    return get_node(app_id, node_id)

def get_chatbot_node_id(app_id, chatbot_id):
    redis = lanying_redis.get_redis_connection()
    key = get_chatbot_node_bind_key(app_id)
    return lanying_redis.redis_hget(redis, key, chatbot_id)

def get_node_chatbot_id(app_id, node_id):
    redis = lanying_redis.get_redis_connection()
    key = get_node_chatbot_bind_key(app_id)
    return lanying_redis.redis_hget(redis, key, node_id)

def update_token_info(token, app_id, node_id, status):
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_token_key(token), {
        'app_id': app_id,
        'node_id': node_id,
        'status': status
    })

def get_access_token():
    return os.getenv('OPENCLAW_LANYING_AUTHORIZATION_TOKEN')

def get_openclaw_server():
    return os.getenv('OPENCLAW_LANYING_SERVER')

def redirect_to_openclaw(node_info, message):
    if node_info['status'] != 'normal':
        return 'OpenClaw状态异常'
    else:
        url = get_openclaw_server() + "/api/message"
        headers = {
            'authorization': get_access_token()
        }
        body = {
            'token': node_info['token'],
            'message': message
        }
        try:
            response = requests.post(url, headers= headers, json = body)
            response_json = response.json()
            logging.info(f"redirect_to_openclaw response:{response_json}")
            if response_json.get('code') == 200:
                return ''
            else:
                return f'转发到OpenClaw失败: {response_json.get("message")}'
        except Exception:
            logging.exception("redirect_to_openclaw error")
        return '转发到OpenClaw失败'

def generate_node_id():
    redis = lanying_redis.get_redis_connection()
    return str(redis.incrby("lanying_connector:openclaw:node_id_generator", 1))

def get_node_key(app_id, node_id):
    return f"lanying_connector:openclaw:node:{app_id}:{node_id}"

def get_node_prepare_key(app_id, node_id):
    return f"lanying_connector:openclaw:node_prepare:{app_id}:{node_id}"

def get_node_list_key(app_id):
    return f"lanying_connector:openclaw:node_list:{app_id}"

def get_node_chatbot_bind_key(app_id):
    return f"lanying_connector:openclaw:node_bind:{app_id}"

def get_chatbot_node_bind_key(app_id):
    return f"lanying_connector:openclaw:chatbot_bind:{app_id}"

def get_token_key(token):
    return f"lanying_connector:openclaw:token:{token}"

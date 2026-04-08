import time
import lanying_redis
import logging
import secrets
import os
import lanying_config
import lanying_chatbot
import lanying_im_api
import lanying_utils
import json
import lanying_vendor
from lanying_async import executor

class NodeSetting:
    def __init__(self, app_id, name, product_id, charge_id, node_id, lanying_link, access_type, access_list, chatbot_id):
        self.app_id = app_id
        self.name = name
        self.product_id = product_id
        self.charge_id = charge_id
        self.node_id = node_id
        self.lanying_link = lanying_link
        self.access_type = access_type
        self.access_list = access_list
        self.chatbot_id = chatbot_id

    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'name': self.name,
            'product_id': self.product_id,
            'charge_id': self.charge_id,
            'node_id': self.node_id,
            'lanying_link': self.lanying_link,
            'access_type': self.access_type,
            'access_list': self.access_list,
            'chatbot_id': self.chatbot_id
        }

class ConfigureNodeParam:
    def __init__(self, name, lanying_link, access_type, access_list, chatbot_id):
        self.name = name
        self.lanying_link = lanying_link
        self.access_type = access_type
        self.access_list = access_list
        self.chatbot_id = chatbot_id

    def to_hmset_fields(self):
        return {
            'name': self.name,
            'lanying_link': self.lanying_link,
            'access_type': self.access_type,
            'access_list': self.access_list,
            'chatbot_id': self.chatbot_id
        }

def extract_system_prompt_text_from_preset(preset):
    if not isinstance(preset, dict):
        return ''
    messages = preset.get('messages', [])
    if not isinstance(messages, list):
        return ''
    system_contents = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if str(message.get('role', '')) != 'system':
            continue
        content = message.get('content')
        if isinstance(content, str):
            system_contents.append(content)
    return '\n\n'.join(system_contents)

def sync_bound_chatbot_preset_prompt(app_id, node_id, chatbot_id):
    try:
        node_info = get_node(app_id, node_id)
        if node_info is None:
            logging.info(f"sync_bound_chatbot_preset_prompt skip for missing node | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}")
            return
        chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot_info is None:
            logging.info(f"sync_bound_chatbot_preset_prompt skip for missing chatbot | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}")
            return
        prompt = extract_system_prompt_text_from_preset(chatbot_info.get('preset', {}))
        chatbot_name = str(chatbot_info.get('name', ''))
        sync_result = sync_chatbot_preset_prompt(node_info, chatbot_id, chatbot_name, prompt)
        logging.info(f"sync_bound_chatbot_preset_prompt result | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}, result:{sync_result}")
    except Exception as e:
        logging.exception(e)

def clear_bound_chatbot_preset_prompt(app_id, node_id, chatbot_id):
    try:
        node_info = get_node(app_id, node_id)
        if node_info is None:
            logging.info(f"clear_bound_chatbot_preset_prompt skip for missing node | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}")
            return
        chatbot_name = ''
        chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot_info is not None:
            chatbot_name = str(chatbot_info.get('name', ''))
        sync_result = sync_chatbot_preset_prompt(node_info, chatbot_id, chatbot_name, '')
        logging.info(f"clear_bound_chatbot_preset_prompt result | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}, result:{sync_result}")
    except Exception as e:
        logging.exception(e)

def maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id):
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None or chatbot_id == '':
        logging.info(f"maybe_sync_node_bound_chatbot_preset_prompt skip for no bind | app_id:{app_id}, node_id:{node_id}")
        return
    executor.submit(sync_bound_chatbot_preset_prompt, app_id, node_id, chatbot_id)

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
    access_type = node_setting.access_type
    if access_type not in ['public', 'friend']:
        return {
            'result': 'error',
            'message': 'bad access_type value'
        }
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
    async_init_node_im_user_setting(app_id, None, node_info)
    return {
        'result': 'ok',
        'data': node_info
    }

def configure_node(app_id, node_id, param: ConfigureNodeParam):
    logging.info(f"configure_node | app_id: {app_id}, node_id: {node_id}, param: {param.to_hmset_fields()}")
    old_node_info = get_node(app_id, node_id)
    if old_node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    old_bind_chatbot_id = get_node_chatbot_id(app_id, node_id)
    if old_bind_chatbot_id == '':
        old_bind_chatbot_id = None
    new_chatbot_id = param.chatbot_id
    if new_chatbot_id is None:
        new_chatbot_id = ''
    if new_chatbot_id != old_node_info['chatbot_id'] and new_chatbot_id != '':
        chatbot_info = lanying_chatbot.get_chatbot(app_id, new_chatbot_id)
        if chatbot_info is None:
            return {
                'result': 'error',
                'message': 'chatbot not exist'
            }
        conflict_node_id = get_chatbot_node_id(app_id, new_chatbot_id)
        if conflict_node_id is not None and conflict_node_id != node_id:
            return {
                'result': 'error',
                'message': 'chatbot already bind to another node'
            }
    old_bind_chatbot_id_str = old_bind_chatbot_id if old_bind_chatbot_id is not None else ''
    if new_chatbot_id != old_bind_chatbot_id_str:
        if old_bind_chatbot_id is not None:
            unbind_chatbot(app_id, node_id, old_bind_chatbot_id, clear_prompt=(new_chatbot_id == ''))
        if new_chatbot_id != '':
            bind_result = bind_chatbot(app_id, node_id, new_chatbot_id)
            if bind_result['result'] == 'error':
                return bind_result
    redis = lanying_redis.get_redis_connection()
    fields = param.to_hmset_fields()
    logging.info(f"configure openclaw node start | app_id:{app_id}, node_id: {node_id}, node_info:{fields}")
    redis.hmset(get_node_key(app_id, node_id), fields)
    node_info = get_node(app_id, node_id)
    async_init_node_im_user_setting(app_id, old_node_info, node_info)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
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
    ctype = msg['ctype']
    if from_user_id == to_user_id:
        ext = lanying_utils.safe_json_loads(msg['ext'], {})
        if isinstance(ext, dict) and 'openclaw' in ext:
            event = ext['openclaw']
            handle_client_event(event, app_id, from_user_id, ctype)

def handle_client_event(event, app_id, user_id, ctype):
    logging.info(f"handle client event | event: {event}, app_id: {app_id}, user_id: {user_id}, ctype: {ctype}")
    if event['type'] == 'online':
        node_list = get_node_list(app_id)['data']['list']
        for node in node_list:
            if node['user_id'] == user_id:
                node_id = node['node_id']
                if node['status'] == 'wait':
                    logging.info(f"change node status to normal | node_id: {node_id}")
                    update_node_field(app_id, node_id, 'status', 'normal')
                    model_patch_config = get_model_patch_config(app_id, node_id)
                    update_node_config(app_id, node_id, model_patch_config)
                    maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
                elif 'provider_inited' in event and event['provider_inited'] == False:
                    logging.info(f"update node config for provider_inited is false | node_id: {node_id}")
                    model_patch_config = get_model_patch_config(app_id, node_id)
                    update_node_config(app_id, node_id, model_patch_config)
                    maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
    elif event['type'] == 'router_reply':
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command router_reply | ctype: {ctype}, event: {event}")
            return
        node_list = get_node_list(app_id)['data']['list']
        for node in node_list:
            if node['user_id'] == user_id:
                node_id = node['node_id']
                logging.info(f"handle_client router_reply | node_id: {node_id}, event: {event}")
                if 'message' in event:
                    meta_message = event['message']
                    message = convert_from_meta_message(meta_message)
                    logging.info(f"convert_from_meta_message: meta_message{meta_message}, message: {message}")
                    router_reply_message(app_id, node, message)
                return

def router_reply_message(app_id, node_info, message):
    logging.info(f"router_reply_message start | node: {node_info}, message: {message}")
    node_id = node_info['node_id']
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None:
        return
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return
    chatbot_user_id = chatbot_info['user_id']
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 2 if message['type'] == 'GROUPCHAT' else 1
    content_type = 0
    content = message['content']
    to_id = message['to']['uid']
    if str(chatbot_user_id) == str(to_id):
        logging.info(f"router_reply_message stop for to is chatbot | chatbot_user_id: {chatbot_user_id}, to_id: {to_id}, message: {message}")
        return
    ext = {
        'ai': {
          'role': 'ai'
        }
    }
    extra = {
        'ext': ext
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, chatbot_user_id, to_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        logging.info(f"router_reply_message send message failed")

def sync_model_config(app_id, node_id):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    model_patch_config = get_model_patch_config(app_id, node_id)
    update_node_config(app_id, node_id, model_patch_config)
    maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }    

def get_model_patch_config(app_id, node_id=None, primary="openai/gpt-4o-mini", fallbacks=['volcengine/Doubao-1.5-pro-32k', 'volcengine/DeepSeek-R1']):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        token = config.get('access_token', '')
        if len(token) > 0:
            use_primary = primary
            use_fallbacks = list(fallbacks)
            if node_id is not None:
                chatbot_id = get_node_chatbot_id(app_id, node_id)
                if chatbot_id is not None and chatbot_id != '':
                    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
                    if chatbot_info and isinstance(chatbot_info.get('preset', {}), dict):
                        chatbot_model = str(chatbot_info['preset'].get('model', '')).strip()
                        if chatbot_model != '':
                            use_primary = chatbot_model
                            use_fallbacks = [model for model in use_fallbacks if str(model).strip() != chatbot_model]
            new_fallbacks = []
            for fallback in use_fallbacks:
                new_fallbacks.append(f"lanying/{fallback}")
            return {
                "models": {
                    "providers": {
                        "lanying": {
                            "baseUrl": "https://connector.lanyingim.com/v1",
                            "apiKey": token,
                            "api": "openai-completions",
                            "models": get_model_list(app_id)
                        }
                    }
                },
                "agents": {
                    "defaults":{
                        "model": {
                            "primary": f"lanying/{use_primary}",
                            "fallbacks": new_fallbacks
                        }
                    }
                }
            }
    return None

def get_model_list(app_id):
    all_models = lanying_vendor.list_models(app_id)
    models = []
    for model in all_models:
        if model.get('type') != 'chat':
            continue
        if model.get('token_limit', 0) < 16000:
            continue
        full_model_id = model['vendor'] + "/" + model['model']
        models.append({
            'id': full_model_id,
            'name': full_model_id,
            'reasoning': model.get('reasoning', False),
            "input": ["text"],
            "contextWindow": model['token_limit'],
            "maxTokens": model.get('max_output_tokens', 8192)
        })
    return models

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

def sync_chatbot_preset_prompt(node_info, chatbot_id, chatbot_name, prompt):
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    app_id = node_info['app_id']
    user_id = node_info['user_id']
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 6 # COMMAND = 6;
    content = ''
    ext = {
        'openclaw': {
            'type': 'preset_prompt_sync',
            'formatVersion': 1,
            'chatbotId': str(chatbot_id),
            'chatbotName': str(chatbot_name),
            'prompt': str(prompt)
        }
    }
    extra = {
        'ext': ext,
        'skip_antispam_prompt': True
    }
    logging.info(f"sync_chatbot_preset_prompt start | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, chatbot_id:{chatbot_id}, prompt_len:{len(str(prompt))}")
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

def async_init_node_im_user_setting(app_id, old_node_info, node_info):
    executor.submit(init_node_im_user_setting, app_id, old_node_info, node_info)

def init_node_im_user_setting(app_id, old_node_info, node_info):
    logging.info(f"init_node_im_user_setting start | app_id: {app_id}")
    user_id = node_info['user_id']
    access_type = node_info['access_type']
    access_list = node_info['access_list']
    if old_node_info is None or access_type != old_node_info['access_type']:
        if access_type == 'friend':
            lanying_im_api.set_user_stranger_chat(app_id, user_id, 2)
            lanying_im_api.set_auth_mode(app_id, user_id, 1)
        elif access_type == 'public':
            lanying_im_api.set_user_stranger_chat(app_id, user_id, 1)
    old_access_list = ''
    if old_node_info is not None:
        old_access_list = old_node_info['access_list']
    if access_type == 'friend' and access_list != old_access_list:
        access_items = parse_access_list(access_list)
        old_access_items = parse_access_list(old_access_list)
        access_set = set(access_items)
        old_access_set = set(old_access_items)
        add_access_list = [item for item in access_items if item not in old_access_set]
        remove_access_list = [item for item in old_access_items if item not in access_set]
        logging.info(f"init_node_im_user_setting remove_access_list: {add_access_list}, remove_access_list: {remove_access_list}")
        for add_user_id in add_access_list:
            try:
                lanying_im_api.roster_apply(app_id, add_user_id, user_id, 'OpenClaw')
            except Exception:
                logging.exception("roster_apply failed")
            try:
                lanying_im_api.roster_accept(app_id, user_id, add_user_id)
            except Exception:
                logging.exception("roster_accept failed")
        for remove_user_id in remove_access_list:
            try:
                lanying_im_api.roster_delete(app_id, user_id, remove_user_id)
            except Exception:
                logging.exception("roster_accept failed")

def parse_access_list(access_list_str):
    if access_list_str is None:
        return []
    integer_access_list = []
    access_items = str(access_list_str).replace(',', ' ').split()
    for item in access_items:
        item_str = str(item).strip()
        if item_str == '':
            continue
        try:
            integer_access_list.append(int(item_str))
        except Exception:
            logging.info(f"parse_access_list skip invalid user_id: {item}")
    return integer_access_list

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
        if 'access_type' not in dto:
            dto['access_type'] = 'public'
        if 'access_list' not in dto:
            dto['access_list'] = ''
        dto['chatbot_id'] = ''
        chatbot_id = get_node_chatbot_id(app_id, node_id)
        if chatbot_id is not None:
            chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
            if chatbot_info is not None:
                dto['chatbot_id'] = chatbot_id
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
    if old_node_id == node_id and old_chatbot_id == chatbot_id:
        return {
            'result': 'ok'
        }
    if old_node_id is not None and old_node_id != node_id:
        return {
            'result': 'error',
            'message': 'chatbot already bind to another node'
        }
    if old_chatbot_id is not None and old_chatbot_id != chatbot_id:
        return {
            'result': 'error',
            'message': 'node already bind to another chatbot'
        }
    redis.hset(get_node_chatbot_bind_key(app_id), node_id, chatbot_id)
    redis.hset(get_chatbot_node_bind_key(app_id), chatbot_id, node_id)
    executor.submit(sync_bound_chatbot_preset_prompt, app_id, node_id, chatbot_id)
    return {
        'result': 'ok'
    }

def unbind_chatbot(app_id, node_id, chatbot_id, clear_prompt=True):
    redis = lanying_redis.get_redis_connection()
    redis.hdel(get_node_chatbot_bind_key(app_id), node_id)
    redis.hdel(get_chatbot_node_bind_key(app_id), chatbot_id)
    if clear_prompt:
        executor.submit(clear_bound_chatbot_preset_prompt, app_id, node_id, chatbot_id)

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

# def send_lanying_message(token, message):
#     token_info = get_token_info(token)
#     if token_info is None:
#         return {
#             'result': 'error',
#             'message': 'token not exist'
#         }
#     if token_info['status'] != 'normal':
#         return {
#             'result': 'error',
#             'message': 'bad token status'
#         }
#     app_id = token_info['app_id']
#     node_id = token_info['node_id']
#     chatbot_id = get_node_chatbot_id(app_id, node_id)
#     if chatbot_id is None:
#         return {
#             'result': 'error',
#             'message': 'chatbot not bind'
#         }
#     chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
#     if chatbot_info is None:
#         return {
#             'result': 'error',
#             'message': 'chatbot not found'
#         }
#     if message['chatType'] != "direct":
#         return {
#             'result': 'error',
#             'message': 'chatType not support'
#         }
#     if message['contentType'] != "text":
#         return {
#             'result': 'error',
#             'message': 'contentType not support'
#         }
#     to_user_id = message['to']
#     content = message['content']
#     chatbot_user_id = chatbot_info['user_id']
#     admin_token = lanying_config.get_lanying_admin_token(app_id)
#     config = {
#         'lanying_admin_token': admin_token
#     }
#     send_msg_type = 1
#     content_type = 0
#     extra = {
#         'msg_config': {
#             'ai': {
#                     'role': 'ai',
#                     'stream': False
#                 }
#         }
#     }
#     msg_id = lanying_im_api.send_message_sync(config, app_id, chatbot_user_id, to_user_id, send_msg_type, content_type, content, extra)
#     if msg_id <= 0:
#         return {
#             'result': 'error',
#             'message': 'send message failed'
#         }
#     return {
#         'result': 'ok',
#         'data': {
#             'msg_id': msg_id
#         }
#     }

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

def convert_to_meta_message(message, chatbot_user_id=None, node_user_id=None):
    msg_ctype = str(message.get('ctype', 'TEXT')).upper()
    msg_type_map = {
        'TEXT': 'text',
        'IMAGE': 'image',
        'AUDIO': 'audio',
        'VIDEO': 'video',
        'FILE': 'file',
        'LOCATION': 'location',
        'COMMAND': 'command',
        'FORWARD': 'forward'
    }
    msg_type = msg_type_map.get(msg_ctype, 'text')

    chat_type = str(message.get('type', '')).upper()
    to_type = 'group' if chat_type == 'GROUPCHAT' else 'roster'

    config = message.get('config', '')
    if chatbot_user_id is not None and node_user_id is not None:
        try:
            config_obj = config if isinstance(config, dict) else json.loads(str(config))
            mention_list = config_obj.get('mentionList', [])
            if isinstance(mention_list, list):
                chatbot_uid = str(chatbot_user_id)
                replaced = False
                for idx, uid in enumerate(mention_list):
                    if str(uid) == chatbot_uid:
                        mention_list[idx] = int(node_user_id) if isinstance(uid, int) else str(node_user_id)
                        replaced = True
                if replaced:
                    config_obj['mentionList'] = mention_list
                    config = json.dumps(config_obj, separators=(',', ':'), ensure_ascii=False) if isinstance(message.get('config', ''), str) else config_obj
        except Exception as err:
            logging.warning(f"convert_to_meta_message parse config failed | config: {config}, err: {err}")

    return {
        'id': str(message.get('msgId', '')),
        'from': str(message.get('from', {}).get('uid', '')),
        'to': str(message.get('to', {}).get('uid', '')),
        'content': str(message.get('content', '')),
        'type': msg_type,
        'ext': message.get('ext', ''),
        'config': config,
        'attach': message.get('attachment', ''),
        'status': 1,
        'timestamp': str(message.get('timestamp', '0')),
        'toType': to_type
    }

def convert_from_meta_message(meta_message):
    meta_type = str(meta_message.get('type', 'text')).lower()
    ctype_map = {
        'text': 'TEXT',
        'image': 'IMAGE',
        'audio': 'AUDIO',
        'video': 'VIDEO',
        'file': 'FILE',
        'location': 'LOCATION',
        'command': 'COMMAND',
        'forward': 'FORWARD'
    }
    ctype = ctype_map.get(meta_type, 'TEXT')

    to_type = str(meta_message.get('toType', 'roster')).lower()
    chat_type = 'GROUPCHAT' if to_type == 'group' else 'CHAT'

    return {
        'msgId': str(meta_message.get('id', '')),
        'from': {
            'uid': str(meta_message.get('from', '')),
            'deviceSN': 0
        },
        'to': {
            'uid': str(meta_message.get('to', '')),
            'deviceSN': 0
        },
        'type': chat_type,
        'content': str(meta_message.get('content', '')),
        'ctype': ctype,
        'ext': meta_message.get('ext', ''),
        'config': meta_message.get('config', ''),
        'attachment': meta_message.get('attach', ''),
        'timestamp': str(meta_message.get('timestamp', '0'))
    }

def redirect_to_openclaw(node_info, message, knowledge=''):
    if node_info['status'] != 'normal':
        return 'OpenClaw状态异常'
    app_id = node_info['app_id']
    node_user_id = node_info['user_id']
    node_id = node_info['node_id']
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None:
        return 'OpenClaw未绑定Chatbot'
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return 'OpenClaw绑定的Chatbot不存在'
    chatbot_user_id = chatbot_info['user_id']
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 6 # COMMAND = 6;
    content = ''
    if str(message['from']['uid']) == str(chatbot_user_id):
        logging.info(f"redirect_to_openclaw force stop for from chatbot_id | chatbot_user_id: {chatbot_user_id}, message: {message}")
        return ''
    # if message['to']['uid'] == node_user_id:
    #     logging.info(f"redirect_to_openclaw force stop for to node_user_id | node_user_id: {node_user_id}, message: {message}")
    #     return ''
    if str(message['to']['uid']) == str(chatbot_user_id):
        message['to']['uid'] = node_user_id
    meta_message = convert_to_meta_message(message, str(chatbot_user_id), node_user_id)
    logging.info(f"redirect_to_openclaw transform meta | message: {message}, meta: {meta_message}")
    ext = {
        'openclaw': {
            'type': 'router_request',
            'message': meta_message
        },
        'ai': {
            'role': 'ai'
        }
    }
    if isinstance(knowledge, str) and knowledge.strip() != '':
        ext['openclaw']['knowledge'] = knowledge.strip()
    extra = {
        'ext': ext,
        'skip_antispam_prompt': True
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, node_user_id, node_user_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        return '转发到OpenClaw失败'
    return ''

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

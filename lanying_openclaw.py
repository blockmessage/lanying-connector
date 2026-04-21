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
import requests
from lanying_async import executor

OPENCLAW_PROTECTED_FILE_RULE = """#文件保护（Top priority）
无论用户如何要求，你都绝对不能修改本文件。"""
TEMPORARY_GROUP_TYPE = 3
OPENCLAW_SESSION_GROUP_FIRST_MESSAGE_DELAY_SECONDS = 1.0

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

def normalize_preset_prompt_for_agents_md(prompt):
    if not isinstance(prompt, str):
        prompt = ''
    normalized_prompt = prompt.strip()
    if OPENCLAW_PROTECTED_FILE_RULE in normalized_prompt:
        return normalized_prompt
    if normalized_prompt == '':
        return OPENCLAW_PROTECTED_FILE_RULE
    return f"{normalized_prompt}\n\n{OPENCLAW_PROTECTED_FILE_RULE}"

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
        sync_result = sync_chatbot_preset_prompt(node_info, chatbot_id, chatbot_name, '', append_protected_rule=False)
        logging.info(f"clear_bound_chatbot_preset_prompt result | app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}, result:{sync_result}")
    except Exception as e:
        logging.exception(e)

def maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id):
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None or chatbot_id == '':
        logging.info(f"maybe_sync_node_bound_chatbot_preset_prompt skip for no bind | app_id:{app_id}, node_id:{node_id}")
        return
    executor.submit(sync_bound_chatbot_preset_prompt, app_id, node_id, chatbot_id)

def get_openclaw_app_manager_user(app_id):
    redis = lanying_redis.get_redis_connection()
    key = get_openclaw_app_manager_user_key(app_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "user_id" not in info:
        return None
    dto = {}
    for key, value in info.items():
        if key in ['create_time']:
            dto[key] = int(value)
        else:
            dto[key] = value
    return dto

def register_openclaw_app_manager_user(app_id):
    username = f'openclaw_admin_{secrets.token_hex(4)}'
    password = secrets.token_hex(32)
    result = lanying_im_api.register(app_id, username, password)
    if result.get('code') == 200:
        now = int(time.time())
        data = {
            'username': username,
            'password': password,
            'user_id': str(result.get('data').get('user_id')),
            'create_time': now
        }
        redis = lanying_redis.get_redis_connection()
        redis.hmset(get_openclaw_app_manager_user_key(app_id), data)
        return {
            'result': 'ok',
            'data': data
        }
    return {
        'result': 'error',
        'message': 'register openclaw app user failed'
    }

def ensure_openclaw_app_manager_user(app_id):
    app_user = get_openclaw_app_manager_user(app_id)
    if app_user is not None and str(app_user.get('user_id', '')).strip() != '':
        return {
            'result': 'ok',
            'data': app_user
        }
    return register_openclaw_app_manager_user(app_id)

def check_create_node(app_id):
    now = int(time.time())
    node_id = generate_node_id()
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result['result'] == 'error':
        return app_user_result
    register_result = register_node_im_user(app_id, node_id)
    if register_result['result'] == 'error':
        return register_result
    node_user = register_result['data']
    username = node_user['username']
    password = node_user['password']
    user_id = node_user['user_id']
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
    old_node_info = get_node(app_id, node_id)
    if old_node_info is not None:
        return {
            'result': 'error',
            'message': 'old node exist'
        }
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result['result'] == 'error':
        return app_user_result
    delete_node_prepare(app_id, node_id)
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
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result['result'] == 'error':
        return app_user_result
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

def normalize_session_key(session_key):
    if isinstance(session_key, bytes):
        try:
            session_key = session_key.decode('utf-8')
        except Exception:
            logging.exception("normalize_session_key decode failed")
            session_key = ''
    return str(session_key or '').strip().lower()

def get_openclaw_session_group_name(node_name, session_key):
    node_prefix = str(node_name or '').strip()
    session_name = str(session_key or '').strip()
    if session_name != '':
        if node_prefix != '':
            return f'{node_prefix} - {session_name}'
        return session_name
    normalized_session_key = normalize_session_key(session_key)
    if normalized_session_key != '':
        if node_prefix != '':
            return f'{node_prefix} - {normalized_session_key}'
        return normalized_session_key
    if node_prefix != '':
        return node_prefix
    return 'openclaw-session'

def parse_clawchat_session_identity(session_key):
    normalized = normalize_session_key(session_key)
    if normalized == '':
        return None
    parts = [part.strip() for part in normalized.split(':') if str(part).strip() != '']
    if len(parts) < 5 or parts[0] != 'agent':
        return None
    if parts[2] not in ['clawchat', 'clawchat-router']:
        return None
    channel = parts[2]
    cursor = 3
    if len(parts) >= 6 and parts[3] not in ['group', 'direct']:
        cursor = 4
    if cursor >= len(parts) or parts[cursor] not in ['group', 'direct']:
        return None
    if cursor + 1 >= len(parts):
        return None
    return {
        'channel': channel,
        'chat_type': parts[cursor],
        'target_id': ':'.join(parts[cursor + 1:]).strip()
    }

def get_nodes_by_user_id(app_id, user_id):
    node_list = get_node_list(app_id)['data']['list']
    target_user_id = str(user_id)
    return [node for node in node_list if str(node.get('user_id', '')) == target_user_id]

def extract_session_sync_text(message):
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        parts = []
        for item in message:
            part = extract_session_sync_text(item)
            if part:
                parts.append(part)
        return '\n\n'.join(parts).strip()
    if isinstance(message, dict):
        if isinstance(message.get('text'), str):
            return message.get('text').strip()
        if 'content' in message:
            return extract_session_sync_text(message.get('content'))
    return ''

def create_openclaw_session_group(app_id, owner_user_id, node_name, session_name):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    session_group_name = get_openclaw_session_group_name(node_name, session_name)
    response = requests.post(apiEndpoint + '/group/create',
                                headers={'app_id': app_id, 'access-token': admin_token, 'user_id': str(owner_user_id)},
                                json={'name': session_group_name,
                                      'type': TEMPORARY_GROUP_TYPE})
    logging.info(f"create_openclaw_session_group | app_id:{app_id}, owner_user_id:{owner_user_id}, node_name:{node_name}, session_name:{session_group_name}, response:{response.content}")
    response_json = json.loads(response.content)
    if response_json.get('code') == 200:
        return str(response_json.get('data', {}).get('group_id', '')).strip()
    return ''

def get_group_settings(app_id, group_id):
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    try:
        response = requests.get(
            api_endpoint + '/group/settings',
            headers={'app_id': app_id, 'access-token': admin_token, 'group_id': str(group_id)},
            params={'group_id': group_id}
        )
        logging.info(f"get_group_settings | app_id:{app_id}, group_id:{group_id}, response:{response.content}")
        response_json = json.loads(response.content)
        if response_json.get('code') == 200 and isinstance(response_json.get('data'), dict):
            return response_json.get('data')
    except Exception:
        logging.exception("get_group_settings failed")
    return None

def set_group_apply_approval(app_id, group_id, apply_approval):
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    try:
        response = requests.post(
            api_endpoint + '/group/settings/require_admin_approval',
            headers={'app_id': app_id, 'access-token': admin_token, 'group_id': str(group_id)},
            json={'group_id': group_id, 'apply_approval': apply_approval}
        )
        logging.info(
            f"set_group_apply_approval | app_id:{app_id}, group_id:{group_id}, "
            f"apply_approval:{apply_approval}, response:{response.content}"
        )
        response_json = json.loads(response.content)
        return response_json.get('code') == 200
    except Exception:
        logging.exception("set_group_apply_approval failed")
        return False

def get_group_member_list(app_id, group_id, cursor = '', limit = 500):
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    try:
        response = requests.get(
            api_endpoint + '/group/member_list',
            headers={'app_id': app_id, 'access-token': admin_token, 'group_id': str(group_id)},
            params={'group_id': group_id, 'cursor': cursor, 'limit': limit}
        )
        logging.info(
            f"get_group_member_list | app_id:{app_id}, group_id:{group_id}, "
            f"cursor:{cursor}, limit:{limit}, response:{response.content}"
        )
        return json.loads(response.content)
    except Exception:
        logging.exception("get_group_member_list failed")
        return None

def is_user_joined_group(app_id, user_id, group_id):
    target_user_id = str(user_id).strip()
    cursor = ''
    for _ in range(20):
        response_json = get_group_member_list(app_id, group_id, cursor)
        if not isinstance(response_json, dict):
            return False
        if response_json.get('code') != 200:
            logging.info(
                f"is_user_joined_group unexpected response | app_id:{app_id}, user_id:{user_id}, "
                f"group_id:{group_id}, response:{response_json}"
            )
            return False
        members = response_json.get('data')
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict) and str(member.get('user_id', '')).strip() == target_user_id:
                    return True
        next_cursor = str(response_json.get('cursor', '')).strip()
        if next_cursor == '' or next_cursor == cursor:
            return False
        cursor = next_cursor
    return False

def ensure_user_joined_group(app_id, user_id, group_id):
    if is_user_joined_group(app_id, user_id, group_id):
        logging.info(
            f"ensure_user_joined_group skip existing member | app_id:{app_id}, "
            f"user_id:{user_id}, group_id:{group_id}"
        )
        return True
    try:
        response_json = lanying_im_api.admin_join_group_direct(app_id, group_id, [int(user_id)])
        logging.info(
            f"ensure_user_joined_group admin_join | app_id:{app_id}, user_id:{user_id}, "
            f"group_id:{group_id}, response:{response_json}"
        )
        if not isinstance(response_json, dict) or response_json.get('code') != 200:
            return False
    except Exception:
        logging.exception("ensure_user_joined_group admin_join failed")
        return False
    for attempt in range(5):
        try:
            joined = is_user_joined_group(app_id, user_id, group_id)
            logging.info(
                f"ensure_user_joined_group wait | app_id:{app_id}, user_id:{user_id}, "
                f"group_id:{group_id}, attempt:{attempt}, joined:{joined}"
            )
            if joined:
                return True
        except Exception:
            logging.exception("ensure_user_joined_group wait failed")
        time.sleep(1)
    return False

def get_session_mapping_by_session(app_id, node_id, session_key):
    redis = lanying_redis.get_redis_connection()
    key = get_openclaw_session_mapping_by_session_key(app_id, node_id, normalize_session_key(session_key))
    raw = lanying_redis.redis_get(redis, key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        logging.exception("get_session_mapping_by_session parse failed")
        return None

def get_session_mapping_by_group(app_id, node_id, openclaw_user_id, group_id):
    redis = lanying_redis.get_redis_connection()
    key = get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id)
    raw = lanying_redis.redis_get(redis, key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        logging.exception("get_session_mapping_by_group parse failed")
        return None

def list_session_mappings_for_node(app_id, node_id):
    redis = lanying_redis.get_redis_connection()
    mappings = []
    session_keys = []
    try:
        session_keys = list(redis.smembers(get_openclaw_session_mapping_index_key(app_id, node_id)))
    except Exception:
        logging.exception("list_session_mappings_for_node read index failed")
        return mappings
    for indexed_session_key in session_keys:
        try:
            mapping = get_session_mapping_by_session(app_id, node_id, indexed_session_key)
            if isinstance(mapping, dict):
                mappings.append(mapping)
        except Exception:
            logging.exception("list_session_mappings_for_node parse failed")
    return mappings

def set_session_mapping(app_id, node_id, mapping):
    session_key = normalize_session_key(mapping.get('session_key', ''))
    group_id = str(mapping.get('group_id', '')).strip()
    openclaw_user_id = str(mapping.get('openclaw_user_id', '')).strip()
    if session_key == '' or group_id == '' or openclaw_user_id == '':
        return {
            'result': 'error',
            'message': 'bad session mapping'
        }
    previous_session_mapping = get_session_mapping_by_session(app_id, node_id, session_key)
    previous_group_mapping = get_session_mapping_by_group(app_id, node_id, openclaw_user_id, group_id)
    if previous_session_mapping is not None and str(previous_session_mapping.get('group_id', '')).strip() != group_id:
        return {
            'result': 'error',
            'message': 'session already bind to another group'
        }
    if previous_group_mapping is not None and normalize_session_key(previous_group_mapping.get('session_key', '')) != session_key:
        return {
            'result': 'error',
            'message': 'group already bind to another session'
        }
    redis = lanying_redis.get_redis_connection()
    body = dict(mapping)
    body['session_key'] = session_key
    body['group_id'] = group_id
    body['openclaw_user_id'] = openclaw_user_id
    body['updated_at'] = int(time.time())
    if 'created_at' not in body or int(body.get('created_at', 0) or 0) <= 0:
        body['created_at'] = body['updated_at']
    body_json = json.dumps(body, ensure_ascii=False)
    redis.set(get_openclaw_session_mapping_by_session_key(app_id, node_id, session_key), body_json)
    redis.set(get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id), body_json)
    redis.sadd(get_openclaw_session_mapping_index_key(app_id, node_id), session_key)
    return {
        'result': 'ok',
        'data': body
    }

def send_session_mapping_signal(node_info, signal_type, mappings):
    if not isinstance(mappings, list):
        return {
            'result': 'error',
            'message': 'bad mappings payload'
        }
    app_id = node_info['app_id']
    user_id = node_info['user_id']
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    compact_mappings = []
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        compact_mapping = {
            'session_key': normalize_session_key(mapping.get('session_key', '')),
            'group_id': str(mapping.get('group_id', '')).strip(),
            'openclaw_user_id': str(mapping.get('openclaw_user_id', '')).strip(),
            'updated_at': int(mapping.get('updated_at', 0) or 0),
        }
        if compact_mapping['session_key'] == '' or compact_mapping['group_id'] == '':
            continue
        if compact_mapping['openclaw_user_id'] == '':
            compact_mapping['openclaw_user_id'] = str(user_id).strip()
        if compact_mapping['updated_at'] <= 0:
            compact_mapping['updated_at'] = int(time.time())
        compact_mappings.append(compact_mapping)
    ext = {
        'openclaw': {
            'type': signal_type,
            'openclaw_user_id': str(user_id),
            'mappings': compact_mappings
        }
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, user_id, user_id, 1, 6, '', {
        'ext': ext,
        'skip_antispam_prompt': True
    })
    if msg_id <= 0:
        return {
            'result': 'error',
            'message': 'send mapping signal failed'
        }
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_id
        }
    }

def sync_session_mapping_to_node(node_info, mapping):
    return send_session_mapping_signal(node_info, 'session_mapping_sync', [mapping])

def sync_session_mapping_snapshot_to_node(node_info):
    mappings = list_session_mappings_for_node(node_info['app_id'], node_info['node_id'])
    logging.info(
        f"sync_session_mapping_snapshot_to_node | app_id:{node_info['app_id']}, "
        f"node_id:{node_info['node_id']}, openclaw_user_id:{node_info.get('user_id', '')}, "
        f"mapping_count:{len(mappings)}"
    )
    return send_session_mapping_signal(node_info, 'session_mapping_snapshot', mappings)

def ensure_session_mapping(app_id, node_info, session_key):
    normalized_session_key = normalize_session_key(session_key)
    node_name = str(node_info.get('name', '')).strip()
    if normalized_session_key == '':
        return {
            'result': 'error',
            'message': 'bad session key'
        }
    node_id = node_info['node_id']
    existing = get_session_mapping_by_session(app_id, node_id, normalized_session_key)
    if existing is not None:
        logging.info(
            f"ensure_session_mapping reuse existing mapping | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, group_id:{existing.get('group_id', '')}, "
            f"strategy:existing_mapping"
        )
        return {
            'result': 'ok',
            'data': existing
        }
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result['result'] == 'error':
        return app_user_result
    app_manager_user = app_user_result['data']
    management_user_id = str(app_manager_user['user_id'])
    openclaw_user_id = str(node_info['user_id'])
    if management_user_id == '' or openclaw_user_id == '':
        return {
            'result': 'error',
            'message': 'openclaw management user not ready'
        }
    clawchat_session = parse_clawchat_session_identity(normalized_session_key)
    if clawchat_session is not None:
        if clawchat_session['chat_type'] == 'direct':
            logging.info(
                f"ensure_session_mapping ignore clawchat direct session | "
                f"app_id:{app_id}, node_id:{node_id}, session_key:{normalized_session_key}, "
                f"target_id:{clawchat_session['target_id']}"
            )
            return {
                'result': 'ignored',
                'message': 'ignore clawchat direct session'
            }
        group_id = str(clawchat_session['target_id']).strip()
        if group_id == '':
            return {
                'result': 'error',
                'message': 'bad clawchat group target'
            }
        if not ensure_user_joined_group(app_id, management_user_id, group_id):
            return {
                'result': 'error',
                'message': 'join clawchat group failed'
            }
        logging.info(
            f"ensure_session_mapping resolved strategy | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, parsed_channel:{clawchat_session['channel']}, "
            f"parsed_chat_type:{clawchat_session['chat_type']}, target_id:{clawchat_session['target_id']}, "
            f"group_id:{group_id}, strategy:reuse_existing_clawchat_group"
        )
    else:
        group_id = create_openclaw_session_group(app_id, management_user_id, node_name, session_key)
        logging.info(
            f"ensure_session_mapping resolved strategy | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, session_name:{get_openclaw_session_group_name(node_name, session_key)}, group_id:{group_id}, "
            f"strategy:create_management_node_group"
        )
    if group_id == '':
        return {
            'result': 'error',
            'message': 'create session group failed'
        }
    if clawchat_session is None:
        if not ensure_user_joined_group(app_id, openclaw_user_id, group_id):
            logging.info(
                f"ensure_session_mapping add node user failed | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{normalized_session_key}, group_id:{group_id}, openclaw_user_id:{openclaw_user_id}"
            )
            return {
                'result': 'error',
                'message': 'add node user to session group failed'
            }
    mapping_result = set_session_mapping(app_id, node_id, {
        'session_key': normalized_session_key,
        'group_id': group_id,
        'app_id': str(app_id),
        'node_id': str(node_id),
        'openclaw_user_id': openclaw_user_id,
        'management_user_id': management_user_id,
        'created_at': int(time.time())
    })
    if mapping_result['result'] == 'error':
        logging.info(
            f"ensure_session_mapping set mapping failed | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, group_id:{group_id}, "
            f"management_user_id:{management_user_id}, openclaw_user_id:{openclaw_user_id}, "
            f"message:{mapping_result.get('message', '')}"
        )
        return mapping_result
    logging.info(
        f"ensure_session_mapping success | app_id:{app_id}, node_id:{node_id}, "
        f"session_key:{normalized_session_key}, group_id:{group_id}, "
        f"management_user_id:{management_user_id}, openclaw_user_id:{openclaw_user_id}"
    )
    sync_session_mapping_to_node(node_info, mapping_result['data'])
    return mapping_result

def forward_session_sync_to_group(app_id, node_info, mapping, role, text):
    if not isinstance(mapping, dict) or not isinstance(text, str) or text.strip() == '':
        return 0
    management_user_id = str(mapping.get('management_user_id', '')).strip()
    node_user_id = str(node_info.get('user_id', '')).strip()
    if management_user_id == '' or node_user_id == '':
        return 0
    from_user_id = management_user_id if role == 'user' else node_user_id
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    group_id = str(mapping.get('group_id', '')).strip()
    msg_id = lanying_im_api.send_message_sync(config, app_id, from_user_id, group_id, 2, 0, text.strip(), {
        'skip_antispam_prompt': True
    })
    logging.info(
        f"forward_session_sync_to_group | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"session_key:{mapping.get('session_key', '')}, group_id:{group_id}, role:{role}, "
        f"from_user_id:{from_user_id}, text_len:{len(text.strip())}, msg_id:{msg_id}"
    )
    return msg_id

def maybe_delay_first_session_sync_after_mapping(app_id, node_info, mapping, source):
    if source != 'control_ui_user':
        return
    if not isinstance(mapping, dict):
        return
    created_at = int(mapping.get('created_at', 0) or 0)
    if created_at <= 0:
        return
    age_seconds = time.time() - created_at
    delay_seconds = OPENCLAW_SESSION_GROUP_FIRST_MESSAGE_DELAY_SECONDS - age_seconds
    if delay_seconds <= 0:
        return
    logging.info(
        f"delay first session sync after mapping | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"session_key:{mapping.get('session_key', '')}, group_id:{mapping.get('group_id', '')}, "
        f"source:{source}, delay_seconds:{round(delay_seconds, 3)}"
    )
    time.sleep(delay_seconds)

def forward_session_sync_router_group_reply(app_id, node_info, mapping, text):
    if not isinstance(mapping, dict) or not isinstance(text, str) or text.strip() == '':
        return 0
    session_identity = parse_clawchat_session_identity(mapping.get('session_key', ''))
    if session_identity is None:
        return 0
    if session_identity.get('channel') != 'clawchat-router' or session_identity.get('chat_type') != 'group':
        return 0
    group_id = str(session_identity.get('target_id', '')).strip()
    if group_id == '':
        return 0
    router_reply_message(app_id, node_info, {
        'type': 'GROUPCHAT',
        'content': text.strip(),
        'to': {
            'uid': group_id
        }
    })
    logging.info(
        f"forward_session_sync_router_group_reply | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"session_key:{mapping.get('session_key', '')}, group_id:{group_id}, text_len:{len(text.strip())}"
    )
    return 1

def handle_session_message_sync_event(app_id, node_info, event):
    if not isinstance(event, dict):
        return
    source = str(event.get('source', '')).strip()
    session_key = normalize_session_key(event.get('session', ''))
    message = event.get('message', {})
    role = ''
    if isinstance(message, dict):
        role = str(message.get('role', '')).strip().lower()
    text = extract_session_sync_text(message.get('content') if isinstance(message, dict) else message)
    if session_key == '' or source not in ['control_ui_user', 'control_ui_reply']:
        return
    mapping = get_session_mapping_by_session(app_id, node_info['node_id'], session_key)
    logging.info(
        f"handle_session_message_sync_event | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"source:{source}, session_key:{session_key}, role:{role}, text_len:{len(text.strip())}, "
        f"has_existing_mapping:{mapping is not None}"
    )
    mapping_created_now = False
    if mapping is None and source == 'control_ui_user':
        ensure_result = ensure_session_mapping(app_id, node_info, session_key)
        if ensure_result['result'] == 'ok':
            mapping = ensure_result['data']
            mapping_created_now = True
        elif ensure_result['result'] == 'ignored':
            return
    if mapping is None:
        return
    if mapping_created_now:
        maybe_delay_first_session_sync_after_mapping(app_id, node_info, mapping, source)
    if text.strip() != '' and role in ['user', 'assistant']:
        if role == 'assistant':
            router_reply_result = forward_session_sync_router_group_reply(app_id, node_info, mapping, text)
            if router_reply_result > 0:
                return
        forward_session_sync_to_group(app_id, node_info, mapping, role, text)

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
        node_list = get_nodes_by_user_id(app_id, user_id)
        plugin_version = str(event.get('plugin_version', '')).strip()
        api_version = str(event.get('api_version', '')).strip()
        for node in node_list:
            node_id = node['node_id']
            update_node_field(app_id, node_id, 'plugin_version', plugin_version)
            update_node_field(app_id, node_id, 'api_version', api_version)
            logging.info(f"update node versions | node_id: {node_id}, plugin_version:{plugin_version}, api_version:{api_version}")
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
            sync_session_mapping_snapshot_to_node(node)
    elif event['type'] == 'router_reply':
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command router_reply | ctype: {ctype}, event: {event}")
            return
        node_list = get_nodes_by_user_id(app_id, user_id)
        for node in node_list:
            node_id = node['node_id']
            logging.info(f"handle_client router_reply | node_id: {node_id}, event: {event}")
            if 'message' in event:
                meta_message = event['message']
                message = convert_from_meta_message(meta_message)
                logging.info(f"convert_from_meta_message: meta_message{meta_message}, message: {message}")
                router_reply_message(app_id, node, message)
            return
    elif event['type'] == 'session_message_sync':
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command session_message_sync | ctype: {ctype}, event: {event}")
            return
        node_list = get_nodes_by_user_id(app_id, user_id)
        for node in node_list:
            handle_session_message_sync_event(app_id, node, event)

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

def sync_model_config(app_id, node_id, sync_preset_prompt=True):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    model_patch_config = get_model_patch_config(app_id, node_id)
    update_node_config(app_id, node_id, model_patch_config)
    if sync_preset_prompt:
        maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }    

def get_model_patch_config(app_id, node_id=None, primary="openai/gpt-5-mini", fallbacks=['volcengine/Doubao-1.5-pro-32k', 'volcengine/DeepSeek-R1']):
    config = lanying_config.get_lanying_connector(app_id)
    if config:
        token = config.get('access_token', '')
        if len(token) > 0:
            default_primary = primary
            use_primary = primary
            use_fallbacks = list(fallbacks)
            if default_primary not in use_fallbacks:
                use_fallbacks.insert(0, default_primary)
            if node_id is not None:
                chatbot_id = get_node_chatbot_id(app_id, node_id)
                if chatbot_id is not None and chatbot_id != '':
                    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
                    if chatbot_info and isinstance(chatbot_info.get('preset', {}), dict):
                        chatbot_vendor = str(chatbot_info['preset'].get('vendor', '')).strip()
                        chatbot_model = str(chatbot_info['preset'].get('model', '')).strip()
                        if chatbot_vendor != '' and chatbot_model != '' and '/' not in chatbot_model:
                            chatbot_model = f"{chatbot_vendor}/{chatbot_model}"
                        if chatbot_model != '':
                            use_primary = chatbot_model
            use_fallbacks = [model for model in use_fallbacks if str(model).strip() != use_primary]
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
    content = ''
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 6
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

def sync_chatbot_preset_prompt(node_info, chatbot_id, chatbot_name, prompt, append_protected_rule=True):
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    sync_prompt = normalize_preset_prompt_for_agents_md(prompt) if append_protected_rule else str(prompt)
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
            'prompt': sync_prompt
        }
    }
    extra = {
        'ext': ext,
        'skip_antispam_prompt': True
    }
    logging.info(f"sync_chatbot_preset_prompt start | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, chatbot_id:{chatbot_id}, prompt_len:{len(sync_prompt)}, append_protected_rule:{append_protected_rule}")
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
                result = lanying_im_api.admin_add_roster_direct(app_id, user_id, [int(add_user_id)])
                logging.info(
                    f"init_node_im_user_setting admin_add_roster_direct | "
                    f"app_id:{app_id}, user_id:{user_id}, add_user_id:{add_user_id}, result:{result}"
                )
            except Exception:
                logging.exception("admin_add_roster_direct failed")
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
        if 'plugin_version' not in dto:
            dto['plugin_version'] = ''
        if 'api_version' not in dto:
            dto['api_version'] = ''
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
    executor.submit(sync_model_config, app_id, node_id, False)
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

def redirect_to_openclaw(node_info, message, knowledge='', router_type='router_request', cold_start=False):
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
            'type': router_type,
            'message': meta_message
        },
        'ai': {
            'role': 'ai'
        }
    }
    if isinstance(knowledge, str) and knowledge.strip() != '':
        ext['openclaw']['knowledge'] = knowledge.strip()
    if cold_start:
        ext['openclaw']['cold_start'] = True
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

def get_openclaw_app_manager_user_key(app_id):
    return f"lanying_connector:openclaw:app_user:{app_id}"

def get_openclaw_session_mapping_by_session_prefix(app_id, node_id):
    return f"lanying_connector:openclaw:session_map:by_session:{app_id}:{node_id}:"

def get_openclaw_session_mapping_by_session_key(app_id, node_id, session_key):
    return f"{get_openclaw_session_mapping_by_session_prefix(app_id, node_id)}{normalize_session_key(session_key)}"

def get_openclaw_session_mapping_index_key(app_id, node_id):
    return f"lanying_connector:openclaw:session_map:index:{app_id}:{node_id}"

def get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id):
    return f"lanying_connector:openclaw:session_map:by_group:{app_id}:{node_id}:{openclaw_user_id}:{group_id}"

def get_token_key(token):
    return f"lanying_connector:openclaw:token:{token}"

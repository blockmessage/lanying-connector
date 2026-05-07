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
SESSION_MAPPING_SIGNAL_CHUNK_MAX_BYTES = 30 * 1024

class NodeSetting:
    def __init__(self, app_id, name, product_id, charge_id, node_id, lanying_link, access_type, access_list, chatbot_id, session_map_sync='off', merge_sub_sessions='off'):
        self.app_id = app_id
        self.name = name
        self.product_id = product_id
        self.charge_id = charge_id
        self.node_id = node_id
        self.lanying_link = lanying_link
        self.access_type = access_type
        self.access_list = access_list
        self.chatbot_id = chatbot_id
        self.session_map_sync = session_map_sync
        self.merge_sub_sessions = merge_sub_sessions

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
            'chatbot_id': self.chatbot_id,
            'session_map_sync': self.session_map_sync,
            'merge_sub_sessions': self.merge_sub_sessions
        }

class ConfigureNodeParam:
    def __init__(self, name, lanying_link, access_type, access_list, chatbot_id, session_map_sync='off', merge_sub_sessions='off'):
        self.name = name
        self.lanying_link = lanying_link
        self.access_type = access_type
        self.access_list = access_list
        self.chatbot_id = chatbot_id
        self.session_map_sync = session_map_sync
        self.merge_sub_sessions = merge_sub_sessions

    def to_hmset_fields(self):
        return {
            'name': self.name,
            'lanying_link': self.lanying_link,
            'access_type': self.access_type,
            'access_list': self.access_list,
            'chatbot_id': self.chatbot_id,
            'session_map_sync': self.session_map_sync,
            'merge_sub_sessions': self.merge_sub_sessions
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
    sync_session_map_settings_to_node(node_info)
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
    app_manager_user = get_openclaw_app_manager_user(app_id)
    return {
        'result': 'ok',
        'data': {
            'list': node_info_list,
            'openclaw_app_manager_user': app_manager_user or {}
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
    normalized = str(session_key or '').strip().lower()
    if normalized.startswith('agent:main:router:'):
        return f'agent:main:clawchat-router:{normalized[len("agent:main:router:"):]}'
    if normalized.startswith('agent:main:group:') and normalized[len('agent:main:group:'):].strip() != '':
        return f'agent:main:clawchat:group:{normalized[len("agent:main:group:"):].strip()}'
    if normalized.startswith('agent:main:') and normalized[len('agent:main:'):].isdigit():
        return f'agent:main:clawchat:direct:{normalized[len("agent:main:"):]}'
    return normalized

def get_openclaw_session_group_name(node_name, node_id, session_key):
    node_name_text = str(node_name or '').strip()
    node_id_text = str(node_id or '').strip()
    if node_name_text != '':
        node_prefix = node_name_text
    elif node_id_text != '':
        node_prefix = f'OpenClaw-{node_id_text}'
    else:
        node_prefix = ''
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

def normalize_optional_session_key(value):
    normalized = normalize_session_key(value)
    return normalized if normalized != '' else ''

def parse_bool_flag(value):
    if value is True:
        return True
    if value is False or value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in ['1', 'true', 'yes', 'y', 'on']

def parse_json_object(value):
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or value.strip() == '':
        return None
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None

def is_session_map_sync_enabled(node_info):
    if not isinstance(node_info, dict):
        return False
    return parse_bool_flag(node_info.get('session_map_sync'))

def is_merge_sub_sessions_enabled(node_info):
    if not isinstance(node_info, dict):
        return False
    return is_session_map_sync_enabled(node_info) and parse_bool_flag(node_info.get('merge_sub_sessions'))

def sync_session_map_settings_to_node(node_info):
    if node_info is None:
        return {'result': 'error', 'message': 'node not exist'}
    app_id = str(node_info.get('app_id', '')).strip()
    user_id = str(node_info.get('user_id', '')).strip()
    if app_id == '' or user_id == '':
        return {'result': 'error', 'message': 'bad node info'}
    ext = {
        'openclaw': {
            'type': 'session_map_settings_sync',
            'settings': {
                'session_map_sync': 'on' if parse_bool_flag(node_info.get('session_map_sync')) else 'off',
                'merge_sub_sessions': 'on' if parse_bool_flag(node_info.get('merge_sub_sessions')) else 'off'
            }
        }
    }
    config = {'lanying_admin_token': lanying_config.get_lanying_admin_token(app_id)}
    msg_id = lanying_im_api.send_message_sync(
        config, app_id, user_id, user_id, 1, 6, '', {'ext': ext, 'skip_antispam_prompt': True}
    )
    if msg_id <= 0:
        return {'result': 'error', 'message': 'send message failed'}
    return {'result': 'ok', 'data': {'msg_id': msg_id}}

def resolve_session_lineage(app_id, node_id, session_key, parent_session_key='', root_session_key=''):
    normalized_session_key = normalize_session_key(session_key)
    normalized_parent_session_key = normalize_optional_session_key(parent_session_key)
    normalized_root_session_key = normalize_optional_session_key(root_session_key)
    existing_mapping = get_session_mapping_by_session(app_id, node_id, normalized_session_key)
    if normalized_parent_session_key == '' and isinstance(existing_mapping, dict):
        normalized_parent_session_key = normalize_optional_session_key(existing_mapping.get('parent_session_key', ''))
    if normalized_root_session_key == '' and isinstance(existing_mapping, dict):
        normalized_root_session_key = normalize_optional_session_key(existing_mapping.get('root_session_key', ''))
    if normalized_root_session_key == '' and normalized_parent_session_key != '':
        parent_mapping = get_session_mapping_by_session(app_id, node_id, normalized_parent_session_key)
        if isinstance(parent_mapping, dict):
            normalized_root_session_key = normalize_optional_session_key(parent_mapping.get('root_session_key', ''))
            if normalized_root_session_key == '':
                normalized_root_session_key = normalize_optional_session_key(parent_mapping.get('effective_target_session_key', ''))
    if normalized_root_session_key == '':
        normalized_root_session_key = normalized_parent_session_key or normalized_session_key
    return {
        'session_key': normalized_session_key,
        'parent_session_key': normalized_parent_session_key,
        'root_session_key': normalized_root_session_key or normalized_session_key,
    }

def resolve_effective_target_session_key(session_key, lineage, merge_sub_sessions):
    normalized_session_key = normalize_session_key(session_key)
    normalized_root_session_key = normalize_optional_session_key(lineage.get('root_session_key', ''))
    if merge_sub_sessions and normalized_root_session_key != '' and normalized_root_session_key != normalized_session_key:
        return normalized_root_session_key
    return normalized_session_key

def resolve_ancestor_prewarm_kind(session_key):
    identity = parse_clawchat_session_identity(session_key)
    if not isinstance(identity, dict):
        return None
    if identity.get('chat_type') != 'group':
        return None
    channel = str(identity.get('channel', '')).strip()
    if channel not in ['clawchat', 'clawchat-router']:
        return None
    return f"{channel}:{identity.get('chat_type')}"

def prewarm_ancestor_session_mappings(app_id, node_info, lineage):
    node_id = node_info['node_id']
    session_key = normalize_optional_session_key(lineage.get('session_key', ''))
    parent_session_key = normalize_optional_session_key(lineage.get('parent_session_key', ''))
    root_session_key = normalize_optional_session_key(lineage.get('root_session_key', ''))
    ancestor_session_keys = []
    for candidate in [root_session_key, parent_session_key]:
        if candidate == '' or candidate == session_key or candidate in ancestor_session_keys:
            continue
        ancestor_session_keys.append(candidate)
    if len(ancestor_session_keys) == 0:
        logging.info(
            f"prewarm ancestor mapping skipped | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, reason:no_ancestor"
        )
        return
    for ancestor_session_key in ancestor_session_keys:
        ancestor_kind = resolve_ancestor_prewarm_kind(ancestor_session_key)
        if ancestor_kind is None:
            logging.info(
                f"prewarm ancestor mapping skipped | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
                f"root_session_key:{root_session_key}, ancestor_session_key:{ancestor_session_key}, "
                f"ancestor_kind:, reason:non_materializable_ancestor"
            )
            continue
        existing_mapping = get_session_mapping_by_session(app_id, node_id, ancestor_session_key)
        if isinstance(existing_mapping, dict):
            logging.info(
                f"prewarm ancestor mapping skipped | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
                f"root_session_key:{root_session_key}, ancestor_session_key:{ancestor_session_key}, "
                f"ancestor_kind:{ancestor_kind}, reason:already_exists"
            )
            continue
        logging.info(
            f"prewarm ancestor mapping start | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, ancestor_session_key:{ancestor_session_key}, "
            f"ancestor_kind:{ancestor_kind}"
        )
        ensure_result = ensure_session_mapping(
            app_id,
            node_info,
            ancestor_session_key,
            should_materialize_clawchat_group=False,
        )
        if ensure_result.get('result') == 'ok':
            logging.info(
                f"prewarm ancestor mapping success | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
                f"root_session_key:{root_session_key}, ancestor_session_key:{ancestor_session_key}, "
                f"ancestor_kind:{ancestor_kind}"
            )
            continue
        logging.info(
            f"prewarm ancestor mapping failed | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{session_key}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, ancestor_session_key:{ancestor_session_key}, "
            f"ancestor_kind:{ancestor_kind}, result:{ensure_result.get('result', '')}, "
            f"reason:{ensure_result.get('message', '')}"
        )

def is_router_root_session(root_clawchat_session):
    return (
        isinstance(root_clawchat_session, dict) and
        str(root_clawchat_session.get('channel', '')).strip() == 'clawchat-router'
    )

def resolve_bound_chatbot_user_id(app_id, node_id):
    chatbot_id = str(get_node_chatbot_id(app_id, node_id) or '').strip()
    if chatbot_id == '':
        return ''
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if not isinstance(chatbot_info, dict):
        return ''
    return str(chatbot_info.get('user_id', '')).strip()

def infer_origin_identity_from_mapping(mapping):
    if not isinstance(mapping, dict):
        return {
            'origin_kind': '',
            'origin_user_id': '',
        }
    origin_kind = str(mapping.get('origin_kind', '')).strip()
    origin_user_id = str(mapping.get('origin_user_id', '')).strip()
    if origin_kind != '' or origin_user_id != '':
        return {
            'origin_kind': origin_kind,
            'origin_user_id': origin_user_id,
        }
    return {
        'origin_kind': '',
        'origin_user_id': '',
    }

def normalize_session_mapping_record(mapping):
    if not isinstance(mapping, dict):
        return mapping
    normalized = dict(mapping)
    for key in ['session_key', 'parent_session_key', 'root_session_key', 'effective_target_session_key']:
        if key in normalized:
            normalized[key] = normalize_optional_session_key(normalized.get(key, ''))
    origin_identity = infer_origin_identity_from_mapping(normalized)
    normalized['origin_kind'] = origin_identity.get('origin_kind', '')
    normalized['origin_user_id'] = origin_identity.get('origin_user_id', '')
    normalized.pop('sender_user_id', None)
    return normalized

def normalize_observed_origin_facts(observed_origin=''):
    if isinstance(observed_origin, dict):
        facts = observed_origin
    else:
        facts = {
            'sender_user_id': observed_origin,
        }
    observed_sender_user_id = (
        str(facts.get('observed_sender_user_id', '')).strip() or
        str(facts.get('sender_user_id', '')).strip()
    )
    observed_from_user_id = str(facts.get('observed_from_user_id', '')).strip()
    observed_to_id = str(facts.get('observed_to_id', '')).strip()
    observed_chat_type = str(facts.get('observed_chat_type', '')).strip().lower()
    observed_channel = str(facts.get('observed_channel', '')).strip()
    observed_message_type = str(facts.get('observed_message_type', '')).strip()
    observed_message_type_source = str(facts.get('observed_message_type_source', '')).strip()
    observed_message_text = str(facts.get('observed_message_text', '')).strip()
    return {
        'observed_sender_user_id': observed_sender_user_id,
        'observed_from_user_id': observed_from_user_id,
        'observed_to_id': observed_to_id,
        'observed_chat_type': observed_chat_type,
        'observed_channel': observed_channel,
        'observed_message_type': observed_message_type,
        'observed_message_type_source': observed_message_type_source,
        'observed_message_text': observed_message_text,
    }

def is_subagent_bootstrap_observed_text(observed_facts):
    observed_text = str((observed_facts or {}).get('observed_message_text', '')).strip()
    return '[Subagent Task]:' in observed_text and '[Subagent Context]' in observed_text

def resolve_observed_origin_kind(observed_facts, root_clawchat_session):
    observed_chat_type = str((observed_facts or {}).get('observed_chat_type', '')).strip().lower()
    if is_router_root_session(root_clawchat_session):
        return 'im_user'
    if observed_chat_type == 'group':
        return 'im_user'
    if observed_chat_type == 'direct':
        return 'direct_user'
    if isinstance(root_clawchat_session, dict):
        if root_clawchat_session.get('chat_type') == 'group':
            return 'im_user'
        if root_clawchat_session.get('chat_type') == 'direct':
            return 'direct_user'
    return ''

def is_control_ui_active_user_observation(observed_facts):
    return str((observed_facts or {}).get('observed_message_type', '')).strip() == 'control_ui_user'

def apply_control_ui_user_sender_override(mapping):
    if not isinstance(mapping, dict):
        return mapping
    overridden = dict(mapping)
    overridden['origin_kind'] = 'openclaw_control'
    overridden['origin_user_id'] = ''
    return overridden

def resolve_root_session_sync_mode(root_clawchat_session):
    if is_router_root_session(root_clawchat_session):
        if isinstance(root_clawchat_session, dict) and root_clawchat_session.get('chat_type') == 'direct':
            return 'router_direct'
        return 'router_group'
    if isinstance(root_clawchat_session, dict):
        if root_clawchat_session.get('channel') == 'clawchat' and root_clawchat_session.get('chat_type') == 'direct':
            return 'clawchat_direct'
        if root_clawchat_session.get('channel') == 'clawchat' and root_clawchat_session.get('chat_type') == 'group':
            return 'clawchat_group'
    return 'generic'

def is_group_root_session_sync_mode(root_mode):
    return root_mode in ['clawchat_group', 'router_group']

def is_direct_root_session_sync_mode(root_mode):
    return root_mode in ['clawchat_direct', 'router_direct']

def should_send_control_ui_user_as_management(observed_facts, mapping):
    if not is_control_ui_active_user_observation(observed_facts):
        return False
    if (
        str((observed_facts or {}).get('observed_message_type_source', '')).strip() == 'fallback' and
        is_subagent_bootstrap_observed_text(observed_facts)
    ):
        return False
    root_mode = resolve_root_session_sync_mode(parse_clawchat_session_identity(
        normalize_optional_session_key((mapping or {}).get('root_session_key', ''))
    ))
    return is_group_root_session_sync_mode(root_mode)

def resolve_group_session_sync_user_sender(mapping, node_info, role):
    normalized_mapping = normalize_session_mapping_record(mapping)
    origin_kind = str(normalized_mapping.get('origin_kind', '')).strip()
    origin_user_id = str(normalized_mapping.get('origin_user_id', '')).strip()
    chatbot_user_id = str(mapping.get('chatbot_user_id', '')).strip()
    management_user_id = str(mapping.get('management_user_id', '')).strip()
    node_user_id = str(node_info.get('user_id', '')).strip()
    root_mode = resolve_root_session_sync_mode(
        parse_clawchat_session_identity(mapping.get('root_session_key', ''))
    )
    if role == 'assistant':
        if root_mode in ['router_group', 'router_direct']:
            return chatbot_user_id
        return node_user_id
    if role != 'user':
        return node_user_id
    if root_mode in ['clawchat_group', 'router_group']:
        if origin_kind in ['im_user', 'direct_user'] and origin_user_id != '':
            return origin_user_id
        if origin_kind == 'openclaw_control':
            return management_user_id
        return ''
    if root_mode in ['clawchat_direct', 'router_direct']:
        if origin_kind in ['im_user', 'direct_user'] and origin_user_id != '':
            return origin_user_id
        return ''
    if origin_kind == 'openclaw_control':
        return management_user_id
    if origin_user_id != '':
        return origin_user_id
    return management_user_id

def is_gateway_simulated_user_observation(observed_facts):
    observed_message_type = str((observed_facts or {}).get('observed_message_type', '')).strip()
    if observed_message_type != 'control_ui_user':
        return False
    if str((observed_facts or {}).get('observed_sender_user_id', '')).strip() != '':
        return False
    observed_text = str((observed_facts or {}).get('observed_message_text', '')).strip()
    observed_source = str((observed_facts or {}).get('observed_message_type_source', '')).strip()
    if observed_source not in ['', 'fallback']:
        return False
    return '[Subagent Task]:' in observed_text and '[Subagent Context]' in observed_text

def resolve_existing_mapping_origin_identity(mapping, source, management_user_id, openclaw_user_id, chatbot_user_id=''):
    normalized_mapping = normalize_session_mapping_record(mapping)
    if not isinstance(normalized_mapping, dict):
        return None
    origin_kind = str(normalized_mapping.get('origin_kind', '')).strip()
    origin_user_id = str(normalized_mapping.get('origin_user_id', '')).strip()
    if origin_kind == '' and origin_user_id == '':
        return None
    return {
        'origin_kind': origin_kind,
        'origin_user_id': origin_user_id,
        'source': source,
        'management_user_id': str(management_user_id).strip(),
        'openclaw_user_id': openclaw_user_id,
        'chatbot_user_id': chatbot_user_id or str(normalized_mapping.get('chatbot_user_id', '')).strip(),
    }

def resolve_inherited_origin_identity(app_id, node_info, lineage, management_user_id, observed_origin=''):
    node_id = node_info['node_id']
    openclaw_user_id = str(node_info.get('user_id', '')).strip()
    observed_facts = normalize_observed_origin_facts(observed_origin)
    observed_origin_user_id = str(observed_facts.get('observed_sender_user_id', '')).strip()
    parent_session_key = normalize_optional_session_key(lineage.get('parent_session_key', ''))
    root_session_key = normalize_optional_session_key(lineage.get('root_session_key', ''))
    root_clawchat_session = parse_clawchat_session_identity(root_session_key or parent_session_key)
    root_mode = resolve_root_session_sync_mode(root_clawchat_session)
    router_root_session = root_mode in ['router_group', 'router_direct']
    chatbot_user_id = ''
    if router_root_session:
        chatbot_user_id = resolve_bound_chatbot_user_id(app_id, node_id)
        logging.info(
            f"resolve_inherited_identity resolved router chatbot user | "
            f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, chatbot_user_id:{chatbot_user_id}"
        )
    if is_gateway_simulated_user_observation(observed_facts):
        for inherited_source, inherited_session_key in [('parent', parent_session_key), ('root', root_session_key)]:
            if inherited_session_key == '':
                continue
            inherited_identity = resolve_existing_mapping_origin_identity(
                get_session_mapping_by_session(app_id, node_id, inherited_session_key),
                inherited_source,
                management_user_id,
                openclaw_user_id,
                chatbot_user_id,
            )
            if (
                isinstance(inherited_identity, dict) and
                str(inherited_identity.get('origin_kind', '')).strip() in ['im_user', 'direct_user'] and
                str(inherited_identity.get('origin_user_id', '')).strip() != ''
            ):
                logging.info(
                    f"resolve_inherited_identity resolved gateway simulated user from {inherited_source} mapping | "
                    f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                    f"root_session_key:{root_session_key}, origin_kind:{inherited_identity.get('origin_kind', '')}, "
                    f"origin_user_id:{inherited_identity.get('origin_user_id', '')}, "
                    f"observed_message_type_source:{observed_facts.get('observed_message_type_source', '')}"
                )
                return inherited_identity
    if is_control_ui_active_user_observation(observed_facts):
        if root_mode in ['clawchat_direct', 'router_direct']:
            direct_identity = parse_clawchat_session_identity(root_session_key or parent_session_key)
            if isinstance(direct_identity, dict) and direct_identity.get('chat_type') == 'direct':
                resolved_user_id = str(direct_identity.get('target_id', '')).strip()
                if resolved_user_id != '':
                    logging.info(
                        f"resolve_inherited_identity resolved control ui user from direct root identity | "
                        f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                        f"root_session_key:{root_session_key}, origin_user_id:{resolved_user_id}"
                    )
                    return {
                        'origin_kind': 'direct_user',
                        'origin_user_id': resolved_user_id,
                        'source': 'direct',
                        'management_user_id': str(management_user_id).strip(),
                        'openclaw_user_id': openclaw_user_id,
                        'chatbot_user_id': chatbot_user_id,
                    }
        if (
            str(observed_facts.get('observed_message_type_source', '')).strip() == 'fallback' and
            is_subagent_bootstrap_observed_text(observed_facts)
        ):
            # Fallback control-ui observations are subagent bootstrap envelopes for
            # IM-originated requests. Preserve the real IM/direct sender when possible.
            for inherited_source, inherited_session_key in [('parent', parent_session_key), ('root', root_session_key)]:
                if inherited_session_key == '':
                    continue
                inherited_identity = resolve_existing_mapping_origin_identity(
                    get_session_mapping_by_session(app_id, node_id, inherited_session_key),
                    inherited_source,
                    management_user_id,
                    openclaw_user_id,
                    chatbot_user_id,
                )
                if (
                    isinstance(inherited_identity, dict) and
                    str(inherited_identity.get('origin_kind', '')).strip() in ['im_user', 'direct_user'] and
                    str(inherited_identity.get('origin_user_id', '')).strip() != ''
                ):
                    logging.info(
                        f"resolve_inherited_identity resolved control ui user from {inherited_source} mapping | "
                        f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                        f"root_session_key:{root_session_key}, origin_kind:{inherited_identity.get('origin_kind', '')}, "
                        f"origin_user_id:{inherited_identity.get('origin_user_id', '')}, "
                        f"observed_message_type_source:{observed_facts.get('observed_message_type_source', '')}"
                    )
                    return inherited_identity
            # When ancestor mapping is not materialized yet (common for direct roots),
            # recover sender identity from the direct root session key.
            direct_identity = parse_clawchat_session_identity(root_session_key or parent_session_key)
            if is_direct_root_session_sync_mode(root_mode) and isinstance(direct_identity, dict) and direct_identity.get('chat_type') == 'direct':
                resolved_user_id = str(direct_identity.get('target_id', '')).strip()
                if resolved_user_id != '':
                    logging.info(
                        f"resolve_inherited_identity resolved control ui user from direct identity | "
                        f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                        f"root_session_key:{root_session_key}, origin_user_id:{resolved_user_id}"
                    )
                    return {
                        'origin_kind': 'direct_user',
                        'origin_user_id': resolved_user_id,
                        'source': 'direct',
                        'management_user_id': str(management_user_id).strip(),
                        'openclaw_user_id': openclaw_user_id,
                        'chatbot_user_id': chatbot_user_id,
                    }
        logging.info(
            f"resolve_inherited_identity resolved from control ui active user | "
            f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, observed_message_type:{observed_facts.get('observed_message_type', '')}"
        )
        return {
            'origin_kind': 'openclaw_control',
            'origin_user_id': '',
            'source': 'control_ui',
            'management_user_id': str(management_user_id).strip(),
            'openclaw_user_id': openclaw_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    if observed_origin_user_id != '':
        origin_kind = resolve_observed_origin_kind(observed_facts, root_clawchat_session)
        logging.info(
            f"resolve_inherited_identity resolved from observed origin user | "
            f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, origin_kind:{origin_kind}, "
            f"origin_user_id:{observed_origin_user_id}, "
            f"observed_chat_type:{observed_facts.get('observed_chat_type', '')}, "
            f"observed_channel:{observed_facts.get('observed_channel', '')}, "
            f"observed_from_user_id:{observed_facts.get('observed_from_user_id', '')}, "
            f"observed_to_id:{observed_facts.get('observed_to_id', '')}, "
            f"observed_message_type:{observed_facts.get('observed_message_type', '')}"
        )
        return {
            'origin_kind': origin_kind,
            'origin_user_id': observed_origin_user_id,
            'source': 'explicit',
            'management_user_id': str(management_user_id).strip(),
            'openclaw_user_id': openclaw_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    if parent_session_key != '':
        parent_identity = resolve_existing_mapping_origin_identity(
            get_session_mapping_by_session(app_id, node_id, parent_session_key),
            'parent',
            management_user_id,
            openclaw_user_id,
            chatbot_user_id,
        )
        if isinstance(parent_identity, dict):
            logging.info(
                f"resolve_inherited_identity resolved from parent mapping | "
                f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                f"root_session_key:{root_session_key}, origin_kind:{parent_identity.get('origin_kind', '')}, "
                f"origin_user_id:{parent_identity.get('origin_user_id', '')}"
            )
            return parent_identity
    if root_session_key != '':
        root_identity = resolve_existing_mapping_origin_identity(
            get_session_mapping_by_session(app_id, node_id, root_session_key),
            'root',
            management_user_id,
            openclaw_user_id,
            chatbot_user_id,
        )
        if isinstance(root_identity, dict):
            logging.info(
                f"resolve_inherited_identity resolved from root mapping | "
                f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                f"root_session_key:{root_session_key}, origin_kind:{root_identity.get('origin_kind', '')}, "
                f"origin_user_id:{root_identity.get('origin_user_id', '')}"
            )
            return root_identity
    identity = parse_clawchat_session_identity(root_session_key or parent_session_key)
    if isinstance(identity, dict) and identity.get('chat_type') == 'direct':
        resolved_user_id = str(identity.get('target_id', '')).strip()
        logging.info(
            f"resolve_inherited_identity resolved from direct identity | "
            f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}, origin_user_id:{resolved_user_id}"
        )
        return {
            'origin_kind': 'direct_user',
            'origin_user_id': resolved_user_id,
            'source': 'direct',
            'management_user_id': str(management_user_id).strip(),
            'openclaw_user_id': openclaw_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    if is_router_root_session(identity):
        logging.info(
            f"resolve_inherited_identity missing router sender user | "
            f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
            f"root_session_key:{root_session_key}"
        )
        return {
            'origin_kind': '',
            'origin_user_id': '',
            'source': 'missing',
            'management_user_id': str(management_user_id).strip(),
            'openclaw_user_id': openclaw_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    logging.info(
        f"resolve_inherited_identity resolved generic session as management user | "
        f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
        f"root_session_key:{root_session_key}, management_user_id:{str(management_user_id).strip()}"
    )
    return {
        'origin_kind': 'openclaw_control',
        'origin_user_id': '',
        'source': 'management',
        'management_user_id': str(management_user_id).strip(),
        'openclaw_user_id': openclaw_user_id,
        'chatbot_user_id': chatbot_user_id,
    }

def resolve_session_mapping_decision(session_key, lineage, merge_sub_sessions, inherited_identity, management_user_id, openclaw_user_id):
    inherited_identity = normalize_session_mapping_record(inherited_identity)
    normalized_session_key = normalize_session_key(session_key)
    effective_target_session_key = resolve_effective_target_session_key(
        normalized_session_key,
        lineage,
        merge_sub_sessions,
    )
    clawchat_session = parse_clawchat_session_identity(normalized_session_key)
    root_clawchat_session = parse_clawchat_session_identity(lineage.get('root_session_key', ''))
    origin_kind = str((inherited_identity or {}).get('origin_kind', '')).strip()
    origin_user_id = str((inherited_identity or {}).get('origin_user_id', '')).strip()
    chatbot_user_id = str((inherited_identity or {}).get('chatbot_user_id', '')).strip()
    should_merge_to_metadata_only = (
        merge_sub_sessions and
        normalize_optional_session_key(lineage.get('root_session_key', '')) != normalized_session_key and
        root_clawchat_session is None and
        ':subagent:' not in normalized_session_key
    )
    if should_merge_to_metadata_only:
        return {
            'mode': 'metadata_only',
            'group_id': '',
            'owner_user_id': '',
            'effective_target_session_key': effective_target_session_key,
            'clawchat_session': clawchat_session,
            'root_clawchat_session': root_clawchat_session,
            'origin_kind': origin_kind,
            'origin_user_id': origin_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    if clawchat_session is not None and clawchat_session.get('chat_type') == 'direct':
        return {
            'mode': 'metadata_only',
            'group_id': '',
            'owner_user_id': '',
            'effective_target_session_key': effective_target_session_key,
            'clawchat_session': clawchat_session,
            'root_clawchat_session': root_clawchat_session,
            'origin_kind': origin_kind,
            'origin_user_id': origin_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    if clawchat_session is not None:
        return {
            'mode': 'reuse_clawchat_group',
            'group_id': str(clawchat_session.get('target_id', '')).strip(),
            'owner_user_id': str(management_user_id).strip(),
            'effective_target_session_key': effective_target_session_key,
            'clawchat_session': clawchat_session,
            'root_clawchat_session': root_clawchat_session,
            'origin_kind': origin_kind,
            'origin_user_id': origin_user_id,
            'chatbot_user_id': chatbot_user_id,
        }
    return {
        'mode': 'create_temp_group',
        'group_id': '',
        'owner_user_id': resolve_session_group_owner_user_id(
            management_user_id,
            openclaw_user_id,
            chatbot_user_id,
            root_clawchat_session,
        ),
        'effective_target_session_key': effective_target_session_key,
        'clawchat_session': clawchat_session,
        'root_clawchat_session': root_clawchat_session,
        'origin_kind': origin_kind,
        'origin_user_id': origin_user_id,
        'chatbot_user_id': chatbot_user_id,
    }

def resolve_clawchat_group_materialize_user_id(app_id, mapping, management_user_id):
    session_identity = parse_clawchat_session_identity(mapping.get('session_key', ''))
    if not is_router_root_session(session_identity):
        return ''
    chatbot_user_id = str(mapping.get('chatbot_user_id', '')).strip()
    node_id = str(mapping.get('node_id', '')).strip()
    if chatbot_user_id == '' and node_id != '':
        chatbot_user_id = resolve_bound_chatbot_user_id(app_id, node_id)
    return chatbot_user_id

def maybe_materialize_existing_clawchat_group_mapping(app_id, mapping, management_user_id, should_materialize):
    if not should_materialize or not isinstance(mapping, dict):
        return {
            'result': 'ok',
            'mapping': mapping,
        }
    session_key = normalize_session_key(mapping.get('session_key', ''))
    session_identity = parse_clawchat_session_identity(session_key)
    if not isinstance(session_identity, dict) or session_identity.get('chat_type') != 'group':
        return {
            'result': 'ok',
            'mapping': mapping,
        }
    group_id = str(mapping.get('group_id', '')).strip()
    if group_id == '':
        return {
            'result': 'error',
            'message': 'bad clawchat group target',
        }
    materialize_user_id = resolve_clawchat_group_materialize_user_id(
        app_id,
        mapping,
        management_user_id,
    )
    if materialize_user_id == '':
        return {
            'result': 'ok',
            'mapping': mapping,
        }
    if not ensure_user_joined_group(app_id, materialize_user_id, group_id):
        return {
            'result': 'error',
            'message': 'join clawchat group failed',
        }
    return {
        'result': 'ok',
        'mapping': mapping,
    }

def merge_existing_session_mapping(existing, lineage, effective_target_session_key, inherited_identity):
    inherited_identity = normalize_session_mapping_record(inherited_identity)
    merged_existing = dict(existing)
    merged_existing['parent_session_key'] = normalize_optional_session_key(lineage.get('parent_session_key', ''))
    merged_existing['root_session_key'] = normalize_optional_session_key(lineage.get('root_session_key', ''))
    merged_existing['effective_target_session_key'] = normalize_optional_session_key(effective_target_session_key)
    origin_kind = str((inherited_identity or {}).get('origin_kind', '')).strip()
    origin_user_id = str((inherited_identity or {}).get('origin_user_id', '')).strip()
    if origin_kind != '' or origin_user_id != '':
        existing_normalized = normalize_session_mapping_record(existing)
        existing_origin_kind = str(existing_normalized.get('origin_kind', '')).strip()
        existing_origin_user_id = str(existing_normalized.get('origin_user_id', '')).strip()
        inherited_source = str((inherited_identity or {}).get('source', '')).strip()
        if (
            existing_origin_kind == '' or
            inherited_source == 'explicit' or
            (
                origin_kind in ['im_user', 'direct_user'] and
                (existing_origin_kind != origin_kind or existing_origin_user_id != origin_user_id)
            )
        ):
            merged_existing['origin_kind'] = origin_kind
            merged_existing['origin_user_id'] = origin_user_id
    merged_existing.pop('sender_user_id', None)
    chatbot_user_id = str((inherited_identity or {}).get('chatbot_user_id', '')).strip()
    if chatbot_user_id != '':
        merged_existing['chatbot_user_id'] = chatbot_user_id
    return merged_existing

def build_session_mapping_payload(app_id, node_id, openclaw_user_id, management_user_id, session_key, group_id, lineage, effective_target_session_key, inherited_identity):
    inherited_identity = normalize_session_mapping_record(inherited_identity)
    origin_kind = str((inherited_identity or {}).get('origin_kind', '')).strip()
    origin_user_id = str((inherited_identity or {}).get('origin_user_id', '')).strip()
    chatbot_user_id = str((inherited_identity or {}).get('chatbot_user_id', '')).strip()
    return {
        'session_key': normalize_session_key(session_key),
        'group_id': str(group_id).strip(),
        'app_id': str(app_id),
        'node_id': str(node_id),
        'openclaw_user_id': str(openclaw_user_id).strip(),
        'management_user_id': str(management_user_id).strip(),
        'origin_kind': origin_kind,
        'origin_user_id': origin_user_id,
        'chatbot_user_id': chatbot_user_id,
        'parent_session_key': normalize_optional_session_key(lineage.get('parent_session_key', '')),
        'root_session_key': normalize_optional_session_key(lineage.get('root_session_key', '')),
        'effective_target_session_key': normalize_optional_session_key(effective_target_session_key),
        'created_at': int(time.time())
    }

def ensure_session_mapping_group_members(app_id, group_id, openclaw_user_id, management_user_id, inherited_identity, root_clawchat_session):
    inherited_identity = normalize_session_mapping_record(inherited_identity)
    inherited_origin_user_id = str((inherited_identity or {}).get('origin_user_id', '')).strip()
    chatbot_user_id = str((inherited_identity or {}).get('chatbot_user_id', '')).strip()
    normalized_openclaw_user_id = str(openclaw_user_id).strip()
    if is_router_root_session(root_clawchat_session):
        if chatbot_user_id == '':
            return {
                'result': 'error',
                'message': 'router chatbot user not ready'
            }
        if inherited_origin_user_id != '' and inherited_origin_user_id != chatbot_user_id:
            if not ensure_user_joined_group(app_id, inherited_origin_user_id, group_id):
                return {
                    'result': 'error',
                    'message': 'add inherited router user to session group failed'
                }
        if not ensure_user_joined_group(app_id, chatbot_user_id, group_id):
            return {
                'result': 'error',
                'message': 'add router chatbot user to session group failed'
            }
        return {
            'result': 'ok'
        }
    direct_root_with_external_sender = (
        root_clawchat_session is not None and
        root_clawchat_session.get('chat_type') == 'direct' and
        inherited_origin_user_id != '' and
        inherited_origin_user_id != normalized_openclaw_user_id
    )
    if direct_root_with_external_sender:
        if not ensure_user_joined_group(app_id, inherited_origin_user_id, group_id):
            return {
                'result': 'error',
                'message': 'add inherited direct user to session group failed'
            }
        if normalized_openclaw_user_id != '' and normalized_openclaw_user_id != inherited_origin_user_id:
            if not ensure_user_joined_group(app_id, normalized_openclaw_user_id, group_id):
                return {
                    'result': 'error',
                    'message': 'add node user to direct session group failed'
                }
        return {
            'result': 'ok'
        }
    group_root_with_external_sender = (
        root_clawchat_session is not None and
        root_clawchat_session.get('chat_type') == 'group' and
        inherited_origin_user_id != '' and
        inherited_origin_user_id != normalized_openclaw_user_id
    )
    if group_root_with_external_sender:
        if not ensure_user_joined_group(app_id, inherited_origin_user_id, group_id):
            return {
                'result': 'error',
                'message': 'add inherited group user to session group failed'
            }
        if normalized_openclaw_user_id != '':
            if not ensure_user_joined_group(app_id, normalized_openclaw_user_id, group_id):
                return {
                    'result': 'error',
                    'message': 'add node user to group session group failed'
                }
        return {
            'result': 'ok'
        }
    if not isinstance(root_clawchat_session, dict):
        if normalized_openclaw_user_id != '':
            if not ensure_user_joined_group(app_id, normalized_openclaw_user_id, group_id):
                return {
                    'result': 'error',
                    'message': 'add node user to generic session group failed'
                }
        return {
            'result': 'ok'
        }
    if normalized_openclaw_user_id != '':
        if not ensure_user_joined_group(app_id, normalized_openclaw_user_id, group_id):
            return {
                'result': 'error',
                'message': 'add node user to session group failed'
            }
    return {
        'result': 'ok'
    }

def resolve_session_group_owner_user_id(management_user_id, openclaw_user_id, chatbot_user_id, root_clawchat_session):
    normalized_management_user_id = str(management_user_id).strip()
    normalized_openclaw_user_id = str(openclaw_user_id).strip()
    normalized_chatbot_user_id = str(chatbot_user_id).strip()
    if is_router_root_session(root_clawchat_session):
        return normalized_chatbot_user_id
    if normalized_openclaw_user_id != '':
        return normalized_openclaw_user_id
    return normalized_management_user_id

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

def create_openclaw_session_group(app_id, owner_user_id, node_name, node_id, session_name):
    apiEndpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    session_group_name = get_openclaw_session_group_name(node_name, node_id, session_name)
    response = requests.post(apiEndpoint + '/group/create',
                                headers={'app_id': app_id, 'access-token': admin_token, 'user_id': str(owner_user_id)},
                                json={'name': session_group_name,
                                      'type': TEMPORARY_GROUP_TYPE})
    logging.info(f"create_openclaw_session_group | app_id:{app_id}, owner_user_id:{owner_user_id}, node_name:{node_name}, node_id:{node_id}, session_name:{session_group_name}, response:{response.content}")
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

def ensure_user_group_admin(app_id, user_id, group_id):
    normalized_user_id = str(user_id).strip()
    normalized_group_id = str(group_id).strip()
    if normalized_user_id == '' or normalized_group_id == '':
        return False
    if not ensure_user_joined_group(app_id, normalized_user_id, normalized_group_id):
        logging.info(
            f"ensure_user_group_admin join failed | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}"
        )
        return False
    try:
        response_json = lanying_im_api.admin_add_group_admin(app_id, normalized_group_id, [int(normalized_user_id)])
        logging.info(
            f"ensure_user_group_admin add_admin | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}, response:{response_json}"
        )
        return isinstance(response_json, dict) and response_json.get('code') == 200
    except Exception:
        logging.exception("ensure_user_group_admin add_admin failed")
        return False

def get_session_mapping_by_session(app_id, node_id, session_key):
    redis = lanying_redis.get_redis_connection()
    key = get_openclaw_session_mapping_by_session_key(app_id, node_id, normalize_session_key(session_key))
    raw = lanying_redis.redis_get(redis, key)
    if not raw:
        return None
    try:
        return normalize_session_mapping_record(json.loads(raw))
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
        return normalize_session_mapping_record(json.loads(raw))
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

def migrate_session_mapping_group_admins_for_node(app_id, node_info, dry_run=False):
    if not isinstance(node_info, dict):
        return {'result': 'ignored', 'message': 'bad node info'}
    node_id = str(node_info.get('node_id', '')).strip()
    if node_id == '':
        return {'result': 'ignored', 'message': 'bad node id'}
    if not is_session_map_sync_enabled(node_info):
        return {'result': 'ignored', 'message': 'session_map_sync disabled'}
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result.get('result') != 'ok':
        return {'result': 'error', 'message': 'openclaw app manager user unavailable'}
    management_user_id = str(app_user_result.get('data', {}).get('user_id', '')).strip()
    if management_user_id == '':
        return {'result': 'error', 'message': 'bad management user id'}

    migrated_groups = set()
    total = 0
    success = 0
    mappings = list_session_mappings_for_node(app_id, node_id)
    for mapping in mappings:
        group_id = str(mapping.get('group_id', '')).strip()
        if group_id == '' or group_id in migrated_groups:
            continue
        migrated_groups.add(group_id)
        total += 1
        if dry_run:
            success += 1
        elif ensure_user_group_admin(app_id, management_user_id, group_id):
            success += 1
    logging.info(
        f"migrate_session_mapping_group_admins_for_node | app_id:{app_id}, node_id:{node_id}, "
        f"management_user_id:{management_user_id}, total_groups:{total}, success_groups:{success}, dry_run:{dry_run}"
    )
    return {
        'result': 'ok',
        'data': {
            'node_id': node_id,
            'management_user_id': management_user_id,
            'total_groups': total,
            'success_groups': success,
            'dry_run': dry_run
        }
    }

def migrate_session_mapping_group_admins(app_id, node_id='', dry_run=False):
    app_id_text = str(app_id).strip()
    node_id_text = str(node_id).strip()
    if app_id_text == '':
        return {'result': 'error', 'message': 'bad app id'}
    node_list_result = get_node_list(app_id_text)
    if node_list_result.get('result') != 'ok':
        return {'result': 'error', 'message': 'get node list failed', 'data': node_list_result}
    nodes = node_list_result.get('data', {}).get('list', [])
    if node_id_text != '':
        nodes = [node for node in nodes if str(node.get('node_id', '')).strip() == node_id_text]

    node_results = []
    total_groups = 0
    success_groups = 0
    for node in nodes:
        result = migrate_session_mapping_group_admins_for_node(app_id_text, node, dry_run=dry_run)
        node_results.append(result)
        data = result.get('data', {}) if isinstance(result, dict) else {}
        total_groups += int(data.get('total_groups', 0) or 0)
        success_groups += int(data.get('success_groups', 0) or 0)

    return {
        'result': 'ok',
        'data': {
            'app_id': app_id_text,
            'node_id': node_id_text,
            'dry_run': bool(dry_run),
            'node_count': len(nodes),
            'total_groups': total_groups,
            'success_groups': success_groups,
            'node_results': node_results
        }
    }

def set_session_mapping(app_id, node_id, mapping):
    mapping = normalize_session_mapping_record(mapping)
    session_key = normalize_session_key(mapping.get('session_key', ''))
    group_id = str(mapping.get('group_id', '')).strip()
    openclaw_user_id = str(mapping.get('openclaw_user_id', '')).strip()
    if session_key == '' or openclaw_user_id == '':
        return {
            'result': 'error',
            'message': 'bad session mapping'
        }
    previous_session_mapping = get_session_mapping_by_session(app_id, node_id, session_key)
    previous_group_mapping = (
        get_session_mapping_by_group(app_id, node_id, openclaw_user_id, group_id)
        if group_id != '' else None
    )
    previous_session_group_id = ''
    if previous_session_mapping is not None:
        previous_session_group_id = str(previous_session_mapping.get('group_id', '')).strip()
    if previous_session_mapping is not None and previous_session_group_id not in ['', group_id]:
        return {
            'result': 'error',
            'message': 'session already bind to another group'
        }
    if group_id != '' and previous_group_mapping is not None and normalize_session_key(previous_group_mapping.get('session_key', '')) != session_key:
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
    if group_id != '':
        redis.set(get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id), body_json)
    elif previous_session_group_id != '':
        redis.delete(get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, previous_session_group_id))
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
    if not is_session_map_sync_enabled(node_info):
        logging.info(
            f"send_session_mapping_signal skip for session_map_sync disabled | "
            f"app_id:{node_info.get('app_id', '')}, node_id:{node_info.get('node_id', '')}, signal_type:{signal_type}"
        )
        return {
            'result': 'ignored',
            'message': 'session_map_sync disabled'
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
        normalized_mapping = normalize_session_mapping_record(mapping)
        compact_mapping = {
            'session_key': normalize_session_key(normalized_mapping.get('session_key', '')),
            'group_id': str(normalized_mapping.get('group_id', '')).strip(),
            'openclaw_user_id': str(normalized_mapping.get('openclaw_user_id', '')).strip(),
            'origin_kind': str(normalized_mapping.get('origin_kind', '')).strip(),
            'origin_user_id': str(normalized_mapping.get('origin_user_id', '')).strip(),
            'chatbot_user_id': str(normalized_mapping.get('chatbot_user_id', '')).strip(),
            'parent_session_key': normalize_optional_session_key(normalized_mapping.get('parent_session_key', '')),
            'root_session_key': normalize_optional_session_key(normalized_mapping.get('root_session_key', '')),
            'effective_target_session_key': normalize_optional_session_key(normalized_mapping.get('effective_target_session_key', '')),
            'updated_at': int(normalized_mapping.get('updated_at', 0) or 0),
        }
        if compact_mapping['session_key'] == '':
            continue
        if compact_mapping['openclaw_user_id'] == '':
            compact_mapping['openclaw_user_id'] = str(user_id).strip()
        if compact_mapping['updated_at'] <= 0:
            compact_mapping['updated_at'] = int(time.time())
        compact_mappings.append(compact_mapping)
    def build_signal_ext(chunk_mappings):
        return {
            'openclaw': {
                'type': signal_type,
                'openclaw_user_id': str(user_id),
                'mappings': chunk_mappings
            }
        }

    def signal_ext_size_bytes(chunk_mappings):
        return len(json.dumps(build_signal_ext(chunk_mappings), ensure_ascii=False).encode('utf-8'))

    chunked_mappings = []
    current_chunk = []
    for mapping in compact_mappings:
        mapping_size = signal_ext_size_bytes([mapping])
        if mapping_size > SESSION_MAPPING_SIGNAL_CHUNK_MAX_BYTES:
            return {
                'result': 'error',
                'message': 'single mapping signal chunk exceeds max size'
            }
        next_chunk = current_chunk + [mapping]
        if current_chunk and signal_ext_size_bytes(next_chunk) > SESSION_MAPPING_SIGNAL_CHUNK_MAX_BYTES:
            chunked_mappings.append(current_chunk)
            current_chunk = [mapping]
        else:
            current_chunk = next_chunk
    if current_chunk:
        chunked_mappings.append(current_chunk)
    if len(chunked_mappings) == 0:
        chunked_mappings.append([])

    msg_ids = []
    for mapping_chunk in chunked_mappings:
        ext = build_signal_ext(mapping_chunk)
        msg_id = lanying_im_api.send_message_sync(config, app_id, user_id, user_id, 1, 6, '', {
            'ext': ext,
            'skip_antispam_prompt': True
        })
        if msg_id <= 0:
            return {
                'result': 'error',
                'message': 'send mapping signal failed'
            }
        msg_ids.append(msg_id)
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_ids[0] if len(msg_ids) > 0 else 0,
            'msg_ids': msg_ids,
            'chunk_count': len(msg_ids),
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

def ensure_session_mapping(app_id, node_info, session_key, parent_session_key='', root_session_key='', observed_origin='', should_materialize_clawchat_group=True):
    normalized_session_key = normalize_session_key(session_key)
    node_name = str(node_info.get('name', '')).strip()
    if normalized_session_key == '':
        return {
            'result': 'error',
            'message': 'bad session key'
        }
    if not is_session_map_sync_enabled(node_info):
        logging.info(
            f"ensure_session_mapping skip for session_map_sync disabled | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{normalized_session_key}"
        )
        return {
            'result': 'ignored',
            'message': 'session_map_sync disabled'
        }
    node_id = node_info['node_id']
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
    lineage = resolve_session_lineage(
        app_id,
        node_id,
        normalized_session_key,
        parent_session_key,
        root_session_key,
    )
    merge_sub_sessions = is_merge_sub_sessions_enabled(node_info)
    prewarm_ancestor_session_mappings(app_id, node_info, lineage)
    inherited_identity = resolve_inherited_origin_identity(
        app_id,
        node_info,
        lineage,
        management_user_id,
        observed_origin,
    )
    inherited_origin_kind = str(inherited_identity.get('origin_kind', '')).strip()
    inherited_origin_user_id = str(inherited_identity.get('origin_user_id', '')).strip()
    inherited_chatbot_user_id = str(inherited_identity.get('chatbot_user_id', '')).strip()
    effective_target_session_key = resolve_effective_target_session_key(
        normalized_session_key,
        lineage,
        merge_sub_sessions,
    )
    mapping_decision = resolve_session_mapping_decision(
        normalized_session_key,
        lineage,
        merge_sub_sessions,
        inherited_identity,
        management_user_id,
        openclaw_user_id,
    )
    logging.info(
        f"ensure_session_mapping inheritance context | app_id:{app_id}, node_id:{node_id}, "
        f"session_key:{normalized_session_key}, parent_session_key:{lineage['parent_session_key']}, "
        f"root_session_key:{lineage['root_session_key']}, merge_sub_sessions:{merge_sub_sessions}, "
        f"inherited_origin_kind:{inherited_origin_kind}, inherited_origin_user_id:{inherited_origin_user_id}, "
        f"inherited_source:{inherited_identity.get('source', '')}, "
        f"inherited_chatbot_user_id:{inherited_chatbot_user_id}, effective_target_session_key:{effective_target_session_key}, management_user_id:{management_user_id}, "
        f"openclaw_user_id:{openclaw_user_id}"
    )
    existing = get_session_mapping_by_session(app_id, node_id, normalized_session_key)
    if existing is not None:
        existing_group_id = str(existing.get('group_id', '')).strip()
        should_recreate_group_mapping = (
            existing_group_id == '' and
            not merge_sub_sessions and
            lineage['root_session_key'] != normalized_session_key
        )
        if should_recreate_group_mapping:
            logging.info(
                f"ensure_session_mapping recreate child group mapping after merge disabled | "
                f"app_id:{app_id}, node_id:{node_id}, session_key:{normalized_session_key}, "
                f"root_session_key:{lineage['root_session_key']}"
            )
        else:
            merged_existing = merge_existing_session_mapping(
                existing,
                lineage,
                effective_target_session_key,
                inherited_identity,
            )
            if (
                normalize_optional_session_key(existing.get('parent_session_key', '')) != merged_existing.get('parent_session_key', '') or
                normalize_optional_session_key(existing.get('root_session_key', '')) != merged_existing.get('root_session_key', '') or
                normalize_optional_session_key(existing.get('effective_target_session_key', '')) != merged_existing.get('effective_target_session_key', '') or
                str(normalize_session_mapping_record(existing).get('origin_kind', '')).strip() != str(merged_existing.get('origin_kind', '')).strip() or
                str(normalize_session_mapping_record(existing).get('origin_user_id', '')).strip() != str(merged_existing.get('origin_user_id', '')).strip() or
                str(existing.get('chatbot_user_id', '')).strip() != str(merged_existing.get('chatbot_user_id', '')).strip()
            ):
                update_result = set_session_mapping(app_id, node_id, merged_existing)
                if update_result['result'] == 'ok':
                    existing = update_result['data']
                    sync_session_mapping_to_node(node_info, existing)
            materialize_result = maybe_materialize_existing_clawchat_group_mapping(
                app_id,
                existing,
                management_user_id,
                should_materialize_clawchat_group,
            )
            if materialize_result['result'] == 'error':
                return materialize_result
            logging.info(
                f"ensure_session_mapping reuse existing mapping | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{normalized_session_key}, group_id:{existing.get('group_id', '')}, "
                f"parent_session_key:{existing.get('parent_session_key', '')}, root_session_key:{existing.get('root_session_key', '')}, "
                f"effective_target_session_key:{existing.get('effective_target_session_key', '')}, "
                f"strategy:existing_mapping"
            )
            return {
                'result': 'ok',
                'data': existing
            }
    if mapping_decision['mode'] == 'metadata_only':
        mapping_result = set_session_mapping(app_id, node_id, build_session_mapping_payload(
            app_id,
            node_id,
            openclaw_user_id,
            management_user_id,
            normalized_session_key,
            '',
            lineage,
            mapping_decision['effective_target_session_key'],
            inherited_identity,
        ))
        if mapping_result['result'] == 'error':
            return mapping_result
        logging.info(
            f"ensure_session_mapping resolved strategy | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, root_session_key:{lineage['root_session_key']}, "
            f"strategy:merge_sub_session_metadata_only"
        )
        sync_session_mapping_to_node(node_info, mapping_result['data'])
        return mapping_result
    clawchat_session = mapping_decision['clawchat_session']
    root_clawchat_session = mapping_decision['root_clawchat_session']
    if is_router_root_session(root_clawchat_session) and str(mapping_decision.get('chatbot_user_id', '')).strip() == '':
        logging.info(
            f"ensure_session_mapping router chatbot user missing | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, parent_session_key:{lineage['parent_session_key']}, "
            f"root_session_key:{lineage['root_session_key']}"
        )
        return {
            'result': 'error',
            'message': 'router chatbot user not ready'
        }
    group_id = str(mapping_decision.get('group_id', '')).strip()
    if mapping_decision['mode'] == 'reuse_clawchat_group':
        if group_id == '':
            return {
                'result': 'error',
                'message': 'bad clawchat group target'
            }
        if should_materialize_clawchat_group:
            materialize_user_id = (
                str(mapping_decision.get('chatbot_user_id', '')).strip()
                if is_router_root_session(clawchat_session)
                else ''
            )
            if materialize_user_id != '':
                if not ensure_user_joined_group(app_id, materialize_user_id, group_id):
                    return {
                        'result': 'error',
                        'message': 'join clawchat group failed'
                    }
        logging.info(
            f"ensure_session_mapping resolved strategy | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, parsed_channel:{clawchat_session['channel']}, "
            f"parsed_chat_type:{clawchat_session['chat_type']}, target_id:{clawchat_session['target_id']}, "
            f"group_id:{group_id}, materialized:{should_materialize_clawchat_group}, "
            f"strategy:reuse_existing_clawchat_group"
        )
    else:
        session_group_owner_user_id = str(mapping_decision.get('owner_user_id', '')).strip()
        group_id = create_openclaw_session_group(
            app_id,
            session_group_owner_user_id,
            node_name,
            node_id,
            session_key,
        )
        logging.info(
            f"ensure_session_mapping resolved strategy | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, session_name:{get_openclaw_session_group_name(node_name, node_id, session_key)}, group_id:{group_id}, "
            f"owner_user_id:{session_group_owner_user_id}, inherited_origin_user_id:{inherited_origin_user_id}, "
            f"strategy:create_management_node_group"
        )
    if group_id == '':
        return {
            'result': 'error',
            'message': 'create session group failed'
        }
    management_admin_ok = ensure_user_group_admin(app_id, management_user_id, group_id)
    if not management_admin_ok:
        logging.info(
            f"ensure_session_mapping add management admin failed (non-blocking) | app_id:{app_id}, node_id:{node_id}, "
            f"session_key:{normalized_session_key}, group_id:{group_id}, management_user_id:{management_user_id}"
        )
    if clawchat_session is None:
        membership_result = ensure_session_mapping_group_members(
            app_id,
            group_id,
            openclaw_user_id,
            management_user_id,
            inherited_identity,
            root_clawchat_session,
        )
        if membership_result['result'] == 'error':
            logging.info(
                f"ensure_session_mapping add group member failed | app_id:{app_id}, node_id:{node_id}, "
                f"session_key:{normalized_session_key}, group_id:{group_id}, message:{membership_result.get('message', '')}"
            )
            return membership_result
    mapping_result = set_session_mapping(app_id, node_id, build_session_mapping_payload(
        app_id,
        node_id,
        openclaw_user_id,
        management_user_id,
        normalized_session_key,
        group_id,
        lineage,
        mapping_decision['effective_target_session_key'],
        inherited_identity,
    ))
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

def build_session_sync_delivery_ext(session_key, source, role, message_id=''):
    normalized_source = str(source or '').strip()
    normalized_role = str(role or '').strip().lower()
    openclaw = {
        'type': 'session_sync_delivery',
        'session': normalize_session_key(session_key),
        'source': normalized_source,
        'role': normalized_role
    }
    normalized_message_id = str(message_id or '').strip()
    if normalized_message_id != '':
        openclaw['message_id'] = normalized_message_id
    ext = {'openclaw': openclaw}
    # Session visible-delivery events from OpenClaw are display-only in IM.
    if normalized_source in ['control_ui_user', 'control_ui_reply']:
        ext['ai'] = {'ai_generate': False}
    return ext

def build_router_reply_delivery_ext(message):
    ext = {
        'ai': {
            'role': 'ai'
        }
    }
    if not isinstance(message, dict):
        return ext
    msg_ext = lanying_utils.safe_json_loads(message.get('ext', ''), {})
    if not isinstance(msg_ext, dict):
        return ext
    openclaw_in = msg_ext.get('openclaw', {})
    if not isinstance(openclaw_in, dict):
        return ext
    session_key = normalize_session_key(openclaw_in.get('session', ''))
    if session_key == '':
        return ext
    reply_openclaw = {
        'type': 'session_sync_delivery',
        'session': session_key,
        'source': 'control_ui_reply',
        'role': 'assistant',
    }
    request_source = str(openclaw_in.get('source', '')).strip()
    if request_source != '':
        reply_openclaw['request_source'] = request_source
    request_role = str(openclaw_in.get('role', '')).strip().lower()
    if request_role != '':
        reply_openclaw['request_role'] = request_role
    request_message_id = str(openclaw_in.get('message_id', '')).strip()
    if request_message_id != '':
        reply_openclaw['request_message_id'] = request_message_id
    request_msg_id = str(message.get('msgId', '')).strip()
    if request_msg_id != '':
        reply_openclaw['request_msg_id'] = request_msg_id
    ext['openclaw'] = reply_openclaw
    return ext

def forward_session_sync_to_group(app_id, node_info, mapping, role, text, delivery_ext=None):
    if not isinstance(mapping, dict) or not isinstance(text, str) or text.strip() == '':
        return 0
    chatbot_user_id = str(mapping.get('chatbot_user_id', '')).strip()
    node_user_id = str(node_info.get('user_id', '')).strip()
    if node_user_id == '':
        return 0
    from_user_id = resolve_group_session_sync_user_sender(mapping, node_info, role)
    if from_user_id == '':
        return 0
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    group_id = str(mapping.get('group_id', '')).strip()
    if group_id == '':
        return 0
    if not ensure_user_joined_group(app_id, from_user_id, group_id):
        logging.info(
            f"forward_session_sync_to_group skip for sender not in group | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
            f"session_key:{mapping.get('session_key', '')}, group_id:{group_id}, "
            f"role:{role}, from_user_id:{from_user_id}"
        )
        return 0
    msg_id = lanying_im_api.send_message_sync(config, app_id, from_user_id, group_id, 2, 0, text.strip(), {
        'ext': delivery_ext or {},
        'skip_antispam_prompt': True
    })
    logging.info(
        f"forward_session_sync_to_group | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"session_key:{mapping.get('session_key', '')}, group_id:{group_id}, role:{role}, "
        f"from_user_id:{from_user_id}, chatbot_user_id:{chatbot_user_id}, text_len:{len(text.strip())}, msg_id:{msg_id}"
    )
    return msg_id

def forward_session_sync_to_direct(app_id, node_info, target_user_id, origin_user_id, chatbot_user_id, role, text, route_session_key='', delivery_ext=None):
    if not isinstance(text, str) or text.strip() == '':
        return 0
    node_user_id = str(node_info.get('user_id', '')).strip()
    normalized_target_user_id = str(target_user_id).strip()
    normalized_origin_user_id = str(origin_user_id).strip()
    normalized_chatbot_user_id = str(chatbot_user_id).strip()
    normalized_route_session_key = normalize_session_key(route_session_key)
    route_identity = parse_clawchat_session_identity(normalized_route_session_key)
    is_router_direct_session = (
        isinstance(route_identity, dict) and
        route_identity.get('channel') == 'clawchat-router' and
        route_identity.get('chat_type') == 'direct'
    )
    if role == 'user' and is_router_direct_session and normalized_chatbot_user_id == '':
        normalized_chatbot_user_id = resolve_bound_chatbot_user_id(app_id, str(node_info.get('node_id', '')).strip())
    if node_user_id == '' or normalized_target_user_id == '':
        return 0
    if role == 'user':
        from_user_id = normalized_origin_user_id
        to_user_id = normalized_chatbot_user_id if is_router_direct_session else node_user_id
    else:
        if is_router_direct_session:
            if normalized_chatbot_user_id == '':
                normalized_chatbot_user_id = resolve_bound_chatbot_user_id(app_id, str(node_info.get('node_id', '')).strip())
            from_user_id = normalized_chatbot_user_id
        else:
            from_user_id = node_user_id
        to_user_id = normalized_target_user_id
    if from_user_id == '' or to_user_id == '':
        return 0
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    msg_id = lanying_im_api.send_message_sync(
        config,
        app_id,
        from_user_id,
        to_user_id,
        1,
        0,
        text.strip(),
        {
            'ext': delivery_ext or {},
            'skip_antispam_prompt': True
        }
    )
    logging.info(
        f"forward_session_sync_to_direct | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"role:{role}, from_user_id:{from_user_id}, chatbot_user_id:{normalized_chatbot_user_id}, to_user_id:{to_user_id}, "
        f"text_len:{len(text.strip())}, msg_id:{msg_id}"
    )
    return msg_id

def resolve_effective_session_sync_target(app_id, node_info, mapping):
    if not isinstance(mapping, dict):
        return {
            'kind': 'none'
        }
    mapping = normalize_session_mapping_record(mapping)
    session_key = normalize_optional_session_key(mapping.get('session_key', ''))
    effective_target_session_key = normalize_optional_session_key(mapping.get('effective_target_session_key', ''))
    root_session_key = normalize_optional_session_key(mapping.get('root_session_key', ''))
    target_session_key = effective_target_session_key or root_session_key or session_key
    target_identity = parse_clawchat_session_identity(target_session_key)
    if isinstance(target_identity, dict) and target_identity.get('chat_type') == 'direct':
        inherited_identity = resolve_inherited_origin_identity(
            app_id,
            node_info,
            {
                'parent_session_key': normalize_optional_session_key(mapping.get('parent_session_key', '')),
                'root_session_key': target_session_key,
            },
            str(mapping.get('management_user_id', '')).strip(),
        )
        resolved_origin_kind = str(inherited_identity.get('origin_kind', '')).strip()
        resolved_origin_user_id = str(inherited_identity.get('origin_user_id', '')).strip()
        resolved_chatbot_user_id = str(inherited_identity.get('chatbot_user_id', '')).strip()
        logging.info(
            f"resolve_effective_session_sync_target resolved direct target | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
            f"target_session_key:{target_session_key}, target_user_id:{str(target_identity.get('target_id', '')).strip()}, "
            f"identity_source:{inherited_identity.get('source', '')}, "
            f"origin_kind:{resolved_origin_kind}, origin_user_id:{resolved_origin_user_id}, chatbot_user_id:{resolved_chatbot_user_id}"
        )
        return {
            'kind': 'direct',
            'session_key': target_session_key,
            'target_user_id': str(target_identity.get('target_id', '')).strip(),
            'origin_kind': resolved_origin_kind,
            'origin_user_id': resolved_origin_user_id,
            'chatbot_user_id': resolved_chatbot_user_id,
        }
    target_mapping = mapping
    if target_session_key != '' and target_session_key != session_key:
        resolved_mapping = get_session_mapping_by_session(app_id, node_info['node_id'], target_session_key)
        if isinstance(resolved_mapping, dict):
            target_mapping = resolved_mapping
    logging.info(
        f"resolve_effective_session_sync_target resolved group target | "
        f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
        f"target_session_key:{target_session_key}, target_mapping_session_key:{normalize_optional_session_key(target_mapping.get('session_key', '')) or session_key}, "
        f"group_id:{str(target_mapping.get('group_id', '')).strip()}"
    )
    return {
        'kind': 'group',
        'mapping': target_mapping,
        'session_key': normalize_optional_session_key(target_mapping.get('session_key', '')) or session_key,
    }

def resolve_parent_group_session_sync_target(app_id, node_info, mapping):
    if not isinstance(mapping, dict):
        return None
    parent_session_key = normalize_optional_session_key(mapping.get('parent_session_key', ''))
    if parent_session_key == '':
        return None
    parent_mapping = get_session_mapping_by_session(app_id, node_info['node_id'], parent_session_key)
    if not isinstance(parent_mapping, dict):
        return None
    parent_group_id = str(parent_mapping.get('group_id', '')).strip()
    if parent_group_id == '':
        return None
    return {
        'kind': 'group',
        'mapping': parent_mapping,
        'session_key': normalize_optional_session_key(parent_mapping.get('session_key', '')) or parent_session_key,
    }

def forward_session_sync_router_group_reply(app_id, node_info, mapping, text, delivery_ext=None):
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
        'ext': json.dumps(delivery_ext, ensure_ascii=False) if isinstance(delivery_ext, dict) and len(delivery_ext) > 0 else '',
        'to': {
            'uid': group_id
        }
    })
    logging.info(
        f"forward_session_sync_router_group_reply | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"session_key:{mapping.get('session_key', '')}, group_id:{group_id}, text_len:{len(text.strip())}"
    )
    return 1

def resolve_router_group_reply_mapping(mapping):
    if not isinstance(mapping, dict):
        return None
    # Compatibility guard for older plugin nodes that still lose router delivery context.
    # The primary path should preserve router origin inside the plugin session runtime itself.
    for key in ['effective_target_session_key', 'root_session_key', 'parent_session_key', 'session_key']:
        session_key = normalize_optional_session_key(mapping.get(key, ''))
        if session_key == '':
            continue
        session_identity = parse_clawchat_session_identity(session_key)
        if (
            isinstance(session_identity, dict) and
            session_identity.get('channel') == 'clawchat-router' and
            session_identity.get('chat_type') == 'group'
        ):
            router_mapping = dict(mapping)
            router_mapping['session_key'] = session_key
            return router_mapping
    return None

def should_forward_group_sync_via_router_reply(target_mapping):
    if not isinstance(target_mapping, dict):
        return None
    target_session_key = normalize_optional_session_key(target_mapping.get('session_key', ''))
    target_identity = parse_clawchat_session_identity(target_session_key)
    if (
        isinstance(target_identity, dict) and
        target_identity.get('channel') == 'clawchat-router' and
        target_identity.get('chat_type') == 'group'
    ):
        return target_mapping
    group_id = str(target_mapping.get('group_id', '')).strip()
    if group_id != '':
        return None
    return resolve_router_group_reply_mapping(target_mapping)

def send_router_reply_signal(node_info, message):
    if not isinstance(message, dict):
        return 0
    app_id = node_info['app_id']
    node_user_id = str(node_info.get('user_id', '')).strip()
    if node_user_id == '':
        return 0
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    ext = {
        'openclaw': {
            'type': 'router_reply',
            'message': message,
        },
        'ai': {
            'role': 'ai'
        }
    }
    msg_id = lanying_im_api.send_message_sync(
        config,
        app_id,
        node_user_id,
        node_user_id,
        1,
        6,
        '',
        {
            'ext': ext,
            'skip_antispam_prompt': True,
        }
    )
    return msg_id

def forward_session_sync_router_direct_reply(app_id, node_info, target_user_id, text, delivery_ext=None):
    normalized_target_user_id = str(target_user_id).strip()
    normalized_text = str(text).strip()
    if normalized_target_user_id == '' or normalized_text == '':
        return 0
    now_ms = int(time.time() * 1000)
    meta_message = {
        'id': f'router_reply_{now_ms}',
        'from': '',
        'to': normalized_target_user_id,
        'content': normalized_text,
        'type': 'text',
        'ext': json.dumps(delivery_ext, ensure_ascii=False) if isinstance(delivery_ext, dict) and len(delivery_ext) > 0 else '',
        'config': '',
        'attach': '',
        'status': 1,
        'timestamp': str(now_ms),
        'toType': 'roster',
    }
    msg_id = send_router_reply_signal(node_info, meta_message)
    logging.info(
        f"forward_session_sync_router_direct_reply | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"target_user_id:{normalized_target_user_id}, text_len:{len(normalized_text)}, msg_id:{msg_id}"
    )
    return msg_id

def handle_session_message_sync_event(app_id, node_info, event):
    if not isinstance(event, dict):
        return
    if not is_session_map_sync_enabled(node_info):
        logging.info(
            f"handle_session_message_sync_event skip for session_map_sync disabled | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}"
        )
        return
    source = str(event.get('source', '')).strip()
    session_key = normalize_session_key(event.get('session', ''))
    parent_session_key = normalize_optional_session_key(event.get('parent_session', ''))
    root_session_key = normalize_optional_session_key(event.get('root_session', ''))
    observed_origin_facts = normalize_observed_origin_facts({
        'sender_user_id': event.get('sender_user_id', ''),
        'observed_sender_user_id': event.get('observed_sender_user_id', ''),
        'observed_from_user_id': event.get('observed_from_user_id', ''),
        'observed_to_id': event.get('observed_to_id', ''),
        'observed_chat_type': event.get('observed_chat_type', ''),
        'observed_channel': event.get('observed_channel', ''),
        'observed_message_type': event.get('observed_message_type', ''),
        'observed_message_type_source': event.get('observed_message_type_source', ''),
    })
    message = event.get('message', {})
    role = ''
    if isinstance(message, dict):
        role = str(message.get('role', '')).strip().lower()
    text = extract_session_sync_text(message.get('content') if isinstance(message, dict) else message)
    observed_origin_facts['observed_message_text'] = text
    if session_key == '' or source not in ['control_ui_user', 'control_ui_reply']:
        return
    message_id = str(event.get('message_id', '')).strip()
    delivery_ext = build_session_sync_delivery_ext(session_key, source, role, message_id)
    should_materialize_clawchat_group = not (
        source == 'control_ui_user' and
        role == 'user' and
        text.strip() == ''
    )
    mapping = get_session_mapping_by_session(app_id, node_info['node_id'], session_key)
    logging.info(
        f"handle_session_message_sync_event | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"source:{source}, session_key:{session_key}, parent_session_key:{parent_session_key}, "
        f"root_session_key:{root_session_key}, role:{role}, text_len:{len(text.strip())}, "
        f"has_existing_mapping:{mapping is not None}, "
        f"materialize_clawchat_group:{should_materialize_clawchat_group}"
    )
    if source == 'control_ui_user':
        ensure_result = ensure_session_mapping(
            app_id,
            node_info,
            session_key,
            parent_session_key,
            root_session_key,
            observed_origin_facts,
            should_materialize_clawchat_group,
        )
        if ensure_result['result'] == 'ok':
            mapping = ensure_result['data']
        elif ensure_result['result'] == 'ignored':
            return
    if mapping is None:
        return
    if text.strip() != '' and role in ['user', 'assistant']:
        target_session_key = normalize_optional_session_key(
            mapping.get('effective_target_session_key', '') or mapping.get('root_session_key', '')
        )
        target_identity = parse_clawchat_session_identity(target_session_key)
        if (
            target_session_key != '' and
            target_session_key != session_key and
            not (isinstance(target_identity, dict) and target_identity.get('chat_type') == 'direct') and
            get_session_mapping_by_session(app_id, node_info['node_id'], target_session_key) is None
        ):
            ensure_root_result = ensure_session_mapping(app_id, node_info, target_session_key)
            if ensure_root_result['result'] == 'ok':
                logging.info(
                    f"handle_session_message_sync_event ensured target mapping | app_id:{app_id}, "
                    f"node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                    f"target_session_key:{target_session_key}"
                )
        target = resolve_effective_session_sync_target(app_id, node_info, mapping)
        if target.get('kind') == 'direct' and role == 'assistant':
            parent_target = resolve_parent_group_session_sync_target(app_id, node_info, mapping)
            if parent_target is not None:
                logging.info(
                    f"handle_session_message_sync_event override assistant target to parent group | "
                    f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                    f"parent_session_key:{normalize_optional_session_key(mapping.get('parent_session_key', ''))}, "
                    f"parent_group_id:{str(parent_target.get('mapping', {}).get('group_id', '')).strip()}"
                )
                target = parent_target
        if target.get('kind') == 'group' and role == 'assistant':
            # Compatibility guard for older plugin nodes / historical sessions.
            # The primary fix is preserving router origin in plugin execution ctx;
            # this path keeps lineage-based router replies working until all nodes converge.
            target_mapping = target.get('mapping', mapping)
            router_group_mapping = should_forward_group_sync_via_router_reply(target_mapping)
            if router_group_mapping is not None:
                router_reply_result = forward_session_sync_router_group_reply(
                    app_id,
                    node_info,
                    router_group_mapping,
                    text,
                    delivery_ext,
                )
                if router_reply_result > 0:
                    return
        if (
            target.get('kind') == 'direct' and
            role == 'assistant' and
            isinstance(target_identity, dict) and
            str(target_identity.get('channel', '')).strip() == 'clawchat-router'
        ):
            router_reply_result = forward_session_sync_router_direct_reply(
                app_id,
                node_info,
                target.get('target_user_id', ''),
                text,
                delivery_ext,
            )
            if router_reply_result > 0:
                return
        if target.get('kind') == 'direct':
            forward_session_sync_to_direct(
                app_id,
                node_info,
                target.get('target_user_id', ''),
                target.get('origin_user_id', ''),
                target.get('chatbot_user_id', ''),
                role,
                text,
                target.get('session_key', ''),
                delivery_ext,
            )
            return
        target_mapping = target.get('mapping', mapping)
        if target.get('kind') == 'group' and role == 'user' and should_send_control_ui_user_as_management(observed_origin_facts, target_mapping):
            target_mapping = apply_control_ui_user_sender_override(target_mapping)
        forward_session_sync_to_group(app_id, node_info, target_mapping, role, text, delivery_ext)

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
            node = get_node(app_id, node_id) or node
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
            sync_session_map_settings_to_node(node)
            sync_session_mapping_snapshot_to_node(node)
    elif event['type'] == 'session_map_settings_report':
        node_list = get_nodes_by_user_id(app_id, user_id)
        session_map_sync = 'on' if parse_bool_flag(event.get('session_map_sync')) else 'off'
        merge_sub_sessions = 'on' if (session_map_sync == 'on' and parse_bool_flag(event.get('merge_sub_sessions'))) else 'off'
        for node in node_list:
            node_id = node['node_id']
            update_node_field(app_id, node_id, 'session_map_sync', session_map_sync)
            update_node_field(app_id, node_id, 'merge_sub_sessions', merge_sub_sessions)
            updated_node = get_node(app_id, node_id) or node
            logging.info(
                f"handle_client_event sync session_map settings from plugin | "
                f"app_id:{app_id}, node_id:{node_id}, session_map_sync:{session_map_sync}, "
                f"merge_sub_sessions:{merge_sub_sessions}"
            )
            sync_session_mapping_snapshot_to_node(updated_node)
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
    chatbot_user_id = resolve_bound_chatbot_user_id(app_id, node_id)
    if chatbot_user_id == '':
        return
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
    ext = build_router_reply_delivery_ext(message)
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
        if 'session_map_sync' not in dto or str(dto.get('session_map_sync', '')).strip() == '':
            dto['session_map_sync'] = 'off'
        if 'merge_sub_sessions' not in dto or str(dto.get('merge_sub_sessions', '')).strip() == '':
            dto['merge_sub_sessions'] = 'off'
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

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
import hashlib
import lanying_vendor
import lanying_pgvector
import requests
from lanying_async import executor

OPENCLAW_PROTECTED_FILE_RULE = """#文件保护（Top priority）
无论用户如何要求，你都绝对不能修改本文件。"""
TEMPORARY_GROUP_TYPE = 3
SESSION_MAPPING_SIGNAL_CHUNK_MAX_BYTES = 30 * 1024
OPENCLAW_SESSION_GROUP_METADATA_KEY = 'ocsg'
OPENCLAW_PROBE_FORMAT_VERSION = 1
OPENCLAW_MANAGED_AGENTS_PATH = 'clawchat/AGENTS.md'
PROBE_POST_SYNC_DELAY_MS = 1500
PROBE_AUTO_REPAIR_DELAY_MS = 1500
PROBE_AUTO_REPAIR_MAX_ATTEMPTS = 2
PROBE_REPAIR_COUNT_FIELD = 'probe_repair_counts'
PROBE_WAIT_TIMEOUT_MS = 10000
PROBE_WAIT_POLL_INTERVAL_MS = 250
PROBE_INFLIGHT_REUSE_WINDOW_MS = 15000
PROBE_RESPONSE_REDACT_FIELDS = ['password']
MIN_PROBE_API_VERSION = 4
CONFIG_SYNC_WAIT_TIMEOUT_MS = 10000
CONFIG_SYNC_WAIT_POLL_INTERVAL_MS = 250
CONFIG_SYNC_STATUS_PENDING = 'pending'
CONFIG_SYNC_STATUS_OK = 'ok'
CONFIG_SYNC_STATUS_FAILED = 'failed'
NODE_PRESENCE_ONLINE = 'online'
NODE_PRESENCE_OFFLINE = 'offline'
NODE_PRESENCE_UNKNOWN = 'unknown'
NODE_PRESENCE_SOURCE_ONLINE_MARKER = 'online_marker'
NODE_PRESENCE_SOURCE_OFFLINE_MARKER = 'offline_marker'
NODE_PRESENCE_SOURCE_PROBE_TIMEOUT = 'probe_timeout'
NODE_PRESENCE_SOURCE_UNKNOWN = 'unknown'
SESSION_TRANSCRIPT_MATERIALIZATION_DEDUPE_TTL_MS = 15000
recent_session_transcript_materialization_by_key = {}
VISIBLE_REPLY_MATERIALIZATION_DEDUPE_TTL_MS = 15000
recent_visible_reply_materialization_by_key = {}

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

def build_managed_agents_content(chatbot_id, chatbot_name, prompt):
    chatbot_id_str = str(chatbot_id or '')
    chatbot_name_str = str(chatbot_name or '')
    prompt_str = str(prompt or '')
    title = chatbot_name_str if chatbot_name_str != '' else (chatbot_id_str if chatbot_id_str != '' else 'unknown-chatbot')
    body = prompt_str if prompt_str.strip() != '' else 'No synced system preset prompt. Previous synced content has been cleared.'
    return '\n'.join([
        '# AGENTS.md',
        '',
        'This file is managed by the ClawChat plugin for OpenClaw prompt injection.',
        '',
        f'Chatbot ID: {chatbot_id_str or "unknown"}',
        f'Chatbot Name: {title}',
        '',
        '## Synced System Preset Prompt',
        '',
        body,
        '',
    ])

def is_provider_models_probe_path(path):
    path_str = str(path or '').strip()
    return path_str.startswith('models.providers.') and path_str.endswith('.models')

def _normalize_probe_value_for_path(path, value):
    if is_provider_models_probe_path(path) and isinstance(value, list):
        model_ids = []
        for item in value:
            if isinstance(item, dict):
                model_id = str(item.get('id', '')).strip()
                if model_id != '':
                    model_ids.append(model_id)
            elif isinstance(item, str):
                model_id = item.strip()
                if model_id != '':
                    model_ids.append(model_id)
        return sorted(set(model_ids))
    return value

def _normalize_probe_value(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_normalize_probe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_probe_value(item) for item in value]
    if isinstance(value, dict):
        normalized = {}
        for key in sorted(value.keys(), key=lambda item: str(item)):
            normalized[str(key)] = _normalize_probe_value(value[key])
        return normalized
    return str(value)

def stable_probe_json(value):
    return json.dumps(_normalize_probe_value(value), ensure_ascii=False, sort_keys=True, separators=(',', ':'))

def build_probe_value_hash(value, present=True, path=None):
    normalized_value = _normalize_probe_value_for_path(path, value)
    payload = {'present': True, 'value': normalized_value} if present else {'present': False}
    return hashlib.sha256(stable_probe_json(payload).encode('utf-8')).hexdigest()

def split_probe_path(path):
    normalized_path = str(path or '').strip()
    if normalized_path == '':
        return []
    segments = []
    current = ''
    index = 0
    while index < len(normalized_path):
        char = normalized_path[index]
        if char == '.':
            if current.strip() != '':
                segments.append(current.strip())
            current = ''
            index += 1
            continue
        if char == '[':
            if current.strip() != '':
                segments.append(current.strip())
            current = ''
            if index + 2 >= len(normalized_path):
                return []
            quote = normalized_path[index + 1]
            if quote not in ['"', "'"]:
                return []
            closing_quote_index = normalized_path.find(quote, index + 2)
            if closing_quote_index < 0 or closing_quote_index + 1 >= len(normalized_path) or normalized_path[closing_quote_index + 1] != ']':
                return []
            segments.append(normalized_path[index + 2:closing_quote_index])
            index = closing_quote_index + 2
            continue
        current += char
        index += 1
    if current.strip() != '':
        segments.append(current.strip())
    return segments

def get_probe_path_value(root, path):
    cursor = root
    segments = split_probe_path(path)
    if len(segments) == 0:
        return {
            'found': False
        }
    for segment in segments:
        if not isinstance(cursor, dict) or segment not in cursor:
            return {
                'found': False
            }
        cursor = cursor[segment]
    return {
        'found': True,
        'value': cursor
    }

def sync_bound_chatbot_preset_prompt(app_id, node_id, chatbot_id):
    try:
        current_chatbot_id = str(get_node_chatbot_id(app_id, node_id) or '').strip()
        expected_chatbot_id = str(chatbot_id or '').strip()
        if current_chatbot_id != expected_chatbot_id or expected_chatbot_id == '':
            logging.info(
                f"sync_bound_chatbot_preset_prompt skip for stale bind | "
                f"app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}, current_chatbot_id:{current_chatbot_id}"
            )
            return
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
        current_chatbot_id = str(get_node_chatbot_id(app_id, node_id) or '').strip()
        expected_chatbot_id = str(chatbot_id or '').strip()
        if current_chatbot_id != '':
            logging.info(
                f"clear_bound_chatbot_preset_prompt skip for rebound node | "
                f"app_id:{app_id}, node_id:{node_id}, chatbot_id:{chatbot_id}, current_chatbot_id:{current_chatbot_id}"
            )
            return
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

def has_node_bound_chatbot(app_id, node_id):
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    return chatbot_id is not None and str(chatbot_id).strip() != ''

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

def generate_openclaw_app_manager_login_code(app_id, expire_seconds=300):
    app_user_result = ensure_openclaw_app_manager_user(app_id)
    if app_user_result['result'] == 'error':
        return app_user_result
    app_user = app_user_result['data']
    username = str(app_user.get('username', '')).strip()
    password = str(app_user.get('password', '')).strip()
    user_id = str(app_user.get('user_id', '')).strip()
    if username == '' or password == '' or user_id == '':
        return {
            'result': 'error',
            'message': 'openclaw app manager user info invalid'
        }
    secret_text = json.dumps({
        'app_id': app_id,
        'username': username,
        'password': password
    }, ensure_ascii=False)
    try:
        expire_seconds = int(expire_seconds)
    except Exception:
        expire_seconds = 300
    if expire_seconds <= 0:
        expire_seconds = 300
    result = lanying_im_api.generate_secret_info(app_id, user_id, expire_seconds, secret_text)
    if result is None or result.get('code') != 200 or 'data' not in result:
        logging.warning(f"generate_openclaw_app_manager_login_code failed | app_id:{app_id}, user_id:{user_id}, expire_seconds:{expire_seconds}, result:{result}")
        return {
            'result': 'error',
            'message': 'generate openclaw app manager login code failed'
        }
    data = result.get('data') or {}
    code = str(data.get('code', '')).strip()
    if code == '':
        return {
            'result': 'error',
            'message': 'generate openclaw app manager login code failed'
        }
    return {
        'result': 'ok',
        'data': {
            'code': code
        }
    }

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

def clamp_probe_wait_timeout_ms(value):
    try:
        timeout_ms = int(value or 0)
    except Exception:
        timeout_ms = 0
    if timeout_ms <= 0:
        timeout_ms = PROBE_WAIT_TIMEOUT_MS
    return max(1000, min(timeout_ms, 10000))

def sanitize_probe_response_node(node_info):
    if not isinstance(node_info, dict):
        return node_info
    sanitized = dict(node_info)
    for field in PROBE_RESPONSE_REDACT_FIELDS:
        if field in sanitized:
            sanitized.pop(field, None)
    return sanitized

def parse_api_version_number(value):
    try:
        return int(str(value or '').strip())
    except Exception:
        return 0

def resolve_probe_support_state(node_info):
    if not isinstance(node_info, dict):
        return 'unknown'
    api_version = parse_api_version_number(node_info.get('api_version', ''))
    if api_version >= MIN_PROBE_API_VERSION:
        return 'supported'
    if api_version > 0:
        return 'unsupported'
    statuses = get_probe_check_statuses(node_info)
    if any(status != 'not_checked' for status in statuses):
        return 'supported'
    if int(node_info.get('last_probe_report_at', 0) or 0) > 0:
        return 'supported'
    if str(node_info.get('probe_repair_last_probe_id', '')).strip() != '':
        return 'supported'
    return 'unknown'

def is_config_sync_wait_supported(node_info):
    if not isinstance(node_info, dict):
        return False
    return parse_api_version_number(node_info.get('api_version', '')) >= MIN_PROBE_API_VERSION

def clamp_config_sync_wait_timeout_ms(value):
    try:
        timeout_ms = int(value or 0)
    except Exception:
        timeout_ms = 0
    if timeout_ms <= 0:
        timeout_ms = CONFIG_SYNC_WAIT_TIMEOUT_MS
    return max(1000, min(timeout_ms, 10000))

def build_pending_config_sync_state_updates(sync_id):
    return {
        'last_config_sync_id': str(sync_id or '').strip(),
        'last_config_sync_at': int(time.time() * 1000),
        'last_config_sync_report_at': 0,
        'last_config_sync_status': CONFIG_SYNC_STATUS_PENDING,
        'last_config_sync_error_code': '',
        'last_config_sync_error_message': '',
    }

def build_config_sync_state_updates(event):
    now_ms = int(time.time() * 1000)
    status = str(event.get('status', '')).strip().lower()
    if status not in [CONFIG_SYNC_STATUS_OK, CONFIG_SYNC_STATUS_FAILED]:
        status = CONFIG_SYNC_STATUS_FAILED
    return {
        'last_config_sync_id': str(event.get('sync_id', '')).strip(),
        'last_config_sync_report_at': int(event.get('reported_at', 0) or 0) if str(event.get('reported_at', '')).strip() != '' else now_ms,
        'last_config_sync_status': status,
        'last_config_sync_error_code': str(event.get('error_code', '') or ''),
        'last_config_sync_error_message': str(event.get('error_message', '') or ''),
        'plugin_version': str(event.get('plugin_version', '')).strip(),
        'api_version': str(event.get('api_version', '')).strip(),
    }

def is_matching_config_sync_report(node_info, sync_id, started_report_at=0):
    if not isinstance(node_info, dict):
        return False
    expected_sync_id = str(sync_id or '').strip()
    if expected_sync_id == '':
        return False
    current_sync_id = str(node_info.get('last_config_sync_id', '')).strip()
    try:
        latest_report_at = int(node_info.get('last_config_sync_report_at', 0) or 0)
    except Exception:
        latest_report_at = 0
    current_status = str(node_info.get('last_config_sync_status', '')).strip().lower()
    return current_sync_id == expected_sync_id and current_status in [CONFIG_SYNC_STATUS_OK, CONFIG_SYNC_STATUS_FAILED] and latest_report_at >= int(started_report_at or 0)

def build_config_sync_response_data(triggered, completed, timeout, legacy_fallback, sync_id, node_info):
    latest_node = sanitize_probe_response_node(node_info)
    status = str((latest_node or {}).get('last_config_sync_status', '')).strip().lower() if isinstance(latest_node, dict) else ''
    success = bool(completed) and status == CONFIG_SYNC_STATUS_OK
    return {
        'triggered': bool(triggered),
        'completed': bool(completed),
        'timeout': bool(timeout),
        'legacy_fallback': bool(legacy_fallback),
        'success': bool(success),
        'sync_id': str(sync_id or '').strip(),
        'error_code': str((latest_node or {}).get('last_config_sync_error_code', '') or '') if isinstance(latest_node, dict) else '',
        'error_message': str((latest_node or {}).get('last_config_sync_error_message', '') or '') if isinstance(latest_node, dict) else '',
        'node': latest_node,
    }

def is_probe_supported(node_info):
    return resolve_probe_support_state(node_info) == 'supported'

def is_probe_inflight(node_info, max_age_ms=PROBE_INFLIGHT_REUSE_WINDOW_MS):
    if not isinstance(node_info, dict):
        return False
    probe_id = str(node_info.get('last_probe_id', '')).strip()
    if probe_id == '':
        return False
    if parse_bool_flag(node_info.get('probe_completed')):
        return False
    if parse_bool_flag(node_info.get('probe_timeout')):
        return False
    try:
        last_probe_at = int(node_info.get('last_probe_at', 0) or 0)
    except Exception:
        last_probe_at = 0
    if last_probe_at <= 0:
        return False
    return int(time.time() * 1000) - last_probe_at <= max(1000, int(max_age_ms or 0))

def is_matching_probe_report(node_info, probe_id, started_report_at=0):
    if not isinstance(node_info, dict):
        return False
    expected_probe_id = str(probe_id or '').strip()
    if expected_probe_id == '':
        return False
    current_probe_id = str(node_info.get('last_probe_id', '')).strip()
    report_probe_id = str(node_info.get('probe_repair_last_probe_id', '')).strip()
    try:
        latest_report_at = int(node_info.get('last_probe_report_at', 0) or 0)
    except Exception:
        latest_report_at = 0
    return (
        current_probe_id == expected_probe_id and
        report_probe_id == expected_probe_id and
        latest_report_at >= int(started_report_at or 0)
    )

def build_probe_response_data(triggered, completed, timeout, probe_id, node_info):
    raw_node = dict(node_info) if isinstance(node_info, dict) else node_info
    latest_node = sanitize_probe_response_node(enrich_node_probe_snapshot(node_info))
    if isinstance(raw_node, dict) and isinstance(latest_node, dict):
        raw_summary = str(raw_node.get('probe_summary_text', '')).strip()
        if raw_summary != '':
            latest_node['probe_summary_text'] = raw_summary
    summary = latest_node.get('probe_summary_text', 'not_checked') if isinstance(latest_node, dict) else 'not_checked'
    if bool(timeout) and not bool(completed):
        summary = 'failed'
    return {
        'triggered': bool(triggered),
        'completed': bool(completed),
        'timeout': bool(timeout),
        'probe_id': str(probe_id or '').strip(),
        'summary': summary,
        'node': latest_node
    }

def normalize_presence_status(value):
    normalized = str(value or '').strip().lower()
    if normalized in [NODE_PRESENCE_ONLINE, NODE_PRESENCE_OFFLINE, NODE_PRESENCE_UNKNOWN]:
        return normalized
    return NODE_PRESENCE_UNKNOWN

def normalize_presence_source(value):
    normalized = str(value or '').strip().lower()
    if normalized in [
        NODE_PRESENCE_SOURCE_ONLINE_MARKER,
        NODE_PRESENCE_SOURCE_OFFLINE_MARKER,
        NODE_PRESENCE_SOURCE_PROBE_TIMEOUT,
        NODE_PRESENCE_SOURCE_UNKNOWN,
    ]:
        return normalized
    return NODE_PRESENCE_SOURCE_UNKNOWN

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

def normalize_session_key_text(session_key):
    if isinstance(session_key, bytes):
        try:
            session_key = session_key.decode('utf-8')
        except Exception:
            logging.exception("normalize_session_key_text decode failed")
            session_key = ''
    return str(session_key or '').strip().lower()

def normalize_session_key(session_key):
    normalized = normalize_session_key_text(session_key)
    if normalized.startswith('agent:main:router:'):
        return f'agent:main:clawchat-router:{normalized[len("agent:main:router:"):]}'
    if normalized.startswith('agent:main:group:') and normalized[len('agent:main:group:'):].strip() != '':
        return f'agent:main:clawchat:group:{normalized[len("agent:main:group:"):].strip()}'
    if normalized.startswith('agent:main:') and normalized[len('agent:main:'):].isdigit():
        return f'agent:main:clawchat:direct:{normalized[len("agent:main:"):]}'
    return normalized

def get_session_key_facts(session_key):
    raw_session_key = normalize_session_key_text(session_key)
    canonical_session_key = normalize_session_key(raw_session_key)
    facts = {
        'raw_session_key': raw_session_key,
        'canonical_session_key': canonical_session_key,
        'channel': '',
        'chat_type': '',
        'target_id': '',
        'is_legacy_alias': raw_session_key != '' and raw_session_key != canonical_session_key,
        'is_clawchat_session': False,
        'is_router': False,
        'is_group': False,
        'is_direct': False,
        'is_subagent': ':subagent:' in canonical_session_key,
    }
    if canonical_session_key == '':
        return facts
    parts = [part.strip() for part in canonical_session_key.split(':') if str(part).strip() != '']
    if len(parts) < 5 or parts[0] != 'agent':
        return facts
    if parts[2] not in ['clawchat', 'clawchat-router']:
        return facts
    channel = parts[2]
    cursor = 3
    if len(parts) >= 6 and parts[3] not in ['group', 'direct']:
        cursor = 4
    if cursor >= len(parts) or parts[cursor] not in ['group', 'direct']:
        return facts
    if cursor + 1 >= len(parts):
        return facts
    target_id = ':'.join(parts[cursor + 1:]).strip()
    if target_id == '':
        return facts
    facts['channel'] = channel
    facts['chat_type'] = parts[cursor]
    facts['target_id'] = target_id
    facts['is_clawchat_session'] = True
    facts['is_router'] = channel == 'clawchat-router'
    facts['is_group'] = parts[cursor] == 'group'
    facts['is_direct'] = parts[cursor] == 'direct'
    return facts

def get_legacy_session_key_aliases(session_key):
    facts = get_session_key_facts(session_key)
    canonical_session_key = facts.get('canonical_session_key', '')
    aliases = []
    if facts.get('channel') == 'clawchat-router' and facts.get('chat_type') in ['group', 'direct'] and facts.get('target_id') != '':
        aliases.append(f"agent:main:router:{facts.get('chat_type')}:{facts.get('target_id')}")
    if facts.get('channel') == 'clawchat' and facts.get('chat_type') == 'group' and facts.get('target_id') != '':
        aliases.append(f"agent:main:group:{facts.get('target_id')}")
    if (
        facts.get('channel') == 'clawchat' and
        facts.get('chat_type') == 'direct' and
        facts.get('target_id') != '' and
        str(facts.get('target_id')).isdigit()
    ):
        aliases.append(f"agent:main:{facts.get('target_id')}")
    result = []
    for alias in aliases:
        normalized_alias = normalize_session_key_text(alias)
        if normalized_alias == '' or normalized_alias == canonical_session_key or normalized_alias in result:
            continue
        result.append(normalized_alias)
    return result

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
    facts = get_session_key_facts(session_key)
    if not facts.get('is_clawchat_session'):
        return None
    return {
        'channel': facts.get('channel'),
        'chat_type': facts.get('chat_type'),
        'target_id': facts.get('target_id')
    }

def is_clawchat_session_identity(identity):
    return isinstance(identity, dict) and str(identity.get('channel', '')).strip() in ['clawchat', 'clawchat-router']

def is_group_session_identity(identity):
    return is_clawchat_session_identity(identity) and str(identity.get('chat_type', '')).strip() == 'group'

def is_direct_session_identity(identity):
    return is_clawchat_session_identity(identity) and str(identity.get('chat_type', '')).strip() == 'direct'

def is_clawchat_router_session_identity(identity):
    return is_clawchat_session_identity(identity) and str(identity.get('channel', '')).strip() == 'clawchat-router'

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
    if 'session_map_sync' not in node_info:
        return True
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
    if not is_group_session_identity(identity):
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
    return is_clawchat_router_session_identity(root_clawchat_session)

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
    sync_variant = str(facts.get('sync_variant', '')).strip()
    observed_message_text = str(facts.get('observed_message_text', '')).strip()
    return {
        'observed_sender_user_id': observed_sender_user_id,
        'observed_from_user_id': observed_from_user_id,
        'observed_to_id': observed_to_id,
        'observed_chat_type': observed_chat_type,
        'observed_channel': observed_channel,
        'observed_message_type': observed_message_type,
        'observed_message_type_source': observed_message_type_source,
        'sync_variant': sync_variant,
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
    if is_group_session_identity(root_clawchat_session):
            return 'im_user'
    if is_direct_session_identity(root_clawchat_session):
            return 'direct_user'
    return ''

def is_control_ui_active_user_observation(observed_facts):
    return str((observed_facts or {}).get('observed_message_type', '')).strip() == 'control_ui_user'

def is_im_subagent_bootstrap_observation(observed_facts):
    return str((observed_facts or {}).get('sync_variant', '')).strip() == 'im_subagent_bootstrap'

def apply_control_ui_user_sender_override(mapping):
    if not isinstance(mapping, dict):
        return mapping
    overridden = dict(mapping)
    overridden['origin_kind'] = 'openclaw_control'
    overridden['origin_user_id'] = ''
    return overridden

def resolve_root_session_sync_mode(root_clawchat_session):
    if is_router_root_session(root_clawchat_session):
        if is_direct_session_identity(root_clawchat_session):
            return 'router_direct'
        return 'router_group'
    if is_direct_session_identity(root_clawchat_session) and str(root_clawchat_session.get('channel', '')).strip() == 'clawchat':
        return 'clawchat_direct'
    if is_group_session_identity(root_clawchat_session) and str(root_clawchat_session.get('channel', '')).strip() == 'clawchat':
        return 'clawchat_group'
    return 'generic'

def is_group_root_session_sync_mode(root_mode):
    return root_mode in ['clawchat_group', 'router_group']

def is_direct_root_session_sync_mode(root_mode):
    return root_mode in ['clawchat_direct', 'router_direct']

def requires_management_user_group_admin(root_clawchat_session):
    return resolve_root_session_sync_mode(root_clawchat_session) == 'generic'

def should_send_control_ui_user_as_management(observed_facts, mapping):
    if not is_control_ui_active_user_observation(observed_facts):
        return False
    if is_subagent_bootstrap_observed_text(observed_facts):
        return False
    session_identity = parse_clawchat_session_identity(
        normalize_optional_session_key((mapping or {}).get('session_key', ''))
    )
    root_mode = resolve_root_session_sync_mode(parse_clawchat_session_identity(
        normalize_optional_session_key((mapping or {}).get('root_session_key', ''))
    ))
    if is_group_session_identity(session_identity):
        return is_group_root_session_sync_mode(root_mode)
    session_key = normalize_optional_session_key((mapping or {}).get('session_key', ''))
    if ':subagent:' in session_key and is_group_root_session_sync_mode(root_mode):
        return True
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
    if is_im_subagent_bootstrap_observation(observed_facts):
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
                    f"resolve_inherited_identity resolved im subagent bootstrap from {inherited_source} mapping | "
                    f"app_id:{app_id}, node_id:{node_id}, parent_session_key:{parent_session_key}, "
                    f"root_session_key:{root_session_key}, origin_kind:{inherited_identity.get('origin_kind', '')}, "
                    f"origin_user_id:{inherited_identity.get('origin_user_id', '')}"
                )
                return inherited_identity
        direct_identity = parse_clawchat_session_identity(root_session_key or parent_session_key)
        if is_direct_session_identity(direct_identity):
            resolved_user_id = str(direct_identity.get('target_id', '')).strip()
            if resolved_user_id != '':
                logging.info(
                    f"resolve_inherited_identity resolved im subagent bootstrap from direct identity | "
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
    if is_control_ui_active_user_observation(observed_facts):
        if root_mode in ['clawchat_direct', 'router_direct']:
            direct_identity = parse_clawchat_session_identity(root_session_key or parent_session_key)
            if is_direct_session_identity(direct_identity):
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
            if is_direct_root_session_sync_mode(root_mode) and is_direct_session_identity(direct_identity):
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
    if is_direct_session_identity(identity):
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
    if is_direct_session_identity(clawchat_session):
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
    if is_clawchat_session_identity(clawchat_session):
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
    if not is_group_session_identity(session_identity):
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

def strip_openclaw_runtime_context_from_visible_text(text):
    if not isinstance(text, str):
        return ''
    stripped = text.strip()
    current_marker = '[Current message]'
    if current_marker in stripped:
        return stripped.rsplit(current_marker, 1)[1].strip()
    return stripped

def extract_session_sync_text(message):
    if isinstance(message, str):
        return strip_openclaw_runtime_context_from_visible_text(message)
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
        if str(message.get('type', '')).strip() == 'toolCall' and str(message.get('name', '')).strip() == 'sessions_yield':
            arguments = message.get('arguments')
            if isinstance(arguments, str):
                arguments = lanying_utils.safe_json_loads(arguments, {})
            if isinstance(arguments, dict) and isinstance(arguments.get('message'), str):
                return arguments.get('message').strip()
    return ''

def normalize_session_sync_text(value):
    text = extract_session_sync_text(value)
    if not isinstance(text, str):
        text = str(text or '')
    normalized = ''.join(text.split()).strip()
    punctuation_map = str.maketrans('', '', """，。！？、；：“”"'`~!?,.;:""")
    return normalized.translate(punctuation_map)

def session_sync_texts_look_duplicated(left, right):
    a = normalize_session_sync_text(left)
    b = normalize_session_sync_text(right)
    if a == '' or b == '':
        return False
    shorter = a if len(a) <= len(b) else b
    longer = b if len(a) <= len(b) else a
    if len(shorter) < 12:
        return shorter == longer
    return shorter in longer

def has_sessions_yield_result(message):
    if isinstance(message, list):
        for item in message:
            if has_sessions_yield_result(item):
                return True
        return False
    if not isinstance(message, dict):
        return False
    if str(message.get('type', '')).strip() == 'toolCall' and str(message.get('name', '')).strip() == 'sessions_yield':
        arguments = message.get('arguments')
        if isinstance(arguments, str):
            arguments = lanying_utils.safe_json_loads(arguments, {})
        return isinstance(arguments, dict) and isinstance(arguments.get('message'), str) and arguments.get('message').strip() != ''
    if 'content' in message:
        return has_sessions_yield_result(message.get('content'))
    return False

def is_session_sync_silent_reply_text(text):
    if not isinstance(text, str):
        return False
    normalized_text = text.strip()
    if normalized_text == '':
        return False
    return normalized_text.upper() == 'NO_REPLY'


def serialize_openclaw_session_group_metadata_value(metadata):
    if not isinstance(metadata, dict) or len(metadata) == 0:
        return ''
    compact_metadata = {
        'sc': str(metadata.get('scene', '')).strip(),
        'p': str(metadata.get('peer_user_id', '')).strip(),
        'c': str(metadata.get('created_by_user_id', '')).strip(),
        'sk': normalize_session_key(metadata.get('session_key', '')),
        'rk': normalize_optional_session_key(metadata.get('root_session_key', '')),
    }
    parent_session_key = normalize_optional_session_key(metadata.get('parent_session_key', ''))
    if parent_session_key != '' and parent_session_key != compact_metadata['rk']:
        compact_metadata['pk'] = parent_session_key
    value = json.dumps(
        {OPENCLAW_SESSION_GROUP_METADATA_KEY: compact_metadata},
        ensure_ascii=False,
        separators=(',', ':'),
    )
    if len(value) <= 255:
        return value
    return ''


def build_openclaw_session_group_metadata(node_name, node_id, session_key, lineage, effective_target_session_key, owner_user_id, inherited_identity, mapping_mode=''):
    inherited_identity = normalize_session_mapping_record(inherited_identity)
    origin_user_id = str((inherited_identity or {}).get('origin_user_id', '')).strip()
    peer_user_id = ''
    if origin_user_id != '':
        peer_user_id = origin_user_id
    return {
        'scene': 'openclaw_session_group',
        'peer_user_id': peer_user_id,
        'created_by_user_id': str(owner_user_id).strip(),
        'session_key': normalize_session_key(session_key),
        'root_session_key': normalize_optional_session_key((lineage or {}).get('root_session_key', '')),
        'parent_session_key': normalize_optional_session_key((lineage or {}).get('parent_session_key', '')),
    }

def _set_openclaw_session_group_metadata(app_id, group_id, metadata, log_context=None):
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '' or not isinstance(metadata, dict) or len(metadata) == 0:
        return None
    log_context = log_context if isinstance(log_context, dict) else {}
    value = serialize_openclaw_session_group_metadata_value(metadata)
    if value == '':
        logging.info(
            f"openclaw_session_group metadata update skipped | app_id:{app_id}, "
            f"node_id:{log_context.get('node_id', '')}, session_key:{log_context.get('session_key', '')}, "
            f"group_id:{normalized_group_id}, owner_user_id:{log_context.get('owner_user_id', '')}, "
            f"metadata_key:{OPENCLAW_SESSION_GROUP_METADATA_KEY}, reason:serialized_value_too_long"
        )
        return {'code': 0, 'message': 'serialized_value_too_long'}
    logging.info(
        f"openclaw_session_group metadata update start | app_id:{app_id}, "
        f"node_id:{log_context.get('node_id', '')}, session_key:{log_context.get('session_key', '')}, "
        f"group_id:{normalized_group_id}, owner_user_id:{log_context.get('owner_user_id', '')}, "
        f"metadata_key:{OPENCLAW_SESSION_GROUP_METADATA_KEY}"
    )
    result = lanying_im_api.set_group_ext(app_id, normalized_group_id, value)
    if isinstance(result, dict) and result.get('code') == 200:
        logging.info(
            f"openclaw_session_group metadata update success | app_id:{app_id}, "
            f"node_id:{log_context.get('node_id', '')}, session_key:{log_context.get('session_key', '')}, "
            f"group_id:{normalized_group_id}, owner_user_id:{log_context.get('owner_user_id', '')}, "
            f"metadata_key:{OPENCLAW_SESSION_GROUP_METADATA_KEY}"
        )
    else:
        logging.info(
            f"openclaw_session_group metadata update failed | app_id:{app_id}, "
            f"node_id:{log_context.get('node_id', '')}, session_key:{log_context.get('session_key', '')}, "
            f"group_id:{normalized_group_id}, owner_user_id:{log_context.get('owner_user_id', '')}, "
            f"metadata_key:{OPENCLAW_SESSION_GROUP_METADATA_KEY}, result:{result}"
        )
    return result

def update_openclaw_session_group_metadata_async(app_id, group_id, metadata, log_context=None):
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '' or not isinstance(metadata, dict) or len(metadata) == 0:
        return
    try:
        executor.submit(_set_openclaw_session_group_metadata, app_id, normalized_group_id, metadata, log_context)
    except Exception:
        logging.exception(
            f"openclaw_session_group metadata update submit failed | app_id:{app_id}, "
            f"node_id:{(log_context or {}).get('node_id', '')}, session_key:{(log_context or {}).get('session_key', '')}, "
            f"group_id:{normalized_group_id}, owner_user_id:{(log_context or {}).get('owner_user_id', '')}, "
            f"metadata_key:{OPENCLAW_SESSION_GROUP_METADATA_KEY}"
        )

def create_openclaw_session_group(app_id, owner_user_id, node_name, node_id, session_name, metadata=None, log_context=None):
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
        group_id = str(response_json.get('data', {}).get('group_id', '')).strip()
        update_openclaw_session_group_metadata_async(app_id, group_id, metadata, log_context)
        return group_id
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

def get_group_member_list_for_group_admin(app_id, group_id, cursor = '', limit = 500):
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '':
        return None
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    try:
        response = requests.get(
            api_endpoint + '/group/member_list',
            headers={'app_id': app_id, 'access-token': admin_token, 'group_id': normalized_group_id},
            params={'group_id': group_id, 'cursor': cursor, 'limit': limit}
        )
        logging.info(
            f"get_group_member_list_for_group_admin | app_id:{app_id}, group_id:{group_id}, "
            f"cursor:{cursor}, limit:{limit}, response:{response.content}"
        )
        return json.loads(response.content)
    except Exception:
        logging.exception("get_group_member_list_for_group_admin failed")
        return None

def get_group_admin_list_for_group_admin(app_id, group_id):
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '':
        return None
    api_endpoint = lanying_config.get_lanying_api_endpoint(app_id)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    try:
        response = requests.get(
            api_endpoint + '/group/admin_list',
            headers={'app_id': app_id, 'access-token': admin_token, 'group_id': normalized_group_id},
            params={'group_id': group_id}
        )
        logging.info(
            f"get_group_admin_list_for_group_admin | app_id:{app_id}, group_id:{group_id}, "
            f"response:{response.content}"
        )
        return json.loads(response.content)
    except Exception:
        logging.exception("get_group_admin_list_for_group_admin failed")
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

def is_user_group_admin_or_owner(app_id, user_id, group_id):
    normalized_user_id = str(user_id).strip()
    normalized_group_id = str(group_id).strip()
    if normalized_user_id == '' or normalized_group_id == '':
        return False
    try:
        group_info_result = lanying_im_api.get_group_info(app_id, normalized_group_id)
        if isinstance(group_info_result, dict) and group_info_result.get('code') == 200:
            owner_user_id = str(group_info_result.get('data', {}).get('owner_id', '')).strip()
            if owner_user_id != '' and owner_user_id == normalized_user_id:
                return True
    except Exception:
        logging.exception("is_user_group_admin_or_owner get_group_info failed")
    try:
        admin_list_result = list_group_admin_user_ids(app_id, normalized_group_id)
        admin_user_ids = set(admin_list_result.get('admin_user_ids', set()))
        if normalized_user_id in admin_user_ids:
            return True
    except Exception:
        logging.exception("is_user_group_admin_or_owner list_group_admin_user_ids failed")
    return False

def ensure_user_group_admin_sync(app_id, user_id, group_id):
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
    if is_user_group_admin_or_owner(app_id, normalized_user_id, normalized_group_id):
        logging.info(
            f"ensure_user_group_admin skip existing admin | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}"
        )
        return True
    try:
        response_json = lanying_im_api.admin_add_group_admin(app_id, normalized_group_id, [int(normalized_user_id)])
        logging.info(
            f"ensure_user_group_admin add_admin | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}, response:{response_json}"
        )
        if not (isinstance(response_json, dict) and response_json.get('code') == 200):
            return False
        return is_user_group_admin_or_owner(app_id, normalized_user_id, normalized_group_id)
    except Exception:
        logging.exception("ensure_user_group_admin add_admin failed")
        return False

def ensure_required_management_user_group_admin(app_id, node_id, session_key, management_user_id, group_id, root_clawchat_session):
    normalized_group_id = str(group_id).strip()
    normalized_management_user_id = str(management_user_id).strip()
    if not requires_management_user_group_admin(root_clawchat_session):
        return {
            'result': 'ignored',
            'message': 'management group admin not required',
        }
    if normalized_group_id == '' or normalized_management_user_id == '':
        return {
            'result': 'error',
            'message': 'management user group admin target missing',
        }
    ready = ensure_user_group_admin_sync(app_id, normalized_management_user_id, normalized_group_id)
    logging.info(
        f"ensure_required_management_user_group_admin | app_id:{app_id}, node_id:{node_id}, "
        f"session_key:{session_key}, group_id:{normalized_group_id}, "
        f"management_user_id:{normalized_management_user_id}, ready:{ready}"
    )
    if ready:
        return {
            'result': 'ok'
        }
    return {
        'result': 'error',
        'message': 'management user must join group and become group admin',
    }

def ensure_user_group_admin(app_id, user_id, group_id):
    normalized_user_id = str(user_id).strip()
    normalized_group_id = str(group_id).strip()
    if normalized_user_id == '' or normalized_group_id == '':
        return False
    if not ensure_user_joined_group(app_id, normalized_user_id, normalized_group_id):
        logging.info(
            f"ensure_user_group_admin async join failed | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}"
        )
        return False
    try:
        executor.submit(ensure_user_group_admin_sync, app_id, normalized_user_id, normalized_group_id)
        logging.info(
            f"ensure_user_group_admin async scheduled | app_id:{app_id}, user_id:{normalized_user_id}, "
            f"group_id:{normalized_group_id}"
        )
        return True
    except Exception:
        logging.exception("ensure_user_group_admin async schedule failed")
        return False

def session_mapping_signature(mapping):
    normalized = normalize_session_mapping_record(mapping or {})
    signature = {}
    for key in [
        'session_key',
        'group_id',
        'app_id',
        'node_id',
        'openclaw_user_id',
        'management_user_id',
        'origin_kind',
        'origin_user_id',
        'chatbot_user_id',
        'parent_session_key',
        'root_session_key',
        'effective_target_session_key',
    ]:
        signature[key] = str(normalized.get(key, '')).strip()
    return signature

def session_mapping_conflicts(existing_mapping, incoming_mapping):
    return session_mapping_signature(existing_mapping) != session_mapping_signature(incoming_mapping)

def build_session_mapping_change_log_entry(
    app_id,
    node_id,
    previous_mapping,
    new_mapping,
    change_source,
    legacy_session_keys=None,
    extra_metadata=None,
):
    previous_normalized = normalize_session_mapping_record(previous_mapping or {})
    new_normalized = normalize_session_mapping_record(new_mapping or {})
    session_key = normalize_optional_session_key(
        new_normalized.get('session_key', '') or previous_normalized.get('session_key', '')
    )
    group_id = str(new_normalized.get('group_id', '') or previous_normalized.get('group_id', '')).strip()
    openclaw_user_id = str(
        new_normalized.get('openclaw_user_id', '') or previous_normalized.get('openclaw_user_id', '')
    ).strip()
    return {
        'app_id': str(app_id).strip(),
        'node_id': str(node_id).strip(),
        'session_key': session_key,
        'group_id': group_id,
        'openclaw_user_id': openclaw_user_id,
        'change_source': str(change_source or '').strip(),
        'previous_signature': session_mapping_signature(previous_normalized),
        'new_signature': session_mapping_signature(new_normalized),
        'previous_mapping': previous_normalized,
        'new_mapping': new_normalized,
        'legacy_session_keys': list(legacy_session_keys or []),
        'extra_metadata': dict(extra_metadata or {}),
    }

def write_session_mapping_change_log(log_entry):
    try:
        append_result = lanying_pgvector.append_openclaw_session_map_log(log_entry)
        if append_result.get('result') not in ['ok', 'ignored']:
            logging.info(
                f"write_session_mapping_change_log unexpected result | "
                f"session_key:{log_entry.get('session_key', '')}, result:{append_result}"
            )
    except Exception:
        logging.exception(
            f"write_session_mapping_change_log failed | "
            f"session_key:{log_entry.get('session_key', '')}, change_source:{log_entry.get('change_source', '')}"
        )

def record_session_mapping_change_async(
    app_id,
    node_id,
    previous_mapping,
    new_mapping,
    change_source,
    legacy_session_keys=None,
    extra_metadata=None,
):
    previous_normalized = normalize_session_mapping_record(previous_mapping or {})
    new_normalized = normalize_session_mapping_record(new_mapping or {})
    if session_mapping_signature(previous_normalized) == session_mapping_signature(new_normalized):
        return
    log_entry = build_session_mapping_change_log_entry(
        app_id,
        node_id,
        previous_normalized,
        new_normalized,
        change_source,
        legacy_session_keys=legacy_session_keys,
        extra_metadata=extra_metadata,
    )
    try:
        executor.submit(write_session_mapping_change_log, log_entry)
    except Exception:
        logging.exception(
            f"record_session_mapping_change_async submit failed | "
            f"session_key:{log_entry.get('session_key', '')}, change_source:{log_entry.get('change_source', '')}"
        )

def get_existing_canonical_session_mapping(redis, app_id, node_id, session_key):
    canonical_session_key = normalize_optional_session_key(session_key)
    if canonical_session_key == '':
        return None
    raw = lanying_redis.redis_get(
        redis,
        get_openclaw_session_mapping_by_session_key(app_id, node_id, canonical_session_key),
    )
    if not raw:
        return None
    return normalize_session_mapping_record(json.loads(raw))

def remove_session_mapping_index_entry(redis, app_id, node_id, session_key):
    normalized_session_key = normalize_session_key_text(session_key)
    if normalized_session_key == '':
        return
    index_key = get_openclaw_session_mapping_index_key(app_id, node_id)
    try:
        redis.srem(index_key, normalized_session_key)
    except Exception:
        logging.exception("remove_session_mapping_index_entry failed")

def converge_session_mapping_record(redis, app_id, node_id, mapping, legacy_session_keys=None):
    normalized_mapping = normalize_session_mapping_record(mapping)
    session_key = normalize_optional_session_key(normalized_mapping.get('session_key', ''))
    openclaw_user_id = str(normalized_mapping.get('openclaw_user_id', '')).strip()
    group_id = str(normalized_mapping.get('group_id', '')).strip()
    if session_key == '' or openclaw_user_id == '':
        return normalized_mapping
    body = dict(normalized_mapping)
    body['session_key'] = session_key
    body['openclaw_user_id'] = openclaw_user_id
    body['group_id'] = group_id
    body_json = json.dumps(body, ensure_ascii=False)
    redis.set(get_openclaw_session_mapping_by_session_key(app_id, node_id, session_key), body_json)
    redis.sadd(get_openclaw_session_mapping_index_key(app_id, node_id), session_key)
    if group_id != '':
        redis.set(get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id), body_json)
    for legacy_session_key in legacy_session_keys or []:
        normalized_legacy_session_key = normalize_session_key_text(legacy_session_key)
        if normalized_legacy_session_key == '' or normalized_legacy_session_key == session_key:
            continue
        redis.delete(get_openclaw_session_mapping_by_session_storage_key(app_id, node_id, normalized_legacy_session_key))
        remove_session_mapping_index_entry(redis, app_id, node_id, normalized_legacy_session_key)
    record_session_mapping_change_async(
        app_id,
        node_id,
        normalized_mapping,
        body,
        'read_time_converge',
        legacy_session_keys=legacy_session_keys,
        extra_metadata={
            'group_lookup_key_written': group_id != '',
        },
    )
    return body

def resolve_read_time_session_mapping(redis, app_id, node_id, mapping, legacy_session_keys=None):
    normalized_mapping = normalize_session_mapping_record(mapping)
    canonical_session_key = normalize_optional_session_key(normalized_mapping.get('session_key', ''))
    if canonical_session_key == '':
        return normalized_mapping
    existing_canonical_mapping = get_existing_canonical_session_mapping(
        redis,
        app_id,
        node_id,
        canonical_session_key,
    )
    if (
        isinstance(existing_canonical_mapping, dict) and
        session_mapping_conflicts(existing_canonical_mapping, normalized_mapping)
    ):
        logging.warning(
            f"resolve_read_time_session_mapping conflict | app_id:{app_id}, node_id:{node_id}, "
            f"canonical_session_key:{canonical_session_key}, "
            f"incoming_signature:{session_mapping_signature(normalized_mapping)}, "
            f"existing_signature:{session_mapping_signature(existing_canonical_mapping)}"
        )
        return existing_canonical_mapping
    return converge_session_mapping_record(
        redis,
        app_id,
        node_id,
        normalized_mapping,
        legacy_session_keys=legacy_session_keys,
    )

def load_session_mapping_by_session(redis, app_id, node_id, session_key):
    canonical_session_key = normalize_session_key(session_key)
    candidates = []
    for candidate in [canonical_session_key] + get_legacy_session_key_aliases(session_key):
        normalized_candidate = normalize_session_key_text(candidate)
        if normalized_candidate == '' or normalized_candidate in candidates:
            continue
        candidates.append(normalized_candidate)
    for candidate in candidates:
        raw = lanying_redis.redis_get(
            redis,
            get_openclaw_session_mapping_by_session_storage_key(app_id, node_id, candidate),
        )
        if not raw:
            continue
        mapping = normalize_session_mapping_record(json.loads(raw))
        if candidate != canonical_session_key or normalize_optional_session_key(mapping.get('session_key', '')) != canonical_session_key:
            mapping = resolve_read_time_session_mapping(
                redis,
                app_id,
                node_id,
                mapping,
                legacy_session_keys=[candidate],
            )
        return normalize_session_mapping_record(mapping)
    return None

def get_session_mapping_by_session(app_id, node_id, session_key):
    redis = lanying_redis.get_redis_connection()
    try:
        return load_session_mapping_by_session(redis, app_id, node_id, session_key)
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
        mapping = normalize_session_mapping_record(json.loads(raw))
        canonical_session_key = normalize_optional_session_key(mapping.get('session_key', ''))
        if canonical_session_key != '':
            mapping = resolve_read_time_session_mapping(redis, app_id, node_id, mapping)
        return normalize_session_mapping_record(mapping)
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
    seen_session_keys = set()
    for indexed_session_key in session_keys:
        try:
            mapping = get_session_mapping_by_session(app_id, node_id, indexed_session_key)
            normalized_session_key = normalize_optional_session_key((mapping or {}).get('session_key', ''))
            if isinstance(mapping, dict) and normalized_session_key != '' and normalized_session_key not in seen_session_keys:
                seen_session_keys.add(normalized_session_key)
                mappings.append(mapping)
        except Exception:
            logging.exception("list_session_mappings_for_node parse failed")
    return mappings

def summarize_group_member(member):
    if not isinstance(member, dict):
        return None
    user_id = str(member.get('user_id', '')).strip()
    if user_id == '':
        return None
    return {
        'user_id': user_id,
        'display_name': str(member.get('display_name', '')).strip(),
        'join_time': int(member.get('join_time', 0) or 0),
        'expired_time': int(member.get('expired_time', 0) or 0),
    }

def list_group_admin_user_ids(app_id, group_id):
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '':
        return {
            'admin_user_ids': set(),
            'admin_list_error': '',
        }
    response_json = get_group_admin_list_for_group_admin(app_id, normalized_group_id)
    if not isinstance(response_json, dict):
        return {
            'admin_user_ids': set(),
            'admin_list_error': 'group admin list unavailable',
        }
    if response_json.get('code') != 200:
        return {
            'admin_user_ids': set(),
            'admin_list_error': str(response_json.get('message', '') or 'group admin list request failed').strip(),
        }
    admin_user_ids = set()
    raw_admins = response_json.get('data')
    if not isinstance(raw_admins, list):
        return {
            'admin_user_ids': set(),
            'admin_list_error': 'group admin list data is invalid',
        }
    for raw_admin in raw_admins:
        admin_member = summarize_group_member(raw_admin)
        if not isinstance(admin_member, dict):
            continue
        user_id = str(admin_member.get('user_id', '')).strip()
        if user_id != '':
            admin_user_ids.add(user_id)
    return {
        'admin_user_ids': admin_user_ids,
        'admin_list_error': '',
    }

def list_group_member_summaries(app_id, group_id, limit = 500, max_pages = 20):
    members = []
    seen_user_ids = set()
    complete = True
    error_message = ''
    normalized_group_id = str(group_id).strip()
    if normalized_group_id == '':
        return {
            'members': members,
            'members_loaded_complete': True,
            'member_list_error': '',
        }
    cursor = ''
    for _ in range(max_pages):
        response_json = get_group_member_list_for_group_admin(app_id, normalized_group_id, cursor, limit)
        if not isinstance(response_json, dict):
            complete = False
            error_message = 'group member list unavailable'
            break
        if response_json.get('code') != 200:
            complete = False
            error_message = str(response_json.get('message', '') or 'group member list request failed').strip()
            break
        raw_members = response_json.get('data')
        if not isinstance(raw_members, list):
            complete = False
            error_message = 'group member list data is invalid'
            break
        for raw_member in raw_members:
            member = summarize_group_member(raw_member)
            if not isinstance(member, dict):
                continue
            user_id = member.get('user_id', '')
            if user_id in seen_user_ids:
                continue
            seen_user_ids.add(user_id)
            members.append(member)
        next_cursor = str(response_json.get('cursor', '')).strip()
        if next_cursor == '':
            break
        if next_cursor == cursor:
            complete = False
            error_message = 'group member list cursor did not advance'
            break
        cursor = next_cursor
    else:
        complete = False
        error_message = 'group member list exceeded page limit'
    return {
        'members': members,
        'members_loaded_complete': complete,
        'member_list_error': error_message,
    }

def build_session_mapping_key_user_group_status(user_id, owner_user_id, member_user_ids, admin_user_ids, admin_list_error=''):
    normalized_user_id = str(user_id).strip()
    normalized_owner_user_id = str(owner_user_id).strip()
    admin_status = 'unknown'
    if normalized_user_id != '' and str(admin_list_error).strip() == '':
        admin_status = 'admin' if normalized_user_id in admin_user_ids else 'not_admin'
    return {
        'user_id': normalized_user_id,
        'present_in_group': normalized_user_id != '' and normalized_user_id in member_user_ids,
        'is_group_owner': normalized_user_id != '' and normalized_user_id == normalized_owner_user_id,
        'admin_status': admin_status,
    }

def get_session_mapping_group_detail(app_id, group_id, mapping=None):
    normalized_group_id = str(group_id).strip()
    empty_detail = {
        'group_info': {
            'group_id': normalized_group_id,
            'owner_id': '',
        },
        'member_summary': {
            'member_count_reported': 0,
            'member_count_loaded': 0,
            'members_loaded_complete': True,
            'members': [],
        },
        'group_info_error': '',
        'member_list_error': '',
        'admin_list_error': '',
        'member_list_viewer_user_id': '',
        'admin_list_viewer_user_id': '',
    }
    if normalized_group_id == '':
        return empty_detail
    group_info = {
        'group_id': normalized_group_id,
        'owner_id': '',
    }
    group_info_error = ''
    raw_group_info_result = lanying_im_api.get_group_info(app_id, normalized_group_id)
    if not isinstance(raw_group_info_result, dict):
        group_info_error = 'group info unavailable'
    elif raw_group_info_result.get('code') != 200:
        group_info_error = str(raw_group_info_result.get('message', '') or 'group info request failed').strip()
    else:
        raw_group_info = raw_group_info_result.get('data')
        if not isinstance(raw_group_info, dict):
            group_info_error = 'group info data is invalid'
        else:
            group_info = dict(raw_group_info)
            group_info['group_id'] = str(raw_group_info.get('group_id', normalized_group_id)).strip() or normalized_group_id
            group_info['owner_id'] = str(raw_group_info.get('owner_id', '')).strip()
    member_list_result = list_group_member_summaries(
        app_id,
        normalized_group_id,
    )
    members = list(member_list_result.get('members', []))
    admin_list_result = list_group_admin_user_ids(
        app_id,
        normalized_group_id,
    )
    return {
        'group_info': group_info,
        'member_summary': {
            'member_count_reported': len(members),
            'member_count_loaded': len(members),
            'members_loaded_complete': bool(member_list_result.get('members_loaded_complete', False)),
            'members': members,
        },
        'group_info_error': group_info_error,
        'member_list_error': str(member_list_result.get('member_list_error', '')).strip(),
        'admin_list_error': str(admin_list_result.get('admin_list_error', '')).strip(),
        'member_list_viewer_user_id': '',
        'admin_list_viewer_user_id': '',
        'admin_user_ids': set(admin_list_result.get('admin_user_ids', set())),
    }

def build_session_mapping_detail_from_mapping(app_id, mapping, group_detail_cache=None):
    normalized_mapping = normalize_session_mapping_record(mapping)
    if not isinstance(normalized_mapping, dict):
        return None
    node_id = str(normalized_mapping.get('node_id', '')).strip()
    session_key = str(normalized_mapping.get('session_key', '')).strip()
    group_id = str(normalized_mapping.get('group_id', '')).strip()
    cache = group_detail_cache if isinstance(group_detail_cache, dict) else {}
    if group_id != '':
        if group_id not in cache:
            cache[group_id] = get_session_mapping_group_detail(app_id, group_id, normalized_mapping)
        group_detail = cache[group_id]
    else:
        group_detail = get_session_mapping_group_detail(app_id, '', normalized_mapping)
    group_info = dict(group_detail.get('group_info', {}))
    member_summary = dict(group_detail.get('member_summary', {}))
    members = list(member_summary.get('members', []))
    member_user_ids = set()
    for member in members:
        if isinstance(member, dict):
            user_id = str(member.get('user_id', '')).strip()
            if user_id != '':
                member_user_ids.add(user_id)
    admin_user_ids = set(group_detail.get('admin_user_ids', set()))
    owner_user_id = str(group_info.get('owner_id', '')).strip()
    admin_list_error = str(group_detail.get('admin_list_error', '')).strip()
    detail = dict(normalized_mapping)
    detail['group_info'] = group_info
    detail['member_summary'] = member_summary
    detail['key_user_status'] = {
        'openclaw_user_id': build_session_mapping_key_user_group_status(
            normalized_mapping.get('openclaw_user_id', ''),
            owner_user_id,
            member_user_ids,
            admin_user_ids,
            admin_list_error,
        ),
        'management_user_id': build_session_mapping_key_user_group_status(
            normalized_mapping.get('management_user_id', ''),
            owner_user_id,
            member_user_ids,
            admin_user_ids,
            admin_list_error,
        ),
        'origin_user_id': build_session_mapping_key_user_group_status(
            normalized_mapping.get('origin_user_id', ''),
            owner_user_id,
            member_user_ids,
            admin_user_ids,
            admin_list_error,
        ),
        'chatbot_user_id': build_session_mapping_key_user_group_status(
            normalized_mapping.get('chatbot_user_id', ''),
            owner_user_id,
            member_user_ids,
            admin_user_ids,
            admin_list_error,
        ),
    }
    detail['group_info_error'] = str(group_detail.get('group_info_error', '')).strip()
    detail['member_list_error'] = str(group_detail.get('member_list_error', '')).strip()
    detail['admin_list_error'] = admin_list_error
    detail['member_list_viewer_user_id'] = str(group_detail.get('member_list_viewer_user_id', '')).strip()
    detail['admin_list_viewer_user_id'] = str(group_detail.get('admin_list_viewer_user_id', '')).strip()
    detail['last_message_time'] = get_session_last_message_time(app_id, node_id, session_key)
    return detail

def get_session_mapping_detail_by_session(app_id, node_id, session_key, group_detail_cache=None):
    mapping = get_session_mapping_by_session(app_id, node_id, session_key)
    if not isinstance(mapping, dict):
        return None
    return build_session_mapping_detail_from_mapping(
        app_id,
        mapping,
        group_detail_cache=group_detail_cache,
    )

def list_session_mapping_details_for_node(app_id, node_id):
    mappings = list_session_mappings_for_node(app_id, node_id)
    details = []
    group_detail_cache = {}
    for mapping in mappings:
        detail = build_session_mapping_detail_from_mapping(
            app_id,
            mapping,
            group_detail_cache=group_detail_cache,
        )
        if isinstance(detail, dict):
            details.append(detail)
    return details

def list_openclaw_node_list_app_ids():
    redis = lanying_redis.get_redis_connection()
    app_ids = set()
    prefix = "lanying_connector:openclaw:node_list:"
    try:
        for raw_key in redis.scan_iter(match=f"{prefix}*", count=100):
            key = raw_key.decode('utf-8') if isinstance(raw_key, bytes) else str(raw_key)
            app_id = key[len(prefix):].strip() if key.startswith(prefix) else ''
            if app_id != '':
                app_ids.add(app_id)
    except Exception:
        logging.exception("list_openclaw_node_list_app_ids scan failed")
        return []
    resolved_app_ids = sorted(app_ids)
    logging.info(
        f"list_openclaw_node_list_app_ids | app_count:{len(resolved_app_ids)}, app_ids:{resolved_app_ids}"
    )
    return resolved_app_ids

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
    record_session_mapping_change_async(
        app_id,
        node_id,
        previous_session_mapping,
        body,
        'set_session_mapping',
        extra_metadata={
            'previous_session_group_id': previous_session_group_id,
            'group_lookup_key_written': group_id != '',
            'group_lookup_key_deleted': previous_session_group_id != '' and group_id == '',
        },
    )
    return {
        'result': 'ok',
        'data': body
    }

def rewrite_session_mapping_for_migration(app_id, node_id, mapping, change_source='migration_rewrite_session_mapping'):
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
    if previous_session_mapping is None:
        return {
            'result': 'error',
            'message': 'session mapping not found'
        }
    previous_session_group_id = str(previous_session_mapping.get('group_id', '')).strip()
    previous_openclaw_user_id = str(previous_session_mapping.get('openclaw_user_id', '')).strip()
    previous_group_mapping = (
        get_session_mapping_by_group(app_id, node_id, openclaw_user_id, group_id)
        if group_id != '' else None
    )
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
        body['created_at'] = int(previous_session_mapping.get('created_at', 0) or 0) or body['updated_at']
    body_json = json.dumps(body, ensure_ascii=False)
    redis.set(get_openclaw_session_mapping_by_session_key(app_id, node_id, session_key), body_json)
    should_delete_previous_group_lookup = (
        previous_session_group_id != '' and (
            previous_session_group_id != group_id or previous_openclaw_user_id != openclaw_user_id
        )
    )
    if should_delete_previous_group_lookup:
        redis.delete(get_openclaw_session_mapping_by_group_key(app_id, node_id, previous_openclaw_user_id, previous_session_group_id))
    if group_id != '':
        redis.set(get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id), body_json)
    redis.sadd(get_openclaw_session_mapping_index_key(app_id, node_id), session_key)
    record_session_mapping_change_async(
        app_id,
        node_id,
        previous_session_mapping,
        body,
        change_source,
        extra_metadata={
            'previous_session_group_id': previous_session_group_id,
            'previous_openclaw_user_id': previous_openclaw_user_id,
            'group_lookup_key_written': group_id != '',
            'group_lookup_key_deleted': should_delete_previous_group_lookup,
            'migration_rewrite': True,
        },
    )
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
            existing_root_clawchat_session = parse_clawchat_session_identity(
                normalize_optional_session_key(merged_existing.get('root_session_key', ''))
            )
            required_admin_result = ensure_required_management_user_group_admin(
                app_id,
                node_id,
                normalized_session_key,
                management_user_id,
                existing.get('group_id', ''),
                existing_root_clawchat_session,
            )
            if required_admin_result['result'] == 'error':
                return required_admin_result
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
        group_metadata = build_openclaw_session_group_metadata(
            node_name,
            node_id,
            normalized_session_key,
            lineage,
            mapping_decision['effective_target_session_key'],
            session_group_owner_user_id,
            inherited_identity,
            mapping_decision['mode'],
        )
        group_id = create_openclaw_session_group(
            app_id,
            session_group_owner_user_id,
            node_name,
            node_id,
            session_key,
            metadata=group_metadata,
            log_context={
                'node_id': node_id,
                'session_key': normalized_session_key,
                'owner_user_id': session_group_owner_user_id,
            },
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
    required_admin_result = ensure_required_management_user_group_admin(
        app_id,
        node_id,
        normalized_session_key,
        management_user_id,
        group_id,
        root_clawchat_session,
    )
    if required_admin_result['result'] == 'error':
        return required_admin_result
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

def build_session_sync_delivery_ext(
    session_key,
    source,
    role,
    message_id='',
    trigger_msg_id='',
    parent_session_key='',
    root_session_key='',
    sync_variant='',
    display_kind='',
):
    normalized_source = str(source or '').strip()
    normalized_role = str(role or '').strip().lower()
    normalized_sync_variant = str(sync_variant or '').strip()
    openclaw = {
        'type': 'session_sync_delivery',
        'session': normalize_session_key(session_key),
        'source': normalized_source,
        'role': normalized_role
    }
    normalized_message_id = str(message_id or '').strip()
    if normalized_message_id != '':
        openclaw['message_id'] = normalized_message_id
    normalized_trigger_msg_id = str(trigger_msg_id or '').strip()
    if normalized_trigger_msg_id != '':
        openclaw['trigger_msg_id'] = normalized_trigger_msg_id
        openclaw['request_msg_id'] = normalized_trigger_msg_id
    normalized_parent_session_key = normalize_optional_session_key(parent_session_key)
    if normalized_parent_session_key != '':
        openclaw['parent_session'] = normalized_parent_session_key
    normalized_root_session_key = normalize_optional_session_key(root_session_key)
    if normalized_root_session_key != '':
        openclaw['root_session'] = normalized_root_session_key
    if normalized_sync_variant != '':
        openclaw['sync_variant'] = normalized_sync_variant
    normalized_display_kind = str(display_kind or '').strip()
    if normalized_display_kind != '':
        openclaw['display_kind'] = normalized_display_kind
    ext = {'openclaw': openclaw}
    # Session visible-delivery events from OpenClaw are display-only in IM.
    ext['ai'] = {'ai_generate': False}
    return ext

def build_router_reply_delivery_ext(message):
    ext = {
        'ai': {
            'role': 'ai',
            'ai_generate': False
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
    openclaw_type = str(openclaw_in.get('type', '')).strip()
    session_key = normalize_session_key(openclaw_in.get('session', ''))
    if openclaw_type == 'im_reply_delivery':
        source = str(openclaw_in.get('source', '')).strip() or 'im_reply'
        role = str(openclaw_in.get('role', '')).strip() or 'assistant'
        reply_openclaw = {
            'type': 'im_reply_delivery',
            'source': source,
            'role': role,
        }
        if session_key != '':
            reply_openclaw['session'] = session_key
        message_id = str(openclaw_in.get('message_id', '')).strip()
        if message_id != '':
            reply_openclaw['message_id'] = message_id
        visible_delivery_owner = str(openclaw_in.get('visible_delivery_owner', '')).strip()
        if visible_delivery_owner != '':
            reply_openclaw['visible_delivery_owner'] = visible_delivery_owner
        trigger_msg_id = str(openclaw_in.get('trigger_msg_id', '')).strip()
        if trigger_msg_id != '':
            reply_openclaw['trigger_msg_id'] = trigger_msg_id
        request_msg_id = str(openclaw_in.get('request_msg_id', '')).strip()
        if request_msg_id == '':
            request_msg_id = str(openclaw_in.get('router_request_sid', '')).strip()
        if request_msg_id == '':
            request_msg_id = trigger_msg_id
        if request_msg_id != '':
            reply_openclaw['request_msg_id'] = request_msg_id
        ext['openclaw'] = reply_openclaw
        return ext
    if session_key == '':
        return ext
    reply_openclaw = {
        'type': 'session_sync_delivery',
        'session': session_key,
        'source': 'control_ui_reply',
        'role': 'assistant',
    }
    parent_session_key = normalize_optional_session_key(openclaw_in.get('parent_session', ''))
    if parent_session_key != '':
        reply_openclaw['parent_session'] = parent_session_key
    root_session_key = normalize_optional_session_key(openclaw_in.get('root_session', ''))
    if root_session_key != '':
        reply_openclaw['root_session'] = root_session_key
    sync_variant = str(openclaw_in.get('sync_variant', '')).strip()
    if sync_variant != '':
        reply_openclaw['sync_variant'] = sync_variant
    display_kind = str(openclaw_in.get('display_kind', '')).strip()
    if display_kind != '':
        reply_openclaw['display_kind'] = display_kind
    visible_delivery_owner = str(openclaw_in.get('visible_delivery_owner', '')).strip()
    if visible_delivery_owner != '':
        reply_openclaw['visible_delivery_owner'] = visible_delivery_owner
    request_role = str(openclaw_in.get('role', '')).strip().lower()
    if request_role == '':
        request_message = openclaw_in.get('message', {})
        if isinstance(request_message, dict):
            request_role = str(request_message.get('role', '')).strip().lower()
    request_source = str(openclaw_in.get('source', '')).strip()
    is_reply_context = request_source == 'control_ui_reply' or request_role == 'assistant'
    if request_source != '' and not is_reply_context:
        reply_openclaw['request_source'] = request_source
    if request_role != '' and not is_reply_context:
        reply_openclaw['request_role'] = request_role
    request_message_id = str(openclaw_in.get('message_id', '')).strip()
    if request_message_id != '' and not is_reply_context:
        reply_openclaw['request_message_id'] = request_message_id
    trigger_msg_id = str(openclaw_in.get('trigger_msg_id', '')).strip()
    if trigger_msg_id != '':
        reply_openclaw['trigger_msg_id'] = trigger_msg_id
    request_msg_id = str(openclaw_in.get('request_msg_id', '')).strip()
    if request_msg_id == '':
        request_msg_id = str(openclaw_in.get('router_request_sid', '')).strip()
    if request_msg_id == '':
        request_msg_id = trigger_msg_id
    if request_msg_id != '':
        reply_openclaw['request_msg_id'] = request_msg_id
    ext['openclaw'] = reply_openclaw
    ai_in = msg_ext.get('ai', {})
    if isinstance(ai_in, dict) and ai_in.get('ai_generate') is False:
        ext['ai']['ai_generate'] = False
    return ext

def forward_session_sync_to_group(app_id, node_info, mapping, role, text, delivery_ext=None):
    if not isinstance(mapping, dict) or not isinstance(text, str) or text.strip() == '':
        return 0
    chatbot_user_id = str(mapping.get('chatbot_user_id', '')).strip()
    management_user_id = str(mapping.get('management_user_id', '')).strip()
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
    if from_user_id == management_user_id:
        sender_ready = ensure_user_group_admin(app_id, from_user_id, group_id)
    else:
        sender_ready = ensure_user_joined_group(app_id, from_user_id, group_id)
    if not sender_ready:
        logging.info(
            f"forward_session_sync_to_group skip for sender not ready in group | "
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
    is_router_direct_session = is_clawchat_router_session_identity(route_identity) and is_direct_session_identity(route_identity)
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
    if is_direct_session_identity(target_identity):
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
    if not (is_clawchat_router_session_identity(session_identity) and is_group_session_identity(session_identity)):
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
        if is_clawchat_router_session_identity(session_identity) and is_group_session_identity(session_identity):
            router_mapping = dict(mapping)
            router_mapping['session_key'] = session_key
            return router_mapping
    return None

def should_forward_group_sync_via_router_reply(target_mapping):
    if not isinstance(target_mapping, dict):
        return None
    target_session_key = normalize_optional_session_key(target_mapping.get('session_key', ''))
    target_identity = parse_clawchat_session_identity(target_session_key)
    if is_clawchat_router_session_identity(target_identity) and is_group_session_identity(target_identity):
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

def resolve_inherited_observed_origin_facts_for_transcript_event(app_id, node_info, session_key, parent_session_key, root_session_key, source, role, observed_origin_facts, text):
    normalized_session_key = normalize_session_key(session_key)
    normalized_parent_session_key = normalize_optional_session_key(parent_session_key)
    normalized_root_session_key = normalize_optional_session_key(root_session_key)
    normalized_source = str(source or '').strip()
    normalized_role = str(role or '').strip().lower()
    if (
        normalized_session_key == '' or
        ':subagent:' not in normalized_session_key or
        normalized_source != 'control_ui_user' or
        normalized_role != 'user'
    ):
        return {}
    if (
        str(observed_origin_facts.get('sender_user_id', '')).strip() != '' or
        str(observed_origin_facts.get('observed_sender_user_id', '')).strip() != ''
    ):
        return {}
    if str(observed_origin_facts.get('observed_message_type_source', '')).strip() != 'fallback':
        return {}
    if str(observed_origin_facts.get('observed_message_type', '')).strip() != 'control_ui_user':
        return {}
    candidate_session_keys = [normalized_parent_session_key, normalized_root_session_key]
    for candidate_session_key in candidate_session_keys:
        if candidate_session_key == '':
            continue
        mapping = get_session_mapping_by_session(app_id, node_info['node_id'], candidate_session_key)
        if not isinstance(mapping, dict):
            continue
        inherited = normalize_observed_origin_facts({
            'sender_user_id': mapping.get('origin_user_id', ''),
            'observed_sender_user_id': mapping.get('origin_user_id', ''),
            'observed_from_user_id': mapping.get('origin_user_id', ''),
            'observed_to_id': mapping.get('chatbot_user_id', ''),
            'observed_chat_type': 'GROUPCHAT' if str(mapping.get('group_id', '')).strip() != '' else 'CHAT',
            'observed_channel': 'clawchat',
            'observed_message_type': 'im_inbound_user',
            'observed_message_type_source': 'inherited_mapping',
            'sync_variant': '',
        })
        if str(text or '').strip().find('[Subagent Context]') >= 0 and str(inherited.get('sync_variant', '')).strip() == '':
            inherited['sync_variant'] = 'im_subagent_bootstrap'
        return inherited
    return {}

def normalize_session_transcript_event(app_id, node_info, event):
    if not isinstance(event, dict):
        return None
    event_type = str(event.get('type', '')).strip()
    if event_type == '' or event_type == 'session_message_sync':
        return event
    if event_type != 'session_transcript_observed':
        return None
    message = event.get('message', {})
    role = ''
    if isinstance(message, dict):
        role = str(message.get('role', '')).strip().lower()
    text = extract_session_sync_text(message.get('content') if isinstance(message, dict) else message)
    normalized_event = dict(event)
    normalized_event['type'] = 'session_message_sync'
    observed_origin_facts = normalize_observed_origin_facts({
        'sender_user_id': event.get('sender_user_id', ''),
        'observed_sender_user_id': event.get('observed_sender_user_id', ''),
        'observed_from_user_id': event.get('observed_from_user_id', ''),
        'observed_to_id': event.get('observed_to_id', ''),
        'observed_chat_type': event.get('observed_chat_type', ''),
        'observed_channel': event.get('observed_channel', ''),
        'observed_message_type': event.get('observed_message_type', ''),
        'observed_message_type_source': event.get('observed_message_type_source', ''),
        'sync_variant': event.get('sync_variant', ''),
    })
    inherited = resolve_inherited_observed_origin_facts_for_transcript_event(
        app_id,
        node_info,
        event.get('session', ''),
        event.get('parent_session', ''),
        event.get('root_session', ''),
        event.get('source', ''),
        role,
        observed_origin_facts,
        text,
    )
    merged = dict(observed_origin_facts)
    inherited_override_keys = [
        'sender_user_id',
        'observed_sender_user_id',
        'observed_from_user_id',
        'observed_to_id',
        'observed_chat_type',
        'observed_channel',
        'observed_message_type',
        'observed_message_type_source',
    ]
    for key in inherited_override_keys:
        value = str(inherited.get(key, '')).strip()
        if value != '':
            merged[key] = value
    inherited_sync_variant = str(inherited.get('sync_variant', '')).strip()
    if inherited_sync_variant != '' and str(merged.get('sync_variant', '')).strip() == '':
        merged['sync_variant'] = inherited_sync_variant
    field_map = {
        'sender_user_id': 'sender_user_id',
        'observed_sender_user_id': 'observed_sender_user_id',
        'observed_from_user_id': 'observed_from_user_id',
        'observed_to_id': 'observed_to_id',
        'observed_chat_type': 'observed_chat_type',
        'observed_channel': 'observed_channel',
        'observed_message_type': 'observed_message_type',
        'observed_message_type_source': 'observed_message_type_source',
        'sync_variant': 'sync_variant',
    }
    for source_key, target_key in field_map.items():
        value = str(merged.get(source_key, '')).strip()
        if value != '':
            normalized_event[target_key] = value
    return normalized_event

def handle_session_message_sync_event(app_id, node_info, event):
    event = normalize_session_transcript_event(app_id, node_info, event)
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
        'sync_variant': event.get('sync_variant', ''),
    })
    message = event.get('message', {})
    role = ''
    if isinstance(message, dict):
        role = str(message.get('role', '')).strip().lower()
    text = extract_session_sync_text(message.get('content') if isinstance(message, dict) else message)
    observed_origin_facts['observed_message_text'] = text
    if session_key == '' or source not in ['control_ui_user', 'control_ui_reply']:
        return
    update_session_last_message_time(app_id, node_info.get('node_id', ''), session_key)
    if source == 'control_ui_reply' and role == 'assistant' and is_session_sync_silent_reply_text(text):
        logging.info(
            f"handle_session_message_sync_event skip silent NO_REPLY delivery | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}"
        )
        return
    visible_delivery_owner = str(event.get('visible_delivery_owner', '')).strip()
    visible_delivery_reason = str(event.get('visible_delivery_reason', '')).strip()
    if visible_delivery_owner == 'plugin':
        logging.info(
            f"visible_reply_route plugin_owned_skip | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
            f"parent_session_key:{parent_session_key}, root_session_key:{root_session_key}, "
            f"visible_delivery_reason:{visible_delivery_reason}"
        )
        return
    suppression_reason = str(event.get('suppression_reason', '')).strip()
    connector_drop_suppression_reasons = {
        'duplicate_parent_after_subagent',
        'internal_runtime_context',
        'internal_runtime_context_reply',
        'prompt_context_envelope',
    }
    if suppression_reason in connector_drop_suppression_reasons:
        logging.info(
            f"handle_session_message_sync_event skip by connector suppression marker | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
            f"suppression_reason:{suppression_reason}"
        )
        return
    if source == 'control_ui_reply' and role == 'assistant':
        logging.info(
            f"visible_reply_route connector_materialize | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
            f"visible_delivery_owner:{visible_delivery_owner or 'connector'}, "
            f"visible_delivery_reason:{visible_delivery_reason or 'transcript_sync'}"
        )
    message_id = str(event.get('message_id', '')).strip()
    trigger_msg_id = str(event.get('trigger_msg_id', '')).strip()
    message_seq = str(event.get('message_seq', '')).strip()
    message_timestamp = str(event.get('message_timestamp', '')).strip()
    display_kind = 'yield_result' if role == 'assistant' and has_sessions_yield_result(message.get('content') if isinstance(message, dict) else message) else ''
    should_materialize_clawchat_group = not (
        source == 'control_ui_user' and
        role == 'user' and
        text.strip() == ''
    )
    mapping = get_session_mapping_by_session(app_id, node_info['node_id'], session_key)
    logging.info(
        f"handle_session_message_sync_event | app_id:{app_id}, node_id:{node_info.get('node_id', '')}, "
        f"source:{source}, session_key:{session_key}, parent_session_key:{parent_session_key}, "
        f"root_session_key:{root_session_key}, role:{role}, message_id:{message_id}, "
        f"trigger_msg_id:{trigger_msg_id}, "
        f"message_seq:{message_seq}, message_timestamp:{message_timestamp}, text_len:{len(text.strip())}, "
        f"has_existing_mapping:{mapping is not None}, "
        f"materialize_clawchat_group:{should_materialize_clawchat_group}"
    )
    if source in ['control_ui_user', 'control_ui_reply'] and mapping is None:
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
            logging.info(
                f"handle_session_message_sync_event skip because ensure_session_mapping ignored | "
                f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                f"source:{source}, role:{role}"
            )
            return
    if mapping is None:
        logging.info(
            f"handle_session_message_sync_event skip because session mapping missing | "
            f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
            f"source:{source}, role:{role}"
        )
        return
    delivery_lineage = resolve_session_lineage(
        app_id,
        node_info['node_id'],
        normalize_session_key(mapping.get('session_key', '')) or session_key,
        normalize_optional_session_key(mapping.get('parent_session_key', '')) or parent_session_key,
        normalize_optional_session_key(mapping.get('root_session_key', '')) or root_session_key,
    )
    delivery_ext = build_session_sync_delivery_ext(
        delivery_lineage.get('session_key', '') or session_key,
        source,
        role,
        message_id,
        trigger_msg_id,
        delivery_lineage.get('parent_session_key', ''),
        delivery_lineage.get('root_session_key', ''),
        observed_origin_facts.get('sync_variant', ''),
        display_kind,
    )
    if text.strip() != '' and role in ['user', 'assistant']:
        target_session_key = normalize_optional_session_key(
            mapping.get('effective_target_session_key', '') or mapping.get('root_session_key', '')
        )
        target_identity = parse_clawchat_session_identity(target_session_key)
        if (
            target_session_key != '' and
            target_session_key != session_key and
            not is_direct_session_identity(target_identity) and
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
        dedupe_state = reserve_recent_session_transcript_materialization(event, role)
        dedupe_key = dedupe_state.get('dedupe_key', '')
        if dedupe_state.get('duplicate'):
            logging.info(
                f"transcript_materialization_dedupe duplicate_skip | "
                f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                f"message_id:{message_id}, source:{source}, role:{role}"
            )
            return
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
                logging.info(
                    f"handle_session_message_sync_event router group reply forward returned empty; fallback continues | "
                    f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                    f"message_id:{message_id}, source:{source}, role:{role}"
                )
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
            logging.info(
                f"handle_session_message_sync_event router direct reply forward returned empty; fallback continues | "
                f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                f"message_id:{message_id}, source:{source}, role:{role}"
            )
        visible_reply_dedupe_state = reserve_recent_visible_reply_materialization(
            session_key=session_key,
            source=source,
            role=role,
            message_id=message_id,
            request_msg_id=trigger_msg_id,
            text=text,
        )
        visible_reply_dedupe_keys = visible_reply_dedupe_state.get('dedupe_keys', [])
        if visible_reply_dedupe_state.get('duplicate'):
            logging.info(
                f"visible_reply_dedupe duplicate_skip_transcript_entry | "
                f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                f"message_id:{message_id}, trigger_msg_id:{trigger_msg_id}, source:{source}, role:{role}"
            )
            return
        if target.get('kind') == 'direct':
            msg_id = forward_session_sync_to_direct(
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
            if msg_id <= 0:
                forget_recent_session_transcript_materialization(dedupe_key)
                forget_recent_visible_reply_materialization(visible_reply_dedupe_keys)
                logging.info(
                    f"visible_reply_materialization direct_send_empty | "
                    f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                    f"message_id:{message_id}, source:{source}, role:{role}"
                )
            return
        target_mapping = target.get('mapping', mapping)
        if target.get('kind') == 'group' and role == 'user' and should_send_control_ui_user_as_management(observed_origin_facts, target_mapping):
            target_mapping = apply_control_ui_user_sender_override(target_mapping)
        msg_id = forward_session_sync_to_group(app_id, node_info, target_mapping, role, text, delivery_ext)
        if msg_id <= 0:
            forget_recent_session_transcript_materialization(dedupe_key)
            forget_recent_visible_reply_materialization(visible_reply_dedupe_keys)
            logging.info(
                f"visible_reply_materialization group_send_empty | "
                f"app_id:{app_id}, node_id:{node_info.get('node_id', '')}, session_key:{session_key}, "
                f"message_id:{message_id}, source:{source}, role:{role}"
            )

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

def cleanup_recent_session_transcript_materialization(now_ms=None):
    current_ms = int(now_ms if isinstance(now_ms, int) else time.time() * 1000)
    stale_keys = []
    for key, updated_at in list(recent_session_transcript_materialization_by_key.items()):
        try:
            if current_ms - int(updated_at or 0) > SESSION_TRANSCRIPT_MATERIALIZATION_DEDUPE_TTL_MS:
                stale_keys.append(key)
        except Exception:
            stale_keys.append(key)
    for key in stale_keys:
        recent_session_transcript_materialization_by_key.pop(key, None)

def build_session_transcript_materialization_dedupe_key(event, role):
    if not isinstance(event, dict):
        return ''
    session_key = normalize_session_key(event.get('session', ''))
    message_id = str(event.get('message_id', '')).strip()
    source = str(event.get('source', '')).strip()
    normalized_role = str(role or '').strip().lower()
    if session_key == '' or message_id == '' or source == '' or normalized_role == '':
        return ''
    return '\u0000'.join([session_key, message_id, source, normalized_role])

def reserve_recent_session_transcript_materialization(event, role):
    dedupe_key = build_session_transcript_materialization_dedupe_key(event, role)
    if dedupe_key == '':
        return {
            'dedupe_key': '',
            'duplicate': False,
        }
    now_ms = int(time.time() * 1000)
    cleanup_recent_session_transcript_materialization(now_ms)
    if dedupe_key in recent_session_transcript_materialization_by_key:
        return {
            'dedupe_key': dedupe_key,
            'duplicate': True,
        }
    recent_session_transcript_materialization_by_key[dedupe_key] = now_ms
    return {
        'dedupe_key': dedupe_key,
        'duplicate': False,
    }

def forget_recent_session_transcript_materialization(dedupe_key):
    normalized_key = str(dedupe_key or '').strip()
    if normalized_key == '':
        return
    recent_session_transcript_materialization_by_key.pop(normalized_key, None)

def cleanup_recent_visible_reply_materialization(now_ms=None):
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    expired_keys = [
        dedupe_key
        for dedupe_key, updated_at in recent_visible_reply_materialization_by_key.items()
        if now_ms - int(updated_at or 0) > VISIBLE_REPLY_MATERIALIZATION_DEDUPE_TTL_MS
    ]
    for dedupe_key in expired_keys:
        recent_visible_reply_materialization_by_key.pop(dedupe_key, None)

def build_visible_reply_materialization_dedupe_keys(
    session_key='',
    source='',
    role='',
    message_id='',
    request_msg_id='',
    text='',
):
    normalized_session_key = normalize_session_key(session_key)
    normalized_source = str(source or '').strip()
    normalized_role = str(role or '').strip().lower()
    normalized_message_id = str(message_id or '').strip()
    normalized_request_msg_id = str(request_msg_id or '').strip()
    normalized_text = str(text or '').strip()
    if (
        normalized_session_key == '' or
        normalized_source != 'control_ui_reply' or
        normalized_role != 'assistant'
    ):
        return []
    keys = []
    if normalized_message_id != '':
        keys.append('\u0000'.join([
            'message_id',
            normalized_session_key,
            normalized_message_id,
            normalized_source,
            normalized_role,
        ]))
    if normalized_request_msg_id != '' and normalized_text != '':
        keys.append('\u0000'.join([
            'request_msg_id_text',
            normalized_session_key,
            normalized_request_msg_id,
            normalized_source,
            normalized_role,
            normalized_text,
        ]))
    return keys

def reserve_recent_visible_reply_materialization(
    session_key='',
    source='',
    role='',
    message_id='',
    request_msg_id='',
    text='',
):
    dedupe_keys = build_visible_reply_materialization_dedupe_keys(
        session_key=session_key,
        source=source,
        role=role,
        message_id=message_id,
        request_msg_id=request_msg_id,
        text=text,
    )
    if len(dedupe_keys) == 0:
        return {
            'dedupe_keys': [],
            'duplicate': False,
            'matched_key': '',
        }
    now_ms = int(time.time() * 1000)
    cleanup_recent_visible_reply_materialization(now_ms)
    for dedupe_key in dedupe_keys:
        if dedupe_key in recent_visible_reply_materialization_by_key:
            return {
                'dedupe_keys': dedupe_keys,
                'duplicate': True,
                'matched_key': dedupe_key,
            }
    for dedupe_key in dedupe_keys:
        recent_visible_reply_materialization_by_key[dedupe_key] = now_ms
    return {
        'dedupe_keys': dedupe_keys,
        'duplicate': False,
        'matched_key': '',
    }

def forget_recent_visible_reply_materialization(dedupe_keys):
    if not isinstance(dedupe_keys, list):
        return
    for dedupe_key in dedupe_keys:
        normalized_key = str(dedupe_key or '').strip()
        if normalized_key == '':
            continue
        recent_visible_reply_materialization_by_key.pop(normalized_key, None)

def normalize_probe_status(status, fallback='not_checked'):
    normalized = str(status or '').strip().lower()
    if normalized in ['ok', 'mismatch', 'degraded', 'failed']:
        return normalized
    return fallback

def normalize_probe_result_details(value, default_value):
    if isinstance(value, dict):
        return value
    return default_value

def normalize_probe_result_key_list(value):
    if not isinstance(value, list):
        return []
    items = []
    for item in value:
        item_str = str(item or '').strip()
        if item_str != '':
            items.append(item_str)
    return items

def normalize_probe_repair_counts(value):
    if isinstance(value, dict):
        raw = value
    else:
        raw = parse_json_object(value) or {}
    counts = {}
    for key in ['config_patch', 'preset_prompt', 'session_map_runtime']:
        try:
            counts[key] = max(0, int(raw.get(key, 0) or 0))
        except Exception:
            counts[key] = 0
    return counts

def build_pending_probe_state_updates(probe_id):
    return {
        'last_probe_id': str(probe_id).strip(),
        'last_probe_at': int(time.time() * 1000),
        'probe_completed': 'false',
        'probe_timeout': 'false',
        'health_probe_status': 'not_checked',
        'health_probe_details': '{}',
        'account_config_status': 'not_checked',
        'account_config_details': '{}',
        'config_patch_status': 'not_checked',
        'config_patch_mismatched_keys': '[]',
        'config_patch_failed_keys': '[]',
        'preset_prompt_content_status': 'not_checked',
        'preset_prompt_hook_status': 'not_checked',
        'preset_prompt_hook_missing_requirements': '[]',
        'workspace_files_status': 'not_checked',
        'workspace_files_details': '{}',
        'session_map_runtime_status': 'not_checked',
        'session_map_runtime_details': '{}',
        'online_marker_status': 'not_checked',
        'online_marker_details': '{}',
    }

def build_presence_state_updates(status, source, updated_at=None):
    return {
        'presence_status': str(status or NODE_PRESENCE_UNKNOWN).strip() or NODE_PRESENCE_UNKNOWN,
        'presence_source': str(source or NODE_PRESENCE_SOURCE_UNKNOWN).strip() or NODE_PRESENCE_SOURCE_UNKNOWN,
        'presence_updated_at': int(updated_at or int(time.time() * 1000)),
    }

def build_node_probe_state_updates(event):
    now_ms = int(time.time() * 1000)
    results = event.get('results', {}) if isinstance(event.get('results', {}), dict) else {}
    health_result = results.get('health', {}) if isinstance(results.get('health', {}), dict) else {}
    account_config_result = results.get('account_config', {}) if isinstance(results.get('account_config', {}), dict) else {}
    config_patch_result = results.get('config_patch', {}) if isinstance(results.get('config_patch', {}), dict) else {}
    preset_prompt_content_result = results.get('preset_prompt_content', {}) if isinstance(results.get('preset_prompt_content', {}), dict) else {}
    preset_prompt_hook_result = results.get('preset_prompt_hook', {}) if isinstance(results.get('preset_prompt_hook', {}), dict) else {}
    workspace_files_result = results.get('workspace_files', {}) if isinstance(results.get('workspace_files', {}), dict) else {}
    session_map_runtime_result = results.get('session_map_runtime', {}) if isinstance(results.get('session_map_runtime', {}), dict) else {}
    online_marker_result = results.get('online_marker', {}) if isinstance(results.get('online_marker', {}), dict) else {}
    updates = {
        'last_probe_id': str(event.get('probe_id', '')).strip(),
        'last_probe_report_at': int(event.get('reported_at', 0) or 0) if str(event.get('reported_at', '')).strip() != '' else now_ms,
        'probe_completed': 'true',
        'probe_timeout': 'false',
        'plugin_version': str(event.get('plugin_version', '')).strip(),
        'api_version': str(event.get('api_version', '')).strip(),
        'health_probe_status': normalize_probe_status(health_result.get('status')),
        'health_probe_details': json.dumps(normalize_probe_result_details(health_result.get('details'), {}), ensure_ascii=False, separators=(',', ':')),
        'account_config_status': normalize_probe_status(account_config_result.get('status')),
        'account_config_details': json.dumps(normalize_probe_result_details(account_config_result.get('details'), {}), ensure_ascii=False, separators=(',', ':')),
        'config_patch_status': normalize_probe_status(config_patch_result.get('status')),
        'config_patch_mismatched_keys': json.dumps(normalize_probe_result_key_list((config_patch_result.get('details') or {}).get('mismatched_keys', [])), ensure_ascii=False, separators=(',', ':')),
        'config_patch_failed_keys': json.dumps(normalize_probe_result_key_list((config_patch_result.get('details') or {}).get('failed_keys', [])), ensure_ascii=False, separators=(',', ':')),
        'preset_prompt_content_status': normalize_probe_status(preset_prompt_content_result.get('status')),
        'preset_prompt_hook_status': normalize_probe_status(preset_prompt_hook_result.get('status')),
        'preset_prompt_hook_missing_requirements': json.dumps(normalize_probe_result_key_list((preset_prompt_hook_result.get('details') or {}).get('missing_requirements', [])), ensure_ascii=False, separators=(',', ':')),
        'workspace_files_status': normalize_probe_status(workspace_files_result.get('status')),
        'workspace_files_details': json.dumps(normalize_probe_result_details(workspace_files_result.get('details'), {}), ensure_ascii=False, separators=(',', ':')),
        'session_map_runtime_status': normalize_probe_status(session_map_runtime_result.get('status')),
        'session_map_runtime_details': json.dumps(normalize_probe_result_details(session_map_runtime_result.get('details'), {}), ensure_ascii=False, separators=(',', ':')),
        'online_marker_status': normalize_probe_status(online_marker_result.get('status')),
        'online_marker_details': json.dumps(normalize_probe_result_details(online_marker_result.get('details'), {}), ensure_ascii=False, separators=(',', ':')),
        'probe_repair_last_probe_id': str(event.get('probe_id', '')).strip(),
    }
    updates.update(build_presence_state_updates(
        NODE_PRESENCE_ONLINE,
        NODE_PRESENCE_SOURCE_ONLINE_MARKER,
        updates['last_probe_report_at'],
    ))
    return updates

def build_probe_issue_categories(node_info, event):
    results = event.get('results', {}) if isinstance(event.get('results', {}), dict) else {}
    categories = set()
    config_patch_result = results.get('config_patch', {}) if isinstance(results.get('config_patch', {}), dict) else {}
    preset_prompt_content_result = results.get('preset_prompt_content', {}) if isinstance(results.get('preset_prompt_content', {}), dict) else {}
    preset_prompt_hook_result = results.get('preset_prompt_hook', {}) if isinstance(results.get('preset_prompt_hook', {}), dict) else {}
    workspace_files_result = results.get('workspace_files', {}) if isinstance(results.get('workspace_files', {}), dict) else {}
    session_map_runtime_result = results.get('session_map_runtime', {}) if isinstance(results.get('session_map_runtime', {}), dict) else {}
    if normalize_probe_status(config_patch_result.get('status')) in ['mismatch', 'failed']:
        categories.add('config_patch')
    has_bound_chatbot = has_node_bound_chatbot(node_info.get('app_id', ''), node_info.get('node_id', ''))
    if has_bound_chatbot:
        if normalize_probe_status(preset_prompt_content_result.get('status')) in ['mismatch', 'failed']:
            categories.add('preset_prompt')
        if normalize_probe_status(preset_prompt_hook_result.get('status')) in ['mismatch', 'failed']:
            categories.add('preset_prompt')
        if normalize_probe_status(workspace_files_result.get('status')) in ['degraded', 'failed']:
            categories.add('preset_prompt')
    if normalize_probe_status(session_map_runtime_result.get('status')) == 'mismatch':
        categories.add('session_map_runtime')
    return categories

def update_probe_repair_counts(app_id, node_id, counts):
    update_node_field(
        app_id,
        node_id,
        PROBE_REPAIR_COUNT_FIELD,
        json.dumps(normalize_probe_repair_counts(counts), ensure_ascii=False, separators=(',', ':'))
    )

def maybe_auto_repair_probe_mismatch(node_info, event):
    app_id = str(node_info.get('app_id', '')).strip()
    node_id = str(node_info.get('node_id', '')).strip()
    if app_id == '' or node_id == '':
        return
    counts = normalize_probe_repair_counts(node_info.get(PROBE_REPAIR_COUNT_FIELD, '{}'))
    issue_categories = build_probe_issue_categories(node_info, event)
    changed = False
    for category in ['config_patch', 'preset_prompt', 'session_map_runtime']:
        if category not in issue_categories and counts.get(category, 0) != 0:
            counts[category] = 0
            changed = True
    actions_triggered = False
    model_patch_config = None
    if 'config_patch' in issue_categories and counts.get('config_patch', 0) < PROBE_AUTO_REPAIR_MAX_ATTEMPTS:
        model_patch_config = get_model_patch_config(app_id, node_id)
        update_node_config(app_id, node_id, model_patch_config)
        counts['config_patch'] = counts.get('config_patch', 0) + 1
        changed = True
        actions_triggered = True
        logging.info(
            f"probe auto repair config_patch | app_id:{app_id}, node_id:{node_id}, attempt:{counts['config_patch']}"
        )
    if 'preset_prompt' in issue_categories and counts.get('preset_prompt', 0) < PROBE_AUTO_REPAIR_MAX_ATTEMPTS:
        chatbot_id = str(get_node_chatbot_id(app_id, node_id) or '').strip()
        if chatbot_id != '':
            sync_bound_chatbot_preset_prompt(app_id, node_id, chatbot_id)
            counts['preset_prompt'] = counts.get('preset_prompt', 0) + 1
            changed = True
            actions_triggered = True
            logging.info(
                f"probe auto repair preset_prompt | app_id:{app_id}, node_id:{node_id}, attempt:{counts['preset_prompt']}, chatbot_id:{chatbot_id}"
            )
    if 'session_map_runtime' in issue_categories and counts.get('session_map_runtime', 0) < PROBE_AUTO_REPAIR_MAX_ATTEMPTS:
        refreshed_node = get_node(app_id, node_id) or node_info
        sync_session_map_settings_to_node(refreshed_node)
        sync_session_mapping_snapshot_to_node(refreshed_node)
        counts['session_map_runtime'] = counts.get('session_map_runtime', 0) + 1
        changed = True
        actions_triggered = True
        logging.info(
            f"probe auto repair session_map_runtime | app_id:{app_id}, node_id:{node_id}, attempt:{counts['session_map_runtime']}"
        )
    if changed:
        update_probe_repair_counts(app_id, node_id, counts)
    if actions_triggered:
        refreshed_node = get_node(app_id, node_id) or node_info
        schedule_probe_to_node(
            refreshed_node,
            build_default_probe_checks(refreshed_node, model_patch_config),
            delay_ms=PROBE_AUTO_REPAIR_DELAY_MS,
        )

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
            update_node_fields(app_id, node_id, build_presence_state_updates(
                NODE_PRESENCE_ONLINE,
                NODE_PRESENCE_SOURCE_ONLINE_MARKER,
            ))
            node = get_node(app_id, node_id) or node
            logging.info(f"update node versions | node_id: {node_id}, plugin_version:{plugin_version}, api_version:{api_version}")
            if node['status'] == 'wait':
                logging.info(f"change node status to normal | node_id: {node_id}")
                update_node_field(app_id, node_id, 'status', 'normal')
                model_patch_config = get_model_patch_config(app_id, node_id)
                update_node_config(app_id, node_id, model_patch_config)
                maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
                node = get_node(app_id, node_id) or node
                sync_session_map_settings_to_node(node)
                sync_session_mapping_snapshot_to_node(node)
                schedule_probe_to_node(
                    node,
                    build_default_probe_checks(node, model_patch_config),
                    delay_ms=PROBE_POST_SYNC_DELAY_MS,
                )
                continue
            elif 'provider_inited' in event and event['provider_inited'] == False:
                logging.info(f"update node config for provider_inited is false | node_id: {node_id}")
                model_patch_config = get_model_patch_config(app_id, node_id)
                update_node_config(app_id, node_id, model_patch_config)
                maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
                node = get_node(app_id, node_id) or node
                sync_session_map_settings_to_node(node)
                sync_session_mapping_snapshot_to_node(node)
                schedule_probe_to_node(
                    node,
                    build_default_probe_checks(node, model_patch_config),
                    delay_ms=PROBE_POST_SYNC_DELAY_MS,
                )
                continue
            sync_session_map_settings_to_node(node)
            sync_session_mapping_snapshot_to_node(node)
            send_probe_to_node(node)
    elif event['type'] == 'offline':
        node_list = get_nodes_by_user_id(app_id, user_id)
        for node in node_list:
            node_id = node['node_id']
            logging.info(f"mark node presence offline by offline marker | node_id: {node_id}")
            update_node_fields(app_id, node_id, build_presence_state_updates(
                NODE_PRESENCE_OFFLINE,
                NODE_PRESENCE_SOURCE_OFFLINE_MARKER,
            ))
    elif event['type'] == 'probe_report':
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command probe_report | ctype: {ctype}, event: {event}")
            return
        node_list = get_nodes_by_user_id(app_id, user_id)
        for node in node_list:
            node_id = str(node.get('node_id', '')).strip()
            current_probe_id = str(node.get('last_probe_id', '')).strip()
            report_probe_id = str(event.get('probe_id', '')).strip()
            if current_probe_id != '' and report_probe_id != '' and current_probe_id != report_probe_id:
                logging.info(
                    f"handle_client_event skip stale probe_report | app_id:{app_id}, node_id:{node_id}, "
                    f"current_probe_id:{current_probe_id}, report_probe_id:{report_probe_id}"
                )
                continue
            update_node_fields(app_id, node_id, build_node_probe_state_updates(event))
    elif event['type'] == 'config_sync_report':
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command config_sync_report | ctype: {ctype}, event: {event}")
            return
        if str(event.get('object_type', '')).strip() != 'config_patch':
            logging.info(f"handle_client_event skip non config_patch config_sync_report | event: {event}")
            return
        node_list = get_nodes_by_user_id(app_id, user_id)
        for node in node_list:
            node_id = str(node.get('node_id', '')).strip()
            current_sync_id = str(node.get('last_config_sync_id', '')).strip()
            report_sync_id = str(event.get('sync_id', '')).strip()
            if current_sync_id != '' and report_sync_id != '' and current_sync_id != report_sync_id:
                logging.info(
                    f"handle_client_event skip stale config_sync_report | app_id:{app_id}, node_id:{node_id}, "
                    f"current_sync_id:{current_sync_id}, report_sync_id:{report_sync_id}"
                )
                continue
            update_node_fields(app_id, node_id, build_config_sync_state_updates(event))
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
        suppression_reason = str(event.get('suppression_reason', '')).strip()
        if suppression_reason == 'duplicate_parent_after_subagent':
            logging.info(
                f"handle_client_event skip router_reply by plugin suppression hint | "
                f"app_id:{app_id}, user_id:{user_id}, suppression_reason:{suppression_reason}"
            )
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
    elif event['type'] in ['session_message_sync', 'session_transcript_observed']:
        if ctype != 'COMMAND':
            logging.info(f"handle_client_event skip not command transcript sync event | ctype: {ctype}, event: {event}")
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
    openclaw_ext = ext.get('openclaw', {}) if isinstance(ext.get('openclaw', {}), dict) else {}
    visible_reply_dedupe_state = reserve_recent_visible_reply_materialization(
        session_key=openclaw_ext.get('session', ''),
        source=openclaw_ext.get('source', ''),
        role=openclaw_ext.get('role', ''),
        message_id=openclaw_ext.get('message_id', ''),
        request_msg_id=openclaw_ext.get('request_msg_id', '') or openclaw_ext.get('trigger_msg_id', ''),
        text=content,
    )
    visible_reply_dedupe_keys = visible_reply_dedupe_state.get('dedupe_keys', [])
    if visible_reply_dedupe_state.get('duplicate'):
        logging.info(
            f"visible_reply_dedupe duplicate_skip_router_reply_entry | "
            f"app_id:{app_id}, node_id:{node_id}, session_key:{normalize_session_key(openclaw_ext.get('session', ''))}, "
            f"request_msg_id:{str(openclaw_ext.get('request_msg_id', '') or openclaw_ext.get('trigger_msg_id', '')).strip()}, "
            f"to_id:{to_id}"
        )
        return
    extra = {
        'ext': ext
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, chatbot_user_id, to_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        forget_recent_visible_reply_materialization(visible_reply_dedupe_keys)
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
    refreshed_node = get_node(app_id, node_id) or node_info
    probe_delay_ms = PROBE_POST_SYNC_DELAY_MS if (sync_preset_prompt and has_node_bound_chatbot(app_id, node_id)) else 0
    schedule_probe_to_node(
        refreshed_node,
        build_default_probe_checks(refreshed_node, model_patch_config),
        delay_ms=probe_delay_ms,
    )
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }    

def sync_model_config_and_wait(app_id, node_id, wait_timeout_ms=CONFIG_SYNC_WAIT_TIMEOUT_MS):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    if not is_config_sync_wait_supported(node_info):
        legacy_result = sync_model_config(app_id, node_id, True)
        if legacy_result.get('result') == 'error':
            return legacy_result
        latest_node = get_node(app_id, node_id) or node_info
        return {
            'result': 'ok',
            'data': build_config_sync_response_data(True, False, False, True, '', latest_node)
        }
    timeout_ms = clamp_config_sync_wait_timeout_ms(wait_timeout_ms)
    model_patch_config = get_model_patch_config(app_id, node_id)
    sync_id = secrets.token_hex(16)
    send_result = update_node_config(app_id, node_id, model_patch_config, sync_id=sync_id)
    if send_result.get('result') == 'error':
        return send_result
    maybe_sync_node_bound_chatbot_preset_prompt(app_id, node_id)
    started_report_at = int(node_info.get('last_config_sync_report_at', 0) or 0)
    deadline = int(time.time() * 1000) + timeout_ms
    while int(time.time() * 1000) < deadline:
        latest_node = get_node(app_id, node_id)
        if latest_node is None:
            return {
                'result': 'error',
                'message': 'node not exist'
            }
        if is_matching_config_sync_report(latest_node, sync_id, started_report_at):
            success = str(latest_node.get('last_config_sync_status', '')).strip().lower() == CONFIG_SYNC_STATUS_OK
            return {
                'result': 'ok',
                'data': build_config_sync_response_data(True, True, False, False, sync_id, latest_node)
            } if success else {
                'result': 'ok',
                'data': build_config_sync_response_data(True, True, False, False, sync_id, latest_node)
            }
        time.sleep(CONFIG_SYNC_WAIT_POLL_INTERVAL_MS / 1000.0)
    latest_node = get_node(app_id, node_id) or node_info
    if is_matching_config_sync_report(latest_node, sync_id, started_report_at):
        return {
            'result': 'ok',
            'data': build_config_sync_response_data(True, True, False, False, sync_id, latest_node)
        }
    return {
        'result': 'ok',
        'data': build_config_sync_response_data(True, False, True, False, sync_id, latest_node)
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
    models.sort(key=lambda item: (
        str(item.get('id', '')),
        str(item.get('name', '')),
        1 if item.get('reasoning') else 0,
        int(item.get('contextWindow', 0) or 0),
        int(item.get('maxTokens', 0) or 0),
    ))
    return models

def resolve_probe_preset_prompt_payload(node_info):
    if node_info is None:
        return {
            'chatbot_id': '',
            'chatbot_name': '',
            'prompt': ''
        }
    app_id = str(node_info.get('app_id', '')).strip()
    node_id = str(node_info.get('node_id', '')).strip()
    chatbot_id = get_node_chatbot_id(app_id, node_id)
    if chatbot_id is None or str(chatbot_id).strip() == '':
        return {
            'chatbot_id': '',
            'chatbot_name': '',
            'prompt': ''
        }
    chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot_info is None:
        return {
            'chatbot_id': str(chatbot_id),
            'chatbot_name': '',
            'prompt': ''
        }
    prompt = normalize_preset_prompt_for_agents_md(
        extract_system_prompt_text_from_preset(chatbot_info.get('preset', {}))
    )
    return {
        'chatbot_id': str(chatbot_id),
        'chatbot_name': str(chatbot_info.get('name', '')),
        'prompt': prompt
    }

def build_default_probe_checks(node_info, patch_config=None):
    if patch_config is None:
        patch_config = get_model_patch_config(node_info.get('app_id', ''), node_info.get('node_id', ''))
    config_items = []
    for entry in build_config_batch_entries_from_patch_config(patch_config):
        config_items.append({
            'path': entry['path'],
            'expected_hash': build_probe_value_hash(entry.get('value'), True, entry['path']),
        })
    prompt_payload = resolve_probe_preset_prompt_payload(node_info)
    expected_prompt_content = build_managed_agents_content(
        prompt_payload.get('chatbot_id', ''),
        prompt_payload.get('chatbot_name', ''),
        prompt_payload.get('prompt', '')
    )
    has_prompt_probe = str(prompt_payload.get('chatbot_id', '')).strip() != ''
    checks = {
        'health': {},
        'account_config': {},
        'config_patch': {
            'items': config_items
        },
        'session_map_runtime': {
            'expected_session_map_sync_enabled': is_session_map_sync_enabled(node_info),
            'expected_merge_sub_sessions_enabled': is_merge_sub_sessions_enabled(node_info),
            'expected_effective_enabled': is_session_map_sync_enabled(node_info)
        },
        'online_marker': {}
    }
    if has_prompt_probe:
        checks['preset_prompt_content'] = {
            'expected_hash': hashlib.sha256(expected_prompt_content.encode('utf-8')).hexdigest()
        }
        checks['preset_prompt_hook'] = {
            'required_path': OPENCLAW_MANAGED_AGENTS_PATH
        }
        checks['workspace_files'] = {}
    return checks

def _delayed_send_probe_to_node(app_id, node_id, delay_ms):
    try:
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
        node_info = get_node(app_id, node_id)
        if node_info is None:
            logging.info(f"delayed probe skipped for missing node | app_id:{app_id}, node_id:{node_id}")
            return
        send_probe_to_node(node_info, build_default_probe_checks(node_info))
    except Exception:
        logging.exception(f"delayed probe failed | app_id:{app_id}, node_id:{node_id}")

def schedule_probe_to_node(node_info, checks=None, delay_ms=0):
    if delay_ms <= 0:
        return send_probe_to_node(node_info, checks)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    app_id = str(node_info.get('app_id', '')).strip()
    node_id = str(node_info.get('node_id', '')).strip()
    if checks is None:
        checks = build_default_probe_checks(node_info)
    executor.submit(_delayed_send_probe_to_node, app_id, node_id, int(delay_ms))
    return {
        'result': 'ok',
        'data': {
            'scheduled': True,
            'delay_ms': int(delay_ms)
        }
    }

def send_probe_to_node(node_info, checks=None):
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    app_id = str(node_info.get('app_id', '')).strip()
    user_id = str(node_info.get('user_id', '')).strip()
    node_id = str(node_info.get('node_id', '')).strip()
    if app_id == '' or user_id == '' or node_id == '':
        return {
            'result': 'error',
            'message': 'bad node info'
        }
    latest_node = get_node(app_id, node_id) or node_info
    if is_probe_inflight(latest_node):
        return {
            'result': 'ok',
            'data': {
                'probe_id': str(latest_node.get('last_probe_id', '')).strip(),
                'reused': True
            }
        }
    if checks is None:
        checks = build_default_probe_checks(node_info)
    probe_id = secrets.token_hex(16)
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    config = {
        'lanying_admin_token': admin_token
    }
    extra = {
        'ext': {
            'openclaw': {
                'type': 'probe',
                'probe_id': probe_id,
                'formatVersion': OPENCLAW_PROBE_FORMAT_VERSION,
                'checks': checks
            }
        },
        'skip_antispam_prompt': True
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, user_id, user_id, 1, 6, '', extra)
    if msg_id <= 0:
        return {
            'result': 'error',
            'message': 'send message failed'
        }
    update_node_fields(app_id, node_id, build_pending_probe_state_updates(probe_id))
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_id,
            'probe_id': probe_id
        }
    }

def probe_node(app_id, node_id, wait_timeout_ms=PROBE_WAIT_TIMEOUT_MS, wait_for_fresh_report=True):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    if resolve_probe_support_state(node_info) == 'unsupported':
        latest_node = get_node(app_id, node_id) or node_info
        return {
            'result': 'ok',
            'data': build_probe_response_data(False, True, False, '', latest_node)
        }
    timeout_ms = clamp_probe_wait_timeout_ms(wait_timeout_ms)
    triggered = False
    reuse_window_ms = max(timeout_ms, PROBE_INFLIGHT_REUSE_WINDOW_MS)
    if is_probe_inflight(node_info, reuse_window_ms):
        probe_id = str(node_info.get('last_probe_id', '')).strip()
    else:
        send_result = send_probe_to_node(node_info, build_default_probe_checks(node_info))
        if send_result.get('result') == 'error':
            return send_result
        probe_id = str((send_result.get('data') or {}).get('probe_id', '')).strip()
        triggered = not parse_bool_flag((send_result.get('data') or {}).get('reused'))
    if probe_id == '':
        return {
            'result': 'error',
            'message': 'probe id is missing'
        }
    if not parse_bool_flag(wait_for_fresh_report):
        latest_node = get_node(app_id, node_id) or node_info
        return {
            'result': 'ok',
            'data': build_probe_response_data(triggered, False, False, probe_id, latest_node)
        }
    started_report_at = int(node_info.get('last_probe_report_at', 0) or 0)
    deadline = int(time.time() * 1000) + timeout_ms
    while int(time.time() * 1000) < deadline:
        latest_node = get_node(app_id, node_id)
        if latest_node is None:
            return {
                'result': 'error',
                'message': 'node not exist'
            }
        if is_matching_probe_report(latest_node, probe_id, started_report_at):
            return {
                'result': 'ok',
                'data': build_probe_response_data(triggered, True, False, probe_id, latest_node)
            }
        time.sleep(PROBE_WAIT_POLL_INTERVAL_MS / 1000.0)
    latest_node = get_node(app_id, node_id) or node_info
    if is_matching_probe_report(latest_node, probe_id, started_report_at):
        return {
            'result': 'ok',
            'data': build_probe_response_data(triggered, True, False, probe_id, latest_node)
        }
    current_probe_id = str(latest_node.get('last_probe_id', '')).strip()
    if current_probe_id == probe_id:
        timeout_report_at = int(time.time() * 1000)
        timeout_updates = {
            'last_probe_report_at': timeout_report_at,
            'probe_repair_last_probe_id': probe_id,
            'probe_completed': 'true',
            'probe_timeout': 'true'
        }
        timeout_updates.update(build_presence_state_updates(
            NODE_PRESENCE_OFFLINE,
            NODE_PRESENCE_SOURCE_PROBE_TIMEOUT,
            timeout_report_at,
        ))
        update_node_fields(app_id, node_id, timeout_updates)
        latest_node = get_node(app_id, node_id) or latest_node
    return {
        'result': 'ok',
        'data': build_probe_response_data(triggered, False, True, probe_id, latest_node)
    }

def build_config_batch_entries_from_patch_config(patch_config, path_prefix=''):
    if not isinstance(patch_config, dict):
        return []
    batch_entries = []
    for key, value in patch_config.items():
        key_str = str(key).strip()
        if key_str == '':
            continue
        next_path = f"{path_prefix}.{key_str}" if path_prefix else key_str
        if isinstance(value, dict):
            batch_entries.extend(build_config_batch_entries_from_patch_config(value, next_path))
            continue
        batch_entries.append({
            'path': next_path,
            'value': value,
        })
    return batch_entries

def update_node_config(app_id, node_id, patch_config, sync_id=None):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    user_id = node_info['user_id']
    content = ''
    admin_token = lanying_config.get_lanying_admin_token(app_id)
    batch_entries = build_config_batch_entries_from_patch_config(patch_config)
    config = {
        'lanying_admin_token': admin_token
    }
    send_msg_type = 1
    content_type = 6
    sync_id_str = str(sync_id or '').strip()
    if sync_id_str != '':
        update_node_fields(app_id, node_id, build_pending_config_sync_state_updates(sync_id_str))
    extra = {
        'ext': {
            'openclaw': {
                'type': 'config_patch',
                'formatVersion': 3,
                'restart': True,
                'sync_id': sync_id_str,
                'raw': json.dumps(patch_config),
                'batchEntries': batch_entries,
                'batch_entries': batch_entries
            },
        },
        'skip_antispam_prompt': True
    }
    msg_id = lanying_im_api.send_message_sync(config, app_id, user_id, user_id, send_msg_type, content_type, content, extra)
    if msg_id <= 0:
        if sync_id_str != '':
            update_node_fields(app_id, node_id, {
                'last_config_sync_id': sync_id_str,
                'last_config_sync_status': CONFIG_SYNC_STATUS_FAILED,
                'last_config_sync_error_code': 'send_message_failed',
                'last_config_sync_error_message': 'send message failed'
            })
        return {
            'result': 'error',
            'message': 'send message failed'
        }
    return {
        'result': 'ok',
        'data': {
            'msg_id': msg_id,
            'sync_id': sync_id_str,
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

def update_node_fields(app_id, node_id, fields):
    node_info = get_node(app_id, node_id)
    if node_info is None:
        return {
            'result': 'error',
            'message': 'node not exist'
        }
    redis = lanying_redis.get_redis_connection()
    redis.hmset(get_node_key(app_id, node_id), fields)
    return {
        'result': 'ok'
    }

def get_probe_check_statuses(node_info):
    if not isinstance(node_info, dict):
        return []
    statuses = []
    for field_name in [
        'health_probe_status',
        'account_config_status',
        'config_patch_status',
        'preset_prompt_content_status',
        'preset_prompt_hook_status',
        'workspace_files_status',
        'session_map_runtime_status',
        'online_marker_status',
    ]:
        statuses.append(normalize_probe_status(node_info.get(field_name), 'not_checked'))
    return statuses

def resolve_probe_summary_text(node_info):
    if not isinstance(node_info, dict):
        return 'not_checked'
    support_state = resolve_probe_support_state(node_info)
    if str(node_info.get('status', '')).strip() == 'normal' and support_state == 'unsupported':
        return 'unsupported'
    if parse_bool_flag(node_info.get('probe_timeout')):
        return 'failed'
    statuses = get_probe_check_statuses(node_info)
    if len(statuses) == 0 or all(status == 'not_checked' for status in statuses):
        return 'not_checked'
    if 'failed' in statuses:
        return 'failed'
    if 'mismatch' in statuses or 'degraded' in statuses:
        return 'partial_issue'
    if 'ok' in statuses:
        return 'ok'
    return 'not_checked'

def enrich_node_probe_snapshot(node_info):
    if not isinstance(node_info, dict):
        return node_info
    snapshot = dict(node_info)
    snapshot['presence_source'] = normalize_presence_source(snapshot.get('presence_source'))
    snapshot['presence_status'] = normalize_presence_status(snapshot.get('presence_status'))
    if snapshot['presence_status'] == NODE_PRESENCE_UNKNOWN:
        if snapshot['presence_source'] in [NODE_PRESENCE_SOURCE_OFFLINE_MARKER, NODE_PRESENCE_SOURCE_PROBE_TIMEOUT]:
            snapshot['presence_status'] = NODE_PRESENCE_OFFLINE
        elif snapshot['presence_source'] == NODE_PRESENCE_SOURCE_ONLINE_MARKER:
            snapshot['presence_status'] = NODE_PRESENCE_ONLINE
    if 'presence_updated_at' not in snapshot or str(snapshot.get('presence_updated_at', '')).strip() == '':
        snapshot['presence_updated_at'] = 0
    else:
        snapshot['presence_updated_at'] = int(snapshot.get('presence_updated_at') or 0)
    snapshot['probe_support_state'] = resolve_probe_support_state(snapshot)
    snapshot['probe_supported'] = snapshot['probe_support_state'] == 'supported'
    summary_text = resolve_probe_summary_text(snapshot)
    snapshot['probe_summary_text'] = summary_text
    snapshot['probe_in_sync'] = summary_text == 'ok'
    cached_at = int(snapshot.get('last_probe_report_at', 0) or 0) if str(snapshot.get('last_probe_report_at', '')).strip() != '' else 0
    snapshot['probe_cached_at'] = cached_at
    if 'probe_completed' in snapshot:
        snapshot['probe_completed'] = parse_bool_flag(snapshot.get('probe_completed'))
    else:
        snapshot['probe_completed'] = cached_at > 0 and summary_text != 'not_checked'
    if 'probe_timeout' in snapshot:
        snapshot['probe_timeout'] = parse_bool_flag(snapshot.get('probe_timeout'))
    else:
        snapshot['probe_timeout'] = False
    return snapshot

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
        if 'presence_status' not in dto or str(dto.get('presence_status', '')).strip() == '':
            dto['presence_status'] = NODE_PRESENCE_UNKNOWN
        else:
            dto['presence_status'] = normalize_presence_status(dto.get('presence_status'))
        if 'presence_source' not in dto or str(dto.get('presence_source', '')).strip() == '':
            dto['presence_source'] = NODE_PRESENCE_SOURCE_UNKNOWN
        else:
            dto['presence_source'] = normalize_presence_source(dto.get('presence_source'))
        if 'presence_updated_at' not in dto or str(dto.get('presence_updated_at', '')).strip() == '':
            dto['presence_updated_at'] = 0
        else:
            dto['presence_updated_at'] = int(dto.get('presence_updated_at') or 0)
        if 'session_map_sync' not in dto or str(dto.get('session_map_sync', '')).strip() == '':
            dto['session_map_sync'] = 'off'
        if 'merge_sub_sessions' not in dto or str(dto.get('merge_sub_sessions', '')).strip() == '':
            dto['merge_sub_sessions'] = 'off'
        if 'last_probe_id' not in dto:
            dto['last_probe_id'] = ''
        if 'last_config_sync_id' not in dto:
            dto['last_config_sync_id'] = ''
        if 'last_config_sync_at' not in dto or str(dto.get('last_config_sync_at', '')).strip() == '':
            dto['last_config_sync_at'] = 0
        else:
            dto['last_config_sync_at'] = int(dto['last_config_sync_at'])
        if 'last_config_sync_report_at' not in dto or str(dto.get('last_config_sync_report_at', '')).strip() == '':
            dto['last_config_sync_report_at'] = 0
        else:
            dto['last_config_sync_report_at'] = int(dto['last_config_sync_report_at'])
        if 'last_config_sync_status' not in dto:
            dto['last_config_sync_status'] = ''
        if 'last_config_sync_error_code' not in dto:
            dto['last_config_sync_error_code'] = ''
        if 'last_config_sync_error_message' not in dto:
            dto['last_config_sync_error_message'] = ''
        if 'last_probe_at' not in dto or str(dto.get('last_probe_at', '')).strip() == '':
            dto['last_probe_at'] = 0
        else:
            dto['last_probe_at'] = int(dto['last_probe_at'])
        if 'last_probe_report_at' not in dto or str(dto.get('last_probe_report_at', '')).strip() == '':
            dto['last_probe_report_at'] = 0
        else:
            dto['last_probe_report_at'] = int(dto['last_probe_report_at'])
        if 'probe_completed' not in dto:
            dto['probe_completed'] = dto['last_probe_report_at'] > 0
        else:
            dto['probe_completed'] = parse_bool_flag(dto.get('probe_completed'))
        if 'probe_timeout' not in dto:
            dto['probe_timeout'] = False
        else:
            dto['probe_timeout'] = parse_bool_flag(dto.get('probe_timeout'))
        if PROBE_REPAIR_COUNT_FIELD not in dto:
            dto[PROBE_REPAIR_COUNT_FIELD] = {}
        else:
            dto[PROBE_REPAIR_COUNT_FIELD] = normalize_probe_repair_counts(dto.get(PROBE_REPAIR_COUNT_FIELD, '{}'))
        if 'probe_repair_last_probe_id' not in dto:
            dto['probe_repair_last_probe_id'] = ''
        if 'health_probe_status' not in dto:
            dto['health_probe_status'] = 'not_checked'
        dto['health_probe_details'] = lanying_utils.safe_json_loads(dto.get('health_probe_details', '{}'), {})
        if 'account_config_status' not in dto:
            dto['account_config_status'] = 'not_checked'
        dto['account_config_details'] = lanying_utils.safe_json_loads(dto.get('account_config_details', '{}'), {})
        if 'config_patch_status' not in dto:
            dto['config_patch_status'] = 'not_checked'
        dto['config_patch_mismatched_keys'] = lanying_utils.safe_json_loads(dto.get('config_patch_mismatched_keys', '[]'), [])
        dto['config_patch_failed_keys'] = lanying_utils.safe_json_loads(dto.get('config_patch_failed_keys', '[]'), [])
        if 'preset_prompt_content_status' not in dto:
            dto['preset_prompt_content_status'] = 'not_checked'
        if 'preset_prompt_hook_status' not in dto:
            dto['preset_prompt_hook_status'] = 'not_checked'
        dto['preset_prompt_hook_missing_requirements'] = lanying_utils.safe_json_loads(dto.get('preset_prompt_hook_missing_requirements', '[]'), [])
        if 'workspace_files_status' not in dto:
            dto['workspace_files_status'] = 'not_checked'
        dto['workspace_files_details'] = lanying_utils.safe_json_loads(dto.get('workspace_files_details', '{}'), {})
        if 'session_map_runtime_status' not in dto:
            dto['session_map_runtime_status'] = 'not_checked'
        dto['session_map_runtime_details'] = lanying_utils.safe_json_loads(dto.get('session_map_runtime_details', '{}'), {})
        if 'online_marker_status' not in dto:
            dto['online_marker_status'] = 'not_checked'
        dto['online_marker_details'] = lanying_utils.safe_json_loads(dto.get('online_marker_details', '{}'), {})
        dto['chatbot_id'] = ''
        chatbot_id = get_node_chatbot_id(app_id, node_id)
        if chatbot_id is not None:
            chatbot_info = lanying_chatbot.get_chatbot(app_id, chatbot_id)
            if chatbot_info is not None:
                dto['chatbot_id'] = chatbot_id
        return enrich_node_probe_snapshot(dto)
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

def check_rebind_chatbot(app_id, chatbot_id, old_node_id, new_node_id):
    old_node_id = str(old_node_id or '').strip()
    new_node_id = str(new_node_id or '').strip()
    chatbot_id = str(chatbot_id or '').strip()
    if chatbot_id == '':
        return {
            'result': 'error',
            'message': 'chatbot not exist'
        }
    if old_node_id == new_node_id:
        return {
            'result': 'ok'
        }
    if new_node_id != '':
        target_node_info = get_node(app_id, new_node_id)
        if target_node_info is None:
            return {
                'result': 'error',
                'message': 'node not exist'
            }
        target_chatbot_id = str(get_node_chatbot_id(app_id, new_node_id) or '').strip()
        if target_chatbot_id not in ['', chatbot_id]:
            return {
                'result': 'error',
                'message': 'chatbot already bind to another node'
            }
    return {
        'result': 'ok'
    }

def rebind_chatbot(app_id, chatbot_id, old_node_id, new_node_id):
    old_node_id = str(old_node_id or '').strip()
    new_node_id = str(new_node_id or '').strip()
    chatbot_id = str(chatbot_id or '').strip()
    check_result = check_rebind_chatbot(app_id, chatbot_id, old_node_id, new_node_id)
    if check_result.get('result') == 'error':
        return check_result
    if old_node_id == new_node_id:
        return {
            'result': 'ok'
        }
    if old_node_id != '':
        unbind_chatbot(app_id, old_node_id, chatbot_id, clear_prompt=True)
    if new_node_id != '':
        return bind_chatbot(app_id, new_node_id, chatbot_id)
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

def get_openclaw_session_mapping_by_session_storage_key(app_id, node_id, session_key):
    return f"{get_openclaw_session_mapping_by_session_prefix(app_id, node_id)}{normalize_session_key_text(session_key)}"

def get_openclaw_session_mapping_by_session_key(app_id, node_id, session_key):
    return f"{get_openclaw_session_mapping_by_session_prefix(app_id, node_id)}{normalize_session_key(session_key)}"

def get_openclaw_session_mapping_index_key(app_id, node_id):
    return f"lanying_connector:openclaw:session_map:index:{app_id}:{node_id}"

def get_openclaw_session_mapping_by_group_key(app_id, node_id, openclaw_user_id, group_id):
    return f"lanying_connector:openclaw:session_map:by_group:{app_id}:{node_id}:{openclaw_user_id}:{group_id}"

def get_openclaw_parent_reply_suppression_key(app_id, node_id, session_key):
    return f"lanying_connector:openclaw:parent_reply_suppression:{app_id}:{node_id}:{normalize_session_key_text(session_key)}"

def get_openclaw_session_last_message_time_key(app_id, node_id):
    return f"lanying_connector:openclaw:session_last_message_time:{app_id}:{node_id}"

def get_openclaw_session_last_message_time_field(session_key):
    return normalize_session_key_text(session_key)

def update_session_last_message_time(app_id, node_id, session_key, now_ms=None):
    normalized_node_id = str(node_id or '').strip()
    normalized_session_key = normalize_session_key(session_key)
    if normalized_node_id == '' or normalized_session_key == '':
        return 0
    timestamp_ms = int(now_ms if isinstance(now_ms, int) else time.time() * 1000)
    redis = lanying_redis.get_redis_connection()
    if redis is None or not hasattr(redis, 'hset'):
        return 0
    key = get_openclaw_session_last_message_time_key(app_id, normalized_node_id)
    field = get_openclaw_session_last_message_time_field(normalized_session_key)
    redis.hset(key, field, timestamp_ms)
    return timestamp_ms

def get_session_last_message_time(app_id, node_id, session_key):
    normalized_node_id = str(node_id or '').strip()
    normalized_session_key = normalize_session_key(session_key)
    if normalized_node_id == '' or normalized_session_key == '':
        return 0
    redis = lanying_redis.get_redis_connection()
    if redis is None or not hasattr(redis, 'hget'):
        return 0
    key = get_openclaw_session_last_message_time_key(app_id, normalized_node_id)
    field = get_openclaw_session_last_message_time_field(normalized_session_key)
    try:
        return int(redis.hget(key, field) or 0)
    except Exception:
        return 0

def get_token_key(token):
    return f"lanying_connector:openclaw:token:{token}"

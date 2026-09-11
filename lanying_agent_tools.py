"""Server-owned Seenical client tools and Skill repository state.

The IM message is only a notification transport.  Tool arguments, approval
state and execution results stay on the server and are addressed by a random
request id.  This keeps old clients compatible and prevents a client from
changing arguments after the user has reviewed them.
"""

import base64
import copy
import hashlib
import json
import logging
import os
import re
import time
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse

import requests
import yaml

import lanying_ai_plugin
import lanying_chatbot
import lanying_grow_ai
import lanying_pgvector
import lanying_redis
import lanying_vendor


SCHEMA_VERSION = 1
CAPABILITY_TTL_SECONDS = 150
REQUEST_TTL_SECONDS = 600
RESULT_TTL_SECONDS = 24 * 3600
MAX_SKILL_BYTES = 256 * 1024
MAX_SKILL_TOTAL_BYTES = 512 * 1024
MAX_SKILL_REPOSITORY_FILES = 256
MAX_MANIFEST_BYTES = 128 * 1024
MAX_LOCAL_RESULT_BYTES = 64 * 1024
MAX_TOOL_ARGUMENT_BYTES = 64 * 1024
MAX_NOTIFY_BYTES = 4096
OFFICIAL_SKILL_ID = 'seenical-console'
SUPPORTED_CLIENT_RUNTIMES = {('butler_api', 1)}
PUBLIC_CATALOG_CACHE_KEY = 'lanying_connector:agent_tools:public_catalog:active'
PUBLIC_CATALOG_DIRTY_KEY = 'lanying_connector:agent_tools:public_catalog:dirty'
PUBLIC_CATALOG_SYNC_LOCK_KEY = 'lanying_connector:agent_tools:public_catalog:sync_lock'


def _tool(function_name, tool_id, title, risk, execution, parameters, handler=None):
    return {
        'function_name': function_name,
        'tool_id': tool_id,
        'title': title,
        'risk': risk,
        'execution': execution,
        'version': 1,
        'parameters': parameters,
        'handler': handler,
    }


OBJECT_SCHEMA = {'type': 'object', 'additionalProperties': False}
PLAN_CHANGE_PROPERTIES = {
    'name': {'type': 'string'}, 'note': {'type': 'string'},
    'prompt': {'type': 'string'}, 'article_prompt': {'type': 'string'},
    'article_language': {'type': 'string', 'enum': ['auto', 'zh-hans', 'en']},
    'keywords': {'type': 'string'},
    'word_count_min': {'type': 'integer'}, 'word_count_max': {'type': 'integer'},
    'image_count': {'type': 'integer'}, 'article_count': {'type': 'integer'},
    'cycle_type': {'type': 'string', 'enum': ['none', 'cycle']},
    'cycle_interval': {'type': 'integer'},
    'title_reuse': {'type': 'string', 'enum': ['on', 'off']},
    'site_id_list': {'type': 'array', 'items': {'type': 'string'}},
    'target_dir': {'type': 'string'},
    'commit_type': {'type': 'string', 'enum': ['branch', 'pull_request']},
    'target_summary_dir': {'type': 'string'},
    'auto_deploy': {'type': 'string', 'enum': ['on', 'off']},
}
SITE_CHANGE_PROPERTIES = {
    field: {'type': 'string'} for field in [
        'name', 'footer_note', 'lanying_link', 'title', 'copyright',
        'canonical_link', 'meta_keywords', 'official_website_url',
        'icp_number', 'hook_sentence_slogan', 'hook_sentence_image',
        'collaborator'
    ]
}
SITE_CHANGE_PROPERTIES.update({
    'max_latest_num': {'type': 'integer'},
    'language': {'type': 'string', 'enum': ['zh-hans', 'en']},
    'commit_type': {'type': 'string', 'enum': ['branch', 'pull_request']},
})
TOOL_REGISTRY = {
    item['tool_id']: item for item in [
        _tool('seenical_plan_list', 'seenical.plan.list', '查看生成计划', 'read', 'console_action', {
            **OBJECT_SCHEMA, 'properties': {}
        }, '_plan_list'),
        _tool('seenical_plan_get', 'seenical.plan.get', '查看生成计划详情', 'read', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'task_id': {'type': 'string'}},
            'required': ['task_id']
        }, '_plan_get'),
        _tool('seenical_plan_create', 'seenical.plan.create', '创建生成计划', 'write', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                **PLAN_CHANGE_PROPERTIES,
            },
            'required': ['name', 'prompt']
        }, '_plan_create'),
        _tool('seenical_plan_update', 'seenical.plan.update', '修改生成计划', 'write', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'task_id': {'type': 'string'},
                'changes': {
                    'type': 'object', 'properties': PLAN_CHANGE_PROPERTIES,
                    'additionalProperties': False
                },
                'expected_revision': {'type': 'integer'}
            },
            'required': ['task_id', 'changes', 'expected_revision']
        }, '_plan_update'),
        _tool('seenical_plan_schedule', 'seenical.plan.schedule', '修改计划调度状态', 'write', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'task_id': {'type': 'string'},
                'schedule': {'type': 'string', 'enum': ['on', 'off']},
                'expected_revision': {'type': 'integer'}
            },
            'required': ['task_id', 'schedule', 'expected_revision']
        }, '_plan_schedule'),
        _tool('seenical_plan_run', 'seenical.plan.run', '立即运行生成计划', 'execute', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'task_id': {'type': 'string'}},
            'required': ['task_id']
        }, '_plan_run'),
        _tool('seenical_plan_run_list', 'seenical.plan_run.list', '查看计划生成记录', 'read', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'task_id': {'type': 'string'}},
            'required': ['task_id']
        }, '_plan_run_list'),
        _tool('seenical_preview_create', 'seenical.preview.create', '创建内容预览', 'execute', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'task_run_id': {'type': 'string'}},
            'required': ['task_run_id']
        }, '_preview_create'),
        _tool('seenical_preview_get', 'seenical.preview.get', '查看内容预览状态', 'read', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'preview_id': {'type': 'string'}},
            'required': ['preview_id']
        }, '_preview_get'),
        _tool('seenical_preview_publish', 'seenical.preview.publish', '发布内容预览', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'preview_id': {'type': 'string'}},
            'required': ['preview_id']
        }, '_preview_publish'),
        _tool('seenical_preview_discard', 'seenical.preview.discard', '丢弃内容预览', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'preview_id': {'type': 'string'}},
            'required': ['preview_id']
        }, '_preview_discard'),
        _tool('seenical_deploy_rollback', 'seenical.deploy.rollback', '回退到上一个已发布版本', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'site_id': {'type': 'string'}},
            'required': ['site_id']
        }, '_deploy_rollback'),
        _tool('seenical_plan_archive', 'seenical.plan.archive', '归档生成计划', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'task_id': {'type': 'string'}, 'expected_revision': {'type': 'integer'}},
            'required': ['task_id', 'expected_revision']
        }, '_plan_archive'),
        _tool('seenical_plan_rollback', 'seenical.plan.rollback', '回退生成计划配置', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'task_id': {'type': 'string'}, 'revision': {'type': 'integer'},
                'expected_revision': {'type': 'integer'}
            },
            'required': ['task_id', 'revision', 'expected_revision']
        }, '_plan_rollback'),
        _tool('seenical_agent_get', 'seenical.agent.get', '查看 Agent 配置', 'read', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'chatbot_id': {'type': 'string'}}
        }, '_agent_get'),
        _tool('seenical_agent_update', 'seenical.agent.update', '修改 Agent 创作配置', 'write', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'chatbot_id': {'type': 'string'},
                'changes': {
                    'type': 'object',
                    'properties': {
                        'model': {'type': 'string'}, 'vendor': {'type': 'string'},
                        'system_prompt': {'type': 'string'},
                        'plugin_ids': {'type': 'array', 'items': {'type': 'string'}}
                    },
                    'additionalProperties': False
                },
                'expected_revision': {'type': 'integer'}
            },
            'required': ['changes', 'expected_revision']
        }, '_agent_update'),
        _tool('seenical_agent_rollback', 'seenical.agent.rollback', '回退 Agent 创作配置', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'chatbot_id': {'type': 'string'}, 'revision': {'type': 'integer'},
                'expected_revision': {'type': 'integer'}
            },
            'required': ['revision', 'expected_revision']
        }, '_agent_rollback'),
        _tool('seenical_site_list', 'seenical.site.list', '查看网站列表', 'read', 'console_action', {
            **OBJECT_SCHEMA, 'properties': {}
        }, '_site_list'),
        _tool('seenical_site_get', 'seenical.site.get', '查看网站配置', 'read', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {'site_id': {'type': 'string'}},
            'required': ['site_id']
        }, '_site_get'),
        _tool('seenical_site_update', 'seenical.site.update', '修改网站非敏感配置', 'write', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'site_id': {'type': 'string'},
                'changes': {
                    'type': 'object', 'properties': SITE_CHANGE_PROPERTIES,
                    'additionalProperties': False
                },
                'expected_revision': {'type': 'integer'}
            },
            'required': ['site_id', 'changes', 'expected_revision']
        }, '_site_update'),
        _tool('seenical_site_rollback', 'seenical.site.rollback', '回退网站非敏感配置', 'destructive', 'console_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'site_id': {'type': 'string'}, 'revision': {'type': 'integer'},
                'expected_revision': {'type': 'integer'}
            },
            'required': ['site_id', 'revision', 'expected_revision']
        }, '_site_rollback'),
        _tool('seenical_console_navigate', 'seenical.console.navigate', '打开 Console 配置页面', 'local', 'local_action', {
            **OBJECT_SCHEMA,
            'properties': {
                'target': {
                    'type': 'string',
                    'enum': ['agent', 'loop', 'knowledge', 'site', 'deployment', 'skills']
                }
            },
            'required': ['target']
        }),
    ]
}

FUNCTION_TO_TOOL = {tool['function_name']: tool_id for tool_id, tool in TOOL_REGISTRY.items()}
SITE_PATCH_FIELDS = {
    'name', 'footer_note', 'lanying_link', 'title', 'copyright',
    'canonical_link', 'meta_keywords', 'official_website_url',
    'max_latest_num', 'language', 'commit_type', 'icp_number',
    'hook_sentence_slogan', 'hook_sentence_image', 'collaborator'
}
SECRET_FIELD_NAMES = {
    'access_token', 'access-token', 'token', 'github_token', 'api_key',
    'secret_key', 'password', 'authorization', 'baidu_token', 'google_token'
}
def _redis():
    return lanying_redis.get_redis_connection()


def _json(value):
    return json.dumps(
        value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def _load(value, default=None):
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode('utf-8')
    try:
        return json.loads(value)
    except Exception:
        return default


def _truthy(value):
    return str(value or '').strip().lower() in ['1', 'true', 'yes', 'on']


def _validate_tool_value(value, schema, path='arguments'):
    """Validate the small JSON-Schema subset used by the built-in registry."""
    expected_type = schema.get('type') if isinstance(schema, dict) else None
    valid = {
        'object': isinstance(value, dict),
        'array': isinstance(value, list),
        'string': isinstance(value, str),
        'integer': isinstance(value, int) and not isinstance(value, bool),
        'boolean': isinstance(value, bool),
    }
    if expected_type in valid and not valid[expected_type]:
        raise ValueError(path + ' has an invalid type')
    if 'enum' in schema and value not in schema['enum']:
        raise ValueError(path + ' has an invalid value')
    if expected_type == 'object':
        properties = schema.get('properties', {})
        missing = [key for key in schema.get('required', []) if key not in value]
        if missing:
            raise ValueError(path + ' is missing: ' + ','.join(sorted(missing)))
        if schema.get('additionalProperties') is False:
            unknown = sorted(set(value) - set(properties))
            if unknown:
                raise ValueError(path + ' has unsupported fields: ' + ','.join(unknown))
        for key, item in value.items():
            if key in properties:
                _validate_tool_value(item, properties[key], path + '.' + key)
    elif expected_type == 'array' and isinstance(schema.get('items'), dict):
        for index, item in enumerate(value):
            _validate_tool_value(item, schema['items'], f'{path}[{index}]')


def validate_tool_arguments(tool, arguments):
    if not isinstance(arguments, dict):
        raise ValueError('tool arguments must be an object')
    if len(_json(arguments).encode('utf-8')) > MAX_TOOL_ARGUMENT_BYTES:
        raise ValueError('tool arguments are too large')
    if _contains_secret_key(arguments):
        raise ValueError('tool arguments contain a forbidden credential field')
    _validate_tool_value(arguments, tool.get('parameters', {}))


def feature_key(app_id, chatbot_id='*'):
    return f'lanying_connector:agent_tools:feature:{app_id}:{chatbot_id or "*"}'


def configure_feature(app_id, enabled, chatbot_id='*'):
    normalized = enabled if isinstance(enabled, bool) else _truthy(enabled)
    _redis().set(feature_key(app_id, chatbot_id), 'on' if normalized else 'off')
    return {'result': 'ok', 'data': {'enabled': normalized, 'chatbot_id': str(chatbot_id or '*')}}


def is_feature_enabled(app_id, chatbot_id=''):
    redis = _redis()
    for key in [feature_key(app_id, chatbot_id), feature_key(app_id, '*')]:
        value = lanying_redis.redis_get(redis, key)
        if value is not None:
            return _truthy(value)
    return _truthy(os.getenv('LANYING_AGENT_TOOLS_ENABLED', 'off'))


def capability_index_key(app_id, chatbot_id, conversation_type, conversation_id):
    return (f'lanying_connector:agent_tools:capability_index:{app_id}:{chatbot_id}:'
            f'{conversation_type}:{conversation_id}')


def capability_key(app_id, client_instance_id):
    return f'lanying_connector:agent_tools:capability:{app_id}:{client_instance_id}'


def register_capabilities(app_id, actor, data):
    chatbot_id = str(data.get('chatbot_id', '')).strip()
    raw_chatbot_ids = data.get('chatbot_ids', [chatbot_id])
    chatbot_ids = list(dict.fromkeys(
        str(value).strip() for value in
        (raw_chatbot_ids[:50] if isinstance(raw_chatbot_ids, list) else [])
        if str(value).strip()))
    if chatbot_id and chatbot_id not in chatbot_ids:
        chatbot_ids.insert(0, chatbot_id)
    conversation_type = str(data.get('conversation_type', '')).upper()
    conversation_id = str(data.get('conversation_id', '')).strip()
    im_user_id = str(data.get('im_user_id', '')).strip()
    client_instance_id = str(data.get('client_instance_id', '')).strip()
    seenical_session_id = str(data.get('seenical_session_id', '')).strip()
    if int(data.get('schema_version', 0) or 0) != SCHEMA_VERSION:
        return {'result': 'error', 'message': 'unsupported capability schema_version'}
    if (not chatbot_id or len(chatbot_id) > 128 or not chatbot_ids
            or any(len(value) > 128 for value in chatbot_ids)
            or conversation_type not in ['CHAT', 'GROUPCHAT']
            or not conversation_id or len(conversation_id) > 128):
        return {'result': 'error', 'message': 'invalid conversation capability scope'}
    if not re.fullmatch(r'[A-Za-z0-9._:-]{1,128}', client_instance_id):
        return {'result': 'error', 'message': 'invalid client_instance_id'}
    if not re.fullmatch(r'[A-Za-z0-9._:-]{1,128}', seenical_session_id):
        return {'result': 'error', 'message': 'invalid seenical_session_id'}
    if not actor.get('subject_id') or not im_user_id or str(actor.get('im_user_id', '')) != im_user_id:
        return {'result': 'error', 'message': 'Console and IM user identities do not match'}
    if not is_feature_enabled(app_id, chatbot_id):
        return {'result': 'ok', 'data': {'enabled': False, 'expires_in': 0}}
    raw_runtimes = data.get('runtimes', [])
    supported = {}
    for item in (raw_runtimes[:20] if isinstance(raw_runtimes, list) else []):
        if not isinstance(item, dict):
            continue
        runtime_type = str(item.get('type', '')).strip()
        try:
            version = int(item.get('version', 0))
        except (TypeError, ValueError):
            continue
        if (runtime_type, version) in SUPPORTED_CLIENT_RUNTIMES:
            supported[runtime_type] = version
    if not supported:
        return {'result': 'error', 'message': 'no supported Seenical runtime'}
    payload = {
        'schema_version': SCHEMA_VERSION,
        'app_id': str(app_id),
        'chatbot_id': chatbot_id,
        'chatbot_ids': chatbot_ids,
        'conversation_type': conversation_type,
        'conversation_id': conversation_id,
        'im_user_id': im_user_id,
        'client_instance_id': client_instance_id,
        'seenical_session_id': seenical_session_id,
        'actor_subject_id': str(actor.get('subject_id', '')),
        'actor_tenement_id': str(actor.get('tenement_id', '')),
        'runtimes': supported,
        'updated_at': int(time.time()),
    }
    redis = _redis()
    key = capability_key(app_id, client_instance_id)
    old = _load(redis.get(key), {})
    if old:
        for old_chatbot_id in old.get('chatbot_ids', [old.get('chatbot_id', '')]):
            redis.srem(capability_index_key(
                app_id, old_chatbot_id, old.get('conversation_type', ''),
                old.get('conversation_id', '')), client_instance_id)
    pipe = redis.pipeline(transaction=True)
    pipe.setex(key, CAPABILITY_TTL_SECONDS, _json(payload))
    for supported_chatbot_id in chatbot_ids:
        pipe.sadd(capability_index_key(
            app_id, supported_chatbot_id, conversation_type, conversation_id),
            client_instance_id)
    pipe.execute()
    return {
        'result': 'ok',
        'data': {'enabled': True, 'expires_in': CAPABILITY_TTL_SECONDS,
                 'runtimes': [{'type': key, 'version': value}
                              for key, value in sorted(supported.items())]}
    }


def _conversation_scope(config):
    return (
        str(config.get('chatbot_id', '')),
        str(config.get('reply_msg_type', '')).upper(),
        str(config.get('reply_to', '')),
        str(config.get('send_from', config.get('from_user_id', '')))
    )


def find_capability(app_id, config, tool_id=None, runtime=None):
    chatbot_id, conversation_type, conversation_id, im_user_id = _conversation_scope(config)
    if not is_feature_enabled(app_id, chatbot_id):
        return None
    binding = get_im_binding_projection(app_id)
    if (not binding or binding.get('status') != 'BOUND'
            or str(binding.get('im_user_id', '')) != im_user_id):
        return None
    redis = _redis()
    index = capability_index_key(app_id, chatbot_id, conversation_type, conversation_id)
    instance_ids = redis.smembers(index)
    selected = None
    for raw_instance_id in instance_ids:
        instance_id = raw_instance_id.decode('utf-8') if isinstance(raw_instance_id, bytes) else str(raw_instance_id)
        capability = _load(redis.get(capability_key(app_id, instance_id)), None)
        if capability is None:
            redis.srem(index, instance_id)
            continue
        if capability.get('im_user_id') and str(capability.get('im_user_id')) != im_user_id:
            continue
        if chatbot_id not in capability.get('chatbot_ids', [capability.get('chatbot_id', '')]):
            continue
        if runtime:
            runtime_type = str(runtime.get('type', ''))
            runtime_version = int(runtime.get('version', 0) or 0)
            if int(capability.get('runtimes', {}).get(runtime_type, 0) or 0) < runtime_version:
                continue
        elif tool_id and tool_id not in capability.get('tools', {}):
            continue
        if selected is None or capability.get('updated_at', 0) > selected.get('updated_at', 0):
            selected = capability
    return selected


def resolve_tool_id(function_info):
    function_call = function_info.get('function_call', {}) if isinstance(function_info, dict) else {}
    return str(function_call.get('tool_id') or FUNCTION_TO_TOOL.get(function_info.get('name', ''), ''))


def _official_skill():
    catalog = get_public_catalog() or {}
    for skill in catalog.get('skills', []):
        if str(skill.get('skill_id', '')) == OFFICIAL_SKILL_ID:
            return skill
    return None


def dynamic_tool(tool_id):
    skill = _official_skill()
    if not skill:
        return None
    for tool in skill.get('tools', []):
        if str(tool.get('tool_id', '')) == str(tool_id):
            return copy.deepcopy(tool)
    return None


def tool_definition(tool_id):
    return dynamic_tool(tool_id) or TOOL_REGISTRY.get(tool_id)


def _active_skill_authorizations(app_id, chatbot_id, tool_id):
    result = []
    for repository in get_active_skills(app_id, chatbot_id):
        if any(tool_id in skill.get('required_tools', []) for skill in repository.get('skills', [])):
            result.append({
                'skill_id': str(repository.get('repository_id', '')),
                'revision': str(repository.get('revision', '')),
            })
    return sorted(result, key=lambda item: (item['skill_id'], item['revision']))


def _bound_plugin_id(app_id, chatbot_id, function_info):
    if str(function_info.get('owner_app_id', app_id)) != str(app_id):
        return ''
    doc_id = str(function_info.get('doc_id', ''))
    if not doc_id:
        return ''
    plugin_id = str(lanying_ai_plugin.get_plugin_id_by_doc_id(app_id, doc_id) or '')
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if not plugin_id or not chatbot:
        return ''
    relation = lanying_ai_plugin.get_ai_plugin_bind_relation(app_id)
    bound_ids = set(str(value) for value in relation.get(chatbot.get('name', ''), []))
    return plugin_id if plugin_id in bound_ids else ''


def _plugin_still_allows_tool(app_id, chatbot_id, plugin_id, tool_id):
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if not chatbot or not lanying_ai_plugin.get_ai_plugin(app_id, plugin_id):
        return False
    relation = lanying_ai_plugin.get_ai_plugin_bind_relation(app_id)
    if str(plugin_id) not in set(str(value) for value in relation.get(chatbot.get('name', ''), [])):
        return False
    for function_id in lanying_ai_plugin.list_ai_function_ids(app_id, plugin_id):
        function_info = lanying_ai_plugin.get_ai_function(app_id, function_id)
        if function_info and resolve_tool_id(function_info) == tool_id:
            function_call = function_info.get('function_call', {})
            if isinstance(function_call, str):
                function_call = _load(function_call, {})
            if function_call.get('type') == 'client':
                return True
    return False


def filter_supported_client_functions(app_id, config, functions):
    filtered = []
    for function_info in functions:
        function_call = function_info.get('function_call', {}) if isinstance(function_info, dict) else {}
        if function_call.get('type', 'http') != 'client':
            filtered.append(function_info)
            continue
        tool_id = resolve_tool_id(function_info)
        registry_tool = tool_definition(tool_id)
        chatbot_id = str(config.get('chatbot_id', ''))
        skill_versions = _active_skill_authorizations(app_id, chatbot_id, tool_id)
        if function_info.get('seenical_builtin_tool'):
            if (registry_tool and skill_versions
                    and find_capability(app_id, config, runtime=registry_tool.get('runtime'))):
                copied = registry_function(tool_id)
                copied['seenical_builtin_tool'] = True
                copied['seenical_skill_versions'] = skill_versions
                filtered.append(copied)
            continue
        plugin_id = _bound_plugin_id(app_id, chatbot_id, function_info)
        if (registry_tool and skill_versions and plugin_id
                and find_capability(app_id, config, runtime=registry_tool.get('runtime'))):
            # The plugin authorizes a reference to a platform Tool.  It must
            # not be able to replace the model-visible schema, description,
            # risk or handler metadata with repository-controlled content.
            copied = registry_function(tool_id)
            for field in ['doc_id', 'function_id', 'owner_app_id']:
                if field in function_info:
                    copied[field] = function_info[field]
            copied['seenical_plugin_id'] = plugin_id
            copied['seenical_skill_versions'] = skill_versions
            filtered.append(copied)
    return filtered


def registry_function(tool_id):
    tool = tool_definition(tool_id)
    if not tool:
        raise KeyError(tool_id)
    return {
        'name': tool['function_name'],
        'description': tool['title'],
        'parameters': copy.deepcopy(tool['parameters']),
        'priority': 5,
        'function_call': {
            'type': 'client', 'tool_id': tool_id,
            'execution': tool['execution'], 'risk': tool['risk'],
            'runtime': copy.deepcopy(tool.get('runtime'))
        }
    }


def _safe_task(task):
    result = copy.deepcopy(task) if isinstance(task, dict) else task
    if isinstance(result, dict):
        result.pop('site_cdn_token', None)
        files = result.pop('file_list', [])
        result['attachment_count'] = len(files) if isinstance(files, list) else 0
        if isinstance(result.get('deploy'), dict):
            result['deploy'] = {
                key: value for key, value in result['deploy'].items()
                if str(key).strip().lower() not in SECRET_FIELD_NAMES
            }
    return result


def _safe_site(site):
    if not isinstance(site, dict):
        return site
    result = copy.deepcopy(site)
    for field in ['github_token', 'baidu_token', 'google_token', 'site_cdn_token']:
        result.pop(field, None)
    result['agent_tools_revision'] = int(result.get('agent_tools_revision', 0) or 0)
    return result


def _site_revision_snapshot(site):
    safe = _safe_site(site) or {}
    fields = SITE_PATCH_FIELDS | {'site_id', 'agent_tools_revision'}
    return {
        field: copy.deepcopy(safe[field])
        for field in fields if field in safe
    }


def _safe_preview(preview):
    if not isinstance(preview, dict):
        return preview
    result = copy.deepcopy(preview)
    result.pop('cdn_token', None)
    return result


def _safe_task_run(task_run):
    if not isinstance(task_run, dict):
        return task_run
    result = copy.deepcopy(task_run)
    for field in ['user_id', 'zip_file']:
        result.pop(field, None)
    if isinstance(result.get('preview'), dict):
        result['preview'] = _safe_preview(result['preview'])
    return result


def _safe_agent(chatbot):
    if not isinstance(chatbot, dict):
        return chatbot
    preset = chatbot.get('preset', {}) if isinstance(chatbot.get('preset', {}), dict) else {}
    system_prompt = ''
    for message in preset.get('messages', []):
        if message.get('role') in ['system', 'developer']:
            system_prompt = str(message.get('content', ''))
            break
    plugin_relation = lanying_ai_plugin.get_ai_plugin_bind_relation(str(chatbot.get('app_id', '')))
    return {
        'chatbot_id': str(chatbot.get('chatbot_id', '')),
        'name': chatbot.get('name', ''),
        'model': preset.get('model', ''),
        'vendor': preset.get('vendor', ''),
        'system_prompt': system_prompt,
        'plugin_ids': [str(value) for value in plugin_relation.get(chatbot.get('name', ''), [])],
        'revision': int(chatbot.get('agent_tools_revision', 0) or 0),
    }


def _save_config_revision(app_id, resource_type, resource_id, revision,
                          snapshot, request_id=''):
    try:
        return lanying_pgvector.save_seenical_config_revision(
            app_id, resource_type, resource_id, revision, snapshot, request_id)
    except Exception:
        logging.exception('failed to save Seenical configuration revision')
        return {'result': 'error', 'message': 'configuration revision store unavailable'}


def _get_config_revision(app_id, resource_type, resource_id, revision):
    try:
        return lanying_pgvector.get_seenical_config_revision(
            app_id, resource_type, resource_id, revision)
    except Exception:
        logging.exception('failed to read Seenical configuration revision')
        return None


def _list_config_revisions(app_id, resource_type, resource_id, limit=20):
    try:
        return lanying_pgvector.list_seenical_config_revisions(
            app_id, resource_type, resource_id, limit)
    except Exception:
        logging.exception('failed to list Seenical configuration revisions')
        return []


def _plan_list(app_id, arguments, request_info):
    result = lanying_grow_ai.get_task_list(app_id)
    if result.get('result') == 'ok':
        result['data']['list'] = [
            _safe_task(item) for item in result['data'].get('list', [])
            if item.get('status', 'normal') != 'archived'
        ]
    return result


def _plan_get(app_id, arguments, request_info):
    task_id = str(arguments.get('task_id', ''))
    task = lanying_grow_ai.get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task_id not exist'}
    return {'result': 'ok', 'data': {
        'task': _safe_task(task),
        'available_revisions': lanying_grow_ai.list_task_revisions(
            app_id, task_id, 20)
    }}


def _plan_create(app_id, arguments, request_info):
    chatbot_id = str(arguments.get('chatbot_id') or request_info.get('chatbot_id', ''))
    site_ids = list(arguments.get('site_id_list', []))
    if not str(arguments.get('name', '')).strip() or len(str(arguments.get('name', ''))) > 200:
        return {'result': 'error', 'message': 'invalid plan name'}
    if len(str(arguments.get('prompt', ''))) > 20000:
        return {'result': 'error', 'message': 'prompt is too long'}
    if len(str(arguments.get('article_prompt', ''))) > 5000:
        return {'result': 'error', 'message': 'article_prompt is too long'}
    if str(arguments.get('article_language', 'auto')) not in lanying_grow_ai.ARTICLE_LANGUAGE_VALUES:
        return {'result': 'error', 'message': 'article_language has an invalid value'}
    if str(arguments.get('cycle_type', 'none')) not in ['none', 'cycle']:
        return {'result': 'error', 'message': 'cycle_type has an invalid value'}
    if len(site_ids) > 5 or any(
            lanying_grow_ai.get_site(app_id, str(site_id)) is None
            for site_id in site_ids):
        return {'result': 'error', 'message': 'site_id_list contains an invalid site'}
    setting = lanying_grow_ai.TaskSetting(
        app_id=app_id,
        name=str(arguments.get('name', '')),
        note=str(arguments.get('note', arguments.get('prompt', ''))),
        chatbot_id=chatbot_id,
        prompt=str(arguments.get('prompt', '')),
        article_prompt=str(arguments.get('article_prompt', '')),
        article_language=str(arguments.get('article_language', 'auto')),
        keywords=str(arguments.get('keywords', '')),
        word_count_min=int(arguments.get('word_count_min', 800)),
        word_count_max=int(arguments.get('word_count_max', 1200)),
        image_count=max(0, int(arguments.get('image_count', 0))),
        article_count=min(100000, max(1, int(arguments.get('article_count', 1)))),
        cycle_type=str(arguments.get('cycle_type', 'none')),
        cycle_interval=max(3600, int(arguments.get('cycle_interval', 86400))),
        file_list=list(arguments.get('file_list', [])),
        deploy=dict(arguments.get('deploy', {'type': 'none'})),
        title_reuse=str(arguments.get('title_reuse', 'off')),
        site_id_list=site_ids,
        target_dir=str(arguments.get('target_dir', '/articles')),
        commit_type=str(arguments.get('commit_type', 'branch')),
        target_summary_dir=str(arguments.get('target_summary_dir', '')),
        embedding_condition=dict(arguments.get('embedding_condition', {})),
        auto_deploy=str(arguments.get('auto_deploy', 'on' if site_ids else 'off')),
    )
    # Creating a plan and executing it are separate user-confirmed operations.
    # Keep the existing Console create API unchanged; conversational Tools use
    # seenical.plan.run when the user explicitly asks to start generation.
    return lanying_grow_ai.create_task(setting, run_immediately=False)


def _plan_update(app_id, arguments, request_info):
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    return lanying_grow_ai.patch_task(
        app_id, str(arguments.get('task_id', '')), arguments.get('changes', {}),
        arguments.get('expected_revision'), request_info.get('request_id', ''))


def _plan_schedule(app_id, arguments, request_info):
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    return lanying_grow_ai.set_task_schedule_revisioned(
        app_id, str(arguments.get('task_id', '')),
        str(arguments.get('schedule', '')),
        arguments.get('expected_revision'), request_info.get('request_id', ''))


def _plan_run(app_id, arguments, request_info):
    task_id = str(arguments.get('task_id', ''))
    task = lanying_grow_ai.get_task(app_id, task_id)
    if task is None:
        return {'result': 'error', 'message': 'task_id not exist'}
    if task.get('status') == 'archived':
        return {'result': 'error', 'message': 'archived task cannot run'}
    return lanying_grow_ai.run_task(app_id, task_id)


def _plan_run_list(app_id, arguments, request_info):
    result = lanying_grow_ai.get_task_run_list(
        app_id, str(arguments.get('task_id', '')))
    if result.get('result') == 'ok':
        result['data']['list'] = [
            _safe_task_run(item) for item in result['data'].get('list', [])]
    return result


def _preview_create(app_id, arguments, request_info):
    return lanying_grow_ai.task_run_preview(
        app_id, str(arguments.get('task_run_id', '')))


def _preview_get(app_id, arguments, request_info):
    preview = lanying_grow_ai.get_preview(
        app_id, str(arguments.get('preview_id', '')))
    if preview is None:
        return {'result': 'error', 'message': 'preview not found'}
    return {'result': 'ok', 'data': {'preview': _safe_preview(preview)}}


def _preview_publish(app_id, arguments, request_info):
    return lanying_grow_ai.preview_publish(
        app_id, str(arguments.get('preview_id', '')))


def _preview_discard(app_id, arguments, request_info):
    return lanying_grow_ai.preview_discard(
        app_id, str(arguments.get('preview_id', '')))


def _deploy_rollback(app_id, arguments, request_info):
    return lanying_grow_ai.rollback_site_deployment(
        app_id, str(arguments.get('site_id', '')))


def _plan_archive(app_id, arguments, request_info):
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    return lanying_grow_ai.archive_task(
        app_id, str(arguments.get('task_id', '')),
        arguments.get('expected_revision'), request_info.get('request_id', ''))


def _plan_rollback(app_id, arguments, request_info):
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    return lanying_grow_ai.rollback_task_revision(
        app_id, str(arguments.get('task_id', '')), int(arguments.get('revision')),
        arguments.get('expected_revision'), request_info.get('request_id', ''))


def _agent_get(app_id, arguments, request_info):
    chatbot_id = str(arguments.get('chatbot_id') or request_info.get('chatbot_id', ''))
    chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
    if chatbot is None:
        return {'result': 'error', 'message': 'chatbot not exist'}
    return {'result': 'ok', 'data': {
        'agent': _safe_agent(chatbot),
        'available_revisions': _list_config_revisions(
            app_id, 'agent', chatbot_id, 20)
    }}


def _agent_update(app_id, arguments, request_info):
    chatbot_id = str(arguments.get('chatbot_id') or request_info.get('chatbot_id', ''))
    changes = arguments.get('changes', {})
    if not isinstance(changes, dict) or not changes or set(changes) - {'model', 'vendor', 'system_prompt', 'plugin_ids'}:
        return {'result': 'error', 'message': 'unsupported Agent changes'}
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    redis = _redis()
    key = lanying_chatbot.get_chatbot_key(app_id, chatbot_id)
    relation_key = lanying_ai_plugin.ai_plugin_bind_relation_key(app_id)
    pipe = redis.pipeline(transaction=True)
    try:
        pipe.watch(key, relation_key)
        chatbot = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        if chatbot is None:
            pipe.unwatch()
            return {'result': 'error', 'message': 'chatbot not exist'}
        current_revision = int(chatbot.get('agent_tools_revision', 0) or 0)
        expected = arguments.get('expected_revision')
        if expected is not None and int(expected) != current_revision:
            pipe.unwatch()
            return {'result': 'error', 'code': 'revision_conflict', 'message': 'Agent revision changed', 'data': {'agent': _safe_agent(chatbot)}}
        preset = copy.deepcopy(chatbot.get('preset', {}))
        candidate_model = str(changes.get('model', preset.get('model', '')))
        candidate_vendor = str(changes.get('vendor', preset.get('vendor', 'openai')))
        if lanying_vendor.get_chat_model_config(app_id, candidate_vendor, candidate_model) is None:
            pipe.unwatch()
            return {'result': 'error', 'message': 'model configuration does not exist'}
        if len(str(changes.get('system_prompt', ''))) > 20000:
            pipe.unwatch()
            return {'result': 'error', 'message': 'system_prompt is too long'}
        plugin_ids = None
        relation = lanying_ai_plugin.get_ai_plugin_bind_relation(app_id)
        if 'plugin_ids' in changes:
            if not isinstance(changes['plugin_ids'], list):
                pipe.unwatch()
                return {'result': 'error', 'message': 'plugin_ids must be an array'}
            plugin_ids = list(dict.fromkeys(str(value) for value in changes['plugin_ids']))
            if any(lanying_ai_plugin.get_ai_plugin(app_id, plugin_id) is None for plugin_id in plugin_ids):
                pipe.unwatch()
                return {'result': 'error', 'message': 'AI plugin does not exist'}
            relation[chatbot.get('name', '')] = plugin_ids
        if 'model' in changes:
            preset['model'] = str(changes['model'])
        if 'vendor' in changes:
            preset['vendor'] = str(changes['vendor'])
        if 'system_prompt' in changes:
            messages = list(preset.get('messages', []))
            target = next((item for item in messages if item.get('role') in ['system', 'developer']), None)
            if target is None:
                messages.insert(0, {'role': 'system', 'content': str(changes['system_prompt'])})
            else:
                target['content'] = str(changes['system_prompt'])
            preset['messages'] = messages
        next_revision = current_revision + 1
        snapshot = _safe_agent(chatbot)
        saved = _save_config_revision(
            app_id, 'agent', chatbot_id, current_revision, snapshot,
            request_info.get('request_id', ''))
        if saved.get('result') != 'ok':
            pipe.unwatch()
            return {
                'result': 'error', 'code': 'revision_store_unavailable',
                'message': 'configuration revision could not be saved'
            }
        pipe.multi()
        pipe.hset(key, 'preset', _json(preset))
        pipe.hset(key, 'agent_tools_revision', next_revision)
        if plugin_ids is not None:
            pipe.set(relation_key, _json(relation))
        pipe.execute()
    except Exception as error:
        logging.exception(error)
        latest = lanying_chatbot.get_chatbot(app_id, chatbot_id)
        return {'result': 'error', 'code': 'revision_conflict', 'message': 'Agent revision changed', 'data': {'agent': _safe_agent(latest)}}
    return {'result': 'ok', 'data': {'agent': _safe_agent(lanying_chatbot.get_chatbot(app_id, chatbot_id))}}


def patch_agent(app_id, arguments, request_info):
    """Apply a revision-checked partial Agent update from the Butler API."""
    return _agent_update(app_id, arguments, request_info)


def _agent_rollback(app_id, arguments, request_info):
    chatbot_id = str(arguments.get('chatbot_id') or request_info.get('chatbot_id', ''))
    revision = int(arguments.get('revision'))
    snapshot = _get_config_revision(
        app_id, 'agent', chatbot_id, revision)
    if snapshot is None:
        return {'result': 'error', 'message': 'Agent revision snapshot not found'}
    return _agent_update(app_id, {
        'chatbot_id': chatbot_id,
        'expected_revision': arguments.get('expected_revision'),
        'changes': {
            field: snapshot[field] for field in
            ['model', 'vendor', 'system_prompt', 'plugin_ids'] if field in snapshot
        }
    }, request_info)


def _site_list(app_id, arguments, request_info):
    result = lanying_grow_ai.get_site_list(app_id)
    if result.get('result') == 'ok':
        result['data']['list'] = [_safe_site(item) for item in result['data'].get('list', [])]
    return result


def _site_get(app_id, arguments, request_info):
    site = lanying_grow_ai.get_site(app_id, str(arguments.get('site_id', '')))
    if site is None:
        return {'result': 'error', 'message': 'site_id not exist'}
    return {'result': 'ok', 'data': {
        'site': _safe_site(site),
        'available_revisions': _list_config_revisions(
            app_id, 'site', str(arguments.get('site_id', '')), 20)
    }}


def _site_update(app_id, arguments, request_info):
    site_id = str(arguments.get('site_id', ''))
    changes = arguments.get('changes', {})
    if not isinstance(changes, dict) or not changes:
        return {'result': 'error', 'message': 'changes must be a non-empty object'}
    if 'expected_revision' not in arguments:
        return {'result': 'error', 'message': 'expected_revision is required'}
    unknown = sorted(set(changes) - SITE_PATCH_FIELDS)
    if unknown:
        return {'result': 'error', 'message': 'unsupported site fields: ' + ','.join(unknown)}
    redis = _redis()
    key = lanying_grow_ai.get_site_key(app_id, site_id)
    pipe = redis.pipeline(transaction=True)
    try:
        pipe.watch(key)
        old_site = lanying_grow_ai.get_site(app_id, site_id)
        if old_site is None:
            pipe.unwatch()
            return {'result': 'error', 'message': 'site_id not exist'}
        current_revision = int(old_site.get('agent_tools_revision', 0) or 0)
        expected = arguments.get('expected_revision')
        if expected is not None and int(expected) != current_revision:
            pipe.unwatch()
            return {'result': 'error', 'code': 'revision_conflict', 'message': 'site revision changed', 'data': {'site': _safe_site(old_site)}}
        normalized = {field: (int(value) if field == 'max_latest_num' else str(value)) for field, value in changes.items()}
        if any(len(value) > 20000 for value in normalized.values() if isinstance(value, str)):
            pipe.unwatch()
            return {'result': 'error', 'message': 'site field is too long'}
        if normalized.get('language', old_site.get('language', 'zh-hans')) not in ['zh-hans', 'en']:
            pipe.unwatch()
            return {'result': 'error', 'message': 'language has an invalid value'}
        if normalized.get('commit_type', old_site.get('commit_type', 'branch')) not in ['branch', 'pull_request']:
            pipe.unwatch()
            return {'result': 'error', 'message': 'commit_type has an invalid value'}
        if 'max_latest_num' in normalized and not 1 <= normalized['max_latest_num'] <= 100:
            pipe.unwatch()
            return {'result': 'error', 'message': 'max_latest_num has an invalid value'}
        for field in ['lanying_link', 'canonical_link', 'official_website_url', 'hook_sentence_image']:
            value = normalized.get(field, '')
            if value:
                parsed = urlparse(value)
                if parsed.scheme not in ['http', 'https'] or not parsed.netloc:
                    pipe.unwatch()
                    return {'result': 'error', 'message': field + ' has an invalid URL'}
        collaborator = normalized.get('collaborator')
        if collaborator and not re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?', collaborator):
            pipe.unwatch()
            return {'result': 'error', 'message': 'collaborator has an invalid value'}
        snapshot = _site_revision_snapshot(old_site)
        saved = _save_config_revision(
            app_id, 'site', site_id, current_revision, snapshot,
            request_info.get('request_id', ''))
        if saved.get('result') != 'ok':
            pipe.unwatch()
            return {
                'result': 'error', 'code': 'revision_store_unavailable',
                'message': 'configuration revision could not be saved'
            }
        fields = dict(normalized)
        fields['agent_tools_revision'] = current_revision + 1
        fields['update_time'] = int(time.time())
        pipe.multi()
        pipe.hmset(key, fields)
        pipe.execute()
    except Exception as error:
        logging.exception(error)
        latest = lanying_grow_ai.get_site(app_id, site_id)
        return {'result': 'error', 'code': 'revision_conflict', 'message': 'site revision changed', 'data': {'site': _safe_site(latest)}}
    new_site = lanying_grow_ai.get_site(app_id, site_id)
    lanying_grow_ai.maybe_sync_to_github(old_site, new_site)
    return {'result': 'ok', 'data': {'site': _safe_site(new_site)}}


def patch_site(app_id, arguments, request_info):
    """Apply a revision-checked partial site update from the Butler API."""
    return _site_update(app_id, arguments, request_info)


def _site_rollback(app_id, arguments, request_info):
    site_id = str(arguments.get('site_id', ''))
    revision = int(arguments.get('revision'))
    snapshot = _get_config_revision(
        app_id, 'site', site_id, revision)
    if snapshot is None:
        return {'result': 'error', 'message': 'site revision snapshot not found'}
    return _site_update(app_id, {
        'site_id': site_id,
        'expected_revision': arguments.get('expected_revision'),
        'changes': {
            field: snapshot[field] for field in SITE_PATCH_FIELDS if field in snapshot
        }
    }, request_info)



def _safe_execution_result(value):
    result = copy.deepcopy(value)
    if not isinstance(result, dict) or not isinstance(result.get('data'), dict):
        return result
    data = result['data']
    if isinstance(data.get('task'), dict):
        data['task'] = _safe_task(data['task'])
    if isinstance(data.get('site'), dict):
        data['site'] = _safe_site(data['site'])
    if isinstance(data.get('preview'), dict):
        data['preview'] = _safe_preview(data['preview'])
    return result


def execute_tool(app_id, tool_id, arguments, request_info=None):
    request_info = request_info or {}
    tool = TOOL_REGISTRY.get(tool_id)
    if tool is None or not tool.get('handler'):
        return {'result': 'error', 'message': 'tool is not server executable'}
    handler = globals()[tool['handler']]
    try:
        return _safe_execution_result(handler(
            str(app_id), arguments if isinstance(arguments, dict) else {},
            request_info))
    except Exception as error:
        logging.exception(error)
        return {'result': 'error', 'message': 'tool execution failed'}


def _preview_tool(app_id, tool_id, arguments, request_info):
    if tool_id == 'seenical.plan.update':
        current = lanying_grow_ai.get_task(app_id, str(arguments.get('task_id', '')))
        changes = {
            key: value for key, value in arguments.items()
            if key not in ['task_id', 'expected_revision']
        }
        return {
            'before': {key: (current or {}).get(key) for key in changes},
            'after': changes,
        }
    if tool_id == 'seenical.plan.schedule':
        current = lanying_grow_ai.get_task(app_id, str(arguments.get('task_id', '')))
        return {'before': {'schedule': (current or {}).get('schedule')}, 'after': {'schedule': arguments.get('schedule')}}
    if tool_id in ['seenical.plan.run', 'seenical.plan.archive', 'seenical.plan.rollback']:
        return {'target': _safe_task(lanying_grow_ai.get_task(app_id, str(arguments.get('task_id', ''))))}
    if tool_id == 'seenical.preview.create':
        return {'target': _safe_task_run(lanying_grow_ai.get_task_run(
            app_id, str(arguments.get('task_run_id', ''))))}
    if tool_id in ['seenical.preview.publish', 'seenical.preview.discard']:
        return {'target': _safe_preview(lanying_grow_ai.get_preview(
            app_id, str(arguments.get('preview_id', ''))))}
    if tool_id in ['seenical.agent.update', 'seenical.agent.rollback']:
        current = _agent_get(app_id, arguments, request_info)
        before = current.get('data', {}).get('agent')
        changes = {
            key: value for key, value in arguments.items()
            if key not in ['chatbot_id', 'expected_revision']
        }
        return ({'before': {key: (before or {}).get(key) for key in changes}, 'after': changes}
                if tool_id == 'seenical.agent.update'
                else {'target': before, 'revision': arguments.get('revision')})
    if tool_id in ['seenical.site.update', 'seenical.site.rollback']:
        current = lanying_grow_ai.get_site(app_id, str(arguments.get('site_id', '')))
        changes = {
            key: value for key, value in arguments.items()
            if key not in ['site_id', 'expected_revision']
        }
        return ({'before': {key: (current or {}).get(key) for key in changes}, 'after': changes}
                if tool_id == 'seenical.site.update'
                else {'target': _safe_site(current), 'revision': arguments.get('revision')})
    if tool_id == 'seenical.deploy.rollback':
        return {'target': _safe_site(lanying_grow_ai.get_site(
            app_id, str(arguments.get('site_id', ''))))}
    return {'arguments': arguments}


def request_key(request_id):
    return f'lanying_connector:agent_tools:request:{request_id}'


def result_key(request_id):
    return f'lanying_connector:agent_tools:result:{request_id}'


def audit_key(app_id):
    return f'lanying_connector:agent_tools:audit:{app_id}'


def _audit_diff_summary(preview):
    if not isinstance(preview, dict):
        return {}
    before = preview.get('before')
    after = preview.get('after')
    if isinstance(before, dict) and isinstance(after, dict):
        changed_fields = sorted(
            key for key in set(before) | set(after)
            if _json(before.get(key)) != _json(after.get(key)))
        return {
            'changed_fields': changed_fields,
            'before_hash': hashlib.sha256(_json(before).encode('utf-8')).hexdigest(),
            'after_hash': hashlib.sha256(_json(after).encode('utf-8')).hexdigest(),
        }
    target = preview.get('target')
    if isinstance(target, dict):
        return {
            'target_id': str(target.get('task_id') or target.get('site_id') or target.get('chatbot_id') or ''),
            'target_revision': int(target.get('revision', 0) or 0),
        }
    return {'parameter_fields': sorted(preview.get('arguments', {}).keys())} if isinstance(preview.get('arguments'), dict) else {}


def _audit(app_id, request_id, event, fields=None):
    entry = {'request_id': request_id, 'event': event, 'time': int(time.time())}
    if fields:
        entry.update(fields)
    request_context = _load(_redis().get(request_key(request_id)), None)
    if isinstance(request_context, dict):
        for field in [
                'chatbot_id', 'conversation_type', 'conversation_id',
                'actor_subject_id', 'tool_id', 'tool_version',
                'skill_versions', 'arguments_hash', 'risk']:
            if field not in entry and field in request_context:
                entry[field] = request_context[field]
    redis = _redis()
    pipe = redis.pipeline(transaction=True)
    pipe.rpush(audit_key(app_id), _json(entry))
    pipe.ltrim(audit_key(app_id), -2000, -1)
    pipe.execute()
    audit_entry = {
        'app_id': str(app_id),
        'request_id': str(request_id),
        'event': str(event),
        'chatbot_id': str(entry.get('chatbot_id', '')),
        'conversation_type': str(entry.get('conversation_type', '')),
        'conversation_id': str(entry.get('conversation_id', '')),
        'actor_subject_id': str(entry.get('actor_subject_id', '')),
        'tool_id': str(entry.get('tool_id', '')),
        'tool_version': int(entry.get('tool_version', 0) or 0),
        'skill_versions': entry.get('skill_versions', []),
        'arguments_hash': str(entry.get('arguments_hash', '')),
        'result_status': str(entry.get('status', entry.get('result', ''))),
        'diff_summary': entry.get('diff_summary', {}),
        'extra_metadata': {
            key: value for key, value in entry.items()
            if key in ['risk', 'code', 'message', 'repository_id', 'revision']
        },
    }
    try:
        lanying_pgvector.append_agent_tool_audit_log(audit_entry)
    except Exception:
        # Redis already contains the append-only fallback entry.  Do not repeat
        # an already completed business action because audit storage is down.
        logging.exception('failed to persist agent tool audit log')


def record_resume_status(app_id, request_id, status, message=''):
    fields = {'status': str(status)}
    if message:
        fields['message'] = str(message)[:300]
    request_info = _load(_redis().get(request_key(request_id)), None)
    if request_info is not None and str(request_info.get('app_id')) == str(app_id):
        request_info['resume_status'] = str(status)
        request_info['resume_updated_at'] = int(time.time())
        if message:
            request_info['resume_message'] = str(message)[:300]
        else:
            request_info.pop('resume_message', None)
        _store_request(request_info)
    _audit(app_id, request_id, 'model_resume', fields)


def create_client_request(app_id, config, tool_call, function_info, arguments, continuation):
    tool_id = resolve_tool_id(function_info)
    tool = tool_definition(tool_id)
    if tool is None:
        return {'result': 'error', 'message': 'client tool is not registered'}
    try:
        validate_tool_arguments(tool, arguments)
    except (TypeError, ValueError) as error:
        return {'result': 'error', 'message': str(error)}
    capability = find_capability(app_id, config, runtime=tool.get('runtime'))
    if capability is None:
        return {'result': 'error', 'message': 'compatible Seenical client is not online'}
    chatbot_id, conversation_type, conversation_id, im_user_id = _conversation_scope(config)
    plugin_id = _bound_plugin_id(app_id, chatbot_id, function_info)
    skill_versions = _active_skill_authorizations(app_id, chatbot_id, tool_id)
    builtin_tool = bool(function_info.get('seenical_builtin_tool'))
    if (not builtin_tool and not plugin_id) or not skill_versions:
        return {'result': 'error', 'message': 'client tool authorization changed'}
    client_context = config.get('seenical_client_context', {})
    if (not isinstance(client_context, dict)
            or str(client_context.get('client_instance_id', '')) != str(capability['client_instance_id'])
            or str(client_context.get('seenical_session_id', '')) != str(capability.get('seenical_session_id', ''))):
        return {'result': 'error', 'message': 'Seenical message context is missing or stale'}
    request_id = uuid.uuid4().hex
    request_info = {
        'schema_version': SCHEMA_VERSION,
        'request_id': request_id,
        'app_id': str(app_id),
        'chatbot_id': chatbot_id,
        'conversation_type': conversation_type,
        'conversation_id': conversation_id,
        'im_user_id': im_user_id,
        'client_instance_id': capability['client_instance_id'],
        'seenical_session_id': str(client_context.get('seenical_session_id', '')),
        'trigger_message_id': str(config.get('request_msg_id', '')),
        'trigger_from_user_id': im_user_id,
        'actor_subject_id': capability['actor_subject_id'],
        'actor_tenement_id': capability.get('actor_tenement_id', ''),
        'tool_id': tool_id,
        'tool_name': tool['title'],
        'tool_version': tool['version'],
        'plugin_id': plugin_id,
        'authorization_source': 'builtin' if builtin_tool else 'plugin',
        'skill_versions': skill_versions,
        'execution': tool['execution'],
        'risk': tool['risk'],
        'runtime': copy.deepcopy(tool.get('runtime')),
        'request': copy.deepcopy(tool.get('request')),
        'result_fields': copy.deepcopy(tool.get('result_fields', [])),
        'parameters': copy.deepcopy(tool.get('parameters', {})),
        'arguments': arguments,
        'arguments_hash': hashlib.sha256(_json(arguments).encode('utf-8')).hexdigest(),
        'preview': _preview_tool(app_id, tool_id, arguments, {
            'chatbot_id': chatbot_id, 'request_id': request_id
        }),
        'tool_call': tool_call,
        'continuation': continuation,
        'status': 'pending',
        'created_at': int(time.time()),
        'expires_at': int(time.time()) + REQUEST_TTL_SECONDS,
    }
    _redis().setex(request_key(request_id), REQUEST_TTL_SECONDS, _json(request_info))
    _audit(app_id, request_id, 'created', {
        'tool_id': tool_id, 'risk': tool['risk'],
        'tool_version': tool['version'], 'chatbot_id': chatbot_id,
        'conversation_type': conversation_type, 'conversation_id': conversation_id,
        'skill_versions': skill_versions,
        'actor_subject_id': capability['actor_subject_id'],
        'arguments_hash': request_info['arguments_hash'],
        'diff_summary': _audit_diff_summary(request_info.get('preview', {}))
    })
    return {'result': 'ok', 'data': request_info}


def public_request(request_info):
    if not isinstance(request_info, dict):
        return None
    result = copy.deepcopy(request_info)
    result.pop('continuation', None)
    result.pop('tool_call', None)
    result.pop('actor_subject_id', None)
    result.pop('actor_tenement_id', None)
    result.pop('arguments_hash', None)
    return result


def _request_actor_error(request_info, actor):
    if str(request_info.get('actor_subject_id')) != str(actor.get('subject_id', '')):
        return 'tool request does not belong to current user'
    if str(request_info.get('im_user_id')) != str(actor.get('im_user_id', '')):
        return 'Console and IM user identities do not match'
    actor_instance_id = str(actor.get('client_instance_id', ''))
    if str(request_info.get('client_instance_id')) != actor_instance_id:
        return 'tool request belongs to another client instance'
    capability = _load(_redis().get(capability_key(
        request_info.get('app_id', ''), actor_instance_id)), None)
    expected = {
        'app_id': str(request_info.get('app_id', '')),
        'conversation_type': str(request_info.get('conversation_type', '')),
        'conversation_id': str(request_info.get('conversation_id', '')),
        'im_user_id': str(request_info.get('im_user_id', '')),
        'actor_subject_id': str(request_info.get('actor_subject_id', '')),
        'seenical_session_id': str(request_info.get('seenical_session_id', '')),
    }
    runtime = request_info.get('runtime', {})
    if (not capability
            or any(str(capability.get(key, '')) != value for key, value in expected.items())
            or str(request_info.get('chatbot_id', '')) not in
               capability.get('chatbot_ids', [capability.get('chatbot_id', '')])
            or int(capability.get('runtimes', {}).get(str(runtime.get('type', '')), 0) or 0)
               < int(runtime.get('version', 0) or 0)):
        return 'tool request client capability is no longer valid'
    return ''


def _request_execution_error(app_id, request_info, client_instance_id=''):
    chatbot_id = str(request_info.get('chatbot_id', ''))
    tool_id = str(request_info.get('tool_id', ''))
    registry_tool = tool_definition(tool_id)
    if (not registry_tool
            or int(registry_tool.get('version', 0)) != int(request_info.get('tool_version', 0))
            or registry_tool.get('execution') != request_info.get('execution')
            or registry_tool.get('risk') != request_info.get('risk')
            or registry_tool.get('runtime') != request_info.get('runtime')
            or registry_tool.get('request') != request_info.get('request')):
        return 'platform Tool definition changed; please request the operation again'
    if not is_feature_enabled(app_id, chatbot_id):
        return 'client tools are disabled'
    capability = _load(_redis().get(capability_key(
        app_id, client_instance_id or request_info.get('client_instance_id', ''))), None)
    if (not capability or str(capability.get('actor_subject_id')) != str(request_info.get('actor_subject_id'))
            or str(capability.get('im_user_id')) != str(request_info.get('im_user_id'))
            or int(capability.get('runtimes', {}).get(
                str(request_info.get('runtime', {}).get('type', '')), 0) or 0)
               < int(request_info.get('runtime', {}).get('version', 0) or 0)):
        return 'compatible Seenical client is not online'
    current_skills = _active_skill_authorizations(app_id, chatbot_id, tool_id)
    if current_skills != request_info.get('skill_versions', []):
        return 'Skill authorization changed; please request the operation again'
    if (request_info.get('authorization_source') != 'builtin'
            and not _plugin_still_allows_tool(
                app_id, chatbot_id, str(request_info.get('plugin_id', '')), tool_id)):
        return 'AI plugin authorization changed; please request the operation again'
    return ''


def get_request_for_actor(app_id, request_id, actor):
    request_info = _load(_redis().get(request_key(request_id)), None)
    if request_info is None or str(request_info.get('app_id')) != str(app_id):
        return {'result': 'error', 'message': 'tool request not found'}
    actor_error = _request_actor_error(request_info, actor)
    if actor_error:
        return {'result': 'error', 'message': actor_error}
    if (request_info.get('status') in ['pending', 'awaiting_client_result']
            and int(request_info.get('expires_at', 0)) <= int(time.time())):
        return {'result': 'error', 'message': 'tool request expired'}
    return {'result': 'ok', 'data': public_request(request_info)}


def _store_request(request_info):
    if request_info.get('status') in ['completed', 'failed', 'rejected']:
        ttl = RESULT_TTL_SECONDS
    else:
        ttl = max(1, int(request_info.get('expires_at', 0)) - int(time.time()))
    _redis().setex(request_key(request_info['request_id']), ttl, _json(request_info))


def decide_request(app_id, request_id, actor, decision,
                   authorization_revision=None):
    redis = _redis()
    request_info = _load(redis.get(request_key(request_id)), None)
    if request_info is None or str(request_info.get('app_id')) != str(app_id):
        return {'result': 'error', 'message': 'tool request not found'}
    actor_error = _request_actor_error(request_info, actor)
    if actor_error:
        return {'result': 'error', 'message': actor_error}
    lock_key = f'lanying_connector:agent_tools:decision_lock:{request_id}'
    lock_value = uuid.uuid4().hex
    if not redis.set(lock_key, lock_value, ex=300, nx=True):
        existing = _load(redis.get(result_key(request_id)), None)
        if existing is not None:
            return {
                'result': 'ok', 'data': existing,
                'request': request_info, 'resume': False
            }
        return {'result': 'error', 'message': 'tool request is being decided'}
    try:
        return _decide_request_locked(
            app_id, request_id, actor, decision, authorization_revision)
    finally:
        _delete_redis_key_if_value(lock_key, lock_value)


def _decide_request_locked(app_id, request_id, actor, decision,
                           authorization_revision=None):
    redis = _redis()
    request_info = _load(redis.get(request_key(request_id)), None)
    if request_info is None or str(request_info.get('app_id')) != str(app_id):
        return {'result': 'error', 'message': 'tool request not found'}
    actor_error = _request_actor_error(request_info, actor)
    if actor_error:
        return {'result': 'error', 'message': actor_error}
    existing = _load(redis.get(result_key(request_id)), None)
    if existing is not None:
        # A repeated approval never repeats business execution.  It can only
        # claim a retry of a model continuation that is explicitly recorded
        # as failed.
        if request_info.get('resume_status') == 'failed':
            retry_lock = f'lanying_connector:agent_tools:resume_retry_lock:{request_id}'
            if redis.set(retry_lock, uuid.uuid4().hex, ex=60, nx=True):
                request_info['resume_status'] = 'queued'
                request_info['resume_updated_at'] = int(time.time())
                request_info.pop('resume_message', None)
                _store_request(request_info)
                _audit(app_id, request_id, 'model_resume_retry', {
                    'actor_subject_id': str(actor.get('subject_id', ''))
                })
                return {
                    'result': 'ok', 'data': existing,
                    'request': request_info, 'resume': True
                }
        return {'result': 'ok', 'data': existing, 'request': request_info, 'resume': False}
    if int(request_info.get('expires_at', 0)) <= int(time.time()):
        return {'result': 'error', 'message': 'tool request expired'}
    if request_info.get('status') == 'awaiting_client_result' and decision == 'approve':
        data = public_request(request_info)
        data['execute_allowed'] = False
        return {
            'result': 'ok', 'data': data,
            'request': request_info, 'resume': False
        }
    if request_info.get('status') != 'pending':
        return {'result': 'error', 'message': 'tool request already decided'}
    if decision != 'approve':
        result = {'status': 'rejected', 'result': 'error', 'message': 'user rejected the operation'}
        redis.setex(result_key(request_id), RESULT_TTL_SECONDS, _json(result))
        request_info['status'] = 'rejected'
        _store_request(request_info)
        _audit(app_id, request_id, 'rejected', {'actor_subject_id': str(actor.get('subject_id', ''))})
        return {'result': 'ok', 'data': result, 'request': request_info, 'resume': True}
    execution_error = _request_execution_error(
        app_id, request_info, str(actor.get('client_instance_id', '')))
    if execution_error:
        result = {
            'status': 'rejected',
            'execution': {'result': 'error', 'message': execution_error}
        }
        redis.setex(result_key(request_id), RESULT_TTL_SECONDS, _json(result))
        request_info['status'] = 'rejected'
        _store_request(request_info)
        _audit(app_id, request_id, 'authorization_rejected', {
            'actor_subject_id': str(actor.get('subject_id', '')),
            'message': execution_error
        })
        return {'result': 'ok', 'data': result, 'request': request_info, 'resume': True}
    if request_info.get('execution') in ['local_action', 'butler_api']:
        request_info['status'] = 'awaiting_client_result'
        request_info['client_execution_started_at'] = int(time.time())
        _store_request(request_info)
        _audit(app_id, request_id, 'approved_local', {'actor_subject_id': str(actor.get('subject_id', ''))})
        data = public_request(request_info)
        data['execute_allowed'] = True
        return {'result': 'ok', 'data': data, 'request': request_info, 'resume': False}

    lock_key = f'lanying_connector:agent_tools:execute_lock:{request_id}'
    lock_value = uuid.uuid4().hex
    if not redis.set(lock_key, lock_value, ex=60, nx=True):
        existing = _load(redis.get(result_key(request_id)), None)
        if existing is not None:
            return {'result': 'ok', 'data': existing, 'request': request_info, 'resume': False}
        return {'result': 'error', 'message': 'tool request is being executed'}
    try:
        # Persist the execution state before calling business code.  If the
        # process dies, a retry cannot execute the same mutation again; the
        # user can safely create a fresh request after inspecting its state.
        request_info['status'] = 'executing'
        request_info['executed_client_instance_id'] = str(
            actor.get('client_instance_id', ''))
        _store_request(request_info)
        if hashlib.sha256(_json(request_info.get('arguments', {})).encode('utf-8')).hexdigest() != request_info.get('arguments_hash'):
            execution = {
                'result': 'error', 'code': 'arguments_tampered',
                'message': 'tool request arguments changed'
            }
        else:
            execution = execute_tool(
                app_id, request_info['tool_id'],
                request_info.get('arguments', {}), request_info)
        result = {'status': 'completed' if execution.get('result') == 'ok' else 'failed', 'execution': execution}
        redis.setex(result_key(request_id), RESULT_TTL_SECONDS, _json(result))
        request_info['status'] = result['status']
        request_info['completed_at'] = int(time.time())
        _store_request(request_info)
        _audit(app_id, request_id, result['status'], {
            'actor_subject_id': str(actor.get('subject_id', '')),
            'result': execution.get('result'), 'code': execution.get('code', '')
        })
        return {'result': 'ok', 'data': result, 'request': request_info, 'resume': True}
    finally:
        current_lock = redis.get(lock_key)
        if current_lock and (current_lock.decode('utf-8') if isinstance(current_lock, bytes) else current_lock) == lock_value:
            redis.delete(lock_key)


def submit_local_result(app_id, request_id, actor, client_result):
    redis = _redis()
    request_info = _load(redis.get(request_key(request_id)), None)
    if request_info is None or str(request_info.get('app_id')) != str(app_id):
        return {'result': 'error', 'message': 'tool request not found'}
    actor_error = _request_actor_error(request_info, actor)
    if actor_error:
        return {'result': 'error', 'message': actor_error}
    lock_key = f'lanying_connector:agent_tools:local_result_lock:{request_id}'
    lock_value = uuid.uuid4().hex
    if not redis.set(lock_key, lock_value, ex=60, nx=True):
        return {'result': 'error', 'message': 'tool result is being submitted'}
    try:
        return _submit_local_result_locked(
            app_id, request_id, actor, client_result)
    finally:
        _delete_redis_key_if_value(lock_key, lock_value)


def _submit_local_result_locked(app_id, request_id, actor, client_result):
    redis = _redis()
    request_info = _load(redis.get(request_key(request_id)), None)
    if request_info is None or str(request_info.get('app_id')) != str(app_id):
        return {'result': 'error', 'message': 'tool request not found'}
    if not isinstance(client_result, dict):
        return {'result': 'error', 'message': 'client result must be an object'}
    actor_error = _request_actor_error(request_info, actor)
    if actor_error:
        return {'result': 'error', 'message': actor_error}
    existing = _load(redis.get(result_key(request_id)), None)
    if existing is not None:
        return {'result': 'ok', 'data': existing, 'request': request_info, 'resume': False}
    if int(request_info.get('expires_at', 0)) <= int(time.time()):
        return {'result': 'error', 'message': 'tool request expired'}
    if (request_info.get('status') != 'awaiting_client_result'
            or request_info.get('execution') not in ['local_action', 'butler_api']):
        return {'result': 'error', 'message': 'tool request is not waiting for a client result'}
    execution_error = _request_execution_error(
        app_id, request_info, str(actor.get('client_instance_id', '')))
    if execution_error:
        return {'result': 'error', 'message': execution_error}
    if request_info.get('execution') == 'butler_api':
        try:
            client_result = _constrain_client_result(
                client_result, request_info.get('result_fields', []))
        except (TypeError, ValueError) as error:
            return {'result': 'error', 'message': str(error)}
    encoded = _json(client_result)
    if len(encoded.encode('utf-8')) > MAX_LOCAL_RESULT_BYTES:
        return {'result': 'error', 'message': 'client result is too large'}
    is_butler_api = request_info.get('execution') == 'butler_api'
    succeeded = not is_butler_api or bool(client_result.get('ok'))
    success_data = client_result.get('data', {}) if is_butler_api else client_result
    execution = ({'result': 'ok', 'data': success_data} if succeeded else {
                     'result': 'error',
                     'code': client_result.get('error_code', 'request_failed'),
                     'message': client_result.get('message', 'Butler API request failed')
                 })
    result = {'status': 'completed' if succeeded else 'failed', 'execution': execution}
    redis.setex(result_key(request_id), RESULT_TTL_SECONDS, _json(result))
    request_info['status'] = result['status']
    request_info['completed_at'] = int(time.time())
    _store_request(request_info)
    _audit(app_id, request_id, 'completed_client', {'actor_subject_id': str(actor.get('subject_id', ''))})
    return {'result': 'ok', 'data': result, 'request': request_info, 'resume': True}


def _constrain_client_result(value, allowed_fields):
    if not isinstance(value, dict):
        raise ValueError('client result must be an object')
    if _contains_secret_key(value):
        raise ValueError('client result contains a forbidden credential field')
    allowed = set(str(field) for field in allowed_fields)
    envelope = {'ok': bool(value.get('ok', False))}
    if not envelope['ok']:
        envelope['error_code'] = str(value.get('error_code', 'request_failed'))[:100]
        envelope['message'] = str(value.get('message', 'Butler API request failed'))[:300]
        return envelope
    payload = value.get('data', {})
    if not isinstance(payload, dict):
        envelope['data'] = {'value': payload}
        return envelope
    envelope['data'] = {
        key: copy.deepcopy(item) for key, item in payload.items()
        if key in allowed and not _contains_secret_key({key: item})
    }
    return envelope


def tool_result_for_model(result):
    if isinstance(result, dict) and 'execution' in result:
        return result['execution']
    return result


def _validate_repo_path(path):
    raw_value = str(path or '').strip()
    if raw_value.startswith('/'):
        raise ValueError('invalid repository path')
    value = raw_value.rstrip('/')
    pure = PurePosixPath(value)
    if not value or pure.is_absolute() or '..' in pure.parts or '\\' in value:
        raise ValueError('invalid repository path')
    return value


def _validate_git_ref(ref):
    value = str(ref or '').strip()
    if (not value or len(value) > 200 or value.startswith('/')
            or '..' in value or not re.fullmatch(r'[A-Za-z0-9._/-]+', value)):
        raise ValueError('invalid Git ref')
    return value


def _parse_github_repository(repository_url):
    parsed = lanying_grow_ai.parse_github_url(str(repository_url or '').strip())
    if parsed.get('result') != 'ok':
        raise ValueError('only GitHub repositories are supported')
    if (not re.fullmatch(r'[A-Za-z0-9-]{1,39}', str(parsed.get('github_owner', '')))
            or not re.fullmatch(r'[A-Za-z0-9._-]{1,100}', str(parsed.get('github_repo', '')))):
        raise ValueError('invalid GitHub repository')
    return parsed


def _github_headers(token):
    headers = {'Accept': 'application/vnd.github.v3+json'}
    if token:
        headers['Authorization'] = 'token ' + token
    return headers


def _github_file(owner, repo, path, ref, token, limit):
    url = f'https://api.github.com/repos/{owner}/{repo}/contents/{path}'
    response = requests.get(url, params={'ref': ref}, headers=_github_headers(token), timeout=(10, 30))
    if response.status_code != 200:
        raise ValueError(f'repository file unavailable: {path}')
    info = response.json()
    if info.get('type') != 'file' or int(info.get('size', 0)) > limit:
        raise ValueError(f'invalid repository file: {path}')
    content = base64.b64decode(info.get('content', '')).decode('utf-8')
    if len(content.encode('utf-8')) > limit:
        raise ValueError(f'repository file too large: {path}')
    return content, str(info.get('sha', ''))


def _github_commit(owner, repo, ref, token):
    url = f'https://api.github.com/repos/{owner}/{repo}/commits/{ref}'
    response = requests.get(url, headers=_github_headers(token), timeout=(10, 30))
    if response.status_code != 200:
        raise ValueError('repository revision is unavailable')
    revision = str(response.json().get('sha', ''))
    if not revision or len(revision) != 40:
        raise ValueError('repository revision is invalid')
    return revision


def _github_tree(owner, repo, revision, token):
    url = f'https://api.github.com/repos/{owner}/{repo}/git/trees/{revision}'
    response = requests.get(
        url, params={'recursive': '1'}, headers=_github_headers(token),
        timeout=(10, 30))
    if response.status_code != 200:
        raise ValueError('repository tree is unavailable')
    value = response.json()
    if value.get('truncated'):
        raise ValueError('repository tree is too large')
    tree = value.get('tree', [])
    if not isinstance(tree, list):
        raise ValueError('repository tree is invalid')
    public_files = set()
    for raw in tree:
        item = raw if isinstance(raw, dict) else {}
        path = str(item.get('path', ''))
        relevant = (
            path == '.seenical' or path.startswith('.seenical/')
            or path == 'SKILL.md' or path == 'agents'
            or path.startswith('agents/') or path == 'references'
            or path.startswith('references/') or path == 'scripts'
            or path.startswith('scripts/'))
        if not relevant:
            continue
        entry_type = str(item.get('type', ''))
        mode = str(item.get('mode', ''))
        if entry_type == 'tree':
            continue
        if entry_type != 'blob' or mode == '120000':
            raise ValueError('public Skill repository contains links or submodules')
        if int(item.get('size', 0) or 0) > MAX_SKILL_BYTES:
            raise ValueError('public Skill repository file is too large')
        public_files.add(_validate_repo_path(path))
    if len(public_files) > MAX_SKILL_REPOSITORY_FILES:
        raise ValueError('public Skill repository contains too many files')
    return public_files


def _contains_secret_key(value):
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in SECRET_FIELD_NAMES or _contains_secret_key(item):
                return True
    elif isinstance(value, list):
        return any(_contains_secret_key(item) for item in value)
    return False


def _normalize_tool_requirements(values, inherited=None, available_tools=None):
    inherited = inherited or {}
    result = {}
    if isinstance(values, str):
        values = [values]
    if isinstance(values, dict):
        values = [{'id': key, 'min_version': value} for key, value in values.items()]
    for raw in values if isinstance(values, list) else []:
        if isinstance(raw, str):
            tool_id = raw
            min_version = inherited.get(tool_id, 1)
        elif isinstance(raw, dict):
            tool_id = str(raw.get('id') or raw.get('tool_id') or '')
            min_version = raw.get('min_version', raw.get('version', inherited.get(tool_id, 1)))
        else:
            raise ValueError('invalid Tool requirement')
        try:
            min_version = int(min_version)
        except (TypeError, ValueError):
            raise ValueError('invalid Tool minimum version')
        registry_tool = ((available_tools or {}).get(tool_id)
                         or TOOL_REGISTRY.get(tool_id))
        if registry_tool is None:
            raise ValueError('unknown Skill tool: ' + tool_id)
        if min_version < 1 or registry_tool['version'] < min_version:
            raise ValueError('unsupported Skill tool version: ' + tool_id)
        result[tool_id] = max(result.get(tool_id, 1), min_version)
    return result


def _parse_skill_markdown(text):
    metadata = {}
    instructions = str(text or '')
    if instructions.startswith('---\n'):
        end = instructions.find('\n---\n', 4)
        if end >= 0:
            parsed = yaml.safe_load(instructions[4:end]) or {}
            if not isinstance(parsed, dict):
                raise ValueError('invalid SKILL.md frontmatter')
            metadata = parsed
            instructions = instructions[end + 5:]
    for forbidden in ['handler', 'command', 'script', 'callback_url', 'auth']:
        if forbidden in metadata:
            raise ValueError('SKILL.md cannot declare executable metadata: ' + forbidden)
    return metadata, instructions.strip()


def _skill_resource_paths(repository_files, skill_dir):
    prefix = '' if skill_dir == '.' else skill_dir + '/'
    result = []
    for path in sorted(repository_files or []):
        if path == '.seenical/manifest.json':
            continue
        if skill_dir == '.':
            if path == 'SKILL.md' or not path.startswith(
                    ('agents/', 'references/', 'scripts/', '.seenical/')):
                continue
            relative = path
        else:
            if not path.startswith(prefix) or path == prefix + 'SKILL.md':
                continue
            relative = path[len(prefix):]
        allowed = (
            relative == 'agents/openai.yaml'
            or relative in ['.seenical/runtime.json', '.seenical/tools.json']
            or (relative.startswith('references/')
                and PurePosixPath(relative).suffix.lower()
                in ['.md', '.json', '.yaml', '.yml'])
        )
        if not allowed:
            raise ValueError(
                'public Skill repository contains unsupported files: ' + path)
        result.append((path, relative))
    return result


def _resolve_local_schema(schema, definitions):
    if not isinstance(schema, dict):
        raise ValueError('Tool parameters must be a JSON Schema object')
    if '$ref' in schema:
        ref = str(schema.get('$ref', ''))
        prefix = '#/definitions/'
        if not ref.startswith(prefix) or ref[len(prefix):] not in definitions:
            raise ValueError('unsupported Tool schema reference')
        return _resolve_local_schema(definitions[ref[len(prefix):]], definitions)
    result = copy.deepcopy(schema)
    if isinstance(result.get('properties'), dict):
        result['properties'] = {
            key: _resolve_local_schema(value, definitions)
            for key, value in result['properties'].items()
        }
    if isinstance(result.get('items'), dict):
        result['items'] = _resolve_local_schema(result['items'], definitions)
    return result


def _normalize_butler_runtime(runtime_text, tools_text, skill_id):
    runtime_doc = json.loads(runtime_text)
    tools_doc = json.loads(tools_text)
    if set(runtime_doc) != {'schema_version', 'skill_id', 'runtime', 'tools_file'}:
        raise ValueError('invalid runtime.json fields')
    if (int(runtime_doc.get('schema_version', 0) or 0) != SCHEMA_VERSION
            or str(runtime_doc.get('skill_id', '')) != skill_id
            or str(runtime_doc.get('tools_file', '')) != '.seenical/tools.json'):
        raise ValueError('invalid Seenical runtime descriptor')
    runtime = runtime_doc.get('runtime', {})
    if (not isinstance(runtime, dict)
            or set(runtime) != {'type', 'version', 'authentication'}
            or runtime.get('type') != 'butler_api'
            or int(runtime.get('version', 0) or 0) != 1
            or runtime.get('authentication') != 'host_console_session'):
        raise ValueError('unsupported Seenical runtime')
    if set(tools_doc) - {'schema_version', 'tools', 'definitions'}:
        raise ValueError('unsupported tools.json fields')
    if int(tools_doc.get('schema_version', 0) or 0) != SCHEMA_VERSION:
        raise ValueError('unsupported tools.json schema_version')
    definitions = tools_doc.get('definitions', {})
    if not isinstance(definitions, dict):
        raise ValueError('invalid Tool schema definitions')
    normalized = {}
    function_names = set()
    for raw in tools_doc.get('tools', []):
        if not isinstance(raw, dict) or set(raw) != {
                'tool_id', 'version', 'function_name', 'title', 'description',
                'risk', 'parameters', 'request', 'result_fields'}:
            raise ValueError('invalid Tool definition fields')
        tool_id = str(raw.get('tool_id', ''))
        function_name = str(raw.get('function_name', ''))
        if (not re.fullmatch(r'[a-z0-9][a-z0-9._-]{0,99}', tool_id)
                or not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]{0,99}', function_name)
                or tool_id in normalized or function_name in function_names):
            raise ValueError('invalid or duplicate Tool identifier')
        request_info = raw.get('request', {})
        method = str(request_info.get('method', '')).upper()
        path = str(request_info.get('path', ''))
        placement = str(request_info.get('arguments', ''))
        if (set(request_info) != {'method', 'path', 'arguments'}
                or method not in ['GET', 'POST', 'PUT', 'PATCH']
                or not re.fullmatch(r'/app/[A-Za-z0-9_./-]+', path)
                or '..' in PurePosixPath(path).parts
                or placement not in ['query', 'body']
                or (method == 'GET') != (placement == 'query')):
            raise ValueError('unsafe Butler API Tool request')
        risk = str(raw.get('risk', ''))
        if risk not in ['read', 'write', 'execute', 'destructive']:
            raise ValueError('invalid Tool risk')
        if method == 'GET' and risk != 'read' or method != 'GET' and risk == 'read':
            raise ValueError('Tool risk cannot weaken HTTP method risk')
        result_fields = raw.get('result_fields', [])
        if (not isinstance(result_fields, list) or not result_fields
                or any(not re.fullmatch(r'[A-Za-z0-9_.-]{1,100}', str(value))
                       or str(value).lower() in SECRET_FIELD_NAMES
                       for value in result_fields)):
            raise ValueError('invalid Tool result field constraint')
        parameters = _resolve_local_schema(raw.get('parameters'), definitions)
        tool = {
            'tool_id': tool_id, 'version': int(raw.get('version', 0) or 0),
            'function_name': function_name, 'title': str(raw.get('title', ''))[:200],
            'description': str(raw.get('description', ''))[:1000],
            'risk': risk, 'execution': 'butler_api', 'parameters': parameters,
            'runtime': copy.deepcopy(runtime),
            'request': {'method': method, 'path': path, 'arguments': placement},
            'result_fields': [str(value) for value in result_fields],
        }
        if tool['version'] < 1 or not tool['title'] or not tool['description']:
            raise ValueError('invalid Tool metadata')
        validate_tool_arguments(tool, {}) if not parameters.get('required') else None
        normalized[tool_id] = tool
        function_names.add(function_name)
    if not normalized or len(normalized) > 100:
        raise ValueError('invalid Tool count')
    return copy.deepcopy(runtime), normalized


def _skill_file_path(skill_dir, relative_path):
    return relative_path if skill_dir == '.' else skill_dir + '/' + relative_path


# Public Skill catalog -----------------------------------------------------

def _public_repository_config():
    repository_url = str(os.getenv(
        'SEENICAL_PUBLIC_SKILL_REPOSITORY_URL', '')).strip()
    if not repository_url:
        raise ValueError('public Skill repository is not configured')
    parsed = _parse_github_repository(repository_url)
    return {
        'repository_url': repository_url,
        'owner': parsed['github_owner'],
        'repo': parsed['github_repo'],
        'ref': _validate_git_ref(os.getenv(
            'SEENICAL_PUBLIC_SKILL_REPOSITORY_REF', 'main')),
        'manifest_path': _validate_repo_path(os.getenv(
            'SEENICAL_PUBLIC_SKILL_MANIFEST_PATH',
            '.seenical/manifest.json')),
    }


def _normalize_public_skill_catalog(config, source_commit, manifest_text,
                                    manifest_sha, repository_files=None):
    manifest = json.loads(manifest_text)
    if int(manifest.get('schema_version', 0) or 0) != SCHEMA_VERSION:
        raise ValueError('unsupported manifest schema_version')
    unknown = set(manifest) - {'schema_version', 'skills'}
    if unknown:
        raise ValueError('unsupported manifest fields: ' + ','.join(sorted(unknown)))
    descriptors = manifest.get('skills', [])
    if not isinstance(descriptors, list) or len(descriptors) != 1:
        raise ValueError('public Skill repository must contain exactly one Skill')
    skills = []
    seen_ids = set()
    total_bytes = 0
    for raw in descriptors:
        if not isinstance(raw, dict):
            raise ValueError('invalid Skill descriptor')
        descriptor = dict(raw)
        descriptor_unknown = set(descriptor) - {
            'skill_id', 'name', 'description', 'path', 'tools', 'scopes'
        }
        if descriptor_unknown:
            raise ValueError('unsupported Skill fields: ' + ','.join(
                sorted(descriptor_unknown)))
        skill_id = str(descriptor.get('skill_id', '')).strip()
        if (not re.fullmatch(r'[a-z0-9][a-z0-9._-]{0,99}', skill_id)
                or skill_id in seen_ids):
            raise ValueError('invalid or duplicate skill_id')
        seen_ids.add(skill_id)
        skill_dir = _validate_repo_path(descriptor.get('path'))
        if skill_dir != '.':
            raise ValueError('single-repository Skill path must be the repository root')
        skill_text, skill_sha = _github_file(
            config['owner'], config['repo'], _skill_file_path(skill_dir, 'SKILL.md'),
            source_commit, '', MAX_SKILL_BYTES)
        total_bytes += len(skill_text.encode('utf-8'))
        if total_bytes > MAX_SKILL_TOTAL_BYTES:
            raise ValueError('Skill content is too large')
        metadata, instructions = _parse_skill_markdown(skill_text)
        if set(metadata) - {'name', 'description'}:
            raise ValueError('SKILL.md can only contain descriptive frontmatter')
        resources = {}
        for resource_path, relative_path in _skill_resource_paths(
                repository_files, skill_dir):
            resource_text, _ = _github_file(
                config['owner'], config['repo'], resource_path,
                source_commit, '', MAX_SKILL_BYTES)
            total_bytes += len(resource_text.encode('utf-8'))
            if total_bytes > MAX_SKILL_TOTAL_BYTES:
                raise ValueError('Skill content is too large')
            if relative_path.endswith(('.json', '.yaml', '.yml')):
                parsed_resource = (json.loads(resource_text)
                                   if relative_path.endswith('.json')
                                   else yaml.safe_load(resource_text))
                if not isinstance(parsed_resource, dict):
                    raise ValueError('invalid structured Skill resource: ' + resource_path)
            resources[relative_path] = resource_text
        if ('.seenical/runtime.json' not in resources
                or '.seenical/tools.json' not in resources):
            raise ValueError('public Skill runtime files are missing')
        runtime, available_tools = _normalize_butler_runtime(
            resources['.seenical/runtime.json'],
            resources['.seenical/tools.json'], skill_id)
        tool_requirements = _normalize_tool_requirements(
            descriptor.get('tools', []), available_tools=available_tools)
        if set(tool_requirements) != set(available_tools):
            raise ValueError('manifest Tool list must match tools.json')
        scopes = descriptor.get('scopes', [])
        if isinstance(scopes, str):
            scopes = [scopes]
        if not isinstance(scopes, list) or len(scopes) > 100:
            raise ValueError('invalid Skill scopes')
        normalized = {
            'skill_id': skill_id,
            'name': str(descriptor.get('name') or metadata.get('name') or skill_id)[:200],
            'description': str(descriptor.get('description') or metadata.get('description', ''))[:1000],
            'path': skill_dir,
            'instructions': instructions,
            'content_summary': ' '.join(
                line.strip() for line in instructions.splitlines()
                if line.strip())[:300],
            'required_tools': sorted(tool_requirements),
            'tool_requirements': tool_requirements,
            'runtime': runtime,
            'tools': [available_tools[key] for key in sorted(available_tools)],
            'scopes': sorted(set(str(value) for value in scopes)),
            'sha': skill_sha,
            'resources': resources,
        }
        normalized['security_digest'] = hashlib.sha256(_json({
            'skill_id': skill_id,
            'tool_requirements': tool_requirements,
            'runtime': runtime,
            'tools': normalized['tools'],
            'scopes': normalized['scopes'],
        }).encode('utf-8')).hexdigest()
        normalized['revision'] = hashlib.sha256(
            _json(normalized).encode('utf-8')).hexdigest()
        normalized['source_commit'] = source_commit
        skills.append(normalized)
    catalog = {
        'schema_version': SCHEMA_VERSION,
        'repository_url': config['repository_url'],
        'ref': config['ref'],
        'source_commit': source_commit,
        'manifest_sha': manifest_sha,
        'skills': sorted(skills, key=lambda value: value['skill_id']),
        'synced_at': int(time.time()),
    }
    catalog['revision'] = hashlib.sha256(
        _json(catalog).encode('utf-8')).hexdigest()
    if repository_files is not None:
        expected_files = {config['manifest_path']}
        for skill in skills:
            expected_files.add(_skill_file_path(skill['path'], 'SKILL.md'))
            expected_files.update(
                _skill_file_path(skill['path'], path)
                for path in skill.get('resources', {}))
        if set(repository_files) != expected_files:
            raise ValueError(
                'public Skill repository contains unsupported files')
    return catalog


def _cache_public_catalog(catalog):
    redis = _redis()
    pipe = redis.pipeline(transaction=True)
    pipe.set(PUBLIC_CATALOG_CACHE_KEY, _json(catalog))
    for skill in catalog.get('skills', []):
        pipe.set(
            'lanying_connector:agent_tools:public_skill:'
            + skill['skill_id'] + ':' + skill['revision'], _json(skill))
    pipe.execute()


def get_public_catalog():
    cached = _load(_redis().get(PUBLIC_CATALOG_CACHE_KEY), None)
    if cached:
        return cached
    catalog = lanying_pgvector.get_active_public_skill_catalog()
    if catalog:
        _cache_public_catalog(catalog)
    return catalog


def public_catalog_view(catalog=None):
    value = copy.deepcopy(catalog or get_public_catalog() or {})
    for skill in value.get('skills', []):
        skill['instructions_digest'] = hashlib.sha256(
            str(skill.get('instructions', '')).encode('utf-8')).hexdigest()
        skill.pop('instructions', None)
    return value


def public_skill_detail(skill_id):
    catalog = get_public_catalog() or {}
    for skill in catalog.get('skills', []):
        if str(skill.get('skill_id', '')) == str(skill_id):
            result = copy.deepcopy(skill)
            result['instructions_digest'] = hashlib.sha256(
                str(result.get('instructions', '')).encode('utf-8')).hexdigest()
            result.pop('instructions', None)
            result['catalog_revision'] = catalog.get('revision', '')
            return {'result': 'ok', 'data': result}
    return {'result': 'error', 'message': 'public Skill not found'}


def sync_public_catalog():
    try:
        config = _public_repository_config()
        source_commit = _github_commit(
            config['owner'], config['repo'], config['ref'], '')
        current = get_public_catalog()
        if current and str(current.get('source_commit', '')) == source_commit:
            return {'result': 'ok', 'data': public_catalog_view(current), 'unchanged': True}
        repository_files = _github_tree(
            config['owner'], config['repo'], source_commit, '')
        manifest_text, manifest_sha = _github_file(
            config['owner'], config['repo'], config['manifest_path'],
            source_commit, '', MAX_MANIFEST_BYTES)
        catalog = _normalize_public_skill_catalog(
            config, source_commit, manifest_text, manifest_sha,
            repository_files)
        persisted = lanying_pgvector.save_public_skill_catalog(catalog)
        if persisted.get('result') != 'ok':
            return persisted
        _cache_public_catalog(catalog)
        return {'result': 'ok', 'data': public_catalog_view(catalog)}
    except (ValueError, TypeError, json.JSONDecodeError,
            UnicodeDecodeError) as error:
        return {'result': 'error', 'message': str(error)}
    except requests.RequestException:
        return {'result': 'error', 'message': 'failed to read public Skill repository'}


def _rate_limit_notification(remote_addr):
    minute = int(time.time()) // 60
    ip_digest = hashlib.sha256(str(remote_addr or '').encode('utf-8')).hexdigest()[:24]
    redis = _redis()
    pipe = redis.pipeline(transaction=True)
    pipe.incr(f'lanying_connector:agent_tools:catalog_notify:ip:{minute}:{ip_digest}')
    pipe.expire(f'lanying_connector:agent_tools:catalog_notify:ip:{minute}:{ip_digest}', 120)
    pipe.incr(f'lanying_connector:agent_tools:catalog_notify:global:{minute}')
    pipe.expire(f'lanying_connector:agent_tools:catalog_notify:global:{minute}', 120)
    values = pipe.execute()
    return int(values[0]) <= 10 and int(values[2]) <= 60


def enqueue_public_catalog_sync(remote_addr='', queue_task=None):
    if not _rate_limit_notification(remote_addr):
        return {'result': 'error', 'code': 'rate_limited',
                'message': 'too many catalog notifications'}
    redis = _redis()
    dirty_value = uuid.uuid4().hex
    redis.set(PUBLIC_CATALOG_DIRTY_KEY, dirty_value, ex=24 * 3600)
    lock_value = uuid.uuid4().hex
    if not redis.set(PUBLIC_CATALOG_SYNC_LOCK_KEY, lock_value, ex=600, nx=True):
        return {'result': 'ok', 'data': {'status': 'coalesced'}}
    try:
        recovery_task = None
        if queue_task is None:
            from lanying_tasks import (
                public_skill_catalog_recovery_task,
                public_skill_catalog_sync_task,
            )
            queue_task = public_skill_catalog_sync_task
            recovery_task = public_skill_catalog_recovery_task
        if recovery_task is not None:
            recovery_task.apply_async(countdown=610)
        queue_task.apply_async(args=[lock_value])
    except Exception:
        _delete_redis_key_if_value(PUBLIC_CATALOG_SYNC_LOCK_KEY, lock_value)
        raise
    return {'result': 'ok', 'data': {'status': 'accepted'}}


def _delete_redis_key_if_value(key, expected):
    redis = _redis()
    pipe = redis.pipeline(transaction=True)
    try:
        pipe.watch(key)
        current = pipe.get(key)
        current = current.decode('utf-8') if isinstance(current, bytes) else current
        if current != expected:
            pipe.unwatch()
            return False
        pipe.multi()
        pipe.delete(key)
        pipe.execute()
        return True
    except Exception:
        return False


def _release_catalog_lock(lock_value):
    _delete_redis_key_if_value(PUBLIC_CATALOG_SYNC_LOCK_KEY, lock_value)


def run_public_catalog_sync(lock_value, release_lock=True):
    redis = _redis()
    try:
        for _ in range(3):
            marker = lanying_redis.redis_get(redis, PUBLIC_CATALOG_DIRTY_KEY)
            result = sync_public_catalog()
            if result.get('result') != 'ok':
                return result
            if _delete_redis_key_if_value(PUBLIC_CATALOG_DIRTY_KEY, marker):
                return result
        return {'result': 'ok', 'data': public_catalog_view(), 'dirty': True}
    finally:
        if release_lock:
            _release_catalog_lock(lock_value)


def im_binding_projection_key(app_id):
    return f'lanying_connector:agent_tools:im_binding:{app_id}'


def sync_im_binding_projection(app_id, data):
    try:
        revision = int(data.get('revision', 0) or 0)
    except (TypeError, ValueError):
        return {'result': 'error', 'message': 'invalid IM binding revision'}
    normalized = {
        'schema_version': SCHEMA_VERSION,
        'app_id': str(app_id),
        'status': str(data.get('status', '')),
        'im_user_id': str(data.get('im_user_id', '')),
        'revision': revision,
        'synced_at': int(time.time()),
    }
    if (normalized['status'] != 'BOUND'
            or not normalized['im_user_id'].isdigit() or revision < 1):
        return {'result': 'error', 'message': 'invalid Seenical IM binding'}
    key = im_binding_projection_key(app_id)
    redis = _redis()
    pipe = redis.pipeline(transaction=True)
    try:
        pipe.watch(key)
        current = _load(pipe.get(key), None)
        if current:
            current_revision = int(current.get('revision', 0) or 0)
            if revision < current_revision:
                pipe.unwatch()
                return {'result': 'error', 'message': 'IM binding revision cannot move backwards'}
            if (revision == current_revision
                    and any(str(current.get(field, '')) != str(normalized.get(field, ''))
                            for field in ['app_id', 'status', 'im_user_id', 'revision'])):
                pipe.unwatch()
                return {'result': 'error', 'message': 'IM binding revision content changed'}
        pipe.multi()
        pipe.set(key, _json(normalized))
        pipe.execute()
    except Exception:
        return {'result': 'error', 'message': 'IM binding projection changed concurrently'}
    return {'result': 'ok', 'data': {'revision': revision, 'status': 'synced'}}


def get_im_binding_projection(app_id):
    return _load(_redis().get(im_binding_projection_key(app_id)), None)


def get_public_skill_revision(skill_id, revision):
    key = ('lanying_connector:agent_tools:public_skill:'
           + str(skill_id) + ':' + str(revision))
    skill = _load(_redis().get(key), None)
    if skill:
        return skill
    skill = lanying_pgvector.get_public_skill_revision(skill_id, revision)
    if skill:
        _redis().set(key, _json(skill))
    return skill


def get_active_skills(app_id, chatbot_id):
    binding = get_im_binding_projection(app_id)
    skill = _official_skill()
    if not binding or binding.get('status') != 'BOUND' or not skill:
        return []
    return [{
        'repository_id': skill.get('skill_id', ''),
        'revision': skill.get('revision', ''),
        'source_commit': skill.get('source_commit', ''),
        'skills': [skill],
    }]


_legacy_is_feature_enabled = is_feature_enabled


def is_feature_enabled(app_id, chatbot_id=''):
    if not _truthy(os.getenv('LANYING_AGENT_TOOLS_PLATFORM_ENABLED', 'on')):
        return False
    return _legacy_is_feature_enabled(app_id, chatbot_id)


def apply_active_skills(app_id, config, messages, functions):
    chatbot_id = str(config.get('chatbot_id', ''))
    binding = get_im_binding_projection(app_id)
    current_im_user_id = _conversation_scope(config)[3]
    if (not binding or str(binding.get('im_user_id', '')) != current_im_user_id):
        return messages, functions
    active_tool_ids = set()
    insert_at = 0
    while insert_at < len(messages) and messages[insert_at].get('role') in ['system', 'developer']:
        insert_at += 1
    for repository in get_active_skills(app_id, chatbot_id):
        for skill in repository.get('skills', []):
            active_tool_ids.update(skill.get('required_tools', []))
            instructions = str(skill.get('instructions', '')).strip()
            if instructions:
                messages.insert(insert_at, {
                    'role': 'system',
                    'content': (f"Seenical Skill: {skill.get('name', '')}\n"
                                f"Repository revision: {repository.get('revision', '')}\n\n{instructions}")
                })
                insert_at += 1
    existing_tool_ids = {
        resolve_tool_id(function_info) for function_info in functions
        if isinstance(function_info, dict)
    }
    for tool_id in sorted(active_tool_ids - existing_tool_ids):
        tool = tool_definition(tool_id)
        if tool and find_capability(app_id, config, runtime=tool.get('runtime')):
            function_info = registry_function(tool_id)
            function_info['seenical_builtin_tool'] = True
            function_info['seenical_skill_versions'] = (
                _active_skill_authorizations(app_id, chatbot_id, tool_id))
            functions.append(function_info)
    return messages, functions

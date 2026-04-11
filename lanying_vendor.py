import lanying_vendor_openai
import lanying_vendor_minimax
import lanying_vendor_baidu
import lanying_vendor_zhipuai
import lanying_vendor_azure
import lanying_vendor_claude
import lanying_vendor_deepseek
import lanying_vendor_aliyun
import lanying_vendor_volcengine
import lanying_vendor_moonshot
import lanying_vendor_wanjie
import lanying_vendor_aws
import lanying_vendor_siliconflow
import lanying_vendor_ppinfra
import copy
import logging
import lanying_config
import lanying_slack
from datetime import datetime
import time
import lanying_utils
import json
import lanying_redis
import re
import lanying_openai_compat
from urllib.parse import urlparse, urlunparse

vendor_to_module = {
    'openai': lanying_vendor_openai,
    'aws': lanying_vendor_aws,
    'volcengine': lanying_vendor_volcengine,
    'siliconflow': lanying_vendor_siliconflow,
    'deepseek': lanying_vendor_deepseek,
    # 'ppinfra': lanying_vendor_ppinfra,
    'minimax': lanying_vendor_minimax,
    'baidu': lanying_vendor_baidu,
    'zhipuai': lanying_vendor_zhipuai,
    "azure": lanying_vendor_azure,
    "claude": lanying_vendor_claude,
    'aliyun': lanying_vendor_aliyun,
    'moonshot': lanying_vendor_moonshot,
    'wanjie': lanying_vendor_wanjie
}

OPENROUTER_SERVICES = ['chatgpt', 'claude', 'deepseek', 'doubao', 'kimi', 'ernie', 'qwen', 'zhipuai', 'minimax']
SERVICE_CATALOG_ORDER = OPENROUTER_SERVICES + ['xiaomi']
AGGREGATE_API_TYPE_VENDORS = ['openai', 'openrouter', 'volcengine', 'aws', 'siliconflow', 'azure', 'aliyun']


def _default_extra_param_defs():
    return []


def _is_aggregate_api_type_vendor(vendor, handler_vendor):
    if vendor in AGGREGATE_API_TYPE_VENDORS:
        return True
    if handler_vendor == 'openai':
        return True
    return False


def _finalize_api_type_config(config):
    config_copy = copy.deepcopy(config)
    is_aggregate_platform = _is_aggregate_api_type_vendor(
        config_copy.get('vendor', ''),
        config_copy.get('handler_vendor', '')
    )
    if is_aggregate_platform:
        config_copy['services'] = copy.deepcopy(OPENROUTER_SERVICES)
    return config_copy


def _custom_model_extra_param_defs():
    return [
        {
            'key': 'id',
            'label': '模型 ID',
            'required': True,
            'input_type': 'text',
            'placeholder': '请输入模型 ID'
        },
        {
            'key': 'reasoning',
            'label': '是否推理模型',
            'required': True,
            'input_type': 'boolean',
            'placeholder': ''
        },
        {
            'key': 'input',
            'label': '模型输入类型',
            'required': True,
            'input_type': 'checkbox',
            'options': [
                {'label': '文本', 'value': 'text'},
                {'label': '图片', 'value': 'image'},
                {'label': '音频', 'value': 'audio'}
            ],
            'placeholder': ''
        },
        {
            'key': 'token_limit',
            'label': '上下文窗口',
            'required': True,
            'input_type': 'number',
            'placeholder': '请输入上下文长度'
        },
        {
            'key': 'max_output_tokens',
            'label': '最大输出 Token',
            'required': True,
            'input_type': 'number',
            'placeholder': '请输入最大输出长度'
        }
    ]


def _service_name_to_label_key(service):
    return f"service_name_{service}"


def _chat_models_by_service():
    grouped = {}
    for vendor, module in vendor_to_module.items():
        if vendor in HIDDEN_VENDOR_CONFIGS:
            continue
        for index, config in enumerate(module.model_configs()):
            if config.get('type') != 'chat':
                continue
            service = config.get('service', '')
            if service == '':
                continue
            grouped.setdefault(service, [])
            config_copy = copy.deepcopy(config)
            config_copy['handler_vendor'] = vendor
            config_copy['_source_index'] = index
            grouped[service].append(config_copy)
    for _, models in grouped.items():
        models[:] = _sort_chat_models_for_display(models)
        deduped_models = []
        used_models = set()
        for model in models:
            model_name = str(model.get('model', '')).strip()
            handler_vendor = str(model.get('handler_vendor', '')).strip()
            dedupe_key = (handler_vendor, model_name)
            if model_name == '' or dedupe_key in used_models:
                continue
            used_models.add(dedupe_key)
            model.pop('_source_index', None)
            deduped_models.append(model)
        models[:] = deduped_models
    return grouped


def _sort_model_configs_for_display(configs):
    chat_models = []
    other_models = []
    for config in configs:
        if config.get('type') == 'chat':
            chat_models.append(config)
        else:
            other_models.append(config)
    chat_models = _sort_chat_models_for_display(chat_models)
    return chat_models + other_models


def _should_preserve_openai_chat_order(item):
    return item.get('handler_vendor') == 'openai' or item.get('vendor') == 'openai'


def _sort_chat_models_for_display(models):
    default_models = [item for item in models if item.get('is_default', False)]
    normal_models = [item for item in models if not item.get('is_default', False)]

    default_models.sort(key=lambda item: str(item.get('model', '')), reverse=True)

    preserved_models = []
    remaining_models = []
    for item in normal_models:
        if _should_preserve_openai_chat_order(item):
            preserved_models.append(item)
        else:
            remaining_models.append(item)

    preserved_models.sort(key=lambda item: int(item.get('_source_index', 0)))
    remaining_models.sort(key=lambda item: str(item.get('model', '')), reverse=True)
    return default_models + preserved_models + remaining_models


def _default_model_template_by_service():
    templates = {}
    for service, models in _chat_models_by_service().items():
        if len(models) > 0:
            templates[service] = copy.deepcopy(models[0])
    return templates


def service_catalog():
    catalog = []
    grouped = _chat_models_by_service()
    services = []
    for service in SERVICE_CATALOG_ORDER:
        if service not in services:
            services.append(service)
    for service in grouped.keys():
        if service not in services:
            services.append(service)
    for service in services:
        service_models = grouped.get(service, [])
        models = []
        for config in service_models:
            models.append({
                'model': config['model'],
                'label': config['model'],
                'service': service,
                'handler_vendor': config.get('handler_vendor', ''),
                'default_fields': {},
                'default_extra_params': {},
                'extra_param_defs': _default_extra_param_defs()
            })
        catalog.append({
            'service': service,
            'label_key': _service_name_to_label_key(service),
            'models': models
        })
    return catalog


def api_type_configs():
    configs = [
        {
            'vendor': 'openai',
            'label_key': 'vendor_openai',
            'handler_vendor': 'openai',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': [],
            'services': OPENROUTER_SERVICES,
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': True
        },
        {
            'vendor': 'wanjie',
            'label_key': 'vendor_wanjie',
            'handler_vendor': 'wanjie',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': [],
            'services': ['xiaomi', 'kimi', 'chatgpt', 'claude'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'openrouter',
            'label_key': 'vendor_openrouter',
            'handler_vendor': 'openai',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': [],
            'services': OPENROUTER_SERVICES,
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': True
        },
        {
            'vendor': 'aws',
            'label_key': 'vendor_aws',
            'handler_vendor': 'aws',
            'fields': ['api_key', 'secret_key', 'api_endpoint'],
            'model_fields': [],
            'services': ['claude'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': True
        },
        {
            'vendor': 'volcengine',
            'label_key': 'vendor_volcengine',
            'handler_vendor': 'volcengine',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['doubao', 'deepseek', 'kimi'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'siliconflow',
            'label_key': 'vendor_siliconflow',
            'handler_vendor': 'siliconflow',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['deepseek'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'deepseek',
            'label_key': 'vendor_deepseek',
            'handler_vendor': 'deepseek',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['deepseek'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'minimax',
            'label_key': 'vendor_minimax',
            'handler_vendor': 'minimax',
            'fields': ['api_key', 'api_group_id'],
            'model_fields': [],
            'services': ['minimax'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'baidu',
            'label_key': 'vendor_baidu',
            'handler_vendor': 'baidu',
            'fields': ['api_key', 'secret_key'],
            'model_fields': [],
            'services': ['ernie'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'zhipuai',
            'label_key': 'vendor_zhipuai',
            'handler_vendor': 'zhipuai',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['zhipuai'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'azure',
            'label_key': 'vendor_azure',
            'handler_vendor': 'azure',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': ['api_type', 'deployment'],
            'services': ['chatgpt'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': True
        },
        {
            'vendor': 'claude',
            'label_key': 'vendor_claude',
            'handler_vendor': 'claude',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': [],
            'services': ['claude'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': True
        },
        {
            'vendor': 'aliyun',
            'label_key': 'vendor_aliyun',
            'handler_vendor': 'aliyun',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['qwen'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        },
        {
            'vendor': 'moonshot',
            'label_key': 'vendor_moonshot',
            'handler_vendor': 'moonshot',
            'fields': ['api_key'],
            'model_fields': [],
            'services': ['kimi'],
            'allow_custom_models': True,
            'extra_param_defs': _default_extra_param_defs(),
            'custom_model_extra_param_defs': _custom_model_extra_param_defs(),
            'endpoint_required': False
        }
    ]
    return [_finalize_api_type_config(config) for config in configs]


def get_api_type_config(vendor_type):
    for config in api_type_configs():
        if config['vendor'] == vendor_type:
            return copy.deepcopy(config)
    return None


def vendor_configs():
    configs = api_type_configs()
    return {
        'api_types': configs,
        'service_catalog': service_catalog(),
        'list': configs
    }

def backup_rules():
    return [
        {
            'vendor': 'azure',
            'backups':[
                {
                    'vendor': 'openai',
                    'transforms':{
                        'gpt-35-turbo': 'gpt-3.5-turbo',
                        'gpt-35-turbo-16k': 'gpt-3.5-turbo'
                    }
                }
            ]
        },
        {
            'vendor': 'aws',
            'backups':[
                {
                    'vendor': 'claude',
                    'transforms':{
                        'anthropic.claude-3-5-haiku-20241022-v1:0':'claude-3-5-haiku-20241022',
                        'anthropic.claude-3-5-sonnet-20241022-v2:0':'claude-3-5-sonnet-20241022',
                        'anthropic.claude-3-opus-20240229-v1:0':'claude-3-opus-20240229',
                        'anthropic.claude-3-5-sonnet-20240620-v1:0':'claude-3-5-sonnet-20241022',
                        'anthropic.claude-3-sonnet-20240229-v1:0':'claude-3-sonnet-20240229',
                        'anthropic.claude-3-haiku-20240307-v1:0':'claude-3-haiku-20240307',
                        'anthropic.claude-v2:1':'claude-2.1',
                        'anthropic.claude-v2':'claude-2.0',
                        'anthropic.claude-instant-v1':'claude-instant-1.2'
                    }
                }
            ]
        },
        {
            'vendor': 'deepseek',
            'backups':[
                {
                    'vendor': 'volcengine',
                    'transforms':{
                        'deepseek-chat': 'DeepSeek-V3',
                        'deepseek-reasoner': 'DeepSeek-R1'
                    }
                },
                {
                    'vendor': 'siliconflow',
                    'transforms':{
                        'deepseek-chat': 'DeepSeek-V3',
                        'deepseek-reasoner': 'DeepSeek-R1'
                    }
                }
            ]
        },
        {
            'vendor': 'siliconflow',
            'backups':[
                {
                    'vendor': 'volcengine',
                    'transforms':{
                    }
                },
                {
                    'vendor': 'deepseek',
                    'transforms':{
                        'DeepSeek-V3': 'deepseek-chat',
                        'DeepSeek-R1': 'deepseek-reasoner'
                    }
                }
            ]
        },
        {
            'vendor': 'volcengine',
            'backups':[
                {
                    'vendor': 'siliconflow',
                    'transforms':{
                    }
                },
                {
                    'vendor': 'deepseek',
                    'transforms':{
                        'DeepSeek-V3': 'deepseek-chat',
                        'DeepSeek-R1': 'deepseek-reasoner'
                    }
                }
            ]
        }
    ]

def chat_same_model_retry_rules():
    return [
        {
            'vendor': 'siliconflow',
            'type': 'code',
            'code': '50501',
            'sleep_time': 5
        },
        {
            'vendor': 'siliconflow',
            'type': 'status_code',
            'status_code': '504',
            'sleep_time': 5
        },
        {
            'vendor': 'openai',
            'type': 'status_code_min',
            'status_code_min': 500,
            'sleep_time': 5
        }
    ]

def embedding_backup_rules():
    return [
        {
            'vendor': 'azure',
            'backups':[
                {
                    'vendor': 'openai',
                    'transforms':{
                    }
                }
            ]
        },
    ]

def get_module(app_id, vendor):
    if vendor in vendor_to_module:
        return vendor_to_module.get(vendor)
    custom_vendor_info = get_vendor(app_id, vendor)
    if custom_vendor_info:
        handler_vendor = get_vendor_handler_vendor(custom_vendor_info)
        if handler_vendor in vendor_to_module:
            return vendor_to_module.get(handler_vendor)
    raise Exception('vendor_not_exist')


def is_v2_custom_vendor(vendor_info):
    try:
        return int(vendor_info.get('config_version', 1)) >= 2
    except Exception:
        return False


def get_vendor_handler_vendor(vendor_info):
    api_type_config = get_api_type_config(vendor_info.get('vendor_type', ''))
    if api_type_config:
        handler_vendor = api_type_config.get('handler_vendor', vendor_info.get('vendor_type', ''))
        if handler_vendor in vendor_to_module:
            return handler_vendor
    handler_vendor = vendor_info.get('handler_vendor', '')
    if handler_vendor in vendor_to_module:
        return handler_vendor
    return vendor_info.get('vendor_type', '')


def normalize_vendor_setting_handler_vendor(vendor_setting):
    api_type_config = get_api_type_config(vendor_setting.vendor_type)
    if api_type_config:
        vendor_setting.handler_vendor = api_type_config.get('handler_vendor', '')
    return vendor_setting


def is_valid_vendor_api_endpoint(api_endpoint):
    endpoint = str(api_endpoint or '').strip()
    if endpoint == '':
        return True
    return lanying_utils.is_valid_public_url(endpoint)


def _is_openai_compatible_vendor_setting(vendor_setting):
    vendor_info = _vendor_setting_to_vendor_info(vendor_setting)
    return get_vendor_handler_vendor(vendor_info) == 'openai'


def _replace_api_endpoint_path(api_endpoint, new_path):
    parsed = urlparse(api_endpoint)
    return urlunparse(parsed._replace(path=new_path, params='', query='', fragment=''))


def _normalize_openai_api_endpoint_candidates(api_endpoint):
    endpoint = str(api_endpoint or '').strip().rstrip('/')
    if endpoint == '':
        return []
    candidates = []
    seen = set()

    def add_candidate(candidate):
        candidate = str(candidate or '').strip().rstrip('/')
        if candidate == '' or candidate in seen:
            return
        if not is_valid_vendor_api_endpoint(candidate):
            return
        seen.add(candidate)
        candidates.append(candidate)

    add_candidate(endpoint)
    parsed = urlparse(endpoint)
    path = parsed.path.rstrip('/')
    stripped_candidates = []
    if path.endswith('/v1/chat/completions'):
        stripped_candidates.append(_replace_api_endpoint_path(endpoint, path[:-len('/chat/completions')]))
    elif path.endswith('/chat/completions'):
        stripped_candidates.append(_replace_api_endpoint_path(endpoint, path[:-len('/chat/completions')]))
    for candidate in stripped_candidates:
        add_candidate(candidate)
    current_candidates = list(candidates)
    for candidate in current_candidates:
        parsed_candidate = urlparse(candidate)
        candidate_path = parsed_candidate.path.rstrip('/')
        if candidate_path == '':
            candidate_path = '/'
        if not candidate_path.endswith('/v1'):
            candidate_with_v1 = _replace_api_endpoint_path(candidate, candidate_path.rstrip('/') + '/v1')
            add_candidate(candidate_with_v1)
    return candidates


def _vendor_setting_to_vendor_info(vendor_setting):
    return {
        'app_id': vendor_setting.app_id,
        'tenement_id': vendor_setting.tenement_id,
        'vendor_type': vendor_setting.vendor_type,
        'name': vendor_setting.name,
        'api_key': vendor_setting.api_key,
        'secret_key': vendor_setting.secret_key,
        'api_group_id': vendor_setting.api_group_id,
        'api_endpoint': vendor_setting.api_endpoint,
        'api_endpoint_server_location': vendor_setting.api_endpoint_server_location,
        'config_version': vendor_setting.config_version,
        'handler_vendor': vendor_setting.handler_vendor,
        'model_config': copy.deepcopy(vendor_setting.model_config),
    }


def _first_handler_chat_model(handler_vendor):
    module = vendor_to_module.get(handler_vendor)
    if module is None:
        return None
    candidates = []
    for config in module.model_configs():
        if config.get('type') == 'chat':
            candidates.append(copy.deepcopy(config))
    if len(candidates) == 0:
        return None
    candidates.sort(key=lambda item: (
        float(item.get('quota', 999999)),
        int(item.get('order', 999999)),
        str(item.get('model', ''))
    ))
    return candidates[0]


def _get_vendor_validation_chat_model_config(vendor_setting):
    vendor_info = _vendor_setting_to_vendor_info(vendor_setting)
    handler_vendor = get_vendor_handler_vendor(vendor_info)
    if handler_vendor not in vendor_to_module:
        return None
    if is_v2_custom_vendor(vendor_info):
        entries = _build_v2_vendor_model_entries(vendor_info)
        enabled_entries = [entry for entry in entries if entry.get('enabled') is True]
        preferred_entries = [entry for entry in enabled_entries if entry.get('source') == 'catalog']
        for entry in preferred_entries + enabled_entries:
            model_config = _get_v2_chat_model_config(vendor_info, '__vendor_validation__', entry.get('model', ''))
            if model_config is not None:
                return model_config
        return _first_handler_chat_model(handler_vendor)
    module = vendor_to_module.get(handler_vendor)
    for config in module.model_configs():
        if config.get('type') != 'chat':
            continue
        new_config = copy.deepcopy(config)
        if not model_config_valid(new_config, vendor_info):
            continue
        maybe_update_custom_vendor_model_config(new_config, vendor_info, new_config.get('model', ''))
        return new_config
    return _first_handler_chat_model(handler_vendor)


def _test_vendor_connection_once(vendor_setting, handler_vendor, model_config):
    model_name = model_config.get('model', '')
    api_endpoint = str(vendor_setting.api_endpoint or '').strip()
    auth_info = {
        'app_id': vendor_setting.app_id,
        'vendor_type': vendor_setting.vendor_type,
        'api_key': vendor_setting.api_key,
        'secret_key': vendor_setting.secret_key,
        'api_group_id': vendor_setting.api_group_id,
        'api_endpoint': api_endpoint,
        'api_endpoint_server_location': vendor_setting.api_endpoint_server_location,
        'key_type': 'self',
        'validation_mode': True,
        'validation_timeout_seconds': 8
    }
    preset = {
        'model': model_name,
        'messages': [
            {
                'role': 'user',
                'content': 'hello'
            }
        ],
        'stream': False,
        'temperature': 0,
        'max_tokens': 1,
    }
    logging.info(
        f"vendor connection test start | app_id:{vendor_setting.app_id}, vendor_type:{vendor_setting.vendor_type}, "
        f"handler_vendor:{handler_vendor}, model:{preset['model']}, validation_mode:{auth_info.get('validation_mode', False)}, "
        f"timeout:{auth_info.get('validation_timeout_seconds', 0)}, api_endpoint:{api_endpoint}"
    )
    try:
        module = vendor_to_module.get(handler_vendor)
        prepare_info = prepare_chat(vendor_setting.app_id, handler_vendor, auth_info, copy.deepcopy(preset))
        use_native_tools = getattr(module, 'SUPPORT_NATIVE_TOOLS', False)
        if use_native_tools:
            vendor_preset = copy.deepcopy(preset)
        else:
            vendor_preset = lanying_openai_compat.to_legacy_vendor_preset(copy.deepcopy(preset))
        response = chat_with_same_model_retry(module, handler_vendor, prepare_info, vendor_preset, model_config)
        response = normalize_chat_response(response)
        if isinstance(response, dict) and response.get('result') == 'ok':
            logging.info(f"vendor connection test success | app_id:{vendor_setting.app_id}, vendor_type:{vendor_setting.vendor_type}, model:{preset['model']}")
            return {
                'result': 'ok'
            }
        logging.info(f"vendor connection test failed | app_id:{vendor_setting.app_id}, vendor_type:{vendor_setting.vendor_type}, model:{preset['model']}, response:{response}")
    except Exception as e:
        logging.info(f"vendor connection test exception | app_id:{vendor_setting.app_id}, vendor_type:{vendor_setting.vendor_type}, model:{preset['model']}")
        logging.exception(e)
    return {
        'result': 'error',
        'message': 'vendor_connection_test_failed'
    }


def test_vendor_connection(vendor_setting):
    vendor_info = _vendor_setting_to_vendor_info(vendor_setting)
    handler_vendor = get_vendor_handler_vendor(vendor_info)
    if handler_vendor not in vendor_to_module:
        return {
            'result': 'error',
            'message': 'handler_vendor_not_valid'
        }
    model_config = _get_vendor_validation_chat_model_config(vendor_setting)
    if model_config is None:
        logging.info(f"skip vendor connection test | app_id:{vendor_setting.app_id}, vendor_type:{vendor_setting.vendor_type}, reason:no_chat_model")
        return {
            'result': 'ok'
        }
    original_api_endpoint = str(vendor_setting.api_endpoint or '').strip()
    if _is_openai_compatible_vendor_setting(vendor_setting) and original_api_endpoint != '':
        candidates = _normalize_openai_api_endpoint_candidates(original_api_endpoint)
        if len(candidates) > 0:
            logging.info(
                f"vendor connection endpoint normalize candidates | app_id:{vendor_setting.app_id}, "
                f"vendor_type:{vendor_setting.vendor_type}, api_endpoint:{original_api_endpoint}, candidates:{candidates}"
            )
            last_result = None
            for candidate in candidates:
                vendor_setting.api_endpoint = candidate
                last_result = _test_vendor_connection_once(vendor_setting, handler_vendor, model_config)
                if last_result.get('result') == 'ok':
                    if candidate != original_api_endpoint:
                        logging.info(
                            f"vendor connection endpoint normalized | app_id:{vendor_setting.app_id}, "
                            f"vendor_type:{vendor_setting.vendor_type}, from:{original_api_endpoint}, to:{candidate}"
                        )
                    return last_result
            vendor_setting.api_endpoint = original_api_endpoint
            return last_result or {
                'result': 'error',
                'message': 'vendor_connection_test_failed'
            }
    return _test_vendor_connection_once(vendor_setting, handler_vendor, model_config)


def fetch_vendor_remote_models(vendor_setting):
    vendor_setting = normalize_vendor_setting_handler_vendor(vendor_setting)
    vendor_info = _vendor_setting_to_vendor_info(vendor_setting)
    handler_vendor = get_vendor_handler_vendor(vendor_info)
    if handler_vendor not in vendor_to_module:
        return {
            'result': 'error',
            'message': 'handler_vendor_not_valid'
        }
    if not is_valid_vendor_api_endpoint(vendor_setting.api_endpoint):
        return {
            'result': 'error',
            'message': 'api_endpoint_not_valid'
        }
    module = vendor_to_module.get(handler_vendor)
    if module is None or not hasattr(module, 'list_remote_models'):
        return {
            'result': 'error',
            'message': 'vendor_remote_model_list_not_supported'
        }
    auth_info = {
        'app_id': vendor_setting.app_id,
        'vendor_type': vendor_setting.vendor_type,
        'api_key': vendor_setting.api_key,
        'secret_key': vendor_setting.secret_key,
        'api_group_id': vendor_setting.api_group_id,
        'api_endpoint': str(vendor_setting.api_endpoint or '').strip(),
        'api_endpoint_server_location': vendor_setting.api_endpoint_server_location,
        'key_type': 'self',
        'validation_mode': True,
        'validation_timeout_seconds': 8
    }
    try:
        result = module.list_remote_models(auth_info)
        if isinstance(result, dict):
            return result
    except Exception as e:
        logging.info(
            f"fetch vendor remote models exception | app_id:{vendor_setting.app_id}, "
            f"vendor_type:{vendor_setting.vendor_type}, handler_vendor:{handler_vendor}"
        )
        logging.exception(e)
    return {
        'result': 'error',
        'message': 'fetch_vendor_remote_models_failed'
    }


def _sanitize_model_config(new_config):
    for field in ['url', 'endpoint', 'input_price', 'output_price', 'currency']:
        if field in new_config:
            del new_config[field]
    return new_config


def _apply_custom_vendor_common_fields(new_config, vendor, vendor_show_name, is_custom_vendor):
    new_config['vendor'] = vendor
    if vendor_show_name:
        new_config['vendor_show_name'] = vendor_show_name
    new_config['is_origin_vendor'] = False
    new_config['is_custom_vendor'] = is_custom_vendor
    new_config['api_key_type'] = 'self'
    new_config['quota'] = get_custom_vendor_quota()
    new_config['quota_without_content_security'] = 0
    if 'image_quota' in new_config:
        new_config['image_quota_without_content_security'] = {}
        for k, _ in new_config['image_quota'].items():
            new_config['image_quota'][k] = get_custom_vendor_quota()
            new_config['image_quota_without_content_security'][k] = 0
    return new_config


def _catalog_models_by_name():
    catalog = {}
    for service_info in service_catalog():
        service = service_info['service']
        for model_info in service_info.get('models', []):
            catalog[(service, model_info['model'])] = copy.deepcopy(model_info)
    return catalog


def _normalize_vendor_model_entry(entry, model_fields):
    dto = {
        'service': str(entry.get('service', '')).strip(),
        'model': str(entry.get('model', '')).strip(),
        'enabled': bool(entry.get('enabled', False)),
        'source': str(entry.get('source', 'catalog')).strip() or 'catalog',
        'extra_params': []
    }
    extra_params = entry.get('extra_params', [])
    if isinstance(extra_params, list):
        for item in extra_params:
            key = str(item.get('key', '')).strip()
            if key == '':
                continue
            dto['extra_params'].append({
                'key': key,
                'value': item.get('value', '')
            })
    for field in model_fields:
        value = entry.get(field, '')
        if isinstance(value, str):
            value = value.strip()
        if field == 'api_type' and value == '':
            value = 'azure'
        dto[field] = value
    return dto


def _build_v2_vendor_model_entries(vendor_info):
    api_type_config = get_api_type_config(vendor_info.get('vendor_type', ''))
    if api_type_config is None:
        return []
    model_fields = api_type_config.get('model_fields', [])
    catalog_by_name = _catalog_models_by_name()
    entries = []
    for raw_entry in vendor_info.get('model_config', []):
        entry = _normalize_vendor_model_entry(raw_entry, model_fields)
        if entry['service'] == '' or entry['model'] == '':
            continue
        if entry['source'] == 'catalog' and (entry['service'], entry['model']) not in catalog_by_name:
            continue
        entries.append(entry)
    return entries


def _service_default_template(service):
    return copy.deepcopy(_default_model_template_by_service().get(service))


def _find_catalog_model_template(service, model):
    grouped = _chat_models_by_service()
    for config in grouped.get(service, []):
        if config.get('model') == model:
            return copy.deepcopy(config)
    return None


def _find_handler_model_template(vendor_info, service, model):
    handler_vendor = get_vendor_handler_vendor(vendor_info)
    module = vendor_to_module.get(handler_vendor)
    if module is None:
        return None
    for config in module.model_configs():
        if config.get('type') != 'chat':
            continue
        if config.get('model') != model:
            continue
        config_service = str(config.get('service', '')).strip()
        if service not in ['', config_service]:
            continue
        return copy.deepcopy(config)
    return None


def _extra_param_definitions_for_entry(vendor_info, model_entry):
    defs = []
    api_type_config = get_api_type_config(vendor_info.get('vendor_type', ''))
    if api_type_config:
        defs.extend(api_type_config.get('extra_param_defs', []))
        if model_entry.get('source') == 'custom':
            defs.extend(api_type_config.get('custom_model_extra_param_defs', []))
    catalog_model = _catalog_models_by_name().get((model_entry.get('service', ''), model_entry.get('model', '')))
    if catalog_model:
        defs.extend(catalog_model.get('extra_param_defs', []))
    mapping = {}
    for item in defs:
        key = str(item.get('key', '')).strip()
        if key == '':
            continue
        mapping[key] = item
    return mapping


def _cast_extra_param_value(value, input_type):
    if input_type == 'boolean':
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ['true', '1', 'yes', 'on']
    if input_type == 'checkbox':
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text == '':
                return []
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
            return [x.strip() for x in text.split(',') if x.strip() != '']
        return []
    if input_type == 'number':
        try:
            text = str(value).strip()
            if text == '':
                return value
            if '.' in text:
                return float(text)
            return int(text)
        except Exception:
            return value
    if input_type == 'json':
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return value
    return value

def _apply_model_entry_fields(config, model_entry, vendor_info):
    for field in ['deployment', 'api_type', 'endpoint']:
        if field in model_entry and model_entry[field] not in [None, '']:
            if field == 'deployment':
                url = config.get('url')
                api_endpoint = vendor_info.get('api_endpoint', '')
                if isinstance(url, str) and api_endpoint != '':
                    api_endpoint = api_endpoint.strip('/') + '/'
                    url = url.replace('https://xiaolanai-eastus.openai.azure.com/', api_endpoint)
                    url = re.sub(r"(/deployments/).*/", r"\1" + str(model_entry['deployment']) + "/", url)
                    config['url'] = url
            else:
                config[field] = model_entry[field]
    extra_param_defs = _extra_param_definitions_for_entry(vendor_info, model_entry)
    for item in model_entry.get('extra_params', []):
        key = item['key']
        input_type = extra_param_defs.get(key, {}).get('input_type', 'text')
        config[key] = _cast_extra_param_value(item.get('value', ''), input_type)
    return config


def _get_v2_chat_model_config(vendor_info, vendor_id, model):
    vendor_show_name = vendor_info.get('name', '')
    entries = _build_v2_vendor_model_entries(vendor_info)
    for entry in entries:
        if entry.get('model') != model or entry.get('enabled') is not True:
            continue
        template = _find_catalog_model_template(entry['service'], entry['model'])
        if template is None:
            template = _find_handler_model_template(vendor_info, entry['service'], entry['model'])
        if template is None and entry.get('source') == 'custom':
            template = _service_default_template(entry['service'])
            if template:
                template['model'] = entry['model']
        if template is None:
            continue
        new_config = _sanitize_model_config(copy.deepcopy(template))
        _apply_custom_vendor_common_fields(new_config, vendor_id, vendor_show_name, True)
        _apply_model_entry_fields(new_config, entry, vendor_info)
        return new_config
    return None


def _append_v2_chat_models(models, vendor_info, vendor_id):
    vendor_show_name = vendor_info.get('name', '')
    for entry in _build_v2_vendor_model_entries(vendor_info):
        template = _find_catalog_model_template(entry['service'], entry['model'])
        if template is None:
            template = _find_handler_model_template(vendor_info, entry['service'], entry['model'])
        if template is None and entry.get('source') == 'custom':
            template = _service_default_template(entry['service'])
            if template:
                template['model'] = entry['model']
        if template is None:
            continue
        new_config = _sanitize_model_config(copy.deepcopy(template))
        _apply_custom_vendor_common_fields(new_config, vendor_id, vendor_show_name, True)
        _apply_model_entry_fields(new_config, entry, vendor_info)
        new_config['enabled'] = entry.get('enabled') is True
        if new_config['enabled']:
            models.append(new_config)


def _get_legacy_handler_models(handler_vendor):
    module = vendor_to_module.get(handler_vendor)
    if module is None:
        return []
    configs = module.model_configs()
    if handler_vendor == 'openai':
        return copy.deepcopy(configs)
    return _sort_model_configs_for_display(configs)

def list_models(app_id):
    models = []
    for vendor,module in vendor_to_module.items():
        module_configs = module.model_configs()
        if vendor == 'openai':
            iter_configs = module_configs
        else:
            iter_configs = _sort_model_configs_for_display(module_configs)
        for config in iter_configs:
            new_config = copy.deepcopy(config)
            _sanitize_model_config(new_config)
            new_config['vendor'] = vendor
            new_config['is_custom_vendor'] = False
            new_config['api_key_type'] = 'share'
            if new_config['type'] == 'chat':
                new_config['quota_without_content_security'] = get_quota_when_content_security(new_config['quota'])
            else:
                new_config['quota_without_content_security'] = new_config['quota']
            models.append(new_config)
    custom_vendor_list = get_vendor_list(app_id)['data']['list']
    for vendor_info in custom_vendor_list:
        vendor_id = vendor_info['vendor_id']
        if is_v2_custom_vendor(vendor_info):
            _append_v2_chat_models(models, vendor_info, vendor_id)
            handler_vendor = get_vendor_handler_vendor(vendor_info)
            for config in _get_legacy_handler_models(handler_vendor):
                if config.get('type') == 'chat':
                    continue
                new_config = copy.deepcopy(config)
                _sanitize_model_config(new_config)
                _apply_custom_vendor_common_fields(new_config, vendor_id, vendor_info.get('name', ''), True)
                models.append(new_config)
            continue
        vendor_type = vendor_info['vendor_type']
        vender_show_name = vendor_info['name']
        if vendor_type in vendor_to_module:
            module = vendor_to_module[vendor_type]
            for config in module.model_configs():
                new_config = copy.deepcopy(config)
                if not model_config_valid(new_config, vendor_info):
                    continue
                _sanitize_model_config(new_config)
                _apply_custom_vendor_common_fields(new_config, vendor_id, vender_show_name, True)
                models.append(new_config)
    return models

def get_quota_when_content_security(quota):
    if quota > 0.01:
        return round(100 * quota * 0.778) / 100
    else:
        return round(10000 * quota * 0.778) / 10000

def get_custom_vendor_quota():
    return 0.22

def get_chat_model_config(app_id, vendor, model):
    return get_model_config(app_id, vendor, model, 'chat')

def get_vendor_by_model(model):
    for vendor,module in vendor_to_module.items():
       for config in module.model_configs():
           now_model = config.get('model')
           if model == now_model:
               return vendor
    return None

def get_model_config(app_id, vendor, model, type):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    if vendor in vendor_to_module:
        module = vendor_to_module.get(vendor)
        if module:
            model_configs = module.model_configs()
            for config in model_configs:
                if config['type'] == type:
                    now_model = config.get('model')
                    if model == now_model:
                        new_config = copy.deepcopy(config)
                        new_config['vendor'] = vendor
                        new_config['is_custom_vendor'] = False
                        new_config['api_key_type'] = 'share'
                        if new_config['type'] == 'chat':
                            new_config['quota_without_content_security'] = get_quota_when_content_security(new_config['quota'])
                        else:
                            new_config['quota_without_content_security'] = new_config['quota']
                        return new_config
    custom_vendor_info = get_vendor(app_id, vendor)
    if custom_vendor_info:
        if is_v2_custom_vendor(custom_vendor_info):
            if type == 'chat':
                return _get_v2_chat_model_config(custom_vendor_info, vendor, model)
            handler_vendor = get_vendor_handler_vendor(custom_vendor_info)
            module = vendor_to_module.get(handler_vendor)
            if module:
                model_configs = module.model_configs()
                for config in model_configs:
                    if config['type'] == type and model == config.get('model'):
                        new_config = copy.deepcopy(config)
                        _sanitize_model_config(new_config)
                        _apply_custom_vendor_common_fields(new_config, vendor, custom_vendor_info.get('name', ''), True)
                        return new_config
        vendor_type = custom_vendor_info['vendor_type']
        vender_show_name = custom_vendor_info['name']
        if vendor_type in vendor_to_module:
            module = vendor_to_module.get(vendor_type)
            if module:
                model_configs = module.model_configs()
                for config in model_configs:
                    if config['type'] == type:
                        now_model = config.get('model')
                        if model == now_model:
                            new_config = copy.deepcopy(config)
                            if not model_config_valid(new_config, custom_vendor_info):
                                continue
                            _sanitize_model_config(new_config)
                            _apply_custom_vendor_common_fields(new_config, vendor, vender_show_name, True)
                            maybe_update_custom_vendor_model_config(new_config, custom_vendor_info, model)
                            return new_config
    return None

def maybe_update_custom_vendor_model_config(config, custom_vendor_info, model):
    vendor_model_config = custom_vendor_info['model_config']
    for vmc in vendor_model_config:
        if vmc['model'] == model:
            fields = ['deployment', 'api_type', 'endpoint']
            for field in fields:
                if field in vmc:
                    if field == 'deployment':
                        url = config['url']
                        api_endpoint = custom_vendor_info['api_endpoint']
                        api_endpoint = api_endpoint.strip('/') + '/'
                        url = url.replace('https://xiaolanai-eastus.openai.azure.com/', api_endpoint)
                        url = re.sub(r"(/deployments/).*/",r"\1"+ model +"/",url)
                        logging.info(f"maybe_update_custom_vendor_model_config | new_url:{url}")
                        config['url'] = url
                    else:
                        config[field] = vmc[field]

def model_config_valid(new_config, vendor_info):
    if is_v2_custom_vendor(vendor_info):
        return True
    model_config = vendor_info.get('model_config', [])
    if model_config == []:
        return True
    model = new_config['model']
    for now_model_config in model_config:
        if now_model_config['model'] == model:
            fields = ['deployment', 'endpoint']
            hasConfig = False
            for field in fields:
                if field in now_model_config and now_model_config[field].strip() != '':
                    hasConfig = True
            if hasConfig:
                return True
    return False

def get_image_model_config(app_id, vendor, model):
    return get_model_config(app_id, vendor, model, 'image')

def get_text_to_speech_model_config(app_id, vendor, model):
    return get_model_config(app_id, vendor, model, 'text_to_speech')

def get_speech_to_text_model_config(app_id, vendor, model):
    return get_model_config(app_id, vendor, model, 'speech_to_text')

def get_embedding_model(app_id, vendor):
    module = get_module(app_id, vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "embedding":
                return config.get('model')
    return None

def get_embedding_model_config(app_id, vendor, model):
    if model == '':
        model = get_embedding_model(app_id, vendor)
    return get_model_config(app_id, vendor, model, 'embedding')

def prepare_chat(app_id, vendor, auth_info, preset):
    module = get_module(app_id, vendor)
    use_native_tools = getattr(module, 'SUPPORT_NATIVE_TOOLS', False)
    if use_native_tools:
        vendor_preset = preset
    else:
        vendor_preset = lanying_openai_compat.to_legacy_vendor_preset(preset)
    result = module.prepare_chat(auth_info, vendor_preset)
    if isinstance(result, dict):
        result['auth_info'] = auth_info
    return result

def chat(app_id, vendor, prepare_info, preset):
    module = get_module(app_id, vendor)
    model_config = get_chat_model_config(app_id, vendor, preset['model'])
    use_native_tools = getattr(module, 'SUPPORT_NATIVE_TOOLS', False)
    if use_native_tools:
        vendor_preset = preset
    else:
        vendor_preset = lanying_openai_compat.to_legacy_vendor_preset(preset)
    try:
        resp = chat_with_same_model_retry(module, vendor, prepare_info, vendor_preset, model_config)
        resp = normalize_chat_response(resp)
        if 'result' in resp and resp['result'] == 'ok':
            return resp
        return chat_retry(vendor, prepare_info, vendor_preset, resp)
    except Exception as e:
        logging.error(e)
        error_message = 'exception'
        try:
            error_message = str(e)
        except Exception as ee:
            pass
        resp = {
            'result': 'error',
            'reason': error_message
        }
        return chat_retry(vendor, prepare_info, vendor_preset, resp)

def normalize_chat_response(resp):
    resp = lanying_openai_compat.normalize_vendor_response(resp)
    if isinstance(resp, dict) and 'reply_generator' in resp:
        old_generator = resp.get('reply_generator')
        if old_generator is not None:
            def wrapped_generator():
                for delta in old_generator:
                    yield lanying_openai_compat.normalize_stream_delta(delta)
            resp['reply_generator'] = wrapped_generator()
    return resp

def chat_with_same_model_retry(module, vendor, prepare_info, preset, model_config):
    auth_info = prepare_info.get('auth_info', {}) if isinstance(prepare_info, dict) else {}
    try_times = 1 if auth_info.get('validation_mode') is True else 3
    for i in range(try_times):
        try:
            resp = module.chat(prepare_info, preset, model_config)
            if 'result' in resp and resp['result'] == 'ok':
                return resp
        except Exception as e:
            logging.error(e)
            error_message = 'exception'
            try:
                error_message = str(e)
            except Exception as ee:
                pass
            resp = {
                'result': 'error',
                'reason': error_message
            }
        rules = chat_same_model_retry_rules()
        need_retry = False
        sleep_time = 3
        for rule in rules:
            if vendor == rule['vendor']:
                type = rule.get('type')
                if type == 'code':
                    if str(resp.get('code', '')) == rule['code']:
                        need_retry = True
                        sleep_time = rule.get('sleep_time', sleep_time)
                        break
                elif type =='status_code':
                    if str(resp.get('status_code', '')) == rule['status_code']:
                        need_retry = True
                        sleep_time = rule.get('sleep_time', sleep_time)
                        break
                elif type == 'status_code_min':
                    try:
                        status_code = resp.get('status_code', 0)
                        if status_code >= rule['status_code_min']:
                            need_retry = True
                            sleep_time = rule.get('sleep_time', sleep_time)
                            break
                    except Exception as e:
                        logging.exception(e)
        if need_retry:
            if i >= try_times - 1:
                logging.info(f"chat_with_same_model_retry no retry times| vendor:{vendor}, resp:{resp}, sleep_time:{sleep_time}, progress: {i}/{try_times}")
                return resp
            else:
                logging.info(f"chat_with_same_model_retry schedule retry | vendor:{vendor}, resp:{resp}, sleep_time:{sleep_time}, progress: {i}/{try_times}")
                time.sleep(sleep_time)
        else:
            logging.info(f"chat_with_same_model_retry no need retry | vendor:{vendor}, resp:{resp}, sleep_time:{sleep_time}, progress: {i}/{try_times}")
            return resp

def chat_retry(vendor, prepare_info, preset, resp):
    unique_id = datetime.now().strftime('%Y-%m-%d-%H-%M-%S.%f')
    model = preset['model']
    async_send_message_with_filter(f'【蓝莺Connector】AI Chat 返回异常, id:{unique_id}, vendor:{vendor}, model:{model}, resp:{resp}', f'ai_chat_resp_failed_{vendor}')
    try:
        new_resp = do_chat_retry(vendor, prepare_info, preset, resp, unique_id)
        new_resp = normalize_chat_response(new_resp)
        if 'result' in new_resp and new_resp['result'] == 'ok':
            return new_resp
        return resp
    except Exception as e:
        logging.error(e)
        return resp

def do_chat_retry(vendor, prepare_info, preset, resp, unique_id):
    if 'auth_info' not in prepare_info:
        logging.info("do_chat_retry | auth_info not exist")
        return resp
    auth_info = prepare_info['auth_info']
    if 'key_type' not in auth_info:
        logging.info("do_chat_retry | key_type not exist")
        return resp
    key_type = auth_info['key_type']
    if key_type != 'share':
        logging.info("do_chat_retry | key_type not share")
        return resp
    app_id = auth_info['app_id']
    model = preset['model']
    for rule in backup_rules():
        if rule['vendor'] == vendor:
            backups = rule.get('backups',[])
            for backup in backups:
                new_vendor = backup['vendor']
                transforms = backup.get('transforms', {})
                new_model = model
                if new_model in transforms:
                    new_model = transforms[new_model]
                try:
                    new_model_config = get_chat_model_config(app_id, new_vendor, new_model)
                    if new_model_config:
                        new_preset = copy.deepcopy(preset)
                        new_preset['model'] = new_model
                        logging.info(f"chat backup start | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                        new_auth_info = lanying_config.get_lanying_connector_share_auth_info(new_vendor)
                        new_prepare_info = prepare_chat(app_id, new_vendor, new_auth_info, new_preset)
                        new_module = get_module(app_id, new_vendor)
                        new_resp = chat_with_same_model_retry(new_module, new_vendor, new_prepare_info, new_preset, new_model_config)
                        if 'result' in new_resp and new_resp['result'] == 'ok':
                            logging.info(f"chat backup success | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                            async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回成功, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                            return new_resp
                        else:
                            async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                except Exception as e:
                    async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                    logging.error(e)
            logging.info(f"chat backup failed | app_id:{app_id}, vendor:{vendor}, model:{model}")
    return resp

def prepare_embedding(app_id, vendor, auth_info, type):
    module = get_module(app_id, vendor)
    result = module.prepare_embedding(auth_info, type)
    if isinstance(result, dict):
        result['auth_info'] = auth_info
        result['type'] = type
    return result

def embedding(app_id, vendor, prepare_info, model, text):
    module = get_module(app_id, vendor)
    model_config = get_embedding_model_config(app_id, vendor, model)
    retry_times = 5
    for i in range(retry_times):
        try:
            resp = module.embedding(prepare_info, model, text, model_config)
            if 'result' in resp and resp['result'] == 'ok':
                return resp
            if i == retry_times - 1:
                logging.info(f"embedding finally failed: {i}/{retry_times}, resp:{resp}")
                return embedding_retry(app_id, vendor, prepare_info, model, text, resp)
            else:
                logging.info(f"embedding schedule retry: {i}/{retry_times}, resp:{resp}")
                time.sleep(0.5)
        except Exception as e:
            logging.error(e)
            error_message = 'exception'
            try:
                error_message = str(e)
            except Exception as ee:
                pass
            resp = {
                'result': 'error',
                'reason': error_message
            }
            if i == retry_times - 1:
                logging.info(f"embedding finally failed: {i}/{retry_times}, resp:{resp}")
                return embedding_retry(app_id, vendor, prepare_info, model, text, resp)
            else:
                logging.info(f"embedding schedule retry: {i}/{retry_times}, resp:{resp}")
                time.sleep(0.5)

def embedding_retry(app_id, vendor, prepare_info, model, text, resp):
    unique_id = datetime.now().strftime('%Y-%m-%d-%H-%M-%S.%f')
    async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 返回异常, id:{unique_id}, app_id:{app_id}, vendor:{vendor}, model:{model}, resp:{resp}', f'ai_embedding_resp_failed_{vendor}')
    try:
        new_resp = do_embedding_retry(app_id, vendor, prepare_info, model, text, resp, unique_id)
        if 'result' in new_resp and new_resp['result'] == 'ok':
            return new_resp
        return resp
    except Exception as e:
        logging.error(e)
        return resp

def do_embedding_retry(app_id, vendor, prepare_info, model, text, resp, unique_id):
    if 'auth_info' not in prepare_info:
        logging.info("do_embedding_retry | auth_info not exist")
        return resp
    auth_info = prepare_info['auth_info']
    if 'type' not in prepare_info:
        logging.info("do_embedding_retry | type not exist")
        return resp
    type = prepare_info['type']
    if 'key_type' not in auth_info:
        logging.info("do_embedding_retry | key_type not exist")
        return resp
    key_type = auth_info['key_type']
    if key_type != 'share':
        logging.info("do_embedding_retry | key_type not share")
        return resp
    app_id = auth_info['app_id']
    for rule in embedding_backup_rules():
        if rule['vendor'] == vendor:
            backups = rule.get('backups',[])
            for backup in backups:
                new_vendor = backup['vendor']
                transforms = backup.get('transforms', {})
                new_model = model
                if new_model in transforms:
                    new_model = transforms[new_model]
                try:
                    new_model_config = get_embedding_model_config(app_id, new_vendor, new_model)
                    if new_model_config:
                        logging.info(f"embedding backup start | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                        new_auth_info = lanying_config.get_lanying_connector_share_auth_info(new_vendor)
                        new_prepare_info = prepare_embedding(app_id, new_vendor, new_auth_info, type)
                        new_module = get_module(app_id, new_vendor)
                        new_resp = new_module.embedding(new_prepare_info, new_model, text, new_model_config)
                        if 'result' in new_resp and new_resp['result'] == 'ok':
                            logging.info(f"embedding backup success | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                            async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回成功, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                            return new_resp
                        else:
                            async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                except Exception as e:
                    async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                    logging.error(e)
            logging.info(f"embedding backup failed | app_id:{app_id}, vendor:{vendor}, model:{model}")
    return resp

def encoding_for_model(app_id, vendor, model):
    module = get_module(app_id, vendor)
    return module.encoding_for_model(model)

def async_send_message_with_filter(text, filter_name):
    if lanying_utils.is_preview_server():
        logging.info(f"async_send_message_with_filter skip for preview server | text: {text}, filter_name: {filter_name}")
    else:
        lanying_slack.async_send_message_with_filter(text, filter_name)

class VendorSetting:
    def __init__(self, app_id, tenement_id, vendor_type, name, api_key, secret_key, api_group_id, api_endpoint, model_config, config_version=1, handler_vendor='', api_endpoint_server_location='overseas'):
        self.app_id = app_id
        self.tenement_id = tenement_id
        self.vendor_type = vendor_type
        self.name = name
        self.api_key = api_key
        self.secret_key = secret_key
        self.api_group_id = api_group_id
        self.api_endpoint = api_endpoint
        self.api_endpoint_server_location = api_endpoint_server_location
        self.model_config = model_config
        self.config_version = config_version
        self.handler_vendor = handler_vendor
    def to_hmset_fields(self):
        return {
            'app_id': self.app_id,
            'tenement_id': self.tenement_id,
            'vendor_type': self.vendor_type,
            'name': self.name,
            'api_key': self.api_key,
            'secret_key': self.secret_key,
            'api_group_id': self.api_group_id,
            'api_endpoint':  self.api_endpoint,
            'api_endpoint_server_location': self.api_endpoint_server_location,
            'config_version': self.config_version,
            'handler_vendor': self.handler_vendor,
            'model_config': json.dumps(self.model_config, ensure_ascii=False)
        }

def create_vendor(vendor_setting: VendorSetting):
    now = int(time.time())
    vendor_setting = normalize_vendor_setting_handler_vendor(vendor_setting)
    result = check_vendor_valid(vendor_setting)
    if result['result'] == 'error':
        return result
    app_id = vendor_setting.app_id
    vendor_id = generate_vendor_id(vendor_setting.vendor_type)
    redis = lanying_redis.get_redis_connection()
    fields = vendor_setting.to_hmset_fields()
    fields['status'] = 'normal'
    fields['create_time'] = now
    fields['vendor_id'] = vendor_id
    logging.info(f"create vendor start | app_id:{app_id}, vendor_info:{hide_secret_info(fields)}")
    redis.hmset(get_vendor_key(app_id, vendor_id), fields)
    redis.rpush(get_vendor_list_key(app_id), vendor_id)
    return {
        'result': 'ok',
        'data': {
            'vendor_id': vendor_id
        }
    }

def configure_vendor(vendor_id, vendor_setting: VendorSetting):
    now = int(time.time())
    vendor_setting = normalize_vendor_setting_handler_vendor(vendor_setting)
    app_id = vendor_setting.app_id
    vendor_info = get_vendor(app_id, vendor_id)
    if vendor_info is None:
        return {'result': 'error', 'message': 'vendor not exist'}
    result = check_vendor_valid(vendor_setting, vendor_info)
    if result['result'] == 'error':
        return result
    redis = lanying_redis.get_redis_connection()
    fields = vendor_setting.to_hmset_fields()
    fields['update_time'] = now
    logging.info(f"configure vendor start | app_id:{app_id}, vendor_info:{hide_secret_info(fields)}")
    redis.hmset(get_vendor_key(app_id, vendor_id), fields)
    return {
        'result': 'ok',
        'data': {
            'success': True
        }
    }

def _should_test_vendor_connection(vendor_setting: VendorSetting, old_vendor_info=None):
    if old_vendor_info is None:
        return True
    old_api_key = str(old_vendor_info.get('api_key', '') or '').strip()
    old_api_endpoint = str(old_vendor_info.get('api_endpoint', '') or '').strip()
    old_api_endpoint_server_location = str(old_vendor_info.get('api_endpoint_server_location', 'overseas') or 'overseas').strip()
    new_api_key = str(vendor_setting.api_key or '').strip()
    new_api_endpoint = str(vendor_setting.api_endpoint or '').strip()
    new_api_endpoint_server_location = str(vendor_setting.api_endpoint_server_location or 'overseas').strip()
    return old_api_key != new_api_key or old_api_endpoint != new_api_endpoint or old_api_endpoint_server_location != new_api_endpoint_server_location


def check_vendor_valid(vendor_setting: VendorSetting, old_vendor_info=None):
    vendor_type = vendor_setting.vendor_type
    api_type_config = get_api_type_config(vendor_type)
    if api_type_config is None:
        return {
            'result': 'error',
            'message': 'vendor_type_not_valid'
        }
    expected_handler_vendor = api_type_config.get('handler_vendor', '')
    vendor_setting.handler_vendor = expected_handler_vendor
    handler_vendor = expected_handler_vendor
    if handler_vendor not in vendor_to_module:
        return {
            'result': 'error',
            'message': 'handler_vendor_not_valid'
        }
    if not is_valid_vendor_api_endpoint(vendor_setting.api_endpoint):
        return {
            'result': 'error',
            'message': 'api_endpoint_not_valid'
        }
    if _should_test_vendor_connection(vendor_setting, old_vendor_info):
        connection_result = test_vendor_connection(vendor_setting)
        if connection_result['result'] == 'error':
            return connection_result
    return {
        'result': 'ok'
    }

def delete_vendor(app_id, vendor_id):
    vendor_info = get_vendor(app_id, vendor_id)
    if vendor_info is None:
        return {'result': 'error', 'message': 'vendor not exist'}
    redis = lanying_redis.get_redis_connection()
    redis.delete(get_vendor_key(app_id, vendor_id))
    redis.lrem(get_vendor_list_key(app_id), 1, vendor_id)

def hide_secret_info(vendor_info):
    new_vendor_info = copy.deepcopy(vendor_info)
    new_vendor_info['api_key'] = "****"
    new_vendor_info['secret_key'] = "****"
    return new_vendor_info

def get_vendor(app_id, vendor_id):
    redis = lanying_redis.get_redis_connection()
    key = get_vendor_key(app_id, vendor_id)
    info = lanying_redis.redis_hgetall(redis, key)
    if "create_time" in info:
        dto = {}
        for key,value in info.items():
            if key in ['create_time', 'update_time']:
                dto[key] = int(value)
            elif key in ['config_version']:
                dto[key] = int(value)
            elif key in ['model_config']:
                dto[key] = lanying_utils.safe_json_loads(value, [])
            else:
                dto[key] = value
        return dto
    return None

def get_vendor_list(app_id):
    redis = lanying_redis.get_redis_connection()
    vendor_ids = lanying_redis.redis_lrange(redis, get_vendor_list_key(app_id), 0, -1)
    vendor_list = []
    for vendor_id in vendor_ids:
        vendor_info = get_vendor(app_id, vendor_id)
        if vendor_info:
            vendor_list.append(vendor_info)
    return {
        'result': 'ok',
        'data':
            {
                'list': vendor_list
            }
    }

def generate_vendor_id(vendor_type):
    redis = lanying_redis.get_redis_connection()
    raw_id = redis.incrby("lanying_connector:grow_ai:vendor_id_generator", 1)
    return f'custom_vendor_{vendor_type}_{raw_id}'

def get_vendor_key(app_id, vendor_id):
    return f"lanying_connector:grow_ai:vendor:{app_id}:{vendor_id}"

def get_vendor_list_key(app_id):
    return f"lanying_connector:grow_ai:vendor_list:{app_id}"

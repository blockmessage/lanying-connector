import lanying_vendor_openai
import lanying_vendor_minimax
import lanying_vendor_baidu
import lanying_vendor_zhipuai
import lanying_vendor_azure
import lanying_vendor_azure2
import lanying_vendor_claude
import lanying_vendor_deepseek
import lanying_vendor_aliyun
import lanying_vendor_volcengine
import lanying_vendor_moonshot
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
    "azure2": lanying_vendor_azure2,
    "claude": lanying_vendor_claude,
    'aliyun': lanying_vendor_aliyun,
    'moonshot': lanying_vendor_moonshot
}

def vendor_configs():
    return [
        {
            'vendor': 'openai',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'aws',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'volcengine',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'siliconflow',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'deepseek',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'minimax',
            'fields': ['api_key', 'api_group_id'],
            'model_fields': []
        },
        {
            'vendor': 'baidu',
            'fields': ['api_key', 'secret_key'],
            'model_fields': []
        },
        {
            'vendor': 'zhipuai',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'azure',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': ['api_type', 'deployment'],
            'models': [
                'gpt-4-32k',
                'gpt-4',
                'gpt-35-turbo-16k',
                'gpt-35-turbo',
                'text-embedding-ada-002'
            ],
        },
        {
            'vendor': 'azure2',
            'fields': ['api_key', 'api_endpoint'],
            'model_fields': []
        },
        {
            'vendor': 'claude',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'aliyun',
            'fields': ['api_key'],
            'model_fields': []
        },
        {
            'vendor': 'moonshot',
            'fields': ['api_key'],
            'model_fields': []
        }
    ]

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
            'vendor': 'azure2',
            'backups':[
                {
                    'vendor': 'openai',
                    'transforms':{
                    }
                }
            ]
        },
        {
            'vendor': 'openai',
            'backups':[
                {
                    'vendor': 'azure2',
                    'transforms':{
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
        {
            'vendor': 'azure2',
            'backups':[
                {
                    'vendor': 'openai',
                    'transforms':{
                    }
                }
            ]
        }
    ]

def get_module(app_id, vendor):
    if vendor in vendor_to_module:
        return vendor_to_module.get(vendor)
    custom_vendor_info = get_vendor(app_id, vendor)
    if custom_vendor_info:
        vendor_type = custom_vendor_info['vendor_type']
        if vendor_type in vendor_to_module:
            return vendor_to_module.get(vendor_type)
    raise Exception('vendor_not_exist')

def list_models(app_id):
    models = []
    for vendor,module in vendor_to_module.items():
        for config in module.model_configs():
            new_config = copy.deepcopy(config)
            if 'url' in new_config:
                del new_config['url']
            if 'endpoint' in new_config:
                del new_config['endpoint']
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
        vendor_type = vendor_info['vendor_type']
        vender_show_name = vendor_info['name']
        vendor_id = vendor_info['vendor_id']
        if vendor_type in vendor_to_module:
            module = vendor_to_module[vendor_type]
            for config in module.model_configs():
                new_config = copy.deepcopy(config)
                if not model_config_valid(new_config, vendor_info):
                    continue
                if 'url' in new_config:
                    del new_config['url']
                if 'endpoint' in new_config:
                    del new_config['endpoint']
                new_config['vendor'] = vendor_id
                new_config['vendor_show_name'] = vender_show_name
                new_config['is_origin_vendor'] = False
                new_config['is_custom_vendor'] = True
                new_config['api_key_type'] = 'self'
                new_config['quota'] = get_custom_vendor_quota()
                new_config['quota_without_content_security'] = 0
                if 'image_quota' in config:
                    new_config['image_quota_without_content_security'] = {}
                    for k,_ in config['image_quota'].items():
                        new_config['image_quota'][k] = get_custom_vendor_quota()
                        new_config['image_quota_without_content_security'][k] = 0
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
                            new_config['vendor'] = vendor
                            new_config['vendor_show_name'] = vender_show_name
                            new_config['is_origin_vendor'] = False
                            new_config['is_custom_vendor'] = True
                            new_config['api_key_type'] = 'self'
                            new_config['quota'] = get_custom_vendor_quota()
                            new_config['quota_without_content_security'] = 0
                            if 'image_quota' in config:
                                new_config['image_quota_without_content_security'] = {}
                                for k,_ in config['image_quota'].items():
                                    new_config['image_quota'][k] = get_custom_vendor_quota()
                                    new_config['image_quota_without_content_security'][k] = 0
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
    legacy_preset = lanying_openai_compat.to_legacy_vendor_preset(preset)
    result = module.prepare_chat(auth_info, legacy_preset)
    if isinstance(result, dict):
        result['auth_info'] = auth_info
    return result

def chat(app_id, vendor, prepare_info, preset):
    module = get_module(app_id, vendor)
    model_config = get_chat_model_config(app_id, vendor, preset['model'])
    legacy_preset = lanying_openai_compat.to_legacy_vendor_preset(preset)
    try:
        resp = chat_with_same_model_retry(module, vendor, prepare_info, legacy_preset, model_config)
        resp = normalize_chat_response(resp)
        if 'result' in resp and resp['result'] == 'ok':
            return resp
        return chat_retry(vendor, prepare_info, legacy_preset, resp)
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
        return chat_retry(vendor, prepare_info, legacy_preset, resp)

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
    try_times = 3
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
    def __init__(self, app_id, tenement_id, vendor_type, name, api_key, secret_key, api_group_id, api_endpoint, model_config):
        self.app_id = app_id
        self.tenement_id = tenement_id
        self.vendor_type = vendor_type
        self.name = name
        self.api_key = api_key
        self.secret_key = secret_key
        self.api_group_id = api_group_id
        self.api_endpoint = api_endpoint
        self.model_config = model_config
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
            'model_config': json.dumps(self.model_config, ensure_ascii=False)
        }

def create_vendor(vendor_setting: VendorSetting):
    now = int(time.time())
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
    result = check_vendor_valid(vendor_setting)
    if result['result'] == 'error':
        return result
    app_id = vendor_setting.app_id
    vendor_info = get_vendor(app_id, vendor_id)
    if vendor_info is None:
        return {'result': 'error', 'message': 'vendor not exist'}
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

def check_vendor_valid(vendor_setting: VendorSetting):
    vendor_type = vendor_setting.vendor_type
    if vendor_type not in vendor_to_module:
        return {
            'result': 'error',
            'message': 'vendor_type_not_valid'
        }
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

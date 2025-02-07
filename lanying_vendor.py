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

vendor_to_module = {
    'openai': lanying_vendor_openai,
    'aws': lanying_vendor_aws,
    'siliconflow': lanying_vendor_siliconflow,
    'ppinfra': lanying_vendor_ppinfra,
    'minimax': lanying_vendor_minimax,
    'baidu': lanying_vendor_baidu,
    'zhipuai': lanying_vendor_zhipuai,
    "azure": lanying_vendor_azure,
    "azure2": lanying_vendor_azure2,
    "claude": lanying_vendor_claude,
    'deepseek': lanying_vendor_deepseek,
    'aliyun': lanying_vendor_aliyun,
    'volcengine': lanying_vendor_volcengine,
    'moonshot': lanying_vendor_moonshot
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

def get_module(vendor):
    return vendor_to_module.get(vendor)

def list_models():
    models = []
    for vendor,module in vendor_to_module.items():
        for config in module.model_configs():
            new_config = copy.deepcopy(config)
            if 'url' in new_config:
                del new_config['url']
            if 'endpoint' in new_config:
                del new_config['endpoint']
            new_config['vendor'] = vendor
            models.append(new_config)
    return models

def get_vendor_by_model(model):
    for vendor,module in vendor_to_module.items():
        for config in module.model_configs():
            is_prefix = config.get('is_prefix', True)
            now_model = config.get('model')
            if is_prefix and model.startswith(now_model):
                return vendor
            if model == now_model:
                return vendor
    return None

def get_chat_model_config(vendor, model):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "chat":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
                if model == now_model:
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
    return None

def get_image_model_config(vendor, model):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "image":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
                if model == now_model:
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
    return None

def get_text_to_speech_model_config(vendor, model):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "text_to_speech":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
                if model == now_model:
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
    return None

def get_speech_to_text_model_config(vendor, model):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "speech_to_text":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
                if model == now_model:
                    newConfig = copy.deepcopy(config)
                    newConfig['vendor'] = vendor
                    return newConfig
    return None

def get_embedding_model(vendor):
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "embedding":
                return config.get('model')
    return None

def get_embedding_model_config(vendor, model):
    if vendor is None:
        vendor = get_vendor_by_model(model)
    module = get_module(vendor)
    if module:
        model_configs = module.model_configs()
        for config in model_configs:
            if config['type'] == "embedding":
                is_prefix = config.get('is_prefix', True)
                now_model = config.get('model')
                if is_prefix and model.startswith(now_model):
                    return config
                if model == now_model:
                    return config
                if model == '':
                    return config
    return None

def prepare_chat(vendor, auth_info, preset):
    module = get_module(vendor)
    result = module.prepare_chat(auth_info, preset)
    if isinstance(result, dict):
        result['auth_info'] = auth_info
    return result

def chat(vendor, prepare_info, preset):
    module = get_module(vendor)
    try:
        resp = chat_with_same_model_retry(module, vendor, prepare_info, preset)
        if 'result' in resp and resp['result'] == 'ok':
            return resp
        return chat_retry(vendor, prepare_info, preset, resp)
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
        return chat_retry(vendor, prepare_info, preset, resp)

def chat_with_same_model_retry(module, vendor, prepare_info, preset):
    try_times = 3
    for i in range(try_times):
        try:
            resp = module.chat(prepare_info, preset)
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
    if need_notify_failed(vendor):
        lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Chat 返回异常, id:{unique_id}, vendor:{vendor}, model:{model}, resp:{resp}', f'ai_chat_resp_failed_{vendor}')
    try:
        new_resp = do_chat_retry(vendor, prepare_info, preset, resp, unique_id)
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
                    new_model_config = get_chat_model_config(new_vendor, new_model)
                    if new_model_config:
                        new_preset = copy.deepcopy(preset)
                        new_preset['model'] = new_model
                        logging.info(f"chat backup start | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                        new_auth_info = lanying_config.get_lanying_connector_share_auth_info(new_vendor)
                        new_prepare_info = prepare_chat(new_vendor, new_auth_info, new_preset)
                        new_module = get_module(new_vendor)
                        new_resp = chat_with_same_model_retry(new_module, new_vendor, new_prepare_info, new_preset)
                        if 'result' in new_resp and new_resp['result'] == 'ok':
                            logging.info(f"chat backup success | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                            lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回成功, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                            return new_resp
                        else:
                            lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                except Exception as e:
                    lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Chat 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_switch_{new_vendor}')
                    logging.error(e)
            logging.info(f"chat backup failed | app_id:{app_id}, vendor:{vendor}, model:{model}")
    return resp

def prepare_embedding(vendor, auth_info, type):
    module = get_module(vendor)
    result = module.prepare_embedding(auth_info, type)
    if isinstance(result, dict):
        result['auth_info'] = auth_info
        result['type'] = type
    return result

def embedding(vendor, prepare_info, model, text):
    module = get_module(vendor)
    retry_times = 5
    for i in range(retry_times):
        try:
            resp = module.embedding(prepare_info, model, text)
            if 'result' in resp and resp['result'] == 'ok':
                return resp
            if i == retry_times - 1:
                logging.info(f"embedding finally failed: {i}/{retry_times}, resp:{resp}")
                return embedding_retry(vendor, prepare_info, model, text, resp)
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
                return embedding_retry(vendor, prepare_info, model, text, resp)
            else:
                logging.info(f"embedding schedule retry: {i}/{retry_times}, resp:{resp}")
                time.sleep(0.5)

def need_notify_failed(vendor):
    if lanying_utils.is_preview_server():
        return False
    # if vendor == 'siliconflow' or vendor == 'deepseek':
    #     return False
    return True

def embedding_retry(vendor, prepare_info, model, text, resp):
    unique_id = datetime.now().strftime('%Y-%m-%d-%H-%M-%S.%f')
    if need_notify_failed(vendor):
        lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 返回异常, id:{unique_id}, vendor:{vendor}, model:{model}, resp:{resp}', f'ai_embedding_resp_failed_{vendor}')
    try:
        new_resp = do_embedding_retry(vendor, prepare_info, model, text, resp, unique_id)
        if 'result' in new_resp and new_resp['result'] == 'ok':
            return new_resp
        return resp
    except Exception as e:
        logging.error(e)
        return resp

def do_embedding_retry(vendor, prepare_info, model, text, resp, unique_id):
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
                    new_model_config = get_embedding_model_config(new_vendor, new_model)
                    if new_model_config:
                        logging.info(f"embedding backup start | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                        new_auth_info = lanying_config.get_lanying_connector_share_auth_info(new_vendor)
                        new_prepare_info = prepare_embedding(new_vendor, new_auth_info, type)
                        new_module = get_module(new_vendor)
                        new_resp = new_module.embedding(new_prepare_info, new_model, text)
                        if 'result' in new_resp and new_resp['result'] == 'ok':
                            logging.info(f"embedding backup success | app_id:{app_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}")
                            lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回成功, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                            return new_resp
                        else:
                            lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                except Exception as e:
                    lanying_slack.async_send_message_with_filter(f'【蓝莺Connector】AI Embedding 切换厂商，新厂商返回失败, id:{unique_id}, vendor:{vendor}, model:{model}, new_vendor:{new_vendor}, new_model:{new_model}', f'ai_embedding_switch_{new_vendor}')
                    logging.error(e)
            logging.info(f"embedding backup failed | app_id:{app_id}, vendor:{vendor}, model:{model}")
    return resp

def encoding_for_model(vendor, model):
    module = get_module(vendor)
    return module.encoding_for_model(model)

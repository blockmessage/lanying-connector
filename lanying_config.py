import etcd3
import logging
import json
import os

prefix = os.getenv('LANYING_CONNECTOR_APP_CONFIG_PREFIX')
configs = {}
mode = 'env'
etcd = None

def key_changed(watch_response):
    for event in watch_response.events:
        if isinstance(event, etcd3.events.PutEvent):
            configs[event.key.decode("utf-8") ] = parse_value(event.value)
        elif isinstance(event, etcd3.events.DeleteEvent):
            configs.pop(event.key.decode("utf-8"))

def init():
    etcdServer = os.getenv('LANYING_CONNECTOR_ETCD_SERVER')
    etcdPort = os.getenv('LANYING_CONNECTOR_ETCD_PORT')
    if etcdServer != None and etcdPort != None:
        global mode
        global etcd
        mode = 'etcd'
        etcd = etcd3.client(host = etcdServer, port=etcdPort)
        for (value, meta) in etcd.get_prefix(prefix):
            configs[meta.key.decode("utf-8") ] = parse_value(value)
        etcd.add_watch_prefix_callback(prefix, key_changed)

def parse_value(value):
    try:
        return json.loads(value)
    except Exception as e:
        return None

def get_config(appId, key, default):
    return configs.get(prefix + appId + '.'+key, default)

def save_config(appId, key, value):
    global etcd
    if etcd:
        etcd.put(prefix + appId + '.'+key, value)

def get_config_field(appId, key, field, default):
    value = configs.get(prefix + appId + '.'+key)
    if value:
        return value.get(field, default)
    return default

def get_all_config(app_id = None):
    if app_id is None:
        return configs
    app_prefix = prefix + app_id + '.'
    ret = {}
    for k,v in configs.items():
        if k.startswith(app_prefix):
            ret[k] = v
    return ret

def parse_key(key):
    key_without_prefix = key[len(prefix):]
    app_id = key_without_prefix.split('.')[0]
    short_key = key_without_prefix[(len(app_id)+1):]
    return (app_id, short_key)

def get_lanying_user_id(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_user_id', None)
    return os.getenv('LANYING_USER_ID')

def get_lanying_connector_service(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_service', None)
    return os.getenv('LANYING_CONNECTOR_SERVICE')

def get_message_404(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_404', "抱歉，因为某些无法说明的原因，我暂时无法回答你的问题。")
    return os.getenv('LANYING_CONNECTOR_MESSAGE_404')

def get_message_no_quota(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_no_quota', "抱歉，当前应用的本月消息配额已经用完，请联系管理员或者下月重试。")
    return os.getenv('LANYING_CONNECTOR_MESSAGE_NO_QUOTA')

def get_message_reach_user_message_limit(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_reach_user_message_limit', "抱歉，您本月消息配额已经用完，请联系管理员或者下月重试。")
    return os.getenv('LANYING_CONNECTOR_MESSAGE_REACH_USER_MESSAGE_LIMIT')

def get_message_deduct_failed(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_deduct_failed', "抱歉，当前应用已欠费，请联系管理员或者稍后重试。")
    return os.getenv('LANYING_CONNECTOR_MESSAGE_DEDUCT_FAILED')

def get_message_too_long(appId):
    default = "抱歉，当前消息长度超过限制，建议分段处理。"
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_too_long', default)
    return os.getenv('LANYING_CONNECTOR_MESSAGE_TOO_LONG', default)

def get_message_antispam(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_connector_message_antispam', "对不起，因为系统设定的原因，这个问题我无法回答，请您谅解。")
    return os.getenv('LANYING_CONNECTOR_MESSAGE_ANTISPAM')

def get_lanying_admin_token(appId):
    if mode == 'etcd':
        return get_config_field(appId, 'lanying_connector', 'lanying_admin_token', None)
    return os.getenv('LANYING_ADMIN_TOKEN')

def get_lanying_callback_signature(appId):
    if mode == 'etcd':
         return get_config_field(appId, 'lanying_connector', 'lanying_callback_signature', None)
    return os.getenv('LANYING_CALLBACK_SIGNATURE')

def get_lanying_connector_rate_limit(appId):
    if mode == 'etcd':
        rate_limit = get_config(appId, 'lanying_connector.rate_limit', None)
        if rate_limit and rate_limit >= 0:
            return rate_limit
        return get_config("global", 'lanying_connector.rate_limit', 30)
    return int(os.getenv('LANYING_RATE_LIMIT', "30"))

def get_lanying_connector_function_num_limit(appId):
    if mode == 'etcd':
        function_num = get_config(appId, 'lanying_connector.function_num', None)
        if function_num and function_num >= 0:
            return function_num
        return get_config("global", 'lanying_connector.function_num', 0)
    return int(os.getenv('LANYING_CONNECTOR_AI_PLUGIN_FUNCTION_NUM_LIMIT', "10"))

def get_lanying_connector_deduct_failed(appId):
    if mode == 'etcd':
        return get_config(appId, 'lanying_connector.deduct_failed', False)
    return False

def get_lanying_connector(appId):
    if mode == 'etcd':
        return get_config(appId, 'lanying_connector', None)
    service = get_lanying_connector_service(appId)
    if service == 'openai':
        with open(f"configs/{service}.json", "r") as f:
            config = json.load(f)
            openaiAPIKey = os.getenv('OPENAI_API_KEY')
            if openaiAPIKey:
                config['openai_api_key'] = openaiAPIKey
            return config

def get_lanying_connector_default_api_key(vendor):
    return os.getenv(f'{vendor.upper()}_API_KEY')

def get_lanying_connector_default_api_group_id(vendor):
    return os.getenv(f'{vendor.upper()}_API_GROUP_ID', '')

def get_lanying_connector_default_secret_key(vendor):
    return os.getenv(f'{vendor.upper()}_SECRET_KEY', '')

def get_service_config(app_id, service):
    if mode == 'etcd':
        config = get_config(app_id, f"lanying_connector.service.{service}", None)
        if service == 'openai' and config is None:
            config = get_config(app_id, 'lanying_connector', None)
        if config and 'enable' in config and config['enable'] == True:
            return config
        return None
    with open(f"configs/{service}.json", "r") as f:
        config = json.load(f)
        if service == 'openai':
            openaiAPIKey = os.getenv('OPENAI_API_KEY')
            if openaiAPIKey:
                config['openai_api_key'] = openaiAPIKey
        return config

def get_lanying_api_endpoint(appId):
    return os.getenv('LANYING_API_ENDPOINT', 'https://s-1-3-api.maximtop.cn')

def get_service_list():
    return os.getenv('LANYING_CONNECTOR_SERVICE_LIST', 'openai,wechat_official_account,wechat').split(',')

def is_show_info_page():
    if mode == 'etcd':
        return False
    return True

def get_embedding_auth_secret():
    return os.getenv('EMBEDDING_AUTH_SECRET')